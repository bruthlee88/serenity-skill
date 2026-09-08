import os, time, json, math
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
from curl_cffi import requests

BJ = timezone(timedelta(hours=8))
RUN_DATE = datetime.now(BJ).strftime('%Y-%m-%d')
RUN_DATE_COMPACT = datetime.now(BJ).strftime('%Y%m%d')
OUTDIR = os.path.join(os.path.dirname(__file__), 'out_final')
os.makedirs(OUTDIR, exist_ok=True)

T0 = time.perf_counter()
timing = {}
errors = []


def now_bj():
    return datetime.now(BJ).isoformat(timespec='seconds')

class Timer:
    def __init__(self, name): self.name=name
    def __enter__(self): self.t=time.perf_counter(); return self
    def __exit__(self, *args): timing[self.name]=round(time.perf_counter()-self.t,4)


def fetch_spot_eastmoney():
    url='https://push2.eastmoney.com/api/qt/clist/get'
    params={
        'pn':1,'pz':6000,'po':1,'np':1,'fltt':2,'invt':2,'fid':'f3',
        'fs':'m:1+t:2,m:1+t:23,m:0+t:6,m:0+t:80',
        'fields':'f12,f14,f2,f3,f5,f6,f15,f16,f17,f18'
    }
    headers={'Referer':'https://quote.eastmoney.com/','Accept':'application/json,text/plain,*/*'}
    last=None
    for attempt in range(4):
        try:
            r=requests.get(url,params=params,headers=headers,timeout=20,impersonate='chrome')
            r.raise_for_status()
            j=r.json()
            diff=j.get('data',{}).get('diff',[]) or []
            rows=[]
            for x in diff:
                rows.append({
                    '代码':str(x.get('f12','')).zfill(6), '名称':str(x.get('f14','')),
                    '最新价':x.get('f2'), '涨跌幅':x.get('f3'), '成交量':x.get('f5'), '成交额':x.get('f6'),
                    '最高':x.get('f15'), '最低':x.get('f16'), '今开':x.get('f17'), '昨收':x.get('f18')
                })
            d=pd.DataFrame(rows)
            for c in ['最新价','涨跌幅','成交量','成交额','最高','最低','今开','昨收']:
                d[c]=pd.to_numeric(d[c],errors='coerce')
            d=d.dropna(subset=['代码','名称','最新价','成交额'])
            d=d[~d['名称'].str.contains('ST',case=False,na=False)]
            # Eastmoney fs above is SH/SZ A shares only; no Beijing board.
            d=d[d['成交额']>=200_000_000]
            d=d[d['最新价']>0]
            return d.drop_duplicates('代码').reset_index(drop=True), int(j.get('data',{}).get('total',len(diff)))
        except Exception as e:
            last=f'{type(e).__name__}: {e}'
            time.sleep(1.0+attempt)
    raise RuntimeError('spot failed: '+str(last))


def hist_symbol(code):
    return ('sh' if str(code).startswith(('5','6','9')) else 'sz') + str(code)


def fetch_hist_one(code, spot_row):
    sym=hist_symbol(code)
    last=None
    for attempt in range(3):
        try:
            df=ak.stock_zh_a_daily(symbol=sym,start_date='19900101',end_date=RUN_DATE_COMPACT,adjust='')
            if df is None or df.empty: raise RuntimeError('empty history')
            x=df.reset_index(drop=False).copy()
            # AKShare returns: date/open/high/low/close/volume/amount/...
            ren={'date':'日期','open':'开盘','high':'最高','low':'最低','close':'收盘','volume':'成交量','amount':'成交额'}
            x=x.rename(columns=ren)
            for c in ['日期','开盘','最高','最低','收盘','成交量']:
                if c not in x.columns: raise RuntimeError(f'missing {c}')
            x['日期']=pd.to_datetime(x['日期'])
            for c in ['开盘','最高','最低','收盘','成交量']:
                x[c]=pd.to_numeric(x[c],errors='coerce')
            if '成交额' not in x.columns: x['成交额']=pd.NA
            x['成交额']=pd.to_numeric(x['成交额'],errors='coerce')
            x=x[['日期','开盘','最高','最低','收盘','成交量','成交额']].dropna(subset=['日期','开盘','最高','最低','收盘','成交量']).sort_values('日期').drop_duplicates('日期',keep='last').reset_index(drop=True)
            today=pd.Timestamp(RUN_DATE)
            cur=datetime.now(BJ)
            # Before close, force today's temporary daily bar from realtime snapshot. After close, keep official daily history if present.
            if x.empty or x.iloc[-1]['日期'].normalize() < today:
                sr=spot_row
                tmp=pd.DataFrame([{'日期':today,'开盘':float(sr['今开']),'最高':float(sr['最高']),'最低':float(sr['最低']),'收盘':float(sr['最新价']),'成交量':float(sr['成交量']),'成交额':float(sr['成交额'])}])
                x=pd.concat([x,tmp],ignore_index=True)
            elif x.iloc[-1]['日期'].normalize()==today and cur.hour<15:
                sr=spot_row
                x.loc[x.index[-1],['开盘','最高','最低','收盘','成交量','成交额']]=[float(sr['今开']),float(sr['最高']),float(sr['最低']),float(sr['最新价']),float(sr['成交量']),float(sr['成交额'])]
            x['EMA13']=x['收盘'].ewm(span=13,adjust=False).mean()
            x['EMA21']=x['收盘'].ewm(span=21,adjust=False).mean()
            x['EMA610']=x['收盘'].ewm(span=610,adjust=False).mean()
            x['MA60V']=x['成交量'].rolling(60,min_periods=60).mean()
            return str(code),x,None
        except Exception as e:
            last=f'{type(e).__name__}: {e}'
            time.sleep(0.7*(attempt+1))
    return str(code),None,last


def base_streak(df,end_idx=None):
    if end_idx is None: end_idx=len(df)-1
    if end_idx<0: return 0,None,None,end_idx+1
    c=df['收盘'].to_numpy(float); h=df['最高'].to_numpy(float); e=df['EMA13'].to_numpy(float)
    if not(c[end_idx]>e[end_idx]): return 0,None,None,end_idx+1
    cnt=0; used=False; bi=None; ri=None; i=end_idx
    while i>=0:
        if c[i]>e[i]: cnt+=1; i-=1; continue
        if (not used and c[i]<e[i] and c[i]>e[i]*0.97 and i+1<=end_idx and c[i+1]>h[i] and c[i+1]>e[i+1]):
            used=True; bi=i; ri=i+1; cnt+=1; i-=1; continue
        break
    return cnt,bi,ri,i+1


def strict_streak(close, benchmark):
    n=0
    for c,b in zip(reversed(close),reversed(benchmark)):
        if pd.notna(b) and float(c)>float(b): n+=1
        else: break
    return n


def v10_streak(df):
    # Per user rule, exception requires O=H=L=C and V<MA60. Price-limit status cannot be reliably inferred for every special board solely from the row;
    # we additionally require one-price bar and >=9.5% absolute close-to-prev-close move, covering standard/20% limit boards conservatively.
    n=0
    for i in range(len(df)-1,-1,-1):
        ma=df.iloc[i]['MA60V']
        if pd.isna(ma): break
        v=float(df.iloc[i]['成交量']); ma=float(ma)
        if v>ma: n+=1; continue
        if v<ma and i>0:
            o=float(df.iloc[i]['开盘']);h=float(df.iloc[i]['最高']);l=float(df.iloc[i]['最低']);c=float(df.iloc[i]['收盘']);pc=float(df.iloc[i-1]['收盘'])
            one=abs(o-h)<1e-9 and abs(o-l)<1e-9 and abs(o-c)<1e-9
            move=abs(c/pc-1) if pc else 0
            if one and move>=0.095: n+=1; continue
        break
    return n


def e22_streak(df):
    n=0
    for i in range(len(df)-1,-1,-1):
        if float(df.iloc[i]['收盘'])>float(df.iloc[i]['EMA21'])*1.1: n+=1
        else: break
    return n


def previous_long_short_trend(df,sig_i):
    for j in range(max(0,sig_i-20),sig_i):
        if base_streak(df,j)[0]>15: return True
    return False


def shortest_pullback(df,sig_i):
    c=df['收盘'].to_numpy(float); h=df['最高'].to_numpy(float)
    for k in range(3,min(20,sig_i)+1):
        st=sig_i-k; en=sig_i-1
        if not(c[en]<c[st]-0.001): continue
        hi=float(max(h[st:sig_i]))
        if c[sig_i]+0.001>=hi: return k,hi
    return None,None


def find_recovery(df,current_start,ema610_days):
    if ema610_days<10 or len(df)<65: return None
    c=df['收盘'].to_numpy(float);o=df['开盘'].to_numpy(float);e=df['EMA13'].to_numpy(float);v=df['成交量'].to_numpy(float);m=df['MA60V'].to_numpy(float)
    for s in range(max(3,len(df)-5),len(df)):
        if s<current_start: continue
        if not(c[s-1]<e[s-1] and c[s-2]<e[s-2] and c[s-3]<e[s-3]): continue
        if not c[s]>e[s]: continue
        pct=(c[s]/c[s-1]-1)*100; body=(c[s]/o[s]-1)*100 if o[s] else -999
        if pct<5 or body<3: continue
        if not(math.isfinite(m[s]) and v[s]>m[s]): continue
        if not previous_long_short_trend(df,s): continue
        k,hi=shortest_pullback(df,s)
        if k is None: continue
        return {'收复日':df.iloc[s]['日期'].strftime('%Y-%m-%d'),'收复日涨幅(%)':round(pct,4),'阳线实体(%)':round(body,4),'收复日成交量':float(v[s]),'60日均量':round(float(m[s]),4),'V>MA60':True,'收复日前3日均在线下':True,'此前>15天趋势':True,'最短回落区间(根)':int(k),'回落区间最高价':round(hi,4),'收复收盘覆盖':True}
    return None


def dt(df,i): return '' if i is None else df.iloc[i]['日期'].strftime('%Y-%m-%d')

run_started=now_bj()
with Timer('spot_fetch_seconds'):
    spot, total_market = fetch_spot_eastmoney()
spot_completed=now_bj()

hist={}
with Timer('history_fetch_seconds'):
    workers=16
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(fetch_hist_one,str(r['代码']),r.to_dict()):str(r['代码']) for _,r in spot.iterrows()}
        for fut in as_completed(futs):
            code,df,err=fut.result()
            if df is not None: hist[code]=df
            else: errors.append({'代码':code,'错误':err})

basic=[];focus=[];recovery=[]
with Timer('screen_compute_seconds'):
    for _,sr in spot.iterrows():
        code=str(sr['代码']); df=hist.get(code)
        if df is None or df.empty: continue
        st,bi,ri,start_i=base_streak(df)
        if st<4: continue
        e610=strict_streak(df['收盘'].tolist(),df['EMA610'].tolist())
        row={'股票代码':code,'股票简称':str(sr['名称']),'统计时点价':float(sr['最新价']),'涨跌幅(%)':float(sr['涨跌幅']),'成交金额(亿元)':round(float(sr['成交额'])/1e8,4),'EMA13':round(float(df.iloc[-1]['EMA13']),4),'EMA13持续天数':int(st),'分类':'关注' if st<=5 else ('趋势' if st<=20 else '长期趋势'),'跌破日':dt(df,bi),'收复日':dt(df,ri),'v1.0连续天数':int(v10_streak(df)),'e22连续天数':int(e22_streak(df)),'EMA610':round(float(df.iloc[-1]['EMA610']),4),'EMA610连续天数':int(e610),'重点关注':bool(e610>=3),'当前持续区间起始日':dt(df,start_i)}
        basic.append(row)
        if e610>=3: focus.append(row.copy())
        rec=find_recovery(df,start_i,e610)
        if rec:
            rr={'股票代码':code,'股票简称':str(sr['名称']),'统计时点价':float(sr['最新价']),'统计日成交额(亿元)':round(float(sr['成交额'])/1e8,4),'EMA13持续天数':int(st),'EMA610连续天数':int(e610)}; rr.update(rec); recovery.append(rr)

basic.sort(key=lambda x:(-x['EMA13持续天数'],-x['成交金额(亿元)']))
focus.sort(key=lambda x:(-x['EMA13持续天数'],-x['成交金额(亿元)']))
recovery.sort(key=lambda x:(x['收复日'],-x['统计日成交额(亿元)']))

with Timer('csv_write_seconds'):
    pd.DataFrame(basic).to_csv(os.path.join(OUTDIR,'basic.csv'),index=False,encoding='utf-8-sig')
    pd.DataFrame(focus).to_csv(os.path.join(OUTDIR,'focus.csv'),index=False,encoding='utf-8-sig')
    pd.DataFrame(recovery).to_csv(os.path.join(OUTDIR,'recovery.csv'),index=False,encoding='utf-8-sig')
    pd.DataFrame(errors).to_csv(os.path.join(OUTDIR,'errors.csv'),index=False,encoding='utf-8-sig')
    spot.to_csv(os.path.join(OUTDIR,'spot_filtered.csv'),index=False,encoding='utf-8-sig')

meta={'run_date':RUN_DATE,'run_started_bj':run_started,'spot_fetch_completed_bj':spot_completed,'run_completed_bj':now_bj(),'market_total_reported':total_market,'universe_after_filters':len(spot),'history_success_count':len(hist),'history_failure_count':len(errors),'basic_count':len(basic),'focus_count':len(focus),'recovery_count':len(recovery),'workers':workers,'timing':timing,'total_seconds':round(time.perf_counter()-T0,4),'data_sources':{'realtime':'Eastmoney push2 clist/get via curl_cffi browser fingerprint','history':'AKShare stock_zh_a_daily (Sina-backed daily history)'},'snapshot_note':'Manual run uses the actual execution-time Eastmoney snapshot. Scheduled production run should start at 13:30 Beijing time.'}
with open(os.path.join(OUTDIR,'meta.json'),'w',encoding='utf-8') as f: json.dump(meta,f,ensure_ascii=False,indent=2)
print(json.dumps(meta,ensure_ascii=False,indent=2))
