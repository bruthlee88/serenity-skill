import os
import time
import json
import math
import traceback
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak

BJ = timezone(timedelta(hours=8))
TODAY = datetime.now(BJ).strftime('%Y%m%d')
TODAY_DASH = datetime.now(BJ).strftime('%Y-%m-%d')
OUTDIR = os.path.join(os.path.dirname(__file__), 'out')
os.makedirs(OUTDIR, exist_ok=True)

T0 = time.perf_counter()
timing = {}
errors = []


def now_bj():
    return datetime.now(BJ).isoformat(timespec='seconds')


def timed(label):
    class Ctx:
        def __enter__(self):
            self.t = time.perf_counter()
            return self
        def __exit__(self, exc_type, exc, tb):
            timing[label] = round(time.perf_counter() - self.t, 4)
    return Ctx()


def norm_spot(df):
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    for c in ['代码','名称']:
        d[c] = d[c].astype(str)
    nums = ['最新价','涨跌幅','成交量','成交额','最高','最低','今开','昨收']
    for c in nums:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors='coerce')
    return d


def fetch_spot():
    # Parallel SH/SZ fetch; both are Eastmoney-backed AKShare interfaces.
    with ThreadPoolExecutor(max_workers=2) as ex:
        fs = [ex.submit(ak.stock_sh_a_spot_em), ex.submit(ak.stock_sz_a_spot_em)]
        parts = [norm_spot(f.result()) for f in fs]
    d = pd.concat(parts, ignore_index=True)
    d = d.drop_duplicates('代码', keep='last')
    d = d[~d['名称'].str.contains('ST', case=False, na=False)].copy()
    d = d[pd.to_numeric(d['成交额'], errors='coerce') >= 200_000_000].copy()
    d = d[d['最新价'].notna() & (d['最新价'] > 0)].copy()
    return d.reset_index(drop=True)


def fetch_hist_one(code, spot_row):
    last_err = None
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_hist(
                symbol=str(code), period='daily', start_date='19900101',
                end_date=TODAY, adjust='', timeout=20
            )
            if df is None or df.empty:
                raise RuntimeError('empty history')
            x = df.copy()
            x['日期'] = pd.to_datetime(x['日期'])
            for c in ['开盘','收盘','最高','最低','成交量','成交额']:
                x[c] = pd.to_numeric(x[c], errors='coerce')
            x = x.dropna(subset=['日期','开盘','收盘','最高','最低','成交量']).sort_values('日期')
            x = x.drop_duplicates('日期', keep='last').reset_index(drop=True)

            # At 13:30 the daily-history endpoint normally has not published today's bar yet.
            # Append the current Eastmoney snapshot as a temporary daily K bar if needed.
            today_ts = pd.Timestamp(TODAY_DASH)
            if x.empty or x.iloc[-1]['日期'].normalize() < today_ts:
                sr = spot_row
                tmp = pd.DataFrame([{
                    '日期': today_ts,
                    '开盘': float(sr['今开']),
                    '收盘': float(sr['最新价']),
                    '最高': float(sr['最高']),
                    '最低': float(sr['最低']),
                    '成交量': float(sr['成交量']),
                    '成交额': float(sr['成交额']),
                }])
                x = pd.concat([x[['日期','开盘','收盘','最高','最低','成交量','成交额']], tmp], ignore_index=True)
            elif x.iloc[-1]['日期'].normalize() == today_ts:
                # If history already contains today (e.g. manual run after close), keep official daily row.
                x = x[['日期','开盘','收盘','最高','最低','成交量','成交额']].copy()
            else:
                x = x[['日期','开盘','收盘','最高','最低','成交量','成交额']].copy()

            x['EMA13'] = x['收盘'].ewm(span=13, adjust=False).mean()
            x['EMA21'] = x['收盘'].ewm(span=21, adjust=False).mean()
            x['EMA610'] = x['收盘'].ewm(span=610, adjust=False).mean()
            x['MA60V'] = x['成交量'].rolling(60, min_periods=60).mean()
            return str(code), x, None
        except Exception as e:
            last_err = f'{type(e).__name__}: {e}'
            time.sleep(0.8 * (attempt + 1))
    return str(code), None, last_err


def base_streak(df, end_idx=None):
    if end_idx is None:
        end_idx = len(df) - 1
    if end_idx < 0:
        return 0, None, None, end_idx + 1
    c = df['收盘'].to_numpy(float)
    h = df['最高'].to_numpy(float)
    e = df['EMA13'].to_numpy(float)
    if not (math.isfinite(c[end_idx]) and math.isfinite(e[end_idx]) and c[end_idx] > e[end_idx]):
        return 0, None, None, end_idx + 1
    cnt = 0
    used = False
    break_i = None
    recover_i = None
    i = end_idx
    while i >= 0:
        if c[i] > e[i]:
            cnt += 1
            i -= 1
            continue
        # Equality is neither above nor tolerated.
        if (not used and c[i] < e[i] and c[i] > e[i] * 0.97 and i + 1 <= end_idx
                and c[i+1] > h[i] and c[i+1] > e[i+1]):
            used = True
            break_i = i
            recover_i = i + 1
            cnt += 1
            i -= 1
            continue
        break
    return cnt, break_i, recover_i, i + 1


def ema610_streak(df):
    c = df['收盘'].to_numpy(float)
    e = df['EMA610'].to_numpy(float)
    n = 0
    for i in range(len(df)-1, -1, -1):
        if math.isfinite(e[i]) and c[i] > e[i]:
            n += 1
        else:
            break
    return n


def price_limit_rate(code):
    s = str(code)
    return Decimal('0.20') if s.startswith(('300','301','688','689')) else Decimal('0.10')


def q2(x):
    return Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def is_one_price_limit(df, i, code):
    if i <= 0:
        return False
    o,h,l,c = [float(df.iloc[i][k]) for k in ['开盘','最高','最低','收盘']]
    if not (abs(o-h) < 1e-9 and abs(o-l) < 1e-9 and abs(o-c) < 1e-9):
        return False
    prev = Decimal(str(float(df.iloc[i-1]['收盘'])))
    rate = price_limit_rate(code)
    up = q2(prev * (Decimal('1') + rate))
    dn = q2(prev * (Decimal('1') - rate))
    cc = Decimal(str(c)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    return cc == up or cc == dn


def v10_streak(df, code):
    n = 0
    for i in range(len(df)-1, -1, -1):
        v = float(df.iloc[i]['成交量'])
        ma = df.iloc[i]['MA60V']
        if pd.isna(ma):
            break
        ma = float(ma)
        if v > ma:
            n += 1
            continue
        # Only BELOW-average one-price limit-up/down bars are exceptions.
        # Exactly equal to MA60 fails even if one-price.
        if v < ma and is_one_price_limit(df, i, code):
            n += 1
            continue
        break
    return n


def e22_streak(df):
    n = 0
    for i in range(len(df)-1, -1, -1):
        c = float(df.iloc[i]['收盘'])
        e = float(df.iloc[i]['EMA21'])
        if c > e * 1.10:
            n += 1
        else:
            break
    return n


def previous_long_short_trend(df, signal_i):
    lo = max(0, signal_i - 20)
    for j in range(lo, signal_i):
        st, _, _, _ = base_streak(df, j)
        if st > 15:
            return True
    return False


def shortest_pullback(df, signal_i):
    c = df['收盘'].to_numpy(float)
    h = df['最高'].to_numpy(float)
    for k in range(3, min(20, signal_i) + 1):
        st = signal_i - k
        en = signal_i - 1
        # Overall decline over the prior interval.
        if not (c[en] < c[st] - 0.001):
            continue
        hi = float(max(h[st:signal_i]))
        # Code-side tolerance requested for some price comparisons.
        if c[signal_i] + 0.001 >= hi:
            return k, hi
    return None, None


def find_big_recovery(df, current_start, ema610_days):
    if ema610_days < 10 or len(df) < 65:
        return None
    c = df['收盘'].to_numpy(float)
    o = df['开盘'].to_numpy(float)
    e = df['EMA13'].to_numpy(float)
    v = df['成交量'].to_numpy(float)
    mav = df['MA60V'].to_numpy(float)
    dates = df['日期'].dt.strftime('%Y-%m-%d').tolist()
    n = len(df)
    # Earliest qualifying signal within the latest five actual bars.
    for s in range(max(3, n-5), n):
        if s < current_start:
            continue
        if not (c[s-1] < e[s-1] and c[s-2] < e[s-2] and c[s-3] < e[s-3]):
            continue
        if not (c[s] > e[s]):
            continue
        pct = (c[s] / c[s-1] - 1.0) * 100.0
        body = (c[s] / o[s] - 1.0) * 100.0 if o[s] else float('-inf')
        if pct + 1e-12 < 5.0 or body + 1e-12 < 3.0:
            continue
        if not (math.isfinite(mav[s]) and v[s] > mav[s]):
            continue
        if not previous_long_short_trend(df, s):
            continue
        k, hi = shortest_pullback(df, s)
        if k is None:
            continue
        return {
            '收复日': dates[s],
            '收复日涨幅(%)': round(pct, 4),
            '阳线实体(%)': round(body, 4),
            '收复日成交量': float(v[s]),
            '60日均量': round(float(mav[s]), 4),
            'V>MA60': True,
            '收复日前3日均在线下': True,
            '此前>15天趋势': True,
            '最短回落区间(根)': int(k),
            '回落区间最高价': round(float(hi), 4),
            '收复收盘覆盖': bool(c[s] + 0.001 >= hi),
        }
    return None


def safe_date(df, idx):
    if idx is None:
        return ''
    return df.iloc[idx]['日期'].strftime('%Y-%m-%d')


with timed('spot_fetch_seconds'):
    spot = fetch_spot()
spot_fetch_at = now_bj()

history = {}
with timed('history_fetch_seconds'):
    workers = min(20, max(4, (os.cpu_count() or 4) * 3))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {}
        for _, r in spot.iterrows():
            fut = ex.submit(fetch_hist_one, str(r['代码']), r.to_dict())
            futs[fut] = str(r['代码'])
        for fut in as_completed(futs):
            code, df, err = fut.result()
            if df is not None:
                history[code] = df
            else:
                errors.append({'代码': code, '错误': err})

basic = []
focus = []
recovery = []

with timed('screen_compute_seconds'):
    for _, sr in spot.iterrows():
        code = str(sr['代码'])
        df = history.get(code)
        if df is None or df.empty:
            continue
        st, bi, ri, start_i = base_streak(df)
        if st < 4:
            continue
        em610 = ema610_streak(df)
        v10 = v10_streak(df, code)
        e22 = e22_streak(df)
        if st <= 5:
            cat = '关注'
        elif st <= 20:
            cat = '趋势'
        else:
            cat = '长期趋势'
        row = {
            '股票代码': code,
            '股票简称': str(sr['名称']),
            '统计时点价': float(sr['最新价']),
            '涨跌幅(%)': float(sr['涨跌幅']),
            '成交金额(亿元)': round(float(sr['成交额']) / 1e8, 4),
            'EMA13': round(float(df.iloc[-1]['EMA13']), 4),
            'EMA13持续天数': int(st),
            '分类': cat,
            '跌破日': safe_date(df, bi),
            '收复日': safe_date(df, ri),
            'v1.0连续天数': int(v10),
            'e22连续天数': int(e22),
            'EMA610': round(float(df.iloc[-1]['EMA610']), 4),
            'EMA610连续天数': int(em610),
            '重点关注': bool(em610 >= 3),
            '当前持续区间起始日': safe_date(df, start_i),
        }
        basic.append(row)
        if em610 >= 3:
            focus.append(row.copy())
        rec = find_big_recovery(df, start_i, em610)
        if rec is not None:
            rr = {
                '股票代码': code,
                '股票简称': str(sr['名称']),
                '统计时点价': float(sr['最新价']),
                '统计日成交额(亿元)': round(float(sr['成交额']) / 1e8, 4),
                'EMA13持续天数': int(st),
                'EMA610连续天数': int(em610),
            }
            rr.update(rec)
            recovery.append(rr)

basic.sort(key=lambda x: (-x['EMA13持续天数'], -x['成交金额(亿元)']))
focus.sort(key=lambda x: (-x['EMA13持续天数'], -x['成交金额(亿元)']))
recovery.sort(key=lambda x: (x['收复日'], -x['统计日成交额(亿元)']))

with timed('write_intermediate_seconds'):
    pd.DataFrame(basic).to_csv(os.path.join(OUTDIR, 'basic.csv'), index=False, encoding='utf-8-sig')
    pd.DataFrame(focus).to_csv(os.path.join(OUTDIR, 'focus.csv'), index=False, encoding='utf-8-sig')
    pd.DataFrame(recovery).to_csv(os.path.join(OUTDIR, 'recovery.csv'), index=False, encoding='utf-8-sig')
    pd.DataFrame(errors).to_csv(os.path.join(OUTDIR, 'errors.csv'), index=False, encoding='utf-8-sig')

meta = {
    'run_date': TODAY_DASH,
    'run_started_bj': datetime.fromtimestamp(time.time() - (time.perf_counter() - T0), BJ).isoformat(timespec='seconds'),
    'spot_fetch_completed_bj': spot_fetch_at,
    'run_completed_bj': now_bj(),
    'akshare_version': getattr(ak, '__version__', 'unknown'),
    'universe_after_amount_st_stocks': int(len(spot)),
    'history_success_count': int(len(history)),
    'history_failure_count': int(len(errors)),
    'basic_count': int(len(basic)),
    'focus_count': int(len(focus)),
    'recovery_count': int(len(recovery)),
    'timing': timing,
    'total_seconds': round(time.perf_counter() - T0, 4),
    'workers': workers,
    'notes': [
        'Real-time snapshot uses AKShare Eastmoney SH+SZ interfaces.',
        'Historical daily data uses stock_zh_a_hist with start_date=19900101 and no adjustment.',
        'If today is missing from daily history, the current spot snapshot is appended as a temporary daily bar.',
        'This manual run uses the actual execution-time snapshot; scheduled 13:30 runs will use the 13:30 snapshot.'
    ]
}
with open(os.path.join(OUTDIR, 'meta.json'), 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)

print(json.dumps(meta, ensure_ascii=False, indent=2))
