import json,time
import akshare as ak
for sym in ['sh600000','sz000001']:
 t=time.perf_counter()
 try:
  df=ak.stock_zh_a_daily(symbol=sym, start_date='20200101', end_date='20260908', adjust='')
  print(json.dumps({'symbol':sym,'seconds':round(time.perf_counter()-t,3),'rows':len(df),'columns':list(df.columns),'tail':df.tail(2).reset_index().astype(str).to_dict('records')},ensure_ascii=False))
 except Exception as e:
  print(json.dumps({'symbol':sym,'seconds':round(time.perf_counter()-t,3),'error':f'{type(e).__name__}: {e}'},ensure_ascii=False))
