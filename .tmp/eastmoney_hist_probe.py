import json,time
from curl_cffi import requests
url='https://push2his.eastmoney.com/api/qt/stock/kline/get'
params={
 'secid':'1.600000','fields1':'f1,f2,f3,f4,f5,f6','fields2':'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
 'klt':'101','fqt':'0','beg':'0','end':'20500101','lmt':'1000000'
}
headers={'Referer':'https://quote.eastmoney.com/','Accept':'application/json,text/plain,*/*'}
t=time.perf_counter()
try:
 r=requests.get(url,params=params,headers=headers,timeout=15,impersonate='chrome')
 print(json.dumps({'status':r.status_code,'seconds':round(time.perf_counter()-t,3),'len':len(r.content),'text':r.text[:500]},ensure_ascii=False,indent=2))
except Exception as e:
 print(json.dumps({'seconds':round(time.perf_counter()-t,3),'error':f'{type(e).__name__}: {e}'},ensure_ascii=False,indent=2))
