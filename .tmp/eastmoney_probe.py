import json, time
from curl_cffi import requests

hosts = [
    'https://push2.eastmoney.com',
    'https://82.push2.eastmoney.com',
    'https://17.push2.eastmoney.com',
    'https://push2delay.eastmoney.com',
]
path='/api/qt/clist/get'
params={
    'pn':1,'pz':5,'po':1,'np':1,'fltt':2,'invt':2,'fid':'f3',
    'fs':'m:1+t:2,m:1+t:23,m:0+t:6,m:0+t:80',
    'fields':'f12,f14,f2,f3,f5,f6,f15,f16,f17,f18'
}
headers={'Referer':'https://quote.eastmoney.com/','Accept':'application/json,text/plain,*/*'}
results=[]
for host in hosts:
    for imp in ['chrome','safari','edge']:
        t=time.perf_counter()
        try:
            r=requests.get(host+path,params=params,headers=headers,timeout=12,impersonate=imp)
            results.append({'host':host,'impersonate':imp,'status':r.status_code,'seconds':round(time.perf_counter()-t,3),'len':len(r.content),'text':r.text[:160]})
        except Exception as e:
            results.append({'host':host,'impersonate':imp,'seconds':round(time.perf_counter()-t,3),'error':f'{type(e).__name__}: {e}'})
print(json.dumps(results,ensure_ascii=False,indent=2))
