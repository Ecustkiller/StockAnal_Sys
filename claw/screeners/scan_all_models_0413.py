#!/usr/bin/env python3
"""全模型扫描
使用统一的 market_sentiment 模块获取市场情绪数据，确保数据口径一致。
"""
import akshare as ak
import sys; sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price
from market_sentiment import get_market_sentiment, print_sentiment_summary, match_industry
import requests, json, time, numpy as np, warnings
warnings.filterwarnings('ignore')

# ===== 配置 =====
SCAN_DATE = '20260413'  # 扫描日期，修改此处即可切换日期

token = 'ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59'
def ts(api, fields='', **params):
    d = {'api_name': api, 'token': token, 'params': params, 'fields': fields}
    try:
        r = requests.post('http://api.tushare.pro', json=d, timeout=15)
        j = r.json()
        if j.get('data'): return j['data']
    except: pass
    return {'items':[], 'fields':[]}

# ===== 基础：使用统一情绪模块 =====
print("Step 1: 基础数据（统一情绪模块）")
sentiment = get_market_sentiment(SCAN_DATE)
print_sentiment_summary(sentiment)

# 从统一模块获取数据
zt = sentiment['zt_df']          # 涨停池DataFrame
zb = sentiment['zb_df']          # 炸板池DataFrame
dt_pool = sentiment['dt_df']     # 跌停池DataFrame
zt_codes = sentiment['zt_codes'] # 涨停股代码集合
ind_zt_cnt = sentiment['ind_zt_dict']  # 行业涨停统计
up_cnt = sentiment['up_cnt']
down_cnt = sentiment['down_cnt']

# 昨日涨停股表现
try:
    prev = ak.stock_zt_pool_previous_em(date=SCAN_DATE)
except:
    import pandas as pd
    prev = pd.DataFrame(columns=['涨跌幅'])

daily_all = ts('daily','ts_code,pct_chg,vol',trade_date=SCAN_DATE).get('items',[])
print(f"涨停{sentiment['zt_cnt']}(非ST) 炸板{sentiment['zb_cnt']} 封板率{sentiment['fbl']:.0f}% "
      f"跌停{sentiment['dt_cnt']}(非ST) 涨{up_cnt}跌{down_cnt} "
      f"全口径涨停{sentiment['zt_cnt_all']}(含ST{sentiment['st_zt_cnt']})")

# ===== WR-1 =====
print("\nStep 2: WR-1")
wr1 = []
for _, r in zt.iterrows():
    if r['连板数'] != 1: continue
    ck = {'首板':True, '换手≥8%':r['换手率']>=8, '市值30-150亿':3e9<=r['流通市值']<=1.5e11,
          '封板10:30前':str(r['首次封板时间']).zfill(6)<='103000',
          '炸板≤1次':r['炸板次数']<=1, '封单≥0.5亿':r['封板资金']>=5e7}
    p = sum(ck.values())
    if p >= 4:
        wr1.append({'name':r['名称'],'code':r['代码'],'score':p,'hs':round(r['换手率'],1),
                    'fund':round(r['封板资金']/1e8,1),'mv':round(r['流通市值']/1e8,0),
                    'ft':str(r['首次封板时间']).zfill(6)[:4],'zbc':r['炸板次数'],'ind':r['所属行业']})
wr1.sort(key=lambda x: x['score'], reverse=True)
print(f"6/6={sum(1 for x in wr1 if x['score']==6)} 5/6={sum(1 for x in wr1 if x['score']==5)} 4/6={sum(1 for x in wr1 if x['score']==4)}")

# ===== WR-2 =====
print("\nStep 3: WR-2")
hot = ts('daily','ts_code,trade_date,open,high,low,close,vol,pct_chg',trade_date='20260413').get('items',[])
hot5 = [r for r in hot if r[7] and r[7]>=5]
print(f"涨≥5%: {len(hot5)}只")

# 名称映射
name_map = {}
for r in ts('stock_basic','ts_code,name,industry',list_status='L').get('items',[]):
    name_map[r[0]] = {'name':r[1],'ind':r[2]}
mv_map = {}
for r in ts('daily_basic','ts_code,circ_mv',trade_date='20260413').get('items',[]):
    if r[1]: mv_map[r[0]] = r[1]/10000

wr2 = []
for i, stk in enumerate(hot5):
    tc = stk[0]
    kd = ts('daily','ts_code,trade_date,open,high,low,close,vol,pct_chg',ts_code=tc,start_date='20260201',end_date='20260413').get('items',[])
    if len(kd)<20: continue
    kd.sort(key=lambda x:x[1])
    c=[r[5] for r in kd]; v=[r[6] for r in kd]; h=[r[3] for r in kd]; l=[r[4] for r in kd]
    ma5=np.mean(c[-5:]); ma20=np.mean(c[-20:]); ma60=np.mean(c[-60:]) if len(c)>=60 else np.mean(c[-30:])
    av5=np.mean(v[-6:-1]) if len(v)>=6 else np.mean(v[:-1])
    vr=v[-1]/av5 if av5>0 else 0
    r30=((max(h[-31:-1])-min(l[-31:-1]))/max(h[-31:-1])) if len(c)>=30 else 1
    bbw=np.std(c[-21:-1])/ma20*2 if len(c)>=20 else 1
    m60_up = np.mean(c[-60:])>=np.mean(c[-70:-60])*0.99 if len(c)>=70 else True
    
    w = {'均线多头':ma5>ma20>ma60, 'M60向上':m60_up, '整理充分':r30<0.20 or bbw<0.15,
         '放量≥2.5x':vr>=2.5, '涨≥5%':stk[7]>=5}
    p = sum(w.values())
    if p >= 4:
        nm = name_map.get(tc,{})
        wr2.append({'ts_code':tc,'name':nm.get('name',''),'ind':nm.get('ind',''),
                    'close':c[-1],'pct':round(stk[7],1),'vol_ratio':round(vr,1),
                    'bbw':round(bbw,3),'mv':round(mv_map.get(tc,0),0),
                    'is_zt':stk[7]>=9.5,'score':p})
    if (i+1)%30==0: print(f"  {i+1}/{len(hot5)}...")
    time.sleep(0.35)

wr2.sort(key=lambda x:(x['score'],x['vol_ratio']),reverse=True)
print(f"5/5={sum(1 for x in wr2 if x['score']==5)} 4/5={sum(1 for x in wr2 if x['score']==4)}")

# ===== WR-3 =====
print("\nStep 4: WR-3 (Ashare 60min)")
active = []
for r in hot:
    if r[7] is None: continue
    if -2<=r[7]<=9.5 and r[6] and r[6]>50000:
        parts=r[0].split('.'); pfx='sh' if parts[1]=='SH' else 'sz'
        active.append({'ts_code':r[0],'code':pfx+parts[0],'pct':r[7],'close':r[5]})
active.sort(key=lambda x:abs(x['pct']),reverse=True)
seen=set(); uniq=[]
for a in active:
    if a['code'] not in seen: seen.add(a['code']); uniq.append(a)
active = uniq[:400]
print(f"候选{len(active)}只")

wr3c, wr3p = [], []
ck3 = 0
for stk in active:
    try:
        df = get_price(stk['code'], frequency='60m', count=30)
        if df is None or len(df)<12: ck3+=1; continue
        vo=df.volume.values.astype(float); cl=df.close.values.astype(float)
        op=df.open.values.astype(float); hi=df.high.values.astype(float); lo=df.low.values.astype(float)
        for i in range(len(vo)-2,3,-1):
            if vo[i-1]<=0 or vo[i]<vo[i-1]*2: continue
            if cl[i]<=op[i]: continue
            rh=max(hi[max(0,i-8):i]); rl=min(lo[max(0,i-8):i])
            if cl[i]>(rh+rl)/2*1.08: continue
            sup=lo[i]; vr2=round(vo[i]/vo[i-1],1)
            conf=False
            for j in range(i+1,min(i+6,len(vo))):
                if vo[j]>=vo[j-1]*1.8 and cl[j]>hi[i] and lo[j]>=sup*0.99: conf=True; break
            e = {'code':stk['code'],'ts_code':stk['ts_code'],'pct':round(stk['pct'],1),
                 'close':round(cl[-1],2),'support':round(sup,2),'vol_ratio':vr2}
            if conf: wr3c.append(e)
            else: wr3p.append(e)
            break
    except: pass
    ck3+=1
    if ck3%80==0: print(f"  {ck3}/{len(active)}... c{len(wr3c)} p{len(wr3p)}")
    time.sleep(0.1)

# 附加信息
for lst in [wr3c,wr3p]:
    for item in lst:
        nm=name_map.get(item['ts_code'],{}); item['name']=nm.get('name',''); item['ind']=nm.get('ind','')
        item['mv']=round(mv_map.get(item['ts_code'],0),0)
wr3c=[x for x in wr3c if 'ST' not in x.get('name','') and 20<=x.get('mv',0)<=500]
wr3p=[x for x in wr3p if 'ST' not in x.get('name','') and 20<=x.get('mv',0)<=500]
wr3c.sort(key=lambda x:x['vol_ratio'],reverse=True)
wr3p.sort(key=lambda x:x['vol_ratio'],reverse=True)

print(f"WR-3: 确认{len(wr3c)}只 待确认{len(wr3p)}只")

# ===== 保存 =====
res = {'wr1':wr1,'wr2':wr2[:30],'wr3c':wr3c[:30],'wr3p':wr3p[:20],
       'zt_high':[{'name':r['名称'],'code':r['代码'],'zb':r['连板数'],'hs':round(r['换手率'],1),
                   'fund':round(r['封板资金']/1e8,1),'ft':str(r['首次封板时间']).zfill(6)[:4],
                   'zbc':r['炸板次数'],'ind':r['所属行业']}
                  for _,r in zt[zt['连板数']>=2].sort_values('连板数',ascending=False).iterrows()],
       'industry':dict(zt['所属行业'].value_counts().head(15)),
       'market':{'zt':sentiment['zt_cnt'],'zb':sentiment['zb_cnt'],'dt':sentiment['dt_cnt'],
                 'fbl':sentiment['fbl'],'earn_rate':sentiment['earn_rate'],
                 'st_zt':sentiment['st_zt_cnt'],'st_dt':sentiment['st_dt_cnt'],
                 'zt_all':sentiment['zt_cnt_all'],'dt_all':sentiment['dt_cnt_all'],
                 'up':up_cnt,'down':down_cnt,
                 'prev_avg':round(prev['涨跌幅'].mean(),1) if not prev.empty else 0,
                 'total_amount':sentiment['total_amount']}}
with open('all_models_20260413.json','w',encoding='utf-8') as f:
    json.dump(res,f,ensure_ascii=False,indent=2,default=str)

print(f"\n{'='*60}")
print("全模型扫描完成!")
print(f"\nWR-1: 6/6={sum(1 for x in wr1 if x['score']==6)}只")
for x in [a for a in wr1 if a['score']==6]:
    print(f"  ⭐{x['name']}({x['code']}) 换{x['hs']}% 封{x['fund']}亿 {x['ind']}")
print(f"WR-2 5/5非涨停:")
for x in [a for a in wr2 if a['score']==5 and not a['is_zt']]:
    print(f"  ⭐{x['name']}({x['ts_code']}) +{x['pct']}% 量{x['vol_ratio']}x {x['ind']}")
print(f"WR-3确认TOP10:")
for x in wr3c[:10]:
    print(f"  🎯{x['name']}({x['code']}) +{x['pct']}% 60m量{x['vol_ratio']}x 支撑{x['support']} {x['ind']}")
