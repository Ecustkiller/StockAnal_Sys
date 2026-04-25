#!/usr/bin/env python3
"""WR-2/WR-3补充扫描 — 用AKShare全市场涨幅榜替代Tushare"""
import akshare as ak
import sys; sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price
from MyTT import *
import numpy as np, time, json, warnings
warnings.filterwarnings('ignore')

# Step1: 用AKShare获取全市场涨幅排名（今日涨≥5%）
print("获取全市场涨幅排名...")
try:
    df_rank = ak.stock_zh_a_spot_em()
    df_rank = df_rank[df_rank['涨跌幅'] >= 5]
    df_rank = df_rank.sort_values('涨跌幅', ascending=False)
    print(f"今日涨≥5%: {len(df_rank)}只")
except Exception as e:
    print(f"AKShare获取失败: {e}")
    df_rank = None

if df_rank is None or len(df_rank) == 0:
    print("无数据，退出")
    exit()

# Step2: 对每只用Ashare获取日线做WR-2检测
print(f"\nWR-2扫描({len(df_rank)}只)...")
wr2_results = []
for i, (_, row) in enumerate(df_rank.iterrows()):
    code_raw = str(row['代码']).zfill(6)
    name = row.get('名称', '')
    pct = row.get('涨跌幅', 0)
    mv = row.get('流通市值', 0)
    close = row.get('最新价', 0)
    
    if 'ST' in name or '*ST' in name: continue
    
    # Ashare代码
    ashare_code = ('sh' if code_raw.startswith('6') else 'sz') + code_raw
    
    try:
        df = get_price(ashare_code, frequency='1d', count=60)
        if df is None or len(df) < 20: continue
        
        c = df.close.values; v = df.volume.values
        h = df.high.values; l = df.low.values
        
        ma5 = np.mean(c[-5:]); ma20 = np.mean(c[-20:])
        ma60 = np.mean(c[-60:]) if len(c)>=60 else np.mean(c[-30:])
        av5 = np.mean(v[-6:-1]) if len(v)>=6 else np.mean(v[:-1])
        vr = v[-1]/av5 if av5>0 else 0
        
        # 近30天波动（不含今天）
        if len(c) >= 31:
            r30 = (max(h[-31:-1])-min(l[-31:-1]))/max(h[-31:-1]) if max(h[-31:-1])>0 else 1
        else: r30 = 1
        
        bbw = np.std(c[-21:-1])/ma20*2 if len(c)>=20 and ma20>0 else 1
        m60_up = np.mean(c[-60:])>=np.mean(c[-70:-60])*0.99 if len(c)>=70 else True
        
        checks = {
            '均线多头': ma5>ma20>ma60,
            'M60向上': m60_up,
            '整理充分': r30<0.20 or bbw<0.15,
            '放量≥2.5x': vr>=2.5,
            '涨≥5%': pct>=5,
        }
        score = sum(checks.values())
        
        if score >= 4:
            wr2_results.append({
                'code': ashare_code, 'name': name, 'close': round(close,2),
                'pct': round(pct,1), 'vol_ratio': round(vr,1), 'bbw': round(bbw,3),
                'mv': round(mv/1e8,0) if mv else 0, 'is_zt': pct>=9.5,
                'score': score, 'checks': {k:v for k,v in checks.items()},
            })
    except: pass
    
    if (i+1)%30==0: print(f"  WR-2: {i+1}/{len(df_rank)}... 找到{len(wr2_results)}只")
    time.sleep(0.15)

wr2_results.sort(key=lambda x:(x['score'],x['vol_ratio']),reverse=True)

# Step3: WR-3用Ashare 60分钟
print(f"\nWR-3扫描(涨幅0-9%活跃股)...")
df_active = ak.stock_zh_a_spot_em()
df_active = df_active[(df_active['涨跌幅']>=-2) & (df_active['涨跌幅']<=9.5)]
df_active = df_active[df_active['成交量']>50000]
df_active = df_active.sort_values('涨跌幅', ascending=False).head(400)
print(f"WR-3候选: {len(df_active)}只")

wr3c, wr3p = [], []
for i, (_, row) in enumerate(df_active.iterrows()):
    code_raw = str(row['代码']).zfill(6)
    name = row.get('名称','')
    if 'ST' in name: continue
    ashare_code = ('sh' if code_raw.startswith('6') else 'sz') + code_raw
    
    try:
        df = get_price(ashare_code, frequency='60m', count=30)
        if df is None or len(df)<12: continue
        vo=df.volume.values.astype(float); cl=df.close.values.astype(float)
        op=df.open.values.astype(float); hi=df.high.values.astype(float); lo=df.low.values.astype(float)
        
        for j in range(len(vo)-2,3,-1):
            if vo[j-1]<=0 or vo[j]<vo[j-1]*2: continue
            if cl[j]<=op[j]: continue
            rh=max(hi[max(0,j-8):j]); rl=min(lo[max(0,j-8):j])
            if cl[j]>(rh+rl)/2*1.08: continue
            sup=lo[j]; vr2=round(vo[j]/vo[j-1],1)
            conf=False
            for k in range(j+1,min(j+6,len(vo))):
                if vo[k]>=vo[k-1]*1.8 and cl[k]>hi[j] and lo[k]>=sup*0.99: conf=True; break
            mv = row.get('流通市值',0)
            mv_yi = round(mv/1e8,0) if mv else 0
            e = {'code':ashare_code,'name':name,'pct':round(row.get('涨跌幅',0),1),
                 'close':round(cl[-1],2),'support':round(sup,2),'vol_ratio':vr2,'mv':mv_yi}
            if 20<=mv_yi<=500:
                if conf: wr3c.append(e)
                else: wr3p.append(e)
            break
    except: pass
    if (i+1)%80==0: print(f"  WR-3: {i+1}/{len(df_active)}... c{len(wr3c)} p{len(wr3p)}")
    time.sleep(0.1)

wr3c.sort(key=lambda x:x['vol_ratio'],reverse=True)
wr3p.sort(key=lambda x:x['vol_ratio'],reverse=True)

# 输出
print(f"\n{'='*60}")
print(f"WR-2: 5/5={sum(1 for x in wr2_results if x['score']==5)}只 4/5={sum(1 for x in wr2_results if x['score']==4)}只")
print(f"  5/5非涨停:")
for x in [a for a in wr2_results if a['score']==5 and not a['is_zt']]:
    fails = [k for k,v in x['checks'].items() if not v]
    print(f"    {x['name']}({x['code']}) +{x['pct']}% 量{x['vol_ratio']}x BBW{x['bbw']} 市值{x['mv']}亿")
print(f"  5/5涨停:")
for x in [a for a in wr2_results if a['score']==5 and a['is_zt']][:10]:
    print(f"    {x['name']}({x['code']}) +{x['pct']}% 量{x['vol_ratio']}x BBW{x['bbw']} 市值{x['mv']}亿")
print(f"  4/5非涨停TOP10:")
for x in [a for a in wr2_results if a['score']==4 and not a['is_zt']][:10]:
    fails = [k for k,v in x['checks'].items() if not v]
    print(f"    {x['name']}({x['code']}) +{x['pct']}% 量{x['vol_ratio']}x 市值{x['mv']}亿 缺:{','.join(fails)}")

print(f"\nWR-3确认{len(wr3c)}只 待确认{len(wr3p)}只")
print(f"  确认TOP15:")
for x in wr3c[:15]:
    print(f"    {x['name']}({x['code']}) +{x['pct']}% 60m量{x['vol_ratio']}x 支撑{x['support']} 市值{x['mv']}亿")

# 保存
with open('wr23_scan_0413.json','w',encoding='utf-8') as f:
    json.dump({'wr2':wr2_results[:30],'wr3c':wr3c[:30],'wr3p':wr3p[:20]},f,ensure_ascii=False,indent=2)
print("\n结果已保存")
