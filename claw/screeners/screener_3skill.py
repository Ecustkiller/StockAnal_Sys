#!/usr/bin/env python3
"""
从多周期共振+6的候选池中精选TOP标的，逐只做四Skill深度分析
筛选条件加强：PE<50 + 市值>100亿 + 5日涨2-12% + 10日涨<20% + 换手<15%
然后批量计算TDS/Mistery/元子元所需指标
"""
import requests, time, json
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0: return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

# Step 1: 重新跑多周期筛选，更严格
print("Step 1: 多周期共振精选")
dates = ["20260409","20260407","20260402","20260327","20260313"]
prices = {}
for d in dates:
    df = ts("daily", {"trade_date":d}, "ts_code,close")
    time.sleep(1)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in prices: prices[row["ts_code"]] = {}
            prices[row["ts_code"]][d] = row["close"]

time.sleep(1)
bas = ts("daily_basic", {"trade_date":"20260409"}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio")
bas_dict = {}
if not bas.empty:
    for _, row in bas.iterrows():
        bas_dict[row["ts_code"]] = row.to_dict()

time.sleep(1)
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
name_dict = {}
if not stk.empty:
    for _, row in stk.iterrows():
        name_dict[row["ts_code"]] = (row["name"], row["industry"])

# 资金流向
time.sleep(1)
mf = ts("moneyflow", {"trade_date":"20260409"}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,net_mf_amount")
mf_dict = {}
if not mf.empty:
    for _, row in mf.iterrows():
        nb = row["buy_elg_amount"]+row["buy_lg_amount"]-row["sell_elg_amount"]-row["sell_lg_amount"]
        mf_dict[row["ts_code"]] = nb

# 筛选
candidates = []
for code, p in prices.items():
    if not code[:2] in ["00","30","60","68"]: continue
    ds = sorted(p.keys(), reverse=True)
    if len(ds) < 5: continue
    c0,c1,c2,c3,c4 = [p[ds[i]] for i in range(5)]
    r5 = (c0-c2)/c2*100; r10 = (c0-c3)/c3*100; r20 = (c0-c4)/c4*100
    
    big = "上涨" if r20>5 else ("下跌" if r20<-5 else "停顿")
    mid = "上涨" if r10>3 else ("下跌" if r10<-3 else "停顿")
    small = "上涨" if r5>2 else ("下跌" if r5<-2 else "停顿")
    s = {"上涨":1,"下跌":-1,"停顿":0}
    score = s[big]*3 + s[mid]*2 + s[small]*1
    
    if score < 6: continue  # 只要满分6
    
    nm, ind = name_dict.get(code, ("?","?"))
    if "ST" in nm or "退" in nm: continue
    
    b = bas_dict.get(code, {})
    pe = b.get("pe_ttm"); mv = b.get("total_mv"); tr = b.get("turnover_rate_f")
    pb = b.get("pb")
    if not pe or pe <= 0 or pe > 50: continue  # PE更严格<50
    if not mv or mv < 1000000: continue  # 市值>100亿
    if r5 > 12: continue  # 5日涨幅<12%
    if r10 > 20: continue  # 10日涨幅<20%
    if tr and tr > 15: continue  # 换手<15%
    
    nb = mf_dict.get(code, 0)
    if nb <= 0: continue  # 主力净流入>0
    
    candidates.append({
        "code":code, "name":nm, "ind":ind, "close":c0,
        "pe":pe, "pb":pb, "mv":mv/10000, "tr":tr,
        "r5":r5, "r10":r10, "r20":r20, "nb":nb/10000,  # 转亿
        "score":score
    })

candidates.sort(key=lambda x: (-x["nb"]))  # 按主力净流入排序
print(f"严格筛选后: {len(candidates)}只")
for c in candidates[:20]:
    print(f"  {c['name']}({c['code'][:6]}) PE={c['pe']:.0f} 市值={c['mv']:.0f}亿 "
          f"5日={c['r5']:+.1f}% 10日={c['r10']:+.1f}% 净流入={c['nb']:.2f}亿 换手={c['tr']:.1f}%")

# Step 2: 对TOP12做三Skill深度分析（TDS + Mistery + 元子元）
top = candidates[:12]
print(f"\n\nStep 2: TOP {len(top)} 三Skill深度分析")
print("="*120)

for item in top:
    code = item["code"]
    name = item["name"]
    
    # 获取K线
    kdf = ts("daily", {"ts_code":code, "start_date":"20260201", "end_date":"20260409"},
             "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")
    time.sleep(0.8)
    if kdf.empty or len(kdf)<20: continue
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    n = len(kdf)
    c = kdf["close"].values; o = kdf["open"].values
    h = kdf["high"].values; l = kdf["low"].values; v = kdf["vol"].values
    
    print(f"\n{'='*100}")
    print(f"  {name}({code}) 收盘{c[-1]:.2f} PE={item['pe']:.0f} 市值={item['mv']:.0f}亿")
    print(f"{'='*100}")
    
    # === TDS分析 ===
    # 波峰波谷
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i] > max(h[max(0,i-3):i]) and h[i] > max(h[i+1:min(n,i+4)]):
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        if l[i] < min(l[max(0,i-3):i]) and l[i] < min(l[i+1:min(n,i+4)]):
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    
    # 趋势
    trend = "未知"
    if len(peaks)>=2 and len(troughs)>=2:
        pk_up = peaks[-1][1] > peaks[-2][1]
        tr_up = troughs[-1][1] > troughs[-2][1]
        if pk_up and tr_up: trend = "上行"
        elif not pk_up and not tr_up: trend = "下行"
        else: trend = "转折"
    
    # K线组合(最近2日)
    combos = []
    for j in range(n-2, n-1):
        k1h,k1l,k1o,k1c = h[j],l[j],o[j],c[j]
        k2h,k2l,k2o,k2c = h[j+1],l[j+1],o[j+1],c[j+1]
        if k2l>k1l and k2h>k1h: combos.append("多头推进")
        if k2h>k1h and k2l<k1l: combos.append("扩张")
        if k2c>k2o and k2c>k1h and k1c<k1o: combos.append("阳线吞没")
    
    # TDS模型
    models = []
    if n>=3 and h[-1]>h[-2] and l[-1]>l[-2] and h[-2]>h[-3] and l[-2]>l[-3]:
        models.append("T1连续推进")
    elif n>=2 and h[-1]>h[-2] and l[-1]>l[-2]:
        models.append("T1推进")
    if n>=2 and c[-1]>o[-1] and c[-1]>h[-2] and c[-2]<o[-2]:
        models.append("T2吞没")
    if peaks and c[-1]>peaks[-1][1]:
        models.append(f"T3突破({peaks[-1][1]:.2f})")
    if n>=3 and trend=="下行":
        body2=abs(c[-2]-o[-2]);body1=abs(c[-3]-o[-3]);body3=abs(c[-1]-o[-1])
        if body2<=max(body1,body3) and c[-1]>o[-1] and c[-1]>h[-2]:
            models.append("T4三K反转")
    if n>=3 and h[-1]>h[-2] and c[-2]<c[-3]:
        models.append("T5回调突刺")
    
    print(f"  [TDS] 趋势:{trend} | 波峰:{peaks[-2:]} | 波谷:{troughs[-2:]}")
    print(f"  [TDS] K线:{combos} | 模型:{models}")
    
    # === Mistery分析 ===
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1]
    ma60 = pd.Series(c).rolling(60).mean().iloc[-1] if n>=60 else None
    
    ema12 = pd.Series(c).ewm(span=12,adjust=False).mean()
    ema26 = pd.Series(c).ewm(span=26,adjust=False).mean()
    dif = (ema12-ema26).iloc[-1]; dea = (ema12-ema26).ewm(span=9,adjust=False).mean().iloc[-1]
    
    delta = pd.Series(c).diff()
    rsi = (100-100/(1+delta.clip(lower=0).rolling(14).mean()/(-delta.clip(upper=0)).rolling(14).mean())).iloc[-1]
    
    bias20 = (c[-1]-ma20)/ma20*100
    
    # 520战法
    ma5s = pd.Series(c).rolling(5).mean(); ma20s = pd.Series(c).rolling(20).mean()
    cross520 = "金叉" if (not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5]<=ma20s.iloc[-5] and ma5>ma20) else ("多头" if ma5>ma20 else "空头")
    
    # 量能
    vol5 = np.mean(v[-5:]); vol20 = np.mean(v[-20:])
    vol_r = vol5/vol20 if vol20>0 else 1
    vol_s = "放量" if vol_r>1.5 else ("温和" if vol_r>1 else "缩量")
    
    # 形态
    forms = []
    if n>=10:
        # W底检测
        recent_lows = [(i,l[i]) for i in range(max(0,n-30),n) if l[i]==min(l[max(0,i-3):min(n,i+4)])]
        if len(recent_lows)>=2 and abs(recent_lows[-1][1]-recent_lows[-2][1])/recent_lows[-2][1]<0.05:
            forms.append("W底")
    if kdf["pct_chg"].iloc[-3:][:].gt(0).all():
        forms.append("红三兵")
    # 破五反五
    below5 = any(c[i]<ma5s.iloc[i] for i in range(max(0,n-7),n-2) if not np.isnan(ma5s.iloc[i]))
    if below5 and c[-1]>ma5:
        forms.append("破五反五")
    
    # Mistery评级
    m_trend = 5 if (c[-1]>ma5>ma10>ma20 and (ma60 is None or ma20>ma60)) else (4 if c[-1]>ma5>ma10>ma20 else (3 if c[-1]>ma20 else 2))
    m_buy = 4 if ("W底" in forms or "破五反五" in forms) else (3 if cross520 in ["金叉","多头"] else 2)
    m_vol = 4 if (vol_s in ["放量","温和"] and item["nb"]>0) else (3 if vol_s=="温和" else 2)
    
    print(f"  [Mistery] MA5={ma5:.2f} MA10={ma10:.2f} MA20={ma20:.2f} MA60={f'{ma60:.2f}' if ma60 else 'N/A'}")
    print(f"  [Mistery] 520={cross520} BIAS20={bias20:.1f}% RSI={rsi:.1f} DIF={dif:.3f}")
    print(f"  [Mistery] 量能={vol_s}({vol_r:.2f}) 形态={forms}")
    print(f"  [Mistery] 趋势⭐{m_trend} 买点⭐{m_buy} 量价⭐{m_vol}")
    
    # === 元子元情绪 ===
    pct_last = kdf["pct_chg"].iloc[-1]
    if pct_last >= 9.5: emo = "主升加速(涨停)"
    elif item["r5"] > 10: emo = "主升加速"
    elif item["r5"] > 5: emo = "发酵确认"
    elif item["r5"] > 0: emo = "冰点启动/发酵"
    else: emo = "调整中"
    
    # 龙头气质（简化）
    anti_drop = "强" if pct_last > 0 else "弱"  # 4/9大盘跌78%还涨=抗跌
    
    print(f"  [元子元] 情绪阶段:{emo} | 抗跌性:{anti_drop} | 大单净流入:{item['nb']:.2f}亿")
    
    # === 综合 ===
    # 安全边际 = BIAS20越低越好
    safety = "🟢优" if bias20<=8 else ("🟡中" if bias20<=15 else "🔴差")
    total_stars = m_trend + m_buy + m_vol + len(models)*1
    
    print(f"  [综合] 多周期:{item['score']:+d} TDS模型:{len(models)}个 "
          f"Mistery总星:{m_trend+m_buy+m_vol}/15 安全边际:{safety}(BIAS{bias20:.0f}%)")
    
    # 关键价位
    supports = sorted(set([round(ma20,2), round(ma10,2)] + [round(t[1],2) for t in troughs[-2:]]))
    supports = [s for s in supports if s < c[-1]][:3]
    resists = sorted(set([round(p[1],2) for p in peaks[-2:]] + [round(max(h[-10:]),2)]))
    resists = [r for r in resists if r > c[-1]][:3]
    
    print(f"  [操作] 支撑:{supports} | 压力:{resists}")
    entry_low = f"{supports[0]:.2f}" if supports else "N/A"
    entry_high = f"{c[-1]:.2f}"
    stop = f"{supports[0]*0.97:.2f}" if supports else "N/A"
    target = f"{resists[0]:.2f}" if resists else f"{c[-1]*1.1:.2f}"
    print(f"  [操作] 进场:{entry_low}-{entry_high} | 止损:{stop} | 目标:{target}")

print("\n\n三Skill深度分析完成！（TDS + Mistery + 元子元）")
