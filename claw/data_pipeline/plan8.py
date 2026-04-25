#!/usr/bin/env python3
"""8只候选股完整技术数据采集"""
import requests, json, time
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts(api, params=None, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params or {}}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0: return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

stocks = [
    ("002536.SZ", "飞龙股份"),
    ("000977.SZ", "浪潮信息"),
    ("002475.SZ", "立讯精密"),
    ("002179.SZ", "中航光电"),
    ("603112.SH", "华翔股份"),
    ("600547.SH", "招金黄金"),  # 山东黄金是600547? 招金矿业? 让我确认
    ("601677.SH", "明泰铝业"),
    ("603588.SH", "高能环境"),
]

# 先确认招金黄金代码
search = ts("stock_basic", {"name":"招金","list_status":"L"}, "ts_code,name,industry")
print("招金搜索:", search.to_string() if not search.empty else "空")
search2 = ts("stock_basic", {"name":"华翔","list_status":"L"}, "ts_code,name,industry")
print("华翔搜索:", search2.to_string() if not search2.empty else "空")
time.sleep(1)

for code, name in stocks:
    print(f"\n{'='*100}")
    print(f"  {name} ({code})")
    print(f"{'='*100}")
    
    # K线
    kdf = ts("daily", {"ts_code":code, "start_date":"20260101", "end_date":"20260409"},
             "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
    time.sleep(0.8)
    if kdf.empty:
        print("  无数据!")
        continue
    
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    n = len(kdf)
    c = kdf["close"].values
    h = kdf["high"].values
    l = kdf["low"].values
    v = kdf["vol"].values
    
    # 均线
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1]
    ma60 = pd.Series(c).rolling(60).mean().iloc[-1] if n>=60 else None
    
    # MACD
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean()
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean()
    dif = (ema12 - ema26).iloc[-1]
    dea = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]
    
    # RSI
    delta = pd.Series(c).diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = (100 - 100/(1+gain/loss)).iloc[-1]
    
    # 波峰波谷
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i] > max(h[max(0,i-3):i]) and h[i] > max(h[i+1:min(n,i+4)]):
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        if l[i] < min(l[max(0,i-3):i]) and l[i] < min(l[i+1:min(n,i+4)]):
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    
    # 近期关键价位
    last_close = c[-1]
    high_20d = max(h[-20:])
    low_20d = min(l[-20:])
    high_10d = max(h[-10:])
    low_10d = min(l[-10:])
    
    # 涨跌幅
    r5 = (c[-1]/c[-6]-1)*100 if n>=6 else 0
    r10 = (c[-1]/c[-11]-1)*100 if n>=11 else 0
    r20 = (c[-1]/c[-21]-1)*100 if n>=21 else 0
    
    bias20 = (last_close - ma20)/ma20*100
    
    # 基本面
    time.sleep(0.5)
    bas = ts("daily_basic", {"ts_code":code, "trade_date":"20260409"},
             "ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate_f,volume_ratio")
    pe = bas["pe_ttm"].iloc[0] if not bas.empty else None
    pb = bas["pb"].iloc[0] if not bas.empty else None
    mv = bas["total_mv"].iloc[0]/10000 if not bas.empty else None
    tr = bas["turnover_rate_f"].iloc[0] if not bas.empty else None
    
    # 资金流向
    time.sleep(0.5)
    mf = ts("moneyflow", {"ts_code":code, "trade_date":"20260409"},
            "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,net_mf_amount")
    if not mf.empty:
        nb = (mf["buy_elg_amount"].iloc[0]+mf["buy_lg_amount"].iloc[0]
              -mf["sell_elg_amount"].iloc[0]-mf["sell_lg_amount"].iloc[0])
    else:
        nb = None
    
    print(f"  收盘: {last_close:.2f}  今涨: {kdf['pct_chg'].iloc[-1]:.2f}%")
    print(f"  5日涨: {r5:.1f}%  10日涨: {r10:.1f}%  20日涨: {r20:.1f}%")
    print(f"  MA5={ma5:.2f}  MA10={ma10:.2f}  MA20={ma20:.2f}  MA60={f'{ma60:.2f}' if ma60 else 'N/A'}")
    print(f"  BIAS20={bias20:.1f}%  DIF={dif:.3f}  DEA={dea:.3f}  RSI={rsi:.1f}")
    print(f"  PE={pe}  PB={pb}  市值={f'{mv:.0f}亿' if mv else 'N/A'}  换手={tr}%")
    print(f"  大单净流入: {f'{nb:.0f}万' if nb else 'N/A'}")
    print(f"  20日高/低: {high_20d:.2f}/{low_20d:.2f}  10日高/低: {high_10d:.2f}/{low_10d:.2f}")
    
    # 近10日K线
    print(f"\n  近10日K线:")
    for _, row in kdf.tail(10).iterrows():
        print(f"    {row['trade_date']}: O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} "
              f"C={row['close']:.2f} {row['pct_chg']:+.2f}% V={row['vol']:.0f}")
    
    # 波峰波谷
    print(f"  波峰: {peaks[-3:] if len(peaks)>=3 else peaks}")
    print(f"  波谷: {troughs[-3:] if len(troughs)>=3 else troughs}")
    
    # 关键支撑/压力计算
    supports = sorted(set([round(ma20,2), round(ma10,2)] + 
                         [round(t[1],2) for t in troughs[-3:]] +
                         [round(low_10d,2)]))
    resistances = sorted(set([round(p[1],2) for p in peaks[-3:]] + 
                            [round(high_10d,2), round(high_20d,2)]))
    supports = [s for s in supports if s < last_close]
    resistances = [r for r in resistances if r > last_close]
    print(f"  支撑位: {supports[:4]}")
    print(f"  压力位: {resistances[:4]}")
