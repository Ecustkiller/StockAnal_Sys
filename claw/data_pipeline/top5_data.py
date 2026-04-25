#!/usr/bin/env python3
"""TOP5个股详细K线数据获取，用于三Skill深度分析"""
import requests, time
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

stocks = [
    ("002938.SZ", "鹏鼎控股"),
    ("002475.SZ", "立讯精密"),
    ("688019.SH", "安集科技"),
    ("301571.SZ", "国科天成"),
    ("688120.SH", "华海清科"),
]

for code, name in stocks:
    print(f"\n{'='*100}")
    print(f"  {name}({code})")
    print(f"{'='*100}")
    
    kdf = ts("daily", {"ts_code":code, "start_date":"20260101", "end_date":"20260409"},
             "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
    time.sleep(0.8)
    if kdf.empty: continue
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    n = len(kdf)
    c=kdf["close"].values; o=kdf["open"].values; h=kdf["high"].values; l=kdf["low"].values; v=kdf["vol"].values
    
    # 近15日K线
    print("  近15日K线:")
    for _, row in kdf.tail(15).iterrows():
        bar = "阳" if row["close"]>row["open"] else "阴"
        print(f"    {row['trade_date']}: O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} "
              f"C={row['close']:.2f} {row['pct_chg']:+.2f}% V={row['vol']:.0f} [{bar}]")
    
    # 均线
    ma5=pd.Series(c).rolling(5).mean(); ma10=pd.Series(c).rolling(10).mean()
    ma20=pd.Series(c).rolling(20).mean(); ma60=pd.Series(c).rolling(60).mean() if n>=60 else pd.Series(np.nan,index=range(n))
    ema12=pd.Series(c).ewm(span=12,adjust=False).mean(); ema26=pd.Series(c).ewm(span=26,adjust=False).mean()
    dif_s=ema12-ema26; dea_s=dif_s.ewm(span=9,adjust=False).mean(); macd_s=2*(dif_s-dea_s)
    delta=pd.Series(c).diff(); rsi=(100-100/(1+delta.clip(lower=0).rolling(14).mean()/(-delta.clip(upper=0)).rolling(14).mean())).iloc[-1]
    
    print(f"\n  均线: MA5={ma5.iloc[-1]:.2f} MA10={ma10.iloc[-1]:.2f} MA20={ma20.iloc[-1]:.2f} "
          f"MA60={'%.2f'%ma60.iloc[-1] if not np.isnan(ma60.iloc[-1]) else 'N/A'}")
    print(f"  MACD: DIF={dif_s.iloc[-1]:.3f} DEA={dea_s.iloc[-1]:.3f} BAR={macd_s.iloc[-1]:.3f} "
          f"{'多头' if dif_s.iloc[-1]>dea_s.iloc[-1] else '空头'}")
    print(f"  RSI14={rsi:.1f} BIAS20={(c[-1]-ma20.iloc[-1])/ma20.iloc[-1]*100:.1f}%")
    
    # 波峰波谷
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i]>max(h[max(0,i-3):i]) and h[i]>max(h[i+1:min(n,i+4)]):
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        if l[i]<min(l[max(0,i-3):i]) and l[i]<min(l[i+1:min(n,i+4)]):
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    print(f"\n  波峰: {peaks[-4:]}")
    print(f"  波谷: {troughs[-4:]}")
    
    # TDS趋势
    trend = "未知"
    if len(peaks)>=2 and len(troughs)>=2:
        if peaks[-1][1]>peaks[-2][1] and troughs[-1][1]>troughs[-2][1]: trend="上行"
        elif not(peaks[-1][1]>peaks[-2][1]) and not(troughs[-1][1]>troughs[-2][1]): trend="下行"
        else: trend="转折"
    print(f"  TDS趋势: {trend}")
    
    # K线组合(近3日两两)
    for j in range(n-3, n-1):
        combos = []
        if h[j+1]>h[j] and l[j+1]>l[j]: combos.append("多头推进")
        if h[j+1]>h[j] and l[j+1]<l[j]: combos.append("扩张")
        if h[j+1]<h[j] and l[j+1]>l[j]: combos.append("收缩")
        if c[j+1]>o[j+1] and c[j+1]>h[j] and c[j]<o[j]: combos.append("阳线吞没")
        d1=kdf["trade_date"].iloc[j]; d2=kdf["trade_date"].iloc[j+1]
        print(f"  K线{d1}→{d2}: {combos if combos else ['无特殊']}")
    
    # TDS模型
    models = []
    if n>=3 and h[-1]>h[-2] and l[-1]>l[-2] and h[-2]>h[-3] and l[-2]>l[-3]: models.append("T1连续推进")
    elif n>=2 and h[-1]>h[-2] and l[-1]>l[-2]: models.append("T1推进")
    if n>=2 and c[-1]>o[-1] and c[-1]>h[-2] and c[-2]<o[-2]: models.append("T2阳线吞没")
    if peaks and c[-1]>peaks[-1][1]: models.append(f"T3突破({peaks[-1][1]:.2f})")
    if n>=3 and trend=="下行":
        body2=abs(c[-2]-o[-2]); body1=abs(c[-3]-o[-3]); body3=abs(c[-1]-o[-1])
        if body2<=max(body1,body3) and c[-1]>o[-1] and c[-1]>h[-2]: models.append("T4三K反转")
    if n>=3 and h[-1]>h[-2] and c[-2]<c[-3]: models.append("T5回调突刺")
    print(f"  TDS模型: {models}")
    
    # 520战法
    cross520 = "金叉" if (not np.isnan(ma5.iloc[-5]) and ma5.iloc[-5]<=ma20.iloc[-5] and ma5.iloc[-1]>ma20.iloc[-1]) else ("多头" if ma5.iloc[-1]>ma20.iloc[-1] else "空头")
    # 破五反五
    below5 = any(c[i]<ma5.iloc[i] for i in range(max(0,n-7),n-2) if not np.isnan(ma5.iloc[i]))
    b5r5 = "是" if (below5 and c[-1]>ma5.iloc[-1]) else "否"
    # 量能
    vol5=np.mean(v[-5:]); vol20=np.mean(v[-20:])
    print(f"\n  Mistery: 520={cross520} 破五反五={b5r5} 量能比={vol5/vol20:.2f}")
    
    # 资金流向
    time.sleep(0.5)
    mf = ts("moneyflow", {"ts_code":code, "trade_date":"20260409"},
            "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
    if not mf.empty:
        nb = mf["buy_elg_amount"].iloc[0]+mf["buy_lg_amount"].iloc[0]-mf["sell_elg_amount"].iloc[0]-mf["sell_lg_amount"].iloc[0]
        print(f"  4/9大单净流入: {nb:.0f}万 ({nb/10000:.2f}亿)")
    
    # 基本面
    time.sleep(0.5)
    bas = ts("daily_basic", {"ts_code":code, "trade_date":"20260409"},
             "ts_code,pe_ttm,pb,total_mv,turnover_rate_f")
    if not bas.empty:
        print(f"  PE={bas['pe_ttm'].iloc[0]} PB={bas['pb'].iloc[0]} 市值={bas['total_mv'].iloc[0]/10000:.0f}亿 换手={bas['turnover_rate_f'].iloc[0]}%")

print("\n完成！")
