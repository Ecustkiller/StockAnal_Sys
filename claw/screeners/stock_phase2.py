#!/usr/bin/env python3
"""Phase 2: 候选股深度技术分析 - K线+均线+MACD+波峰波谷"""
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

# 从Phase1筛选出的13只候选
candidates = [
    "002179.SZ",  # 中航光电
    "601231.SH",  # 环旭电子
    "000977.SZ",  # 浪潮信息
    "002906.SZ",  # 华阳集团
    "002475.SZ",  # 立讯精密
    "000063.SZ",  # 中兴通讯
    "688208.SH",  # 道通科技
    "300456.SZ",  # 赛微电子
    "002342.SZ",  # 巨力索具
    "002281.SZ",  # 光迅科技
    "000078.SZ",  # 海王生物
    "002536.SZ",  # 飞龙股份
    "600151.SH",  # 航天机电
]

print("="*80)
print("Phase 2: 候选股深度技术分析")
print("="*80)

results = []
for code in candidates:
    # 获取近80日K线
    kdf = ts("daily", {"ts_code": code, "start_date": "20260101", "end_date": "20260409"},
             "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
    time.sleep(0.8)
    
    if kdf.empty or len(kdf) < 20:
        print(f"  {code}: 数据不足({len(kdf)}条), 跳过")
        continue
    
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    
    c = kdf["close"].values
    h = kdf["high"].values
    l = kdf["low"].values
    v = kdf["vol"].values
    n = len(c)
    
    # 1. 均线
    ma5 = pd.Series(c).rolling(5).mean().values
    ma10 = pd.Series(c).rolling(10).mean().values
    ma20 = pd.Series(c).rolling(20).mean().values
    ma60 = pd.Series(c).rolling(60).mean().values if n>=60 else np.full(n, np.nan)
    
    # 2. MACD
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean().values
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
    macd_bar = 2 * (dif - dea)
    
    # 3. RSI(14)
    delta = pd.Series(c).diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = (100 - 100/(1+rs)).values
    
    # 4. 波峰波谷检测(简化版: 5日局部极值)
    peaks = []
    troughs = []
    for i in range(5, n-1):
        if h[i] == max(h[i-5:i+1]):
            peaks.append((i, h[i], kdf["trade_date"].iloc[i]))
        if l[i] == min(l[i-5:i+1]):
            troughs.append((i, l[i], kdf["trade_date"].iloc[i]))
    
    # 去重（连续极值取最极端的）
    def dedup_extremes(pts, is_peak=True):
        if not pts: return []
        result = [pts[0]]
        for p in pts[1:]:
            if p[0] - result[-1][0] <= 3:
                if (is_peak and p[1] > result[-1][1]) or (not is_peak and p[1] < result[-1][1]):
                    result[-1] = p
            else:
                result.append(p)
        return result
    
    peaks = dedup_extremes(peaks, True)
    troughs = dedup_extremes(troughs, False)
    
    # 5. 趋势判定
    last_close = c[-1]
    last_ma5 = ma5[-1]
    last_ma10 = ma10[-1]
    last_ma20 = ma20[-1]
    last_ma60 = ma60[-1] if not np.isnan(ma60[-1]) else None
    last_dif = dif[-1]
    last_dea = dea[-1]
    last_macd = macd_bar[-1]
    last_rsi = rsi[-1] if not np.isnan(rsi[-1]) else 50
    
    # 均线多头
    ma_bull = 0
    if last_close > last_ma5: ma_bull += 1
    if last_ma5 > last_ma10: ma_bull += 1
    if last_ma10 > last_ma20: ma_bull += 1
    if last_ma60 and last_ma20 > last_ma60: ma_bull += 1
    
    # MACD状态
    macd_cross = "金叉" if last_dif > last_dea and dif[-2] <= dea[-2] else ("死叉" if last_dif < last_dea and dif[-2] >= dea[-2] else ("多头" if last_dif > 0 else "空头"))
    
    # 偏离度
    bias20 = (last_close - last_ma20)/last_ma20*100
    bias60 = (last_close - last_ma60)/last_ma60*100 if last_ma60 else None
    
    # 量能
    vol5 = np.mean(v[-5:])
    vol20 = np.mean(v[-20:])
    vol_ratio_5_20 = vol5/vol20 if vol20 > 0 else 1
    
    # 波峰波谷趋势
    trend_pk = "未知"
    if len(peaks) >= 2 and len(troughs) >= 2:
        if peaks[-1][1] > peaks[-2][1] and troughs[-1][1] > troughs[-2][1]:
            trend_pk = "上升"
        elif peaks[-1][1] < peaks[-2][1] and troughs[-1][1] < troughs[-2][1]:
            trend_pk = "下降"
        else:
            trend_pk = "震荡"
    
    # 近期形态
    # 检查是否有"回调到MA20附近"的形态
    dist_to_ma20 = abs(bias20)
    near_ma20 = dist_to_ma20 <= 5
    
    # 近3日K线形态
    last3_pct = kdf["pct_chg"].iloc[-3:].tolist()
    pattern = ""
    if all(p > 0 for p in last3_pct):
        pattern = "三连阳"
    elif last3_pct[-1] > 3 and last3_pct[-2] < 0:
        pattern = "反转阳"
    elif kdf["pct_chg"].iloc[-1] > 9.5:
        pattern = "涨停"
    
    # 综合技术评分(0-20)
    tech = 0
    tech += ma_bull * 2  # 均线多头(0-8)
    if last_dif > last_dea: tech += 2  # MACD多
    if last_macd > 0: tech += 1
    if 0 < bias20 <= 5: tech += 3  # 回调到位
    elif 5 < bias20 <= 10: tech += 2
    elif -3 <= bias20 <= 0: tech += 2  # 触碰MA20
    if vol_ratio_5_20 > 1.2: tech += 1  # 放量
    if trend_pk == "上升": tech += 2
    elif trend_pk == "震荡": tech += 1
    if 30 < last_rsi < 70: tech += 1  # RSI适中
    
    # 安全边际评分(0-10): 偏离度越低越好
    safety = 0
    if bias20 <= 5: safety += 3
    elif bias20 <= 10: safety += 2
    elif bias20 <= 15: safety += 1
    if bias60 is not None:
        if bias60 <= 10: safety += 2
        elif bias60 <= 20: safety += 1
    if last_rsi <= 65: safety += 2
    elif last_rsi <= 75: safety += 1
    if vol_ratio_5_20 <= 2: safety += 1  # 不过分放量
    
    info = {
        "code": code,
        "close": round(last_close, 2),
        "ma5": round(last_ma5, 2),
        "ma10": round(last_ma10, 2),
        "ma20": round(last_ma20, 2),
        "ma60": round(last_ma60, 2) if last_ma60 else None,
        "bias20": round(bias20, 1),
        "bias60": round(bias60, 1) if bias60 else None,
        "dif": round(last_dif, 3),
        "dea": round(last_dea, 3),
        "macd": macd_cross,
        "rsi": round(last_rsi, 1),
        "ma_bull": ma_bull,
        "trend": trend_pk,
        "vol_r": round(vol_ratio_5_20, 2),
        "pattern": pattern,
        "near_ma20": near_ma20,
        "peaks": [(p[2], round(p[1],2)) for p in peaks[-3:]],
        "troughs": [(t[2], round(t[1],2)) for t in troughs[-3:]],
        "tech_score": tech,
        "safety_score": safety,
        "total": tech + safety,
    }
    
    results.append(info)
    print(f"  {code}: 收{last_close} MA多头{ma_bull}/4 MACD:{macd_cross} 趋势:{trend_pk} "
          f"BIAS20:{bias20:.1f}% RSI:{last_rsi:.0f} 技术:{tech} 安全:{safety} 总:{tech+safety}")

# 排序输出
results.sort(key=lambda x: x["total"], reverse=True)

print(f"\n{'='*120}")
print("深度技术分析排名")
print(f"{'='*120}")
print(f"{'代码':<12} {'收盘':>6} {'MA5':>7} {'MA10':>7} {'MA20':>7} {'MA60':>7} "
      f"{'BIAS20':>7} {'MACD':>5} {'RSI':>5} {'趋势':>5} {'MA多头':>5} {'量比5/20':>8} "
      f"{'形态':>6} {'技术分':>5} {'安全分':>5} {'总分':>5}")
print("-"*120)
for r in results:
    print(f"{r['code']:<12} {r['close']:>6.2f} {r['ma5']:>7.2f} {r['ma10']:>7.2f} {r['ma20']:>7.2f} "
          f"{str(r['ma60'] or 'N/A'):>7} {r['bias20']:>6.1f}% {r['macd']:>5} {r['rsi']:>5.1f} "
          f"{r['trend']:>5} {r['ma_bull']:>5}/4 {r['vol_r']:>8.2f} "
          f"{r['pattern'] or '-':>6} {r['tech_score']:>5} {r['safety_score']:>5} {r['total']:>5}")

print(f"\n{'='*120}")
print("⭐ 最终推荐（总分最高 + 安全边际好）")
print(f"{'='*120}")

for r in results[:5]:
    print(f"\n{'='*60}")
    print(f"  {r['code']}  收盘:{r['close']}元")
    print(f"  均线: MA5={r['ma5']} MA10={r['ma10']} MA20={r['ma20']} MA60={r['ma60']}")
    print(f"  偏离: BIAS20={r['bias20']}% BIAS60={r['bias60']}%")
    print(f"  MACD: DIF={r['dif']} DEA={r['dea']} 状态={r['macd']}")
    print(f"  RSI: {r['rsi']}")
    print(f"  波峰波谷趋势: {r['trend']}")
    print(f"  近期波峰: {r['peaks']}")
    print(f"  近期波谷: {r['troughs']}")
    print(f"  量比(5/20): {r['vol_r']}")
    print(f"  形态: {r['pattern'] or '无明显形态'}")
    print(f"  技术评分: {r['tech_score']}/20  安全评分: {r['safety_score']}/10  总分: {r['total']}/30")

top5_codes = [r["code"] for r in results[:5]]
print(f"\n\nTOP5代码: {top5_codes}")
