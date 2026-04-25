#!/usr/bin/env python3
"""获取4/9情绪周期数据 + 8只股的TDS波峰波谷+K线组合详细分析"""
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

# ========== 1. 情绪周期数据 ==========
print("=" * 80)
print("一、4/9 全市场情绪数据")
print("=" * 80)

d09 = ts("daily", {"trade_date":"20260409"}, "ts_code,pct_chg,vol,amount")
d08 = ts("daily", {"trade_date":"20260408"}, "ts_code,pct_chg,vol,amount")
time.sleep(1)

if not d09.empty:
    total = len(d09)
    up = len(d09[d09["pct_chg"]>0])
    down = len(d09[d09["pct_chg"]<0])
    limit_up = len(d09[d09["pct_chg"]>=9.5])
    limit_up_20 = len(d09[d09["pct_chg"]>=19.5])  # 20cm
    limit_down = len(d09[d09["pct_chg"]<=-9.5])
    total_amt = d09["amount"].sum()/100000
    
    print(f"4/9: 总{total}只 上涨{up}({up/total*100:.0f}%) 下跌{down}({down/total*100:.0f}%)")
    print(f"  涨停: {limit_up}只(含20cm {limit_up_20}只)  跌停: {limit_down}只")
    print(f"  成交额: {total_amt:.0f}亿")

if not d08.empty:
    total8 = len(d08)
    up8 = len(d08[d08["pct_chg"]>0])
    lu8 = len(d08[d08["pct_chg"]>=9.5])
    ld8 = len(d08[d08["pct_chg"]<=-9.5])
    amt8 = d08["amount"].sum()/100000
    print(f"\n4/8: 总{total8}只 上涨{up8}({up8/total8*100:.0f}%) 涨停{lu8} 跌停{ld8} 成交{amt8:.0f}亿")

# 涨停详情（如果能获取到）
print("\n尝试获取涨停详情...")
time.sleep(62)  # 等60s限流
lim = ts("limit_list_d", {"trade_date":"20260409","limit_type":"U"},
         "ts_code,name,close,pct_chg,fc_ratio,fd_amount,first_time,last_time,open_times,limit_times")
if not lim.empty:
    print(f"4/9涨停: {len(lim)}只")
    # 连板分布
    board_dist = lim["limit_times"].value_counts().sort_index()
    print(f"  连板分布: {dict(board_dist)}")
    max_board = lim["limit_times"].max()
    max_board_stock = lim[lim["limit_times"]==max_board][["ts_code","name","limit_times"]].values.tolist()
    print(f"  最高连板: {max_board}板 {max_board_stock}")
    # 封板率
    if "fc_ratio" in lim.columns:
        avg_fc = lim["fc_ratio"].mean()
        print(f"  平均封板率: {avg_fc:.1f}%")
    # 首板数量
    first_board = len(lim[lim["limit_times"]==1])
    print(f"  首板: {first_board}只")
else:
    print("  涨停板数据获取失败")

# 4/8涨停
time.sleep(62)
lim8 = ts("limit_list_d", {"trade_date":"20260408","limit_type":"U"},
          "ts_code,name,close,limit_times")
if not lim8.empty:
    print(f"\n4/8涨停: {len(lim8)}只")
    bd8 = lim8["limit_times"].value_counts().sort_index()
    print(f"  连板分布: {dict(bd8)}")

# ========== 2. 8只股TDS详细分析 ==========
print("\n" + "=" * 80)
print("二、8只股 TDS K线组合分析")
print("=" * 80)

stocks = [
    ("002536.SZ", "飞龙股份"), ("000977.SZ", "浪潮信息"),
    ("002475.SZ", "立讯精密"), ("002179.SZ", "中航光电"),
    ("603112.SH", "华翔股份"), ("000506.SZ", "招金黄金"),
    ("601677.SH", "明泰铝业"), ("603588.SH", "高能环境"),
]

for code, name in stocks:
    kdf = ts("daily", {"ts_code":code, "start_date":"20260201", "end_date":"20260409"},
             "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")
    time.sleep(0.8)
    if kdf.empty: continue
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    n = len(kdf)
    c = kdf["close"].values; o = kdf["open"].values
    h = kdf["high"].values; l = kdf["low"].values
    
    print(f"\n--- {name}({code}) ---")
    
    # TDS波峰波谷（左右各3根）
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i] > max(h[max(0,i-3):i]) and h[i] > max(h[i+1:min(n,i+4)]):
            peaks.append((i, kdf["trade_date"].iloc[i], h[i]))
        if l[i] < min(l[max(0,i-3):i]) and l[i] < min(l[i+1:min(n,i+4)]):
            troughs.append((i, kdf["trade_date"].iloc[i], l[i]))
    
    # TDS趋势判定
    trend = "未知"
    if len(peaks)>=2 and len(troughs)>=2:
        pk_up = peaks[-1][2] > peaks[-2][2]
        tr_up = troughs[-1][2] > troughs[-2][2]
        if pk_up and tr_up: trend = "上行"
        elif not pk_up and not tr_up: trend = "下行"
        else: trend = "转折/震荡"
    
    print(f"  TDS趋势: {trend}")
    print(f"  波峰: {[(p[1],p[2]) for p in peaks[-3:]]}")
    print(f"  波谷: {[(t[1],t[2]) for t in troughs[-3:]]}")
    
    # 最近3日K线组合类型
    if n >= 3:
        i = n-1
        # K线1: 倒数第三 → K线2: 倒数第二 → K线3: 最新
        for j in range(max(0,n-3), n-1):
            k1_h, k1_l = h[j], l[j]
            k2_h, k2_l = h[j+1], l[j+1]
            k2_o, k2_c = o[j+1], c[j+1]
            k1_o, k1_c = o[j], c[j]
            
            combo = []
            # 多头推进
            if k2_l > k1_l and k2_h > k1_h:
                combo.append("多头推进")
            # 空头推进
            elif k2_h < k1_h and k2_l < k1_l:
                combo.append("空头推进")
            # 扩张
            if k2_h > k1_h and k2_l < k1_l:
                combo.append("扩张K线")
            # 收缩
            elif k2_h < k1_h and k2_l > k1_l:
                combo.append("收缩K线")
            # 阳线吞没
            if k2_c > k2_o and k2_c > k1_h and k1_c < k1_o:
                combo.append("阳线吞没")
            # 阴线吞没
            if k2_c < k2_o and k2_c < k1_l and k1_c > k1_o:
                combo.append("阴线吞没")
            
            d1 = kdf["trade_date"].iloc[j]
            d2 = kdf["trade_date"].iloc[j+1]
            print(f"  {d1}→{d2}: {combo if combo else ['无特殊']}")
    
    # TDS模型检测
    models = []
    last = c[-1]
    
    # T1推进：最近有连续多头推进
    if n>=3 and h[-1]>h[-2] and l[-1]>l[-2] and h[-2]>h[-3] and l[-2]>l[-3]:
        models.append("T1推进(连续多头推进)")
    elif n>=2 and h[-1]>h[-2] and l[-1]>l[-2]:
        models.append("T1推进(单次)")
    
    # T2吞没
    if n>=2 and c[-1]>o[-1] and c[-1]>h[-2] and c[-2]<o[-2]:
        models.append("T2阳线吞没")
    
    # T3突破：突破最近波峰
    if peaks:
        last_peak = peaks[-1][2]
        if last > last_peak:
            models.append(f"T3突破(破峰{last_peak:.2f})")
    
    # T4三K反转（逆趋势）
    if n>=3 and trend == "下行":
        k1, k2, k3 = (o[-3],c[-3],h[-3],l[-3]), (o[-2],c[-2],h[-2],l[-2]), (o[-1],c[-1],h[-1],l[-1])
        body2 = abs(k2[1]-k2[0]); body1 = abs(k1[1]-k1[0]); body3 = abs(k3[1]-k3[0])
        if body2 <= max(body1,body3) and k3[1]>k3[0] and k3[1]>k2[2]:
            models.append("T4三K反转")
    
    # T5回调突刺
    if n>=3 and h[-1]>h[-2] and c[-2]<c[-3]:  # 回调后突破前日高
        models.append("T5回调突刺")
    
    print(f"  TDS模型: {models if models else ['无触发']}")
    
    # Mistery快速判定
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1]
    
    # 520战法
    if ma5 > ma20:
        # 检查是否刚金叉（近5日内）
        ma5s = pd.Series(c).rolling(5).mean()
        ma20s = pd.Series(c).rolling(20).mean()
        cross520 = "金叉" if ma5s.iloc[-5] <= ma20s.iloc[-5] else "多头运行"
    else:
        cross520 = "空头"
    
    # 5日线法则
    above_ma5 = c[-1] > ma5
    
    # 量价
    vol5 = np.mean(kdf["vol"].values[-5:])
    vol20 = np.mean(kdf["vol"].values[-20:])
    vol_status = "放量" if vol5/vol20 > 1.5 else ("温和" if vol5/vol20 > 1 else "缩量")
    
    bias20 = (c[-1]-ma20)/ma20*100
    
    print(f"  Mistery: 520={cross520} 5日线={'站上' if above_ma5 else '跌破'} "
          f"量能={vol_status}({vol5/vol20:.2f}) BIAS20={bias20:.1f}%")
    
    # 是否涨停
    if kdf["pct_chg"].iloc[-1] >= 9.5:
        print(f"  ⚡ 4/9涨停！")
    if kdf["pct_chg"].iloc[-2] >= 9.5:
        print(f"  ⚡ 4/8涨停！")

print("\n数据采集完成！")
