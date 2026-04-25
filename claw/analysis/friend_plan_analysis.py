#!/usr/bin/env python3
"""
朋友416操作计划综合评价 — 基于真实数据 + 多Skill分析
=========================
获取4/15真实行情数据，对朋友计划中的每只个股进行量化检测
"""
import requests, time, json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from market_sentiment import get_market_sentiment, print_sentiment_summary, match_industry, get_trade_dates, get_t_minus_n

# ===== 配置 =====
ANALYSIS_DATE = '20260415'  # 分析日期，修改此处切换

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0:
        print(f"  [API Error] {api}: {j.get('msg','')[:80]}")
        return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

print("="*80)
print("朋友416操作计划 — 真实数据综合评价")
print("数据截止：4/15收盘")
print("="*80)

# ===== 朋友计划中的所有个股 =====
# 先通过名称搜索获取代码
friend_stocks_names = [
    # 核心个股
    "德明利", "佰维存储",                                    # 存储
    "长飞光纤", "汇源通信", "通鼎互联", "长芯博创",          # 光纤
    "强瑞技术", "申菱环境", "新朋股份",                      # 液冷
    "利通电子", "协创数据", "宏景股份",                      # 算力租赁
    # 操作计划
    "华远控股", "圣阳股份", "天邦食品",                      # 连板梯队
    "长源东谷",                                              # 首板低吸
    "常宝股份", "福鞍股份", "联德股份", "东方电气",          # 燃气轮机
    "南京医药", "青山纸业",                                  # 低吸持有
    "沪电股份", "云南锗业",                                  # 清仓/减仓
    "大胜达", "联翔股份", "狮头股份",                        # 继续持有
    # 潜伏跟踪
    "唯科科技",
]

print("\n[Step 0] 搜索股票代码...")
stk_all = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
time.sleep(1)
name_to_code = dict(zip(stk_all["name"], stk_all["ts_code"]))
name_to_ind = dict(zip(stk_all["name"], stk_all["industry"]))
ind_map = dict(zip(stk_all["ts_code"], stk_all["industry"]))
code_to_name = dict(zip(stk_all["ts_code"], stk_all["name"]))

friend_stocks = {}
for name in friend_stocks_names:
    code = name_to_code.get(name)
    if code:
        friend_stocks[code] = {"name": name, "industry": name_to_ind.get(name, "?")}
        print(f"  ✅ {name} → {code} ({name_to_ind.get(name, '?')})")
    else:
        # 模糊搜索
        matches = [(n, c) for n, c in name_to_code.items() if name in n]
        if matches:
            n, c = matches[0]
            friend_stocks[c] = {"name": n, "industry": name_to_ind.get(n, "?")}
            print(f"  ⚠️ {name} → {n}({c}) (模糊匹配)")
        else:
            print(f"  ❌ {name} 未找到")

print(f"\n共找到 {len(friend_stocks)} 只股票")

# ===== Step 1: 市场情绪数据（使用统一模块）=====
print(f"\n[Step 1] 市场情绪数据 ({ANALYSIS_DATE})")
sentiment = get_market_sentiment(ANALYSIS_DATE)
print_sentiment_summary(sentiment)

# 从统一模块提取变量
zt_cnt = sentiment['zt_cnt']
zb_cnt = sentiment['zb_cnt']
dt_cnt = sentiment['dt_cnt']
fbl = sentiment['fbl']
earn_rate = sentiment['earn_rate']
total_amount = sentiment['total_amount']
ak_ind_zt = sentiment['ind_zt_dict']
zt_codes = sentiment['zt_codes']
bjcj3 = sentiment['bjcj3_phase']
bjcj3_pos = sentiment['bjcj3_pos']
bjcj3_max = sentiment['bjcj3_max_pct']

# ===== Step 2: 交易日历（使用统一模块）=====
print("\n[Step 2] 交易日历")
trade_dates = get_trade_dates("20260201", ANALYSIS_DATE)
T0 = ANALYSIS_DATE
T5 = get_t_minus_n(trade_dates, T0, 5)
T10 = get_t_minus_n(trade_dates, T0, 10)
T20 = get_t_minus_n(trade_dates, T0, 20)
print(f"  T0={T0} T-5={T5} T-10={T10} T-20={T20}")

# ===== Step 3: 逐只获取K线+基本面+资金 =====
print("\n[Step 3] 逐只获取详细数据")
results = {}

for code, info in friend_stocks.items():
    name = info["name"]
    ind = info["industry"]
    print(f"\n--- {name}({code}) [{ind}] ---")
    
    # K线(近60天)
    kdf = ts("daily", {"ts_code":code, "start_date":"20260201", "end_date":"20260415"},
             "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
    time.sleep(0.8)
    if kdf.empty:
        print(f"  ❌ 无K线数据")
        continue
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    n = len(kdf)
    c = kdf["close"].astype(float).values
    o = kdf["open"].astype(float).values
    h = kdf["high"].astype(float).values
    l = kdf["low"].astype(float).values
    v = kdf["vol"].astype(float).values
    pcts = kdf["pct_chg"].astype(float).values
    
    # 最新收盘价
    close_now = c[-1]
    pct_today = pcts[-1]
    
    # 均线
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1] if n >= 5 else close_now
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1] if n >= 10 else close_now
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1] if n >= 20 else close_now
    ma60 = pd.Series(c).rolling(60).mean().iloc[-1] if n >= 60 else None
    
    # MACD
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean()
    ema26 = pd.Series(c).ewm(span=min(26, n), adjust=False).mean()
    dif = (ema12 - ema26).iloc[-1]
    dea = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]
    macd = 2 * (dif - dea)
    
    # BIAS
    bias5 = (close_now - ma5) / ma5 * 100 if ma5 > 0 else 0
    bias10 = (close_now - ma10) / ma10 * 100 if ma10 > 0 else 0
    bias20 = (close_now - ma20) / ma20 * 100 if ma20 > 0 else 0
    
    # 涨幅
    r5 = r10 = r20 = 0
    for _, row in kdf.iterrows():
        if row["trade_date"] == T5: r5 = (close_now - row["close"]) / row["close"] * 100
        if row["trade_date"] == T10: r10 = (close_now - row["close"]) / row["close"] * 100
        if row["trade_date"] == T20: r20 = (close_now - row["close"]) / row["close"] * 100
    
    # BBW (布林带宽)
    if n >= 20:
        ma20_s = pd.Series(c).rolling(20).mean()
        std20 = pd.Series(c).rolling(20).std()
        bbw = (2 * std20 / ma20_s).iloc[-1] if ma20_s.iloc[-1] > 0 else 0
    else:
        bbw = 0
    
    # RSI
    delta = pd.Series(c).diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = (100 - 100 / (1 + gain / loss)).iloc[-1] if n >= 15 else 50
    
    # 量比
    vol_today = v[-1]
    vol_5avg = np.mean(v[-6:-1]) if n >= 6 else v[-1]
    vol_ratio = vol_today / vol_5avg if vol_5avg > 0 else 1
    
    # 缩量天数（连续下跌天数）
    adj_days = 0
    for k in range(len(pcts)-2, -1, -1):
        if pcts[k] < 0: adj_days += 1
        else: break
    
    # 是否涨停
    is_zt = pcts[-1] >= 9.5
    code6 = code[:6]
    is_in_zt_pool = code6 in zt_codes
    
    # 基本面
    time.sleep(0.5)
    bas = ts("daily_basic", {"ts_code":code, "trade_date":"20260415"},
             "ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate_f,volume_ratio")
    pe = mv = circ_mv = tr = vr_ts = None
    if not bas.empty:
        pe = bas["pe_ttm"].iloc[0]
        mv = bas["total_mv"].iloc[0] / 10000 if bas["total_mv"].iloc[0] else 0
        circ_mv = bas["circ_mv"].iloc[0] / 10000 if bas["circ_mv"].iloc[0] else 0
        tr = bas["turnover_rate_f"].iloc[0]
        vr_ts = bas["volume_ratio"].iloc[0]
    
    # 资金流向
    time.sleep(0.5)
    mf = ts("moneyflow", {"ts_code":code, "trade_date":"20260415"},
            "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
    net_flow = 0
    if not mf.empty:
        net_flow = (mf["buy_elg_amount"].iloc[0] + mf["buy_lg_amount"].iloc[0] -
                   mf["sell_elg_amount"].iloc[0] - mf["sell_lg_amount"].iloc[0]) / 10000
    
    # TDS分析
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i] > max(h[max(0,i-3):i]) and h[i] > max(h[i+1:min(n,i+4)]):
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        if l[i] < min(l[max(0,i-3):i]) and l[i] < min(l[i+1:min(n,i+4)]):
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    
    tds_trend = "震荡"
    if len(peaks) >= 2 and len(troughs) >= 2:
        if peaks[-1][1] > peaks[-2][1] and troughs[-1][1] > troughs[-2][1]:
            tds_trend = "上升"
        elif peaks[-1][1] < peaks[-2][1] and troughs[-1][1] < troughs[-2][1]:
            tds_trend = "下降"
    
    # 板块涨停数（使用统一模块的行业映射）
    ind_zt_cnt = match_industry(ind, ak_ind_zt)
    
    # 近10日K线
    recent = kdf.tail(10)
    kline_str = ""
    for _, row in recent.iterrows():
        kline_str += f"    {row['trade_date']}: O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} C={row['close']:.2f} Chg={row['pct_chg']:+.2f}% Vol={row['vol']:.0f}\n"
    
    # Mistery趋势评分
    mistery_score = 0
    mistery_tags = []
    if close_now > ma5 > ma10 > ma20:
        mistery_score += 5; mistery_tags.append("多头排列")
    elif close_now > ma5 > ma10:
        mistery_score += 4; mistery_tags.append("短中期多头")
    elif close_now > ma20:
        mistery_score += 3; mistery_tags.append("站上MA20")
    elif close_now > ma5:
        mistery_score += 2; mistery_tags.append("站上MA5")
    else:
        mistery_tags.append("均线下方")
    
    if dif > dea: mistery_score += 2; mistery_tags.append("MACD金叉")
    elif dif > 0: mistery_score += 1; mistery_tags.append("DIF>0")
    
    if vol_ratio > 1.5: mistery_score += 2; mistery_tags.append(f"放量{vol_ratio:.1f}x")
    elif vol_ratio > 1.0: mistery_score += 1; mistery_tags.append(f"温和放量{vol_ratio:.1f}x")
    
    # BJCJ检测
    bjcj_tags = []
    bjcj_score = 0
    
    # BJCJ-1 首板打板检测
    if is_zt or is_in_zt_pool:
        bjcj1 = 0
        if circ_mv and 20 <= circ_mv <= 150: bjcj1 += 3
        if vr_ts and vr_ts >= 3: bjcj1 += 2
        elif vr_ts and vr_ts >= 2: bjcj1 += 1
        if close_now < 20: bjcj1 += 1
        if tr:
            if 8 <= tr <= 15: bjcj1 += 2
            elif 3 <= tr < 8: bjcj1 += 1
        if ind_zt_cnt >= 5: bjcj1 += 2
        elif ind_zt_cnt >= 3: bjcj1 += 1
        bjcj_score += bjcj1
        if bjcj1 >= 6: bjcj_tags.append(f"BJCJ-1首板✅({bjcj1}分)")
        else: bjcj_tags.append(f"BJCJ-1首板({bjcj1}分)")
    
    # BJCJ-2 高效低吸检测
    if not is_zt and adj_days >= 3:
        bjcj2 = 0
        bjcj2_detail = []
        if adj_days >= 4: bjcj2 += 1; bjcj2_detail.append(f"调整{adj_days}天")
        is_first_yang = 0.5 <= pcts[-1] <= 5
        if is_first_yang: bjcj2 += 1; bjcj2_detail.append("首阳")
        # 缩量
        if adj_days >= 3 and n > adj_days + 5:
            adj_vol = np.mean(v[-(adj_days+1):-1])
            pre_vol = np.mean(v[-(adj_days+6):-(adj_days+1)])
            if pre_vol > 0 and adj_vol < pre_vol * 0.8:
                bjcj2 += 1; bjcj2_detail.append("缩量")
        # 放量反弹
        if n >= 6 and vol_ratio > 1.3:
            bjcj2 += 1; bjcj2_detail.append("放量反弹")
        # 近MA20
        if n >= 20 and abs(close_now - ma20) / ma20 < 0.10:
            bjcj2 += 1; bjcj2_detail.append("近MA20")
        bjcj_score += bjcj2
        if bjcj2 >= 3: bjcj_tags.append(f"BJCJ-2低吸✅({bjcj2}分,{'+'.join(bjcj2_detail)})")
        elif bjcj2 >= 1: bjcj_tags.append(f"BJCJ-2低吸({bjcj2}分,{'+'.join(bjcj2_detail)})")
    
    # BJCJ-5 板块共振
    bjcj5 = 0
    if ind_zt_cnt >= 5: bjcj5 = 3; bjcj_tags.append(f"BJCJ-5板块✅({ind_zt_cnt}家涨停)")
    elif ind_zt_cnt >= 3: bjcj5 = 2; bjcj_tags.append(f"BJCJ-5板块({ind_zt_cnt}家涨停)")
    elif ind_zt_cnt >= 1: bjcj5 = 1; bjcj_tags.append(f"BJCJ-5板块弱({ind_zt_cnt}家涨停)")
    else: bjcj_tags.append("BJCJ-5板块❌(0家涨停)")
    bjcj_score += bjcj5
    
    # BJCJ-3 仓位检测（偏离5日线）
    bjcj3_warning = ""
    if bias5 > 8:
        bjcj3_warning = f"⚠️偏离5日线{bias5:.1f}%>8%，有补跌风险"
    elif bias5 > 5:
        bjcj3_warning = f"⚠️偏离5日线{bias5:.1f}%>5%，注意回调"
    
    # 打印详细数据
    pe_str = f"{pe:.1f}" if pe and pe > 0 else "N/A"
    print(f"  收盘:{close_now:.2f} 涨跌:{pct_today:+.2f}% PE:{pe_str} 市值:{mv:.0f}亿 流通:{circ_mv:.0f}亿")
    print(f"  MA5:{ma5:.2f} MA10:{ma10:.2f} MA20:{ma20:.2f} {'MA60:'+str(round(ma60,2)) if ma60 else ''}")
    print(f"  BIAS5:{bias5:+.1f}% BIAS10:{bias10:+.1f}% BIAS20:{bias20:+.1f}%")
    print(f"  5日涨幅:{r5:+.1f}% 10日:{r10:+.1f}% 20日:{r20:+.1f}%")
    print(f"  BBW:{bbw:.3f} RSI:{rsi:.1f} DIF:{dif:.3f} DEA:{dea:.3f} MACD:{macd:.3f}")
    print(f"  量比:{vol_ratio:.2f} 换手:{tr}% 净流入:{net_flow:+.2f}亿")
    print(f"  TDS趋势:{tds_trend} 调整天数:{adj_days} 涨停:{is_zt}")
    print(f"  板块涨停:{ind_zt_cnt}家 Mistery:{mistery_score}/15 {mistery_tags}")
    print(f"  BJCJ: {bjcj_tags}")
    if bjcj3_warning: print(f"  {bjcj3_warning}")
    
    results[code] = {
        "name": name, "code": code, "industry": ind,
        "close": close_now, "pct_today": pct_today,
        "pe": pe, "mv": mv, "circ_mv": circ_mv, "tr": tr,
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
        "bias5": bias5, "bias10": bias10, "bias20": bias20,
        "r5": r5, "r10": r10, "r20": r20,
        "bbw": bbw, "rsi": rsi, "dif": dif, "dea": dea, "macd": macd,
        "vol_ratio": vol_ratio, "net_flow": net_flow,
        "tds_trend": tds_trend, "adj_days": adj_days,
        "is_zt": is_zt, "ind_zt_cnt": ind_zt_cnt,
        "mistery_score": mistery_score, "mistery_tags": mistery_tags,
        "bjcj_score": bjcj_score, "bjcj_tags": bjcj_tags,
        "bjcj3_warning": bjcj3_warning,
        "kline_recent": kline_str,
        "peaks": [(d, float(p)) for d, p in peaks[-3:]],
        "troughs": [(d, float(t)) for d, t in troughs[-3:]],
    }

# ===== Step 4: 输出汇总 =====
print("\n" + "="*120)
print("汇总输出")
print("="*120)

# 市场情绪（使用统一模块数据）
market_data = {
    "zt_cnt": zt_cnt, "zb_cnt": zb_cnt, "dt_cnt": dt_cnt,
    "fbl": round(fbl, 1), "earn_rate": round(earn_rate, 1),
    "total_amount": round(total_amount, 0),
    "st_zt_cnt": sentiment['st_zt_cnt'], "st_dt_cnt": sentiment['st_dt_cnt'],
    "zt_cnt_all": sentiment['zt_cnt_all'], "dt_cnt_all": sentiment['dt_cnt_all'],
    "bjcj3": bjcj3, "bjcj3_pos": bjcj3_pos, "bjcj3_max": bjcj3_max,
    "ak_ind_zt_top10": dict(sentiment['ind_zt_top10']) if sentiment['ind_zt_top10'] else {},
}

print(f"\n市场情绪（统一口径）:")
print(f"  涨停{zt_cnt}(非ST) + ST{sentiment['st_zt_cnt']} = 全口径{sentiment['zt_cnt_all']}")
print(f"  炸板{zb_cnt} 封板率{fbl:.0f}%(非ST口径)")
print(f"  跌停{dt_cnt}(非ST) + ST{sentiment['st_dt_cnt']} = 全口径{sentiment['dt_cnt_all']}")
print(f"  赚钱效应:{earn_rate:.0f}% 成交:{total_amount:.0f}亿")
print(f"  BJCJ-3: {bjcj3} 建议仓位:{bjcj3_pos}")

# 个股汇总表
print(f"\n{'='*160}")
print(f"{'股票':<16} {'行业':<8} {'收盘':>7} {'涨跌%':>6} {'5日%':>6} {'10日%':>6} {'BIAS5':>6} "
      f"{'量比':>5} {'净流亿':>6} {'Mistery':>7} {'BJCJ':>5} {'TDS':>4} {'BBW':>6} {'RSI':>5} {'风险提示'}")
print("-"*160)

for code, r in results.items():
    warning = r.get("bjcj3_warning", "")
    if not warning and r["bias5"] < -5:
        warning = "弱势"
    print(f"{r['name']}({code[:6]}) {r['industry']:<8} {r['close']:>7.2f} {r['pct_today']:>+6.2f} "
          f"{r['r5']:>+6.1f} {r['r10']:>+6.1f} {r['bias5']:>+6.1f} "
          f"{r['vol_ratio']:>5.2f} {r['net_flow']:>+6.2f} {r['mistery_score']:>4d}/15 "
          f"{r['bjcj_score']:>5d} {r['tds_trend']:<4} {r['bbw']:>6.3f} {r['rsi']:>5.1f} {warning}")

# 保存完整结果
output = {
    "date": "20260415",
    "market": market_data,
    "stocks": {code: {k: v for k, v in r.items() if k != "kline_recent"} for code, r in results.items()},
    "stocks_kline": {code: r.get("kline_recent", "") for code, r in results.items()},
}
with open("/Users/ecustkiller/WorkBuddy/Claw/friend_plan_data.json", "w") as f:
    json.dump(output, f, ensure_ascii=False, indent=2, default=str)
print("\n✅ 已保存 friend_plan_data.json")
