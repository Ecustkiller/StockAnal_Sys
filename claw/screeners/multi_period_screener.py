#!/usr/bin/env python3
"""
多周期共振评分选股器
- 大周期(日线): 上涨+3 / 下跌-3 / 停顿0
- 中周期(4小时≈近5日走势): 上涨+2 / 下跌-2 / 停顿0
- 小周期(1小时≈近2日走势): 上涨+1 / 下跌-1 / 停顿0
- 总分范围: -6 ~ +6
- 6=三周期共振强势上涨，-6=三周期共振强势下跌

趋势状态判定方法（基于均线+价格位置+MACD）：
- 上涨: 价格站上对应周期均线 + 均线向上 + MACD多头
- 下跌: 价格跌破对应周期均线 + 均线向下 + MACD空头
- 停顿: 价格在均线附近震荡，方向不明
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

def calc_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def judge_trend(closes, period_name):
    """
    判断某个周期的趋势状态
    返回: ("上涨", score) / ("下跌", score) / ("停顿", score)
    
    判定逻辑:
    1. 均线方向: MA是否向上（最近值 > 5日前值）
    2. 价格位置: 收盘价是否在MA上方
    3. MACD状态: DIF是否 > DEA
    4. 价格动量: 近期涨跌幅
    三项中至少2项看多=上涨, 至少2项看空=下跌, 否则=停顿
    """
    if len(closes) < 20:
        return "停顿", 0
    
    c = np.array(closes)
    n = len(c)
    
    # 根据周期选择不同参数
    if period_name == "大周期":
        # 日线级别: 用MA20判断
        ma = pd.Series(c).rolling(20).mean().values
        ma_ref = pd.Series(c).rolling(20).mean().values  # MA20
        lookback = 10  # 看10日均线方向
        chg_period = 20  # 看20日涨跌幅
    elif period_name == "中周期":
        # 4小时≈近5日: 用MA10判断
        ma = pd.Series(c).rolling(10).mean().values
        lookback = 5
        chg_period = 10
    else:
        # 1小时≈近2日: 用MA5判断
        ma = pd.Series(c).rolling(5).mean().values
        lookback = 3
        chg_period = 5
    
    last_ma = ma[-1]
    if np.isnan(last_ma):
        return "停顿", 0
    
    # 1. 均线方向
    prev_ma = ma[-lookback] if not np.isnan(ma[-lookback]) else ma[-1]
    ma_up = last_ma > prev_ma * 1.001  # 上升
    ma_down = last_ma < prev_ma * 0.999  # 下降
    
    # 2. 价格位置
    price_above = c[-1] > last_ma
    price_below = c[-1] < last_ma
    
    # 3. MACD
    ema12 = calc_ema(pd.Series(c), 12).values
    ema26 = calc_ema(pd.Series(c), 26).values
    dif = ema12[-1] - ema26[-1]
    dea = calc_ema(pd.Series(ema12 - ema26), 9).values[-1]
    macd_bull = dif > dea
    macd_bear = dif < dea
    
    # 4. 动量
    chg = (c[-1] / c[-min(chg_period, n-1)] - 1) * 100
    
    # 综合判定
    bull_count = sum([ma_up, price_above, macd_bull])
    bear_count = sum([ma_down, price_below, macd_bear])
    
    if bull_count >= 2:
        return "上涨", {"ma_up": ma_up, "price_above": price_above, "macd_bull": macd_bull, "chg": round(chg,1)}
    elif bear_count >= 2:
        return "下跌", {"ma_down": ma_down, "price_below": price_below, "macd_bear": macd_bear, "chg": round(chg,1)}
    else:
        return "停顿", {"mixed": True, "chg": round(chg,1)}

def multi_period_score(closes):
    """计算多周期共振得分"""
    big = judge_trend(closes, "大周期")
    mid = judge_trend(closes, "中周期")
    small = judge_trend(closes, "小周期")
    
    score_map = {"上涨": 1, "下跌": -1, "停顿": 0}
    weight = {"大周期": 3, "中周期": 2, "小周期": 1}
    
    total = (score_map[big[0]] * weight["大周期"] + 
             score_map[mid[0]] * weight["中周期"] + 
             score_map[small[0]] * weight["小周期"])
    
    return {
        "大周期": big[0],
        "中周期": mid[0], 
        "小周期": small[0],
        "得分": total,
        "detail": {"大": big[1], "中": mid[1], "小": small[1]}
    }

# 趋势判断表
TREND_TABLE = {
    6: "强势上涨（三周期共振做多）",
    5: "强势上涨（大中上涨+小停顿）",
    4: "上涨（大中上涨+小下跌，回调中）",
    3: "偏多（大上涨+中小不确定）",
    2: "弱多",
    1: "微多/震荡偏多",
    0: "震荡/无方向",
    -1: "微空/震荡偏空",
    -2: "弱空",
    -3: "偏空",
    -4: "下跌",
    -5: "强势下跌",
    -6: "强势下跌（三周期共振做空）",
}

# ========== 主流程 ==========
print("=" * 100)
print("多周期共振评分选股器")
print("评分规则: 大(日线)±3 + 中(5日)±2 + 小(2日)±1 = -6~+6")
print("=" * 100)

# 先测试8只候选股
test_stocks = [
    ("002179.SZ", "中航光电"), ("002475.SZ", "立讯精密"),
    ("000977.SZ", "浪潮信息"), ("000506.SZ", "招金黄金"),
    ("601677.SH", "明泰铝业"), ("603588.SH", "高能环境"),
    ("603112.SH", "华翔股份"), ("002536.SZ", "飞龙股份"),
]

results = []
for code, name in test_stocks:
    kdf = ts("daily", {"ts_code":code, "start_date":"20260201", "end_date":"20260409"},
             "ts_code,trade_date,close")
    time.sleep(0.8)
    if kdf.empty or len(kdf) < 20:
        print(f"  {name}: 数据不足")
        continue
    
    kdf.sort_values("trade_date", inplace=True)
    closes = kdf["close"].tolist()
    
    result = multi_period_score(closes)
    result["code"] = code
    result["name"] = name
    result["close"] = closes[-1]
    results.append(result)

# 排序输出
results.sort(key=lambda x: x["得分"], reverse=True)

print(f"\n{'股票':<16} {'收盘':>7} {'大周期':>6} {'中周期':>6} {'小周期':>6} {'得分':>4}  趋势判断")
print("-" * 90)
for r in results:
    trend_desc = TREND_TABLE.get(r["得分"], "未知")
    print(f"{r['name']}({r['code'][:6]}) {r['close']:>7.2f} {r['大周期']:>6} {r['中周期']:>6} {r['小周期']:>6} {r['得分']:>+4d}  {trend_desc}")

# 详细分析
print(f"\n{'=' * 100}")
print("详细分析")
print(f"{'=' * 100}")
for r in results:
    print(f"\n--- {r['name']}({r['code']}) 得分:{r['得分']:+d} ---")
    for period in ["大", "中", "小"]:
        d = r["detail"][period]
        if isinstance(d, dict):
            print(f"  {period}周期: {r[period+'周期']}")
            for k, v in d.items():
                print(f"    {k}: {v}")

# ========== 全市场扫描（分数>=4的强势股）==========
print(f"\n{'=' * 100}")
print("全市场扫描: 寻找得分>=5的强势股")
print("（基于4/9收盘数据，取全市场日K近40日）")
print(f"{'=' * 100}")

# 获取全市场股票列表（仅沪深主板+创业板+科创板，排除ST/退市）
time.sleep(1)
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry,market")
if not stk.empty:
    stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
    stk = stk[~stk["name"].str.contains("ST|退", na=False)]
    print(f"  全市场有效股票: {len(stk)}只")

# 获取近40日全市场日收盘价（分批）
# 为节省API调用，我们取最近5个关键日期的全市场收盘价来近似判断
key_dates = ["20260409", "20260407", "20260402", "20260327", "20260313"]
all_daily = {}
for d in key_dates:
    time.sleep(1)
    df = ts("daily", {"trade_date": d}, "ts_code,close")
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in all_daily:
                all_daily[row["ts_code"]] = {}
            all_daily[row["ts_code"]][d] = row["close"]
    print(f"  {d}: {len(df)}只")

# 简化版多周期判定（基于5个日期点）
def quick_score(prices_dict):
    """
    基于5个关键日期快速评分
    prices_dict: {日期: 收盘价}
    大周期: 20日方向 (d0 vs d4≈20日前)
    中周期: 10日方向 (d0 vs d3≈10日前)  
    小周期: 5日方向 (d0 vs d2≈5日前)
    极短: 2日方向 (d0 vs d1)
    """
    dates = sorted(prices_dict.keys(), reverse=True)
    if len(dates) < 4:
        return None
    
    d0 = prices_dict[dates[0]]  # 最新(4/9)
    d1 = prices_dict.get(dates[1], d0)  # ~2日前(4/7)
    d2 = prices_dict.get(dates[2], d0)  # ~5日前(4/2)
    d3 = prices_dict.get(dates[3], d0)  # ~10日前(3/27)
    d4 = prices_dict.get(dates[4], d0) if len(dates)>4 else d3  # ~20日前(3/13)
    
    # 大周期(20日趋势)
    chg_big = (d0 - d4) / d4 * 100
    if chg_big > 5: big = "上涨"
    elif chg_big < -5: big = "下跌"
    else: big = "停顿"
    
    # 中周期(10日趋势)
    chg_mid = (d0 - d3) / d3 * 100
    if chg_mid > 3: mid = "上涨"
    elif chg_mid < -3: mid = "下跌"
    else: mid = "停顿"
    
    # 小周期(5日趋势)
    chg_small = (d0 - d2) / d2 * 100
    if chg_small > 2: small = "上涨"
    elif chg_small < -2: small = "下跌"
    else: small = "停顿"
    
    score_map = {"上涨": 1, "下跌": -1, "停顿": 0}
    total = score_map[big]*3 + score_map[mid]*2 + score_map[small]*1
    
    return {"big": big, "mid": mid, "small": small, "score": total,
            "chg_big": round(chg_big,1), "chg_mid": round(chg_mid,1), "chg_small": round(chg_small,1),
            "close": d0}

# 扫描全市场
scan_results = []
for code, prices in all_daily.items():
    if not code[:2] in ["00","30","60","68"]:
        continue
    result = quick_score(prices)
    if result and result["score"] >= 5:
        name_row = stk[stk["ts_code"]==code] if not stk.empty else pd.DataFrame()
        nm = name_row["name"].iloc[0] if not name_row.empty else "?"
        ind = name_row["industry"].iloc[0] if not name_row.empty else "?"
        if "ST" in nm or "退" in nm:
            continue
        result["code"] = code
        result["name"] = nm
        result["industry"] = ind
        scan_results.append(result)

scan_results.sort(key=lambda x: (x["score"], x["chg_mid"]), reverse=True)

print(f"\n得分>=5的强势股: {len(scan_results)}只")
print(f"\n{'股票':<20} {'行业':<8} {'收盘':>7} {'大':>4} {'中':>4} {'小':>4} {'分':>3} {'20日%':>6} {'10日%':>6} {'5日%':>6}")
print("-" * 95)
for r in scan_results[:50]:
    print(f"{r['name']}({r['code'][:6]}) {r['industry']:<8} {r['close']:>7.2f} "
          f"{r['big']:>4} {r['mid']:>4} {r['small']:>4} {r['score']:>+3d} "
          f"{r['chg_big']:>+6.1f} {r['chg_mid']:>+6.1f} {r['chg_small']:>+6.1f}")

# 合并基本面数据（PE/市值）
if scan_results:
    time.sleep(1)
    bas = ts("daily_basic", {"trade_date":"20260409"}, "ts_code,pe_ttm,pb,total_mv")
    if not bas.empty:
        bas_dict = dict(zip(bas["ts_code"], zip(bas["pe_ttm"], bas["pb"], bas["total_mv"])))
        
        print(f"\n\n{'=' * 100}")
        print(f"得分>=5 且 PE>0 且 市值>50亿 的精选（{len(scan_results)}只中筛选）")
        print(f"{'=' * 100}")
        filtered = []
        for r in scan_results:
            if r["code"] in bas_dict:
                pe, pb, mv = bas_dict[r["code"]]
                if pe and pe > 0 and pe < 200 and mv and mv > 500000:
                    r["pe"] = pe
                    r["mv"] = mv/10000
                    filtered.append(r)
        
        filtered.sort(key=lambda x: x["score"], reverse=True)
        print(f"\n精选: {len(filtered)}只")
        print(f"\n{'股票':<20} {'行业':<8} {'收盘':>7} {'分':>3} {'PE':>7} {'市值亿':>7} {'20日%':>6} {'10日%':>6} {'5日%':>6}")
        print("-" * 100)
        for r in filtered[:30]:
            print(f"{r['name']}({r['code'][:6]}) {r['industry']:<8} {r['close']:>7.2f} "
                  f"{r['score']:>+3d} {r['pe']:>7.1f} {r['mv']:>7.0f} "
                  f"{r['chg_big']:>+6.1f} {r['chg_mid']:>+6.1f} {r['chg_small']:>+6.1f}")

print("\n完成！")
