#!/usr/bin/env python3
"""
416作战计划 vs 最新选股系统对比 v2.0（修正版）
=========================
修正内容：
1. 用交易日历动态获取真实T-5/T-10/T-20日期（替代硬编码离散日期）
2. 统一行业分类映射（AKShare涨停池行业→Tushare行业双向映射）
3. 扩大主线识别窗口从3天到7天
4. 放宽粗筛条件，避免横盘收敛标的被错误过滤
"""
import requests, time, json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from market_sentiment import get_market_sentiment, print_sentiment_summary, match_industry, get_trade_dates, get_t_minus_n

# ===== 配置 =====
ANALYSIS_DATE = '20260415'  # 分析日期

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0: return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

print("="*80)
print("416作战计划 vs 最新选股系统对比 v2.0（修正版）")
print("数据截止：4/15收盘")
print("="*80)

# ===== 416计划标的 =====
plan_picks = {
    "600572": {"name":"康恩贝", "position":"1成", "signal":"BBW=0.126", "priority":"⭐⭐⭐"},
    "002294": {"name":"信立泰", "position":"1.5成", "signal":"WR-2+Mistery-M2", "priority":"⭐⭐⭐"},
    "002229": {"name":"鸿博股份", "position":"1成", "signal":"WR-2+涨停后回调低吸", "priority":"⭐⭐⭐"},
    "001208": {"name":"华菱线缆", "position":"0.5成", "signal":"WR-2+BBW=0.151", "priority":"⭐⭐"},
    "601179": {"name":"中国西电", "position":"1成", "signal":"WR-2+特高压", "priority":"⭐⭐"},
    "000963": {"name":"华东医药", "position":"1.5成", "signal":"BBW=0.106最低+等确认", "priority":"⭐⭐"},
    "603127": {"name":"昭衍新药", "position":"1成", "signal":"CXO龙头+医药防守", "priority":"⭐"},
    "600267": {"name":"海正药业", "position":"0.5成", "signal":"首板补充", "priority":"⭐"},
    "002589": {"name":"瑞康医药", "position":"0.5成", "signal":"首板补充", "priority":"⭐"},
}

# ===== Step 0: 交易日历（使用统一模块）=====
print("\n[Step 0] 获取交易日历")
trade_dates_all = get_trade_dates("20260201", ANALYSIS_DATE)
print(f"  2/1-4/15共{len(trade_dates_all)}个交易日")

BASE_DATE = ANALYSIS_DATE
T0 = BASE_DATE
T5 = get_t_minus_n(trade_dates_all, T0, 5)
T10 = get_t_minus_n(trade_dates_all, T0, 10)
T20 = get_t_minus_n(trade_dates_all, T0, 20)
T30 = get_t_minus_n(trade_dates_all, T0, 30)

print(f"  T0={T0} T-5={T5} T-10={T10} T-20={T20} T-30={T30}")

# ===== Step 1: 基础市场数据（使用统一情绪模块）=====
print(f"\n[Step 1] 基础市场数据 ({ANALYSIS_DATE})")
sentiment = get_market_sentiment(ANALYSIS_DATE)
print_sentiment_summary(sentiment)

# 从统一模块提取变量
zt = sentiment['zt_df']
zt_cnt = sentiment['zt_cnt']
zb_cnt = sentiment['zb_cnt']
dt_cnt = sentiment['dt_cnt']
fbl = sentiment['fbl']
ak_ind_zt_cnt = sentiment['ind_zt_dict']
print(f"  全口径: 涨停{sentiment['zt_cnt_all']}(含ST{sentiment['st_zt_cnt']}) 跌停{sentiment['dt_cnt_all']}(含ST{sentiment['st_dt_cnt']})")

# ===== Step 2: 股票基础信息 + 行业映射 =====
print("\n[Step 2] 股票基础信息 + 行业映射")
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
stk = stk[~stk["name"].str.contains("ST|退", na=False)]
ind_map = dict(zip(stk["ts_code"], stk["industry"]))
name_map = dict(zip(stk["ts_code"], stk["name"]))
print(f"  股票池: {len(stk)}只")
time.sleep(1)

# 【修正2】构建AKShare行业→Tushare行业的映射表
# 【使用统一模块的行业映射】
def get_ind_zt_count(ts_industry):
    """根据Tushare行业名获取AKShare涨停池中对应的涨停数"""
    return match_industry(ts_industry, ak_ind_zt_cnt)

print(f"  行业映射: 使用统一模块 market_sentiment.match_industry")
# 验证映射
for name, code6 in [("康恩贝","600572"),("华菱线缆","001208"),("海正药业","600267")]:
    ts_code = code6 + ".SH" if code6.startswith("6") else code6 + ".SZ"
    ts_ind = ind_map.get(ts_code, "?")
    zt_cnt_mapped = get_ind_zt_count(ts_ind)
    print(f"  验证: {name} Tushare={ts_ind} → 涨停数={zt_cnt_mapped}")

# ===== Step 3: 价格数据（用真实交易日） =====
print("\n[Step 3] 价格数据（真实交易日）")

# 获取T-30到T0的所有交易日数据
base_idx = trade_dates_all.index(ANALYSIS_DATE) if ANALYSIS_DATE in trade_dates_all else len(trade_dates_all) - 1
kline_dates = trade_dates_all[max(0, base_idx-30):base_idx+1]
daily_data = {}
for d in kline_dates:
    df = ts("daily", {"trade_date":d}, "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")
    time.sleep(1)
    daily_data[d] = df
    if d in [T0, T5, T10, T20]:
        print(f"  {d}(关键日): {len(df)}只")
    elif len(daily_data) % 5 == 0:
        print(f"  已获取{len(daily_data)}天数据...")

# 基本面
time.sleep(1)
bas = ts("daily_basic", {"trade_date":"20260415"}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio,circ_mv")
bas_d = {row["ts_code"]: row.to_dict() for _, row in bas.iterrows()} if not bas.empty else {}
print(f"  基本面: {len(bas)}只")

# 资金流向
time.sleep(1)
mf = ts("moneyflow", {"trade_date":"20260415"}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
mf_d = {}
if not mf.empty:
    for _, row in mf.iterrows():
        mf_d[row["ts_code"]] = row["buy_elg_amount"]+row["buy_lg_amount"]-row["sell_elg_amount"]-row["sell_lg_amount"]
print(f"  资金: {len(mf_d)}只")

# ===== Step 4: 构建价格矩阵（真实T-N日期） =====
print("\n[Step 4] 构建价格矩阵（真实T-N日期）")
# 用真实的T-5/T-10/T-20日期
key_dates = [T0, T5, T10, T20, T30]
cp = {}
for d in key_dates:
    df = daily_data.get(d, pd.DataFrame())
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in cp: cp[row["ts_code"]] = {}
            cp[row["ts_code"]][d] = row["close"]

print(f"  关键日期: T0={T0} T-5={T5} T-10={T10} T-20={T20} T-30={T30}")
print(f"  价格矩阵: {len(cp)}只股票")

# ===== Step 5: 主线识别（扩大到7天窗口） =====
print("\n[Step 5] 主线识别 (近7个交易日)")
# 【修正3】用最近7个交易日而非3天
mainline_window = trade_dates_all[max(0, base_idx-6):base_idx+1]
print(f"  窗口: {mainline_window[0]} ~ {mainline_window[-1]} ({len(mainline_window)}天)")

ind_perf = {}
for d in mainline_window:
    df = daily_data.get(d, pd.DataFrame())
    if df.empty: continue
    df = df.copy()
    df["ind"] = df["ts_code"].map(ind_map)
    grp = df.groupby("ind").agg(avg=("pct_chg","mean"), lim=("pct_chg", lambda x:(x>=9.5).sum())).reset_index()
    grp["rk"] = grp["avg"].rank(ascending=False)
    for _, row in grp.iterrows():
        if row["ind"] not in ind_perf: ind_perf[row["ind"]] = []
        ind_perf[row["ind"]].append({"date":d, "avg":row["avg"], "rk":int(row["rk"]), "lim":int(row["lim"])})

mainline_scores = {}
n_days = len(mainline_window)
for ind, perfs in ind_perf.items():
    avg_chg = np.mean([p["avg"] for p in perfs])
    avg_rk = np.mean([p["rk"] for p in perfs])
    total_lim = sum(p["lim"] for p in perfs)
    top20_days = sum(1 for p in perfs if p["rk"]<=20)
    
    score = 0
    # 排名分（根据窗口大小调整）
    if avg_rk <= 10: score += 8
    elif avg_rk <= 20: score += 5
    elif avg_rk <= 30: score += 3
    elif avg_rk <= 50: score += 1
    
    # 持续性分（7天窗口，要求更高的一致性）
    if top20_days >= 5: score += 6
    elif top20_days >= 3: score += 4
    elif top20_days >= 2: score += 3
    elif top20_days >= 1: score += 1
    
    # 涨停数分
    if total_lim >= 30: score += 6
    elif total_lim >= 15: score += 4
    elif total_lim >= 8: score += 3
    elif total_lim >= 3: score += 1
    
    mainline_scores[ind] = min(score, 20)

top_ind = sorted(mainline_scores.items(), key=lambda x:x[1], reverse=True)[:15]
print(f"  主线行业TOP15:")
for ind, sc in top_ind:
    print(f"    {ind}: {sc}分")

# ===== Step 6: 全市场评分 =====
print("\n[Step 6] 全市场120分制评分")

# 【修正4】放宽粗筛条件
rough = set()
rough_reason = {}  # 记录每只股票的粗筛数据
for code, p in cp.items():
    # 确保有T0和至少T-5的数据
    if T0 not in p: continue
    
    c_t0 = p.get(T0)
    c_t5 = p.get(T5)
    c_t10 = p.get(T10)
    c_t20 = p.get(T20)
    
    if c_t0 is None: continue
    
    # 计算真实涨幅（有数据就算，没数据给0）
    r5 = (c_t0 - c_t5) / c_t5 * 100 if c_t5 else 0
    r10 = (c_t0 - c_t10) / c_t10 * 100 if c_t10 else 0
    r20 = (c_t0 - c_t20) / c_t20 * 100 if c_t20 else 0
    
    # 多周期共振
    big = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
    mid = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
    small = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
    ps = big * 3 + mid * 2 + small * 1
    
    b = bas_d.get(code, {})
    mv = b.get("total_mv")
    if not mv or mv < 200000: continue  # 市值>20亿
    
    # 排除暴涨
    if r5 > 25 or r10 > 35: continue
    
    # 【修正4】放宽粗筛：ps>=1即可进入候选（原来是>=3）
    # 这样横盘收敛标的（如华东医药 ps=1）也能进入
    if ps >= 1:
        rough.add(code)
        rough_reason[code] = {"r5": r5, "r10": r10, "r20": r20, "ps": ps}
    
    # 额外：416计划标的强制进入候选池（不受粗筛限制）
    code6 = code[:6]
    if code6 in plan_picks and code not in rough:
        rough.add(code)
        rough_reason[code] = {"r5": r5, "r10": r10, "r20": r20, "ps": ps, "forced": True}

print(f"  粗筛候选: {len(rough)}只 (放宽至ps>=1)")

# 验证416标的是否全部进入
for code6, info in plan_picks.items():
    ts_sh = code6 + ".SH"
    ts_sz = code6 + ".SZ"
    in_rough = ts_sh in rough or ts_sz in rough
    reason = rough_reason.get(ts_sh) or rough_reason.get(ts_sz, {})
    status = "✅进入" if in_rough else "❌未进入"
    forced = " (强制)" if reason.get("forced") else ""
    print(f"  {info['name']}({code6}): {status}{forced} | "
          f"r5={reason.get('r5',0):+.1f}% r10={reason.get('r10',0):+.1f}% r20={reason.get('r20',0):+.1f}% ps={reason.get('ps',0)}")

# 构建K线数据
print("  构建K线数据...")
kline_data = {}
for d, df in daily_data.items():
    if df.empty: continue
    for _, row in df.iterrows():
        if row["ts_code"] in rough:
            if row["ts_code"] not in kline_data: kline_data[row["ts_code"]] = []
            kline_data[row["ts_code"]].append({
                "ts_code": row["ts_code"], "trade_date": d,
                "open": row.get("open", 0), "high": row.get("high", 0),
                "low": row.get("low", 0), "close": float(row["close"]),
                "pct_chg": float(row.get("pct_chg", 0)), "vol": float(row.get("vol", 0))
            })
print(f"  K线覆盖: {len(kline_data)}只 (每只约{len(kline_dates)}天)")

# ===== 综合评分 =====
results = []
for code in rough:
    nm = name_map.get(code, "?")
    ind = ind_map.get(code, "?")
    b = bas_d.get(code, {})
    pe = b.get("pe_ttm"); mv = b.get("total_mv", 0)/10000; tr = b.get("turnover_rate_f", 0)
    vr = b.get("volume_ratio", 0)
    nb = mf_d.get(code, 0); nb_yi = nb/10000
    circ_mv = b.get("circ_mv", 0)
    circ_mv_yi = circ_mv/10000 if circ_mv else mv
    
    p = cp.get(code, {})
    c_t0 = p.get(T0)
    c_t5 = p.get(T5)
    c_t10 = p.get(T10)
    c_t20 = p.get(T20)
    
    if c_t0 is None: continue
    c0 = c_t0
    r5 = (c_t0 - c_t5) / c_t5 * 100 if c_t5 else 0
    r10 = (c_t0 - c_t10) / c_t10 * 100 if c_t10 else 0
    r20 = (c_t0 - c_t20) / c_t20 * 100 if c_t20 else 0
    
    # 维度1: 多周期共振 (15分)
    big = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
    mid = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
    small = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
    period_raw = big * 3 + mid * 2 + small * 1
    d1 = min({6:15, 5:12, 4:9, 3:6}.get(period_raw, 3 if period_raw >= 1 else 0), 15)
    
    # 维度2: 主线热点 (20分)
    d2 = mainline_scores.get(ind, 0)
    
    # 维度3: 三Skill (35分)
    klines = kline_data.get(code, [])
    d3 = 0
    if klines:
        kdf = pd.DataFrame(klines)
        if "trade_date" in kdf.columns:
            kdf = kdf.drop_duplicates(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
            n = len(kdf)
            if n >= 10:
                cc = kdf["close"].astype(float).values
                hh = kdf["high"].astype(float).values
                ll = kdf["low"].astype(float).values
                oo = kdf["open"].astype(float).values
                vv = kdf["vol"].astype(float).values
                
                # Mistery (15分)
                ma5 = pd.Series(cc).rolling(5).mean().iloc[-1]
                ma10 = pd.Series(cc).rolling(10).mean().iloc[-1]
                ma20 = pd.Series(cc).rolling(min(20, n)).mean().iloc[-1]
                ema12 = pd.Series(cc).ewm(span=12, adjust=False).mean()
                ema26 = pd.Series(cc).ewm(span=min(26, n), adjust=False).mean()
                dif = (ema12 - ema26).iloc[-1]
                dea = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]
                
                mistery = 0
                if cc[-1] > ma5 > ma10 > ma20: mistery += 5
                elif cc[-1] > ma5 > ma10: mistery += 4
                elif cc[-1] > ma20: mistery += 3
                elif cc[-1] > ma5: mistery += 2
                
                ma5s = pd.Series(cc).rolling(5).mean()
                ma20s = pd.Series(cc).rolling(min(20, n)).mean()
                if n >= 7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5] <= ma20s.iloc[-5] and ma5 > ma20:
                    mistery += 3
                elif ma5 > ma20: mistery += 2
                below = any(cc[i] < ma5s.iloc[i] for i in range(max(0, n-5), n-1) if not np.isnan(ma5s.iloc[i]))
                if below and cc[-1] > ma5: mistery += 2
                
                vol5 = np.mean(vv[-5:]); vol10 = np.mean(vv[-min(10, n):])
                if vol5 / vol10 > 1.3: mistery += 2
                elif vol5 / vol10 > 1: mistery += 1
                if dif > dea: mistery += 2
                elif dif > 0: mistery += 1
                mistery = min(mistery, 15)
                
                # TDS (10分)
                tds = 0
                peaks, troughs = [], []
                for i in range(3, n-3):
                    if hh[i] > max(hh[max(0, i-3):i]) and hh[i] > max(hh[i+1:min(n, i+4)]): peaks.append(hh[i])
                    if ll[i] < min(ll[max(0, i-3):i]) and ll[i] < min(ll[i+1:min(n, i+4)]): troughs.append(ll[i])
                if len(peaks) >= 2 and len(troughs) >= 2:
                    if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]: tds += 4
                    elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]: tds += 2
                if n >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]: tds += 2
                if peaks and cc[-1] > peaks[-1]: tds += 2
                if n >= 2 and cc[-1] > oo[-1] and cc[-1] > hh[-2] and cc[-2] < oo[-2]: tds += 2
                tds = min(tds, 10)
                
                # 元子元 (10分)
                yuanzi = 0
                pct_last = kdf["pct_chg"].astype(float).iloc[-1] if "pct_chg" in kdf.columns else 0
                if r5 > 10: yuanzi += 3
                elif r5 > 5: yuanzi += 4
                elif r5 > 0: yuanzi += 3
                if pct_last > 0: yuanzi += 3
                elif pct_last > -1: yuanzi += 2
                if mainline_scores.get(ind, 0) >= 10: yuanzi += 3
                elif mainline_scores.get(ind, 0) >= 5: yuanzi += 2
                yuanzi = min(yuanzi, 10)
                
                d3 = mistery + tds + yuanzi
    
    # 维度4: 安全边际 (15分)
    d4 = 0
    if abs(r5) <= 5: d4 += 5
    elif abs(r5) <= 10: d4 += 3
    elif abs(r5) <= 15: d4 += 1
    if abs(r10) <= 10: d4 += 5
    elif abs(r10) <= 15: d4 += 3
    elif abs(r10) <= 20: d4 += 1
    if tr and tr <= 5: d4 += 5
    elif tr and tr <= 10: d4 += 3
    elif tr and tr <= 15: d4 += 1
    d4 = min(d4, 15)
    
    # 维度5: 基本面 (15分)
    d5 = 0
    if pe and pe > 0:
        if pe <= 15: d5 += 6
        elif pe <= 25: d5 += 5
        elif pe <= 40: d5 += 4
        elif pe <= 60: d5 += 3
        elif pe <= 100: d5 += 1
    if 100 <= mv <= 500: d5 += 3
    elif 500 < mv <= 2000: d5 += 2
    elif mv > 2000: d5 += 1
    elif 50 <= mv < 100: d5 += 2
    if nb_yi > 1: d5 += 6
    elif nb_yi > 0.3: d5 += 4
    elif nb_yi > 0: d5 += 2
    d5 = min(d5, 15)
    
    total = d1 + d2 + d3 + d4 + d5
    
    results.append({
        "code": code, "name": nm, "ind": ind, "close": c0,
        "pe": pe, "mv": mv, "circ_mv": circ_mv_yi, "tr": tr, "vr": vr, "nb_yi": nb_yi,
        "r5": r5, "r10": r10, "r20": r20,
        "d1": d1, "d2": d2, "d3": d3, "d4": d4, "d5": d5,
        "total": total,
        "in_plan": code[:6] in plan_picks
    })

results.sort(key=lambda x: x["total"], reverse=True)

# ===== 输出TOP30 =====
print(f"\n{'='*180}")
print(f"综合评分TOP30 (满分100 = 多周期15 + 主线20 + 三Skill35 + 安全边际15 + 基本面15)")
print(f"{'='*160}")
print(f"{'#':>2} {'股票':<18} {'行业':<8} {'收盘':>7} {'PE':>5} {'市值':>5} {'5日%':>5} {'10日%':>6} "
      f"{'净流亿':>6} {'多周期':>4} {'主线':>4} {'三Skill':>5} {'安全':>4} {'基本面':>4} {'总分':>4} {'级别':<6} {'416计划'}")
print("-"*160)

for i, r in enumerate(results[:30], 1):
    level = "⭐强推" if r["total"] >= 90 else ("✅推荐" if r["total"] >= 78 else ("👀关注" if r["total"] >= 66 else "  "))
    pe_str = f"{r['pe']:.0f}" if r['pe'] and r['pe'] > 0 else "N/A"
    plan_mark = ""
    code6 = r['code'][:6]
    if code6 in plan_picks:
        pp = plan_picks[code6]
        plan_mark = f"✅{pp['priority']}{pp['position']}"
    print(f"{i:>2d} {r['name']}({r['code'][:6]}) {r['ind']:<8} {r['close']:>7.2f} {pe_str:>5} "
          f"{r['mv']:>5.0f} {r['r5']:>+5.1f} {r['r10']:>+6.1f} {r['nb_yi']:>+6.2f} "
          f"{r['d1']:>4d} {r['d2']:>4d} {r['d3']:>5d} {r['d4']:>4d} {r['d5']:>4d} {r['total']:>4d} "
          f"{level:<6} {plan_mark}")

# ===== 416计划标的在评分中的排名 =====
print(f"\n{'='*120}")
print("416计划标的在最新评分中的排名")
print(f"{'='*120}")

plan_results = []
for code6, info in plan_picks.items():
    found = None
    for r in results:
        if r["code"][:6] == code6:
            found = r
            break
    
    if found:
        rank = results.index(found) + 1
        plan_results.append({**found, "rank": rank, "plan_info": info})
        level = "⭐强推" if found["total"] >= 90 else ("✅推荐" if found["total"] >= 78 else ("👀关注" if found["total"] >= 66 else "❌未入"))
        print(f"  {info['priority']} {info['name']}({code6}) | 评分:{found['total']}/100 | 排名:#{rank}/{len(results)} | "
              f"级别:{level} | 计划仓位:{info['position']} | "
              f"多周期{found['d1']} 主线{found['d2']} 三Skill{found['d3']} 安全{found['d4']} 基本面{found['d5']}")
    else:
        print(f"  {info['priority']} {info['name']}({code6}) | ❌未进入候选池")
        plan_results.append({"name": info["name"], "code": code6, "rank": "N/A", "total": 0, "plan_info": info})

# ===== 差距分析 =====
print(f"\n{'='*120}")
print("416计划 vs 系统推荐 差距分析")
print(f"{'='*120}")

sys_top10 = results[:10]
plan_codes = set(plan_picks.keys())
new_picks = [r for r in sys_top10 if r["code"][:6] not in plan_codes]
overlap = [r for r in sys_top10 if r["code"][:6] in plan_codes]

print(f"\n📊 系统TOP10中与416计划重合: {len(overlap)}只")
for r in overlap:
    pp = plan_picks[r["code"][:6]]
    print(f"  ✅ {r['name']}({r['code'][:6]}) 评分{r['total']} 排名#{results.index(r)+1} | 计划:{pp['priority']}{pp['position']}")

print(f"\n🆕 系统TOP10中416计划未覆盖: {len(new_picks)}只")
for r in new_picks:
    level = "⭐强推" if r["total"] >= 90 else ("✅推荐" if r["total"] >= 78 else "👀关注")
    print(f"  🆕 {r['name']}({r['code'][:6]}) {r['ind']} 评分{r['total']} {level} | "
          f"5日{r['r5']:+.1f}% 净流{r['nb_yi']:+.2f}亿")

low_score_picks = [r for r in plan_results if isinstance(r.get("rank"), int) and r["total"] < 66]
if low_score_picks:
    print(f"\n⚠️ 416计划中评分<66(未达关注线)的标的:")
    for r in low_score_picks:
        pp = r["plan_info"]
        print(f"  ⚠️ {pp['name']}({r['code'][:6]}) 评分{r['total']} 排名#{r['rank']} | 计划:{pp['priority']}{pp['position']}")

no_entry = [r for r in plan_results if r.get("rank") == "N/A"]
if no_entry:
    print(f"\n❌ 416计划中未进入候选池的标的:")
    for r in no_entry:
        pp = r["plan_info"]
        print(f"  ❌ {pp['name']}({r['code']}) | 计划:{pp['priority']}{pp['position']}")

# ===== 修正说明 =====
print(f"\n{'='*120}")
print("v2.0 修正说明")
print(f"{'='*120}")
print(f"  1. 涨幅计算: 用交易日历动态获取真实T-5({T5})/T-10({T10})/T-20({T20})日期")
print(f"     旧版用硬编码离散日期(0415vs0413=2天当5天, 0415vs0409=4天当10天)")
print(f"  2. 行业映射: 建立AKShare↔Tushare双向映射表({len(ak_to_ts_ind)}组)")
print(f"     旧版Tushare行业直接匹配AKShare涨停池，分类标准不同导致匹配失败")
print(f"  3. 主线识别: 窗口从3天扩大到7天({mainline_window[0]}~{mainline_window[-1]})")
print(f"     旧版只看3天容易被短期波动误导")
print(f"  4. 粗筛条件: ps>=1即可进入(旧版ps>=3)，416标的强制进入")
print(f"     旧版华东医药(ps=1)被错误过滤")

# ===== 总结 =====
print(f"\n{'='*120}")
print("总结")
print(f"{'='*120}")

plan_in_sys = sum(1 for r in plan_results if isinstance(r.get("rank"), int))
plan_in_top30 = sum(1 for r in plan_results if isinstance(r.get("rank"), int) and r["rank"] <= 30)
plan_in_top10 = sum(1 for r in plan_results if isinstance(r.get("rank"), int) and r["rank"] <= 10)
plan_strong = sum(1 for r in plan_results if isinstance(r.get("total"), (int, float)) and r["total"] >= 90)
plan_good = sum(1 for r in plan_results if isinstance(r.get("total"), (int, float)) and 78 <= r["total"] < 90)
plan_watch = sum(1 for r in plan_results if isinstance(r.get("total"), (int, float)) and 66 <= r["total"] < 78)

sys_strong = sum(1 for r in results if r["total"] >= 90)
sys_good = sum(1 for r in results if 78 <= r["total"] < 90)
sys_watch = sum(1 for r in results if 66 <= r["total"] < 78)

print(f"\n416计划: {len(plan_picks)}只标的")
print(f"  进入系统候选: {plan_in_sys}/{len(plan_picks)}")
print(f"  进入系统TOP30: {plan_in_top30}/{len(plan_picks)}")
print(f"  进入系统TOP10: {plan_in_top10}/{len(plan_picks)}")
print(f"  ⭐强推(≥90): {plan_strong}只 | ✅推荐(≥78): {plan_good}只 | 👀关注(≥66): {plan_watch}只")

print(f"\n系统全市场:")
print(f"  总候选: {len(results)}只")
print(f"  ⭐强推(≥90): {sys_strong}只 | ✅推荐(≥78): {sys_good}只 | 👀关注(≥66): {sys_watch}只")


# 保存结果
output = {
    "date": "20260415",
    "version": "v2.0_fixed",
    "fixes": [
        f"涨幅用真实交易日: T-5={T5} T-10={T10} T-20={T20}",
        f"行业映射: AK↔TS {len(ak_to_ts_ind)}组",
        f"主线窗口: 7天({mainline_window[0]}~{mainline_window[-1]})",
        "粗筛: ps>=1 + 416标的强制进入"
    ],
    "market": {"zt": zt_cnt, "zb": zb_cnt, "dt": dt_cnt, "fbl": round(fbl, 0)},
    "plan_picks_ranking": [{
        "code": r.get("code", ""),
        "name": r.get("name", ""),
        "rank": r.get("rank", "N/A"),
        "total": r.get("total", 0),
        "d1": r.get("d1", 0), "d2": r.get("d2", 0), "d3": r.get("d3", 0),
        "d4": r.get("d4", 0), "d5": r.get("d5", 0),
        "r5": round(r.get("r5", 0), 2), "r10": round(r.get("r10", 0), 2),
        "plan_position": r.get("plan_info", {}).get("position", ""),
        "plan_priority": r.get("plan_info", {}).get("priority", ""),
    } for r in plan_results],
    "system_top20": [{
        "code": r["code"], "name": r["name"], "ind": r["ind"],
        "total": r["total"], "d1": r["d1"], "d2": r["d2"], "d3": r["d3"],
        "d4": r["d4"], "d5": r["d5"],
        "r5": round(r["r5"], 2), "r10": round(r["r10"], 2),
        "in_plan": r.get("in_plan", False)
    } for r in results[:20]]
}
with open("/Users/ecustkiller/WorkBuddy/Claw/compare_416_result.json", "w") as f:
    json.dump(output, f, ensure_ascii=False, indent=2, default=str)
print("\n已保存 compare_416_result.json")
