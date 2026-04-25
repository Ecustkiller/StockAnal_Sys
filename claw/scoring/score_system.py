#!/usr/bin/env python3
"""
A股综合评分选股系统 v3.3（BCI板块完整性整合 + 风险扣分制 + 完整9 Skill对齐）
=========================
整合九大维度，135分制加权评分 - 风险扣分：

1. 多周期共振 (15分) — 大(±3)+中(±2)+小(±1)三周期独立判定
2. 主线热点   (25分) — 是否在当前市场主线行业 + 板块持续性判断 + ★BCI板块完整性加权
3. Mistery    (20分) — M1趋势+M2买点(520/破五反五/BBW)+M3卖点扣分+M4量价+M5形态+M6仓位管理
4. TDS信号    (10分) — 波峰波谷(±5窗口)+T1推进+T2吞没+T3突破+T5反转
5. 元子元情绪  (10分) — 个股6阶段情绪判定+量价关系+连板接力
6. 安全边际   (15分) — BIAS偏离度 + 近期涨幅可控
7. 基本面     (15分) — PE估值 + 主力资金流向
8. 百胜WR     (15分) — WR-1首板放量7条件(含封板时间) + WR-2右侧起爆5条件 + WR-3底倍量柱4条件（取高分）
10. 风险扣分  (0~-30分) — 超涨/板块弱(★BCI加权)/净流出/市值超标 → 扣分而非排除
   + 保护因子 (0~+15分) — 趋势多头/连板连阳/龙头地位/★BCI高分板块 → 抵消风险扣分

最终得分 = 维度1~9总分(满分150) - 风险扣分 + 保护因子加分
>=110分为强推，>=90分为推荐，>=75分为关注

v3.3核心改进（BCI板块完整性整合）：
- ★ 新增BCI板块完整性指数计算（梯队层次+龙头强度+封板率+持续性斜率+换手板比例）
- ★ 维度2主线热点：BCI得分加权替代简单涨停数计数（BCI≥60额外+3分，≥40额外+1分）
- ★ 风险扣分7b：BCI≥50的板块即使涨停数<3也不重扣（BCI高=板块有梯队有龙头）
- ★ 保护因子新增P7：BCI≥70的板块额外+2分保护

v3.2核心改进（全面提升覆盖率）：
- Mistery新增M6仓位管理(5分)，维度从15→20分
- TXCG新增六大模型量化(连板竞争/分歧策略/反包修复/承接/上影大长腿/唯一性)
- TDS新增T4三K反转信号检测
- 主线热点新增板块持续性判断(连板确认+涨停家数趋势)
- 满分从135→150分，评分阈值相应上调

v3.1改进（对齐SOP全流程）：
- 新增维度9：百胜WR独立检测（WR-1完整6条件+WR-2完整5条件）
- Mistery补充M2买点(520金叉/破五反五)+M3卖点扣分(放量滞涨/3天不创新高)+M5形态
- TDS补充T5反转信号(锤子线/底部反转)+波峰波谷窗口扩大到±5
- 元子元补充个股6阶段情绪判定+量价关系+爆量见顶检测
- 多周期改为大(±3)+中(±2)+小(±1)独立判定
"""
import requests, time
import os, sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0: return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

# ===== 交易日历自动推算 =====
def get_trade_dates(target_date=None):
    """
    自动获取交易日历，推算所有需要的日期。
    参数：target_date — 目标日期(YYYYMMDD字符串)，默认为最近一个交易日。
    返回：dict，包含 target/dates3/dates5/extra_kline_dates 等。
    """
    # 获取近3个月的交易日历（足够覆盖T-20）
    end_dt = target_date or datetime.now().strftime("%Y%m%d")
    # 往前推3个月作为起始日期
    start_dt = (datetime.strptime(end_dt, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
    cal = ts("trade_cal", {"exchange":"SSE", "start_date":start_dt, "end_date":end_dt, "is_open":"1"}, "cal_date")
    if cal.empty:
        raise RuntimeError(f"无法获取交易日历(start={start_dt}, end={end_dt})")
    
    # 按日期降序排列
    all_dates = sorted(cal["cal_date"].tolist(), reverse=True)
    
    # 确定目标日期
    if target_date and target_date in all_dates:
        T = target_date
    else:
        # 如果指定日期不是交易日，或未指定，取最近交易日
        T = all_dates[0]
        if target_date and target_date != T:
            print(f"  ⚠️ {target_date}非交易日，自动回退到最近交易日: {T}")
    
    idx = all_dates.index(T)
    
    # dates3: 最近3个交易日 [T, T-1, T-2]
    dates3 = all_dates[idx:idx+3]
    
    # dates5: 多周期采样 [T, T-2, T-5, T-10, T-20]
    dates5 = [all_dates[idx+i] for i in [0, 2, 5, 10, 20] if idx+i < len(all_dates)]
    
    # extra_kline_dates: T-1到T-10的交易日（补充K线数据用）
    extra_kline_dates = all_dates[idx+1:idx+11]
    
    return {
        "target": T,
        "dates3": dates3,
        "dates5": dates5,
        "extra_kline_dates": extra_kline_dates,
        "all_dates": all_dates,  # 完整交易日历备用
    }

# 解析命令行参数：python3 score_system.py [YYYYMMDD]
TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else None

print("="*80)
print("A股综合评分选股系统 v3.3（BCI板块完整性整合 + 150分制）")
print("="*80)

# ===== 交易日历 =====
print("\n[交易日历]")
trade_dates = get_trade_dates(TARGET_DATE)
TARGET = trade_dates["target"]
print(f"  目标日期: {TARGET}")
print(f"  dates3(近3日): {trade_dates['dates3']}")
print(f"  dates5(多周期): {trade_dates['dates5']}")
print(f"  extra_kline(补充K线): {trade_dates['extra_kline_dates'][:5]}...共{len(trade_dates['extra_kline_dates'])}天")

# ===== 数据采集 =====
print("\n[数据采集]")

# 行业映射
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
stk = stk[~stk["name"].str.contains("ST|退", na=False)]
ind_map = dict(zip(stk["ts_code"], stk["industry"]))
name_map = dict(zip(stk["ts_code"], stk["name"]))
print(f"  股票: {len(stk)}只")
time.sleep(1)

# 近3日全市场（自动推算）
dates3 = trade_dates["dates3"]
daily3 = {}
for d in dates3:
    df = ts("daily", {"trade_date":d}, "ts_code,pct_chg,amount,open,high,low,close,vol")
    time.sleep(1)
    daily3[d] = df
    print(f"  {d}: {len(df)}只")

# 5个日期收盘价(多周期，自动推算)
dates5 = trade_dates["dates5"]
cp = {}
for d in dates5:
    if d in daily3:
        df = daily3.get(d, pd.DataFrame())
    else:
        df = ts("daily", {"trade_date":d}, "ts_code,close")
        time.sleep(1)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in cp: cp[row["ts_code"]] = {}
            cp[row["ts_code"]][d] = row["close"]
    print(f"  价格{d}: {len(df)}只")

# 基本面（使用目标日期）
time.sleep(1)
bas = ts("daily_basic", {"trade_date":TARGET}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio")
bas_d = {row["ts_code"]: row.to_dict() for _, row in bas.iterrows()} if not bas.empty else {}
print(f"  基本面: {len(bas)}只")

# 资金流向（使用目标日期）
time.sleep(1)
mf = ts("moneyflow", {"trade_date":TARGET}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
mf_d = {}
if not mf.empty:
    for _, row in mf.iterrows():
        mf_d[row["ts_code"]] = row["buy_elg_amount"]+row["buy_lg_amount"]-row["sell_elg_amount"]-row["sell_lg_amount"]
print(f"  资金: {len(mf_d)}只")

# ===== 主线识别 =====
print("\n[主线识别]")
ind_perf = {}
for d, df in daily3.items():
    df["ind"] = df["ts_code"].map(ind_map)
    grp = df.groupby("ind").agg(avg=("pct_chg","mean"), lim=("pct_chg", lambda x:(x>=9.5).sum())).reset_index()
    grp["rk"] = grp["avg"].rank(ascending=False)
    for _, row in grp.iterrows():
        if row["ind"] not in ind_perf: ind_perf[row["ind"]] = []
        ind_perf[row["ind"]].append({"date":d, "avg":row["avg"], "rk":int(row["rk"]), "lim":int(row["lim"])})

# 主线得分：近3日平均涨幅排名+涨停数+一致性
mainline_scores = {}
for ind, perfs in ind_perf.items():
    avg_chg = np.mean([p["avg"] for p in perfs])
    avg_rk = np.mean([p["rk"] for p in perfs])
    total_lim = sum(p["lim"] for p in perfs)
    top20_days = sum(1 for p in perfs if p["rk"]<=20)
    
    # 主线热度分(0~20)
    score = 0
    if avg_rk <= 10: score += 8
    elif avg_rk <= 20: score += 5
    elif avg_rk <= 30: score += 3
    elif avg_rk <= 50: score += 1
    
    if top20_days >= 3: score += 6  # 连续3天TOP20
    elif top20_days >= 2: score += 4
    elif top20_days >= 1: score += 2
    
    if total_lim >= 20: score += 6  # 涨停数量多
    elif total_lim >= 10: score += 4
    elif total_lim >= 5: score += 3
    elif total_lim >= 2: score += 1
    
    # ★ 板块持续性判断（v3.2新增，防一日游）
    # 规则1：涨停家数趋势（升温/降温/一日游）
    lim_list = [p["lim"] for p in sorted(perfs, key=lambda x:x["date"])]
    if len(lim_list) >= 2:
        # 最新一天 vs 前一天
        latest_lim = lim_list[-1]
        prev_lim = lim_list[-2]
        if latest_lim >= prev_lim * 1.5 and latest_lim >= 5:
            score += 3  # 强升温：涨停数增加50%+
        elif latest_lim >= prev_lim and latest_lim >= 3:
            score += 2  # 持续：涨停数不减少
        elif latest_lim < prev_lim * 0.5 and prev_lim >= 5:
            score -= 3  # 一日游信号：涨停数骤降50%+
        elif latest_lim < prev_lim:
            score -= 1  # 降温
    
    # 规则2：首日爆发检测（昨天0今天10+→待确认，半仓试探）
    if len(lim_list) >= 2 and lim_list[-2] == 0 and lim_list[-1] >= 10:
        score -= 2  # 首日爆发扣分（可能一日游，需要第二天确认）
    
    # 规则3：连续3天有涨停=持续主线
    if len(lim_list) >= 3 and all(l >= 1 for l in lim_list[-3:]):
        score += 2  # 连续3天有涨停=持续主线确认
    
    mainline_scores[ind] = max(min(score, 25), 0)  # 上限提高到25（含持续性加分）

top_ind = sorted(mainline_scores.items(), key=lambda x:x[1], reverse=True)[:15]
print(f"  主线行业TOP15:")
for ind, sc in top_ind:
    print(f"    {ind}: {sc}分")

# 全市场成交额（用于动态市值阈值）
total_market_amount = 0
if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
    total_market_amount = daily3[dates3[0]]["amount"].sum() / 100000  # 转亿元
print(f"  全市场成交额: {total_market_amount:.0f}亿")

# 动态市值上限：成交额>2万亿→500亿，>1.5万亿→300亿，否则150亿
if total_market_amount > 20000:
    dynamic_mv_cap = 500
elif total_market_amount > 15000:
    dynamic_mv_cap = 300
else:
    dynamic_mv_cap = 150
print(f"  动态市值上限: {dynamic_mv_cap}亿（基于成交额）")

# ===== 逐股评分 =====
print(f"\n[逐股评分]")

# 需要K线做三Skill的股票，先粗筛候选
rough_candidates = set()
for code, p in cp.items():
    ds = sorted(p.keys(), reverse=True)
    if len(ds)<5: continue
    vals = [p[ds[i]] for i in range(5)]
    r5 = (vals[0]-vals[2])/vals[2]*100
    r10 = (vals[0]-vals[3])/vals[3]*100
    r20 = (vals[0]-vals[4])/vals[4]*100
    big = 1 if r20>5 else (-1 if r20<-5 else 0)
    mid = 1 if r10>3 else (-1 if r10<-3 else 0)
    small = 1 if r5>2 else (-1 if r5<-2 else 0)
    ps = big*3+mid*2+small*1
    
    if ps < 4: continue  # 多周期>=4才进候选
    
    b = bas_d.get(code, {})
    pe = b.get("pe_ttm"); mv = b.get("total_mv")
    if not mv or mv < 300000: continue  # >30亿
    # v3.0: 不再硬排除超涨，只排除极端情况（5日>40%或10日>50%）
    if r5 > 40 or r10 > 50: continue  # 仅排除极端暴涨（原为r5>20 or r10>30）
    
    rough_candidates.add(code)

print(f"  粗筛候选: {len(rough_candidates)}只")

# 批量获取K线（分批，每批约50只）
kline_data = {}
rough_list = list(rough_candidates)

# 用全市场日K数据代替逐只查询（更高效）
# 已有daily3中的3天数据，再补2天
for extra_d in trade_dates["extra_kline_dates"]:
    df = ts("daily", {"trade_date":extra_d}, "ts_code,trade_date,open,high,low,close,pct_chg,vol")
    time.sleep(1)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] in rough_candidates:
                if row["ts_code"] not in kline_data:
                    kline_data[row["ts_code"]] = []
                kline_data[row["ts_code"]].append(row.to_dict())
    print(f"  K线{extra_d}: {len(df)}只")

# 补上已有3天数据
for d, df in daily3.items():
    for _, row in df.iterrows():
        if row["ts_code"] in rough_candidates:
            if row["ts_code"] not in kline_data:
                kline_data[row["ts_code"]] = []
            kline_data[row["ts_code"]].append({
                "ts_code":row["ts_code"], "trade_date":d,
                "open":row.get("open",0), "high":row.get("high",0),
                "low":row.get("low",0), "close":float(row["close"]),
                "pct_chg":float(row.get("pct_chg",0)), "vol":float(row.get("vol",0))
            })

print(f"  K线覆盖: {len(kline_data)}只")

# ===== 60分钟K线预加载（用于WR-3底倍量柱检测） =====
LOCAL_60M_DIR = os.path.expanduser('~/Downloads/2026/60min')
kline_60m_data = {}  # {ts_code: {closes, highs, lows, vols}}

import os as _os
if _os.path.exists(LOCAL_60M_DIR):
    print(f"  60分钟K线(WR-3)...", end="", flush=True)
    wr3_loaded = 0
    for code in rough_candidates:
        code6 = code[:6]
        prefix = 'sh' if code.endswith('.SH') else ('sz' if code.endswith('.SZ') else 'bj')
        csv_file = _os.path.join(LOCAL_60M_DIR, f"{prefix}{code6}.csv")
        if _os.path.exists(csv_file):
            try:
                df_60 = pd.read_csv(csv_file, encoding='utf-8')
                df_60.columns = ['date','time','open','high','low','close','volume','amount']
                df_60 = df_60.tail(30)  # 最近30根60分钟K线
                if len(df_60) >= 12:
                    kline_60m_data[code] = {
                        'closes': df_60['close'].astype(float).tolist(),
                        'highs': df_60['high'].astype(float).tolist(),
                        'lows': df_60['low'].astype(float).tolist(),
                        'vols': df_60['volume'].astype(float).tolist(),
                    }
                    wr3_loaded += 1
            except:
                pass
    print(f" {wr3_loaded}/{len(rough_candidates)}只 ⚡")
else:
    print(f"  ⚠ 60分钟K线目录不存在({LOCAL_60M_DIR})，WR-3将跳过")

# ===== 涨停股封板时间检测（用Ashare 5分钟K线，WR-1条件7） =====
zt_time_data = {}  # {ts_code: "HH:MM"}
# 找出涨停股
zt_codes = []
if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
    d0_zt = daily3[dates3[0]]
    zt_codes = d0_zt[d0_zt["pct_chg"] >= 9.5]["ts_code"].tolist()
    zt_codes = [c for c in zt_codes if c in rough_candidates]

if zt_codes:
    try:
        sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
        import warnings; warnings.filterwarnings('ignore')
        from Ashare import get_price as get_price_zt
        print(f"  ⏱ 封板时间({len(zt_codes)}只涨停)...", end="", flush=True)
        t_zt = time.time()
        zt_detected = 0
        # 获取涨停股的收盘价用于判断涨停价
        d0_close = {}
        for _, row in daily3[dates3[0]].iterrows():
            if row["ts_code"] in zt_codes:
                d0_close[row["ts_code"]] = float(row["close"])
        
        for code in zt_codes:
            try:
                ashare_code = ('sh' if code.endswith('.SH') else 'sz') + code[:6]
                df_5m = get_price_zt(ashare_code, frequency='5m', count=48)
                if df_5m is not None and len(df_5m) >= 5:
                    zt_price = d0_close.get(code, 0)
                    if zt_price <= 0: continue
                    for idx_row in range(len(df_5m)):
                        row_high = float(df_5m.iloc[idx_row]['high'])
                        if abs(row_high - zt_price) / zt_price < 0.002:
                            ts_idx = df_5m.index[idx_row]
                            time_str = str(ts_idx)
                            if ' ' in time_str:
                                hm = time_str.split(' ')[1][:5]
                            else:
                                hm = time_str[11:16] if len(time_str) > 16 else time_str[:5]
                            zt_time_data[code] = hm
                            zt_detected += 1
                            break
            except:
                pass
            time.sleep(0.1)
        print(f" {zt_detected}/{len(zt_codes)}只 ({time.time()-t_zt:.1f}s)")
    except ImportError:
        print(f"  ⚠ Ashare不可用，跳过封板时间检测")

# ===== 综合评分 =====
print(f"\n[综合评分]")

# 预计算：全市场涨停数据（用于板块效应判定）
ind_zt_map = {}  # 行业→涨停数
ind_zt_stocks = {}  # 行业→涨停股列表（用于BCI计算）
if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
    d0_df = daily3[dates3[0]].copy()
    d0_df["ind"] = d0_df["ts_code"].map(ind_map)
    for ind_name, grp in d0_df.groupby("ind"):
        zt_stocks_grp = grp[grp["pct_chg"] >= 9.5]
        zt_count = len(zt_stocks_grp)
        ind_zt_map[ind_name] = zt_count
        if zt_count > 0:
            ind_zt_stocks[ind_name] = zt_stocks_grp.to_dict('records')

# ===== BCI板块完整性指数计算（v3.3新增） =====
# 为每个行业计算BCI得分(0-100)，综合梯队层次+龙头强度+封板率+持续性+换手板比例
print("\n[BCI板块完整性计算]")
ind_bci_map = {}  # 行业→BCI得分(0-100)

# 预计算：近3日每个行业的涨停数（用于持续性斜率）
ind_zt_daily = {}  # {行业: [day1_count, day2_count, day3_count]}
for d_idx, d in enumerate(dates3):
    df_d = daily3.get(d, pd.DataFrame())
    if df_d.empty: continue
    df_d_copy = df_d.copy()
    df_d_copy["ind"] = df_d_copy["ts_code"].map(ind_map)
    for ind_name, grp in df_d_copy.groupby("ind"):
        if ind_name not in ind_zt_daily:
            ind_zt_daily[ind_name] = [0] * len(dates3)
        ind_zt_daily[ind_name][d_idx] = int((grp["pct_chg"] >= 9.5).sum())

# 预计算：炸板数据（涨幅在7-9.5%之间且当日最高价触及涨停价附近的近似统计）
ind_zb_map = {}  # 行业→炸板数（近似）
if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
    d0_all = daily3[dates3[0]].copy()
    d0_all["ind"] = d0_all["ts_code"].map(ind_map)
    # 近似炸板：涨幅5-9.5%且最高价接近涨停价（high/close_prev >= 1.095）
    for ind_name, grp in d0_all.groupby("ind"):
        zb_approx = len(grp[(grp["pct_chg"] >= 5) & (grp["pct_chg"] < 9.5)])
        ind_zb_map[ind_name] = max(0, zb_approx // 3)  # 粗略估计1/3为真炸板

for ind_name, zt_count in ind_zt_map.items():
    if zt_count == 0:
        ind_bci_map[ind_name] = 0
        continue
    
    bci = 0
    zt_list = ind_zt_stocks.get(ind_name, [])
    n = len(zt_list)
    
    # --- BCI-1: 涨停数量(0-20) ---
    if n >= 8: bci += 20
    elif n >= 5: bci += 17
    elif n >= 3: bci += 13
    elif n >= 2: bci += 8
    else: bci += 3
    
    # --- BCI-2: 梯队层次(0-20) ---
    # 用涨幅分布模拟梯队：涨停(9.5%+)=当日首板，连续涨停需要前一天数据
    # 简化：用前一天也涨停的股票数来估算连板数
    d1_zt_codes = set()
    if len(dates3) >= 2:
        d1_df = daily3.get(dates3[1], pd.DataFrame())
        if not d1_df.empty:
            d1_zt_codes = set(d1_df[d1_df["pct_chg"] >= 9.5]["ts_code"].tolist())
    
    连板数 = 0
    首板数 = 0
    for s in zt_list:
        if s.get("ts_code", "") in d1_zt_codes:
            连板数 += 1
        else:
            首板数 += 1
    
    层级数 = (1 if 首板数 > 0 else 0) + (1 if 连板数 > 0 else 0)
    最高板估计 = 2 if 连板数 > 0 else 1
    
    # 检查3连板（前2天都涨停）
    if len(dates3) >= 3:
        d2_df = daily3.get(dates3[2], pd.DataFrame())
        if not d2_df.empty:
            d2_zt_codes = set(d2_df[d2_df["pct_chg"] >= 9.5]["ts_code"].tolist())
            三连板 = sum(1 for s in zt_list if s.get("ts_code", "") in d1_zt_codes and s.get("ts_code", "") in d2_zt_codes)
            if 三连板 > 0:
                层级数 = min(层级数 + 1, 3)
                最高板估计 = 3
    
    s2_bci = min(层级数 * 5, 12) + min(最高板估计 * 2, 8)
    s2_bci = min(s2_bci, 20)
    bci += s2_bci
    
    # --- BCI-3: 龙头强度(0-15) ---
    # 用成交额最大的涨停股作为龙头代理
    max_amount = max((s.get("amount", 0) for s in zt_list), default=0)
    if max_amount > 500000:  # >50亿成交额
        bci += 15
    elif max_amount > 200000:  # >20亿
        bci += 12
    elif max_amount > 100000:  # >10亿
        bci += 9
    elif max_amount > 50000:  # >5亿
        bci += 6
    else:
        bci += 3
    
    # --- BCI-4: 封板率(0-10) ---
    zb_count = ind_zb_map.get(ind_name, 0)
    total_try = n + zb_count
    封板率 = n / total_try if total_try > 0 else 1
    if zb_count == 0:
        bci += 10
    elif 封板率 > 0.8:
        bci += 8
    elif 封板率 > 0.6:
        bci += 5
    elif 封板率 > 0.4:
        bci += 3
    else:
        bci += 1
    
    # --- BCI-5: 持续性斜率(0-10) ---
    day_counts = ind_zt_daily.get(ind_name, [0, 0, 0])
    持续天数 = sum(1 for c in day_counts if c > 0)
    # 斜率：最新vs最早
    有效天 = [(i, c) for i, c in enumerate(day_counts) if c > 0]
    if len(有效天) >= 2:
        斜率 = (有效天[-1][1] - 有效天[0][1]) / max(有效天[-1][0] - 有效天[0][0], 1)
    else:
        斜率 = 0
    
    s5_bci = 0
    if 持续天数 >= 3: s5_bci = 6
    elif 持续天数 == 2: s5_bci = 3
    else: s5_bci = 1
    if 斜率 > 1: s5_bci += 4  # 强升温
    elif 斜率 > 0: s5_bci += 3  # 升温
    elif 斜率 == 0: s5_bci += 1  # 持平
    s5_bci = min(s5_bci, 10)
    bci += s5_bci
    
    # --- BCI-6: 换手板比例(0-10) ---
    # 用换手率>8%的涨停股占比来估算
    换手板数 = 0
    for s in zt_list:
        code_s = s.get("ts_code", "")
        tr_s = bas_d.get(code_s, {}).get("turnover_rate_f", 0)
        if tr_s and tr_s > 8:
            换手板数 += 1
    换手比 = 换手板数 / n if n > 0 else 0
    bci += min(int(换手比 * 10), 10)
    
    # --- BCI-7: 板块内聚度(0-15) ---
    # 同行业涨停股之间的涨跌幅相关性（简化：用涨幅标准差的倒数）
    pct_list = [s.get("pct_chg", 0) for s in zt_list]
    if len(pct_list) >= 2:
        pct_std = np.std(pct_list)
        if pct_std < 1:  # 涨幅高度一致
            bci += 15
        elif pct_std < 2:
            bci += 10
        elif pct_std < 3:
            bci += 7
        else:
            bci += 3
    else:
        bci += 5  # 单只涨停
    
    ind_bci_map[ind_name] = min(bci, 100)

# 输出BCI TOP15
bci_top = sorted(ind_bci_map.items(), key=lambda x: x[1], reverse=True)[:15]
print(f"  BCI板块完整性TOP15:")
for ind_name, bci_val in bci_top:
    zt_n = ind_zt_map.get(ind_name, 0)
    评级 = '⭐5' if bci_val >= 80 else ('⭐4' if bci_val >= 60 else ('⭐3' if bci_val >= 40 else '⭐2'))
    print(f"    {ind_name}: BCI={bci_val} {评级} (涨停{zt_n}家)")

results = []

for code in rough_candidates:
    nm = name_map.get(code, "?")
    ind = ind_map.get(code, "?")
    b = bas_d.get(code, {})
    pe = b.get("pe_ttm")
    mv = b.get("total_mv", 0)/10000
    tr = b.get("turnover_rate_f", 0)
    nb = mf_d.get(code, 0)
    
    p = cp.get(code, {})
    ds = sorted(p.keys(), reverse=True)
    if len(ds)<5: continue
    vals = [p[ds[i]] for i in range(5)]
    c0 = vals[0]
    r5=(vals[0]-vals[2])/vals[2]*100
    r10=(vals[0]-vals[3])/vals[3]*100
    r20=(vals[0]-vals[4])/vals[4]*100
    
    # ====== 维度1: 多周期共振 (15分) — 大(±3)+中(±2)+小(±1) ======
    # 大周期(日线/MA20): ±3
    big = 0
    if r20 > 10: big = 3
    elif r20 > 5: big = 2
    elif r20 > 0: big = 1
    elif r20 > -5: big = 0
    elif r20 > -10: big = -1
    else: big = -3
    
    # 中周期(5日/MA10): ±2
    mid = 0
    if r10 > 5: mid = 2
    elif r10 > 2: mid = 1
    elif r10 > -2: mid = 0
    elif r10 > -5: mid = -1
    else: mid = -2
    
    # 小周期(2日/MA5): ±1
    small = 0
    if r5 > 3: small = 1
    elif r5 > 0: small = 0
    elif r5 > -2: small = 0
    else: small = -1
    
    period_raw = big + mid + small  # -6~+6
    
    # 映射到0-15分
    d1_score = int((period_raw + 6) / 12 * 15 + 0.5)
    d1_score = max(0, min(15, d1_score))
    
    # ====== 维度2: 主线热点 (25分，含板块持续性 + BCI加权) ======
    d2_base = mainline_scores.get(ind, 0)
    # v3.3: BCI板块完整性加权（BCI高=板块梯队完整+龙头强+封板率高→额外加分）
    ind_bci = ind_bci_map.get(ind, 0)
    if ind_bci >= 70: d2_base += 3  # BCI≥70=极完整板块
    elif ind_bci >= 50: d2_base += 2  # BCI≥50=较完整板块
    elif ind_bci >= 30: d2_base += 1  # BCI≥30=一般板块
    d2_score = min(d2_base, 25)
    
    # ====== 维度3: 三Skill (35分) ======
    # 需要K线数据
    klines = kline_data.get(code, [])
    d3_score = 0
    mistery = 0
    tds = 0
    is_ma_bull = False  # 用于保护因子判定
    consecutive_yang = 0  # 连阳天数
    
    if klines:
        kdf = pd.DataFrame(klines)
        if "trade_date" in kdf.columns:
            kdf.sort_values("trade_date", inplace=True)
            kdf.reset_index(drop=True, inplace=True)
            kdf = kdf.drop_duplicates(subset=["trade_date"]).copy()
            kdf.sort_values("trade_date", inplace=True)
            kdf.reset_index(drop=True, inplace=True)
            n = len(kdf)
            
            if n >= 10:
                cc = kdf["close"].astype(float).values
                hh = kdf["high"].astype(float).values
                ll = kdf["low"].astype(float).values
                oo = kdf["open"].astype(float).values
                vv = kdf["vol"].astype(float).values
                
                # --- Mistery (15分) — M1趋势+M2买点+M3卖点扣分+M4量价+M5形态 ---
                ma5 = pd.Series(cc).rolling(5).mean().iloc[-1]
                ma10 = pd.Series(cc).rolling(10).mean().iloc[-1]
                ma20 = pd.Series(cc).rolling(min(20,n)).mean().iloc[-1]
                
                ema12 = pd.Series(cc).ewm(span=12,adjust=False).mean()
                ema26 = pd.Series(cc).ewm(span=min(26,n),adjust=False).mean()
                dif = (ema12-ema26).iloc[-1]
                dea = (ema12-ema26).ewm(span=9,adjust=False).mean().iloc[-1]
                
                # 判定均线多头（用于保护因子）
                is_ma_bull = cc[-1]>ma5>ma10>ma20
                
                # 计算连阳天数（用于保护因子）
                for k in range(n-1, -1, -1):
                    if cc[k] > oo[k]:
                        consecutive_yang += 1
                    else:
                        break
                
                # M1趋势(0-3): 均线多头
                if cc[-1]>ma5>ma10>ma20: mistery += 3
                elif cc[-1]>ma5>ma10: mistery += 2
                elif cc[-1]>ma20: mistery += 1
                
                # M2买点(0-3): 520金叉/破五反五/BBW起爆
                ma5s = pd.Series(cc).rolling(5).mean()
                ma20s = pd.Series(cc).rolling(min(20,n)).mean()
                m2 = 0
                if n>=7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5]<=ma20s.iloc[-5] and ma5>ma20:
                    m2 += 2  # 520刚金叉
                # 破五反五
                if n>=5:
                    below = any(cc[i]<ma5s.iloc[i] for i in range(max(0,n-5),n-1) if not np.isnan(ma5s.iloc[i]))
                    if below and cc[-1]>ma5: m2 += 1
                # BBW收缩起爆（WR-2核心信号）
                std20 = np.std(cc[-min(20,n):])
                bbw_val = (4*std20)/ma20 if ma20>0 else 0
                pct_last = kdf["pct_chg"].astype(float).iloc[-1] if "pct_chg" in kdf.columns else 0
                if bbw_val < 0.12 and pct_last > 3: m2 += 2
                elif bbw_val < 0.15 and pct_last > 3: m2 += 1
                mistery += min(3, m2)
                
                # M3卖点扣分(-2~0): 放量滞涨/3天不创新高
                vol5 = np.mean(vv[-5:]); vol10 = np.mean(vv[-min(10,n):])
                if pct_last < 2 and vol5/vol10 > 2.5:
                    mistery -= 1  # 放量滞涨
                if n >= 4 and hh[-1] < max(hh[-4:-1]) and pct_last < 1:
                    mistery -= 1  # 3天不创新高
                
                # M4量价(0-3): 放量+MACD
                if vol5/vol10 > 1.3 and pct_last > 0: mistery += 2  # 量价齐升
                elif vol5/vol10 > 1: mistery += 1
                if dif > dea: mistery += 1  # MACD多头
                
                # M5形态(0-3): 空中加油/仙人指路
                if n >= 3:
                    prev_range = abs(cc[-2]-cc[-3])/cc[-3]*100 if cc[-3]>0 else 0
                    if pct_last > 5 and vol5/vol10 > 1.5 and prev_range < 2:
                        mistery += 2  # 空中加油
                    if n >= 2:
                        prev_upper = (hh[-2]-cc[-2])/cc[-2]*100 if cc[-2]>0 else 0
                        if prev_upper > 3 and cc[-1] > hh[-2]*0.98:
                            mistery += 1  # 仙人指路收复
                
                # M6仓位管理(0-5)：金字塔加仓条件+半仓滚动适配性
                m6 = 0
                # 金字塔加仓条件：趋势确认+突破关键位
                if cc[-1]>ma5>ma10>ma20 and pct_last > 0:
                    m6 += 2  # 趋势确认可加仓
                elif cc[-1]>ma5>ma10:
                    m6 += 1  # 短期多头可试探
                # 半仓滚动适配性：有明确支撑压力位
                if ma20 > 0 and abs(cc[-1] - ma20) / ma20 < 0.08:
                    m6 += 1  # 靠近MA20=有明确支撑/压力位可做T
                # 止损纪律检测：跌破支撑位的风险
                if n >= 20:
                    support_20 = min(ll[-20:])
                    if cc[-1] > support_20 * 1.05:
                        m6 += 1  # 距离支撑位还有空间=止损可控
                # 时间止损检测：5-7天无明显上涨→减仓信号
                if n >= 7:
                    chg_7d = (cc[-1] - cc[-7]) / cc[-7] * 100 if cc[-7] > 0 else 0
                    if chg_7d > 5:
                        m6 += 1  # 7天内有明显上涨=趋势健康
                    elif chg_7d < -3:
                        m6 -= 1  # 7天下跌=时间止损信号
                mistery += min(5, max(0, m6))
                
                mistery = max(min(mistery, 20), 0)
                
                # --- TDS (10分) — 波峰波谷(±5窗口)+T1推进+T2吞没+T3突破+T5反转 ---
                # 波峰波谷（窗口扩大到±5）
                peaks, troughs = [], []
                win = min(5, n//3)  # 窗口大小，至少5
                for i in range(win, n-win):
                    if hh[i]>=max(hh[max(0,i-win):i]) and hh[i]>=max(hh[i+1:min(n,i+win+1)]):
                        peaks.append(hh[i])
                    if ll[i]<=min(ll[max(0,i-win):i]) and ll[i]<=min(ll[i+1:min(n,i+win+1)]):
                        troughs.append(ll[i])
                
                # 趋势(0-3)
                if len(peaks)>=2 and len(troughs)>=2:
                    if peaks[-1]>peaks[-2] and troughs[-1]>troughs[-2]:
                        tds += 3  # 上行趋势（峰谷抬高）
                    elif peaks[-1]>peaks[-2] or troughs[-1]>troughs[-2]:
                        tds += 2  # 转折
                elif len(peaks)>=2 and peaks[-1]>peaks[-2]:
                    tds += 2
                
                # T1推进(0-2): 今日高低点均高于昨日
                if n>=2 and hh[-1]>hh[-2] and ll[-1]>ll[-2]:
                    tds += 2
                elif n>=2 and hh[-1]>hh[-2]:
                    tds += 1
                
                # T2吞没(0-1): 阳线吞没前一根阴线
                if n>=3 and cc[-2]<oo[-2] and cc[-1]>oo[-1] and cc[-1]>hh[-2]:
                    tds += 1
                
                # T3突破(0-2): 突破前波峰/前高
                if peaks and cc[-1]>peaks[-1]:
                    tds += 2
                elif n>=2 and pct_last >= 9.5:
                    tds += 1  # 涨停=突破信号
                
                # T5反转(0-2): 底部反转信号（锤子线/深跌后大涨）
                if r5 < -10 and pct_last > 3:
                    tds += 2  # 深跌后大涨=底部反转
                elif r5 < -5 and pct_last > 0:
                    # 锤子线检测：下影线>实体2倍
                    body = abs(cc[-1]-oo[-1])
                    lower_shadow = min(cc[-1],oo[-1]) - ll[-1]
                    if lower_shadow > body * 2 and lower_shadow > 0:
                        tds += 1  # 锤子线
                
                # T6双向突破(0-2): 趋势方向连续两次改变后的突破信号（疯极派TDS模型6）
                if len(peaks) >= 3 and len(troughs) >= 3:
                    # 检测双向突破：先跌破波谷→再突破波峰（或反之）
                    # 简化：最近3个波峰/波谷中，方向发生了两次改变
                    p_trend1 = 1 if peaks[-2] > peaks[-3] else -1
                    p_trend2 = 1 if peaks[-1] > peaks[-2] else -1
                    t_trend1 = 1 if troughs[-2] > troughs[-3] else -1
                    t_trend2 = 1 if troughs[-1] > troughs[-2] else -1
                    # 双向突破=趋势方向改变了两次
                    if p_trend1 != p_trend2 or t_trend1 != t_trend2:
                        if cc[-1] > peaks[-1]:  # 突破最新波峰=看涨双向突破
                            tds += 2
                        elif pct_last > 5:  # 大涨=可能的突破信号
                            tds += 1
                
                tds = min(tds, 12)  # TDS上限提高到12
                
                # --- 元子元情绪 (10分) — 个股6阶段情绪判定+量价关系 ---
                yuanzi = 0
                pct_last = kdf["pct_chg"].astype(float).iloc[-1] if "pct_chg" in kdf.columns else 0
                is_zt_flag = pct_last >= 9.5
                vol_ratio_val = vol5/vol10 if vol10 > 0 else 1
                
                # 个股情绪阶段判定（6阶段）
                if is_zt_flag and r5 < -5:
                    yuanzi += 5  # 冰点启动：超跌涨停=最佳买点
                elif is_zt_flag and r5 < 5:
                    yuanzi += 4  # 发酵确认：低位涨停
                elif pct_last >= 5 and r5 < 0:
                    yuanzi += 4  # 冰点启动：超跌大涨
                elif is_zt_flag and 5 <= r5 < 15:
                    yuanzi += 3  # 主升加速：涨停
                elif pct_last >= 5 and 0 <= r5 < 10:
                    yuanzi += 3  # 发酵确认：大阳安全
                elif is_zt_flag and r5 >= 15:
                    # 高位涨停：检查是否爆量见顶
                    if vol_ratio_val >= 3:
                        yuanzi += 0  # 爆量高位涨停=高潮见顶
                    else:
                        yuanzi += 2  # 高位涨停但未爆量
                elif pct_last > 0 and r5 < 10:
                    yuanzi += 2  # 发酵/分歧换手
                elif pct_last < -3 and r5 > 15:
                    yuanzi += 0  # 退潮补跌
                elif pct_last <= 0:
                    if r5 < -10:
                        yuanzi += 2  # 深跌待启动
                    else:
                        yuanzi += 1  # 分歧换手
                
                # 量价关系加分
                if pct_last > 3 and vol_ratio_val < 1.2:
                    yuanzi += 2  # 缩量上涨=筹码锁定好
                elif pct_last < 2 and vol_ratio_val > 2.5:
                    yuanzi -= 1  # 放量滞涨=见顶信号
                
                # 在主线行业中额外加分（情绪共振）
                if mainline_scores.get(ind, 0) >= 10: yuanzi += 2
                elif mainline_scores.get(ind, 0) >= 5: yuanzi += 1
                
                yuanzi = max(min(yuanzi, 10), 0)
                
                # --- TXCG六大模型量化加分(0-5) ---
                txcg_model = 0
                ind_zt_count_local = ind_zt_map.get(ind, 0)
                
                # 模型1：连板竞争（涨停+板块内有竞争=晋级机会）
                if is_zt_flag and ind_zt_count_local >= 3:
                    # 板块内有多只涨停=连板竞争激烈，龙头有晋级预期
                    txcg_model += 1
                
                # 模型2：分歧期策略（超跌轮动/新方向首板）
                if r5 < -5 and pct_last > 3:
                    txcg_model += 1  # 超跌人气轮动
                
                # 模型3：反包修复（前一天大阴线+今天反包）
                if n >= 3:
                    prev_chg = (cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                    if prev_chg < -3 and pct_last > 2:
                        txcg_model += 1  # 反包信号
                
                # 模型4：承接战法（分时走承接=弱势中资金找突破口）
                # 简化：均线附近+小涨=承接
                if ma5 > 0 and abs(cc[-1] - ma5) / ma5 < 0.02 and pct_last > 0:
                    txcg_model += 1  # 均线附近承接
                
                # 模型5：上影线/大长腿（长下影线=资金回流）
                if n >= 2:
                    body_prev = abs(cc[-2] - oo[-2])
                    lower_shadow_prev = min(cc[-2], oo[-2]) - ll[-2]
                    if lower_shadow_prev > body_prev * 2 and lower_shadow_prev > 0 and pct_last > 0:
                        txcg_model += 1  # 大长腿后续涨=资金回流
                
                # 模型6：唯一性（板块内唯一涨停=辨识度最高）
                if is_zt_flag and ind_zt_count_local == 1:
                    txcg_model += 1  # 唯一涨停=资金如水聚焦
                
                txcg_model = min(txcg_model, 5)
                
                # --- TDS T4三K反转信号(0-2) ---
                t4_signal = 0
                if n >= 4:
                    # 三K反转条件：逆趋势+中间K不是最长+后两根形成吞没
                    k1_body = abs(cc[-3] - oo[-3])
                    k2_body = abs(cc[-2] - oo[-2])
                    k3_body = abs(cc[-1] - oo[-1])
                    # 下跌后的看涨三K反转
                    if r5 < -3:  # 逆趋势（下跌中找反转）
                        if k2_body <= max(k1_body, k3_body):  # 中间K不是最长
                            if cc[-1] > oo[-1] and cc[-1] > hh[-2]:  # 后两根阳线吞没
                                t4_signal = 2
                                tds += 2
                    # 上涨后的看跌三K反转（扣分）
                    elif r5 > 10:
                        if k2_body <= max(k1_body, k3_body):
                            if cc[-1] < oo[-1] and cc[-1] < ll[-2]:  # 后两根阴线吞没
                                tds -= 1  # 看跌信号扣分
                
                tds = max(min(tds, 12), 0)  # TDS上限提高到12
                
                d3_score = mistery + tds + yuanzi + txcg_model
    
    # ====== 维度4: 安全边际 (15分) ======
    d4_score = 0
    # BIAS20越低越好
    bias_val = (c0 - (cp.get(code,{}).get("20260327",c0))) / max(cp.get(code,{}).get("20260327",c0), 0.01) * 100 if "20260327" in cp.get(code,{}) else r10
    
    if abs(r5) <= 5: d4_score += 5
    elif abs(r5) <= 10: d4_score += 3
    elif abs(r5) <= 15: d4_score += 1
    
    if abs(r10) <= 10: d4_score += 5
    elif abs(r10) <= 15: d4_score += 3
    elif abs(r10) <= 20: d4_score += 1
    
    if tr and tr <= 5: d4_score += 5  # 换手率低=筹码锁定
    elif tr and tr <= 10: d4_score += 3
    elif tr and tr <= 15: d4_score += 1
    
    d4_score = min(d4_score, 15)
    
    # ====== 维度5: 基本面 (15分) ======
    d5_score = 0
    # PE
    if pe and pe > 0:
        if pe <= 15: d5_score += 6
        elif pe <= 25: d5_score += 5
        elif pe <= 40: d5_score += 4
        elif pe <= 60: d5_score += 3
        elif pe <= 100: d5_score += 1
    
    # 市值（100-500亿中盘最佳）
    if 100 <= mv <= 500: d5_score += 3
    elif 500 < mv <= 2000: d5_score += 2
    elif mv > 2000: d5_score += 1
    elif 50 <= mv < 100: d5_score += 2
    
    # 主力资金
    nb_yi = nb/10000  # 转亿
    if nb_yi > 1: d5_score += 6
    elif nb_yi > 0.3: d5_score += 4
    elif nb_yi > 0: d5_score += 2
    
    d5_score = min(d5_score, 15)
    
    # ====== 维度9: 百胜WR (15分) ======
    # WR-1首板放量7条件 + WR-2右侧趋势起爆5条件 + WR-3底倍量柱4条件，取高分
    d9_score = 0
    wr_tags = []
    
    # --- WR-1 首板放量涨停模型(0-7) ---
    wr1 = 0
    vr_wr = bas_d.get(code, {}).get("volume_ratio", 0)
    if is_zt:
        wr1 += 1  # 条件1：涨停
        # 条件2：量比>=3
        if vr_wr and vr_wr >= 3: wr1 += 1
        # 条件3：换手率>=8%
        if tr and tr >= 8: wr1 += 1
        # 条件4：均线多头
        if is_ma_bull: wr1 += 1
        # 条件5：市值30-150亿
        if 30 <= mv <= 150: wr1 += 1
        # 条件6：资金净流入
        if nb_yi > 0: wr1 += 1
        # 条件7：封板时间<=10:30（从5分钟K线检测）
        zt_time = zt_time_data.get(code)  # 格式: "HH:MM" 或 None
        if zt_time:
            if zt_time <= "10:30": wr1 += 1; wr_tags.append(f"封板{zt_time}✅")
            elif zt_time <= "11:30": wr_tags.append(f"封板{zt_time}午前⚠")
            else: wr_tags.append(f"封板{zt_time}偏晚❌")
        if wr1 >= 6: wr_tags.append(f"🔥WR1={wr1}/7")
        elif wr1 >= 5: wr_tags.append(f"WR1={wr1}/7")
    
    # --- WR-2 右侧趋势起爆模型(0-5) ---
    wr2 = 0
    if klines and n >= 10:
        # 条件1：BBW收缩<0.15
        if bbw_val < 0.15: wr2 += 1
        # 条件2：倍量突破（量比>=2.5）
        if vr_wr and vr_wr >= 2.5: wr2 += 1
        elif vol5/vol10 > 2.5: wr2 += 1
        # 条件3：突破形态（涨停或涨>=7%）
        if pct_last >= 9.5: wr2 += 1
        elif pct_last >= 7: wr2 += 1
        # 条件4：均线多头
        if is_ma_bull: wr2 += 1
        # 条件5：突破前高
        if peaks and cc[-1] > peaks[-1]: wr2 += 1
        if wr2 >= 4: wr_tags.append(f"🔥WR2={wr2}/5起爆")
        elif wr2 >= 3: wr_tags.append(f"WR2={wr2}/5")
    
    # --- WR-3 底倍量柱短线模型(0-4)（需60分钟K线） ---
    wr3 = 0
    kline_60m = kline_60m_data.get(code)  # 从预加载的60分钟数据中获取
    if kline_60m and len(kline_60m.get('vols', [])) >= 12:
        vols_60 = kline_60m['vols']
        closes_60 = kline_60m['closes']
        highs_60 = kline_60m['highs']
        lows_60 = kline_60m['lows']
        n60 = len(vols_60)
        
        # 第一步：寻找底倍量柱（低位+成交量≥前一根2倍+阳线）
        first_dbl_idx = None
        for i_60 in range(max(1, n60-20), n60):
            if vols_60[i_60] >= vols_60[i_60-1] * 2 and closes_60[i_60] > closes_60[i_60-1]:
                recent_range = closes_60[max(0, i_60-20):i_60+1]
                mid_price = (max(recent_range) + min(recent_range)) / 2
                if closes_60[i_60] <= mid_price * 1.05:
                    first_dbl_idx = i_60
                    break
        
        if first_dbl_idx is not None:
            wr3 += 1  # 底倍量柱出现
            first_low = lows_60[first_dbl_idx]
            first_high = highs_60[first_dbl_idx]
            
            # 第二步：寻找第二倍量柱确认
            for j_60 in range(first_dbl_idx + 1, n60):
                if vols_60[j_60] >= vols_60[j_60-1] * 2:
                    if closes_60[j_60] > first_high:
                        wr3 += 1  # 二次倍量确认
                    if lows_60[j_60] >= first_low:
                        wr3 += 1  # 支撑不破
                    break
            
            # 第三步：当前价格仍在支撑位上方
            if closes_60[-1] >= first_low:
                wr3 += 1
            
            if wr3 >= 3: wr_tags.append(f"🔥WR3={wr3}/4底倍量")
            elif wr3 >= 2: wr_tags.append(f"WR3={wr3}/4")
    
    # 取三个模型的高分映射到15分
    best_wr = max(wr1, wr2, wr3)
    if best_wr == wr1:
        best_wr_max = 7
    elif best_wr == wr2:
        best_wr_max = 5
    else:
        best_wr_max = 4
    d9_score = int(best_wr / best_wr_max * 15 + 0.5) if best_wr_max > 0 else 0
    d9_score = min(d9_score, 15)
    
    # ====== 维度7: 风险扣分 (0~-30分) ======
    # v3.0核心改进：从"一票否决排除"改为"风险扣分"
    risk_deduct = 0
    risk_tags = []
    
    # --- 7a. 超涨风险 ---
    # 5日涨幅>20%：扣5分（趋势多头时扣3分）
    if r5 > 20:
        if is_ma_bull:
            risk_deduct += 3
            risk_tags.append(f"超涨5日{r5:.0f}%-3")
        else:
            risk_deduct += 5
            risk_tags.append(f"超涨5日{r5:.0f}%-5")
    elif r5 > 15:
        risk_deduct += 2
        risk_tags.append(f"偏涨5日{r5:.0f}%-2")
    
    # 10日涨幅>25%：扣5分（趋势多头时扣3分）
    if r10 > 25:
        if is_ma_bull:
            risk_deduct += 3
            risk_tags.append(f"超涨10日{r10:.0f}%-3")
        else:
            risk_deduct += 5
            risk_tags.append(f"超涨10日{r10:.0f}%-5")
    elif r10 > 20:
        risk_deduct += 2
        risk_tags.append(f"偏涨10日{r10:.0f}%-2")
    
    # 20日涨幅>50%：扣8分（极端超涨，这个可以严格一些）
    if r20 > 50:
        risk_deduct += 8
        risk_tags.append(f"极端超涨20日{r20:.0f}%-8")
    elif r20 > 35:
        risk_deduct += 4
        risk_tags.append(f"超涨20日{r20:.0f}%-4")
    
    # --- 7b. 板块效应不足 (★BCI加权) ---
    # v3.3: 用BCI板块完整性替代简单涨停数判断
    # BCI≥50的板块即使涨停数<3也不重扣（BCI高=板块有梯队有龙头有持续性）
    ind_bci_risk = ind_bci_map.get(ind, 0)
    if ind_zt_count < 3:
        if ind_bci_risk >= 50:
            # BCI≥50=板块完整性较好，轻扣
            risk_deduct += 1
            risk_tags.append(f"行业涨停{ind_zt_count}家但BCI={ind_bci_risk}-1")
        elif mainline_scores.get(ind, 0) >= 8:
            # 虽然行业涨停少，但主线得分高（说明概念/题材层面有效应）
            risk_deduct += 2
            risk_tags.append(f"行业涨停{ind_zt_count}家-2")
        elif ind_zt_count == 0:
            if ind_bci_risk >= 30:
                risk_deduct += 3  # BCI≥30但涨停0=板块有基础但今天没爆发
                risk_tags.append(f"行业涨停0家BCI={ind_bci_risk}-3")
            else:
                risk_deduct += 5
                risk_tags.append(f"行业涨停0家-5")
        else:
            risk_deduct += 3
            risk_tags.append(f"行业涨停{ind_zt_count}家-3")
    
    # --- 7c. 净流出风险 ---
    # v3.0: 净流出仅降权，不排除（涨停板净流出扣1分，阴线净流出扣3分）
    if nb_yi < -2:
        if is_zt:
            risk_deduct += 1  # 涨停板净流出可能是对倒
            risk_tags.append(f"涨停净流出{nb_yi:.1f}亿-1")
        else:
            risk_deduct += 3
            risk_tags.append(f"净流出{nb_yi:.1f}亿-3")
    elif nb_yi < -0.5:
        risk_deduct += 1
        risk_tags.append(f"小幅净流出{nb_yi:.1f}亿-1")
    
    # --- 7d. 市值超标风险 ---
    # v3.0: 动态市值上限，超标扣分而非排除
    if mv > dynamic_mv_cap:
        if mv > 1000:
            risk_deduct += 5
            risk_tags.append(f"市值{mv:.0f}亿-5")
        elif mv > dynamic_mv_cap:
            risk_deduct += 3
            risk_tags.append(f"市值{mv:.0f}亿-3")
    
    # --- 7e. 换手率异常 ---
    if tr and tr > 50:
        risk_deduct += 3
        risk_tags.append(f"高换手{tr:.0f}%-3")
    elif tr and tr > 30:
        risk_deduct += 1
        risk_tags.append(f"换手偏高{tr:.0f}%-1")
    
    risk_deduct = min(risk_deduct, 30)  # 上限30分
    
    # ====== 保护因子 (0~+15分，抵消风险扣分) ======
    protect_bonus = 0
    protect_tags = []
    
    # P1. 趋势多头（MA5>MA10>MA20）：+3分
    if is_ma_bull:
        protect_bonus += 3
        protect_tags.append("趋势多头+3")
    
    # P2. 连阳>=3天：+3分（连续涨停说明资金认可度高）
    if consecutive_yang >= 5:
        protect_bonus += 3
        protect_tags.append(f"连阳{consecutive_yang}天+3")
    elif consecutive_yang >= 3:
        protect_bonus += 2
        protect_tags.append(f"连阳{consecutive_yang}天+2")
    
    # P3. MACD金叉+DIF>0（趋势确认）：+2分
    if klines and d3_score > 0:
        # 已在三Skill中计算过dif/dea
        pass  # dif/dea在局部作用域，这里用mistery得分间接判断
    if mistery >= 12:  # Mistery高分=趋势确认
        protect_bonus += 2
        protect_tags.append("Mistery高分+2")
    
    # P4. 涨停（当日涨停=市场认可）：+3分
    if is_zt:
        protect_bonus += 3
        protect_tags.append("涨停+3")
    
    # P5. 龙头地位（板块内涨幅第一且涨停）：+2分
    # 简化判断：涨停+板块涨停>=3家
    if is_zt and ind_zt_count >= 3:
        protect_bonus += 2
        protect_tags.append("板块龙头+2")
    
    # P6. 主力大单净流入>2亿：+2分
    if nb_yi > 2:
        protect_bonus += 2
        protect_tags.append(f"大单{nb_yi:.1f}亿+2")
    
    # P7. BCI板块完整性高分保护（v3.3新增）
    # BCI≥70=板块梯队完整+龙头强+封板率高+有持续性，这样的板块内的股票应该获得保护
    ind_bci_protect = ind_bci_map.get(ind, 0)
    if ind_bci_protect >= 70:
        protect_bonus += 2
        protect_tags.append(f"BCI={ind_bci_protect}板块完整+2")
    elif ind_bci_protect >= 50:
        protect_bonus += 1
        protect_tags.append(f"BCI={ind_bci_protect}板块较完整+1")
    
    protect_bonus = min(protect_bonus, 15)  # 上隙0分    
    # ====== 最终得分 ======
    raw_total = d1_score + d2_score + d3_score + d4_score + d5_score + d9_score
    # 风险净扣分 = 风险扣分 - 保护因子（但净扣分最低为0，保护因子不能反加分）
    net_risk = max(risk_deduct - protect_bonus, 0)
    total = raw_total - net_risk
    
    results.append({
        "code":code, "name":nm, "ind":ind, "close":c0,
        "pe":pe, "mv":mv, "tr":tr, "nb_yi":nb_yi,
        "r5":r5, "r10":r10, "r20":r20,
        "d1":d1_score, "d2":d2_score, "d3":d3_score, "d4":d4_score, "d5":d5_score,
        "d9":d9_score,
        "wr_tags":"+".join(wr_tags) if wr_tags else "-",
        "risk":risk_deduct, "protect":protect_bonus, "net_risk":net_risk,
        "risk_tags":"|".join(risk_tags) if risk_tags else "-",
        "protect_tags":"|".join(protect_tags) if protect_tags else "-",
        "raw_total":raw_total, "total":total
    })

results.sort(key=lambda x: x["total"], reverse=True)

# ===== 输出 =====
print(f"\n{'='*220}")
print(f"综合评分TOP30 v3.3（BCI板块完整性整合 + 150分制 + 风险扣分制）")
print(f"最终得分 = 九维原始分(满分150) - 风险扣分(0~30) + 保护因子(0~15) | BCI板块完整性加权")
print(f"动态市值上限: {dynamic_mv_cap}亿 | 全市场成交额: {total_market_amount:.0f}亿")
print(f"{'='*220}")
print(f"{'#':>2} {'股票':<18} {'行业':<8} {'收盘':>7} {'PE':>5} {'市值':>5} {'5日%':>5} {'10日%':>6} "
      f"{'净流亿':>6} {'九维':>4} {'风险':>4} {'保护':>4} {'净扣':>4} {'总分':>4} {'WR':>4} {'WR标签':<16} {'级别':<6} {'风险明细'}")
print("-"*220)

for i, r in enumerate(results[:30], 1):
    level = "⭐强推" if r["total"]>=110 else ("✅推荐" if r["total"]>=90 else ("👀关注" if r["total"]>=75 else "  "))
    pe_str = f"{r['pe']:.0f}" if r['pe'] and r['pe']>0 else "N/A"
    wr_tag = r.get('wr_tags', '-')
    risk_detail = r.get('risk_tags', '-')
    protect_detail = r.get('protect_tags', '-')
    # 风险明细：显示扣分项和保护项
    detail_str = risk_detail
    if protect_detail != "-":
        detail_str += " 🛡" + protect_detail
    print(f"{i:>2d} {r['name']}({r['code'][:6]}) {r['ind']:<8} {r['close']:>7.2f} {pe_str:>5} "
          f"{r['mv']:>5.0f} {r['r5']:>+5.1f} {r['r10']:>+6.1f} {r['nb_yi']:>+6.2f} "
          f"{r['raw_total']:>4d} {r['risk']:>4d} {r['protect']:>4d} {r['net_risk']:>4d} {r['total']:>4d} {r.get('d9',0):>4d} {wr_tag:<16} {level:<6} {detail_str}")

# 统计
strong = [r for r in results if r["total"]>=110]
good = [r for r in results if 90<=r["total"]<110]
watch = [r for r in results if 75<=r["total"]<90]

# 百胜WR专项统计
wr_hits = [r for r in results if r.get('wr_tags', '-') != '-']
print(f"\n百胜WR专项: 命中{len(wr_hits)}只")
if wr_hits:
    for r in wr_hits[:10]:
        print(f"  {r['name']}({r['code'][:6]}) WR={r.get('d9',0)}分 {r.get('wr_tags','-')}")

# v3.0新增：风险扣分统计
high_risk = [r for r in results if r["risk"] >= 10]
protected = [r for r in results if r["protect"] >= 5]
print(f"\n风险扣分统计:")
print(f"  高风险(扣≥10分): {len(high_risk)}只")
print(f"  有保护因子(≥5分): {len(protected)}只")
# 显示被扣分最多但仍入选的标的（这些在v2.0中会被排除）
rescued = [r for r in results[:30] if r["risk"] >= 8 and r["total"] >= 75]
if rescued:
    print(f"  🆕 v2.0会被排除但v3.0保留的标的:")
    for r in rescued:
        print(f"    {r['name']}({r['code'][:6]}) 六维{r['raw_total']}分 风险-{r['risk']} 保护+{r['protect']} → 最终{r['total']}分 | {r['risk_tags']} 🛡{r['protect_tags']}")

print(f"\n统计: ⭐强推{len(strong)}只 | ✅推荐{len(good)}只 | 👀关注{len(watch)}只 | 总计{len(results)}只")

# 输出结果JSON
import json
top20 = results[:20]
with open("/Users/ecustkiller/WorkBuddy/Claw/综合评分TOP20.json", "w") as f:
    json.dump(top20, f, ensure_ascii=False, indent=2)
print("\n已保存 综合评分TOP20.json")
