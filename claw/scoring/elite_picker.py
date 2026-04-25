#!/usr/bin/env python3
"""
精选模式选股 v1.0 — 完全独立版（小资金5-10只精选策略）
================================================================
整合评分系统全部逻辑 + 精选策略，一键运行，无需依赖其他文件。

数据来源：Tushare API + 本地60分钟K线（可选）
评分体系：九维150分制 + 风险扣分 + 保护因子（与score_system.py v3.3一致）
精选规则（回测验证，年化+185%，Sharpe 2.29）：
  1. 非涨停优先（涨停票次日溢价是负期望）
  2. WR2≥3 或 Mistery≥10（有趋势延续性）
  3. 5日涨幅<15%（不追高，安全边际优先）
  4. 同行业≤2只（行业分散降低系统性风险）
  5. 维度均衡度加权（五维均衡 > 单维极高）

使用方式：
  python3 elite_picker.py [精选数量] [目标日期YYYYMMDD]
  例如：
    python3 elite_picker.py          # 默认精选5只，最近交易日
    python3 elite_picker.py 8        # 精选8只
    python3 elite_picker.py 5 20260418  # 精选5只，指定日期
================================================================
"""

import requests, time, json, re, sys, os, warnings
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================
TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_60M_DIR = os.path.expanduser('~/Downloads/2026/60min')

# 命令行参数
ELITE_N = 5
TARGET_DATE = None
for arg in sys.argv[1:]:
    if arg.isdigit() and len(arg) <= 2:
        ELITE_N = int(arg)
    elif arg.isdigit() and len(arg) == 8:
        TARGET_DATE = arg

MAX_PER_IND = 2  # 同行业最多选几只
MAINBOARD_PREFIXES = ('600', '601', '603', '605', '000', '001', '002', '003')


# ============================================================
# Tushare API
# ============================================================
def ts(api, params={}, fields=None):
    d = {"api_name": api, "token": TOKEN, "params": params}
    if fields:
        d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0:
        return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])


# ============================================================
# 交易日历
# ============================================================
def get_trade_dates(target_date=None):
    end_dt = target_date or datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.strptime(end_dt, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
    cal = ts("trade_cal", {"exchange": "SSE", "start_date": start_dt, "end_date": end_dt, "is_open": "1"}, "cal_date")
    if cal.empty:
        raise RuntimeError(f"无法获取交易日历(start={start_dt}, end={end_dt})")
    all_dates = sorted(cal["cal_date"].tolist(), reverse=True)
    if target_date and target_date in all_dates:
        T = target_date
    else:
        T = all_dates[0]
        if target_date and target_date != T:
            print(f"  ⚠️ {target_date}非交易日，自动回退到最近交易日: {T}")
    idx = all_dates.index(T)
    dates3 = all_dates[idx:idx + 3]
    dates5 = [all_dates[idx + i] for i in [0, 2, 5, 10, 20] if idx + i < len(all_dates)]
    extra_kline_dates = all_dates[idx + 1:idx + 11]
    return {
        "target": T, "dates3": dates3, "dates5": dates5,
        "extra_kline_dates": extra_kline_dates, "all_dates": all_dates,
    }


# ============================================================
# 数据采集
# ============================================================
def fetch_all_data(trade_dates):
    """采集全部所需数据，返回数据字典"""
    TARGET = trade_dates["target"]
    dates3 = trade_dates["dates3"]
    dates5 = trade_dates["dates5"]

    print("\n[数据采集]")

    # 股票基本信息
    stk = ts("stock_basic", {"list_status": "L"}, "ts_code,name,industry")
    stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
    stk = stk[~stk["name"].str.contains("ST|退", na=False)]
    ind_map = dict(zip(stk["ts_code"], stk["industry"]))
    name_map = dict(zip(stk["ts_code"], stk["name"]))
    print(f"  股票: {len(stk)}只")
    time.sleep(1)

    # 近3日全市场日K
    daily3 = {}
    for d in dates3:
        df = ts("daily", {"trade_date": d}, "ts_code,pct_chg,amount,open,high,low,close,vol")
        time.sleep(1)
        daily3[d] = df
        print(f"  {d}: {len(df)}只")

    # 5个日期收盘价(多周期)
    cp = {}
    for d in dates5:
        if d in daily3:
            df = daily3.get(d, pd.DataFrame())
        else:
            df = ts("daily", {"trade_date": d}, "ts_code,close")
            time.sleep(1)
        if not df.empty:
            for _, row in df.iterrows():
                if row["ts_code"] not in cp:
                    cp[row["ts_code"]] = {}
                cp[row["ts_code"]][d] = row["close"]
        print(f"  价格{d}: {len(df)}只")

    # 基本面
    time.sleep(1)
    bas = ts("daily_basic", {"trade_date": TARGET}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio")
    bas_d = {row["ts_code"]: row.to_dict() for _, row in bas.iterrows()} if not bas.empty else {}
    print(f"  基本面: {len(bas)}只")

    # 资金流向
    time.sleep(1)
    mf = ts("moneyflow", {"trade_date": TARGET}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
    mf_d = {}
    if not mf.empty:
        for _, row in mf.iterrows():
            mf_d[row["ts_code"]] = row["buy_elg_amount"] + row["buy_lg_amount"] - row["sell_elg_amount"] - row["sell_lg_amount"]
    print(f"  资金: {len(mf_d)}只")

    return {
        "ind_map": ind_map, "name_map": name_map,
        "daily3": daily3, "cp": cp, "bas_d": bas_d, "mf_d": mf_d,
    }


# ============================================================
# 主线识别
# ============================================================
def identify_mainline(daily3, ind_map, dates3):
    print("\n[主线识别]")
    ind_perf = {}
    for d, df in daily3.items():
        df = df.copy()
        df["ind"] = df["ts_code"].map(ind_map)
        grp = df.groupby("ind").agg(
            avg=("pct_chg", "mean"),
            lim=("pct_chg", lambda x: (x >= 9.5).sum())
        ).reset_index()
        grp["rk"] = grp["avg"].rank(ascending=False)
        for _, row in grp.iterrows():
            if row["ind"] not in ind_perf:
                ind_perf[row["ind"]] = []
            ind_perf[row["ind"]].append({"date": d, "avg": row["avg"], "rk": int(row["rk"]), "lim": int(row["lim"])})

    mainline_scores = {}
    for ind, perfs in ind_perf.items():
        avg_chg = np.mean([p["avg"] for p in perfs])
        avg_rk = np.mean([p["rk"] for p in perfs])
        total_lim = sum(p["lim"] for p in perfs)
        top20_days = sum(1 for p in perfs if p["rk"] <= 20)

        score = 0
        if avg_rk <= 10: score += 8
        elif avg_rk <= 20: score += 5
        elif avg_rk <= 30: score += 3
        elif avg_rk <= 50: score += 1

        if top20_days >= 3: score += 6
        elif top20_days >= 2: score += 4
        elif top20_days >= 1: score += 2

        if total_lim >= 20: score += 6
        elif total_lim >= 10: score += 4
        elif total_lim >= 5: score += 3
        elif total_lim >= 2: score += 1

        # 板块持续性
        lim_list = [p["lim"] for p in sorted(perfs, key=lambda x: x["date"])]
        if len(lim_list) >= 2:
            latest_lim = lim_list[-1]
            prev_lim = lim_list[-2]
            if latest_lim >= prev_lim * 1.5 and latest_lim >= 5: score += 3
            elif latest_lim >= prev_lim and latest_lim >= 3: score += 2
            elif latest_lim < prev_lim * 0.5 and prev_lim >= 5: score -= 3
            elif latest_lim < prev_lim: score -= 1

        if len(lim_list) >= 2 and lim_list[-2] == 0 and lim_list[-1] >= 10:
            score -= 2

        if len(lim_list) >= 3 and all(l >= 1 for l in lim_list[-3:]):
            score += 2

        mainline_scores[ind] = max(min(score, 25), 0)

    top_ind = sorted(mainline_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    print(f"  主线行业TOP10:")
    for ind, sc in top_ind:
        print(f"    {ind}: {sc}分")

    return mainline_scores


# ============================================================
# BCI板块完整性计算
# ============================================================
def calc_bci(daily3, dates3, ind_map, bas_d):
    print("\n[BCI板块完整性计算]")

    # 涨停数据
    ind_zt_map = {}
    ind_zt_stocks = {}
    d0 = dates3[0]
    if daily3.get(d0) is not None and not daily3[d0].empty:
        d0_df = daily3[d0].copy()
        d0_df["ind"] = d0_df["ts_code"].map(ind_map)
        for ind_name, grp in d0_df.groupby("ind"):
            zt_stocks_grp = grp[grp["pct_chg"] >= 9.5]
            zt_count = len(zt_stocks_grp)
            ind_zt_map[ind_name] = zt_count
            if zt_count > 0:
                ind_zt_stocks[ind_name] = zt_stocks_grp.to_dict('records')

    # 近3日涨停数
    ind_zt_daily = {}
    for d_idx, d in enumerate(dates3):
        df_d = daily3.get(d, pd.DataFrame())
        if df_d.empty: continue
        df_d_copy = df_d.copy()
        df_d_copy["ind"] = df_d_copy["ts_code"].map(ind_map)
        for ind_name, grp in df_d_copy.groupby("ind"):
            if ind_name not in ind_zt_daily:
                ind_zt_daily[ind_name] = [0] * len(dates3)
            ind_zt_daily[ind_name][d_idx] = int((grp["pct_chg"] >= 9.5).sum())

    # 炸板近似
    ind_zb_map = {}
    if daily3.get(d0) is not None and not daily3[d0].empty:
        d0_all = daily3[d0].copy()
        d0_all["ind"] = d0_all["ts_code"].map(ind_map)
        for ind_name, grp in d0_all.groupby("ind"):
            zb_approx = len(grp[(grp["pct_chg"] >= 5) & (grp["pct_chg"] < 9.5)])
            ind_zb_map[ind_name] = max(0, zb_approx // 3)

    ind_bci_map = {}
    for ind_name, zt_count in ind_zt_map.items():
        if zt_count == 0:
            ind_bci_map[ind_name] = 0
            continue

        bci = 0
        zt_list = ind_zt_stocks.get(ind_name, [])
        n = len(zt_list)

        # BCI-1: 涨停数量(0-20)
        if n >= 8: bci += 20
        elif n >= 5: bci += 17
        elif n >= 3: bci += 13
        elif n >= 2: bci += 8
        else: bci += 3

        # BCI-2: 梯队层次(0-20)
        d1_zt_codes = set()
        if len(dates3) >= 2:
            d1_df = daily3.get(dates3[1], pd.DataFrame())
            if not d1_df.empty:
                d1_zt_codes = set(d1_df[d1_df["pct_chg"] >= 9.5]["ts_code"].tolist())

        连板数 = sum(1 for s in zt_list if s.get("ts_code", "") in d1_zt_codes)
        首板数 = n - 连板数
        层级数 = (1 if 首板数 > 0 else 0) + (1 if 连板数 > 0 else 0)
        最高板估计 = 2 if 连板数 > 0 else 1

        if len(dates3) >= 3:
            d2_df = daily3.get(dates3[2], pd.DataFrame())
            if not d2_df.empty:
                d2_zt_codes = set(d2_df[d2_df["pct_chg"] >= 9.5]["ts_code"].tolist())
                三连板 = sum(1 for s in zt_list if s.get("ts_code", "") in d1_zt_codes and s.get("ts_code", "") in d2_zt_codes)
                if 三连板 > 0:
                    层级数 = min(层级数 + 1, 3)
                    最高板估计 = 3

        s2_bci = min(层级数 * 5, 12) + min(最高板估计 * 2, 8)
        bci += min(s2_bci, 20)

        # BCI-3: 龙头强度(0-15)
        max_amount = max((s.get("amount", 0) for s in zt_list), default=0)
        if max_amount > 500000: bci += 15
        elif max_amount > 200000: bci += 12
        elif max_amount > 100000: bci += 9
        elif max_amount > 50000: bci += 6
        else: bci += 3

        # BCI-4: 封板率(0-10)
        zb_count = ind_zb_map.get(ind_name, 0)
        total_try = n + zb_count
        封板率 = n / total_try if total_try > 0 else 1
        if zb_count == 0: bci += 10
        elif 封板率 > 0.8: bci += 8
        elif 封板率 > 0.6: bci += 5
        elif 封板率 > 0.4: bci += 3
        else: bci += 1

        # BCI-5: 持续性斜率(0-10)
        day_counts = ind_zt_daily.get(ind_name, [0, 0, 0])
        持续天数 = sum(1 for c in day_counts if c > 0)
        有效天 = [(i, c) for i, c in enumerate(day_counts) if c > 0]
        斜率 = (有效天[-1][1] - 有效天[0][1]) / max(有效天[-1][0] - 有效天[0][0], 1) if len(有效天) >= 2 else 0
        s5_bci = 6 if 持续天数 >= 3 else (3 if 持续天数 == 2 else 1)
        if 斜率 > 1: s5_bci += 4
        elif 斜率 > 0: s5_bci += 3
        elif 斜率 == 0: s5_bci += 1
        bci += min(s5_bci, 10)

        # BCI-6: 换手板比例(0-10)
        换手板数 = sum(1 for s in zt_list if bas_d.get(s.get("ts_code", ""), {}).get("turnover_rate_f", 0) and bas_d.get(s.get("ts_code", ""), {}).get("turnover_rate_f", 0) > 8)
        bci += min(int((换手板数 / n if n > 0 else 0) * 10), 10)

        # BCI-7: 板块内聚度(0-15)
        pct_list = [s.get("pct_chg", 0) for s in zt_list]
        if len(pct_list) >= 2:
            pct_std = np.std(pct_list)
            if pct_std < 1: bci += 15
            elif pct_std < 2: bci += 10
            elif pct_std < 3: bci += 7
            else: bci += 3
        else:
            bci += 5

        ind_bci_map[ind_name] = min(bci, 100)

    bci_top = sorted(ind_bci_map.items(), key=lambda x: x[1], reverse=True)[:10]
    print(f"  BCI TOP10:")
    for ind_name, bci_val in bci_top:
        zt_n = ind_zt_map.get(ind_name, 0)
        print(f"    {ind_name}: BCI={bci_val} (涨停{zt_n}家)")

    return ind_bci_map, ind_zt_map, ind_zt_stocks


# ============================================================
# 粗筛候选 + K线获取
# ============================================================
def rough_filter_and_kline(cp, bas_d, trade_dates, daily3, ind_map):
    print(f"\n[粗筛候选]")
    rough_candidates = set()
    for code, p in cp.items():
        ds = sorted(p.keys(), reverse=True)
        if len(ds) < 5: continue
        vals = [p[ds[i]] for i in range(5)]
        r5 = (vals[0] - vals[2]) / vals[2] * 100
        r10 = (vals[0] - vals[3]) / vals[3] * 100
        r20 = (vals[0] - vals[4]) / vals[4] * 100
        big = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
        ps = big * 3 + mid * 2 + small * 1
        if ps < 4: continue
        b = bas_d.get(code, {})
        mv = b.get("total_mv")
        if not mv or mv < 300000: continue
        if r5 > 40 or r10 > 50: continue
        rough_candidates.add(code)

    print(f"  粗筛候选: {len(rough_candidates)}只")

    # 批量获取K线
    kline_data = {}
    for extra_d in trade_dates["extra_kline_dates"]:
        df = ts("daily", {"trade_date": extra_d}, "ts_code,trade_date,open,high,low,close,pct_chg,vol")
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
                    "ts_code": row["ts_code"], "trade_date": d,
                    "open": row.get("open", 0), "high": row.get("high", 0),
                    "low": row.get("low", 0), "close": float(row["close"]),
                    "pct_chg": float(row.get("pct_chg", 0)), "vol": float(row.get("vol", 0))
                })

    print(f"  K线覆盖: {len(kline_data)}只")

    # 60分钟K线预加载（WR-3）
    kline_60m_data = {}
    if os.path.exists(LOCAL_60M_DIR):
        print(f"  60分钟K线(WR-3)...", end="", flush=True)
        wr3_loaded = 0
        for code in rough_candidates:
            code6 = code[:6]
            prefix = 'sh' if code.endswith('.SH') else ('sz' if code.endswith('.SZ') else 'bj')
            csv_file = os.path.join(LOCAL_60M_DIR, f"{prefix}{code6}.csv")
            if os.path.exists(csv_file):
                try:
                    df_60 = pd.read_csv(csv_file, encoding='utf-8')
                    df_60.columns = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']
                    df_60 = df_60.tail(30)
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

    return rough_candidates, kline_data, kline_60m_data


# ============================================================
# 封板时间检测
# ============================================================
def detect_zt_time(daily3, dates3, rough_candidates):
    zt_time_data = {}
    zt_codes = []
    if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
        d0_zt = daily3[dates3[0]]
        zt_codes = d0_zt[d0_zt["pct_chg"] >= 9.5]["ts_code"].tolist()
        zt_codes = [c for c in zt_codes if c in rough_candidates]

    if zt_codes:
        try:
            sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
            from Ashare import get_price as get_price_zt
            print(f"  ⏱ 封板时间({len(zt_codes)}只涨停)...", end="", flush=True)
            t_zt = time.time()
            zt_detected = 0
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
                                hm = time_str.split(' ')[1][:5] if ' ' in time_str else (time_str[11:16] if len(time_str) > 16 else time_str[:5])
                                zt_time_data[code] = hm
                                zt_detected += 1
                                break
                except:
                    pass
                time.sleep(0.1)
            print(f" {zt_detected}/{len(zt_codes)}只 ({time.time() - t_zt:.1f}s)")
        except ImportError:
            print(f"  ⚠ Ashare不可用，跳过封板时间检测")

    return zt_time_data


# ============================================================
# 九维综合评分（与score_system.py v3.3完全一致）
# ============================================================
def score_all_stocks(rough_candidates, data, trade_dates, mainline_scores, ind_bci_map, ind_zt_map,
                     kline_data, kline_60m_data, zt_time_data):
    """对所有粗筛候选股进行九维评分"""
    cp = data["cp"]
    bas_d = data["bas_d"]
    mf_d = data["mf_d"]
    ind_map = data["ind_map"]
    name_map = data["name_map"]
    daily3 = data["daily3"]
    dates3 = trade_dates["dates3"]

    # 全市场成交额
    total_market_amount = 0
    if daily3.get(dates3[0]) is not None and not daily3[dates3[0]].empty:
        total_market_amount = daily3[dates3[0]]["amount"].sum() / 100000
    if total_market_amount > 20000: dynamic_mv_cap = 500
    elif total_market_amount > 15000: dynamic_mv_cap = 300
    else: dynamic_mv_cap = 150
    print(f"  全市场成交额: {total_market_amount:.0f}亿 | 动态市值上限: {dynamic_mv_cap}亿")

    print(f"\n[综合评分]")
    results = []

    for code in rough_candidates:
        nm = name_map.get(code, "?")
        ind = ind_map.get(code, "?")
        b = bas_d.get(code, {})
        pe = b.get("pe_ttm")
        mv = b.get("total_mv", 0) / 10000
        tr = b.get("turnover_rate_f", 0)
        nb = mf_d.get(code, 0)

        p = cp.get(code, {})
        ds = sorted(p.keys(), reverse=True)
        if len(ds) < 5: continue
        vals = [p[ds[i]] for i in range(5)]
        c0 = vals[0]
        r5 = (vals[0] - vals[2]) / vals[2] * 100
        r10 = (vals[0] - vals[3]) / vals[3] * 100
        r20 = (vals[0] - vals[4]) / vals[4] * 100

        # ====== 维度1: 多周期共振 (15分) ======
        big = 3 if r20 > 10 else (2 if r20 > 5 else (1 if r20 > 0 else (0 if r20 > -5 else (-1 if r20 > -10 else -3))))
        mid = 2 if r10 > 5 else (1 if r10 > 2 else (0 if r10 > -2 else (-1 if r10 > -5 else -2)))
        small = 1 if r5 > 3 else (0 if r5 > -2 else -1)
        period_raw = big + mid + small
        d1_score = max(0, min(15, int((period_raw + 6) / 12 * 15 + 0.5)))

        # ====== 维度2: 主线热点 (25分，含BCI加权) ======
        d2_base = mainline_scores.get(ind, 0)
        ind_bci = ind_bci_map.get(ind, 0)
        if ind_bci >= 70: d2_base += 3
        elif ind_bci >= 50: d2_base += 2
        elif ind_bci >= 30: d2_base += 1
        d2_score = min(d2_base, 25)

        # ====== 维度3: 三Skill (35分) ======
        klines = kline_data.get(code, [])
        d3_score = 0
        mistery = 0
        tds = 0
        is_ma_bull = False
        consecutive_yang = 0
        pct_last = 0
        is_zt = False
        vol5 = 0
        vol10 = 1
        peaks = []
        bbw_val = 0

        if klines:
            kdf = pd.DataFrame(klines)
            if "trade_date" in kdf.columns:
                kdf = kdf.drop_duplicates(subset=["trade_date"]).copy()
                kdf.sort_values("trade_date", inplace=True)
                kdf.reset_index(drop=True, inplace=True)
                n_k = len(kdf)

                if n_k >= 10:
                    cc = kdf["close"].astype(float).values
                    hh = kdf["high"].astype(float).values
                    ll = kdf["low"].astype(float).values
                    oo = kdf["open"].astype(float).values
                    vv = kdf["vol"].astype(float).values

                    ma5 = pd.Series(cc).rolling(5).mean().iloc[-1]
                    ma10 = pd.Series(cc).rolling(10).mean().iloc[-1]
                    ma20 = pd.Series(cc).rolling(min(20, n_k)).mean().iloc[-1]

                    ema12 = pd.Series(cc).ewm(span=12, adjust=False).mean()
                    ema26 = pd.Series(cc).ewm(span=min(26, n_k), adjust=False).mean()
                    dif = (ema12 - ema26).iloc[-1]
                    dea = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]

                    is_ma_bull = cc[-1] > ma5 > ma10 > ma20
                    pct_last = kdf["pct_chg"].astype(float).iloc[-1] if "pct_chg" in kdf.columns else 0
                    is_zt = pct_last >= 9.5
                    vol5 = np.mean(vv[-5:])
                    vol10 = np.mean(vv[-min(10, n_k):])
                    vol_ratio_val = vol5 / vol10 if vol10 > 0 else 1

                    for k in range(n_k - 1, -1, -1):
                        if cc[k] > oo[k]: consecutive_yang += 1
                        else: break

                    # --- Mistery (20分) ---
                    if cc[-1] > ma5 > ma10 > ma20: mistery += 3
                    elif cc[-1] > ma5 > ma10: mistery += 2
                    elif cc[-1] > ma20: mistery += 1

                    # M2买点
                    ma5s = pd.Series(cc).rolling(5).mean()
                    ma20s = pd.Series(cc).rolling(min(20, n_k)).mean()
                    m2 = 0
                    if n_k >= 7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5] <= ma20s.iloc[-5] and ma5 > ma20:
                        m2 += 2
                    if n_k >= 5:
                        below = any(cc[i] < ma5s.iloc[i] for i in range(max(0, n_k - 5), n_k - 1) if not np.isnan(ma5s.iloc[i]))
                        if below and cc[-1] > ma5: m2 += 1
                    std20 = np.std(cc[-min(20, n_k):])
                    bbw_val = (4 * std20) / ma20 if ma20 > 0 else 0
                    if bbw_val < 0.12 and pct_last > 3: m2 += 2
                    elif bbw_val < 0.15 and pct_last > 3: m2 += 1
                    mistery += min(3, m2)

                    # M3卖点扣分
                    if pct_last < 2 and vol5 / vol10 > 2.5: mistery -= 1
                    if n_k >= 4 and hh[-1] < max(hh[-4:-1]) and pct_last < 1: mistery -= 1

                    # M4量价
                    if vol5 / vol10 > 1.3 and pct_last > 0: mistery += 2
                    elif vol5 / vol10 > 1: mistery += 1
                    if dif > dea: mistery += 1

                    # M5形态
                    if n_k >= 3:
                        prev_range = abs(cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                        if pct_last > 5 and vol5 / vol10 > 1.5 and prev_range < 2: mistery += 2
                        if n_k >= 2:
                            prev_upper = (hh[-2] - cc[-2]) / cc[-2] * 100 if cc[-2] > 0 else 0
                            if prev_upper > 3 and cc[-1] > hh[-2] * 0.98: mistery += 1

                    # M6仓位管理(0-5)
                    m6 = 0
                    if cc[-1] > ma5 > ma10 > ma20 and pct_last > 0: m6 += 2
                    elif cc[-1] > ma5 > ma10: m6 += 1
                    if ma20 > 0 and abs(cc[-1] - ma20) / ma20 < 0.08: m6 += 1
                    if n_k >= 20 and cc[-1] > min(ll[-20:]) * 1.05: m6 += 1
                    if n_k >= 7:
                        chg_7d = (cc[-1] - cc[-7]) / cc[-7] * 100 if cc[-7] > 0 else 0
                        if chg_7d > 5: m6 += 1
                        elif chg_7d < -3: m6 -= 1
                    mistery += min(5, max(0, m6))
                    mistery = max(min(mistery, 20), 0)

                    # --- TDS (12分) ---
                    win = min(5, n_k // 3)
                    for i in range(win, n_k - win):
                        if hh[i] >= max(hh[max(0, i - win):i]) and hh[i] >= max(hh[i + 1:min(n_k, i + win + 1)]):
                            peaks.append(hh[i])

                    troughs = []
                    for i in range(win, n_k - win):
                        if ll[i] <= min(ll[max(0, i - win):i]) and ll[i] <= min(ll[i + 1:min(n_k, i + win + 1)]):
                            troughs.append(ll[i])

                    if len(peaks) >= 2 and len(troughs) >= 2:
                        if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]: tds += 3
                        elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]: tds += 2
                    elif len(peaks) >= 2 and peaks[-1] > peaks[-2]: tds += 2

                    if n_k >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]: tds += 2
                    elif n_k >= 2 and hh[-1] > hh[-2]: tds += 1

                    if n_k >= 3 and cc[-2] < oo[-2] and cc[-1] > oo[-1] and cc[-1] > hh[-2]: tds += 1

                    if peaks and cc[-1] > peaks[-1]: tds += 2
                    elif n_k >= 2 and pct_last >= 9.5: tds += 1

                    if r5 < -10 and pct_last > 3: tds += 2
                    elif r5 < -5 and pct_last > 0:
                        body = abs(cc[-1] - oo[-1])
                        lower_shadow = min(cc[-1], oo[-1]) - ll[-1]
                        if lower_shadow > body * 2 and lower_shadow > 0: tds += 1

                    # T4三K反转
                    if n_k >= 4:
                        k1_body = abs(cc[-3] - oo[-3])
                        k2_body = abs(cc[-2] - oo[-2])
                        k3_body = abs(cc[-1] - oo[-1])
                        if r5 < -3 and k2_body <= max(k1_body, k3_body) and cc[-1] > oo[-1] and cc[-1] > hh[-2]:
                            tds += 2
                        elif r5 > 10 and k2_body <= max(k1_body, k3_body) and cc[-1] < oo[-1] and cc[-1] < ll[-2]:
                            tds -= 1

                    tds = max(min(tds, 12), 0)

                    # --- 元子元情绪 (10分) ---
                    yuanzi = 0
                    if is_zt and r5 < -5: yuanzi += 5
                    elif is_zt and r5 < 5: yuanzi += 4
                    elif pct_last >= 5 and r5 < 0: yuanzi += 4
                    elif is_zt and 5 <= r5 < 15: yuanzi += 3
                    elif pct_last >= 5 and 0 <= r5 < 10: yuanzi += 3
                    elif is_zt and r5 >= 15:
                        yuanzi += 0 if vol_ratio_val >= 3 else 2
                    elif pct_last > 0 and r5 < 10: yuanzi += 2
                    elif pct_last < -3 and r5 > 15: yuanzi += 0
                    elif pct_last <= 0:
                        yuanzi += 2 if r5 < -10 else 1

                    if pct_last > 3 and vol_ratio_val < 1.2: yuanzi += 2
                    elif pct_last < 2 and vol_ratio_val > 2.5: yuanzi -= 1

                    if mainline_scores.get(ind, 0) >= 10: yuanzi += 2
                    elif mainline_scores.get(ind, 0) >= 5: yuanzi += 1
                    yuanzi = max(min(yuanzi, 10), 0)

                    # --- TXCG六大模型(0-5) ---
                    txcg_model = 0
                    ind_zt_count = ind_zt_map.get(ind, 0)
                    if is_zt and ind_zt_count >= 3: txcg_model += 1
                    if r5 < -5 and pct_last > 3: txcg_model += 1
                    if n_k >= 3:
                        prev_chg = (cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                        if prev_chg < -3 and pct_last > 2: txcg_model += 1
                    if ma5 > 0 and abs(cc[-1] - ma5) / ma5 < 0.02 and pct_last > 0: txcg_model += 1
                    if n_k >= 2:
                        body_prev = abs(cc[-2] - oo[-2])
                        lower_shadow_prev = min(cc[-2], oo[-2]) - ll[-2]
                        if lower_shadow_prev > body_prev * 2 and lower_shadow_prev > 0 and pct_last > 0: txcg_model += 1
                    if is_zt and ind_zt_count == 1: txcg_model += 1
                    txcg_model = min(txcg_model, 5)

                    d3_score = mistery + tds + yuanzi + txcg_model

        # ====== 维度4: 安全边际 (15分) ======
        d4_score = 0
        if abs(r5) <= 5: d4_score += 5
        elif abs(r5) <= 10: d4_score += 3
        elif abs(r5) <= 15: d4_score += 1
        if abs(r10) <= 10: d4_score += 5
        elif abs(r10) <= 15: d4_score += 3
        elif abs(r10) <= 20: d4_score += 1
        if tr and tr <= 5: d4_score += 5
        elif tr and tr <= 10: d4_score += 3
        elif tr and tr <= 15: d4_score += 1
        d4_score = min(d4_score, 15)

        # ====== 维度5: 基本面 (15分) ======
        d5_score = 0
        if pe and pe > 0:
            if pe <= 15: d5_score += 6
            elif pe <= 25: d5_score += 5
            elif pe <= 40: d5_score += 4
            elif pe <= 60: d5_score += 3
            elif pe <= 100: d5_score += 1
        if 100 <= mv <= 500: d5_score += 3
        elif 500 < mv <= 2000: d5_score += 2
        elif mv > 2000: d5_score += 1
        elif 50 <= mv < 100: d5_score += 2
        nb_yi = nb / 10000
        if nb_yi > 1: d5_score += 6
        elif nb_yi > 0.3: d5_score += 4
        elif nb_yi > 0: d5_score += 2
        d5_score = min(d5_score, 15)

        # ====== 维度9: 百胜WR (15分) ======
        d9_score = 0
        wr_tags = []
        vr_wr = bas_d.get(code, {}).get("volume_ratio", 0)

        # WR-1 首板放量涨停(0-7)
        wr1 = 0
        if is_zt:
            wr1 += 1
            if vr_wr and vr_wr >= 3: wr1 += 1
            if tr and tr >= 8: wr1 += 1
            if is_ma_bull: wr1 += 1
            if 30 <= mv <= 150: wr1 += 1
            if nb_yi > 0: wr1 += 1
            zt_time = zt_time_data.get(code)
            if zt_time:
                if zt_time <= "10:30": wr1 += 1; wr_tags.append(f"封板{zt_time}✅")
                elif zt_time <= "11:30": wr_tags.append(f"封板{zt_time}午前⚠")
                else: wr_tags.append(f"封板{zt_time}偏晚❌")
            if wr1 >= 6: wr_tags.append(f"🔥WR1={wr1}/7")
            elif wr1 >= 5: wr_tags.append(f"WR1={wr1}/7")

        # WR-2 右侧趋势起爆(0-5)
        wr2 = 0
        if klines and len(klines) >= 10:
            if bbw_val < 0.15: wr2 += 1
            if vr_wr and vr_wr >= 2.5: wr2 += 1
            elif vol5 / vol10 > 2.5 if vol10 > 0 else False: wr2 += 1
            if pct_last >= 9.5: wr2 += 1
            elif pct_last >= 7: wr2 += 1
            if is_ma_bull: wr2 += 1
            if peaks and cc[-1] > peaks[-1]: wr2 += 1
            if wr2 >= 4: wr_tags.append(f"🔥WR2={wr2}/5起爆")
            elif wr2 >= 3: wr_tags.append(f"WR2={wr2}/5")

        # WR-3 底倍量柱(0-4)
        wr3 = 0
        kline_60m = kline_60m_data.get(code)
        if kline_60m and len(kline_60m.get('vols', [])) >= 12:
            vols_60 = kline_60m['vols']
            closes_60 = kline_60m['closes']
            highs_60 = kline_60m['highs']
            lows_60 = kline_60m['lows']
            n60 = len(vols_60)
            first_dbl_idx = None
            for i_60 in range(max(1, n60 - 20), n60):
                if vols_60[i_60] >= vols_60[i_60 - 1] * 2 and closes_60[i_60] > closes_60[i_60 - 1]:
                    recent_range = closes_60[max(0, i_60 - 20):i_60 + 1]
                    mid_price = (max(recent_range) + min(recent_range)) / 2
                    if closes_60[i_60] <= mid_price * 1.05:
                        first_dbl_idx = i_60
                        break
            if first_dbl_idx is not None:
                wr3 += 1
                first_low = lows_60[first_dbl_idx]
                first_high = highs_60[first_dbl_idx]
                for j_60 in range(first_dbl_idx + 1, n60):
                    if vols_60[j_60] >= vols_60[j_60 - 1] * 2:
                        if closes_60[j_60] > first_high: wr3 += 1
                        if lows_60[j_60] >= first_low: wr3 += 1
                        break
                if closes_60[-1] >= first_low: wr3 += 1
                if wr3 >= 3: wr_tags.append(f"🔥WR3={wr3}/4底倍量")
                elif wr3 >= 2: wr_tags.append(f"WR3={wr3}/4")

        best_wr = max(wr1, wr2, wr3)
        best_wr_max = 7 if best_wr == wr1 else (5 if best_wr == wr2 else 4)
        d9_score = min(int(best_wr / best_wr_max * 15 + 0.5) if best_wr_max > 0 else 0, 15)

        # ====== 风险扣分 (0~-30分) ======
        risk_deduct = 0
        risk_tags = []
        ind_zt_count = ind_zt_map.get(ind, 0)

        if r5 > 20:
            d = 3 if is_ma_bull else 5
            risk_deduct += d; risk_tags.append(f"超涨5日{r5:.0f}%-{d}")
        elif r5 > 15:
            risk_deduct += 2; risk_tags.append(f"偏涨5日{r5:.0f}%-2")

        if r10 > 25:
            d = 3 if is_ma_bull else 5
            risk_deduct += d; risk_tags.append(f"超涨10日{r10:.0f}%-{d}")
        elif r10 > 20:
            risk_deduct += 2; risk_tags.append(f"偏涨10日{r10:.0f}%-2")

        if r20 > 50:
            risk_deduct += 8; risk_tags.append(f"极端超涨20日{r20:.0f}%-8")
        elif r20 > 35:
            risk_deduct += 4; risk_tags.append(f"超涨20日{r20:.0f}%-4")

        ind_bci_risk = ind_bci_map.get(ind, 0)
        if ind_zt_count < 3:
            if ind_bci_risk >= 50:
                risk_deduct += 1; risk_tags.append(f"行业涨停{ind_zt_count}家BCI={ind_bci_risk}-1")
            elif mainline_scores.get(ind, 0) >= 8:
                risk_deduct += 2; risk_tags.append(f"行业涨停{ind_zt_count}家-2")
            elif ind_zt_count == 0:
                if ind_bci_risk >= 30:
                    risk_deduct += 3; risk_tags.append(f"行业涨停0家BCI={ind_bci_risk}-3")
                else:
                    risk_deduct += 5; risk_tags.append(f"行业涨停0家-5")
            else:
                risk_deduct += 3; risk_tags.append(f"行业涨停{ind_zt_count}家-3")

        if nb_yi < -2:
            if is_zt:
                risk_deduct += 1; risk_tags.append(f"涨停净流出{nb_yi:.1f}亿-1")
            else:
                risk_deduct += 3; risk_tags.append(f"净流出{nb_yi:.1f}亿-3")
        elif nb_yi < -0.5:
            risk_deduct += 1; risk_tags.append(f"小幅净流出{nb_yi:.1f}亿-1")

        if mv > dynamic_mv_cap:
            if mv > 1000:
                risk_deduct += 5; risk_tags.append(f"市值{mv:.0f}亿-5")
            else:
                risk_deduct += 3; risk_tags.append(f"市值{mv:.0f}亿-3")

        if tr and tr > 50:
            risk_deduct += 3; risk_tags.append(f"高换手{tr:.0f}%-3")
        elif tr and tr > 30:
            risk_deduct += 1; risk_tags.append(f"换手偏高{tr:.0f}%-1")

        risk_deduct = min(risk_deduct, 30)

        # ====== 保护因子 (0~+15分) ======
        protect_bonus = 0
        protect_tags = []

        if is_ma_bull:
            protect_bonus += 3; protect_tags.append("趋势多头+3")
        if consecutive_yang >= 5:
            protect_bonus += 3; protect_tags.append(f"连阳{consecutive_yang}天+3")
        elif consecutive_yang >= 3:
            protect_bonus += 2; protect_tags.append(f"连阳{consecutive_yang}天+2")
        if mistery >= 12:
            protect_bonus += 2; protect_tags.append("Mistery高分+2")
        if is_zt:
            protect_bonus += 3; protect_tags.append("涨停+3")
        if is_zt and ind_zt_count >= 3:
            protect_bonus += 2; protect_tags.append("板块龙头+2")
        if nb_yi > 2:
            protect_bonus += 2; protect_tags.append(f"大单{nb_yi:.1f}亿+2")
        ind_bci_protect = ind_bci_map.get(ind, 0)
        if ind_bci_protect >= 70:
            protect_bonus += 2; protect_tags.append(f"BCI={ind_bci_protect}板块完整+2")
        elif ind_bci_protect >= 50:
            protect_bonus += 1; protect_tags.append(f"BCI={ind_bci_protect}板块较完整+1")
        protect_bonus = min(protect_bonus, 15)

        # ====== 最终得分 ======
        raw_total = d1_score + d2_score + d3_score + d4_score + d5_score + d9_score
        net_risk = max(risk_deduct - protect_bonus, 0)
        total = raw_total - net_risk

        results.append({
            "code": code, "name": nm, "ind": ind, "close": c0,
            "pe": pe, "mv": mv, "tr": tr, "nb_yi": nb_yi,
            "r5": r5, "r10": r10, "r20": r20,
            "d1": d1_score, "d2": d2_score, "d3": d3_score, "d4": d4_score, "d5": d5_score,
            "d9": d9_score, "mistery": mistery, "wr1": wr1, "wr2": wr2, "wr3": wr3,
            "wr_tags": "+".join(wr_tags) if wr_tags else "-",
            "risk": risk_deduct, "protect": protect_bonus, "net_risk": net_risk,
            "risk_tags": "|".join(risk_tags) if risk_tags else "-",
            "protect_tags": "|".join(protect_tags) if protect_tags else "-",
            "raw_total": raw_total, "total": total,
            "is_zt": is_zt,
        })

    results.sort(key=lambda x: x["total"], reverse=True)
    print(f"  评分完成: {len(results)}只")
    return results


# ============================================================
# 精选逻辑
# ============================================================
def calc_dimension_balance(stock):
    """计算维度均衡度得分（0-100）"""
    dim_max = {'d1': 15, 'd2': 25, 'd3': 47, 'd4': 15, 'd5': 15}
    dims = [stock.get(key, 0) / max_val for key, max_val in dim_max.items()]
    mean_val = np.mean(dims)
    if mean_val == 0: return 0
    cv = np.std(dims) / mean_val
    return max(0, 1 - cv) * 100


def calc_elite_score(stock):
    """计算精选得分"""
    total_norm = stock.get('total', 0) / 150 * 100
    balance = calc_dimension_balance(stock)
    net_risk = stock.get('net_risk', 0)
    risk_score = max(0, 100 - net_risk * 10)
    nb = stock.get('nb_yi', 0) or 0
    fund_score = min(100, max(0, 50 + nb * 10))
    return total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15


def is_mainboard(code):
    return str(code)[:6].startswith(MAINBOARD_PREFIXES)


def elite_select(stocks, n=5, max_per_ind=2, mode='strict'):
    """精选模式选股"""
    candidates = [dict(s) for s in stocks]

    for s in candidates:
        s['_is_zt'] = s.get('is_zt', False)
        s['_wr2'] = s.get('wr2', 0)
        s['_mistery'] = s.get('mistery', 0)
        s['_r5'] = s.get('r5', 0)
        s['_ind'] = s.get('ind', '未知')
        s['_is_main'] = is_mainboard(s.get('code', ''))

    # 硬性过滤
    if mode == 'strict':
        filtered = [s for s in candidates if not s['_is_zt'] and s['_r5'] < 15 and (s['_wr2'] >= 3 or s['_mistery'] >= 10)]
        if len(filtered) < n:
            filtered = [s for s in candidates if not s['_is_zt'] and s['_r5'] < 20]
        if len(filtered) < n:
            filtered = [s for s in candidates if not s['_is_zt']]
        if len(filtered) < n:
            filtered = candidates
    elif mode == 'mainboard':
        filtered = [s for s in candidates if s['_is_main'] and not s['_is_zt'] and s['_r5'] < 15]
        if len(filtered) < n:
            filtered = [s for s in candidates if s['_is_main'] and not s['_is_zt']]
        if len(filtered) < n:
            filtered = [s for s in candidates if not s['_is_zt']]
    else:
        filtered = candidates

    # 计算精选得分 + 加权调整
    for s in filtered:
        s['_elite_base'] = calc_elite_score(s)
        s['_elite_adj'] = s['_elite_base']
        if not s['_is_zt']: s['_elite_adj'] += 10
        if s['_wr2'] >= 4: s['_elite_adj'] += 25
        elif s['_wr2'] >= 3: s['_elite_adj'] += 15
        if s['_mistery'] >= 15: s['_elite_adj'] += 20
        elif s['_mistery'] >= 12: s['_elite_adj'] += 12
        elif s['_mistery'] >= 10: s['_elite_adj'] += 5
        if s.get('d4', 0) >= 10: s['_elite_adj'] += 8
        if s.get('nb_yi', 0) and s['nb_yi'] > 0: s['_elite_adj'] += 5
        if abs(s['_r5']) <= 5: s['_elite_adj'] += 5

    filtered.sort(key=lambda x: x.get('_elite_adj', 0), reverse=True)

    # 行业分散
    selected = []
    ind_count = defaultdict(int)
    for s in filtered:
        ind = s['_ind']
        if ind_count[ind] >= max_per_ind: continue
        selected.append(s)
        ind_count[ind] += 1
        if len(selected) >= n: break

    return selected


# ============================================================
# 输出格式化
# ============================================================
def get_level_tag(total):
    if total >= 110: return "⭐强推"
    elif total >= 90: return "✅推荐"
    elif total >= 75: return "👀关注"
    else: return "  一般"


def print_score_table(results, title="综合评分TOP30"):
    """打印评分总表"""
    print(f"\n{'='*200}")
    print(f"{title}")
    print(f"{'='*200}")
    print(f"{'#':>2} {'股票':<18} {'行业':<8} {'收盘':>7} {'PE':>5} {'市值':>5} {'5日%':>5} {'10日%':>6} "
          f"{'净流亿':>6} {'九维':>4} {'风险':>4} {'保护':>4} {'净扣':>4} {'总分':>4} {'WR':>4} {'WR标签':<16} {'级别':<6} {'风险明细'}")
    print("-" * 200)

    for i, r in enumerate(results[:30], 1):
        level = get_level_tag(r["total"])
        pe_str = f"{r['pe']:.0f}" if r['pe'] and r['pe'] > 0 else "N/A"
        detail_str = r.get('risk_tags', '-')
        if r.get('protect_tags', '-') != "-":
            detail_str += " 🛡" + r['protect_tags']
        print(f"{i:>2d} {r['name']}({r['code'][:6]}) {r['ind']:<8} {r['close']:>7.2f} {pe_str:>5} "
              f"{r['mv']:>5.0f} {r['r5']:>+5.1f} {r['r10']:>+6.1f} {r['nb_yi']:>+6.2f} "
              f"{r['raw_total']:>4d} {r['risk']:>4d} {r['protect']:>4d} {r['net_risk']:>4d} {r['total']:>4d} {r.get('d9', 0):>4d} {r.get('wr_tags', '-'):<16} {level:<6} {detail_str}")


def print_elite_result(selected, title="精选结果"):
    """打印精选结果"""
    print(f"\n{'='*130}")
    print(f"🏆 {title}（共{len(selected)}只）")
    print(f"{'='*130}")

    print(f"{'#':>2} {'股票':<18} {'行业':<8} {'收盘':>7} {'总分':>4} {'级别':<6} "
          f"{'精选分':>6} {'WR标签':<18} {'5日%':>6} {'净流亿':>6} "
          f"{'D1':>3} {'D2':>3} {'D3':>3} {'D4':>3} {'D5':>3} {'均衡':>4} {'理由'}")
    print("-" * 130)

    industries = set()
    for i, s in enumerate(selected, 1):
        code = s.get('code', '?')[:6]
        name = s.get('name', '?')
        ind = s['_ind']
        close = s.get('close', 0)
        total = s.get('total', 0)
        level = get_level_tag(total)
        elite_adj = s.get('_elite_adj', 0)
        wr_tags = s.get('wr_tags', '-')
        r5 = s['_r5']
        nb = s.get('nb_yi', 0) or 0
        d1 = s.get('d1', 0); d2 = s.get('d2', 0); d3 = s.get('d3', 0)
        d4 = s.get('d4', 0); d5 = s.get('d5', 0)
        balance = calc_dimension_balance(s)

        reasons = []
        if not s['_is_zt']: reasons.append("非涨停")
        if s['_wr2'] >= 3: reasons.append(f"WR2={s['_wr2']}")
        if s['_mistery'] >= 10: reasons.append(f"M≥{s['_mistery']}")
        if d4 >= 10: reasons.append("安全边际好")
        if nb > 0: reasons.append("资金流入")
        if abs(r5) <= 5: reasons.append("低位")
        reason_str = " ".join(reasons) if reasons else "-"

        print(f"{i:>2d} {name}({code}) {ind:<8} {close:>7.2f} {total:>4d} {level:<6} "
              f"{elite_adj:>6.1f} {wr_tags:<18} {r5:>+6.1f} {nb:>+6.2f} "
              f"{d1:>3d} {d2:>3d} {d3:>3d} {d4:>3d} {d5:>3d} {balance:>4.0f} {reason_str}")
        industries.add(ind)

    print(f"\n📊 统计: 覆盖{len(industries)}个行业 | 平均精选分{np.mean([s.get('_elite_adj',0) for s in selected]):.1f} | "
          f"平均总分{np.mean([s.get('total',0) for s in selected]):.1f}")

    ind_dist = defaultdict(list)
    for s in selected:
        ind_dist[s['_ind']].append(s.get('name', '?'))
    print(f"📋 行业分布: {' | '.join(f'{ind}({len(names)}只)' for ind, names in ind_dist.items())}")


def print_position_advice(selected):
    """打印仓位建议"""
    n = len(selected)
    if n == 0: return

    print(f"\n{'='*130}")
    print(f"💰 仓位分配建议（{n}只标的）")
    print(f"{'='*130}")

    scores = [s.get('_elite_adj', 50) for s in selected]
    total_score = sum(scores)
    equal_pct = 100 / n
    weighted_pcts = [s / total_score * 100 for s in scores]
    mixed_pcts = [equal_pct * 0.6 + w * 0.4 for w in weighted_pcts]

    print(f"\n{'#':>2} {'股票':<18} {'等权%':>6} {'加权%':>6} {'建议%':>6} {'说明'}")
    print("-" * 80)

    for i, s in enumerate(selected):
        name = s.get('name', '?')
        code = s.get('code', '?')[:6]
        if scores[i] == max(scores): note = "⭐ 核心仓位"
        elif scores[i] >= np.mean(scores): note = "标准仓位"
        else: note = "观察仓位"
        print(f"{i+1:>2d} {name}({code}) {equal_pct:>6.1f} {weighted_pcts[i]:>6.1f} {mixed_pcts[i]:>6.1f} {note}")

    print(f"\n💡 建议:")
    print(f"   • 偏暖市场: 集中持有{min(5,n)}只，每只{100/min(5,n):.0f}%")
    print(f"   • 中性市场: 分散持有{min(8,n)}只，每只{100/min(8,n):.1f}%")
    print(f"   • 偏冷市场: 分散持有{n}只，每只{100/n:.1f}%，或降低总仓位")


def print_operation_guide():
    """打印操作指南"""
    print(f"""
{'='*130}
📋 操作指南
{'='*130}

  ┌─────────────────────────────────────────────────────────┐
  │  精选模式操作规范（回测年化+185%，Sharpe 2.29）          │
  ├─────────────────────────────────────────────────────────┤
  │                                                         │
  │  ✅ 买入时机:                                           │
  │     • 集合竞价观察，9:25-9:30确认无异常                  │
  │     • 开盘后5分钟内确认量能正常再买入                    │
  │     • 如果高开>3%，等回落再买（不追高）                  │
  │                                                         │
  │  ✅ 持有期:                                              │
  │     • 默认T+1（次日卖出）                                │
  │     • 如果次日仍在涨且量能配合，可持有到T+2              │
  │     • 最长不超过T+3                                      │
  │                                                         │
  │  ❌ 止损规则:                                            │
  │     • 日内跌>5%: 立即止损                                │
  │     • 开盘低开>3%: 观察15分钟，不回升则止损              │
  │     • 尾盘弱势（14:30后仍跌>3%）: 减半仓                │
  │                                                         │
  │  ⚠️ 注意事项:                                           │
  │     • 同行业最多2只，避免板块系统性风险                  │
  │     • 不追涨停板，不买5日涨>15%的票                     │
  │     • 大盘跌>2%时，降低仓位到50%以下                    │
  │                                                         │
  └─────────────────────────────────────────────────────────┘
""")


# ============================================================
# 主程序
# ============================================================
def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print("=" * 130)
    print(f"🏆 精选模式选股 v1.0 — 完全独立版（小资金{ELITE_N}只精选策略）")
    print(f"   时间: {now}")
    print(f"   规则: 非涨停 + (WR2≥3或Mistery≥10) + 5日涨<15% + 同行业≤{MAX_PER_IND}只")
    print(f"   回测: 年化+185% | 胜率56.5% | Sharpe 2.29 | 最大回撤-27%")
    print("=" * 130)

    # ===== 交易日历 =====
    print("\n[交易日历]")
    trade_dates = get_trade_dates(TARGET_DATE)
    TARGET = trade_dates["target"]
    print(f"  目标日期: {TARGET}")
    print(f"  dates3(近3日): {trade_dates['dates3']}")
    print(f"  dates5(多周期): {trade_dates['dates5']}")

    # ===== 数据采集 =====
    data = fetch_all_data(trade_dates)

    # ===== 主线识别 =====
    mainline_scores = identify_mainline(data["daily3"], data["ind_map"], trade_dates["dates3"])

    # ===== BCI板块完整性 =====
    ind_bci_map, ind_zt_map, ind_zt_stocks = calc_bci(
        data["daily3"], trade_dates["dates3"], data["ind_map"], data["bas_d"])

    # ===== 粗筛 + K线 =====
    rough_candidates, kline_data, kline_60m_data = rough_filter_and_kline(
        data["cp"], data["bas_d"], trade_dates, data["daily3"], data["ind_map"])

    # ===== 封板时间 =====
    zt_time_data = detect_zt_time(data["daily3"], trade_dates["dates3"], rough_candidates)

    # ===== 九维评分 =====
    results = score_all_stocks(
        rough_candidates, data, trade_dates, mainline_scores, ind_bci_map, ind_zt_map,
        kline_data, kline_60m_data, zt_time_data)

    # ===== 评分总表 =====
    print_score_table(results, f"综合评分TOP30 v3.3（{TARGET}）")

    # 统计
    strong = [r for r in results if r["total"] >= 110]
    good = [r for r in results if 90 <= r["total"] < 110]
    watch = [r for r in results if 75 <= r["total"] < 90]
    print(f"\n统计: ⭐强推{len(strong)}只 | ✅推荐{len(good)}只 | 👀关注{len(watch)}只 | 总计{len(results)}只")

    # ===== 精选策略 =====
    top30 = results[:30]

    # 策略一：严格精选
    print(f"\n{'='*130}")
    print(f"📊 策略一：严格精选TOP{ELITE_N}（🥇 回测最优，年化+185%）")
    print(f"   规则: 非涨停 + (WR2≥3或Mistery≥10) + 5日涨<15% + 行业分散")
    strict_picks = elite_select(top30, n=ELITE_N, max_per_ind=MAX_PER_IND, mode='strict')
    print_elite_result(strict_picks, f"严格精选TOP{ELITE_N}")

    # 策略二：WR2加权精选
    print(f"\n{'='*130}")
    print(f"📊 策略二：WR2/Mistery加权精选TOP{ELITE_N}（🥉 回测第三，年化+140%，回撤最小-20%）")
    wr2_picks = elite_select(top30, n=ELITE_N, max_per_ind=MAX_PER_IND, mode='wr2')
    print_elite_result(wr2_picks, f"WR2加权精选TOP{ELITE_N}")

    # 策略三：主板精选
    main_candidates = [s for s in top30 if is_mainboard(s.get('code', ''))]
    if len(main_candidates) >= 3:
        print(f"\n{'='*130}")
        print(f"📊 策略三：主板精选TOP{ELITE_N}（回测年化+117%，波动更小）")
        main_picks = elite_select(top30, n=ELITE_N, max_per_ind=MAX_PER_IND, mode='mainboard')
        print_elite_result(main_picks, f"主板精选TOP{ELITE_N}")

    # 策略重叠分析
    print(f"\n{'='*130}")
    print(f"📊 策略重叠分析")
    print(f"{'='*130}")
    strict_codes = set(s.get('code', '')[:6] for s in strict_picks)
    wr2_codes = set(s.get('code', '')[:6] for s in wr2_picks)
    overlap = strict_codes & wr2_codes
    print(f"\n  两策略共同选中: {len(overlap)}只 → {', '.join(overlap) if overlap else '无'}")
    if overlap:
        print(f"  ⭐ 共同选中的标的信心最高，建议优先配置")

    # 仓位建议
    print_position_advice(strict_picks)

    # 操作指南
    print_operation_guide()

    # ===== 保存结果 =====
    output = {
        'timestamp': now,
        'target_date': TARGET,
        'mode': 'elite_standalone',
        'n': ELITE_N,
        'score_top20': results[:20],
        'strict_picks': [{
            'code': s.get('code', ''), 'name': s.get('name', ''),
            'industry': s['_ind'], 'total': s.get('total', 0),
            'elite_score': round(s.get('_elite_adj', 0), 1),
            'wr2': s['_wr2'], 'mistery': s['_mistery'],
            'r5': s['_r5'], 'is_zt': s['_is_zt'], 'close': s.get('close', 0),
        } for s in strict_picks],
        'wr2_picks': [{
            'code': s.get('code', ''), 'name': s.get('name', ''),
            'industry': s['_ind'], 'total': s.get('total', 0),
            'elite_score': round(s.get('_elite_adj', 0), 1),
        } for s in wr2_picks],
        'overlap_codes': list(overlap),
    }

    # 自定义JSON编码器，处理numpy类型
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)): return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.bool_,)): return bool(obj)
            if isinstance(obj, np.ndarray): return obj.tolist()
            return super().default(obj)

    out_file = os.path.join(BASE_DIR, "精选模式选股结果.json")
    with open(out_file, 'w') as f:
        json.dump(output, f, ensure_ascii=False, indent=2, cls=NpEncoder)
    print(f"\n💾 结果已保存: {out_file}")

    # 同时保存兼容格式的TOP20（供其他脚本使用）
    top20_file = os.path.join(BASE_DIR, "综合评分TOP20.json")
    with open(top20_file, "w") as f:
        json.dump(results[:20], f, ensure_ascii=False, indent=2, cls=NpEncoder)
    print(f"💾 评分TOP20已保存: {top20_file}")

    print(f"\n{'='*130}")
    print(f"✅ 精选模式选股完成！推荐使用【策略一：严格精选】的结果")
    print(f"{'='*130}")


if __name__ == '__main__':
    main()
