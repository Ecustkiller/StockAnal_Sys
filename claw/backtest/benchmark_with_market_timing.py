#!/usr/bin/env python3
"""
市场择时叠加实验（不改 strategy_01/02/03 源文件）
===================================================
【目标】用沪深300+中证1000的大盘状态判断，在大盘恶化时减仓/空仓，
        看能否把 2022 熊市的 -32%/-36%/-34% 回撤打到 -15% 以内。

【核心思想】
  回测时不改变每日选股结果，只改变当日"是否开仓 / 开多少仓"：
    market_state ∈ {RISK_ON, NEUTRAL, RISK_OFF}

  - RISK_ON  : 沪深300 > 年线（MA250） 且 5日动量 > 0 → 满仓 (100%)
  - NEUTRAL  : 沪深300 跌破年线 或 5日动量 < 0 → 半仓 (50%)
  - RISK_OFF : 沪深300 < 年线 且 20日动量 < -5% → 空仓 (0%)

  等价于：backtest 日收益 *= position_ratio

【约束】
  - 不改 strategy_01/02/03 源文件
  - 指数数据用项目统一 tushare 客户端拉取，缓存到 data/cache/

用法：
  python -m claw.backtest.benchmark_with_market_timing
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "backtest_results"
CACHE_DIR = PROJECT_ROOT / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DETAIL_FILE = DATA_DIR / "backtest_v2_detail_20260420_180427.csv"
FACTOR_FILE = DATA_DIR / "aitrader_factors_5year.csv"
INDEX_CACHE = CACHE_DIR / "hs300_20210101_20260420.csv"
OUTPUT_JSON = DATA_DIR / "benchmark_market_timing_compare.json"

sys.path.insert(0, str(PROJECT_ROOT))
from claw.strategies.strategy_01_strict_elite import select_strict_elite
from claw.strategies.strategy_02_mainboard_elite import select_mainboard_strict_elite
from claw.strategies.strategy_03_optimized import select_optimized_elite


# ============================================================
# 1. 拉取并缓存沪深300指数
# ============================================================
def load_hs300_index(start: str = "20210101", end: str = "20260430") -> pd.DataFrame:
    if INDEX_CACHE.exists():
        df = pd.read_csv(INDEX_CACHE, dtype={"trade_date": str})
        return df

    from claw.core.tushare_client import TushareClient
    client = TushareClient(rate_limit_sleep=0.3)

    print(f"📡 拉取 HS300 指数 {start}~{end} ...")
    df = client.call(
        "index_daily",
        ts_code="000300.SH",
        start_date=start,
        end_date=end,
        fields="trade_date,close,pct_chg",
    )
    if df is None or len(df) == 0:
        raise RuntimeError("拉 HS300 失败，请检查 token")

    df = df.sort_values("trade_date").reset_index(drop=True)
    df.to_csv(INDEX_CACHE, index=False)
    print(f"  ✅ 保存 {len(df)} 行 → {INDEX_CACHE}")
    return df


# ============================================================
# 2. 计算每日市场状态 → 仓位比例
# ============================================================
def build_market_state(df_hs: pd.DataFrame) -> pd.DataFrame:
    """
    返回：DataFrame[trade_date, ma250, mom5, mom20, state, position]
    state:    RISK_ON / NEUTRAL / RISK_OFF
    position: 1.0 / 0.5 / 0.0
    """
    d = df_hs.copy()
    d["ma250"] = d["close"].rolling(250, min_periods=60).mean()
    d["mom5"] = d["close"].pct_change(5) * 100
    d["mom20"] = d["close"].pct_change(20) * 100

    def classify(row):
        c, ma, m5, m20 = row["close"], row["ma250"], row["mom5"], row["mom20"]
        if pd.isna(ma) or pd.isna(m20):
            return "RISK_ON", 1.0  # 数据不足默认满仓
        # 强熊：跌破年线 + 20日动量 < -5%
        if c < ma and m20 < -5:
            return "RISK_OFF", 0.0
        # 弱势：跌破年线 or 5日动量 < 0
        if c < ma or m5 < 0:
            return "NEUTRAL", 0.5
        return "RISK_ON", 1.0

    states = d.apply(classify, axis=1)
    d["state"] = states.apply(lambda x: x[0])
    d["position"] = states.apply(lambda x: x[1])
    return d[["trade_date", "close", "ma250", "mom5", "mom20", "state", "position"]]


# ============================================================
# 3. 回测引擎（支持按日仓位缩放）
# ============================================================
def backtest(
    df: pd.DataFrame,
    picker,
    n: int,
    ret_col: str = "ret_1d",
    position_map: dict | None = None,  # {date_str: 0~1}
) -> dict:
    daily_rets, daily_dates, daily_counts = [], [], []
    for date, g in df.groupby("date"):
        picks = picker(g, n)
        if picks is None or len(picks) == 0:
            continue
        rets = picks[ret_col].dropna()
        if len(rets) == 0:
            continue

        raw = float(rets.mean())
        if position_map is not None:
            pos = position_map.get(str(date), 1.0)
            raw = raw * pos
        daily_rets.append(raw)
        daily_dates.append(date)
        daily_counts.append(len(rets))

    if not daily_rets:
        return {"error": "no valid picks"}

    arr = np.array(daily_rets) / 100.0
    nav = np.cumprod(1 + arr)
    years = len(arr) / 250.0
    ann_ret = (nav[-1] ** (1 / max(years, 1e-9)) - 1) * 100
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    max_dd = float(dd.min()) * 100
    std = arr.std(ddof=1)
    sharpe = (arr.mean() / std) * np.sqrt(250) if std > 0 else 0.0
    calmar = ann_ret / abs(max_dd) if max_dd < 0 else 0.0
    wins, losses = arr[arr > 0], arr[arr <= 0]
    win_rate = len(wins) / len(arr) * 100
    profit_ratio = (wins.mean() / abs(losses.mean())) if len(wins) and len(losses) else 0.0

    return {
        "trading_days": len(arr),
        "cum_return_pct": round((nav[-1] - 1) * 100, 2),
        "annual_return_pct": round(ann_ret, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "calmar": round(calmar, 3),
        "win_rate_pct": round(win_rate, 2),
        "profit_ratio": round(profit_ratio, 2),
        "avg_stocks": round(float(np.mean(daily_counts)), 2),
        "_daily_rets": arr.tolist(),
        "_daily_dates": [str(d) for d in daily_dates],
    }


def backtest_by_year(df, picker, n, ret_col="ret_1d", position_map=None):
    df = df.copy()
    df["year"] = df["date"].astype(str).str[:4]
    out = {}
    for year, g in df.groupby("year"):
        res = backtest(g, picker, n, ret_col, position_map)
        if "error" in res:
            continue
        out[year] = {k: v for k, v in res.items() if not k.startswith("_")}
    return out


# ============================================================
# 打印
# ============================================================
def _print_overall(results):
    print("\n" + "=" * 130)
    print("📊 市场择时叠加效果 — 5 年整体")
    print("=" * 130)
    print(
        f"{'策略':<42}{'交易日':>6}{'累计':>10}{'年化':>9}"
        f"{'Sharpe':>8}{'MDD':>9}{'Calmar':>8}{'胜率':>8}"
    )
    print("-" * 130)
    for label, r in results.items():
        if "error" in r:
            print(f"  {label:<40}  ERROR: {r['error']}")
            continue
        print(
            f"{label:<42}{r['trading_days']:>6}"
            f"{r['cum_return_pct']:>+9.1f}%{r['annual_return_pct']:>+8.1f}%"
            f"{r['sharpe']:>8.2f}{r['max_drawdown_pct']:>+8.1f}%"
            f"{r['calmar']:>8.2f}{r['win_rate_pct']:>7.1f}%"
        )


def _print_delta(results, pairs):
    print("\n" + "=" * 130)
    print("📈 市场择时带来的改进（+MT - 原版）")
    print("=" * 130)
    print(
        f"{'策略':<20}"
        f"{'Δ累计':>12}{'Δ年化':>10}{'ΔSharpe':>10}"
        f"{'ΔMDD':>10}{'ΔCalmar':>10}"
    )
    print("-" * 130)
    for base_label, v_label in pairs:
        if base_label not in results or v_label not in results:
            continue
        b, v = results[base_label], results[v_label]
        if "error" in b or "error" in v:
            continue
        print(
            f"{base_label.split()[0]:<20}"
            f"{v['cum_return_pct']-b['cum_return_pct']:>+10.1f}% "
            f"{v['annual_return_pct']-b['annual_return_pct']:>+8.1f}% "
            f"{v['sharpe']-b['sharpe']:>+10.2f}"
            f"{v['max_drawdown_pct']-b['max_drawdown_pct']:>+8.1f}% "
            f"{v['calmar']-b['calmar']:>+10.2f}"
        )


def _print_yearly(yearly):
    print("\n" + "=" * 130)
    print("📊 分年度累计收益")
    print("=" * 130)
    years = sorted({y for res in yearly.values() for y in res.keys()})
    print(f"{'策略':<42}" + "".join(f"{y:>12}" for y in years))
    print("-" * 130)
    for label, yres in yearly.items():
        row = f"{label:<42}"
        for y in years:
            row += f"{yres[y]['cum_return_pct']:>+10.1f}% " if y in yres else f"{'--':>12}"
        print(row)
    print("\n" + "-" * 130)
    print("📉 分年度最大回撤")
    print(f"{'策略':<42}" + "".join(f"{y:>12}" for y in years))
    print("-" * 130)
    for label, yres in yearly.items():
        row = f"{label:<42}"
        for y in years:
            row += f"{yres[y]['max_drawdown_pct']:>+10.1f}% " if y in yres else f"{'--':>12}"
        print(row)


# ============================================================
# 主流程
# ============================================================
def pick_s1(d, n): return select_strict_elite(d, n=n, max_per_ind=2)
def pick_s2(d, n): return select_mainboard_strict_elite(d, n=n, max_per_ind=2)
def pick_s3(d, n): return select_optimized_elite(d, n=n, max_per_ind=2)


def main():
    print("=" * 130)
    print("📡 市场择时叠加实验 — 大盘恶化减仓/空仓")
    print("=" * 130)

    # Step1: HS300 指数 → 市场状态
    hs = load_hs300_index()
    state_df = build_market_state(hs)
    state_df["trade_date"] = state_df["trade_date"].astype(str)
    position_map = dict(zip(state_df["trade_date"], state_df["position"]))

    # 状态占比统计
    print("\n🔎 大盘状态占比（5 年 {} 交易日）".format(len(state_df)))
    for st, cnt in state_df["state"].value_counts().items():
        print(f"   {st:<10} {cnt:>5} 天  ({cnt/len(state_df)*100:5.1f}%)")
    # 分年状态占比
    state_df["year"] = state_df["trade_date"].str[:4]
    print("\n🔎 分年度 RISK_OFF（空仓）天数:")
    for y, g in state_df.groupby("year"):
        off = (g["state"] == "RISK_OFF").sum()
        neu = (g["state"] == "NEUTRAL").sum()
        print(f"   {y}  RISK_OFF={off:3d}  NEUTRAL={neu:3d}  总{len(g)}")

    # Step2: 评分池加载
    df_detail = pd.read_csv(DETAIL_FILE)
    df_detail = df_detail[df_detail["ret_1d"].notna()].copy()
    df_detail["date"] = df_detail["date"].astype(str)

    df_factor = None
    if FACTOR_FILE.exists():
        df_factor = pd.read_csv(FACTOR_FILE)
        df_factor = df_factor[df_factor["ret_1d"].notna()].copy()
        df_factor["date"] = df_factor["date"].astype(str)

    strategies = [
        ("S1-10       策略01 原版",          pick_s1, 10, df_detail, None),
        ("S1-10 +MT   策略01 + 市场择时",     pick_s1, 10, df_detail, position_map),
        ("S2-10       策略02 原版",          pick_s2, 10, df_detail, None),
        ("S2-10 +MT   策略02 + 市场择时",     pick_s2, 10, df_detail, position_map),
    ]
    if df_factor is not None:
        strategies += [
            ("S3-10       策略03 原版",      pick_s3, 10, df_factor, None),
            ("S3-10 +MT   策略03 + 市场择时", pick_s3, 10, df_factor, position_map),
        ]

    results, yearly = {}, {}
    for label, picker, n, df, pmap in strategies:
        print(f"\n  回测 {label} ...")
        res = backtest(df, picker, n, position_map=pmap)
        if "error" in res:
            print(f"    ❌ {res['error']}")
            results[label] = res
            continue
        print(
            f"    累计{res['cum_return_pct']:+.1f}%  "
            f"年化{res['annual_return_pct']:+.1f}%  "
            f"Sharpe={res['sharpe']:.2f}  "
            f"MDD={res['max_drawdown_pct']:.1f}%  "
            f"Calmar={res['calmar']:.2f}"
        )
        results[label] = res
        yearly[label] = backtest_by_year(df, picker, n, position_map=pmap)

    _print_overall(results)
    _print_delta(results, [
        ("S1-10       策略01 原版",   "S1-10 +MT   策略01 + 市场择时"),
        ("S2-10       策略02 原版",   "S2-10 +MT   策略02 + 市场择时"),
        ("S3-10       策略03 原版",   "S3-10 +MT   策略03 + 市场择时"),
    ])
    _print_yearly(yearly)

    to_save = {
        "market_state_summary": {
            str(y): {
                "RISK_OFF": int((g["state"] == "RISK_OFF").sum()),
                "NEUTRAL": int((g["state"] == "NEUTRAL").sum()),
                "RISK_ON": int((g["state"] == "RISK_ON").sum()),
                "total": int(len(g)),
            }
            for y, g in state_df.groupby("year")
        },
        "overall": {
            lbl: {k: v for k, v in r.items() if not k.startswith("_")}
            for lbl, r in results.items()
        },
        "yearly": yearly,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(to_save, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存: {OUTPUT_JSON}")
    print("=" * 130)


if __name__ == "__main__":
    main()
