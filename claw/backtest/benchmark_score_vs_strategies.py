#!/usr/bin/env python3
"""
5 年横评：score_system 裸 TOP vs 策略 01/02/03
============================================================
【目标】回答一个问题："score_system 直接取 TOP5/TOP10 的效果，
和走过一层过滤器的策略 01/02/03 相比，5 年全周期究竟谁强？"

【数据】
  - 评分池: data/backtest_results/backtest_v2_detail_20260420_180427.csv
           2021.01.04 ~ 2026.04.10, 1275 交易日, 每天 TOP20
  - 因子池: data/backtest_results/aitrader_factors_5year.csv (策略03专用)
  - 收益字段: ret_1d (T+1 买入开盘价 → T+1 收盘卖出)

【参与对比的策略】
  ┌──────────────────────────────────────────────────────────┐
  │ S0-5   score_system 裸 TOP5（按 total 排序）              │
  │ S0-10  score_system 裸 TOP10（按 total 排序）             │
  │ S1-5   策略01 严格精选 TOP5                                │
  │ S1-10  策略01 严格精选 TOP10                               │
  │ S2-10  策略02 主板严格精选 TOP10                           │
  │ S3-10  策略03 aiTrader 因子增强 TOP10                      │
  └──────────────────────────────────────────────────────────┘

【输出】
  - 整体 5 年指标对比表
  - 分年度指标对比表（2021-2026）
  - 结果 JSON 存盘：data/backtest_results/benchmark_5y_compare.json

用法：
  python -m claw.backtest.benchmark_score_vs_strategies
"""
from __future__ import annotations

import json
import os
import sys
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ---- 路径 ----
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "backtest_results"
DETAIL_FILE = DATA_DIR / "backtest_v2_detail_20260420_180427.csv"
FACTOR_FILE = DATA_DIR / "aitrader_factors_5year.csv"
OUTPUT_JSON = DATA_DIR / "benchmark_5y_compare.json"

# ---- 复用策略函数 ----
sys.path.insert(0, str(PROJECT_ROOT))
from claw.strategies.strategy_01_strict_elite import select_strict_elite
from claw.strategies.strategy_02_mainboard_elite import select_mainboard_strict_elite
from claw.strategies.strategy_03_optimized import select_optimized_elite


# ============================================================
# 选股函数
# ============================================================
def pick_score_topn(day_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """score_system 裸 TOP-N：按 total 降序取前 n 只"""
    return day_df.nlargest(n, "total").copy()


def pick_s1(day_df: pd.DataFrame, n: int) -> pd.DataFrame:
    return select_strict_elite(day_df, n=n, max_per_ind=2)


def pick_s2(day_df: pd.DataFrame, n: int) -> pd.DataFrame:
    return select_mainboard_strict_elite(day_df, n=n, max_per_ind=2)


def pick_s3(day_df: pd.DataFrame, n: int) -> pd.DataFrame:
    return select_optimized_elite(day_df, n=n, max_per_ind=2)


# ============================================================
# 回测引擎（等权 + 每日换仓 T+1）
# ============================================================
def backtest(df: pd.DataFrame, picker, n: int, ret_col: str = "ret_1d") -> dict:
    """
    按日选股，每日等权持仓 1 天，收益 = 选中 n 只的 ret_1d 均值

    返回指标字典 + 每日收益序列
    """
    daily_rets = []
    daily_dates = []
    daily_counts = []

    for date, g in df.groupby("date"):
        picks = picker(g, n)
        if picks is None or len(picks) == 0:
            continue
        # 过滤掉 ret_1d 为 NaN 的
        rets = picks[ret_col].dropna()
        if len(rets) == 0:
            continue
        daily_rets.append(float(rets.mean()))
        daily_dates.append(date)
        daily_counts.append(len(rets))

    if not daily_rets:
        return {"error": "no valid picks"}

    arr = np.array(daily_rets) / 100.0  # pct → 小数
    n_days = len(arr)

    # 累计净值
    nav = np.cumprod(1 + arr)
    cum_ret = (nav[-1] - 1) * 100

    # 年化收益
    years = n_days / 250.0
    ann_ret = (nav[-1] ** (1 / max(years, 1e-9)) - 1) * 100

    # 最大回撤
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    max_dd = float(dd.min()) * 100  # 负数

    # Sharpe（日频，年化 = √250）
    std = arr.std(ddof=1)
    sharpe = (arr.mean() / std) * np.sqrt(250) if std > 0 else 0.0

    # Calmar
    calmar = ann_ret / abs(max_dd) if max_dd < 0 else 0.0

    # 胜率 / 盈亏比
    wins = arr[arr > 0]
    losses = arr[arr <= 0]
    win_rate = len(wins) / len(arr) * 100
    profit_ratio = (
        (wins.mean() / abs(losses.mean())) if len(wins) and len(losses) else 0.0
    )

    return {
        "trading_days": n_days,
        "cum_return_pct": round(cum_ret, 2),
        "annual_return_pct": round(ann_ret, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "calmar": round(calmar, 3),
        "win_rate_pct": round(win_rate, 2),
        "profit_ratio": round(profit_ratio, 2),
        "avg_stocks": round(float(np.mean(daily_counts)), 2),
        "daily_rets": arr.tolist(),
        "daily_dates": [str(d) for d in daily_dates],
    }


def backtest_by_year(df: pd.DataFrame, picker, n: int, ret_col: str = "ret_1d") -> dict:
    """分年度回测"""
    df = df.copy()
    df["year"] = df["date"].astype(str).str[:4]
    out = {}
    for year, g in df.groupby("year"):
        res = backtest(g, picker, n, ret_col)
        if "error" in res:
            continue
        out[year] = {
            k: v for k, v in res.items() if k not in ("daily_rets", "daily_dates")
        }
    return out


# ============================================================
# 打印
# ============================================================
def print_overall_table(results: dict):
    print("\n" + "=" * 120)
    print("📊 5 年整体表现对比（2021.01.04 ~ 2026.04.10，1275 交易日，T+1 每日换仓，等权）")
    print("=" * 120)
    header = (
        f"{'策略':<38}{'交易日':>6}{'累计':>10}{'年化':>9}"
        f"{'Sharpe':>8}{'最大回撤':>10}{'Calmar':>8}"
        f"{'胜率':>8}{'盈亏比':>8}{'均持股':>8}"
    )
    print(header)
    print("-" * 120)
    for label, r in results.items():
        if "error" in r:
            print(f"  {label:<36}  ERROR: {r['error']}")
            continue
        print(
            f"{label:<38}{r['trading_days']:>6}"
            f"{r['cum_return_pct']:>+9.1f}%{r['annual_return_pct']:>+8.1f}%"
            f"{r['sharpe']:>8.2f}{r['max_drawdown_pct']:>+9.1f}%"
            f"{r['calmar']:>8.2f}{r['win_rate_pct']:>7.1f}%"
            f"{r['profit_ratio']:>8.2f}{r['avg_stocks']:>8.1f}"
        )


def print_yearly_table(yearly: dict):
    print("\n" + "=" * 120)
    print("📊 分年度累计收益对比（每年单独复利）")
    print("=" * 120)
    # 汇总所有年份
    years = sorted({y for res in yearly.values() for y in res.keys()})
    header = f"{'策略':<38}" + "".join(f"{y:>12}" for y in years)
    print(header)
    print("-" * 120)
    for label, yres in yearly.items():
        row = f"{label:<38}"
        for y in years:
            if y in yres:
                row += f"{yres[y]['cum_return_pct']:>+10.1f}% "
            else:
                row += f"{'--':>12}"
        print(row)

    print("\n" + "-" * 120)
    print(f"{'策略':<38}" + "".join(f"{y+'MDD':>12}" for y in years))
    print("-" * 120)
    for label, yres in yearly.items():
        row = f"{label:<38}"
        for y in years:
            if y in yres:
                row += f"{yres[y]['max_drawdown_pct']:>+10.1f}% "
            else:
                row += f"{'--':>12}"
        print(row)


# ============================================================
# 主程序
# ============================================================
def main():
    print("=" * 120)
    print("🎯 score_system 裸 TOP vs 策略 01/02/03 — 5 年横评")
    print("=" * 120)

    # ---- 加载数据 ----
    if not DETAIL_FILE.exists():
        print(f"❌ 找不到 detail 数据：{DETAIL_FILE}")
        sys.exit(1)

    print(f"\n📂 加载评分池：{DETAIL_FILE.name}")
    df_detail = pd.read_csv(DETAIL_FILE)
    df_detail = df_detail[df_detail["ret_1d"].notna()].copy()
    print(
        f"   样本: {len(df_detail):,} 行 | 日期: {df_detail['date'].nunique()} 天 "
        f"({df_detail['date'].min()} ~ {df_detail['date'].max()})"
    )

    has_factor_file = FACTOR_FILE.exists()
    if has_factor_file:
        print(f"📂 加载因子池：{FACTOR_FILE.name}")
        df_factor = pd.read_csv(FACTOR_FILE)
        df_factor = df_factor[df_factor["ret_1d"].notna()].copy()
        print(
            f"   样本: {len(df_factor):,} 行 | 日期: {df_factor['date'].nunique()} 天"
        )
    else:
        print(f"⚠️ 未找到 aiTrader 因子文件，策略03 将被跳过")
        df_factor = None

    # 策略01/02 需要 industry 字段，detail 里叫 industry 没问题
    # 策略03 需要 aitrader 因子字段，必须用 df_factor

    # ---- 配置参与对比的策略 ----
    strategies = [
        ("S0-5   score_system 裸 TOP5",
            lambda d, n=5: pick_score_topn(d, 5), 5, df_detail),
        ("S0-10  score_system 裸 TOP10",
            lambda d, n=10: pick_score_topn(d, 10), 10, df_detail),
        ("S1-5   策略01 严格精选 TOP5",
            lambda d, n=5: pick_s1(d, 5), 5, df_detail),
        ("S1-10  策略01 严格精选 TOP10",
            lambda d, n=10: pick_s1(d, 10), 10, df_detail),
        ("S2-10  策略02 主板严格精选 TOP10",
            lambda d, n=10: pick_s2(d, 10), 10, df_detail),
    ]
    if has_factor_file:
        strategies.append(
            ("S3-10  策略03 aiTrader 因子增强 TOP10",
                lambda d, n=10: pick_s3(d, 10), 10, df_factor)
        )

    # ---- 跑整体回测 ----
    results = {}
    yearly = {}
    for label, picker, n, df in strategies:
        print(f"\n  回测 {label} ...")
        res = backtest(df, picker, n)
        if "error" in res:
            print(f"    ❌ {res['error']}")
            results[label] = res
            continue
        print(
            f"    累计{res['cum_return_pct']:+.1f}% "
            f"年化{res['annual_return_pct']:+.1f}% "
            f"Sharpe={res['sharpe']:.2f} "
            f"MDD={res['max_drawdown_pct']:.1f}% "
            f"Calmar={res['calmar']:.2f}"
        )
        results[label] = res
        yearly[label] = backtest_by_year(df, picker, n)

    # ---- 打印对比表 ----
    print_overall_table(results)
    print_yearly_table(yearly)

    # ---- 排名 & 结论 ----
    print("\n" + "=" * 120)
    print("🏆 综合排名")
    print("=" * 120)
    valid = {k: v for k, v in results.items() if "error" not in v}
    # 按累计收益
    print("\n  📈 按 5 年累计收益:")
    for i, (lbl, r) in enumerate(sorted(valid.items(), key=lambda x: -x[1]["cum_return_pct"]), 1):
        tag = "🥇🥈🥉"[i - 1] if i <= 3 else "  "
        print(f"    {tag} {i}. {lbl:<38} {r['cum_return_pct']:+8.1f}%")

    # 按 Sharpe
    print("\n  ⚖️  按 Sharpe 比率:")
    for i, (lbl, r) in enumerate(sorted(valid.items(), key=lambda x: -x[1]["sharpe"]), 1):
        tag = "🥇🥈🥉"[i - 1] if i <= 3 else "  "
        print(f"    {tag} {i}. {lbl:<38} Sharpe={r['sharpe']:.3f}")

    # 按 Calmar
    print("\n  🛡️  按 Calmar (收益/回撤):")
    for i, (lbl, r) in enumerate(sorted(valid.items(), key=lambda x: -x[1]["calmar"]), 1):
        tag = "🥇🥈🥉"[i - 1] if i <= 3 else "  "
        print(f"    {tag} {i}. {lbl:<38} Calmar={r['calmar']:.3f}")

    # ---- 保存 JSON ----
    to_save = {
        "data_source": str(DETAIL_FILE),
        "overall": {
            lbl: {k: v for k, v in r.items() if k not in ("daily_rets", "daily_dates")}
            for lbl, r in results.items()
        },
        "yearly": yearly,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(to_save, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存: {OUTPUT_JSON}")
    print("\n" + "=" * 120)


if __name__ == "__main__":
    main()
