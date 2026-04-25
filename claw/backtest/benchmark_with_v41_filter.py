#!/usr/bin/env python3
"""
v4.1 附加过滤层实验（不改 strategy_01/02/03 源文件）
============================================================
【目标】验证在策略 01/02/03 外面再套一层"v4.1 风格过滤器"
能否降低 2022 熊市回撤，同时不牺牲牛市收益。

【约束】
  - 不修改 strategy_01_strict_elite.py / strategy_02_mainboard_elite.py
    / strategy_03_optimized.py / score_system.py
  - 仅在 pick() 外层做二次过滤（wrap 模式）
  - 只能使用 detail/factor CSV 里已有的字段

【v4.1 精简过滤器（三层叠加）】
  L1 低波动过滤：|d1|+|d2|+|d3| ≤ 某阈值 → 剔除近 3 日剧烈波动票
  L2 风险收益比：d5 / max(|min(d1..d5)|, 0.5) ≥ 1.2 → 留收益/回撤比好的
  L3 单日暴涨过滤：is_zt==1 且 r5>18%，或 d1>+7% 且 r5>15% → 剔除追高票

【对照组】
  S1-10      策略01 原版                        (baseline)
  S1-10 +V4  策略01 + v4.1 三层过滤
  S2-10      策略02 原版
  S2-10 +V4  策略02 + v4.1 三层过滤
  S3-10      策略03 原版
  S3-10 +V4  策略03 + v4.1 三层过滤

用法：
  python -m claw.backtest.benchmark_with_v41_filter
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ---- 路径 ----
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "backtest_results"
DETAIL_FILE = DATA_DIR / "backtest_v2_detail_20260420_180427.csv"
FACTOR_FILE = DATA_DIR / "aitrader_factors_5year.csv"
OUTPUT_JSON = DATA_DIR / "benchmark_v41_filter_compare.json"

sys.path.insert(0, str(PROJECT_ROOT))
from claw.strategies.strategy_01_strict_elite import select_strict_elite
from claw.strategies.strategy_02_mainboard_elite import select_mainboard_strict_elite
from claw.strategies.strategy_03_optimized import select_optimized_elite


# ============================================================
# v4.1 精简过滤器（纯 DataFrame 操作，零侵入）
# ============================================================
def v41_filter(df: pd.DataFrame,
               max_vol_sum: float = 18.0,
               min_rr: float = 1.2,
               zt_r5_cut: float = 18.0,
               d1_cut: float = 7.0,
               d1_r5_cut: float = 15.0) -> pd.DataFrame:
    """
    在一批候选标的上应用 v4.1 三层过滤

    L1 低波动：|d1|+|d2|+|d3| ≤ max_vol_sum
    L2 风险收益比：d5 / max(|min(d1..d5)|, 0.5) ≥ min_rr
    L3 单日暴涨：剔除 (is_zt==1 且 r5>zt_r5_cut) 或 (d1>d1_cut 且 r5>d1_r5_cut)
    """
    if df is None or len(df) == 0:
        return df
    d = df.copy()

    # L1 低波动
    vol_sum = d[["d1", "d2", "d3"]].abs().sum(axis=1)
    d = d[vol_sum <= max_vol_sum]
    if len(d) == 0:
        return d

    # L2 风险收益比
    d5 = d["d5"].fillna(0)
    worst = d[["d1", "d2", "d3", "d4", "d5"]].min(axis=1).abs().clip(lower=0.5)
    rr = d5 / worst
    d = d[rr >= min_rr]
    if len(d) == 0:
        return d

    # L3 单日暴涨
    is_zt = d.get("is_zt", 0)
    if not isinstance(is_zt, pd.Series):
        is_zt = pd.Series(0, index=d.index)
    r5 = d.get("r5", pd.Series(0, index=d.index)).fillna(0)
    d1 = d.get("d1", pd.Series(0, index=d.index)).fillna(0)
    bad_zt = (is_zt == 1) & (r5 > zt_r5_cut)
    bad_d1 = (d1 > d1_cut) & (r5 > d1_r5_cut)
    d = d[~(bad_zt | bad_d1)]

    return d


def make_v41_picker(base_picker, n_target: int):
    """
    把一个基础 picker 包装成先过滤再选股的 picker：
      1. 用 base_picker 拿到初选集（适当放大到 n_target*2）
      2. 用 v41_filter 过滤
      3. 如果过滤后不足 n_target，则从原始初选集里按 total 补齐
    """
    def wrapped(day_df: pd.DataFrame, n: int = n_target) -> pd.DataFrame:
        # Step1: 先用原策略选出扩展池（放大倍数）
        base = base_picker(day_df, n_target * 2)
        if base is None or len(base) == 0:
            return base
        # Step2: v4.1 过滤
        filt = v41_filter(base)
        # Step3: 取前 n_target
        if len(filt) >= n_target:
            return filt.nlargest(n_target, "total")
        # 不足则从原 base 补齐（按 total 排）
        need = n_target - len(filt)
        rest = base[~base.index.isin(filt.index)].nlargest(need, "total")
        return pd.concat([filt, rest]).head(n_target)

    return wrapped


# ============================================================
# 回测引擎（同 benchmark_score_vs_strategies.py）
# ============================================================
def backtest(df: pd.DataFrame, picker, n: int, ret_col: str = "ret_1d") -> dict:
    daily_rets, daily_dates, daily_counts = [], [], []
    for date, g in df.groupby("date"):
        picks = picker(g, n)
        if picks is None or len(picks) == 0:
            continue
        rets = picks[ret_col].dropna()
        if len(rets) == 0:
            continue
        daily_rets.append(float(rets.mean()))
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


def backtest_by_year(df, picker, n, ret_col="ret_1d"):
    df = df.copy()
    df["year"] = df["date"].astype(str).str[:4]
    out = {}
    for year, g in df.groupby("year"):
        res = backtest(g, picker, n, ret_col)
        if "error" in res:
            continue
        out[year] = {k: v for k, v in res.items() if not k.startswith("_")}
    return out


# ============================================================
# 打印
# ============================================================
def _print_overall(results):
    print("\n" + "=" * 130)
    print("📊 v4.1 三层过滤器叠加效果 — 5 年整体")
    print("=" * 130)
    print(
        f"{'策略':<38}{'交易日':>6}{'累计':>10}{'年化':>9}"
        f"{'Sharpe':>8}{'MDD':>9}{'Calmar':>8}{'胜率':>8}{'均持股':>8}"
    )
    print("-" * 130)
    for label, r in results.items():
        if "error" in r:
            print(f"  {label:<36}  ERROR: {r['error']}")
            continue
        print(
            f"{label:<38}{r['trading_days']:>6}"
            f"{r['cum_return_pct']:>+9.1f}%{r['annual_return_pct']:>+8.1f}%"
            f"{r['sharpe']:>8.2f}{r['max_drawdown_pct']:>+8.1f}%"
            f"{r['calmar']:>8.2f}{r['win_rate_pct']:>7.1f}%"
            f"{r['avg_stocks']:>8.1f}"
        )


def _print_delta(results, pairs):
    """打印 原版 vs V4 过滤 的差值"""
    print("\n" + "=" * 130)
    print("📈 v4.1 过滤器带来的改进（+V4 - 原版）")
    print("=" * 130)
    print(
        f"{'策略对':<38}"
        f"{'Δ累计':>12}{'Δ年化':>10}{'ΔSharpe':>10}"
        f"{'ΔMDD':>10}{'ΔCalmar':>10}{'Δ胜率':>10}"
    )
    print("-" * 130)
    for base_label, v4_label in pairs:
        if base_label not in results or v4_label not in results:
            continue
        b, v = results[base_label], results[v4_label]
        if "error" in b or "error" in v:
            continue
        print(
            f"{base_label.split()[0]:<38}"
            f"{v['cum_return_pct']-b['cum_return_pct']:>+10.1f}% "
            f"{v['annual_return_pct']-b['annual_return_pct']:>+8.1f}% "
            f"{v['sharpe']-b['sharpe']:>+10.2f}"
            f"{v['max_drawdown_pct']-b['max_drawdown_pct']:>+8.1f}% "
            f"{v['calmar']-b['calmar']:>+10.2f}"
            f"{v['win_rate_pct']-b['win_rate_pct']:>+8.1f}%"
        )


def _print_yearly(yearly):
    print("\n" + "=" * 130)
    print("📊 分年度累计收益（关注 2022 熊市救援效果）")
    print("=" * 130)
    years = sorted({y for res in yearly.values() for y in res.keys()})
    print(f"{'策略':<38}" + "".join(f"{y:>12}" for y in years))
    print("-" * 130)
    for label, yres in yearly.items():
        row = f"{label:<38}"
        for y in years:
            row += f"{yres[y]['cum_return_pct']:>+10.1f}% " if y in yres else f"{'--':>12}"
        print(row)
    print("\n" + "-" * 130)
    print("📉 分年度最大回撤")
    print(f"{'策略':<38}" + "".join(f"{y:>12}" for y in years))
    print("-" * 130)
    for label, yres in yearly.items():
        row = f"{label:<38}"
        for y in years:
            row += f"{yres[y]['max_drawdown_pct']:>+10.1f}% " if y in yres else f"{'--':>12}"
        print(row)


# ============================================================
# 原子 picker
# ============================================================
def pick_s1(d, n): return select_strict_elite(d, n=n, max_per_ind=2)
def pick_s2(d, n): return select_mainboard_strict_elite(d, n=n, max_per_ind=2)
def pick_s3(d, n): return select_optimized_elite(d, n=n, max_per_ind=2)


# ============================================================
# 主程序
# ============================================================
def main():
    print("=" * 130)
    print("🧪 v4.1 附加过滤层实验 — 不改源文件，仅 wrap")
    print("=" * 130)

    df_detail = pd.read_csv(DETAIL_FILE)
    df_detail = df_detail[df_detail["ret_1d"].notna()].copy()
    print(
        f"\n📂 评分池: {len(df_detail):,} 行 / "
        f"{df_detail['date'].nunique()} 天 "
        f"({df_detail['date'].min()} ~ {df_detail['date'].max()})"
    )

    has_factor = FACTOR_FILE.exists()
    df_factor = None
    if has_factor:
        df_factor = pd.read_csv(FACTOR_FILE)
        df_factor = df_factor[df_factor["ret_1d"].notna()].copy()
        print(f"📂 因子池: {len(df_factor):,} 行")

    strategies = [
        ("S1-10       策略01 原版",                pick_s1,                        10, df_detail),
        ("S1-10 +V4   策略01 + v4.1 三层过滤",     make_v41_picker(pick_s1, 10),   10, df_detail),
        ("S2-10       策略02 原版",                pick_s2,                        10, df_detail),
        ("S2-10 +V4   策略02 + v4.1 三层过滤",     make_v41_picker(pick_s2, 10),   10, df_detail),
    ]
    if has_factor:
        strategies += [
            ("S3-10       策略03 原版",            pick_s3,                        10, df_factor),
            ("S3-10 +V4   策略03 + v4.1 三层过滤", make_v41_picker(pick_s3, 10),   10, df_factor),
        ]

    results, yearly = {}, {}
    for label, picker, n, df in strategies:
        print(f"\n  回测 {label} ...")
        res = backtest(df, picker, n)
        if "error" in res:
            print(f"    ❌ {res['error']}")
            results[label] = res
            continue
        print(
            f"    累计{res['cum_return_pct']:+.1f}%  "
            f"年化{res['annual_return_pct']:+.1f}%  "
            f"Sharpe={res['sharpe']:.2f}  "
            f"MDD={res['max_drawdown_pct']:.1f}%  "
            f"Calmar={res['calmar']:.2f}  "
            f"均持股{res['avg_stocks']:.1f}"
        )
        results[label] = res
        yearly[label] = backtest_by_year(df, picker, n)

    _print_overall(results)
    _print_delta(results, [
        ("S1-10       策略01 原版",   "S1-10 +V4   策略01 + v4.1 三层过滤"),
        ("S2-10       策略02 原版",   "S2-10 +V4   策略02 + v4.1 三层过滤"),
        ("S3-10       策略03 原版",   "S3-10 +V4   策略03 + v4.1 三层过滤"),
    ])
    _print_yearly(yearly)

    # 保存结果
    to_save = {
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
