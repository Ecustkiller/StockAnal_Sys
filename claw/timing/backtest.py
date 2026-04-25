"""
claw.timing.backtest — 多因子择时 × 策略 S1/S2/S3 叠加回测
===============================================================
不改 strategy_01/02/03 源文件；只在每日收益上乘以 position_ratio。

对比组：
    - 原版（无择时）
    - 单因子择时（benchmark_with_market_timing.py 已有：MA250+动量，3档）
    - 多因子择时（claw.timing.composer 5 因子等权投票，4档）

用法：
    python -m claw.timing.backtest
    python -m claw.timing.backtest --sentiment      # 启用涨停情绪（需拉数据）
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "backtest_results"
DETAIL_FILE = DATA_DIR / "backtest_v2_detail_20260420_180427.csv"
FACTOR_FILE = DATA_DIR / "aitrader_factors_5year.csv"
OUTPUT_JSON = DATA_DIR / "timing_multi_factor_compare.json"

sys.path.insert(0, str(PROJECT_ROOT))
from claw.strategies.strategy_01_strict_elite import select_strict_elite
from claw.strategies.strategy_02_mainboard_elite import select_mainboard_strict_elite
from claw.strategies.strategy_03_optimized import select_optimized_elite
from claw.timing.composer import compute_market_timing


# ============================================================
# 单因子择时参考（与 benchmark_with_market_timing.py 等价）
# ============================================================
def build_single_factor_state(start="20210101", end="20260420") -> dict[str, float]:
    """原有单因子择时：HS300 年线 + 5/20 日动量 → 3 档仓位"""
    from claw.timing.data import load_index_daily
    hs = load_index_daily("000300.SH", start, end)
    d = hs.copy()
    d["ma250"] = d["close"].rolling(250, min_periods=60).mean()
    d["mom5"] = d["close"].pct_change(5) * 100
    d["mom20"] = d["close"].pct_change(20) * 100

    def classify(row):
        c, ma, m5, m20 = row["close"], row["ma250"], row["mom5"], row["mom20"]
        if pd.isna(ma) or pd.isna(m20):
            return 1.0
        if c < ma and m20 < -5:
            return 0.0     # RISK_OFF
        if c < ma or m5 < 0:
            return 0.5     # NEUTRAL
        return 1.0          # RISK_ON

    d["pos"] = d.apply(classify, axis=1)
    d["trade_date"] = d["trade_date"].astype(str)
    return dict(zip(d["trade_date"], d["pos"]))


def build_multi_factor_state(start="20210101", end="20260420",
                              use_sentiment=False,
                              use_breadth=False) -> dict[str, float]:
    """新的多因子择时：5 因子等权投票 → 4 档仓位"""
    df = compute_market_timing(
        start=start, end=end,
        use_sentiment=use_sentiment,
        use_breadth=use_breadth,
    )
    return dict(zip(df["trade_date"].astype(str), df["position"]))


# ============================================================
# 回测引擎（与 benchmark_with_market_timing.py 一致）
# ============================================================
def backtest(df, picker, n, ret_col="ret_1d", position_map=None):
    # ============================================================
    # 🔑 T+1 执行修复（2026-04-21）
    # position_map[date] 是用 date 当日 EOD 数据算出的仓位；
    # 真实交易中只能在 date+1 开盘按新仓位调仓，
    # 所以乘到 date+1 的 ret_1d 上才不构成未来数据泄露。
    # 这里构造 prev_pos[date] = position_map[前一个交易日]。
    # ============================================================
    prev_pos_map = None
    if position_map is not None:
        sorted_dates = sorted(position_map.keys())
        prev_pos_map = {}
        for i, d in enumerate(sorted_dates):
            if i == 0:
                prev_pos_map[d] = 1.0  # 首日无历史信号，保守按满仓
            else:
                prev_pos_map[d] = position_map[sorted_dates[i - 1]]

    daily_rets, daily_dates = [], []
    for date, g in df.groupby("date"):
        picks = picker(g, n)
        if picks is None or len(picks) == 0:
            continue
        rets = picks[ret_col].dropna()
        if len(rets) == 0:
            continue
        raw = float(rets.mean())
        if prev_pos_map is not None:
            raw = raw * prev_pos_map.get(str(date), 1.0)
        daily_rets.append(raw)
        daily_dates.append(date)

    if not daily_rets:
        return {"error": "no valid picks"}

    arr = np.array(daily_rets) / 100.0
    nav = np.cumprod(1 + arr)
    years = len(arr) / 250.0
    ann = (nav[-1] ** (1 / max(years, 1e-9)) - 1) * 100
    peak = np.maximum.accumulate(nav)
    mdd = float(((nav - peak) / peak).min()) * 100
    std = arr.std(ddof=1)
    sharpe = (arr.mean() / std) * np.sqrt(250) if std > 0 else 0.0
    calmar = ann / abs(mdd) if mdd < 0 else 0.0
    wins, losses = arr[arr > 0], arr[arr <= 0]
    win_rate = len(wins) / len(arr) * 100
    pr = wins.mean() / abs(losses.mean()) if len(wins) and len(losses) else 0.0

    return {
        "trading_days": len(arr),
        "cum_return_pct": round((nav[-1] - 1) * 100, 2),
        "annual_return_pct": round(ann, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(mdd, 2),
        "calmar": round(calmar, 3),
        "win_rate_pct": round(win_rate, 2),
        "profit_ratio": round(pr, 2),
    }


def backtest_by_year(df, picker, n, position_map=None):
    d = df.copy()
    d["year"] = d["date"].astype(str).str[:4]
    out = {}
    for y, g in d.groupby("year"):
        r = backtest(g, picker, n, position_map=position_map)
        if "error" in r:
            continue
        out[y] = r
    return out


# ============================================================
# 打印
# ============================================================
def print_overall(results):
    print("\n" + "=" * 130)
    print("📊 多因子择时 × S1/S2/S3 叠加效果 — 5 年整体")
    print("=" * 130)
    print(f"{'策略':<42}{'交易日':>6}{'累计':>12}{'年化':>10}"
          f"{'Sharpe':>8}{'MDD':>10}{'Calmar':>9}{'胜率':>8}")
    print("-" * 130)
    for label, r in results.items():
        if "error" in r:
            print(f"  {label:<40}  ERROR: {r['error']}")
            continue
        print(f"{label:<42}{r['trading_days']:>6}"
              f"{r['cum_return_pct']:>+11.1f}%{r['annual_return_pct']:>+9.1f}%"
              f"{r['sharpe']:>8.2f}{r['max_drawdown_pct']:>+9.1f}%"
              f"{r['calmar']:>9.2f}{r['win_rate_pct']:>7.1f}%")


def print_delta(results, triplets):
    print("\n" + "=" * 130)
    print("📈 择时的改进（相对于原版）")
    print("=" * 130)
    print(f"{'策略':<12}{'变体':<12}"
          f"{'Δ累计':>11}{'Δ年化':>10}{'ΔSharpe':>10}"
          f"{'ΔMDD':>10}{'ΔCalmar':>10}")
    print("-" * 130)
    for base_label, mt1_label, mt2_label in triplets:
        if base_label not in results:
            continue
        b = results[base_label]
        for tag, lbl in [("单因子MT", mt1_label), ("多因子MT", mt2_label)]:
            if lbl not in results:
                continue
            v = results[lbl]
            if "error" in b or "error" in v:
                continue
            strat_tag = base_label.split()[0]
            print(f"{strat_tag:<12}{tag:<12}"
                  f"{v['cum_return_pct']-b['cum_return_pct']:>+10.1f}%"
                  f"{v['annual_return_pct']-b['annual_return_pct']:>+9.1f}%"
                  f"{v['sharpe']-b['sharpe']:>+10.2f}"
                  f"{v['max_drawdown_pct']-b['max_drawdown_pct']:>+9.1f}%"
                  f"{v['calmar']-b['calmar']:>+10.2f}")


def print_yearly(yearly):
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--sentiment", action="store_true",
                    help="启用涨停情绪因子（首次需拉 tushare，约 10 分钟）")
    ap.add_argument("--breadth", action="store_true")
    args = ap.parse_args()

    print("=" * 130)
    print("📡 多因子大盘择时 × 选股策略 叠加回测")
    print(f"    MT1 = 单因子（MA250+动量，3档仓位）")
    print(f"    MT2 = 多因子（RSRS+趋势+波动率"
          f"{'+涨停情绪' if args.sentiment else ''}"
          f"{'+市场宽度' if args.breadth else ''}，4档仓位）")
    print("=" * 130)

    # 构建仓位 map
    print("\n🔧 计算仓位序列 ...")
    pos_mt1 = build_single_factor_state()
    print(f"   单因子仓位分布: {pd.Series(list(pos_mt1.values())).value_counts().to_dict()}")
    pos_mt2 = build_multi_factor_state(
        use_sentiment=args.sentiment,
        use_breadth=args.breadth,
    )
    print(f"   多因子仓位分布: {pd.Series(list(pos_mt2.values())).value_counts().to_dict()}")

    # 加载回测数据
    df_detail = pd.read_csv(DETAIL_FILE)
    df_detail = df_detail[df_detail["ret_1d"].notna()].copy()
    df_detail["date"] = df_detail["date"].astype(str)

    df_factor = None
    if FACTOR_FILE.exists():
        df_factor = pd.read_csv(FACTOR_FILE)
        df_factor = df_factor[df_factor["ret_1d"].notna()].copy()
        df_factor["date"] = df_factor["date"].astype(str)

    # 组装对比列表
    configs = [
        ("S1-10        策略01 原版",          pick_s1, 10, df_detail, None),
        ("S1-10 +MT1   策略01 + 单因子择时",  pick_s1, 10, df_detail, pos_mt1),
        ("S1-10 +MT2   策略01 + 多因子择时",  pick_s1, 10, df_detail, pos_mt2),
        ("S2-10        策略02 原版",          pick_s2, 10, df_detail, None),
        ("S2-10 +MT1   策略02 + 单因子择时",  pick_s2, 10, df_detail, pos_mt1),
        ("S2-10 +MT2   策略02 + 多因子择时",  pick_s2, 10, df_detail, pos_mt2),
    ]
    if df_factor is not None:
        configs += [
            ("S3-10        策略03 原版",         pick_s3, 10, df_factor, None),
            ("S3-10 +MT1   策略03 + 单因子择时", pick_s3, 10, df_factor, pos_mt1),
            ("S3-10 +MT2   策略03 + 多因子择时", pick_s3, 10, df_factor, pos_mt2),
        ]

    results, yearly = {}, {}
    for label, picker, n, df, pmap in configs:
        print(f"\n  回测 {label} ...")
        res = backtest(df, picker, n, position_map=pmap)
        if "error" in res:
            print(f"    ❌ {res['error']}")
            results[label] = res
            continue
        print(f"    累计{res['cum_return_pct']:+.1f}%  "
              f"年化{res['annual_return_pct']:+.1f}%  "
              f"Sharpe={res['sharpe']:.2f}  "
              f"MDD={res['max_drawdown_pct']:.1f}%  "
              f"Calmar={res['calmar']:.2f}")
        results[label] = res
        yearly[label] = backtest_by_year(df, picker, n, position_map=pmap)

    print_overall(results)
    print_delta(results, [
        ("S1-10        策略01 原版",   "S1-10 +MT1   策略01 + 单因子择时",   "S1-10 +MT2   策略01 + 多因子择时"),
        ("S2-10        策略02 原版",   "S2-10 +MT1   策略02 + 单因子择时",   "S2-10 +MT2   策略02 + 多因子择时"),
        ("S3-10        策略03 原版",   "S3-10 +MT1   策略03 + 单因子择时",   "S3-10 +MT2   策略03 + 多因子择时"),
    ])
    print_yearly(yearly)

    with open(OUTPUT_JSON, "w") as f:
        json.dump({"overall": results, "yearly": yearly}, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存: {OUTPUT_JSON}")
    print("=" * 130)


if __name__ == "__main__":
    main()
