"""
claw.timing.market_timer_eval — MarketTimer 诊断与回测

指标：
    1. 单信号 IC（Spearman 与未来 1/5/20 日收益的秩相关）+ 方向准确率
    2. 单信号 → 分层收益（0/1/2 三组的平均未来收益）
    3. 综合仓位回测：
        - 累计收益、年化、Sharpe、最大回撤、Calmar
        - α = Timing - BuyHold
        - 分年度 / 分牛熊阶段
    4. 样本内 / 样本外 对比（前 70% 训练，后 30% 验证）

关键纪律：
    ✔ 所有收益用 position_exec（信号 shift(1) 后的仓位）
    ✔ 分位数等参数在 market_timer 内部已用滚动窗口，无 look-ahead
    ✔ 样本外是严格未见过的
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from claw.timing.market_timer import MarketTimer, MAPPING_PRESETS
from claw.timing import data as tdata

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = PROJECT_ROOT / "data" / "backtest_results"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TRADING_DAYS = 250
CASH_DAILY = (1.0 + 0.02) ** (1 / TRADING_DAYS) - 1  # 2% 年化现金收益


# ================================================================
# 回测指标
# ================================================================
def perf_metrics(daily_ret: np.ndarray, label: str = "") -> dict:
    nav = np.cumprod(1 + daily_ret)
    years = len(daily_ret) / TRADING_DAYS
    ann = (nav[-1] ** (1 / max(years, 1e-9)) - 1) * 100
    peak = np.maximum.accumulate(nav)
    mdd = float(((nav - peak) / peak).min()) * 100
    std = daily_ret.std(ddof=1)
    sharpe = daily_ret.mean() / std * np.sqrt(TRADING_DAYS) if std > 0 else 0.0
    calmar = ann / abs(mdd) if mdd < 0 else 0.0
    wins = daily_ret[daily_ret > 0]
    win_rate = len(wins) / len(daily_ret) * 100
    return {
        "label": label,
        "days": len(daily_ret),
        "cum_return_pct": round((nav[-1] - 1) * 100, 2),
        "annual_pct": round(ann, 2),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(mdd, 2),
        "calmar": round(calmar, 3),
        "win_rate": round(win_rate, 2),
    }


def daily_ret_from_position(df: pd.DataFrame, pos_col: str = "position_exec") -> np.ndarray:
    """仓位 × 大盘日收益 + 剩余仓位 × 现金日收益"""
    bench_ret = df["close"].pct_change().fillna(0).values
    pos = df[pos_col].values
    return pos * bench_ret + (1 - pos) * CASH_DAILY


# ================================================================
# 单信号诊断
# ================================================================
def evaluate_single_signal(df: pd.DataFrame) -> pd.DataFrame:
    """
    对每个 s01..s10：
        - 计算未来 1/5/20 日收益
        - 对 signal=2 / 1 / 0 三组分别看平均未来收益
        - 计算信号与未来收益的 Spearman 秩相关
        - 方向准确率：signal=2 时未来涨 + signal=0 时未来跌 的比例
    """
    d = df.copy()
    d["ret1"] = d["close"].pct_change().shift(-1)   # 未来 1 日
    d["ret5"] = d["close"].pct_change(5).shift(-5)
    d["ret20"] = d["close"].pct_change(20).shift(-20)

    sig_cols = [f"s{i:02d}" for i in range(1, 11)]
    names = {
        "s01": "MA60 趋势",
        "s02": "MA20/60 交叉",
        "s03": "20日动量",
        "s04": "MACD 柱",
        "s05": "ADX 强度",
        "s06": "涨跌停差",
        "s07": "市场宽度",
        "s08": "成交额分位",
        "s09": "波动率(反)",
        "s10": "连板情绪",
    }

    rows = []
    for c in sig_cols:
        sub = d[[c, "ret1", "ret5", "ret20"]].dropna()
        if len(sub) == 0:
            continue

        # 分层收益
        grp = sub.groupby(c).agg({"ret1": "mean", "ret5": "mean", "ret20": "mean"})
        g_bear = grp.loc[0] if 0 in grp.index else pd.Series({"ret1": np.nan, "ret5": np.nan, "ret20": np.nan})
        g_neu  = grp.loc[1] if 1 in grp.index else pd.Series({"ret1": np.nan, "ret5": np.nan, "ret20": np.nan})
        g_bull = grp.loc[2] if 2 in grp.index else pd.Series({"ret1": np.nan, "ret5": np.nan, "ret20": np.nan})

        # 单调性 & 上下分层价差
        ls_ret20 = (g_bull["ret20"] - g_bear["ret20"]) * 100  # 看多-看空 分层价差（%）

        # IC (Spearman 秩相关)
        try:
            ic1 = sub[c].rank().corr(sub["ret1"].rank())
            ic5 = sub[c].rank().corr(sub["ret5"].rank())
            ic20 = sub[c].rank().corr(sub["ret20"].rank())
        except Exception:
            ic1 = ic5 = ic20 = np.nan

        # 方向准确率（signal=2 涨 + signal=0 跌）
        s2 = sub[sub[c] == 2]
        s0 = sub[sub[c] == 0]
        win2 = (s2["ret5"] > 0).mean() if len(s2) else np.nan
        win0 = (s0["ret5"] < 0).mean() if len(s0) else np.nan
        n_use = len(s2) + len(s0)
        dir_acc = ((len(s2) * (win2 or 0)) + (len(s0) * (win0 or 0))) / n_use if n_use else np.nan

        rows.append({
            "signal": c,
            "name": names[c],
            "n_bear": int((sub[c] == 0).sum()),
            "n_neu":  int((sub[c] == 1).sum()),
            "n_bull": int((sub[c] == 2).sum()),
            "ret20_bear_%": round(g_bear["ret20"] * 100, 2),
            "ret20_neu_%":  round(g_neu["ret20"]  * 100, 2),
            "ret20_bull_%": round(g_bull["ret20"] * 100, 2),
            "ls_spread_%":  round(ls_ret20, 2),  # 多-空价差
            "IC_1d":   round(ic1, 4),
            "IC_5d":   round(ic5, 4),
            "IC_20d":  round(ic20, 4),
            "dir_acc_5d": round(dir_acc * 100, 2) if not pd.isna(dir_acc) else None,
        })

    return pd.DataFrame(rows)


def print_single_signal_table(tbl: pd.DataFrame) -> None:
    print("\n" + "=" * 110)
    print("📊 单信号诊断：未来 20 日分层收益 / IC / 方向准确率")
    print("=" * 110)
    print(f"{'信号':<16}{'看空':<7}{'中性':<7}{'看多':<7}"
          f"{'空涨%':>9}{'中涨%':>9}{'多涨%':>9}{'多-空%':>9}"
          f"{'IC_1d':>9}{'IC_5d':>9}{'IC_20d':>9}{'准确率':>9}")
    print("-" * 110)
    for _, r in tbl.iterrows():
        star_ic = "⭐" if abs(r["IC_20d"] or 0) >= 0.05 else " "
        star_ls = "⭐" if (r["ls_spread_%"] or 0) >= 1.0 else " "
        print(f"{star_ic}{star_ls}{r['name']:<14}"
              f"{r['n_bear']:<7}{r['n_neu']:<7}{r['n_bull']:<7}"
              f"{r['ret20_bear_%']:>+8.2f}%{r['ret20_neu_%']:>+8.2f}%{r['ret20_bull_%']:>+8.2f}%"
              f"{r['ls_spread_%']:>+8.2f}%"
              f"{(r['IC_1d'] or 0):>+9.3f}"
              f"{(r['IC_5d'] or 0):>+9.3f}"
              f"{(r['IC_20d'] or 0):>+9.3f}"
              f"{(r['dir_acc_5d'] or 0):>8.1f}%")
    print("\n  标记：⭐⭐ = IC_20d ≥ 0.05 且 多-空价差 ≥ 1%，代表有较强预测力")


# ================================================================
# 综合仓位回测
# ================================================================
def backtest_timing(df: pd.DataFrame) -> dict:
    # 基准（满仓 HS300）
    bench_ret = df["close"].pct_change().fillna(0).values
    # 择时
    timing_ret = daily_ret_from_position(df, "position_exec")

    m_bench = perf_metrics(bench_ret, "BuyHold(满仓)")
    m_timing = perf_metrics(timing_ret, "Timing")

    # α 年化
    alpha = m_timing["annual_pct"] - m_bench["annual_pct"]

    # 方向准确率（仓位>0.7 时涨、仓位<0.3 时跌）
    d = df.copy()
    d["next_ret"] = d["close"].pct_change().shift(-1)
    bull = d[d["position"] >= 0.7]
    bear = d[d["position"] <= 0.3]
    bull_hit = (bull["next_ret"] > 0).mean() if len(bull) else np.nan
    bear_hit = (bear["next_ret"] < 0).mean() if len(bear) else np.nan

    return {
        "bench": m_bench,
        "timing": m_timing,
        "alpha_ann_pct": round(alpha, 2),
        "avg_position": round(float(df["position_exec"].mean()), 3),
        "bull_days": int(len(bull)),
        "bull_hit_pct": round(bull_hit * 100, 2) if not pd.isna(bull_hit) else None,
        "bear_days": int(len(bear)),
        "bear_hit_pct": round(bear_hit * 100, 2) if not pd.isna(bear_hit) else None,
    }


def backtest_by_year(df: pd.DataFrame) -> dict:
    d = df.copy()
    d["year"] = d["trade_date"].astype(str).str[:4]
    out = {}
    for y, g in d.groupby("year"):
        if len(g) < 20:
            continue
        b = perf_metrics(g["close"].pct_change().fillna(0).values, "b")
        t = perf_metrics(daily_ret_from_position(g, "position_exec"), "t")
        out[y] = {
            "bench_cum":   b["cum_return_pct"],
            "bench_mdd":   b["max_dd_pct"],
            "timing_cum":  t["cum_return_pct"],
            "timing_mdd":  t["max_dd_pct"],
            "delta_cum":   round(t["cum_return_pct"] - b["cum_return_pct"], 2),
            "delta_mdd":   round(t["max_dd_pct"] - b["max_dd_pct"], 2),
            "avg_pos":     round(float(g["position_exec"].mean()), 3),
        }
    return out


def print_backtest(r: dict) -> None:
    print("\n" + "=" * 90)
    print("📊 综合仓位回测")
    print("=" * 90)
    print(f"{'指标':<18}{'满仓基准':>14}{'择时':>14}{'变化':>14}")
    print("-" * 90)
    b, t = r["bench"], r["timing"]
    for k, label in [("cum_return_pct", "累计收益%"),
                     ("annual_pct",     "年化%"),
                     ("sharpe",         "Sharpe"),
                     ("max_dd_pct",     "MDD%"),
                     ("calmar",         "Calmar"),
                     ("win_rate",       "胜率%")]:
        delta = t[k] - b[k]
        print(f"{label:<18}{b[k]:>14.2f}{t[k]:>14.2f}{delta:>+14.2f}")
    print("-" * 90)
    print(f"  α 年化        : {r['alpha_ann_pct']:+.2f}%")
    print(f"  平均仓位      : {r['avg_position']*100:.1f}%")
    print(f"  重仓（≥70%）天数 = {r['bull_days']}   次日涨的比例 = "
          f"{r['bull_hit_pct'] if r['bull_hit_pct'] is not None else '--'}%")
    print(f"  轻仓（≤30%）天数 = {r['bear_days']}   次日跌的比例 = "
          f"{r['bear_hit_pct'] if r['bear_hit_pct'] is not None else '--'}%")


def print_yearly(yres: dict) -> None:
    print("\n" + "=" * 90)
    print("📅 分年度对比")
    print("=" * 90)
    print(f"{'年份':<8}{'基准累计':>11}{'择时累计':>11}{'Δ':>9}"
          f"{'基准MDD':>11}{'择时MDD':>11}{'ΔMDD':>9}{'均仓':>8}")
    print("-" * 90)
    for y, v in yres.items():
        print(f"{y:<8}"
              f"{v['bench_cum']:>+10.1f}%{v['timing_cum']:>+10.1f}%{v['delta_cum']:>+8.1f}%"
              f"{v['bench_mdd']:>+10.1f}%{v['timing_mdd']:>+10.1f}%{v['delta_mdd']:>+8.1f}%"
              f"{v['avg_pos']*100:>7.0f}%")


# ================================================================
# 样本内/外分割验证
# ================================================================
def split_validate(df: pd.DataFrame, train_frac: float = 0.7) -> dict:
    n = len(df)
    split = int(n * train_frac)
    d_in = df.iloc[:split].copy()
    d_out = df.iloc[split:].copy()
    in_split_date = df.iloc[split]["trade_date"]

    print("\n" + "=" * 90)
    print(f"🔒 样本内外验证  分割点 = {in_split_date}")
    print("=" * 90)

    for tag, part in [("样本内 (train)", d_in), ("样本外 (test) ", d_out)]:
        r = backtest_timing(part)
        t = r["timing"]
        b = r["bench"]
        print(f"\n  【{tag}】  {part['trade_date'].iloc[0]} ~ {part['trade_date'].iloc[-1]}"
              f"   {len(part)} 天")
        print(f"    基准: 累计{b['cum_return_pct']:+.1f}%  年化{b['annual_pct']:+.1f}%  "
              f"Sharpe={b['sharpe']:.2f}  MDD={b['max_dd_pct']:+.1f}%")
        print(f"    择时: 累计{t['cum_return_pct']:+.1f}%  年化{t['annual_pct']:+.1f}%  "
              f"Sharpe={t['sharpe']:.2f}  MDD={t['max_dd_pct']:+.1f}%")
        print(f"    α = {r['alpha_ann_pct']:+.2f}%/年   均仓 = {r['avg_position']*100:.1f}%   "
              f"重仓命中率 = {r['bull_hit_pct']}%  轻仓命中率 = {r['bear_hit_pct']}%")

    # 衰减比
    r_in = backtest_timing(d_in)
    r_out = backtest_timing(d_out)
    decay = None
    if r_in["alpha_ann_pct"] != 0:
        decay = r_out["alpha_ann_pct"] / r_in["alpha_ann_pct"]

    print("\n" + "-" * 90)
    print(f"  📉 α 衰减比 (out/in) = {decay:.2f}" if decay is not None else "  衰减比无法计算")
    print(f"     判断：> 0.5 认为策略有泛化能力；< 0 视为过拟合")

    return {"in_sample": r_in, "out_sample": r_out, "alpha_decay_ratio": decay}


# ================================================================
# 主流程
# ================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="balanced",
                    choices=list(MAPPING_PRESETS.keys()))
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260420")
    ap.add_argument("--index", default="000300.SH")
    ap.add_argument("--train_frac", type=float, default=0.7)
    args = ap.parse_args()

    print("=" * 90)
    print(f"🎯 MarketTimer 诊断与回测  mode={args.mode}  index={args.index}")
    print(f"   {args.start} ~ {args.end}")
    print("=" * 90)

    mt = MarketTimer(mode=args.mode, index_code=args.index)
    df = mt.generate(args.start, args.end)
    mt.describe(df)

    # 单信号
    single_tbl = evaluate_single_signal(df)
    print_single_signal_table(single_tbl)

    # 综合回测
    r_overall = backtest_timing(df)
    print_backtest(r_overall)

    # 分年度
    yres = backtest_by_year(df)
    print_yearly(yres)

    # 样本内外
    split_result = split_validate(df, train_frac=args.train_frac)

    # 保存 JSON
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"market_timer_eval_{args.mode}_{ts}.json"
    payload = {
        "config": {"mode": args.mode, "index": args.index,
                   "start": args.start, "end": args.end,
                   "train_frac": args.train_frac},
        "single_signals": single_tbl.to_dict(orient="records"),
        "overall": r_overall,
        "yearly": yres,
        "split_validate": {
            "in_sample": split_result["in_sample"],
            "out_sample": split_result["out_sample"],
            "alpha_decay_ratio": split_result["alpha_decay_ratio"],
        },
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存: {out_path}")


if __name__ == "__main__":
    main()
