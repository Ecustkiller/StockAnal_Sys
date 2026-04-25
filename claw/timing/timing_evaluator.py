"""
claw.timing.timing_evaluator — 纯择时能力评测器（策略无关）
============================================================

核心思想：
    把择时信号从"选股策略收益"里剥离出来，直接对比：
        基准 buy&hold    vs    基准 × 仓位 + 现金 × (1-仓位) × rf

这样得到的 α 才是"择时本身"产生的超额收益，不受选股 α 干扰。

评估设计：
    1. 4 个基准：000300.SH(沪深300) / 000905.SH(中证500) /
                 000852.SH(中证1000) / 399006.SZ(创业板指)
    2. 空仓时现金按 2% 年化收益（理财/短债基准，可配置）
    3. 关键指标：
       - 累计收益 / 年化 / Sharpe / MDD / Calmar
       - α 年化   = 择时组合 - 基准（年化）
       - 仓位效率 = α年化 / (1 - 平均仓位)   （每单位减仓产生的 α）
       - 分年度  / 牛熊分段
       - 胜率对比（基准涨时是否跟上、基准跌时是否少跌）

用法：
    from claw.timing.timing_evaluator import evaluate_timing
    report = evaluate_timing(
        position_map={"20210104": 1.0, ...},
        name="full5",
        start="20210101", end="20260420",
        benchmarks=["000300.SH", "000905.SH", "000852.SH", "399006.SZ"],
        cash_rate_annual=0.02,
    )
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from claw.timing import data as tdata

# ============================================================
# 基准定义
# ============================================================
DEFAULT_BENCHMARKS = {
    "000300.SH": "沪深300（大盘蓝筹）",
    "000905.SH": "中证500（中盘）",
    "000852.SH": "中证1000（小盘）",
    "399006.SZ": "创业板指（成长）",
}


# ============================================================
# 指标计算
# ============================================================
def _perf_metrics(daily_ret: np.ndarray, trading_days: int = 250) -> dict:
    """
    输入日收益率数组（小数，如 0.01 表示 +1%）
    输出核心绩效指标
    """
    if len(daily_ret) == 0:
        return {"error": "empty"}

    nav = np.cumprod(1.0 + daily_ret)
    years = len(daily_ret) / trading_days
    cum = (nav[-1] - 1) * 100
    ann = (nav[-1] ** (1 / max(years, 1e-9)) - 1) * 100
    peak = np.maximum.accumulate(nav)
    mdd = float(((nav - peak) / peak).min()) * 100
    std = daily_ret.std(ddof=1)
    sharpe = (daily_ret.mean() / std) * np.sqrt(trading_days) if std > 0 else 0.0
    calmar = ann / abs(mdd) if mdd < 0 else 0.0
    wins = daily_ret[daily_ret > 0]
    loss = daily_ret[daily_ret < 0]
    win_rate = len(wins) / len(daily_ret) * 100
    pr = (wins.mean() / abs(loss.mean())) if len(wins) > 0 and len(loss) > 0 else 0.0

    return {
        "trading_days":      len(daily_ret),
        "cum_return_pct":    round(cum, 2),
        "annual_return_pct": round(ann, 2),
        "sharpe":            round(sharpe, 3),
        "max_drawdown_pct":  round(mdd, 2),
        "calmar":            round(calmar, 3),
        "win_rate_pct":      round(win_rate, 2),
        "profit_ratio":      round(pr, 2),
        "final_nav":         round(float(nav[-1]), 4),
    }


def _conditional_metrics(bench_ret: np.ndarray, timing_ret: np.ndarray) -> dict:
    """
    条件胜率 / 捕获率：
        up_capture   = 基准涨日上，择时组合的平均收益 / 基准涨日平均收益
        down_capture = 基准跌日上，择时组合的平均收益 / 基准跌日平均收益
        up_follow_rate = 基准涨日上 择时组合也为正 的比例
        down_avoid_rate= 基准跌日上 择时组合亏损更小（含转正）的比例
    """
    if len(bench_ret) == 0:
        return {}

    up_mask = bench_ret > 0
    dn_mask = bench_ret < 0

    up_capture = 0.0
    if up_mask.any() and bench_ret[up_mask].mean() != 0:
        up_capture = timing_ret[up_mask].mean() / bench_ret[up_mask].mean() * 100

    down_capture = 0.0
    if dn_mask.any() and bench_ret[dn_mask].mean() != 0:
        down_capture = timing_ret[dn_mask].mean() / bench_ret[dn_mask].mean() * 100

    up_follow = 0.0
    if up_mask.any():
        up_follow = (timing_ret[up_mask] > 0).mean() * 100

    down_avoid = 0.0
    if dn_mask.any():
        down_avoid = (timing_ret[dn_mask] > bench_ret[dn_mask]).mean() * 100

    return {
        "up_capture_pct":      round(up_capture, 1),
        "down_capture_pct":    round(down_capture, 1),
        "up_follow_rate_pct":  round(up_follow, 1),
        "down_avoid_rate_pct": round(down_avoid, 1),
    }


# ============================================================
# 单基准评测
# ============================================================
def _evaluate_one_benchmark(
    bench_code: str,
    bench_label: str,
    position_map: dict[str, float],
    start: str, end: str,
    cash_rate_annual: float = 0.02,
    trading_days: int = 250,
) -> dict:
    """
    对一个基准做择时评测：
        基准：每日 buy&hold
        择时：基准 × pos  + 现金 × (1-pos) × cash_rate_daily
    """
    df = tdata.load_index_daily(bench_code, start, end)
    df["trade_date"] = df["trade_date"].astype(str)
    df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
    df = df.sort_values("trade_date").reset_index(drop=True)

    # 基准日收益
    df["bench_ret"] = df["close"].pct_change().fillna(0)

    # 仓位（缺失按 1.0 —— 即默认满仓）
    df["position"] = df["trade_date"].map(position_map).fillna(1.0).astype(float)

    # ============================================================
    # 🔑 T+1 执行修复（2026-04-21）
    # ------------------------------------------------------------
    # position[t] 是用 t 日收盘价/成交量等 EOD 数据算出来的，
    # 真实交易中只能在 t+1 日开盘后按新仓位调仓，所以 t+1 日才能享受新仓位。
    # 之前的 position[t] * bench_ret[t] 相当于用 t 日收盘后才知道的仓位
    # 去吃 t 日全天的涨跌 —— 典型的未来数据泄露。
    # 正确做法：position.shift(1) —— 昨日收盘决定今日仓位。
    # ============================================================
    df["position_exec"] = df["position"].shift(1)
    # 第一天没有前一日信号，保守按满仓处理（也可改为 0.6 基准仓）
    df["position_exec"] = df["position_exec"].fillna(1.0).astype(float)

    # 现金日收益
    cash_daily = (1.0 + cash_rate_annual) ** (1 / trading_days) - 1

    # 择时组合日收益 = 基准×仓位_exec + 现金×(1-仓位_exec)
    df["timing_ret"] = df["bench_ret"] * df["position_exec"] + cash_daily * (1.0 - df["position_exec"])

    bench_arr = df["bench_ret"].values
    timing_arr = df["timing_ret"].values

    m_bench  = _perf_metrics(bench_arr, trading_days)
    m_timing = _perf_metrics(timing_arr, trading_days)
    cond     = _conditional_metrics(bench_arr, timing_arr)

    # α 年化
    alpha_ann = m_timing["annual_return_pct"] - m_bench["annual_return_pct"]
    avg_pos = float(df["position_exec"].mean())

    # 仓位效率（只在仓位 < 1 时有意义）
    if avg_pos < 0.999:
        pos_eff = alpha_ann / (1.0 - avg_pos)
    else:
        pos_eff = 0.0

    return {
        "bench_code":   bench_code,
        "bench_label":  bench_label,
        "benchmark":    m_bench,
        "timing":       m_timing,
        "conditional":  cond,
        "alpha_annual_pct": round(alpha_ann, 2),
        "avg_position":     round(avg_pos, 3),
        "position_efficiency": round(pos_eff, 2),
        # 分年度
        "yearly": _yearly(df, trading_days),
    }


def _yearly(df: pd.DataFrame, trading_days: int) -> dict:
    """分年度对比"""
    d = df.copy()
    d["year"] = d["trade_date"].str[:4]
    out = {}
    for y, g in d.groupby("year"):
        b = _perf_metrics(g["bench_ret"].values, trading_days)
        t = _perf_metrics(g["timing_ret"].values, trading_days)
        if "error" in b or "error" in t:
            continue
        out[y] = {
            "bench_cum":   b["cum_return_pct"],
            "bench_mdd":   b["max_drawdown_pct"],
            "timing_cum":  t["cum_return_pct"],
            "timing_mdd":  t["max_drawdown_pct"],
            "delta_cum":   round(t["cum_return_pct"] - b["cum_return_pct"], 2),
            "delta_mdd":   round(t["max_drawdown_pct"] - b["max_drawdown_pct"], 2),
            "avg_position": round(float(g["position_exec"].mean()), 3),
        }
    return out


# ============================================================
# 主入口
# ============================================================
def evaluate_timing(
    position_map: dict[str, float],
    name: str = "unnamed",
    start: str = "20210101",
    end: str = "20260420",
    benchmarks: Optional[list[str]] = None,
    cash_rate_annual: float = 0.02,
    trading_days: int = 250,
) -> dict:
    """
    评测一个仓位序列在多个基准上的择时能力。

    参数：
        position_map: {trade_date → position ∈ [0,1]}
        name: 配置名（输出报告用）
        benchmarks: 基准代码列表（默认 4 个）
        cash_rate_annual: 空仓时的现金年化收益率（默认 2%）

    返回：
        {
            "name": ..., "start": ..., "end": ...,
            "benchmarks": {
                "000300.SH": {...},
                ...
            }
        }
    """
    if benchmarks is None:
        benchmarks = list(DEFAULT_BENCHMARKS.keys())

    report = {
        "name": name,
        "start": start, "end": end,
        "cash_rate_annual": cash_rate_annual,
        "benchmarks": {},
    }

    for code in benchmarks:
        label = DEFAULT_BENCHMARKS.get(code, code)
        try:
            report["benchmarks"][code] = _evaluate_one_benchmark(
                code, label, position_map,
                start, end,
                cash_rate_annual=cash_rate_annual,
                trading_days=trading_days,
            )
        except Exception as e:
            report["benchmarks"][code] = {"error": str(e), "bench_label": label}

    return report


# ============================================================
# 报告打印
# ============================================================
def print_benchmark_table(reports: list[dict]) -> None:
    """
    跨配置对比表：每个基准一张表，列出各配置的表现。
    reports: 多个配置的 evaluate_timing 结果列表
    """
    if not reports:
        return

    # 收集所有基准代码
    all_benchmarks = []
    for r in reports:
        for code in r.get("benchmarks", {}):
            if code not in all_benchmarks:
                all_benchmarks.append(code)

    for code in all_benchmarks:
        label = DEFAULT_BENCHMARKS.get(code, code)
        print("\n" + "=" * 142)
        print(f"📊 基准：{code}  {label}")
        print("=" * 142)
        print(f"{'配置':<28}{'累计':>10}{'年化':>9}{'Sharpe':>8}"
              f"{'MDD':>9}{'Calmar':>8}{'胜率':>8}"
              f"{'α年化':>10}{'均仓':>8}{'仓位效率':>10}"
              f"{'涨捕获':>9}{'跌捕获':>9}")
        print("-" * 142)

        # 先打印基准 buy&hold
        first_valid = None
        for r in reports:
            bd = r["benchmarks"].get(code)
            if bd and "error" not in bd:
                first_valid = bd
                break
        if first_valid:
            b = first_valid["benchmark"]
            print(f"{'[BUY&HOLD 基准]':<28}"
                  f"{b['cum_return_pct']:>+9.1f}%{b['annual_return_pct']:>+8.1f}%"
                  f"{b['sharpe']:>8.2f}{b['max_drawdown_pct']:>+8.1f}%"
                  f"{b['calmar']:>8.2f}{b['win_rate_pct']:>7.1f}%"
                  f"{'--':>10}{'100%':>8}{'--':>10}{'100.0':>9}{'100.0':>9}")
            print("-" * 142)

        # 再打印各配置
        for r in reports:
            bd = r["benchmarks"].get(code)
            if not bd:
                continue
            if "error" in bd:
                print(f"{r['name']:<28}  ERROR: {bd['error']}")
                continue
            t = bd["timing"]
            c = bd["conditional"]
            print(f"{r['name']:<28}"
                  f"{t['cum_return_pct']:>+9.1f}%{t['annual_return_pct']:>+8.1f}%"
                  f"{t['sharpe']:>8.2f}{t['max_drawdown_pct']:>+8.1f}%"
                  f"{t['calmar']:>8.2f}{t['win_rate_pct']:>7.1f}%"
                  f"{bd['alpha_annual_pct']:>+9.1f}%"
                  f"{bd['avg_position']*100:>7.0f}%"
                  f"{bd['position_efficiency']:>+9.1f}%"
                  f"{c.get('up_capture_pct', 0):>9.1f}"
                  f"{c.get('down_capture_pct', 0):>9.1f}")


def print_yearly_comparison(reports: list[dict], bench_code: str = "000300.SH") -> None:
    """分年度对比（选一个基准展开）"""
    if not reports:
        return

    # 统一年度集合
    years = set()
    for r in reports:
        bd = r["benchmarks"].get(bench_code)
        if bd and "error" not in bd:
            years.update(bd["yearly"].keys())
    years = sorted(years)
    if not years:
        return

    label = DEFAULT_BENCHMARKS.get(bench_code, bench_code)
    print("\n" + "=" * 142)
    print(f"📅 分年度累计收益   (基准：{bench_code} {label})")
    print("=" * 142)
    header = f"{'配置':<28}" + "".join(f"{y:>14}" for y in years)
    print(header)
    print("-" * 142)

    # 基准行
    first_valid = None
    for r in reports:
        bd = r["benchmarks"].get(bench_code)
        if bd and "error" not in bd:
            first_valid = bd
            break
    if first_valid:
        row = f"{'[BUY&HOLD 基准]':<28}"
        for y in years:
            d = first_valid["yearly"].get(y)
            row += f"{d['bench_cum']:>+13.1f}%" if d else f"{'--':>14}"
        print(row)
        print("-" * 142)

    for r in reports:
        bd = r["benchmarks"].get(bench_code)
        if not bd or "error" in bd:
            continue
        row = f"{r['name']:<28}"
        for y in years:
            d = bd["yearly"].get(y)
            row += f"{d['timing_cum']:>+13.1f}%" if d else f"{'--':>14}"
        print(row)

    print("\n" + "-" * 142)
    print("📉 分年度最大回撤")
    print("-" * 142)
    # 基准
    if first_valid:
        row = f"{'[BUY&HOLD 基准]':<28}"
        for y in years:
            d = first_valid["yearly"].get(y)
            row += f"{d['bench_mdd']:>+13.1f}%" if d else f"{'--':>14}"
        print(row)
        print("-" * 142)
    for r in reports:
        bd = r["benchmarks"].get(bench_code)
        if not bd or "error" in bd:
            continue
        row = f"{r['name']:<28}"
        for y in years:
            d = bd["yearly"].get(y)
            row += f"{d['timing_mdd']:>+13.1f}%" if d else f"{'--':>14}"
        print(row)

    print("\n" + "-" * 142)
    print("📦 分年度平均仓位")
    print("-" * 142)
    for r in reports:
        bd = r["benchmarks"].get(bench_code)
        if not bd or "error" in bd:
            continue
        row = f"{r['name']:<28}"
        for y in years:
            d = bd["yearly"].get(y)
            row += f"{d['avg_position']*100:>13.0f}%" if d else f"{'--':>14}"
        print(row)


def save_reports_json(reports: list[dict], path: Path) -> None:
    import json
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n💾 评测结果已保存: {path}")
