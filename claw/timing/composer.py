"""
claw.timing.composer — 大盘择时多因子合成器
================================================
5 因子等权投票 → 总分 ∈ [-5, +5] → 离散 4 档仓位

投票规则（方案 A，MVP）：
    total_score = rsrs_signal + trend_signal + vol_signal +
                  senti_signal + breadth_signal

仓位映射（离散 4 档）：
    total_score >= +3   → 满仓  (1.00)  state=BULL
    total_score ∈ [+1, +2] → 七成仓 (0.70)  state=RISK_ON
    total_score ∈ [-1, 0]  → 三成仓 (0.30)  state=NEUTRAL
    total_score <= -2   → 空仓  (0.00)  state=BEAR

注：
    - 当某因子数据缺失（例如本地无全市场日线 → breadth_signal 全为 0），
      投票会自动降级，不会崩。
    - 总分阈值可以通过参数调整。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from claw.timing import data as tdata
from claw.timing.factors import (
    compute_rsrs, compute_trend, compute_volatility,
    compute_sentiment_limit, compute_breadth,
)


# ============================================================
# 仓位映射 v2（2026-04-21）
#   v1 NEUTRAL=30% 过于保守，错过 A 股 95% 正常上涨日
#   v2 NEUTRAL=60%、RISK_ON=90%，只有明确看空时才空仓
# ============================================================
def score_to_position(score: int) -> tuple[str, float]:
    """总分 → (状态名, 仓位比例)  v2 仓位映射"""
    if pd.isna(score):
        return "NEUTRAL", 0.60
    if score >= 3:
        return "BULL", 1.00
    if score >= 1:
        return "RISK_ON", 0.90
    if score >= -1:
        return "NEUTRAL", 0.60
    if score >= -2:
        return "RISK_OFF", 0.30
    return "BEAR", 0.00


# ============================================================
# 主函数
# ============================================================
def compute_market_timing(
    start: str = "20210101",
    end: str = "20260430",
    index_code: str = "000300.SH",
    use_breadth: bool = True,
    use_sentiment: bool = True,
    refresh: bool = False,
    save_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    计算每日大盘择时信号与仓位。

    返回列：
        trade_date, close,
        rsrs, rsrs_signal,
        ma250, dev_pct, mom20, trend_signal,
        vol20, vol_pct, vol_signal,
        zt_ratio, up_stat, senti_signal,
        breadth_ma20, breadth_signal,
        total_score, state, position
    """
    # 1) 指数日线（需要更长的回看以计算 MA250 / RSRS M=600）
    #   向前多预留 3 年数据
    _start_int = int(start)
    pre_start = str(max(_start_int - 30000, 20180101))  # 粗略前推 3 年
    df_idx = tdata.load_index_daily(index_code, pre_start, end, refresh=refresh)

    # 2) 各因子
    f_rsrs = compute_rsrs(df_idx)
    f_trend = compute_trend(df_idx)
    f_vol = compute_volatility(df_idx)

    # 3) 情绪因子（涨跌停）
    if use_sentiment:
        try:
            df_limit = tdata.load_limit_stats(start, end, refresh=refresh)
            f_senti = compute_sentiment_limit(df_limit)
        except Exception as e:
            print(f"⚠️  情绪因子加载失败（{e}），降级为 0")
            f_senti = pd.DataFrame(columns=["trade_date", "senti_signal"])
    else:
        f_senti = pd.DataFrame(columns=["trade_date", "senti_signal"])

    # 4) 宽度因子
    if use_breadth:
        try:
            df_br = tdata.load_breadth_from_tushare(start, end, refresh=refresh)
            f_br = compute_breadth(df_br)
        except Exception as e:
            print(f"⚠️  宽度因子 tushare 拉取失败（{e}），降级读本地")
            df_br = tdata.load_market_breadth_from_local(start, end)
            f_br = compute_breadth(df_br)
    else:
        f_br = pd.DataFrame(columns=["trade_date", "breadth_signal"])

    # 5) 合并（以指数交易日为主键）
    df_idx["trade_date"] = df_idx["trade_date"].astype(str)
    base = df_idx[["trade_date", "close"]].copy()

    for tbl in [f_rsrs, f_trend, f_vol, f_senti, f_br]:
        if tbl is None or len(tbl) == 0:
            continue
        base = base.merge(tbl, on="trade_date", how="left")

    # 缺失信号置 0（降级）
    for c in ["rsrs_signal", "trend_signal", "vol_signal",
              "senti_signal", "breadth_signal"]:
        if c not in base.columns:
            base[c] = 0
        else:
            base[c] = base[c].fillna(0).astype(int)

    # 6) 合成
    base["total_score"] = (
        base["rsrs_signal"] + base["trend_signal"] + base["vol_signal"]
        + base["senti_signal"] + base["breadth_signal"]
    ).astype(int)

    base[["state", "position"]] = base["total_score"].apply(
        lambda s: pd.Series(score_to_position(s))
    )

    # 7) 截取用户请求时间段
    out = base[(base["trade_date"] >= start) & (base["trade_date"] <= end)]
    out = out.reset_index(drop=True)

    if save_path is not None:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(save_path, index=False)

    return out


# ============================================================
# 调试打印
# ============================================================
def summarize(df: pd.DataFrame) -> None:
    """打印仓位分布与分年统计"""
    print("\n" + "=" * 90)
    print(f"📊 大盘择时信号 — {df['trade_date'].min()} ~ {df['trade_date'].max()}   ({len(df)} 交易日)")
    print("=" * 90)

    print("\n🔎 仓位分档占比：")
    vc = df["state"].value_counts()
    total = len(df)
    order = ["BULL", "RISK_ON", "NEUTRAL", "BEAR"]
    for st in order:
        cnt = int(vc.get(st, 0))
        print(f"   {st:<10} {cnt:>5} 天  ({cnt/total*100:5.1f}%)")

    print("\n🔎 单因子触发占比：")
    for c, label in [
        ("rsrs_signal", "RSRS"),
        ("trend_signal", "趋势"),
        ("vol_signal", "波动率"),
        ("senti_signal", "涨停情绪"),
        ("breadth_signal", "市场宽度"),
    ]:
        pos = (df[c] == 1).sum()
        neg = (df[c] == -1).sum()
        print(f"   {label:<10}  +1={pos:>4} ({pos/total*100:5.1f}%)   -1={neg:>4} ({neg/total*100:5.1f}%)")

    print("\n🔎 分年度 BEAR（空仓）天数：")
    d = df.copy()
    d["year"] = d["trade_date"].str[:4]
    for y, g in d.groupby("year"):
        bear = (g["state"] == "BEAR").sum()
        neu = (g["state"] == "NEUTRAL").sum()
        ron = (g["state"] == "RISK_ON").sum()
        bull = (g["state"] == "BULL").sum()
        print(f"   {y}  BEAR={bear:3d}  NEUTRAL={neu:3d}  RISK_ON={ron:3d}  BULL={bull:3d}   总{len(g)}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260420")
    ap.add_argument("--sentiment", action="store_true",
                    help="启用涨停情绪因子（首次运行会拉 tushare limit_list_d，约 10 分钟）")
    ap.add_argument("--breadth", action="store_true",
                    help="启用市场宽度因子（需本地有全市场日线）")
    args = ap.parse_args()

    df = compute_market_timing(
        start=args.start, end=args.end,
        use_sentiment=args.sentiment,
        use_breadth=args.breadth,
    )
    summarize(df)
    print("\n最近 10 日：")
    cols = ["trade_date", "rsrs_signal", "trend_signal", "vol_signal",
            "senti_signal", "breadth_signal", "total_score", "state", "position"]
    print(df.tail(10)[cols].to_string(index=False))
