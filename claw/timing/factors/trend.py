"""
因子 2：趋势强度（年线偏离 + 动量）
==========================================
- 年线偏离度：(close - MA250) / MA250
- 5 日动量、20 日动量

信号规则（+1 / 0 / -1）：
    close > MA250  且  mom20 > +2%    →  +1 （强势）
    close < MA250  且  mom20 < -5%    →  -1 （强弱，熊市特征）
    其他                                →   0
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_trend(df_idx: pd.DataFrame,
                  ma_win: int = 250,
                  mom_win: int = 20,
                  up_mom: float = 2.0,
                  dn_mom: float = -5.0) -> pd.DataFrame:
    """
    参数:
        df_idx: 指数日线，含列 trade_date, close
    返回:
        DataFrame[trade_date, ma250, mom5, mom20, dev_pct, trend_signal]
    """
    d = df_idx[["trade_date", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    d["ma250"] = d["close"].rolling(ma_win, min_periods=60).mean()
    d["mom5"] = d["close"].pct_change(5) * 100
    d["mom20"] = d["close"].pct_change(mom_win) * 100
    d["dev_pct"] = (d["close"] - d["ma250"]) / d["ma250"] * 100

    def _sig(row):
        c, ma, m20 = row["close"], row["ma250"], row["mom20"]
        if pd.isna(ma) or pd.isna(m20):
            return 0
        if c > ma and m20 > up_mom:
            return 1
        if c < ma and m20 < dn_mom:
            return -1
        return 0

    d["trend_signal"] = d.apply(_sig, axis=1)
    return d[["trade_date", "ma250", "mom5", "mom20", "dev_pct", "trend_signal"]]
