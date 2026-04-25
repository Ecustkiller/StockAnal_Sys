"""
因子 3：波动率状态
========================
A股经验：高波动往往伴随下跌，低波动通常是趋势行情。

指标：20日已实现波动率（pct_change std × √250），计算其 近 N 年分位。

信号规则：
    vol_pct < 30%  → +1  （低波动 → 风险小，看多）
    vol_pct > 80%  → -1  （高波动 → 风险大，看空）
    else           →  0
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_volatility(df_idx: pd.DataFrame,
                       vol_win: int = 20,
                       rank_win: int = 750,
                       low_th: float = 0.20,
                       high_th: float = 0.90) -> pd.DataFrame:
    """
    参数:
        df_idx: 指数日线，含列 trade_date, close
        vol_win: 已实现波动率窗口
        rank_win: 百分位滚动窗口（默认 3 年 ≈ 750 交易日）
    返回:
        DataFrame[trade_date, vol20, vol_pct, vol_signal]
    """
    d = df_idx[["trade_date", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    ret = d["close"].pct_change()
    d["vol20"] = ret.rolling(vol_win, min_periods=10).std() * np.sqrt(250)

    # 滚动百分位：当前 vol 在过去 rank_win 内的排位
    def _pct_rank(s):
        r = s.rank(pct=True)
        return r.iloc[-1] if len(r) else np.nan

    d["vol_pct"] = d["vol20"].rolling(rank_win, min_periods=60).apply(_pct_rank, raw=False)

    def _sig(v):
        if pd.isna(v):
            return 0
        if v < low_th:
            return 1
        if v > high_th:
            return -1
        return 0

    d["vol_signal"] = d["vol_pct"].apply(_sig)
    return d[["trade_date", "vol20", "vol_pct", "vol_signal"]]
