"""
因子 5：市场宽度（Market Breadth）
======================================
定义：MA20 之上的股票占全市场比例（%）。

信号规则：
    breadth_ma20 > 60   →  +1 （多数股票走强）
    breadth_ma20 < 30   →  -1 （多数股票破位）
    else                →   0

若本地没有全市场日线缓存，data.load_market_breadth_from_local() 将返回空表，
此时信号降级为 0（不参与投票），不影响其他因子。
"""
from __future__ import annotations

import pandas as pd


def compute_breadth(df_breadth: pd.DataFrame,
                    up_th: float = 60.0,
                    dn_th: float = 30.0) -> pd.DataFrame:
    """
    参数:
        df_breadth: DataFrame[trade_date, breadth_ma20]
    返回:
        DataFrame[trade_date, breadth_ma20, breadth_signal]
    """
    if df_breadth is None or len(df_breadth) == 0:
        return pd.DataFrame(columns=["trade_date", "breadth_ma20", "breadth_signal"])

    d = df_breadth[["trade_date", "breadth_ma20"]].copy()
    d["trade_date"] = d["trade_date"].astype(str)

    def _sig(x):
        if pd.isna(x):
            return 0
        if x > up_th:
            return 1
        if x < dn_th:
            return -1
        return 0

    d["breadth_signal"] = d["breadth_ma20"].apply(_sig)
    return d
