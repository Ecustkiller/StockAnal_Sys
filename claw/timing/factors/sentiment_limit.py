"""
因子 4：涨停赚钱效应（国泰海通 2025.05 研报思路）
=====================================================
短期情绪温度计：涨停数/跌停数比 + 最高连板数

指标：
    zt_ratio = log((up_count + 1) / (down_count + 1))   # 对数比，去除量纲
    up_stat  = 当日最高连板数

信号规则：
    zt_ratio > 1.5 且 up_stat >= 3 →  +1 （市场赚钱效应强）
    zt_ratio < -0.5                →  -1 （跌停多于涨停显著 → 恐慌）
    else                           →   0
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_sentiment_limit(df_limit: pd.DataFrame,
                             up_th: float = 1.5,
                             dn_th: float = -0.5,
                             min_lb: int = 3) -> pd.DataFrame:
    """
    参数:
        df_limit: 涨跌停统计，含列 trade_date, up_count, down_count, up_stat
    返回:
        DataFrame[trade_date, zt_ratio, up_stat, senti_signal]
    """
    if df_limit is None or len(df_limit) == 0:
        return pd.DataFrame(columns=[
            "trade_date", "zt_ratio", "up_stat", "senti_signal"
        ])

    d = df_limit[["trade_date", "up_count", "down_count", "up_stat"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)
    d["trade_date"] = d["trade_date"].astype(str)

    d["zt_ratio"] = np.log(
        (d["up_count"].fillna(0) + 1) / (d["down_count"].fillna(0) + 1)
    )

    def _sig(row):
        r, lb = row["zt_ratio"], row["up_stat"]
        if pd.isna(r):
            return 0
        if r > up_th and (pd.notna(lb) and lb >= min_lb):
            return 1
        if r < dn_th:
            return -1
        return 0

    d["senti_signal"] = d.apply(_sig, axis=1)
    return d[["trade_date", "zt_ratio", "up_stat", "senti_signal"]]
