"""
因子 1：RSRS 阻力支撑相对强度（光大证券）
=============================================
1. 取前 N 日的最高价和最低价
2. 以 low 为自变量、high 为因变量 做 OLS 线性回归，得到斜率 beta 和 R²
3. 取前 M 日斜率序列，计算当日斜率的 Z-score（z = (beta - mean) / std）
4. RSRS 标准分 = z * R²   （R²加权，信号更纯净）

信号规则（+1 看多 / 0 中性 / -1 看空）：
    z * R² >= +0.7  →  +1 （多头阻力/支撑放大 → 看多）
    z * R² <= -0.7  →  -1
    else            →   0

默认 N=18, M=600（光大原版参数）。为了回测样本考虑，M 若数据不足则按可用长度退让。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _rolling_ols_beta_r2(high: np.ndarray, low: np.ndarray,
                         n: int = 18) -> tuple[np.ndarray, np.ndarray]:
    """向量化滚动 OLS：y = high, x = low。返回 (beta, r2)。长度同 high。"""
    T = len(high)
    beta = np.full(T, np.nan)
    r2 = np.full(T, np.nan)
    for i in range(n - 1, T):
        x = low[i - n + 1: i + 1]
        y = high[i - n + 1: i + 1]
        if np.isnan(x).any() or np.isnan(y).any():
            continue
        x_mean = x.mean()
        y_mean = y.mean()
        dx = x - x_mean
        dy = y - y_mean
        sxx = (dx * dx).sum()
        if sxx <= 1e-12:
            continue
        b = (dx * dy).sum() / sxx
        y_pred = y_mean + b * dx
        ss_res = ((y - y_pred) ** 2).sum()
        ss_tot = (dy * dy).sum()
        r_sq = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        beta[i] = b
        r2[i] = max(0.0, min(1.0, r_sq))
    return beta, r2


def compute_rsrs(df_idx: pd.DataFrame,
                 n: int = 18,
                 m: int = 600,
                 up_th: float = 0.7,
                 dn_th: float = -0.7) -> pd.DataFrame:
    """
    参数:
        df_idx: 指数日线，必须包含列 trade_date, high, low
        n:      RSRS 回看窗口（默认 18）
        m:      Z-score 标准化窗口（默认 600，数据不足时自动退让）
        up_th:  看多阈值
        dn_th:  看空阈值

    返回:
        DataFrame[trade_date, rsrs_beta, rsrs_r2, rsrs_z, rsrs, rsrs_signal]
    """
    d = df_idx[["trade_date", "high", "low"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    high = d["high"].astype(float).values
    low = d["low"].astype(float).values
    beta, r2 = _rolling_ols_beta_r2(high, low, n=n)

    beta_s = pd.Series(beta)
    m_use = min(m, max(60, len(beta_s) - n))  # 样本不足时退让
    mean = beta_s.rolling(m_use, min_periods=60).mean()
    std = beta_s.rolling(m_use, min_periods=60).std()
    z = (beta_s - mean) / std.replace(0, np.nan)

    d["rsrs_beta"] = beta
    d["rsrs_r2"] = r2
    d["rsrs_z"] = z.values
    d["rsrs"] = d["rsrs_z"] * d["rsrs_r2"]

    def _sig(x):
        if pd.isna(x):
            return 0
        if x >= up_th:
            return 1
        if x <= dn_th:
            return -1
        return 0

    d["rsrs_signal"] = d["rsrs"].apply(_sig)
    return d[["trade_date", "rsrs_beta", "rsrs_r2", "rsrs_z", "rsrs", "rsrs_signal"]]
