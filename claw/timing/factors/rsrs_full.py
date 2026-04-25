"""
因子组：RSRS 完整版（光大证券 5 变体）
================================================
原始 RSRS (Resistance-Support Relative Strength) 基于高价对低价的日内滚动 OLS：
    high_t = alpha + beta * low_t + epsilon
- beta 本身衡量"阻力/支撑的相对强度"
- 标准分 z = (beta - mean) / std 消除趋势
- 修正标准分 z_mod = z * R²，用拟合优度加权，避免噪声信号

本模块导出 5 个独立因子，可单独测试，也可加权融合：
    rsrs_raw_18       —— 18 日窗口原始斜率
    rsrs_raw_24       —— 24 日窗口原始斜率
    rsrs_zscore_18    —— 18 日斜率的 300 日 Z-Score（光大经典参数）
    rsrs_zscore_24    —— 24 日斜率的 300 日 Z-Score
    rsrs_modified     —— 18 日 Z-Score × R²（最稳健版本）

每个因子接口与 compute_rsrs 一致：
    返回 DataFrame[trade_date, <因子列>, <信号列>]
    信号列为 +1 / 0 / -1（看多 / 中性 / 看空）

阈值（up_th / dn_th）可在 __init__ 传参覆盖。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ============================================================
# 工具：滚动 OLS beta + R²
# ============================================================
def _rolling_beta_r2(high: np.ndarray, low: np.ndarray, n: int) -> tuple[np.ndarray, np.ndarray]:
    T = len(high)
    beta = np.full(T, np.nan)
    r2 = np.full(T, np.nan)
    for i in range(n - 1, T):
        x = low[i - n + 1: i + 1]
        y = high[i - n + 1: i + 1]
        if np.isnan(x).any() or np.isnan(y).any():
            continue
        xm, ym = x.mean(), y.mean()
        dx, dy = x - xm, y - ym
        sxx = (dx * dx).sum()
        if sxx <= 1e-12:
            continue
        b = (dx * dy).sum() / sxx
        y_pred = ym + b * dx
        ss_res = ((y - y_pred) ** 2).sum()
        ss_tot = (dy * dy).sum()
        r_sq = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        beta[i] = b
        r2[i] = max(0.0, min(1.0, r_sq))
    return beta, r2


def _sig_from_value(v: float, up: float, dn: float) -> int:
    if pd.isna(v):
        return 0
    if v >= up:
        return 1
    if v <= dn:
        return -1
    return 0


# ============================================================
# 1) rsrs_raw_N —— 原始斜率
# ============================================================
def compute_rsrs_raw(df_idx: pd.DataFrame, n: int = 18,
                     up_th: float = 1.0, dn_th: float = 0.8) -> pd.DataFrame:
    """
    原始 RSRS 斜率。阈值按经验：beta > 1.0 为看多，< 0.8 为看空。
    注意：原始 beta 的量纲依赖行情价格级别，阈值未必稳定，一般只作为参考。
    """
    d = df_idx[["trade_date", "high", "low"]].copy().sort_values("trade_date").reset_index(drop=True)
    beta, r2 = _rolling_beta_r2(d["high"].astype(float).values,
                                 d["low"].astype(float).values, n=n)
    col = f"rsrs_raw_{n}"
    sig = f"{col}_signal"
    d[col] = beta
    d[sig] = [_sig_from_value(v, up_th, dn_th) for v in beta]
    return d[["trade_date", col, sig]]


# ============================================================
# 2) rsrs_zscore_N —— 斜率 Z-Score 标准分
# ============================================================
def compute_rsrs_zscore(df_idx: pd.DataFrame, n: int = 18, m: int = 300,
                        up_th: float = 0.7, dn_th: float = -0.7) -> pd.DataFrame:
    """
    光大经典 RSRS 标准分：z = (beta - mean) / std
    窗口 m=300 个交易日（约 1.5 年）。
    阈值 ±0.7 为光大原版。
    """
    d = df_idx[["trade_date", "high", "low"]].copy().sort_values("trade_date").reset_index(drop=True)
    beta, r2 = _rolling_beta_r2(d["high"].astype(float).values,
                                 d["low"].astype(float).values, n=n)

    beta_s = pd.Series(beta)
    m_use = min(m, max(60, len(beta_s) - n))
    mean = beta_s.rolling(m_use, min_periods=60).mean()
    std = beta_s.rolling(m_use, min_periods=60).std()
    z = (beta_s - mean) / std.replace(0, np.nan)

    col = f"rsrs_zscore_{n}"
    sig = f"{col}_signal"
    d[col] = z.values
    d[sig] = [_sig_from_value(v, up_th, dn_th) for v in z.values]
    return d[["trade_date", col, sig]]


# ============================================================
# 3) rsrs_modified —— 修正标准分（z × R²）
# ============================================================
def compute_rsrs_modified(df_idx: pd.DataFrame, n: int = 18, m: int = 300,
                          up_th: float = 0.7, dn_th: float = -0.7) -> pd.DataFrame:
    """
    RSRS 修正标准分：用 R² 加权 Z-Score，压制拟合噪声。业界最稳定版本。
    """
    d = df_idx[["trade_date", "high", "low"]].copy().sort_values("trade_date").reset_index(drop=True)
    beta, r2 = _rolling_beta_r2(d["high"].astype(float).values,
                                 d["low"].astype(float).values, n=n)

    beta_s = pd.Series(beta)
    m_use = min(m, max(60, len(beta_s) - n))
    mean = beta_s.rolling(m_use, min_periods=60).mean()
    std = beta_s.rolling(m_use, min_periods=60).std()
    z = (beta_s - mean) / std.replace(0, np.nan)
    modified = z * pd.Series(r2)

    d["rsrs_modified"] = modified.values
    d["rsrs_modified_signal"] = [_sig_from_value(v, up_th, dn_th) for v in modified.values]
    return d[["trade_date", "rsrs_modified", "rsrs_modified_signal"]]


# ============================================================
# 统一入口：一次计算全部 5 变体，方便批量测试
# ============================================================
def compute_rsrs_full(df_idx: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """返回一个 dict：{因子名: 每个因子的 DataFrame}"""
    return {
        "rsrs_raw_18":    compute_rsrs_raw(df_idx, n=18),
        "rsrs_raw_24":    compute_rsrs_raw(df_idx, n=24),
        "rsrs_zscore_18": compute_rsrs_zscore(df_idx, n=18, m=300),
        "rsrs_zscore_24": compute_rsrs_zscore(df_idx, n=24, m=300),
        "rsrs_modified":  compute_rsrs_modified(df_idx, n=18, m=300),
    }
