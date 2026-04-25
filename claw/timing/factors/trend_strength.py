"""
因子组：趋势强度（ADX + OBV 动量）
================================================
两个指标都是经典的大盘趋势跟踪信号，对指数日线单序列计算。

1) adx_14    —— 14 日平均趋向指标（Welles Wilder 1978）
   - ADX > 25：趋势明确（多空都算），结合方向判断
   - ADX < 20：震荡市
   - 信号逻辑：
       ADX > adx_th 且 +DI > -DI  →  +1
       ADX > adx_th 且 +DI < -DI  →  -1
       否则                         →   0

2) obv_mom_20 —— 20 日 OBV 动量偏离率
   OBV = 累计有向成交量
   obv_mom = (OBV - MA_OBV_20) / |MA_OBV_20|
   - 放量上攻 → OBV 斜率上行 → +1
   - 放量下跌 → OBV 斜率下行 → -1
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _rma(s: pd.Series, n: int) -> pd.Series:
    """Wilder's smoothing (RMA)，初始值用简单均值。"""
    return s.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


# ============================================================
# 1) ADX + DI 方向
# ============================================================
def compute_adx(df_idx: pd.DataFrame, n: int = 14,
                adx_th: float = 25.0) -> pd.DataFrame:
    """
    ADX 趋势强度 + 方向性指标 (+DI / -DI)。

    参数:
        df_idx: 指数日线，含列 trade_date, high, low, close
        n:      ADX 窗口（默认 14）
        adx_th: ADX 阈值，超过则认为趋势明确

    返回:
        DataFrame[trade_date, adx_<n>, pdi, mdi, adx_<n>_signal]
        （值列和信号列名都带 _<n>，与 factors 字典 key 对齐）
    """
    col = f"adx_{n}"
    sig_col = f"{col}_signal"
    d = df_idx[["trade_date", "high", "low", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    high = d["high"].astype(float)
    low = d["low"].astype(float)
    close = d["close"].astype(float)

    up = high.diff()
    dn = -low.diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = _rma(tr, n)
    pdi = 100.0 * _rma(pd.Series(plus_dm, index=d.index), n) / atr.replace(0, np.nan)
    mdi = 100.0 * _rma(pd.Series(minus_dm, index=d.index), n) / atr.replace(0, np.nan)

    dx = (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan) * 100.0
    adx = _rma(dx, n)

    d[col] = adx.values
    d["pdi"] = pdi.values
    d["mdi"] = mdi.values

    def _sig(row):
        a, p, m = row[col], row["pdi"], row["mdi"]
        if pd.isna(a) or pd.isna(p) or pd.isna(m):
            return 0
        if a < adx_th:
            return 0
        return 1 if p > m else -1

    d[sig_col] = d.apply(_sig, axis=1)
    return d[["trade_date", col, "pdi", "mdi", sig_col]]


# ============================================================
# 2) OBV 动量偏离率
# ============================================================
def compute_obv_momentum(df_idx: pd.DataFrame, n: int = 20,
                         up_th: float = 0.02, dn_th: float = -0.02) -> pd.DataFrame:
    """
    OBV 动量偏离率：(OBV - MA_OBV_n) / |MA_OBV_n|

    参数:
        df_idx: 指数日线，含列 trade_date, close, vol
        n:      偏离率均线窗口
        up_th:  看多阈值（默认 +2%，OBV 比均线高 2%）
        dn_th:  看空阈值

    返回:
        DataFrame[trade_date, obv, obv_mom_<n>, obv_mom_<n>_signal]
    """
    col = f"obv_mom_{n}"
    sig_col = f"{col}_signal"

    d = df_idx[["trade_date", "close", "vol"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    close = d["close"].astype(float)
    vol = d["vol"].astype(float)
    sign = np.sign(close.diff().fillna(0))
    obv = (sign * vol).cumsum()

    ma = obv.rolling(n, min_periods=5).mean()
    obv_mom = (obv - ma) / ma.abs().replace(0, np.nan)

    d["obv"] = obv.values
    d[col] = obv_mom.values

    def _sig(v):
        if pd.isna(v):
            return 0
        if v >= up_th:
            return 1
        if v <= dn_th:
            return -1
        return 0

    d[sig_col] = d[col].apply(_sig)
    return d[["trade_date", "obv", col, sig_col]]


# ============================================================
# 统一入口
# ============================================================
def compute_trend_strength(df_idx: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "adx_14":     compute_adx(df_idx, n=14),
        "obv_mom_20": compute_obv_momentum(df_idx, n=20),
    }
