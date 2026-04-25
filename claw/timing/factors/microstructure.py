"""
因子组：微结构与风险（MFI / Williams%R / CVaR / 量价微结构）
================================================
针对大盘指数，在不同频率上捕捉"资金流 + 超买超卖 + 尾部风险 + 日内结构"。

导出 8 个子因子：
    mfi_14          —— 14 日资金流量指标，反向使用（高位卖出/低位买入）
    willr_14        —— 14 日 Williams %R 超买超卖
    cvar_60         —— 60 日 CVaR 5%（尾部风险），反向使用
    up_strength     —— 连续上涨天数 × 平均涨幅（上攻力度）
    close_pos_5     —— 5 日尾盘强度（收盘在日内高低区间的位置）
    vol_range_div   —— 量幅背离（放量但振幅收窄 = 蓄势）
    intra_eff       —— 日内波动效率（收益绝对值 / 振幅）
    gap_strength_10 —— 10 日平均跳空缺口强度

所有因子输出：trade_date, <value>, <value>_signal
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _sig_up_dn(v, up: float, dn: float) -> int:
    if pd.isna(v):
        return 0
    if v >= up:
        return 1
    if v <= dn:
        return -1
    return 0


# ============================================================
# 1) MFI —— 资金流量指标（反向信号：超买→-1，超卖→+1）
# ============================================================
def compute_mfi(df_idx: pd.DataFrame, n: int = 14,
                overbought: float = 80.0, oversold: float = 20.0) -> pd.DataFrame:
    d = df_idx[["trade_date", "high", "low", "close", "vol"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    mf = tp * d["vol"]
    diff = tp.diff()
    pos_mf = mf.where(diff > 0, 0.0).rolling(n).sum()
    neg_mf = mf.where(diff < 0, 0.0).rolling(n).sum()
    mfr = pos_mf / neg_mf.replace(0, np.nan)
    mfi = 100.0 - 100.0 / (1.0 + mfr)

    d["mfi_14"] = mfi
    # 反向信号：超卖 → +1（低位可买入）；超买 → -1
    d["mfi_14_signal"] = mfi.apply(lambda v: 1 if (not pd.isna(v) and v <= oversold)
                                                 else (-1 if (not pd.isna(v) and v >= overbought) else 0))
    return d[["trade_date", "mfi_14", "mfi_14_signal"]]


# ============================================================
# 2) Williams %R —— 超买超卖（反向使用）
# ============================================================
def compute_willr(df_idx: pd.DataFrame, n: int = 14,
                  overbought: float = -20.0, oversold: float = -80.0) -> pd.DataFrame:
    """
    Williams %R 范围 [-100, 0]，-20 为超买，-80 为超卖。反向使用。
    """
    d = df_idx[["trade_date", "high", "low", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    hh = d["high"].rolling(n).max()
    ll = d["low"].rolling(n).min()
    wr = (hh - d["close"]) / (hh - ll).replace(0, np.nan) * (-100.0)

    d["willr_14"] = wr
    d["willr_14_signal"] = wr.apply(lambda v: 1 if (not pd.isna(v) and v <= oversold)
                                                   else (-1 if (not pd.isna(v) and v >= overbought) else 0))
    return d[["trade_date", "willr_14", "willr_14_signal"]]


# ============================================================
# 3) CVaR 60 —— 尾部风险（A股反向使用）
# ============================================================
def compute_cvar(df_idx: pd.DataFrame, n: int = 60, q: float = 0.05,
                 up_pct: float = 0.75, dn_pct: float = 0.25) -> pd.DataFrame:
    """
    60 日窗内条件在险价值 CVaR(5%)：负尾均值。

    ⚠️ A股经验：尾部风险过大通常发生在已经急跌之后（筑底反转），
              尾部风险过小通常发生在低波高位（过度乐观，后市易调整）。
    所以对 A 股采用 **反向信号**：
        分位 > up_pct  (当前尾部风险显著高于历史，往往是恐慌底部) → +1
        分位 < dn_pct  (尾部风险过低，市场麻木) → -1
    """
    d = df_idx[["trade_date", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    ret = d["close"].pct_change()

    def _cvar(x: np.ndarray) -> float:
        if len(x) < 10 or np.all(np.isnan(x)):
            return np.nan
        x = x[~np.isnan(x)]
        if len(x) == 0:
            return np.nan
        thr = np.quantile(x, q)
        tail = x[x <= thr]
        return float(tail.mean()) if len(tail) else np.nan

    cvar = ret.rolling(n, min_periods=max(20, n // 2)).apply(_cvar, raw=True)
    # CVaR 数值本身是负数，越接近 0 越好；做绝对值后再取分位，便于理解
    cvar_abs = cvar.abs()
    pct = cvar_abs.rolling(500, min_periods=60).apply(lambda s: s.rank(pct=True).iloc[-1], raw=False)

    d["cvar_60"] = cvar
    # 反向信号：高分位（尾部风险大）→ +1（筑底）；低分位（尾部风险小）→ -1（过度乐观）
    d["cvar_60_signal"] = pct.apply(lambda v: 1 if (not pd.isna(v) and v > up_pct)
                                                    else (-1 if (not pd.isna(v) and v < dn_pct) else 0))
    return d[["trade_date", "cvar_60", "cvar_60_signal"]]


# ============================================================
# 4) up_strength —— 连续上涨天数 × 平均涨幅
# ============================================================
def compute_up_strength(df_idx: pd.DataFrame, n: int = 5,
                        up_th: float = 0.003, dn_th: float = -0.003) -> pd.DataFrame:
    d = df_idx[["trade_date", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    ret = d["close"].pct_change()
    # 近 n 日平均收益 + 上涨天数占比的组合
    avg_ret = ret.rolling(n).mean()
    up_ratio = (ret > 0).astype(float).rolling(n).mean()
    strength = avg_ret * up_ratio

    d["up_strength"] = strength
    d["up_strength_signal"] = strength.apply(lambda v: _sig_up_dn(v, up_th, dn_th))
    return d[["trade_date", "up_strength", "up_strength_signal"]]


# ============================================================
# 5) close_pos_5 —— 5 日尾盘强度
# ============================================================
def compute_close_pos(df_idx: pd.DataFrame, n: int = 5,
                      up_th: float = 0.65, dn_th: float = 0.35) -> pd.DataFrame:
    """收盘在 (low, high) 中的位置，5 日均值。>0.65 看多，<0.35 看空。"""
    d = df_idx[["trade_date", "high", "low", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    pos = (d["close"] - d["low"]) / (d["high"] - d["low"]).replace(0, np.nan)
    pos_ma = pos.rolling(n).mean()

    d["close_pos_5"] = pos_ma
    d["close_pos_5_signal"] = pos_ma.apply(lambda v: _sig_up_dn(v, up_th, dn_th))
    return d[["trade_date", "close_pos_5", "close_pos_5_signal"]]


# ============================================================
# 6) vol_range_div —— 量幅背离（放量 + 振幅收窄 = 蓄势）
# ============================================================
def compute_vol_range_div(df_idx: pd.DataFrame, n: int = 10,
                          up_th: float = 1.25, dn_th: float = 0.80) -> pd.DataFrame:
    """
    量幅比：成交量相对 n 日均量 / 振幅相对 n 日均幅。
    值越高 = 放量收窄（看多蓄势），越低 = 缩量放幅（看空）。
    """
    d = df_idx[["trade_date", "high", "low", "vol"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    amp = (d["high"] - d["low"]) / d["low"].replace(0, np.nan)
    v_ratio = d["vol"] / d["vol"].rolling(n).mean().replace(0, np.nan)
    a_ratio = amp / amp.rolling(n).mean().replace(0, np.nan)
    div = v_ratio / a_ratio.replace(0, np.nan)
    div_ma = div.rolling(n).mean()

    d["vol_range_div"] = div_ma
    d["vol_range_div_signal"] = div_ma.apply(lambda v: _sig_up_dn(v, up_th, dn_th))
    return d[["trade_date", "vol_range_div", "vol_range_div_signal"]]


# ============================================================
# 7) intra_eff —— 日内波动效率
# ============================================================
def compute_intra_eff(df_idx: pd.DataFrame, n: int = 5,
                      up_th: float = 0.15, dn_th: float = -0.15) -> pd.DataFrame:
    """
    (close - open) / (high - low)：带符号的方向性效率，n 日均值。
    > 0.15 趋势日且多方占优 → 看多；< -0.15 看空。
    之前阈值 0.5 过严，A 股日内效率均值多在 0.2 附近。
    """
    d = df_idx[["trade_date", "open", "high", "low", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    body = (d["close"] - d["open"])
    rng = (d["high"] - d["low"]).replace(0, np.nan)
    eff = body / rng  # 带符号的效率 [-1, 1]
    eff_ma = eff.rolling(n).mean()

    d["intra_eff"] = eff_ma
    d["intra_eff_signal"] = eff_ma.apply(lambda v: _sig_up_dn(v, up_th, -up_th))
    return d[["trade_date", "intra_eff", "intra_eff_signal"]]


# ============================================================
# 8) gap_strength_10 —— 10 日平均跳空缺口
# ============================================================
def compute_gap_strength(df_idx: pd.DataFrame, n: int = 10,
                         up_th: float = 0.002, dn_th: float = -0.002) -> pd.DataFrame:
    d = df_idx[["trade_date", "open", "close"]].copy()
    d = d.sort_values("trade_date").reset_index(drop=True)

    gap = (d["open"] - d["close"].shift(1)) / d["close"].shift(1).replace(0, np.nan)
    gap_ma = gap.rolling(n).mean()

    d["gap_strength_10"] = gap_ma
    d["gap_strength_10_signal"] = gap_ma.apply(lambda v: _sig_up_dn(v, up_th, dn_th))
    return d[["trade_date", "gap_strength_10", "gap_strength_10_signal"]]


# ============================================================
# 统一入口
# ============================================================
def compute_microstructure(df_idx: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "mfi_14":          compute_mfi(df_idx, n=14),
        "willr_14":        compute_willr(df_idx, n=14),
        "cvar_60":         compute_cvar(df_idx, n=60),
        "up_strength":     compute_up_strength(df_idx, n=5),
        "close_pos_5":     compute_close_pos(df_idx, n=5),
        "vol_range_div":   compute_vol_range_div(df_idx, n=10),
        "intra_eff":       compute_intra_eff(df_idx, n=5),
        "gap_strength_10": compute_gap_strength(df_idx, n=10),
    }
