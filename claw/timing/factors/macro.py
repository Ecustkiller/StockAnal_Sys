"""
claw.timing.factors.macro —— 宏观/资金流择时因子组
========================================================

设计思想：
    纯价量因子在不同 regime 下严重过拟合（IS vs OOS 全部失败），
    本模块改用 **宏观/资金流/利率/汇率** 等低频稳健数据做择时。

导出 2 个子因子（经 IS/OOS 验证筛选后保留）：
    1. turnover_pct_252   —— HS300 成交额 252 日滚动分位
                              高分位（量价配合） → +1；低分位（冷清） → -1
                              ⭐ IS +1.24% / OOS +1.17%，Sharpe 0.58 → 0.62，MDD -21% → -12%
    2. us_cn_10y_spread   —— 中美 10Y 国债利差（美-中），60 日 Z-score
                              Z > +1 (利差异常走阔) → -1；Z < -1 (利差异常收窄) → +1
                              ⭐ 半稳健：IS +2.66% / OOS +0.67%，符号一致 + Sharpe 0.74/0.74 完全稳定

已弃用因子（IS/OOS 验证未通过）：
    - turnover_pct_20     —— IS +6.91% / OOS -3.87%，反向，regime 依赖
    - north_money_ma20    —— IS -2.02% / OOS -2.08%，双失
    - margin_trend_60     —— IS -2.40% / OOS -11.04%，双失
    - amount_momentum_20  —— IS -4.29% / OOS -0.03%，双失

所有因子输出：trade_date, <name>, <name>_signal
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from claw.core.tushare_client import ts

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "timing_macro"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_csv(name: str, start: str, end: str) -> Path:
    return CACHE_DIR / f"{name}_{start}_{end}.csv"


# ============================================================
# 数据加载层（按需拉 tushare，带本地缓存）
# ============================================================
def _load_hsgt(start: str, end: str) -> pd.DataFrame:
    cache = _cache_csv("hsgt", start, end)
    if cache.exists():
        return pd.read_csv(cache, dtype={"trade_date": str})
    df = ts("moneyflow_hsgt", {"start_date": start, "end_date": end})
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "north_money"])
    df["trade_date"] = df["trade_date"].astype(str)
    df = df[["trade_date", "north_money"]].sort_values("trade_date").reset_index(drop=True)
    df["north_money"] = pd.to_numeric(df["north_money"], errors="coerce")
    df.to_csv(cache, index=False)
    return df


def _load_us10y(start: str, end: str) -> pd.DataFrame:
    cache = _cache_csv("us_y10", start, end)
    if cache.exists():
        return pd.read_csv(cache, dtype={"trade_date": str})
    df = ts("us_tycr", {"start_date": start, "end_date": end})
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "us_y10"])
    df = df.rename(columns={"date": "trade_date"})
    df["trade_date"] = df["trade_date"].astype(str)
    df = df[["trade_date", "y10"]].rename(columns={"y10": "us_y10"})
    df["us_y10"] = pd.to_numeric(df["us_y10"], errors="coerce")
    df = df.sort_values("trade_date").reset_index(drop=True)
    df.to_csv(cache, index=False)
    return df


def _load_cn10y(start: str, end: str) -> pd.DataFrame:
    """中债国债 10Y 收益率：yc_cb (curve_type=0 国债)，curve_term=10 年"""
    cache = _cache_csv("cn_y10", start, end)
    if cache.exists():
        return pd.read_csv(cache, dtype={"trade_date": str})
    # tushare yc_cb 一次只能返回 ~2000 行，这里按年拆分
    parts = []
    y_start = int(start[:4])
    y_end = int(end[:4])
    for y in range(y_start, y_end + 1):
        s = f"{y}0101" if y > y_start else start
        e = f"{y}1231" if y < y_end else end
        df = ts("yc_cb", {"curve_type": "0", "start_date": s, "end_date": e})
        if df.empty:
            continue
        # 10Y 对应 curve_term = 10
        df["curve_term"] = pd.to_numeric(df["curve_term"], errors="coerce")
        df10 = df[df["curve_term"] == 10.0].copy()
        parts.append(df10[["trade_date", "yield"]])
    if not parts:
        return pd.DataFrame(columns=["trade_date", "cn_y10"])
    full = pd.concat(parts, ignore_index=True)
    full["trade_date"] = full["trade_date"].astype(str)
    full = full.rename(columns={"yield": "cn_y10"})
    full["cn_y10"] = pd.to_numeric(full["cn_y10"], errors="coerce")
    full = full.drop_duplicates("trade_date").sort_values("trade_date").reset_index(drop=True)
    full.to_csv(cache, index=False)
    return full


def _load_margin(start: str, end: str) -> pd.DataFrame:
    """两融余额：按交易所聚合（sh+sz），取融资余额 rzye"""
    cache = _cache_csv("margin", start, end)
    if cache.exists():
        return pd.read_csv(cache, dtype={"trade_date": str})
    # margin 接口支持日期范围，但字段分 SH/SZ/BSE，聚合即可
    df = ts("margin", {"start_date": start, "end_date": end})
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "rzye_total"])
    df["trade_date"] = df["trade_date"].astype(str)
    df["rzye"] = pd.to_numeric(df["rzye"], errors="coerce")
    agg = df.groupby("trade_date", as_index=False)["rzye"].sum().rename(columns={"rzye": "rzye_total"})
    agg = agg.sort_values("trade_date").reset_index(drop=True)
    agg.to_csv(cache, index=False)
    return agg


# ============================================================
# 因子 1：成交额 252 日分位（趋势跟随逻辑）
# ============================================================
def compute_turnover_pct_252(df_idx: pd.DataFrame, n: int = 252,
                             hot_pct: float = 0.70, cold_pct: float = 0.30) -> pd.DataFrame:
    """
    指数成交额滚动分位。
    经 IS/OOS 验证，采用 **量价配合** 逻辑（趋势跟随）：
      - 分位 > hot_pct  (放量阶段)    → +1  市场活跃看多
      - 分位 < cold_pct (缩量阶段)    → -1  市场冷清看空
      - 中间区间                      → 0
    """
    d = df_idx[["trade_date", "amount"]].copy()
    d["trade_date"] = d["trade_date"].astype(str)
    d = d.sort_values("trade_date").reset_index(drop=True)
    d["amount"] = pd.to_numeric(d["amount"], errors="coerce")

    pct = d["amount"].rolling(n, min_periods=n // 2).apply(
        lambda s: s.rank(pct=True).iloc[-1], raw=False)

    d["turnover_pct_252"] = pct
    d["turnover_pct_252_signal"] = pct.apply(
        lambda v: 1 if (not pd.isna(v) and v > hot_pct)
                  else (-1 if (not pd.isna(v) and v < cold_pct) else 0))
    return d[["trade_date", "turnover_pct_252", "turnover_pct_252_signal"]]


def compute_turnover_pct_20(df_idx: pd.DataFrame, n: int = 20,
                            hot_pct: float = 0.70, cold_pct: float = 0.30) -> pd.DataFrame:
    """短周期成交额分位（趋势跟随）。规则同 #1。"""
    d = df_idx[["trade_date", "amount"]].copy()
    d["trade_date"] = d["trade_date"].astype(str)
    d = d.sort_values("trade_date").reset_index(drop=True)
    d["amount"] = pd.to_numeric(d["amount"], errors="coerce")

    pct = d["amount"].rolling(n, min_periods=n // 2).apply(
        lambda s: s.rank(pct=True).iloc[-1], raw=False)
    d["turnover_pct_20"] = pct
    d["turnover_pct_20_signal"] = pct.apply(
        lambda v: 1 if (not pd.isna(v) and v > hot_pct)
                  else (-1 if (not pd.isna(v) and v < cold_pct) else 0))
    return d[["trade_date", "turnover_pct_20", "turnover_pct_20_signal"]]


# ============================================================
# 因子 2：北向资金 20 日累计
# ============================================================
def compute_north_money_ma20(df_idx: pd.DataFrame,
                             n: int = 20, zwin: int = 120,
                             up_z: float = 0.5, dn_z: float = -0.5) -> pd.DataFrame:
    """
    北向资金 20 日累计净流入，映射到 120 日滚动 Z-score：
        Z > up_z  (最近流入明显强于历史)  → +1
        Z < dn_z  (最近流入明显弱于历史)  → -1

    之前用 252 日分位阈值导致信号常年偏多，换为更稳的滚动 Z-score。
    """
    base = df_idx[["trade_date"]].copy()
    base["trade_date"] = base["trade_date"].astype(str)
    s = base["trade_date"].min()
    e = base["trade_date"].max()
    hsgt = _load_hsgt(s, e)
    if hsgt.empty:
        base["north_money_ma20"] = np.nan
        base["north_money_ma20_signal"] = 0
        return base

    d = base.merge(hsgt, on="trade_date", how="left")
    d["north_money"] = pd.to_numeric(d["north_money"], errors="coerce").fillna(0)
    d["nm_cum20"] = d["north_money"].rolling(n, min_periods=n // 2).sum()
    mu = d["nm_cum20"].rolling(zwin, min_periods=zwin // 3).mean()
    sd = d["nm_cum20"].rolling(zwin, min_periods=zwin // 3).std()
    z = (d["nm_cum20"] - mu) / sd.replace(0, np.nan)

    d["north_money_ma20"] = d["nm_cum20"]
    d["north_money_ma20_signal"] = z.apply(
        lambda v: 1 if (not pd.isna(v) and v > up_z)
                  else (-1 if (not pd.isna(v) and v < dn_z) else 0))
    return d[["trade_date", "north_money_ma20", "north_money_ma20_signal"]]


# ============================================================
# 因子 3：中美 10Y 利差
# ============================================================
def compute_us_cn_10y_spread(df_idx: pd.DataFrame,
                             n: int = 60,
                             up_z: float = 1.0, dn_z: float = -1.0) -> pd.DataFrame:
    """
    美10Y - 中10Y，Z-score 过高 → 资金外流压力 → -1；过低 → +1。
    """
    base = df_idx[["trade_date"]].copy()
    base["trade_date"] = base["trade_date"].astype(str)
    s = base["trade_date"].min()
    e = base["trade_date"].max()

    us = _load_us10y(s, e)
    cn = _load_cn10y(s, e)
    if us.empty or cn.empty:
        base["us_cn_10y_spread"] = np.nan
        base["us_cn_10y_spread_signal"] = 0
        return base

    d = base.merge(us, on="trade_date", how="left") \
            .merge(cn, on="trade_date", how="left")
    d["us_y10"] = pd.to_numeric(d["us_y10"], errors="coerce").ffill()
    d["cn_y10"] = pd.to_numeric(d["cn_y10"], errors="coerce").ffill()
    d["spread"] = d["us_y10"] - d["cn_y10"]
    mu = d["spread"].rolling(n, min_periods=n // 2).mean()
    sd = d["spread"].rolling(n, min_periods=n // 2).std()
    z = (d["spread"] - mu) / sd.replace(0, np.nan)

    d["us_cn_10y_spread"] = d["spread"]
    # 利差变大（美>中更多）→ 资金外流压力 → -1；利差收窄 → +1
    d["us_cn_10y_spread_signal"] = z.apply(
        lambda v: -1 if (not pd.isna(v) and v > up_z)
                  else (1 if (not pd.isna(v) and v < dn_z) else 0))
    return d[["trade_date", "us_cn_10y_spread", "us_cn_10y_spread_signal"]]


# ============================================================
# 因子 5：成交额 20 日动量（替代 margin_trend_60）
# ============================================================
def compute_amount_momentum_20(df_idx: pd.DataFrame,
                                n: int = 20,
                                up_th: float = 0.15, dn_th: float = -0.15) -> pd.DataFrame:
    """
    成交额 20 日环比变化：
        > +15% (量能扩张)  → +1
        < -15% (量能萎缩)  → -1
    """
    d = df_idx[["trade_date", "amount"]].copy()
    d["trade_date"] = d["trade_date"].astype(str)
    d = d.sort_values("trade_date").reset_index(drop=True)
    d["amount"] = pd.to_numeric(d["amount"], errors="coerce")

    amt_ma_n = d["amount"].rolling(n).mean()
    amt_ma_2n = d["amount"].rolling(n * 2).mean()
    mom = (amt_ma_n / amt_ma_2n.replace(0, np.nan)) - 1.0

    d["amount_momentum_20"] = mom
    d["amount_momentum_20_signal"] = mom.apply(
        lambda v: 1 if (not pd.isna(v) and v > up_th)
                  else (-1 if (not pd.isna(v) and v < dn_th) else 0))
    return d[["trade_date", "amount_momentum_20", "amount_momentum_20_signal"]]


# ============================================================
# 因子 (已弃用)：融资余额 60 日变化率
# ============================================================
def compute_margin_trend_60(df_idx: pd.DataFrame,
                            n: int = 60,
                            up_pct: float = 0.02, dn_pct: float = -0.02) -> pd.DataFrame:
    """
    融资余额 60 日变化率：
        > up_pct (杠杆扩张 > 2%)  → +1  乐观
        < dn_pct (杠杆收缩 > 2%)  → -1  悲观
    """
    base = df_idx[["trade_date"]].copy()
    base["trade_date"] = base["trade_date"].astype(str)
    s = base["trade_date"].min()
    e = base["trade_date"].max()
    mg = _load_margin(s, e)
    if mg.empty:
        base["margin_trend_60"] = np.nan
        base["margin_trend_60_signal"] = 0
        return base

    d = base.merge(mg, on="trade_date", how="left")
    d["rzye_total"] = pd.to_numeric(d["rzye_total"], errors="coerce").ffill()
    chg = d["rzye_total"].pct_change(n)

    d["margin_trend_60"] = chg
    d["margin_trend_60_signal"] = chg.apply(
        lambda v: 1 if (not pd.isna(v) and v > up_pct)
                  else (-1 if (not pd.isna(v) and v < dn_pct) else 0))
    return d[["trade_date", "margin_trend_60", "margin_trend_60_signal"]]


# ============================================================
# 统一入口
# ============================================================
def compute_macro(df_idx: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    一次性计算所有宏观因子。经 IS/OOS 筛选后，只保留 2 个验证通过的因子。
    df_idx 需包含 trade_date, amount（由 claw.timing.data.load_index_daily 提供）。
    """
    return {
        "turnover_pct_252":   compute_turnover_pct_252(df_idx),
        "us_cn_10y_spread":   compute_us_cn_10y_spread(df_idx),
    }
