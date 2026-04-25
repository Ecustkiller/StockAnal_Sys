"""
claw.timing.market_timer — 大盘行情择时框架 v1

目标：用 10 个趋势+情绪信号预测大盘行情，输出 0~100% 仓位建议。

设计原则（严格反数据泄露）：
    1. 所有信号 score[t] 基于 t 日 EOD 数据；实际仓位 position_exec[t+1] = score[t]
    2. 参数分位数使用"过去 252 日滚动估计"，不用全样本（避免 look-ahead）
    3. 0/1/2 三档评分 → 总分 0~20 → 5 档仓位映射

10 个信号清单：
    趋势层（5 个）
        S01  HS300 收盘 > MA60                        （快速趋势）
        S02  HS300 MA20 > MA60                        （中期趋势）
        S03  HS300 20 日动量 > 0                      （价格动量）
        S04  HS300 MACD(12,26,9) 柱状图 > 0           （趋势加速）
        S05  ADX(14) > 25                             （趋势强度过滤）

    情绪层（5 个）
        S06  涨停家数 - 跌停家数 > 0                  （赚钱效应）
        S07  市场宽度（收盘>MA20 股票占比）           （参与度）
        S08  沪深300 成交额 252 日滚动分位            （量能）
        S09  沪深300 20 日年化波动率（滚动分位）      （风险偏好，反向）
        S10  连板数（最高涨停连板天数）               （资金活跃度）

评分规则：
    每个信号 0/1/2 三档（看空/中性/看多）
    总分 0~20 → 仓位映射 5 档：
        16+  → 100% (FULL)
        12-16 →  80% (BULL)
         8-12 →  60% (NEUTRAL)
         4-8  →  30% (RISK_OFF)
         0-4  →  10% (BEAR)

用法：
    from claw.timing.market_timer import MarketTimer
    mt = MarketTimer(mode="balanced")
    df = mt.generate("20210101", "20260420")
    # df: [trade_date, score, position, pos_exec, s01..s10]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from claw.timing import data as tdata

# ================================================================
# 仓位映射（三档：保守 / 平衡 / 激进）
# ================================================================
MAPPING_PRESETS = {
    # 激进：看多就上满仓，信号弱就降到 30%
    "aggressive": [(16, 1.00), (12, 0.90), (8, 0.70), (4, 0.40), (0, 0.20)],
    # 平衡（推荐）：牛市跟得上，熊市躲得住
    "balanced":   [(16, 1.00), (12, 0.80), (8, 0.60), (4, 0.30), (0, 0.10)],
    # 保守：看多才给仓位，看空立刻撤
    "conservative": [(17, 1.00), (13, 0.70), (9, 0.50), (5, 0.20), (0, 0.00)],
}


@dataclass
class MarketTimerConfig:
    mode: str = "balanced"
    index_code: str = "000300.SH"
    # 滚动估计窗口（用于分位数计算）
    roll_window: int = 252
    # ADX 阈值
    adx_threshold: float = 25.0
    # 熊市硬保护：ma20 < ma60 且 20d 动量 <-8% 直接 BEAR
    hard_bear_dd: float = -8.0


# ================================================================
# 技术指标工具
# ================================================================
def _macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    dif = ema_f - ema_s
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = (dif - dea) * 2
    return dif, dea, hist


def _adx(df: pd.DataFrame, n=14) -> pd.Series:
    """
    Wilder 的 ADX 实现。df 需有 high, low, close 列。
    """
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    dn = -low.diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = pd.concat([(high - low),
                    (high - close.shift()).abs(),
                    (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/n, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1/n, adjust=False).mean()
    return adx


def _rolling_pct_rank(s: pd.Series, window: int) -> pd.Series:
    """某值在过去 window 个值中的百分位（0~1）。用 min_periods=60 保证早期也有值。"""
    return s.rolling(window, min_periods=60).rank(pct=True)


# ================================================================
# 主类
# ================================================================
class MarketTimer:
    def __init__(self, mode: str = "balanced",
                 index_code: str = "000300.SH",
                 roll_window: int = 252):
        if mode not in MAPPING_PRESETS:
            raise ValueError(f"mode must be one of {list(MAPPING_PRESETS)}")
        self.cfg = MarketTimerConfig(mode=mode, index_code=index_code,
                                     roll_window=roll_window)
        self._debug_last: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------
    # 数据加载与拼接
    # ------------------------------------------------------------
    def _load_all(self, start: str, end: str) -> pd.DataFrame:
        # 为了有足够历史算 MA60 / 滚动分位，前推 400 日
        from datetime import datetime, timedelta
        d0 = datetime.strptime(start, "%Y%m%d")
        pre_start = (d0 - timedelta(days=600)).strftime("%Y%m%d")

        idx = tdata.load_index_daily(self.cfg.index_code, pre_start, end)
        idx["trade_date"] = idx["trade_date"].astype(str)
        idx = idx.sort_values("trade_date").reset_index(drop=True)

        # 涨跌停
        try:
            lim = tdata.load_limit_stats(pre_start, end)
            lim["trade_date"] = lim["trade_date"].astype(str)
        except Exception as e:
            print(f"⚠️  涨跌停数据加载失败: {e}")
            lim = pd.DataFrame(columns=["trade_date", "up_count", "down_count", "up_stat"])

        # 市场宽度
        try:
            brd = tdata.load_breadth_from_tushare(pre_start, end)
            brd["trade_date"] = brd["trade_date"].astype(str)
        except Exception as e:
            print(f"⚠️  市场宽度加载失败: {e}")
            brd = pd.DataFrame(columns=["trade_date", "breadth_ma20"])

        df = idx.merge(lim, on="trade_date", how="left") \
                .merge(brd, on="trade_date", how="left")
        return df

    # ------------------------------------------------------------
    # 信号生成：10 个 0/1/2 评分
    # ------------------------------------------------------------
    def _compute_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        roll = self.cfg.roll_window

        # -------- 趋势层 --------
        d["ma20"] = d["close"].rolling(20, min_periods=10).mean()
        d["ma60"] = d["close"].rolling(60, min_periods=20).mean()
        d["mom20"] = d["close"].pct_change(20) * 100

        # S01: close vs MA60
        d["s01"] = np.where(d["close"] > d["ma60"] * 1.02, 2,
                    np.where(d["close"] < d["ma60"] * 0.98, 0, 1))
        # S02: MA20 vs MA60
        d["s02"] = np.where(d["ma20"] > d["ma60"] * 1.01, 2,
                    np.where(d["ma20"] < d["ma60"] * 0.99, 0, 1))
        # S03: 20d 动量
        d["s03"] = np.where(d["mom20"] > 3, 2,
                    np.where(d["mom20"] < -3, 0, 1))
        # S04: MACD 柱
        _, _, hist = _macd(d["close"])
        d["macd_hist"] = hist
        # 用柱状图的**变化率**来判断加速/减速，保留中性档
        # 标准化到历史分位数：柱值 + 柱值一阶差的组合
        d["macd_rank"] = _rolling_pct_rank(hist, roll)
        d["s04"] = np.where(d["macd_rank"] > 0.6, 2,
                   np.where(d["macd_rank"] < 0.4, 0, 1))
        # S05: ADX 强度（趋势是否有效）
        d["adx"] = _adx(d)
        # ADX 高 + 价格涨 → 2；ADX 高 + 价格跌 → 0；ADX 低 → 1（无效趋势）
        d["s05"] = np.where((d["adx"] > self.cfg.adx_threshold) & (d["close"] > d["ma20"]), 2,
                   np.where((d["adx"] > self.cfg.adx_threshold) & (d["close"] < d["ma20"]), 0, 1))

        # -------- 情绪层 --------
        # S06: 涨跌停差
        d["up_count"] = pd.to_numeric(d.get("up_count", 0), errors="coerce").fillna(0)
        d["down_count"] = pd.to_numeric(d.get("down_count", 0), errors="coerce").fillna(0)
        d["limit_diff"] = d["up_count"] - d["down_count"]
        # 用过去 252 日的"涨跌停差"分位数判断
        d["limit_diff_rank"] = _rolling_pct_rank(d["limit_diff"], roll)
        d["s06"] = np.where(d["limit_diff_rank"] > 0.7, 2,
                   np.where(d["limit_diff_rank"] < 0.3, 0, 1))

        # S07: 市场宽度 MA20
        d["breadth_ma20"] = pd.to_numeric(d.get("breadth_ma20", np.nan), errors="coerce")
        d["breadth_rank"] = _rolling_pct_rank(d["breadth_ma20"], roll)
        # 宽度 > 60 明显看多；< 40 看空
        d["s07"] = np.where(d["breadth_ma20"] > 60, 2,
                   np.where(d["breadth_ma20"] < 40, 0, 1))

        # S08: 成交额分位（缩量看空、放量温和看多）
        d["amount"] = pd.to_numeric(d.get("amount", np.nan), errors="coerce")
        d["amount_rank"] = _rolling_pct_rank(d["amount"], roll)
        # 注意：极端放量（>90 分位）可能是见顶信号 → 中性
        d["s08"] = np.where((d["amount_rank"] > 0.5) & (d["amount_rank"] < 0.9), 2,
                   np.where(d["amount_rank"] < 0.2, 0, 1))

        # S09: 波动率（反向）
        d["ret1"] = d["close"].pct_change()
        d["vol20"] = d["ret1"].rolling(20, min_periods=10).std() * np.sqrt(252) * 100
        d["vol_rank"] = _rolling_pct_rank(d["vol20"], roll)
        # 低波动 = 看多（牛市回撤小）；高波动 = 看空（风险事件）
        d["s09"] = np.where(d["vol_rank"] < 0.3, 2,
                   np.where(d["vol_rank"] > 0.8, 0, 1))

        # S10: 连板数
        d["up_stat"] = pd.to_numeric(d.get("up_stat", np.nan), errors="coerce")
        d["up_stat_ma5"] = d["up_stat"].rolling(5, min_periods=2).mean()
        # 用滚动分位数判断"情绪是否异常热/冷"（避免"连板数常态>2"导致的常量问题）
        d["up_stat_rank"] = _rolling_pct_rank(d["up_stat_ma5"], roll)
        d["s10"] = np.where(d["up_stat_rank"] > 0.7, 2,
                   np.where(d["up_stat_rank"] < 0.3, 0, 1))

        # 汇总
        sig_cols = [f"s{i:02d}" for i in range(1, 11)]
        for c in sig_cols:
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(1).astype(int)
        d["score"] = d[sig_cols].sum(axis=1)

        # -------- 映射到仓位 --------
        mapping = MAPPING_PRESETS[self.cfg.mode]

        def map_score(s):
            if pd.isna(s):
                return 0.6
            for thr, pos in mapping:
                if s >= thr:
                    return pos
            return mapping[-1][1]

        d["position"] = d["score"].apply(map_score)

        # -------- 硬保护：极端熊市强制空仓 --------
        hard_bear = (d["ma20"] < d["ma60"]) & (d["mom20"] < self.cfg.hard_bear_dd)
        d.loc[hard_bear, "position"] = min(d["position"].min(), 0.1)

        # -------- T+1 执行仓位 --------
        d["position_exec"] = d["position"].shift(1).fillna(0.6)

        return d

    # ------------------------------------------------------------
    # 主接口
    # ------------------------------------------------------------
    def generate(self, start: str = "20210101",
                 end: str = "20260420") -> pd.DataFrame:
        raw = self._load_all(start, end)
        sig = self._compute_signals(raw)
        out = sig[(sig["trade_date"] >= start) & (sig["trade_date"] <= end)].copy()
        out = out.reset_index(drop=True)
        self._debug_last = out
        return out

    # ------------------------------------------------------------
    # 打印诊断
    # ------------------------------------------------------------
    def describe(self, df: pd.DataFrame) -> None:
        print(f"\n{'='*70}")
        print(f"📊 MarketTimer[{self.cfg.mode}]  {self.cfg.index_code}")
        print(f"   {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}  "
              f"({len(df)} 交易日)")
        print(f"{'='*70}")

        # 仓位分布
        bins = [0, 0.2, 0.4, 0.7, 0.9, 1.01]
        labels = ["BEAR(<20%)", "RISK_OFF(20-40%)", "NEUTRAL(40-70%)",
                  "BULL(70-90%)", "FULL(≥90%)"]
        cat = pd.cut(df["position_exec"], bins=bins, labels=labels, include_lowest=True)
        print("\n🔎 仓位分布（执行口径）:")
        for k, v in cat.value_counts().reindex(labels).items():
            pct = v / len(df) * 100 if not pd.isna(v) else 0
            print(f"   {k:<20} {int(v or 0):>4} 天  ({pct:5.1f}%)")
        print(f"   平均仓位 = {df['position_exec'].mean()*100:5.1f}%")

        # 各信号触发率
        print("\n🔎 各信号触发分布（0/1/2 占比）:")
        sig_cols = [f"s{i:02d}" for i in range(1, 11)]
        names = {
            "s01": "MA60 趋势  ",
            "s02": "MA20/60 交叉",
            "s03": "20d 动量   ",
            "s04": "MACD 柱    ",
            "s05": "ADX 强度   ",
            "s06": "涨跌停差   ",
            "s07": "市场宽度   ",
            "s08": "成交额分位 ",
            "s09": "波动率(反) ",
            "s10": "连板情绪   ",
        }
        for c in sig_cols:
            vc = df[c].value_counts().reindex([0, 1, 2]).fillna(0)
            tot = vc.sum()
            print(f"   {names[c]} {c}: "
                  f"看空={vc[0]/tot*100:5.1f}%  "
                  f"中性={vc[1]/tot*100:5.1f}%  "
                  f"看多={vc[2]/tot*100:5.1f}%")


if __name__ == "__main__":
    mt = MarketTimer(mode="balanced")
    df = mt.generate("20210101", "20260420")
    mt.describe(df)
