"""
claw.timing.engine — 通用大盘择时引擎（策略无关）
=====================================================

设计目标：
    ✓ 择时逻辑与选股策略完全解耦
    ✓ 输出标准信号：TimingSignal[date, score, regime, position, confidence]
    ✓ 可插拔：下游任意策略只需 `ret *= engine.get_position(date)` 即可接入
    ✓ 可配置：因子子集、权重、仓位映射均可切换，方便 A/B 测试

核心概念：
    - FactorConfig   ：单个因子的启用/权重/参数配置
    - EngineConfig   ：整个择时引擎的配置（因子集 + 仓位映射 + 指数基准）
    - TimingEngine   ：主类，生产 position 序列
    - PositionMapper ：得分 → 仓位的 3 种预设 + 自定义 sigmoid

典型用法：
    from claw.timing.engine import TimingEngine, EngineConfig, PRESET_BALANCED
    eng = TimingEngine(PRESET_BALANCED)
    df = eng.run(start="20210101", end="20260420")
    # df[trade_date, total_score, regime, position, confidence, ...各因子信号]

    # 或只要仓位 map（给回测用）
    pos_map = eng.get_position_map(start="20210101", end="20260420")
    # {"20210104": 1.0, "20210105": 0.6, ...}
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from claw.timing import data as tdata
from claw.timing.factors import (
    compute_rsrs, compute_trend, compute_volatility,
    compute_sentiment_limit, compute_breadth,
    compute_macro  # 新增宏观因子计算
)

# ============================================================
# 仓位映射预设
# ============================================================
# 每个映射接收 score ∈ [-N, +N]，返回 (regime, position)
# 注意这里 score 的量级取决于因子数 × 权重；统一以"有符号整数投票分"为主

def _map_conservative(score: float) -> tuple[str, float]:
    """保守：牛市不敢满仓、熊市极度保守 —— 适合稳健型"""
    if pd.isna(score): return "NEUTRAL", 0.40
    if score >= 3:  return "BULL",     0.90
    if score >= 1:  return "RISK_ON",  0.70
    if score >= -1: return "NEUTRAL",  0.40
    if score >= -2: return "RISK_OFF", 0.20
    return              "BEAR",     0.00


def _map_balanced(score: float) -> tuple[str, float]:
    """平衡：当前 composer v2 的映射 —— 多数时间高仓位，只有明确看空才空仓"""
    if pd.isna(score): return "NEUTRAL", 0.60
    if score >= 3:  return "BULL",     1.00
    if score >= 1:  return "RISK_ON",  0.90
    if score >= -1: return "NEUTRAL",  0.60
    if score >= -2: return "RISK_OFF", 0.30
    return              "BEAR",     0.00


def _map_aggressive(score: float) -> tuple[str, float]:
    """激进：默认高仓位，只在极度看空时才降仓 —— 适合对择时能力不完全信任"""
    if pd.isna(score): return "NEUTRAL", 0.80
    if score >= 3:  return "BULL",     1.00
    if score >= 1:  return "RISK_ON",  1.00
    if score >= -1: return "NEUTRAL",  0.80
    if score >= -2: return "RISK_OFF", 0.50
    return              "BEAR",     0.00


def _map_sigmoid(score: float, k: float = 0.6) -> tuple[str, float]:
    """平滑：sigmoid(score * k) → [0, 1]，避免阶跃跳仓
    - k=0.6 时：score=+3 → 0.86，score=0 → 0.50，score=-3 → 0.14
    """
    if pd.isna(score): return "NEUTRAL", 0.50
    pos = 1.0 / (1.0 + math.exp(-score * k))
    if   score >= 3:  regime = "BULL"
    elif score >= 1:  regime = "RISK_ON"
    elif score >= -1: regime = "NEUTRAL"
    elif score >= -2: regime = "RISK_OFF"
    else:             regime = "BEAR"
    return regime, round(pos, 3)

def _map_2factor(score: float) -> tuple[str, float]:
    """专为2因子组合设计的仓位映射器（总分范围[-2, +2]）
    +2 → BULL 100%
    +1 → RISK_ON 80%  
     0 → NEUTRAL 60%
    -1 → RISK_OFF 30%
    -2 → BEAR 10%
    """
    if pd.isna(score): return "NEUTRAL", 0.60
    if score >= 2:  return "BULL",     1.00
    if score >= 1:  return "RISK_ON",  0.80
    if score >= -1: return "NEUTRAL",  0.60
    if score >= -2: return "RISK_OFF", 0.30
    return              "BEAR",     0.10

POSITION_MAPPERS: dict[str, Callable[[float], tuple[str, float]]] = {
    "conservative": _map_conservative,
    "balanced":     _map_balanced,
    "aggressive":   _map_aggressive,
    "sigmoid":      _map_sigmoid,
    "2factor":      _map_2factor,  # 新增：专为2因子组合设计
}

# ============================================================
# 因子 & 引擎配置
# ============================================================
@dataclass
class FactorConfig:
    """单因子配置 —— 名称、权重、是否启用"""
    name: str            # "rsrs" / "trend" / "vol" / "sentiment" / "breadth"
    weight: float = 1.0  # 投票权重（多数场景 1.0，想强化某因子可调大）
    enabled: bool = True


@dataclass
class EngineConfig:
    """择时引擎总配置"""
    index_code: str = "000300.SH"
    mapping: str = "balanced"                # balanced / conservative / aggressive / sigmoid
    factors: list[FactorConfig] = field(default_factory=lambda: [
        FactorConfig("rsrs",      1.0, True),
        FactorConfig("trend",     1.0, True),
        FactorConfig("vol",       1.0, True),
        FactorConfig("sentiment", 1.0, True),
        FactorConfig("breadth",   1.0, True),
    ])
    refresh: bool = False                    # 是否强制重拉数据（跳过缓存）
    name: str = "default"                    # 配置名（输出报告用）

    def enabled_factor_names(self) -> list[str]:
        return [f.name for f in self.factors if f.enabled]


# ============================================================
# 预设配置（已弃用老的5因子配置，推荐使用 macro2）
# ============================================================
PRESET_CORE_3 = EngineConfig(
    name="core3",
    mapping="balanced",
    factors=[
        FactorConfig("rsrs",  1.0, True),
        FactorConfig("trend", 1.0, True),
        FactorConfig("vol",   1.0, True),
        FactorConfig("sentiment", 1.0, False),  # 关闭
        FactorConfig("breadth",   1.0, False),  # 关闭
    ],
)

PRESET_FULL_5 = EngineConfig(
    name="full5",
    mapping="balanced",
    factors=[
        FactorConfig("rsrs",      1.0, True),
        FactorConfig("trend",     1.0, True),
        FactorConfig("vol",       1.0, True),
        FactorConfig("sentiment", 1.0, True),
        FactorConfig("breadth",   1.0, True),
    ],
)

# 新增：宏观2因子组合（经 IS/OOS 验证有效）
PRESET_MACRO2 = EngineConfig(
    name="macro2",
    mapping="2factor",  # 使用专为2因子设计的映射器
    factors=[
        FactorConfig("turnover_pct_252", 1.0, True),  # 成交额252日分位
        FactorConfig("us_cn_10y_spread", 1.0, True),  # 中美10Y利差
        FactorConfig("rsrs",      1.0, False),  # 关闭原有因子
        FactorConfig("trend",     1.0, False),
        FactorConfig("vol",       1.0, False),
        FactorConfig("sentiment", 1.0, False),
        FactorConfig("breadth",   1.0, False),
    ],
)

ALL_PRESETS: dict[str, EngineConfig] = {
    "core3":              PRESET_CORE_3,
    "full5":              PRESET_FULL_5,
    "full5_sigmoid":      PRESET_FULL_5_SIGMOID,
    "full5_conservative": PRESET_FULL_5_CONSERVATIVE,
    "full5_aggressive":   PRESET_FULL_5_AGGRESSIVE,
    "trend_heavy":        PRESET_TREND_HEAVY,
    "macro2":             PRESET_MACRO2,  # 新增宏观2因子组合
}


# ============================================================
# 主类
# ============================================================
class TimingEngine:
    """
    通用大盘择时引擎。

    关键属性：
        config      : EngineConfig
        df_signal   : 运行后的完整信号 DataFrame，含每个因子信号 + total_score + regime + position

    关键方法：
        run(start, end)              → DataFrame 完整信号
        get_position_map(start, end) → dict[trade_date → position]
        summarize()                  → 打印仓位分布 / 单因子触发率 / 分年度
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        # 默认使用宏观2因子组合（经 IS/OOS 验证有效）
        self.config = config or PRESET_MACRO2
        self.df_signal: Optional[pd.DataFrame] = None

        if self.config.mapping not in POSITION_MAPPERS:
            raise ValueError(
                f"unknown mapping '{self.config.mapping}', "
                f"choices={list(POSITION_MAPPERS.keys())}"
            )
        self._mapper = POSITION_MAPPERS[self.config.mapping]

    # ------------------------------------------------------------
    # 因子计算（按 enabled 列表动态跑）
    # ------------------------------------------------------------
    def _compute_factors(self, start: str, end: str) -> dict[str, pd.DataFrame]:
        tables: dict[str, pd.DataFrame] = {}

        # 指数日线（MA250/RSRS 需要前置 3 年）
        _start_int = int(start)
        pre_start = str(max(_start_int - 30000, 20180101))
        df_idx = tdata.load_index_daily(
            self.config.index_code, pre_start, end, refresh=self.config.refresh,
        )
        df_idx["trade_date"] = df_idx["trade_date"].astype(str)
        tables["_index"] = df_idx

        enabled = self.config.enabled_factor_names()

        if "rsrs" in enabled:
            tables["rsrs"] = compute_rsrs(df_idx)
        if "trend" in enabled:
            tables["trend"] = compute_trend(df_idx)
        if "vol" in enabled:
            tables["vol"] = compute_volatility(df_idx)

        if "sentiment" in enabled:
            try:
                df_limit = tdata.load_limit_stats(start, end, refresh=self.config.refresh)
                tables["sentiment"] = compute_sentiment_limit(df_limit)
            except Exception as e:
                print(f"⚠️  情绪因子失败（{e}），降级为 0")
                tables["sentiment"] = pd.DataFrame(columns=["trade_date", "senti_signal"])

        if "breadth" in enabled:
            try:
                df_br = tdata.load_breadth_from_tushare(start, end, refresh=self.config.refresh)
                tables["breadth"] = compute_breadth(df_br)
            except Exception as e:
                print(f"⚠️  宽度因子失败（{e}），降级为 0")
                tables["breadth"] = pd.DataFrame(columns=["trade_date", "breadth_signal"])

        # 新增：宏观因子计算
        macro_factors = ["turnover_pct_252", "us_cn_10y_spread"]
        if any(f in enabled for f in macro_factors):
            try:
                macro_tables = compute_macro(df_idx)
                for fname, df_macro in macro_tables.items():
                    if fname in enabled:
                        tables[fname] = df_macro
            except Exception as e:
                print(f"⚠️  宏观因子失败（{e}），降级为 0")
                for fname in macro_factors:
                    if fname in enabled:
                        tables[fname] = pd.DataFrame(columns=["trade_date", f"{fname}_signal"])

        return tables

    # ------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------
    def run(self, start: str = "20210101", end: str = "20260420",
            save_path: Optional[Path] = None) -> pd.DataFrame:
        """计算从 start 到 end 的完整择时信号"""
        tables = self._compute_factors(start, end)
        df_idx = tables["_index"]

        base = df_idx[["trade_date", "close"]].copy()

        # 因子名 → 输出信号列名
        signal_cols = {
            "rsrs":      "rsrs_signal",
            "trend":     "trend_signal",
            "vol":       "vol_signal",
            "sentiment": "senti_signal",
            "breadth":   "breadth_signal",
            "turnover_pct_252":   "turnover_pct_252_signal",  # 新增宏观因子信号列
            "us_cn_10y_spread":   "us_cn_10y_spread_signal",  # 新增宏观因子信号列
        }
        # 因子名 → 权重
        weights = {f.name: f.weight for f in self.config.factors if f.enabled}

        # 合并各因子明细
        for fname in self.config.enabled_factor_names():
            tbl = tables.get(fname)
            if tbl is None or len(tbl) == 0:
                continue
            base = base.merge(tbl, on="trade_date", how="left")

        # 缺失 → 0（降级）
        for fname, col in signal_cols.items():
            if fname not in weights:
                continue
            if col not in base.columns:
                base[col] = 0
            else:
                base[col] = base[col].fillna(0).astype(int)

        # 加权总分
        score = pd.Series(0.0, index=base.index)
        for fname, col in signal_cols.items():
            if fname not in weights:
                continue
            score = score + base[col].astype(float) * weights[fname]
        base["total_score"] = score

        # 置信度：|总分| / 总权重 ∈ [0, 1]，越靠近 1 越确定
        total_w = sum(weights.values()) if weights else 1.0
        base["confidence"] = (base["total_score"].abs() / total_w).clip(0, 1).round(3)

        # 映射仓位
        base[["regime", "position"]] = base["total_score"].apply(
            lambda s: pd.Series(self._mapper(s))
        )

        # 截取用户请求时间段
        out = base[(base["trade_date"] >= start) & (base["trade_date"] <= end)]
        out = out.reset_index(drop=True)

        self.df_signal = out

        if save_path is not None:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            out.to_csv(save_path, index=False)

        return out

    # ------------------------------------------------------------
    # 便捷接口
    # ------------------------------------------------------------
    def get_position_map(self, start: str = "20210101",
                         end: str = "20260420") -> dict[str, float]:
        """返回 {trade_date → position} dict（回测用）"""
        if self.df_signal is None:
            self.run(start, end)
        df = self.df_signal
        return dict(zip(df["trade_date"].astype(str), df["position"].astype(float)))

    def get_position_series(self, start: str = "20210101",
                            end: str = "20260420") -> pd.Series:
        """返回以 trade_date 为 index 的仓位 Series（绘图/分析用）"""
        if self.df_signal is None:
            self.run(start, end)
        df = self.df_signal
        return pd.Series(
            df["position"].astype(float).values,
            index=pd.to_datetime(df["trade_date"]),
            name=f"pos_{self.config.name}",
        )

    # ------------------------------------------------------------
    # 统计摘要
    # ------------------------------------------------------------
    def summarize(self) -> None:
        if self.df_signal is None:
            raise RuntimeError("请先调用 run(start, end)")
        df = self.df_signal
        print("\n" + "=" * 90)
        print(f"📊 TimingEngine[{self.config.name}]"
              f"  mapping={self.config.mapping}"
              f"  factors={self.config.enabled_factor_names()}")
        print(f"   {df['trade_date'].min()} ~ {df['trade_date'].max()}  ({len(df)} 交易日)")
        print("=" * 90)

        print("\n🔎 仓位分布：")
        vc = df["regime"].value_counts()
        order = ["BULL", "RISK_ON", "NEUTRAL", "RISK_OFF", "BEAR"]
        total = len(df)
        for st in order:
            cnt = int(vc.get(st, 0))
            print(f"   {st:<10} {cnt:>5} 天  ({cnt/total*100:5.1f}%)")
        print(f"   平均仓位 = {df['position'].mean()*100:5.1f}%")

        print("\n🔎 单因子触发率：")
        pairs = [
            ("rsrs",      "rsrs_signal",      "RSRS"),
            ("trend",     "trend_signal",     "趋势"),
            ("vol",       "vol_signal",       "波动率"),
            ("sentiment", "senti_signal",     "情绪"),
            ("breadth",   "breadth_signal",   "宽度"),
        ]
        for fname, col, label in pairs:
            if col not in df.columns:
                continue
            pos = (df[col] == 1).sum()
            neg = (df[col] == -1).sum()
            print(f"   {label:<8}  +1={pos:>4} ({pos/total*100:5.1f}%)"
                  f"   -1={neg:>4} ({neg/total*100:5.1f}%)")


# ============================================================
# CLI 调试
# ============================================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--preset", default="full5", choices=list(ALL_PRESETS))
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260420")
    args = ap.parse_args()

    cfg = ALL_PRESETS[args.preset]
    eng = TimingEngine(cfg)
    df = eng.run(args.start, args.end)
    eng.summarize()
    print("\n最近 10 日：")
    cols = ["trade_date", "total_score", "confidence", "regime", "position"]
    print(df[cols].tail(10).to_string(index=False))
