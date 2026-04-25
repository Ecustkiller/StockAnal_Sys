"""
Claw 策略包
============
所有策略统一从此处获取：

    from claw.strategies import get_strategy, list_strategies, BaseStrategy

    strat = get_strategy("strict_elite_top5")
    picks = strat.select(day_df, n=5)

原始策略函数仍可直接导入：

    from claw.strategies.strategy_03_optimized import select_optimized_elite
"""
from claw.strategies.base import (
    BaseStrategy, register_strategy, get_strategy, list_strategies,
)

# 注册每个策略为 BaseStrategy 子类
from claw.strategies.strategy_01_strict_elite import select_strict_elite
from claw.strategies.strategy_02_mainboard_elite import select_mainboard_strict_elite
from claw.strategies.strategy_03_optimized import select_optimized_elite
from claw.strategies.strategy_04_risk_managed import (
    select_optimized_elite as select_risk_managed_elite,
)  # 实际使用 strategy_03 的选股，叠加仓位管理


@register_strategy
class StrictEliteStrategy(BaseStrategy):
    """S01: 严格精选（全市场 T+1）"""
    name = "strict_elite"
    description = "严格精选 TOP5/10（全市场）T+1，累计+391%"
    default_n = 5

    def select(self, day_df, n=None, max_per_ind=2, **kw):
        return select_strict_elite(day_df, n=n or self.default_n,
                                    max_per_ind=max_per_ind)


@register_strategy
class MainboardEliteStrategy(BaseStrategy):
    """S02: 主板严格精选"""
    name = "mainboard_elite"
    description = "主板精选 TOP10，Sharpe 第一名"
    default_n = 10

    def select(self, day_df, n=None, max_per_ind=2, **kw):
        return select_mainboard_strict_elite(day_df, n=n or self.default_n,
                                              max_per_ind=max_per_ind)


@register_strategy
class AiTraderFactorStrategy(BaseStrategy):
    """S03: aiTrader 7因子等权"""
    name = "aitrader_factor"
    description = "aiTrader 7因子等权（5年+202%），截面排名打分"
    default_n = 10

    def select(self, day_df, n=None, max_per_ind=2, **kw):
        return select_optimized_elite(day_df, n=n or self.default_n,
                                       max_per_ind=max_per_ind)


__all__ = [
    "BaseStrategy", "register_strategy", "get_strategy", "list_strategies",
    "StrictEliteStrategy", "MainboardEliteStrategy", "AiTraderFactorStrategy",
    "select_strict_elite", "select_mainboard_strict_elite",
    "select_optimized_elite",
]
