"""
策略基类 BaseStrategy
======================
所有 strategy_XX 都应实现 BaseStrategy 接口。
目的：统一 backtest 框架调用入口。

使用模式：
    class MyStrategy(BaseStrategy):
        name = "my_strategy"

        def select(self, day_df, n=5, **kw):
            ...
            return picked_df

然后：
    from claw.strategies import get_strategy
    strat = get_strategy("strict_elite")
    picks = strat.select(day_df, n=5)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

import pandas as pd


class BaseStrategy(ABC):
    """策略统一接口"""

    name: str = "base"
    description: str = ""
    default_n: int = 5
    default_max_per_industry: int = 2

    @abstractmethod
    def select(self, day_df: pd.DataFrame, n: Optional[int] = None,
               **kwargs) -> pd.DataFrame:
        """
        当日选股主入口。

        参数:
            day_df: 当日候选股票 DataFrame（含 total 等评分列）
            n: 选股数量
            **kwargs: 策略特定参数（如 max_per_ind 等）

        返回:
            选中的股票 DataFrame（按选股优先级排序）
        """
        ...

    def pick_top5(self, day_df: pd.DataFrame, **kw) -> pd.DataFrame:
        return self.select(day_df, n=5, **kw)

    def pick_top10(self, day_df: pd.DataFrame, **kw) -> pd.DataFrame:
        return self.select(day_df, n=10, **kw)

    def __repr__(self) -> str:
        return f"<Strategy {self.name}>"


# ============================================================
# 策略注册表
# ============================================================
_registry: Dict[str, Type[BaseStrategy]] = {}


def register_strategy(cls: Type[BaseStrategy]) -> Type[BaseStrategy]:
    """装饰器：注册策略类"""
    _registry[cls.name] = cls
    return cls


def get_strategy(name: str) -> BaseStrategy:
    """按名称获取策略实例"""
    if name not in _registry:
        available = ", ".join(_registry.keys()) or "(无)"
        raise KeyError(f"未注册的策略: {name}。可用: {available}")
    return _registry[name]()


def list_strategies() -> Dict[str, Type[BaseStrategy]]:
    return dict(_registry)
