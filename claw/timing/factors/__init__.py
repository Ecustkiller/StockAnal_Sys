"""择时因子子模块"""
from claw.timing.factors.rsrs import compute_rsrs
from claw.timing.factors.trend import compute_trend
from claw.timing.factors.volatility import compute_volatility
from claw.timing.factors.sentiment_limit import compute_sentiment_limit
from claw.timing.factors.breadth import compute_breadth

# —— 新增：桌面原项目迁移过来的择时因子组 ——
from claw.timing.factors.rsrs_full import compute_rsrs_full
from claw.timing.factors.trend_strength import compute_trend_strength
from claw.timing.factors.microstructure import compute_microstructure

# —— 新增：宏观/资金流择时因子组 ——
from claw.timing.factors.macro import compute_macro

__all__ = [
    "compute_rsrs",
    "compute_trend",
    "compute_volatility",
    "compute_sentiment_limit",
    "compute_breadth",
    # 桌面原项目迁移
    "compute_rsrs_full",
    "compute_trend_strength",
    "compute_microstructure",
    # 宏观
    "compute_macro",
]
