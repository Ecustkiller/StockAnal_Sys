"""
claw.timing — 大盘择时多因子组合（宏观仓位把控）
=================================================

分层架构：
    Layer 0（宏观，月频）：超额流动性 / ERP  —— 预留，后续实现
    Layer 1（趋势，日频）：RSRS / MA250趋势 / 波动率状态
    Layer 2（情绪，日频）：涨停赚钱效应 / 市场宽度

v1 MVP：Tier A 五因子等权投票 → 离散 4 档仓位 {0, 0.3, 0.7, 1.0}

入口：
    from claw.timing.composer import compute_market_timing
    df = compute_market_timing(start="20210101", end="20260420")
    # df 列：trade_date, rsrs, trend, vol, sentiment, breadth,
    #       total_score, state, position
"""
# 故意不在 __init__ 里 import composer，避免 python -m claw.timing.composer 时的 RuntimeWarning
__all__ = []
