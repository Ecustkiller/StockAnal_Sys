"""
Claw v4.0 实盘漏洞修复模块集
=============================
基于 4/10~4/20 真实实盘数据复盘，针对以下 5 个漏洞建立优化：

1. sector_oneday_detector  — 板块一日游识别器
   修复: 4/16 医药一日游导致的-3.64% 亏损（信立泰跌停-16.5%）

2. zt_continuation_risk    — 连续涨停风险衰减器
   修复: 美能/康盛/华远连续涨停第3日跌停-10%

3. early_seal_factor        — 早盘封板时间因子
   修复: 午后封板票(荣科-5.2%)远弱于早盘封板票(可川+21%)

4. top_score_reversal       — 高分反向修正器
   修复: 99分世运-1.2% vs 90分可川+21% 的倒挂

5. sector_next_day_verify   — 板块次日封板率验证
   修复: 首日爆发板块不直接升级主线（医药16→1）

使用方式：
    from optimizations import apply_v4_optimizations
    df_scored_v4 = apply_v4_optimizations(df_scored_v3, market_context)

版本：v4.0
日期：2026-04-21
"""

from .sector_oneday_detector import SectorOnedayDetector
from .zt_continuation_risk import ZtContinuationRisk
from .early_seal_factor import EarlySealFactor
from .top_score_reversal import TopScoreReversal
from .sector_next_day_verify import SectorNextDayVerify

__version__ = '4.0.0'
__all__ = [
    'SectorOnedayDetector',
    'ZtContinuationRisk',
    'EarlySealFactor',
    'TopScoreReversal',
    'SectorNextDayVerify',
    'apply_v4_optimizations',
]


def apply_v4_optimizations(df_scored, market_context):
    """
    一键应用所有 v4 优化到 v3 评分结果

    参数:
        df_scored: v3 评分后的 DataFrame，必须含列：
            ts_code, name, total, industry, is_zt, seal_time, consecutive_zt
        market_context: dict，含:
            - prev_sectors_zt: 前一日各板块涨停数 {sector: count}
            - curr_sectors_zt: 当日各板块涨停数 {sector: count}
            - prev_zt_next_perf: 前日涨停在今日的表现 {ts_code: pct_chg}
            - market_seal_rate: 今日封板率
            - profit_ratio: 今日赚钱效应

    返回:
        DataFrame，新增列：
            v4_score        — 优化后的总分
            oneday_penalty  — 一日游扣分
            zt_risk_penalty — 连涨风险扣分
            early_seal_bonus— 早盘封板加分
            reversal_adj    — 反向修正
            final_rank      — 最终排名
    """
    df = df_scored.copy()

    detector = SectorOnedayDetector()
    zt_risk = ZtContinuationRisk()
    seal = EarlySealFactor()
    reversal = TopScoreReversal()

    # 1. 板块一日游识别 → 扣分
    df['oneday_penalty'] = df.apply(
        lambda r: detector.penalty(
            sector=r.get('industry', ''),
            prev_zt=market_context.get('prev_sectors_zt', {}),
            curr_zt=market_context.get('curr_sectors_zt', {}),
            prev_zt_perf=market_context.get('prev_zt_next_perf', {}).get(r['ts_code'])
        ), axis=1
    )

    # 2. 连续涨停风险
    df['zt_risk_penalty'] = df.apply(
        lambda r: zt_risk.penalty(
            consecutive_zt=r.get('consecutive_zt', 0),
            is_zt=r.get('is_zt', False)
        ), axis=1
    )

    # 3. 早盘封板加分
    df['early_seal_bonus'] = df.apply(
        lambda r: seal.bonus(
            seal_time=r.get('seal_time'),
            is_zt=r.get('is_zt', False)
        ), axis=1
    )

    # 4. 反向修正（对 TOP 超高分）
    df['reversal_adj'] = reversal.adjust(df['total'])

    # 最终得分
    df['v4_score'] = (df['total']
                     - df['oneday_penalty']
                     - df['zt_risk_penalty']
                     + df['early_seal_bonus']
                     + df['reversal_adj'])

    df['final_rank'] = df['v4_score'].rank(ascending=False, method='dense').astype(int)

    return df.sort_values('v4_score', ascending=False).reset_index(drop=True)
