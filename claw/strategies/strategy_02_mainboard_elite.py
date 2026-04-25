#!/usr/bin/env python3
"""
策略02：主板严格精选 TOP10（仅00/60开头）— T+1 每日换仓
================================================================
【历史回测表现】（2024年至今，基于 backtest_v2 TOP30 评分池 → 主板过滤）

  ┌─────────────────────────────────────────────────────────────┐
  │ E2🏛️ 严格精选 TOP10（主板池）T+1                           │
  │   累计收益: +302.9%  │ 年化: +92.1%   │ Sharpe: 1.81       │
  │   胜率: 54.0%        │ 盈亏比: 1.14   │ 最大回撤: -28.4%   │
  │   收益/回撤比: 10.67 │ 日均持股: 10只                       │
  ├─────────────────────────────────────────────────────────────┤
  │ E1🏛️ 严格精选 TOP5（主板池）T+1                            │
  │   累计收益: +311.4%  │ 年化: +94.5%   │ Sharpe: 1.74       │
  │   胜率: 53.5%        │ 盈亏比: 1.16   │ 最大回撤: -32.8%   │
  │   收益/回撤比: 9.49  │ 日均持股: 5只                        │
  ├─────────────────────────────────────────────────────────────┤
  │ B2🏛️ 精选 TOP10（主板池+行业分散）T+1                      │
  │   累计收益: +183.0%  │ 年化: +60.2%   │ Sharpe: 1.52       │
  │   胜率: 52.8%        │ 盈亏比: 1.12   │ 最大回撤: -24.4%   │
  │   收益/回撤比: 7.51  │ 日均持股: 10只                       │
  └─────────────────────────────────────────────────────────────┘

【vs 全市场对比】
  严格精选TOP10: 全市场+322.5%(回撤-42.7%) → 主板+302.9%(回撤-28.4%)
  → 收益略降20%，但回撤降了14个点！收益/回撤比从7.55提升到10.67

【策略核心逻辑】
  1. 主板过滤：仅保留 600/601/603/605/000/001/002/003 开头的股票
  2. 硬性过滤：非涨停 + 5日涨幅<15% + (WR2≥3 或 Mistery≥10)
  3. 精选打分：总分(50%) + 维度均衡度(20%) + 低风险(15%) + 资金流入(15%)
  4. 额外加分：WR2≥4(+15) / Mistery≥12(+12) / 安全边际≥10(+8) / 净流入>0(+5)
  5. 行业分散：同行业最多2只
  6. 持有期：T+1（每日换仓）

【适用场景】
  - 追求最优风险调整收益（Sharpe最高、收益/回撤比最高）
  - 偏好主板票的稳定性，不想碰创业板/科创板
  - 能每天操作换仓
  - 希望回撤控制在30%以内

【排名】
  - 收益/回撤比: 主板策略第1名（10.67）
  - Sharpe:      主板策略第1名（1.81）
  - 最低回撤:    B2🏛️行业分散 仅-24.4%
"""

import pandas as pd
import numpy as np
from collections import defaultdict

try:
    from claw.strategies.strategy_01_strict_elite import calc_elite_score
except ImportError:  # pragma: no cover
    from strategy_01_strict_elite import calc_elite_score


MAINBOARD_PREFIXES = ['600', '601', '603', '605', '000', '001', '002', '003']


def select_mainboard_strict_elite(day_df, n=10, max_per_ind=2):
    """
    主板严格精选策略
    
    参数:
        day_df: 当日TOP30评分数据
        n: 选股数量（默认10）
        max_per_ind: 同行业最多几只
    
    返回:
        选中的股票DataFrame
    """
    # 第零步：主板过滤
    main_mask = day_df['code'].str[:3].isin(MAINBOARD_PREFIXES)
    day_df = day_df[main_mask].copy()
    
    if len(day_df) == 0:
        return pd.DataFrame()
    
    # 第一步：硬性过滤
    mask = (day_df['is_zt'] == False)
    mask &= (day_df['r5'] < 15)
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
    
    filtered = day_df[mask].copy()
    
    if len(filtered) == 0:
        mask2 = (day_df['is_zt'] == False) & (day_df['r5'] < 20)
        filtered = day_df[mask2].copy()
    
    if len(filtered) == 0:
        return pd.DataFrame()
    
    # 第二步：计算精选得分
    filtered['elite_score'] = filtered.apply(calc_elite_score, axis=1)
    
    # 第三步：额外加分
    filtered['elite_score_adj'] = filtered['elite_score']
    filtered.loc[filtered['wr2'] >= 4, 'elite_score_adj'] += 15
    filtered.loc[filtered['mistery'] >= 12, 'elite_score_adj'] += 12
    filtered.loc[filtered['d4'] >= 10, 'elite_score_adj'] += 8
    filtered.loc[filtered['nb_yi'] > 0, 'elite_score_adj'] += 5
    
    filtered = filtered.sort_values('elite_score_adj', ascending=False)
    
    # 第四步：行业分散选股
    selected = []
    ind_count = defaultdict(int)
    
    for _, row in filtered.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= max_per_ind:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= n:
            break
    
    return pd.DataFrame(selected)


# ============================================================
# 快捷调用接口
# ============================================================
def pick_top5(day_df):
    """主板严格精选TOP5（max_per_ind=999，回测最优：主板票池小不适合强制分散）"""
    return select_mainboard_strict_elite(day_df, n=5, max_per_ind=999)

def pick_top10(day_df):
    """主板严格精选TOP10（max_per_ind=999，回测最优：+75pp vs ind=2）"""
    return select_mainboard_strict_elite(day_df, n=10, max_per_ind=999)


if __name__ == '__main__':
    print("策略02：主板严格精选 TOP10（仅00/60开头）— T+1 每日换仓")
    print("请通过 elite_backtest.py 运行回测，或导入 select_mainboard_strict_elite 函数使用")
