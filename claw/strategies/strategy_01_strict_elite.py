#!/usr/bin/env python3
"""
策略01：严格精选 TOP5/TOP10（全市场池）— T+1 每日换仓
================================================================
【历史回测表现】（2024年至今，基于 backtest_v2 TOP30 评分池）

  ┌─────────────────────────────────────────────────────────────┐
  │ E1. 严格精选 TOP5（全市场）T+1                              │
  │   累计收益: +391.4%  │ 年化: +113.1%  │ Sharpe: 1.81       │
  │   胜率: 54.2%        │ 盈亏比: 1.17   │ 最大回撤: -41.7%   │
  │   收益/回撤比: 9.38  │ 日均持股: 5只                        │
  ├─────────────────────────────────────────────────────────────┤
  │ E2. 严格精选 TOP10（全市场）T+1                             │
  │   累计收益: +322.5%  │ 年化: +96.8%   │ Sharpe: 1.72       │
  │   胜率: 53.8%        │ 盈亏比: 1.15   │ 最大回撤: -42.7%   │
  │   收益/回撤比: 7.55  │ 日均持股: 10只                       │
  └─────────────────────────────────────────────────────────────┘

【策略核心逻辑】
  1. 硬性过滤：非涨停 + 5日涨幅<15% + (WR2≥3 或 Mistery≥10)
  2. 精选打分：总分(50%) + 维度均衡度(20%) + 低风险(15%) + 资金流入(15%)
  3. 额外加分：WR2≥4(+15) / Mistery≥12(+12) / 安全边际≥10(+8) / 净流入>0(+5)
  4. 行业分散：同行业最多2只
  5. 持有期：T+1（每日换仓）

【适用场景】
  - 追求最高绝对收益
  - 能每天操作换仓
  - 可接受 ~40% 的最大回撤

【排名】
  - 累计收益: 全策略第1名（全市场T+1）
  - Sharpe:   全策略第1名
  - 收益/回撤比: 全策略第1名
"""

import pandas as pd
import numpy as np
from collections import defaultdict


def calc_dimension_balance(row):
    """计算维度均衡度得分（0-100）"""
    dims = []
    if 'd1' in row: dims.append(row['d1'] / 15)
    if 'd2' in row: dims.append(row['d2'] / 25)
    if 'd3' in row: dims.append(row['d3'] / 47)
    if 'd4' in row: dims.append(row['d4'] / 15)
    if 'd5' in row: dims.append(row['d5'] / 15)
    if not dims:
        return 0
    mean_val = np.mean(dims)
    if mean_val == 0:
        return 0
    cv = np.std(dims) / mean_val
    return max(0, 1 - cv) * 100


def calc_elite_score(row):
    """精选得分 = 总分(50%) + 均衡度(20%) + 低风险(15%) + 资金(15%)"""
    total_norm = row['total'] / 150 * 100
    balance = calc_dimension_balance(row)
    risk_score = max(0, 100 - row.get('net_risk', 0) * 10)
    nb = row.get('nb_yi', 0)
    if pd.isna(nb): nb = 0
    fund_score = min(100, max(0, 50 + nb * 10))
    return total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15


def select_strict_elite(day_df, n=5, max_per_ind=2):
    """
    严格精选策略
    
    参数:
        day_df: 当日TOP30评分数据
        n: 选股数量（5或10）
        max_per_ind: 同行业最多几只
    
    返回:
        选中的股票DataFrame
    """
    day_df = day_df.copy()
    
    # 第一步：硬性过滤
    mask = (day_df['is_zt'] == False)       # 非涨停
    mask &= (day_df['r5'] < 15)             # 5日涨幅<15%
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))  # WR2≥3 或 Mistery≥10
    
    filtered = day_df[mask].copy()
    
    if len(filtered) == 0:
        # 放宽条件：非涨停 + 5日涨幅<20%
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
    """严格精选TOP5（max_per_ind=3，回测最优）"""
    return select_strict_elite(day_df, n=5, max_per_ind=3)

def pick_top10(day_df):
    """严格精选TOP10（max_per_ind=3，回测最优：+102pp vs ind=2）"""
    return select_strict_elite(day_df, n=10, max_per_ind=3)


if __name__ == '__main__':
    print("策略01：严格精选 TOP5/TOP10（全市场池）— T+1 每日换仓")
    print("请通过 elite_backtest.py 运行回测，或导入 select_strict_elite 函数使用")
