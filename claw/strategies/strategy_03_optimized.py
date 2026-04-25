#!/usr/bin/env python3
"""
策略03：aiTrader因子增强精选 TOP10（全市场池）— T+1 每日换仓
================================================================
【来源】
  基于 aiTrader v3.7 因子库全面测试（5年数据 2021-2026）
  从30个aiTrader因子中筛选出7个有效因子（|IR|≥0.05，与现有因子低相关）
  数据区间: 2021.01.04 ~ 2026.04.10 (1275个交易日)

【核心改进 vs 旧策略03】
  1. 打分因子全面升级：用7个aiTrader因子替代原5因子
     - close_position（尾盘强度反向）: IR=-0.134, 与现有因子相关<0.13
     - upper_shadow（上影线）: IR=+0.121, 与现有因子相关<0.14
     - ret_skew（收益偏度反向）: IR=-0.109, 与现有因子相关<0.16
     - willr_14（威廉指标反向）: IR=-0.091, 超卖信号
     - mv_momentum（市值动量反向）: IR=-0.082, 近期缩水反弹
     - breakout_20（突破位置反向）: IR=-0.081, 低位反转
     - dn_buffer（下跌缓冲反向）: IR=-0.076, 跌幅大反弹
  2. 过滤条件不变：非涨停 + r5<10% + (WR2≥3|Mistery≥10) + net_risk≤2
  3. 因子逻辑统一：全部指向"超跌反弹"——选择当日收盘弱、上影线长、
     近期偏度负、超卖、市值缩水、处于低位、跌幅大的票

【5年因子IC/IR测试结果】
  因子               全量IR    牛市IR    熊市IR    牛熊差    分类
  close_position    -0.134    -0.121    -0.154    +0.034   ⚖️全天候
  upper_shadow      +0.121    +0.103    +0.162    -0.059   ⚖️全天候
  ret_skew          -0.109    -0.061    -0.141    +0.080   ⚖️全天候
  willr_14          -0.091    -0.132    -0.114    -0.018   ⚖️全天候
  mv_momentum       -0.082    -0.089    -0.104    +0.015   ⚖️全天候
  breakout_20       -0.081    -0.122    -0.103    -0.019   ⚖️全天候
  dn_buffer         -0.076    -0.158    -0.053    -0.105   📉熊市

  → 7个因子中6个是全天候因子，牛熊都有效！

【历史回测表现】（2021-2026，5年1275天，基于 backtest_v2 TOP30 评分池）

  ┌─────────────────────────────────────────────────────────────┐
  │ 🏆 aiTrader因子增强 TOP10（全市场）T+1                      │
  │   累计收益: +202.2%  │ 年化: +26.1%   │ Sharpe: 0.76       │
  │   最大回撤: -56.3%   │ Calmar: 0.46                         │
  ├─────────────────────────────────────────────────────────────┤
  │ vs 旧策略03 (5年):                                          │
  │   累计: +191.4% → +202.2% (+5.6%)                          │
  │   Sharpe: 0.743 → 0.760 (+2.3%)                            │
  │   回撤: -58.4% → -56.3% (降2.1%)                           │
  │   Calmar: 0.431 → 0.464 (+7.7%)                            │
  ├─────────────────────────────────────────────────────────────┤
  │ 分年表现:                                                    │
  │   2021: +123.9% (旧+138.7%)  牛市旧因子略强                 │
  │   2022:  -42.9% (旧 -43.6%)  熊市新因子略好                 │
  │   2023:   -2.5% (旧  -7.2%)  弱市新因子明显好(+4.8%)        │
  │   2024:  +14.3% (旧  +9.5%)  震荡市新因子好(+4.9%)          │
  │   2025:  +46.4% (旧 +53.8%)  牛市旧因子略强                 │
  │   2026:  +44.8% (旧 +38.6%)  新因子好(+6.2%)               │
  └─────────────────────────────────────────────────────────────┘

【策略核心逻辑】
  1. 硬性过滤：非涨停 + 5日涨幅<10% + (WR2≥3 或 Mistery≥10) + 净风险≤2
  2. 等权7因子打分（截面排名归一化后等权）：
     close_position反向(1/7) + upper_shadow(1/7) + ret_skew反向(1/7) +
     willr_14反向(1/7) + mv_momentum反向(1/7) + breakout_20反向(1/7) +
     dn_buffer反向(1/7)
  3. 行业分散：同行业最多2只
  4. 持有期：T+1（每日换仓）

【适用场景】
  - 追求5年维度最优风险调整收益
  - 能每天操作换仓
  - 可接受 ~56% 的最大回撤（5年跨越牛熊）
  - 需要计算OHLCV衍生因子（需要daily_snapshot历史数据）
"""

import pandas as pd
import numpy as np
from collections import defaultdict


def calc_aitrader_factors(stock_hist_df):
    """
    计算aiTrader因子（需要该股票近60日的OHLCV历史数据）
    
    参数:
        stock_hist_df: 该股票的历史日线数据，需包含列:
            open, high, low, close, vol, total_mv
            至少需要20行数据
    
    返回:
        dict: 7个因子值
    """
    c = stock_hist_df['close'].values.astype(float)
    o = stock_hist_df['open'].values.astype(float)
    h = stock_hist_df['high'].values.astype(float)
    l = stock_hist_df['low'].values.astype(float)
    v = stock_hist_df['vol'].values.astype(float)
    mv = stock_hist_df['total_mv'].values.astype(float)
    n = len(c)
    
    results = {}
    
    # 1. 尾盘强度（close在当日OHLC中的位置，越低越好）
    results['close_position'] = (c[-1] - l[-1]) / (h[-1] - l[-1]) if h[-1] > l[-1] else 0.5
    
    # 2. 上影线比例（越长越好）
    results['upper_shadow'] = (h[-1] - max(o[-1], c[-1])) / (h[-1] - l[-1]) if h[-1] > l[-1] else 0
    
    # 3. 收益偏度（20日，越低越好）
    if n >= 21:
        rets20 = np.diff(c[-21:]) / np.maximum(c[-21:-1], 1e-8)
        results['ret_skew'] = float(pd.Series(rets20).skew())
    else:
        results['ret_skew'] = np.nan
    
    # 4. Williams %R（14日，越低越好=超卖）
    if n >= 14:
        high14 = np.max(h[-14:])
        low14 = np.min(l[-14:])
        results['willr_14'] = (high14 - c[-1]) / (high14 - low14) * (-100) if high14 > low14 else -50
    else:
        results['willr_14'] = np.nan
    
    # 5. 市值动量（20日市值变化率，越低越好）
    if n >= 20 and mv[-20] > 0:
        results['mv_momentum'] = mv[-1] / mv[-20] - 1
    else:
        results['mv_momentum'] = np.nan
    
    # 6. 突破位置（20日，越低越好=处于低位）
    if n >= 20:
        high20 = np.max(h[-20:])
        low20 = np.min(l[-20:])
        rng = high20 - low20
        results['breakout_20'] = (c[-1] - low20) / rng if rng > 0 else np.nan
    else:
        results['breakout_20'] = np.nan
    
    # 7. 下跌缓冲（近10日最高价到当前的跌幅，越低越好）
    if n >= 10:
        max_price_10 = np.max(h[-10:])
        results['dn_buffer'] = (c[-1] - max_price_10) / max_price_10 if max_price_10 > 0 else np.nan
    else:
        results['dn_buffer'] = np.nan
    
    return results


def calc_optimized_score(row):
    """
    aiTrader因子增强打分公式（等权7因子，截面排名归一化）
    
    注意：此函数用于单行打分，实际使用时建议用 rank_and_score() 进行截面排名
    这里提供一个简化版本用于兼容旧接口
    """
    # 简化版：直接用原始值打分（实际策略中应使用截面排名）
    score = 0
    
    # close_position 反向：越低越好
    cp = row.get('close_position', 0.5)
    score += (1 - cp) * 100 / 7
    
    # upper_shadow 正向：越高越好
    us = row.get('upper_shadow', 0)
    score += us * 100 / 7
    
    # ret_skew 反向：越低越好（范围约-3~3）
    sk = row.get('ret_skew', 0)
    if pd.isna(sk): sk = 0
    score += max(0, min(100, (3 - sk) / 6 * 100)) / 7
    
    # willr_14 反向：越低越好（范围-100~0）
    wr = row.get('willr_14', -50)
    if pd.isna(wr): wr = -50
    score += max(0, min(100, (-wr) / 100 * 100)) / 7
    
    # mv_momentum 反向：越低越好（范围约-0.5~0.5）
    mm = row.get('mv_momentum', 0)
    if pd.isna(mm): mm = 0
    score += max(0, min(100, (0.5 - mm) / 1 * 100)) / 7
    
    # breakout_20 反向：越低越好（范围0~1）
    bp = row.get('breakout_20', 0.5)
    if pd.isna(bp): bp = 0.5
    score += (1 - bp) * 100 / 7
    
    # dn_buffer 反向：越低越好（范围约-0.3~0）
    db = row.get('dn_buffer', -0.1)
    if pd.isna(db): db = -0.1
    score += max(0, min(100, (-db) / 0.3 * 100)) / 7
    
    return score


def rank_and_score(day_df):
    """
    截面排名打分（推荐方式）
    对每个因子在当日截面内排名归一化，然后等权求和
    """
    factors = {
        'close_position': -1,   # 反向
        'upper_shadow': +1,     # 正向
        'ret_skew': -1,         # 反向
        'willr_14': -1,         # 反向
        'mv_momentum': -1,      # 反向
        'breakout_20': -1,      # 反向
        'dn_buffer': -1,        # 反向
    }
    
    scores = np.zeros(len(day_df))
    n_valid = 0
    
    for fname, direction in factors.items():
        if fname not in day_df.columns:
            continue
        vals = day_df[fname].values.astype(float) * direction
        valid = ~np.isnan(vals)
        if valid.sum() < 3:
            continue
        ranked = np.full(len(vals), np.nan)
        ranked[valid] = pd.Series(vals[valid]).rank(pct=True).values
        scores += np.nan_to_num(ranked, nan=0.5)
        n_valid += 1
    
    if n_valid > 0:
        scores /= n_valid
    
    return scores


def select_optimized_elite(day_df, n=10, max_per_ind=2):
    """
    aiTrader因子增强精选策略
    
    参数:
        day_df: 当日TOP30评分数据（需包含aiTrader因子列）
        n: 选股数量（默认10）
        max_per_ind: 同行业最多几只
    
    返回:
        选中的股票DataFrame
    """
    day_df = day_df.copy()
    
    # 第一步：硬性过滤
    mask = (day_df['is_zt'] == False)
    mask &= (day_df['r5'] < 10)
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
    mask &= (day_df['net_risk'] <= 2)
    
    filtered = day_df[mask].copy()
    
    if len(filtered) == 0:
        mask2 = (day_df['is_zt'] == False)
        mask2 &= (day_df['r5'] < 15)
        mask2 &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
        filtered = day_df[mask2].copy()
    
    if len(filtered) == 0:
        mask3 = (day_df['is_zt'] == False) & (day_df['r5'] < 20)
        filtered = day_df[mask3].copy()
    
    if len(filtered) == 0:
        return pd.DataFrame()
    
    # 第二步：截面排名打分
    filtered['elite_score'] = rank_and_score(filtered)
    filtered = filtered.sort_values('elite_score', ascending=False)
    
    # 第三步：行业分散选股
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
    """aiTrader因子增强精选TOP5（max_per_ind=3，回测最优）"""
    return select_optimized_elite(day_df, n=5, max_per_ind=3)

def pick_top10(day_df):
    """aiTrader因子增强精选TOP10（max_per_ind=3，回测最优：+59pp收益、回撛降5.5pp）"""
    return select_optimized_elite(day_df, n=10, max_per_ind=3)


if __name__ == '__main__':
    print("策略03：aiTrader因子增强精选 TOP10（全市场池）— T+1 每日换仓")
    print("7因子等权: close_position↓ + upper_shadow↑ + ret_skew↓ + willr_14↓")
    print("           + mv_momentum↓ + breakout_20↓ + dn_buffer↓")
    print("过滤：非涨停 + r5<10% + (WR2≥3|Mistery≥10) + net_risk≤2")
    print("请通过 elite_backtest.py 运行回测，或导入 select_optimized_elite 函数使用")
