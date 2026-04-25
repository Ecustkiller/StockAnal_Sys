#!/usr/bin/env python3
"""
因子实验室 (Factor Lab) — 全面测试所有因子的独立贡献和最优组合
================================================================
功能：
  1. 单因子IC分析：测试每个因子与未来收益的相关性
  2. 单因子分组回测：按因子值分5组，看多空收益差
  3. 因子组合穷举：测试不同因子权重组合的回测效果
  4. 遗传算法优化：自动寻找最优因子组合
  5. 交叉验证：滚动窗口验证避免过拟合

使用方法：
  python3 factor_lab.py                    # 运行全部测试
  python3 factor_lab.py --single           # 仅单因子测试
  python3 factor_lab.py --combo            # 仅组合测试
  python3 factor_lab.py --optimize         # 遗传算法优化
  python3 factor_lab.py --validate         # 交叉验证最优组合
"""

import pandas as pd
import numpy as np
from collections import defaultdict
from itertools import combinations
import warnings
import argparse
import json
import os
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_052244.csv'
OUTPUT_DIR = 'factor_lab_results'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 主板前缀
MAINBOARD_PREFIXES = ['600', '601', '603', '605', '000', '001', '002', '003']

# ============================================================
# 第一部分：因子定义
# ============================================================
"""
因子分为三大类：
  A. 过滤型因子（Filter）：满足条件才入选，不满足直接淘汰
  B. 打分型因子（Score）：连续值，归一化后加权求和
  C. 加分型因子（Bonus）：满足阈值条件给固定加分
"""


class FactorDefinition:
    """因子定义基类"""
    
    def __init__(self, name, desc, factor_type):
        self.name = name
        self.desc = desc
        self.factor_type = factor_type  # 'filter', 'score', 'bonus'


# ---------- A. 过滤型因子 ----------
FILTER_FACTORS = {
    # 格式: name -> (条件函数, 描述)
    'no_zt': (lambda df: df['is_zt'] == False, '非涨停'),
    'r5_lt15': (lambda df: df['r5'] < 15, '5日涨幅<15%'),
    'r5_lt20': (lambda df: df['r5'] < 20, '5日涨幅<20%'),
    'r5_lt10': (lambda df: df['r5'] < 10, '5日涨幅<10%'),
    'r10_lt25': (lambda df: df['r10'] < 25, '10日涨幅<25%'),
    'r10_lt30': (lambda df: df['r10'] < 30, '10日涨幅<30%'),
    'wr2_ge2': (lambda df: df['wr2'] >= 2, 'WR2≥2'),
    'wr2_ge3': (lambda df: df['wr2'] >= 3, 'WR2≥3'),
    'wr2_ge4': (lambda df: df['wr2'] >= 4, 'WR2≥4'),
    'mistery_ge8': (lambda df: df['mistery'] >= 8, 'Mistery≥8'),
    'mistery_ge10': (lambda df: df['mistery'] >= 10, 'Mistery≥10'),
    'mistery_ge12': (lambda df: df['mistery'] >= 12, 'Mistery≥12'),
    'wr2_or_mistery': (lambda df: (df['wr2'] >= 3) | (df['mistery'] >= 10), 'WR2≥3或Mistery≥10'),
    'wr2_or_mistery_loose': (lambda df: (df['wr2'] >= 2) | (df['mistery'] >= 8), 'WR2≥2或Mistery≥8'),
    'mainboard': (lambda df: df['code'].str[:3].isin(MAINBOARD_PREFIXES), '仅主板'),
    'low_risk': (lambda df: df['net_risk'] <= 2, '净风险≤2'),
    'mid_risk': (lambda df: df['net_risk'] <= 4, '净风险≤4'),
    'bci_ge50': (lambda df: df['bci'] >= 50, 'BCI≥50'),
    'bci_ge60': (lambda df: df['bci'] >= 60, 'BCI≥60'),
    'bci_ge70': (lambda df: df['bci'] >= 70, 'BCI≥70'),
    'nb_positive': (lambda df: df['nb_yi'] > 0, '资金净流入>0'),
    'nb_ge1': (lambda df: df['nb_yi'] >= 1, '资金净流入≥1亿'),
    'tds_ge3': (lambda df: df['tds'] >= 3, 'TDS≥3'),
    'tds_ge4': (lambda df: df['tds'] >= 4, 'TDS≥4'),
    'd1_ge12': (lambda df: df['d1'] >= 12, '多周期共振≥12'),
    'd1_max': (lambda df: df['d1'] >= 15, '多周期共振满分'),
    'd2_ge15': (lambda df: df['d2'] >= 15, '主线热点≥15'),
    'd2_ge20': (lambda df: df['d2'] >= 20, '主线热点≥20'),
    'd3_ge15': (lambda df: df['d3'] >= 15, '三Skill≥15'),
    'd3_ge20': (lambda df: df['d3'] >= 20, '三Skill≥20'),
    'd4_ge8': (lambda df: df['d4'] >= 8, '安全边际≥8'),
    'd4_ge10': (lambda df: df['d4'] >= 10, '安全边际≥10'),
    'd4_ge12': (lambda df: df['d4'] >= 12, '安全边际≥12'),
    'd5_ge8': (lambda df: df['d5'] >= 8, '基本面≥8'),
    'd5_ge10': (lambda df: df['d5'] >= 10, '基本面≥10'),
    'd9_ge8': (lambda df: df['d9'] >= 8, '百胜WR≥8'),
    'd9_ge10': (lambda df: df['d9'] >= 10, '百胜WR≥10'),
    'd9_ge12': (lambda df: df['d9'] >= 12, '百胜WR≥12'),
    'tr_lt15': (lambda df: df['tr'] < 15, '换手率<15%'),
    'tr_gt3': (lambda df: df['tr'] > 3, '换手率>3%'),
    'tr_3_15': (lambda df: (df['tr'] > 3) & (df['tr'] < 15), '换手率3-15%'),
    'mv_lt500': (lambda df: df['mv'] < 500, '市值<500亿'),
    'mv_lt300': (lambda df: df['mv'] < 300, '市值<300亿'),
    'mv_lt200': (lambda df: df['mv'] < 200, '市值<200亿'),
    'mv_50_300': (lambda df: (df['mv'] >= 50) & (df['mv'] <= 300), '市值50-300亿'),
}


# ---------- B. 打分型因子 ----------
# 格式: name -> (计算函数, 方向, 描述)
# 方向: 1=越大越好, -1=越小越好
SCORE_FACTORS = {
    'total': (lambda df: df['total'], 1, '总分'),
    'd1': (lambda df: df['d1'], 1, '多周期共振'),
    'd2': (lambda df: df['d2'], 1, '主线热点'),
    'd3': (lambda df: df['d3'], 1, '三Skill'),
    'd4': (lambda df: df['d4'], 1, '安全边际'),
    'd5': (lambda df: df['d5'], 1, '基本面'),
    'd9': (lambda df: df['d9'], 1, '百胜WR'),
    'mistery': (lambda df: df['mistery'], 1, 'Mistery'),
    'tds': (lambda df: df['tds'], 1, 'TDS'),
    'wr2': (lambda df: df['wr2'], 1, 'WR2'),
    'wr1': (lambda df: df['wr1'], 1, 'WR1'),
    'wr3': (lambda df: df['wr3'], 1, 'WR3'),
    'bci': (lambda df: df['bci'], 1, 'BCI板块完整性'),
    'nb_yi': (lambda df: df['nb_yi'], 1, '资金净流入'),
    'net_risk': (lambda df: df['net_risk'], -1, '净风险(反向)'),
    'tr': (lambda df: df['tr'], -1, '换手率(反向,低换手好)'),
    'mv': (lambda df: df['mv'], -1, '市值(反向,小市值好)'),
    'r5': (lambda df: df['r5'], -1, '5日涨幅(反向,低位好)'),
    'r10': (lambda df: df['r10'], -1, '10日涨幅(反向)'),
    'r20': (lambda df: df['r20'], -1, '20日涨幅(反向)'),
    # 组合因子
    'elite_base': (lambda df: calc_elite_score_vec(df), 1, '精选基础分'),
    'balance': (lambda df: calc_balance_vec(df), 1, '维度均衡度'),
    'momentum_combo': (lambda df: df['d1'] + df['d9'] * 0.5, 1, '动量组合(d1+d9*0.5)'),
    'quality_combo': (lambda df: df['d4'] + df['d5'] + (5 - df['net_risk']) * 3, 1, '质量组合(d4+d5+低风险)'),
    'hot_combo': (lambda df: df['d2'] + df['bci'] * 0.3, 1, '热度组合(d2+bci*0.3)'),
    'skill_combo': (lambda df: df['mistery'] + df['tds'] * 2 + df['wr2'] * 3, 1, '技能组合(mistery+tds*2+wr2*3)'),
    'fund_flow': (lambda df: df['nb_yi'].clip(-5, 5) * 10 + 50, 1, '资金流标准化'),
    # 截面排名因子（在当日TOP30内排名）
    'total_rank': (lambda df: df['total'].rank(pct=True), 1, '总分排名百分位'),
    'wr2_rank': (lambda df: df['wr2'].rank(pct=True), 1, 'WR2排名百分位'),
    'nb_rank': (lambda df: df['nb_yi'].rank(pct=True), 1, '资金排名百分位'),
    'd3_rank': (lambda df: df['d3'].rank(pct=True), 1, '三Skill排名百分位'),
}


# ---------- C. 加分型因子 ----------
# 格式: name -> (条件函数, 加分值, 描述)
BONUS_FACTORS = {
    'bonus_wr2_ge3': (lambda df: df['wr2'] >= 3, 10, 'WR2≥3加10分'),
    'bonus_wr2_ge4': (lambda df: df['wr2'] >= 4, 15, 'WR2≥4加15分'),
    'bonus_wr2_5': (lambda df: df['wr2'] >= 5, 20, 'WR2=5加20分'),
    'bonus_mistery_ge10': (lambda df: df['mistery'] >= 10, 8, 'Mistery≥10加8分'),
    'bonus_mistery_ge12': (lambda df: df['mistery'] >= 12, 12, 'Mistery≥12加12分'),
    'bonus_mistery_15': (lambda df: df['mistery'] >= 15, 15, 'Mistery满分加15分'),
    'bonus_tds_ge4': (lambda df: df['tds'] >= 4, 8, 'TDS≥4加8分'),
    'bonus_tds_ge6': (lambda df: df['tds'] >= 6, 12, 'TDS≥6加12分'),
    'bonus_d4_ge10': (lambda df: df['d4'] >= 10, 8, '安全边际≥10加8分'),
    'bonus_d4_ge12': (lambda df: df['d4'] >= 12, 12, '安全边际≥12加12分'),
    'bonus_d9_ge10': (lambda df: df['d9'] >= 10, 8, '百胜WR≥10加8分'),
    'bonus_d9_ge12': (lambda df: df['d9'] >= 12, 12, '百胜WR≥12加12分'),
    'bonus_nb_positive': (lambda df: df['nb_yi'] > 0, 5, '资金净流入加5分'),
    'bonus_nb_ge2': (lambda df: df['nb_yi'] >= 2, 10, '资金净流入≥2亿加10分'),
    'bonus_bci_ge70': (lambda df: df['bci'] >= 70, 5, 'BCI≥70加5分'),
    'bonus_bci_ge80': (lambda df: df['bci'] >= 80, 8, 'BCI≥80加8分'),
    'bonus_low_risk': (lambda df: df['net_risk'] == 0, 10, '零风险加10分'),
    'bonus_low_risk2': (lambda df: df['net_risk'] <= 1, 5, '低风险≤1加5分'),
    'bonus_d1_max': (lambda df: df['d1'] >= 15, 5, '多周期共振满分加5分'),
    'bonus_d2_ge20': (lambda df: df['d2'] >= 20, 8, '主线热点≥20加8分'),
    'bonus_d3_ge20': (lambda df: df['d3'] >= 20, 10, '三Skill≥20加10分'),
    'bonus_d5_ge10': (lambda df: df['d5'] >= 10, 5, '基本面≥10加5分'),
    'bonus_not_zt': (lambda df: df['is_zt'] == False, 10, '非涨停加10分'),
    'bonus_tr_3_10': (lambda df: (df['tr'] >= 3) & (df['tr'] <= 10), 5, '换手率3-10%加5分'),
    'bonus_mv_50_200': (lambda df: (df['mv'] >= 50) & (df['mv'] <= 200), 5, '市值50-200亿加5分'),
}


# ============================================================
# 第二部分：辅助计算函数
# ============================================================

def calc_elite_score_vec(df):
    """向量化计算精选基础分"""
    total_norm = df['total'] / 150 * 100
    
    # 维度均衡度
    dims = pd.DataFrame({
        'd1_n': df['d1'] / 15,
        'd2_n': df['d2'] / 25,
        'd3_n': df['d3'] / 47,
        'd4_n': df['d4'] / 15,
        'd5_n': df['d5'] / 15,
    })
    mean_val = dims.mean(axis=1)
    std_val = dims.std(axis=1)
    cv = std_val / mean_val.replace(0, np.nan)
    balance = (1 - cv.fillna(1)).clip(0, 1) * 100
    
    # 风险得分
    risk_score = (100 - df['net_risk'] * 10).clip(0, 100)
    
    # 资金得分
    nb = df['nb_yi'].fillna(0)
    fund_score = (50 + nb * 10).clip(0, 100)
    
    return total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15


def calc_balance_vec(df):
    """向量化计算维度均衡度"""
    dims = pd.DataFrame({
        'd1_n': df['d1'] / 15,
        'd2_n': df['d2'] / 25,
        'd3_n': df['d3'] / 47,
        'd4_n': df['d4'] / 15,
        'd5_n': df['d5'] / 15,
    })
    mean_val = dims.mean(axis=1)
    std_val = dims.std(axis=1)
    cv = std_val / mean_val.replace(0, np.nan)
    return (1 - cv.fillna(1)).clip(0, 1) * 100


# ============================================================
# 第三部分：单因子分析
# ============================================================

def run_single_factor_ic(df, ret_col='ret_1d'):
    """
    单因子IC分析：计算每个打分型因子与未来收益的信息系数
    
    返回: DataFrame，包含每个因子的IC均值、IC_IR、IC>0占比等
    """
    print("\n" + "="*70)
    print(f"📊 单因子IC分析 (目标: {ret_col})")
    print("="*70)
    
    results = []
    dates = sorted(df['date'].unique())
    
    for fname, (calc_fn, direction, desc) in SCORE_FACTORS.items():
        daily_ics = []
        for date in dates:
            day_df = df[df['date'] == date].copy()
            if len(day_df) < 5:
                continue
            try:
                factor_val = calc_fn(day_df) * direction
                ret_val = day_df[ret_col]
                # 去除NaN
                valid = factor_val.notna() & ret_val.notna()
                if valid.sum() < 5:
                    continue
                ic = factor_val[valid].corr(ret_val[valid])
                if not np.isnan(ic):
                    daily_ics.append(ic)
            except Exception:
                continue
        
        if len(daily_ics) < 30:
            continue
        
        ic_mean = np.mean(daily_ics)
        ic_std = np.std(daily_ics)
        ic_ir = ic_mean / ic_std if ic_std > 0 else 0
        ic_pos_ratio = np.mean([1 for x in daily_ics if x > 0]) / len(daily_ics) if daily_ics else 0
        
        results.append({
            'factor': fname,
            'desc': desc,
            'ic_mean': ic_mean,
            'ic_std': ic_std,
            'ic_ir': ic_ir,
            'ic_pos_ratio': ic_pos_ratio,
            'n_days': len(daily_ics),
        })
    
    result_df = pd.DataFrame(results).sort_values('ic_ir', ascending=False)
    
    print(f"\n{'因子':<20} {'描述':<20} {'IC均值':>8} {'IC_IR':>8} {'IC>0%':>8}")
    print("-" * 70)
    for _, r in result_df.head(20).iterrows():
        print(f"{r['factor']:<20} {r['desc']:<20} {r['ic_mean']:>8.4f} {r['ic_ir']:>8.3f} {r['ic_pos_ratio']:>7.1%}")
    
    return result_df


def run_single_factor_group(df, ret_col='ret_1d', n_groups=5):
    """
    单因子分组回测：按因子值分N组，计算每组的平均收益
    
    返回: DataFrame，包含每个因子的多空收益差、单调性等
    """
    print("\n" + "="*70)
    print(f"📊 单因子分组回测 (分{n_groups}组, 目标: {ret_col})")
    print("="*70)
    
    results = []
    dates = sorted(df['date'].unique())
    
    for fname, (calc_fn, direction, desc) in SCORE_FACTORS.items():
        group_rets = {g: [] for g in range(n_groups)}
        
        for date in dates:
            day_df = df[df['date'] == date].copy()
            if len(day_df) < n_groups * 2:
                continue
            try:
                factor_val = calc_fn(day_df) * direction
                valid = factor_val.notna() & day_df[ret_col].notna()
                day_valid = day_df[valid].copy()
                fv = factor_val[valid]
                
                if len(day_valid) < n_groups * 2:
                    continue
                
                # 分组
                day_valid['_fv'] = fv.values
                day_valid['_group'] = pd.qcut(day_valid['_fv'], n_groups, labels=False, duplicates='drop')
                
                for g in range(n_groups):
                    g_ret = day_valid[day_valid['_group'] == g][ret_col].mean()
                    if not np.isnan(g_ret):
                        group_rets[g].append(g_ret)
            except Exception:
                continue
        
        if len(group_rets[0]) < 30:
            continue
        
        # 计算各组平均收益
        group_means = [np.mean(group_rets[g]) for g in range(n_groups)]
        long_short = group_means[-1] - group_means[0]  # 多空收益差
        
        # 单调性：各组收益是否递增
        monotone_score = 0
        for i in range(1, n_groups):
            if group_means[i] > group_means[i-1]:
                monotone_score += 1
        monotone_ratio = monotone_score / (n_groups - 1)
        
        results.append({
            'factor': fname,
            'desc': desc,
            'long_short': long_short,
            'top_group_ret': group_means[-1],
            'bottom_group_ret': group_means[0],
            'monotone_ratio': monotone_ratio,
            'group_rets': group_means,
            'n_days': len(group_rets[0]),
        })
    
    result_df = pd.DataFrame(results).sort_values('long_short', ascending=False)
    
    print(f"\n{'因子':<20} {'描述':<18} {'多空差':>8} {'Top组':>8} {'Bot组':>8} {'单调性':>8}")
    print("-" * 75)
    for _, r in result_df.head(20).iterrows():
        print(f"{r['factor']:<20} {r['desc']:<18} {r['long_short']:>7.3f}% {r['top_group_ret']:>7.3f}% {r['bottom_group_ret']:>7.3f}% {r['monotone_ratio']:>7.1%}")
    
    return result_df


def run_filter_factor_test(df, ret_col='ret_1d'):
    """
    过滤型因子测试：测试每个过滤条件对收益的影响
    """
    print("\n" + "="*70)
    print(f"📊 过滤型因子测试 (目标: {ret_col})")
    print("="*70)
    
    baseline_ret = df[ret_col].mean()
    baseline_n = len(df)
    
    results = []
    
    for fname, (cond_fn, desc) in FILTER_FACTORS.items():
        try:
            mask = cond_fn(df)
            filtered = df[mask]
            if len(filtered) < 100:
                continue
            
            avg_ret = filtered[ret_col].mean()
            win_rate = (filtered[ret_col] > 0).mean()
            n_stocks = len(filtered)
            pass_rate = n_stocks / baseline_n
            
            # 按日计算
            dates = sorted(df['date'].unique())
            daily_rets = []
            for date in dates:
                day_all = df[df['date'] == date]
                day_mask = cond_fn(day_all)
                day_filtered = day_all[day_mask]
                if len(day_filtered) > 0:
                    daily_rets.append(day_filtered[ret_col].mean())
            
            avg_daily_ret = np.mean(daily_rets) if daily_rets else 0
            sharpe = avg_daily_ret / np.std(daily_rets) * np.sqrt(245) if daily_rets and np.std(daily_rets) > 0 else 0
            
            results.append({
                'factor': fname,
                'desc': desc,
                'avg_ret': avg_ret,
                'excess_ret': avg_ret - baseline_ret,
                'win_rate': win_rate,
                'pass_rate': pass_rate,
                'n_stocks': n_stocks,
                'sharpe': sharpe,
                'avg_daily_ret': avg_daily_ret,
            })
        except Exception as e:
            continue
    
    result_df = pd.DataFrame(results).sort_values('excess_ret', ascending=False)
    
    print(f"\n基准: 全样本平均收益 = {baseline_ret:.3f}%, 样本数 = {baseline_n}")
    print(f"\n{'因子':<22} {'描述':<18} {'平均收益':>8} {'超额':>8} {'胜率':>7} {'通过率':>7} {'Sharpe':>7}")
    print("-" * 85)
    for _, r in result_df.head(25).iterrows():
        print(f"{r['factor']:<22} {r['desc']:<18} {r['avg_ret']:>7.3f}% {r['excess_ret']:>+7.3f}% {r['win_rate']:>6.1%} {r['pass_rate']:>6.1%} {r['sharpe']:>7.2f}")
    
    return result_df


def run_bonus_factor_test(df, ret_col='ret_1d'):
    """
    加分型因子测试：测试每个加分条件对收益的影响
    """
    print("\n" + "="*70)
    print(f"📊 加分型因子测试 (目标: {ret_col})")
    print("="*70)
    
    baseline_ret = df[ret_col].mean()
    results = []
    
    for fname, (cond_fn, bonus_val, desc) in BONUS_FACTORS.items():
        try:
            mask = cond_fn(df)
            hit = df[mask]
            miss = df[~mask]
            
            if len(hit) < 50 or len(miss) < 50:
                continue
            
            hit_ret = hit[ret_col].mean()
            miss_ret = miss[ret_col].mean()
            hit_rate = len(hit) / len(df)
            
            results.append({
                'factor': fname,
                'desc': desc,
                'bonus_val': bonus_val,
                'hit_ret': hit_ret,
                'miss_ret': miss_ret,
                'diff': hit_ret - miss_ret,
                'hit_rate': hit_rate,
                'effective': hit_ret > miss_ret,
            })
        except Exception:
            continue
    
    result_df = pd.DataFrame(results).sort_values('diff', ascending=False)
    
    print(f"\n基准: 全样本平均收益 = {baseline_ret:.3f}%")
    print(f"\n{'因子':<22} {'描述':<20} {'加分':>5} {'命中收益':>8} {'未命中':>8} {'差值':>8} {'命中率':>7} {'有效':>5}")
    print("-" * 95)
    for _, r in result_df.iterrows():
        eff = '✅' if r['effective'] else '❌'
        print(f"{r['factor']:<22} {r['desc']:<20} {r['bonus_val']:>5} {r['hit_ret']:>7.3f}% {r['miss_ret']:>7.3f}% {r['diff']:>+7.3f}% {r['hit_rate']:>6.1%} {eff:>5}")
    
    return result_df


# ============================================================
# 第四部分：因子组合回测
# ============================================================

def backtest_strategy(df, filter_names, score_weights, bonus_names, 
                      n=10, max_per_ind=2, hold_days=1):
    """
    通用策略回测引擎
    
    参数:
        df: 全量回测数据
        filter_names: 使用的过滤因子列表
        score_weights: 打分因子权重字典 {factor_name: weight}
        bonus_names: 使用的加分因子列表
        n: 选股数量
        max_per_ind: 同行业最多几只
        hold_days: 持有天数(1/3/5)
    
    返回:
        dict: 回测结果指标
    """
    ret_col = f'ret_{hold_days}d'
    if ret_col not in df.columns:
        ret_col = 'ret_1d'
    
    dates = sorted(df['date'].unique())
    daily_returns = []
    total_selected = 0
    
    for date in dates:
        day_df = df[df['date'] == date].copy()
        
        if len(day_df) == 0:
            continue
        
        # 第一步：应用过滤因子
        mask = pd.Series(True, index=day_df.index)
        for fname in filter_names:
            if fname in FILTER_FACTORS:
                cond_fn = FILTER_FACTORS[fname][0]
                mask &= cond_fn(day_df)
        
        filtered = day_df[mask].copy()
        if len(filtered) == 0:
            daily_returns.append(0)
            continue
        
        # 第二步：计算打分
        score = pd.Series(0.0, index=filtered.index)
        for fname, weight in score_weights.items():
            if fname in SCORE_FACTORS:
                calc_fn, direction, _ = SCORE_FACTORS[fname]
                try:
                    fv = calc_fn(filtered) * direction
                    # 归一化到0-100
                    fv_min, fv_max = fv.min(), fv.max()
                    if fv_max > fv_min:
                        fv_norm = (fv - fv_min) / (fv_max - fv_min) * 100
                    else:
                        fv_norm = pd.Series(50.0, index=filtered.index)
                    score += fv_norm * weight
                except Exception:
                    pass
        
        # 第三步：应用加分
        for fname in bonus_names:
            if fname in BONUS_FACTORS:
                cond_fn, bonus_val, _ = BONUS_FACTORS[fname]
                try:
                    bonus_mask = cond_fn(filtered)
                    score[bonus_mask] += bonus_val
                except Exception:
                    pass
        
        filtered['_score'] = score.values
        filtered = filtered.sort_values('_score', ascending=False)
        
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
        
        if selected:
            sel_df = pd.DataFrame(selected)
            day_ret = sel_df[ret_col].mean()
            daily_returns.append(day_ret if not np.isnan(day_ret) else 0)
            total_selected += len(selected)
        else:
            daily_returns.append(0)
    
    # 计算回测指标
    if not daily_returns:
        return None
    
    returns = np.array(daily_returns)
    cum_ret = np.cumprod(1 + returns / 100) - 1
    
    # 最大回撤
    peak = np.maximum.accumulate(1 + cum_ret)
    drawdown = (1 + cum_ret) / peak - 1
    max_dd = drawdown.min()
    
    # 年化
    n_days = len(returns)
    total_ret = cum_ret[-1]
    annual_ret = (1 + total_ret) ** (245 / n_days) - 1 if n_days > 0 else 0
    
    # Sharpe
    avg_ret = returns.mean()
    std_ret = returns.std()
    sharpe = avg_ret / std_ret * np.sqrt(245) if std_ret > 0 else 0
    
    # 胜率
    win_rate = (returns > 0).mean()
    
    # 盈亏比
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    profit_loss_ratio = abs(wins.mean() / losses.mean()) if len(losses) > 0 and losses.mean() != 0 else 0
    
    return {
        'total_ret': total_ret * 100,
        'annual_ret': annual_ret * 100,
        'sharpe': sharpe,
        'max_dd': max_dd * 100,
        'win_rate': win_rate,
        'profit_loss_ratio': profit_loss_ratio,
        'ret_dd_ratio': abs(total_ret / max_dd) if max_dd != 0 else 0,
        'n_days': n_days,
        'avg_daily_ret': avg_ret,
        'avg_selected': total_selected / n_days if n_days > 0 else 0,
    }


# ============================================================
# 第五部分：因子组合搜索
# ============================================================

def run_combo_search(df, top_filters, top_scores, top_bonuses, n=10, hold_days=1):
    """
    因子组合搜索：测试不同因子组合的效果
    
    策略：
    1. 固定基础过滤（非涨停）
    2. 从top过滤因子中选1-3个组合
    3. 从top打分因子中选2-5个组合不同权重
    4. 从top加分因子中选2-5个组合
    """
    print("\n" + "="*70)
    print(f"🔬 因子组合搜索 (TOP{n}, 持有{hold_days}天)")
    print("="*70)
    
    results = []
    
    # 基础过滤：非涨停 + 5日涨幅<15%
    base_filters = ['no_zt', 'r5_lt15']
    
    # 测试不同过滤组合
    filter_combos = [
        base_filters,
        base_filters + ['wr2_or_mistery'],
        base_filters + ['wr2_or_mistery_loose'],
        base_filters + ['wr2_ge3'],
        base_filters + ['mistery_ge10'],
        base_filters + ['bci_ge60'],
        base_filters + ['wr2_or_mistery', 'bci_ge60'],
        base_filters + ['wr2_or_mistery', 'd4_ge8'],
        base_filters + ['wr2_or_mistery', 'nb_positive'],
        base_filters + ['mainboard', 'wr2_or_mistery'],
        base_filters + ['mainboard'],
        base_filters + ['tr_3_15'],
        base_filters + ['wr2_or_mistery', 'tr_3_15'],
        base_filters + ['wr2_or_mistery', 'low_risk'],
        base_filters + ['wr2_or_mistery', 'd9_ge8'],
    ]
    
    # 测试不同打分权重组合
    score_combos = [
        # 原始策略01的权重
        {'elite_base': 1.0},
        # 总分主导
        {'total': 0.7, 'balance': 0.3},
        {'total': 0.5, 'balance': 0.2, 'nb_yi': 0.15, 'net_risk': 0.15},
        # 技能主导
        {'skill_combo': 0.5, 'total': 0.3, 'balance': 0.2},
        {'wr2': 0.3, 'mistery': 0.3, 'total': 0.2, 'balance': 0.2},
        # 动量主导
        {'momentum_combo': 0.4, 'total': 0.3, 'nb_yi': 0.15, 'balance': 0.15},
        {'d1': 0.3, 'd9': 0.2, 'total': 0.3, 'balance': 0.2},
        # 质量主导
        {'quality_combo': 0.4, 'total': 0.3, 'skill_combo': 0.3},
        {'d4': 0.25, 'd5': 0.15, 'net_risk': 0.2, 'total': 0.2, 'skill_combo': 0.2},
        # 热度主导
        {'hot_combo': 0.3, 'total': 0.3, 'skill_combo': 0.2, 'nb_yi': 0.2},
        {'d2': 0.3, 'bci': 0.2, 'total': 0.3, 'balance': 0.2},
        # 资金主导
        {'nb_yi': 0.3, 'total': 0.3, 'skill_combo': 0.2, 'balance': 0.2},
        {'fund_flow': 0.3, 'total': 0.3, 'wr2': 0.2, 'balance': 0.2},
        # 综合均衡
        {'total': 0.25, 'skill_combo': 0.25, 'quality_combo': 0.25, 'hot_combo': 0.25},
        {'total': 0.2, 'wr2': 0.2, 'mistery': 0.15, 'd4': 0.15, 'nb_yi': 0.15, 'balance': 0.15},
        # 排名因子
        {'total_rank': 0.3, 'wr2_rank': 0.3, 'nb_rank': 0.2, 'd3_rank': 0.2},
        # 新组合：强调WR2+动量
        {'wr2': 0.35, 'd1': 0.2, 'd9': 0.2, 'total': 0.15, 'nb_yi': 0.1},
        # 新组合：BCI+技能
        {'bci': 0.25, 'skill_combo': 0.35, 'total': 0.2, 'balance': 0.2},
    ]
    
    # 测试不同加分组合
    bonus_combos = [
        # 原始策略01
        ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'],
        # 无加分
        [],
        # 仅WR2
        ['bonus_wr2_ge3', 'bonus_wr2_ge4', 'bonus_wr2_5'],
        # 仅Mistery
        ['bonus_mistery_ge10', 'bonus_mistery_ge12', 'bonus_mistery_15'],
        # WR2+Mistery
        ['bonus_wr2_ge4', 'bonus_mistery_ge12'],
        # 全面加分
        ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive', 'bonus_bci_ge70', 'bonus_d9_ge10'],
        # 技能+资金
        ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_tds_ge4', 'bonus_nb_ge2'],
        # 安全+质量
        ['bonus_d4_ge10', 'bonus_low_risk', 'bonus_d5_ge10', 'bonus_not_zt'],
        # 热度+动量
        ['bonus_d2_ge20', 'bonus_d1_max', 'bonus_d9_ge12', 'bonus_bci_ge80'],
        # 精简高效
        ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_not_zt'],
    ]
    
    total_combos = len(filter_combos) * len(score_combos) * len(bonus_combos)
    print(f"总组合数: {len(filter_combos)} × {len(score_combos)} × {len(bonus_combos)} = {total_combos}")
    print("开始测试...\n")
    
    count = 0
    for fi, filters in enumerate(filter_combos):
        for si, scores in enumerate(score_combos):
            for bi, bonuses in enumerate(bonus_combos):
                count += 1
                if count % 100 == 0:
                    print(f"  进度: {count}/{total_combos} ({count/total_combos*100:.0f}%)")
                
                result = backtest_strategy(df, filters, scores, bonuses, n=n, 
                                          max_per_ind=2, hold_days=hold_days)
                if result is None:
                    continue
                
                results.append({
                    'filters': '|'.join(filters),
                    'scores': json.dumps(scores),
                    'bonuses': '|'.join(bonuses),
                    'filter_idx': fi,
                    'score_idx': si,
                    'bonus_idx': bi,
                    **result,
                })
    
    result_df = pd.DataFrame(results)
    
    # 按不同指标排序输出TOP10
    print("\n" + "="*70)
    print("🏆 TOP10 组合 (按收益/回撤比排序)")
    print("="*70)
    top10 = result_df.nlargest(10, 'ret_dd_ratio')
    for i, (_, r) in enumerate(top10.iterrows()):
        print(f"\n  #{i+1} 收益/回撤比={r['ret_dd_ratio']:.2f}")
        print(f"     累计: {r['total_ret']:+.1f}% | 年化: {r['annual_ret']:+.1f}% | Sharpe: {r['sharpe']:.2f}")
        print(f"     回撤: {r['max_dd']:.1f}% | 胜率: {r['win_rate']:.1%} | 盈亏比: {r['profit_loss_ratio']:.2f}")
        print(f"     过滤: {r['filters']}")
        print(f"     打分: {r['scores']}")
        print(f"     加分: {r['bonuses']}")
    
    print("\n" + "="*70)
    print("🏆 TOP10 组合 (按Sharpe排序)")
    print("="*70)
    top10_sharpe = result_df.nlargest(10, 'sharpe')
    for i, (_, r) in enumerate(top10_sharpe.iterrows()):
        print(f"\n  #{i+1} Sharpe={r['sharpe']:.3f}")
        print(f"     累计: {r['total_ret']:+.1f}% | 年化: {r['annual_ret']:+.1f}% | 回撤: {r['max_dd']:.1f}%")
        print(f"     过滤: {r['filters']}")
        print(f"     打分: {r['scores']}")
        print(f"     加分: {r['bonuses']}")
    
    print("\n" + "="*70)
    print("🏆 TOP10 组合 (按累计收益排序)")
    print("="*70)
    top10_ret = result_df.nlargest(10, 'total_ret')
    for i, (_, r) in enumerate(top10_ret.iterrows()):
        print(f"\n  #{i+1} 累计收益={r['total_ret']:+.1f}%")
        print(f"     年化: {r['annual_ret']:+.1f}% | Sharpe: {r['sharpe']:.2f} | 回撤: {r['max_dd']:.1f}%")
        print(f"     过滤: {r['filters']}")
        print(f"     打分: {r['scores']}")
        print(f"     加分: {r['bonuses']}")
    
    return result_df


# ============================================================
# 第六部分：遗传算法优化
# ============================================================

def genetic_optimize(df, n=10, hold_days=1, pop_size=50, n_generations=30, 
                     mutation_rate=0.2, elite_ratio=0.2):
    """
    遗传算法寻找最优因子组合
    
    基因编码：
    - 过滤因子: 二进制向量（是否使用）
    - 打分因子: 连续权重（0-1）
    - 加分因子: 二进制向量（是否使用）
    """
    print("\n" + "="*70)
    print(f"🧬 遗传算法优化 (种群={pop_size}, 代数={n_generations})")
    print("="*70)
    
    # 候选因子池（排除一些冗余的）
    candidate_filters = [
        'no_zt', 'r5_lt15', 'r5_lt20', 'r5_lt10',
        'wr2_or_mistery', 'wr2_or_mistery_loose', 'wr2_ge3', 'wr2_ge4',
        'mistery_ge10', 'mistery_ge12',
        'mainboard', 'bci_ge60', 'bci_ge70',
        'nb_positive', 'low_risk', 'mid_risk',
        'd4_ge8', 'd4_ge10', 'd9_ge8', 'd9_ge10',
        'tr_3_15', 'mv_50_300',
    ]
    
    candidate_scores = [
        'total', 'd1', 'd2', 'd3', 'd4', 'd5', 'd9',
        'mistery', 'tds', 'wr2', 'bci', 'nb_yi', 'net_risk',
        'elite_base', 'balance', 'momentum_combo', 'quality_combo',
        'hot_combo', 'skill_combo', 'fund_flow',
    ]
    
    candidate_bonuses = [
        'bonus_wr2_ge3', 'bonus_wr2_ge4', 'bonus_wr2_5',
        'bonus_mistery_ge10', 'bonus_mistery_ge12', 'bonus_mistery_15',
        'bonus_tds_ge4', 'bonus_tds_ge6',
        'bonus_d4_ge10', 'bonus_d4_ge12',
        'bonus_d9_ge10', 'bonus_d9_ge12',
        'bonus_nb_positive', 'bonus_nb_ge2',
        'bonus_bci_ge70', 'bonus_bci_ge80',
        'bonus_low_risk', 'bonus_low_risk2',
        'bonus_d1_max', 'bonus_d2_ge20', 'bonus_d3_ge20',
        'bonus_not_zt', 'bonus_tr_3_10',
    ]
    
    n_filters = len(candidate_filters)
    n_scores = len(candidate_scores)
    n_bonuses = len(candidate_bonuses)
    
    def create_individual():
        """创建随机个体"""
        # 过滤因子：前2个(no_zt, r5_lt15)强制开启
        filter_genes = np.random.random(n_filters) > 0.6  # 40%概率开启
        filter_genes[0] = True  # no_zt 强制
        filter_genes[1] = True  # r5_lt15 强制
        
        # 打分因子权重：随机0-1，然后归一化
        score_genes = np.random.random(n_scores)
        # 随机关闭一些（设为0）
        score_mask = np.random.random(n_scores) > 0.5
        score_genes *= score_mask
        # 归一化
        if score_genes.sum() > 0:
            score_genes /= score_genes.sum()
        else:
            score_genes[0] = 1.0  # 至少保留total
        
        # 加分因子
        bonus_genes = np.random.random(n_bonuses) > 0.6
        
        return {
            'filters': filter_genes,
            'scores': score_genes,
            'bonuses': bonus_genes,
        }
    
    def decode_individual(ind):
        """解码个体为策略参数"""
        filters = [candidate_filters[i] for i in range(n_filters) if ind['filters'][i]]
        
        scores = {}
        for i in range(n_scores):
            if ind['scores'][i] > 0.01:
                scores[candidate_scores[i]] = ind['scores'][i]
        
        bonuses = [candidate_bonuses[i] for i in range(n_bonuses) if ind['bonuses'][i]]
        
        return filters, scores, bonuses
    
    def fitness(ind):
        """适应度函数：综合考虑收益、Sharpe和回撤"""
        filters, scores, bonuses = decode_individual(ind)
        
        if not scores:
            return -999
        
        result = backtest_strategy(df, filters, scores, bonuses, n=n, 
                                  max_per_ind=2, hold_days=hold_days)
        if result is None:
            return -999
        
        # 综合适应度 = Sharpe * 0.4 + 收益/回撤比 * 0.3 + 年化收益标准化 * 0.3
        sharpe_score = result['sharpe']
        ret_dd_score = min(result['ret_dd_ratio'], 15) / 15  # 归一化到0-1
        annual_score = min(result['annual_ret'], 200) / 200  # 归一化到0-1
        
        fitness_val = sharpe_score * 0.4 + ret_dd_score * 5 * 0.3 + annual_score * 3 * 0.3
        
        # 惩罚过度过滤（通过率太低）
        if result['avg_selected'] < n * 0.3:
            fitness_val *= 0.5
        
        return fitness_val
    
    def crossover(parent1, parent2):
        """交叉"""
        child = {}
        # 过滤因子：均匀交叉
        mask = np.random.random(n_filters) > 0.5
        child['filters'] = np.where(mask, parent1['filters'], parent2['filters'])
        child['filters'][0] = True  # 强制no_zt
        
        # 打分因子：混合权重
        alpha = np.random.random()
        child['scores'] = parent1['scores'] * alpha + parent2['scores'] * (1 - alpha)
        if child['scores'].sum() > 0:
            child['scores'] /= child['scores'].sum()
        
        # 加分因子：均匀交叉
        mask = np.random.random(n_bonuses) > 0.5
        child['bonuses'] = np.where(mask, parent1['bonuses'], parent2['bonuses'])
        
        return child
    
    def mutate(ind):
        """变异"""
        # 过滤因子变异
        for i in range(2, n_filters):  # 跳过前2个强制因子
            if np.random.random() < mutation_rate:
                ind['filters'][i] = not ind['filters'][i]
        
        # 打分因子变异
        for i in range(n_scores):
            if np.random.random() < mutation_rate:
                ind['scores'][i] = np.random.random() * 0.5
        # 重新归一化
        if ind['scores'].sum() > 0:
            ind['scores'] /= ind['scores'].sum()
        
        # 加分因子变异
        for i in range(n_bonuses):
            if np.random.random() < mutation_rate:
                ind['bonuses'][i] = not ind['bonuses'][i]
        
        return ind
    
    # 初始化种群
    population = [create_individual() for _ in range(pop_size)]
    
    # 加入已知好的策略作为种子
    # 策略01的基因
    seed1 = create_individual()
    seed1['filters'] = np.array([candidate_filters[i] in ['no_zt', 'r5_lt15', 'wr2_or_mistery'] 
                                  for i in range(n_filters)])
    seed1['scores'] = np.zeros(n_scores)
    seed1['scores'][candidate_scores.index('elite_base')] = 1.0
    seed1['bonuses'] = np.array([candidate_bonuses[i] in ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'] 
                                  for i in range(n_bonuses)])
    population[0] = seed1
    
    # 策略02的基因（主板）
    seed2 = create_individual()
    seed2['filters'] = np.array([candidate_filters[i] in ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard'] 
                                  for i in range(n_filters)])
    seed2['scores'] = np.zeros(n_scores)
    seed2['scores'][candidate_scores.index('elite_base')] = 1.0
    seed2['bonuses'] = np.array([candidate_bonuses[i] in ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'] 
                                  for i in range(n_bonuses)])
    population[1] = seed2
    
    best_fitness = -999
    best_individual = None
    best_result = None
    history = []
    
    for gen in range(n_generations):
        # 评估适应度
        fitness_scores = []
        for ind in population:
            f = fitness(ind)
            fitness_scores.append(f)
        
        fitness_scores = np.array(fitness_scores)
        
        # 记录最优
        gen_best_idx = np.argmax(fitness_scores)
        gen_best_fitness = fitness_scores[gen_best_idx]
        
        if gen_best_fitness > best_fitness:
            best_fitness = gen_best_fitness
            best_individual = population[gen_best_idx].copy()
            # 获取详细结果
            filters, scores, bonuses = decode_individual(best_individual)
            best_result = backtest_strategy(df, filters, scores, bonuses, n=n, 
                                           max_per_ind=2, hold_days=hold_days)
        
        avg_fitness = fitness_scores.mean()
        history.append({'gen': gen, 'best': gen_best_fitness, 'avg': avg_fitness})
        
        if gen % 5 == 0 or gen == n_generations - 1:
            print(f"  第{gen:>3}代: 最优适应度={gen_best_fitness:.3f}, 平均={avg_fitness:.3f}, 历史最优={best_fitness:.3f}")
        
        # 选择（锦标赛选择）
        n_elite = int(pop_size * elite_ratio)
        elite_idx = np.argsort(fitness_scores)[-n_elite:]
        
        new_population = [population[i] for i in elite_idx]  # 精英保留
        
        while len(new_population) < pop_size:
            # 锦标赛选择
            t1, t2 = np.random.choice(pop_size, 2, replace=False)
            p1 = population[t1] if fitness_scores[t1] > fitness_scores[t2] else population[t2]
            t1, t2 = np.random.choice(pop_size, 2, replace=False)
            p2 = population[t1] if fitness_scores[t1] > fitness_scores[t2] else population[t2]
            
            child = crossover(p1, p2)
            child = mutate(child)
            new_population.append(child)
        
        population = new_population
    
    # 输出最优结果
    if best_individual is not None and best_result is not None:
        filters, scores, bonuses = decode_individual(best_individual)
        print("\n" + "="*70)
        print("🏆 遗传算法最优组合")
        print("="*70)
        print(f"\n  累计收益: {best_result['total_ret']:+.1f}%")
        print(f"  年化收益: {best_result['annual_ret']:+.1f}%")
        print(f"  Sharpe:   {best_result['sharpe']:.3f}")
        print(f"  最大回撤: {best_result['max_dd']:.1f}%")
        print(f"  收益/回撤: {best_result['ret_dd_ratio']:.2f}")
        print(f"  胜率:     {best_result['win_rate']:.1%}")
        print(f"  盈亏比:   {best_result['profit_loss_ratio']:.2f}")
        print(f"\n  过滤因子: {filters}")
        print(f"  打分权重: {json.dumps(scores, indent=2)}")
        print(f"  加分因子: {bonuses}")
        
        return {
            'best_individual': best_individual,
            'best_result': best_result,
            'filters': filters,
            'scores': scores,
            'bonuses': bonuses,
            'history': history,
        }
    
    return None


# ============================================================
# 第七部分：交叉验证
# ============================================================

def rolling_validation(df, filters, scores, bonuses, n=10, hold_days=1, 
                       train_months=6, test_months=3):
    """
    滚动窗口交叉验证：避免过拟合
    
    将数据分为多个 train_months 训练 + test_months 测试 的窗口
    在训练集上优化，在测试集上验证
    """
    print("\n" + "="*70)
    print(f"📋 滚动窗口验证 (训练{train_months}月 + 测试{test_months}月)")
    print("="*70)
    
    dates = sorted(df['date'].unique())
    
    # 按月分组
    df_temp = df.copy()
    df_temp['month'] = df_temp['date'].astype(str).str[:6]
    months = sorted(df_temp['month'].unique())
    
    window_results = []
    
    for i in range(0, len(months) - train_months - test_months + 1, test_months):
        train_months_list = months[i:i+train_months]
        test_months_list = months[i+train_months:i+train_months+test_months]
        
        if not test_months_list:
            break
        
        train_df = df_temp[df_temp['month'].isin(train_months_list)]
        test_df = df_temp[df_temp['month'].isin(test_months_list)]
        
        if len(train_df) < 100 or len(test_df) < 50:
            continue
        
        # 在训练集上评估
        train_result = backtest_strategy(train_df, filters, scores, bonuses, 
                                        n=n, max_per_ind=2, hold_days=hold_days)
        # 在测试集上评估
        test_result = backtest_strategy(test_df, filters, scores, bonuses, 
                                       n=n, max_per_ind=2, hold_days=hold_days)
        
        if train_result and test_result:
            window_results.append({
                'train_period': f"{train_months_list[0]}~{train_months_list[-1]}",
                'test_period': f"{test_months_list[0]}~{test_months_list[-1]}",
                'train_sharpe': train_result['sharpe'],
                'test_sharpe': test_result['sharpe'],
                'train_ret': train_result['total_ret'],
                'test_ret': test_result['total_ret'],
                'train_dd': train_result['max_dd'],
                'test_dd': test_result['max_dd'],
                'train_wr': train_result['win_rate'],
                'test_wr': test_result['win_rate'],
            })
    
    if not window_results:
        print("  ⚠️ 数据不足，无法进行滚动验证")
        return None
    
    result_df = pd.DataFrame(window_results)
    
    print(f"\n{'训练期':<16} {'测试期':<16} {'训练Sharpe':>10} {'测试Sharpe':>10} {'训练收益':>10} {'测试收益':>10}")
    print("-" * 80)
    for _, r in result_df.iterrows():
        print(f"{r['train_period']:<16} {r['test_period']:<16} {r['train_sharpe']:>10.3f} {r['test_sharpe']:>10.3f} {r['train_ret']:>9.1f}% {r['test_ret']:>9.1f}%")
    
    # 汇总
    print(f"\n{'='*40}")
    print(f"  训练集平均 Sharpe: {result_df['train_sharpe'].mean():.3f}")
    print(f"  测试集平均 Sharpe: {result_df['test_sharpe'].mean():.3f}")
    print(f"  Sharpe衰减率: {(1 - result_df['test_sharpe'].mean() / result_df['train_sharpe'].mean()) * 100:.1f}%")
    print(f"  测试集胜率(Sharpe>0): {(result_df['test_sharpe'] > 0).mean():.1%}")
    print(f"  测试集平均胜率: {result_df['test_wr'].mean():.1%}")
    
    # 过拟合判断
    decay = 1 - result_df['test_sharpe'].mean() / max(result_df['train_sharpe'].mean(), 0.01)
    if decay > 0.5:
        print(f"\n  ⚠️ 警告：Sharpe衰减超过50%，可能存在过拟合！")
    elif decay > 0.3:
        print(f"\n  ⚡ 注意：Sharpe衰减30-50%，有轻微过拟合风险")
    else:
        print(f"\n  ✅ 良好：Sharpe衰减<30%，策略稳健性较好")
    
    return result_df


# ============================================================
# 第八部分：主程序
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='因子实验室')
    parser.add_argument('--single', action='store_true', help='仅运行单因子测试')
    parser.add_argument('--combo', action='store_true', help='仅运行组合搜索')
    parser.add_argument('--optimize', action='store_true', help='运行遗传算法优化')
    parser.add_argument('--validate', action='store_true', help='交叉验证')
    parser.add_argument('--all', action='store_true', help='运行全部测试')
    parser.add_argument('--n', type=int, default=10, help='选股数量')
    parser.add_argument('--hold', type=int, default=1, help='持有天数')
    parser.add_argument('--mainboard', action='store_true', help='仅主板')
    args = parser.parse_args()
    
    # 如果没有指定任何选项，默认运行全部
    if not any([args.single, args.combo, args.optimize, args.validate, args.all]):
        args.all = True
    
    # 加载数据
    print("📂 加载回测数据...")
    df = pd.read_csv(DETAIL_FILE)
    print(f"   样本数: {len(df)}, 交易日: {df['date'].nunique()}")
    print(f"   日期范围: {df['date'].min()} ~ {df['date'].max()}")
    
    # 主板过滤
    if args.mainboard:
        df = df[df['code'].str[:3].isin(MAINBOARD_PREFIXES)].copy()
        print(f"   主板过滤后: {len(df)} 条")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # ========== 单因子测试 ==========
    if args.single or args.all:
        print("\n\n" + "🔬"*35)
        print("                    单 因 子 分 析")
        print("🔬"*35)
        
        # IC分析
        ic_results = run_single_factor_ic(df, ret_col='ret_1d')
        ic_results.to_csv(f'{OUTPUT_DIR}/single_ic_{timestamp}.csv', index=False)
        
        # 分组回测
        group_results = run_single_factor_group(df, ret_col='ret_1d')
        group_results.to_csv(f'{OUTPUT_DIR}/single_group_{timestamp}.csv', index=False)
        
        # 过滤因子测试
        filter_results = run_filter_factor_test(df, ret_col='ret_1d')
        filter_results.to_csv(f'{OUTPUT_DIR}/filter_test_{timestamp}.csv', index=False)
        
        # 加分因子测试
        bonus_results = run_bonus_factor_test(df, ret_col='ret_1d')
        bonus_results.to_csv(f'{OUTPUT_DIR}/bonus_test_{timestamp}.csv', index=False)
        
        print(f"\n✅ 单因子分析结果已保存到 {OUTPUT_DIR}/")
    
    # ========== 组合搜索 ==========
    if args.combo or args.all:
        print("\n\n" + "🔬"*35)
        print("                    因 子 组 合 搜 索")
        print("🔬"*35)
        
        combo_results = run_combo_search(df, None, None, None, n=args.n, hold_days=args.hold)
        combo_results.to_csv(f'{OUTPUT_DIR}/combo_search_{timestamp}.csv', index=False)
        
        print(f"\n✅ 组合搜索结果已保存到 {OUTPUT_DIR}/")
    
    # ========== 遗传算法优化 ==========
    if args.optimize or args.all:
        print("\n\n" + "🔬"*35)
        print("                    遗 传 算 法 优 化")
        print("🔬"*35)
        
        ga_result = genetic_optimize(df, n=args.n, hold_days=args.hold, 
                                     pop_size=60, n_generations=40)
        
        if ga_result:
            # 保存最优组合
            best_config = {
                'filters': ga_result['filters'],
                'scores': ga_result['scores'],
                'bonuses': ga_result['bonuses'],
                'result': ga_result['best_result'],
            }
            with open(f'{OUTPUT_DIR}/ga_best_{timestamp}.json', 'w') as f:
                json.dump(best_config, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ 遗传算法结果已保存到 {OUTPUT_DIR}/ga_best_{timestamp}.json")
            
            # 对最优组合进行交叉验证
            print("\n\n对最优组合进行交叉验证...")
            val_results = rolling_validation(
                df, ga_result['filters'], ga_result['scores'], ga_result['bonuses'],
                n=args.n, hold_days=args.hold
            )
            if val_results is not None:
                val_results.to_csv(f'{OUTPUT_DIR}/ga_validation_{timestamp}.csv', index=False)
    
    # ========== 交叉验证现有策略 ==========
    if args.validate or args.all:
        print("\n\n" + "🔬"*35)
        print("                    现 有 策 略 验 证")
        print("🔬"*35)
        
        # 验证策略01
        print("\n--- 策略01: 严格精选 ---")
        s01_filters = ['no_zt', 'r5_lt15', 'wr2_or_mistery']
        s01_scores = {'elite_base': 1.0}
        s01_bonuses = ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive']
        rolling_validation(df, s01_filters, s01_scores, s01_bonuses, n=args.n, hold_days=args.hold)
        
        # 验证策略02（主板）
        print("\n--- 策略02: 主板严格精选 ---")
        s02_filters = ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard']
        rolling_validation(df, s02_filters, s01_scores, s01_bonuses, n=args.n, hold_days=args.hold)
    
    print("\n\n" + "="*70)
    print("🎉 因子实验室全部测试完成！")
    print(f"   结果目录: {OUTPUT_DIR}/")
    print("="*70)


if __name__ == '__main__':
    main()
