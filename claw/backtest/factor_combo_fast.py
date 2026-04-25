#!/usr/bin/env python3
"""
因子组合快速搜索 — 预计算所有因子值，极速回测
================================================================
核心优化：
  1. 预计算所有因子的归一化值（按日期分组）
  2. 回测时只做矩阵乘法，不再重复计算因子
  3. 简化行业分散（取TOP-N后去重）
  
使用: python3 factor_combo_fast.py
"""

import pandas as pd
import numpy as np
from collections import defaultdict
import time
import json
import os

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_052244.csv'
OUTPUT_DIR = 'factor_lab_results'
os.makedirs(OUTPUT_DIR, exist_ok=True)
MAINBOARD_PREFIXES = ['600', '601', '603', '605', '000', '001', '002', '003']

# ============================================================
# 加载数据
# ============================================================
print("📂 加载数据...")
df = pd.read_csv(DETAIL_FILE)
df['nb_yi'] = df['nb_yi'].fillna(0)
print(f"   {len(df)}条, {df['date'].nunique()}天, {df['date'].min()}~{df['date'].max()}")

# ============================================================
# 预计算因子值（按日期归一化到0-100）
# ============================================================
print("⚙️  预计算因子值...")

# 定义所有打分因子
def calc_elite_base(g):
    total_norm = g['total'] / 150 * 100
    dims = pd.DataFrame({
        'd1_n': g['d1'] / 15, 'd2_n': g['d2'] / 25,
        'd3_n': g['d3'] / 47, 'd4_n': g['d4'] / 15, 'd5_n': g['d5'] / 15,
    })
    mean_v = dims.mean(axis=1)
    std_v = dims.std(axis=1)
    cv = std_v / mean_v.replace(0, np.nan)
    balance = (1 - cv.fillna(1)).clip(0, 1) * 100
    risk_score = (100 - g['net_risk'] * 10).clip(0, 100)
    fund_score = (50 + g['nb_yi'] * 10).clip(0, 100)
    return total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15

# 因子列表: (名称, 计算函数, 方向)
FACTORS = [
    ('total', lambda g: g['total'], 1),
    ('d1', lambda g: g['d1'], 1),
    ('d2', lambda g: g['d2'], 1),
    ('d3', lambda g: g['d3'], 1),
    ('d4', lambda g: g['d4'], 1),
    ('d5', lambda g: g['d5'], 1),
    ('d9', lambda g: g['d9'], 1),
    ('mistery', lambda g: g['mistery'], 1),
    ('tds', lambda g: g['tds'], 1),
    ('wr2', lambda g: g['wr2'], 1),
    ('wr1', lambda g: g['wr1'], 1),
    ('wr3', lambda g: g['wr3'], 1),
    ('bci', lambda g: g['bci'], 1),
    ('nb_yi', lambda g: g['nb_yi'], 1),
    ('net_risk', lambda g: g['net_risk'], -1),
    ('tr', lambda g: g['tr'], -1),
    ('mv', lambda g: g['mv'], -1),
    ('r5', lambda g: g['r5'], -1),
    ('r10', lambda g: g['r10'], -1),
    ('elite_base', calc_elite_base, 1),
    ('skill_combo', lambda g: g['mistery'] + g['tds'] * 2 + g['wr2'] * 3, 1),
    ('momentum_combo', lambda g: g['d1'] + g['d9'] * 0.5, 1),
    ('quality_combo', lambda g: g['d4'] + g['d5'] + (5 - g['net_risk']) * 3, 1),
    ('hot_combo', lambda g: g['d2'] + g['bci'] * 0.3, 1),
]

factor_names = [f[0] for f in FACTORS]
n_factors = len(factor_names)

# 预计算：按日期归一化所有因子到0-100
dates = sorted(df['date'].unique())
date_indices = {d: i for i, d in enumerate(dates)}
n_dates = len(dates)

# 存储每日数据
daily_data = []  # 每天的数据
daily_factor_norm = []  # 每天归一化后的因子矩阵
daily_ret = []  # 每天的收益
daily_filters = []  # 每天的过滤mask

for date in dates:
    day_df = df[df['date'] == date].copy().reset_index(drop=True)
    n_stocks = len(day_df)
    
    # 计算所有因子归一化值
    factor_matrix = np.zeros((n_stocks, n_factors))
    for fi, (fname, calc_fn, direction) in enumerate(FACTORS):
        try:
            fv = calc_fn(day_df).values * direction
            fv_min, fv_max = np.nanmin(fv), np.nanmax(fv)
            if fv_max > fv_min:
                fv_norm = (fv - fv_min) / (fv_max - fv_min) * 100
            else:
                fv_norm = np.full(n_stocks, 50.0)
            factor_matrix[:, fi] = fv_norm
        except:
            factor_matrix[:, fi] = 50.0
    
    # 预计算过滤条件
    filters = {
        'no_zt': ~day_df['is_zt'].values.astype(bool),
        'r5_lt15': day_df['r5'].values < 15,
        'r5_lt10': day_df['r5'].values < 10,
        'r5_lt20': day_df['r5'].values < 20,
        'wr2_ge3': day_df['wr2'].values >= 3,
        'wr2_ge4': day_df['wr2'].values >= 4,
        'mistery_ge10': day_df['mistery'].values >= 10,
        'mistery_ge12': day_df['mistery'].values >= 12,
        'wr2_or_mistery': (day_df['wr2'].values >= 3) | (day_df['mistery'].values >= 10),
        'wr2_or_mistery_loose': (day_df['wr2'].values >= 2) | (day_df['mistery'].values >= 8),
        'mainboard': np.array([c[:3] in MAINBOARD_PREFIXES for c in day_df['code'].values]),
        'low_risk': day_df['net_risk'].values <= 2,
        'd4_ge8': day_df['d4'].values >= 8,
        'd4_ge10': day_df['d4'].values >= 10,
        'tds_ge3': day_df['tds'].values >= 3,
        'bci_ge60': day_df['bci'].values >= 60,
        'd9_ge8': day_df['d9'].values >= 8,
        'nb_positive': day_df['nb_yi'].values > 0,
    }
    
    # 预计算加分条件
    bonuses = {
        'bonus_wr2_ge3': (day_df['wr2'].values >= 3, 10),
        'bonus_wr2_ge4': (day_df['wr2'].values >= 4, 15),
        'bonus_wr2_5': (day_df['wr2'].values >= 5, 20),
        'bonus_mistery_ge10': (day_df['mistery'].values >= 10, 8),
        'bonus_mistery_ge12': (day_df['mistery'].values >= 12, 12),
        'bonus_mistery_15': (day_df['mistery'].values >= 15, 15),
        'bonus_tds_ge4': (day_df['tds'].values >= 4, 8),
        'bonus_d4_ge10': (day_df['d4'].values >= 10, 8),
        'bonus_d4_ge12': (day_df['d4'].values >= 12, 12),
        'bonus_d9_ge10': (day_df['d9'].values >= 10, 8),
        'bonus_d9_ge12': (day_df['d9'].values >= 12, 12),
        'bonus_nb_positive': (day_df['nb_yi'].values > 0, 5),
        'bonus_low_risk2': (day_df['net_risk'].values <= 1, 5),
        'bonus_low_risk': (day_df['net_risk'].values == 0, 10),
        'bonus_not_zt': (~day_df['is_zt'].values.astype(bool), 10),
        'bonus_d3_ge20': (day_df['d3'].values >= 20, 10),
        'bonus_bci_ge70': (day_df['bci'].values >= 70, 5),
        'bonus_d2_ge20': (day_df['d2'].values >= 20, 8),
    }
    
    daily_data.append(day_df)
    daily_factor_norm.append(factor_matrix)
    daily_ret.append(day_df['ret_1d'].values)
    daily_filters.append(filters)

# 把加分也预存
daily_bonuses = []
for date in dates:
    day_df = df[df['date'] == date].reset_index(drop=True)
    bonuses = {
        'bonus_wr2_ge3': (day_df['wr2'].values >= 3, 10),
        'bonus_wr2_ge4': (day_df['wr2'].values >= 4, 15),
        'bonus_wr2_5': (day_df['wr2'].values >= 5, 20),
        'bonus_mistery_ge10': (day_df['mistery'].values >= 10, 8),
        'bonus_mistery_ge12': (day_df['mistery'].values >= 12, 12),
        'bonus_mistery_15': (day_df['mistery'].values >= 15, 15),
        'bonus_tds_ge4': (day_df['tds'].values >= 4, 8),
        'bonus_d4_ge10': (day_df['d4'].values >= 10, 8),
        'bonus_d4_ge12': (day_df['d4'].values >= 12, 12),
        'bonus_d9_ge10': (day_df['d9'].values >= 10, 8),
        'bonus_d9_ge12': (day_df['d9'].values >= 12, 12),
        'bonus_nb_positive': (day_df['nb_yi'].values > 0, 5),
        'bonus_low_risk2': (day_df['net_risk'].values <= 1, 5),
        'bonus_low_risk': (day_df['net_risk'].values == 0, 10),
        'bonus_not_zt': (~day_df['is_zt'].values.astype(bool), 10),
        'bonus_d3_ge20': (day_df['d3'].values >= 20, 10),
        'bonus_bci_ge70': (day_df['bci'].values >= 70, 5),
        'bonus_d2_ge20': (day_df['d2'].values >= 20, 8),
    }
    daily_bonuses.append(bonuses)

print(f"   预计算完成! {n_factors}个因子 × {n_dates}天")


# ============================================================
# 极速回测函数
# ============================================================

def fast_backtest(filter_names, score_weights_vec, bonus_names, n=10):
    """
    极速回测：使用预计算的因子矩阵
    
    参数:
        filter_names: 过滤因子名列表
        score_weights_vec: 长度=n_factors的权重向量
        bonus_names: 加分因子名列表
        n: 选股数量
    """
    daily_returns = np.zeros(n_dates)
    
    for di in range(n_dates):
        factor_matrix = daily_factor_norm[di]
        ret_arr = daily_ret[di]
        filters = daily_filters[di]
        bonuses = daily_bonuses[di]
        n_stocks = len(ret_arr)
        
        # 应用过滤
        mask = np.ones(n_stocks, dtype=bool)
        for fname in filter_names:
            if fname in filters:
                mask &= filters[fname]
        
        valid_idx = np.where(mask)[0]
        if len(valid_idx) == 0:
            continue
        
        # 计算得分 = 因子矩阵 × 权重向量
        scores = factor_matrix[valid_idx] @ score_weights_vec
        
        # 加分
        for bname in bonus_names:
            if bname in bonuses:
                bmask, bval = bonuses[bname]
                scores += bmask[valid_idx] * bval
        
        # 取TOP-N
        if len(valid_idx) <= n:
            top_local_idx = np.arange(len(valid_idx))
        else:
            top_local_idx = np.argpartition(scores, -n)[-n:]
        
        top_global_idx = valid_idx[top_local_idx]
        day_ret = np.nanmean(ret_arr[top_global_idx])
        daily_returns[di] = day_ret if not np.isnan(day_ret) else 0
    
    # 计算指标
    cum_ret = np.cumprod(1 + daily_returns / 100) - 1
    peak = np.maximum.accumulate(1 + cum_ret)
    drawdown = (1 + cum_ret) / peak - 1
    max_dd = drawdown.min()
    total_ret = cum_ret[-1]
    annual_ret = (1 + total_ret) ** (245 / n_dates) - 1 if n_dates > 0 else 0
    avg_ret = daily_returns.mean()
    std_ret = daily_returns.std()
    sharpe = avg_ret / std_ret * np.sqrt(245) if std_ret > 0 else 0
    win_rate = (daily_returns > 0).mean()
    wins = daily_returns[daily_returns > 0]
    losses = daily_returns[daily_returns < 0]
    pl_ratio = abs(wins.mean() / losses.mean()) if len(losses) > 0 and losses.mean() != 0 else 0
    
    return {
        'total_ret': total_ret * 100,
        'annual_ret': annual_ret * 100,
        'sharpe': sharpe,
        'max_dd': max_dd * 100,
        'win_rate': win_rate,
        'ret_dd_ratio': abs(total_ret / max_dd) if max_dd != 0 else 0,
        'pl_ratio': pl_ratio,
    }


def make_weight_vec(score_dict):
    """将因子名->权重字典转为权重向量"""
    vec = np.zeros(n_factors)
    for fname, weight in score_dict.items():
        if fname in factor_names:
            vec[factor_names.index(fname)] = weight
    return vec


# ============================================================
# 测试速度
# ============================================================
t0 = time.time()
wv = make_weight_vec({'elite_base': 1.0})
r = fast_backtest(['no_zt', 'r5_lt15', 'wr2_or_mistery'], wv, 
                  ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'], n=10)
t1 = time.time()
print(f"\n⚡ 单次回测耗时: {(t1-t0)*1000:.1f}ms")
print(f"   策略01基准(无行业分散): 累计{r['total_ret']:+.1f}% | 年化{r['annual_ret']:+.1f}% | Sh={r['sharpe']:.2f} | DD={r['max_dd']:.1f}%")


# ============================================================
# 大规模组合搜索
# ============================================================
print("\n" + "="*70)
print("🔬 大规模因子组合搜索")
print("="*70)

# 过滤组合
filter_combos = [
    ['no_zt', 'r5_lt15', 'wr2_or_mistery'],           # F0: 策略01原版
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard'],  # F1: 策略02原版
    ['no_zt', 'r5_lt10', 'wr2_or_mistery'],            # F2: 更严涨幅
    ['no_zt', 'r5_lt15', 'mistery_ge10'],              # F3: 仅Mistery
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'd4_ge8'],  # F4: +安全边际
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'low_risk'],  # F5: +低风险
    ['no_zt', 'r5_lt15', 'wr2_ge3'],                   # F6: 仅WR2
    ['no_zt', 'r5_lt15', 'mistery_ge12'],              # F7: 高Mistery
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'tds_ge3'],  # F8: +TDS
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'd9_ge8'],  # F9: +百胜WR
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'bci_ge60'],  # F10: +BCI
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'nb_positive'],  # F11: +资金
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard', 'low_risk'],  # F12: 主板+低风险
    ['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard', 'd4_ge8'],  # F13: 主板+安全
    ['no_zt', 'r5_lt20', 'wr2_or_mistery_loose'],      # F14: 宽松
]

# 打分权重组合
score_combos = [
    {'elite_base': 1.0},                                                    # S0: 原版
    {'total': 0.5, 'net_risk': 0.2, 'nb_yi': 0.15, 'd4': 0.15},           # S1: 总分+风险+资金+安全
    {'d9': 0.3, 'd1': 0.2, 'total': 0.3, 'net_risk': 0.2},                # S2: 动量+风险
    {'skill_combo': 0.4, 'net_risk': 0.3, 'total': 0.3},                   # S3: 技能+风险
    {'net_risk': 0.3, 'd4': 0.2, 'total': 0.3, 'tds': 0.2},               # S4: 风险+安全+TDS
    {'wr2': 0.3, 'tds': 0.2, 'net_risk': 0.2, 'total': 0.3},              # S5: WR2+TDS+风险
    {'d9': 0.25, 'mistery': 0.2, 'net_risk': 0.25, 'd4': 0.15, 'total': 0.15},  # S6: 综合
    {'momentum_combo': 0.3, 'net_risk': 0.3, 'skill_combo': 0.2, 'total': 0.2},  # S7: 动量+风险+技能
    {'net_risk': 0.35, 'total': 0.25, 'wr2': 0.2, 'd4': 0.2},             # S8: 风险主导
    {'total': 0.3, 'net_risk': 0.25, 'mistery': 0.2, 'd4': 0.15, 'tds': 0.1},  # S9: 总分+风险+Mistery
    {'net_risk': 0.3, 'skill_combo': 0.3, 'd4': 0.2, 'total': 0.2},       # S10: 风险+技能+安全
    {'wr2': 0.25, 'mistery': 0.2, 'net_risk': 0.2, 'total': 0.2, 'd4': 0.15},  # S11: WR2+Mistery+风险
    {'net_risk': 0.25, 'r5': 0.2, 'total': 0.25, 'wr2': 0.15, 'd4': 0.15},  # S12: 风险+低位+WR2
    {'quality_combo': 0.4, 'skill_combo': 0.3, 'total': 0.3},              # S13: 质量+技能
    {'tds': 0.3, 'wr2': 0.25, 'net_risk': 0.25, 'total': 0.2},            # S14: TDS+WR2+风险
]

# 加分组合
bonus_combos = [
    ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'],  # B0: 原版
    ['bonus_not_zt', 'bonus_mistery_ge12', 'bonus_wr2_ge3', 'bonus_d4_ge10'],  # B1: 优化版
    ['bonus_not_zt', 'bonus_mistery_ge12', 'bonus_wr2_5', 'bonus_low_risk2', 'bonus_tds_ge4'],  # B2: 全有效
    ['bonus_not_zt', 'bonus_mistery_ge10', 'bonus_d4_ge10', 'bonus_low_risk2'],  # B3: 宽松有效
    ['bonus_mistery_ge12', 'bonus_wr2_ge4', 'bonus_d3_ge20', 'bonus_tds_ge4'],  # B4: 技能集中
    [],  # B5: 无加分
    ['bonus_not_zt', 'bonus_mistery_ge12', 'bonus_d4_ge12', 'bonus_low_risk'],  # B6: 高门槛
    ['bonus_wr2_ge3', 'bonus_mistery_ge10', 'bonus_not_zt'],  # B7: 精简
]

# 预计算权重向量
score_vecs = [make_weight_vec(s) for s in score_combos]

total_combos = len(filter_combos) * len(score_combos) * len(bonus_combos)
print(f"组合数: {len(filter_combos)} × {len(score_combos)} × {len(bonus_combos)} = {total_combos}")

t0 = time.time()
results = []

for fi, filters in enumerate(filter_combos):
    for si, (scores, svec) in enumerate(zip(score_combos, score_vecs)):
        for bi, bonuses in enumerate(bonus_combos):
            r = fast_backtest(filters, svec, bonuses, n=10)
            if r:
                results.append({
                    'fi': fi, 'si': si, 'bi': bi,
                    'filters': '|'.join(filters),
                    'scores': json.dumps(scores),
                    'bonuses': '|'.join(bonuses),
                    **r,
                })

t1 = time.time()
print(f"\n✅ {total_combos}个组合完成! 耗时: {t1-t0:.1f}秒 ({(t1-t0)/total_combos*1000:.1f}ms/组合)")

result_df = pd.DataFrame(results).sort_values('ret_dd_ratio', ascending=False)

# ============================================================
# 输出结果
# ============================================================
print("\n" + "="*70)
print("🏆 TOP20 组合 (按收益/回撤比排序)")
print("="*70)
for i, (_, r) in enumerate(result_df.head(20).iterrows()):
    print(f"\n  #{i+1} 收益/回撤={r['ret_dd_ratio']:.2f} | 累计{r['total_ret']:+.1f}% | 年化{r['annual_ret']:+.1f}% | Sh={r['sharpe']:.2f} | DD={r['max_dd']:.1f}% | WR={r['win_rate']:.1%} | PL={r['pl_ratio']:.2f}")
    print(f"     F[{r['fi']}]: {r['filters']}")
    print(f"     S[{r['si']}]: {r['scores']}")
    print(f"     B[{r['bi']}]: {r['bonuses']}")

print("\n" + "="*70)
print("🏆 TOP15 组合 (按Sharpe排序)")
print("="*70)
for i, (_, r) in enumerate(result_df.nlargest(15, 'sharpe').iterrows()):
    print(f"  #{i+1} Sh={r['sharpe']:.3f} | 累计{r['total_ret']:+.1f}% | DD={r['max_dd']:.1f}% | R/D={r['ret_dd_ratio']:.2f} | WR={r['win_rate']:.1%}")
    print(f"     F[{r['fi']}]: {r['filters']}")
    print(f"     S[{r['si']}]: {r['scores']}")

print("\n" + "="*70)
print("🏆 TOP15 组合 (按累计收益排序)")
print("="*70)
for i, (_, r) in enumerate(result_df.nlargest(15, 'total_ret').iterrows()):
    print(f"  #{i+1} 累计{r['total_ret']:+.1f}% | 年化{r['annual_ret']:+.1f}% | Sh={r['sharpe']:.2f} | DD={r['max_dd']:.1f}% | R/D={r['ret_dd_ratio']:.2f}")
    print(f"     F[{r['fi']}]: {r['filters']}")
    print(f"     S[{r['si']}]: {r['scores']}")

# ============================================================
# 分析：哪些因子组件最常出现在TOP组合中
# ============================================================
print("\n" + "="*70)
print("📊 因子组件频率分析 (TOP50组合中各组件出现频率)")
print("="*70)

top50 = result_df.head(50)

print("\n  过滤组合频率:")
for fi, cnt in top50['fi'].value_counts().head(10).items():
    print(f"    F[{fi}] ({filter_combos[fi][-1] if len(filter_combos[fi])>3 else '基础'}): {cnt}次 ({cnt/50*100:.0f}%)")

print("\n  打分组合频率:")
for si, cnt in top50['si'].value_counts().head(10).items():
    desc = list(score_combos[si].keys())
    print(f"    S[{si}] ({'+'.join(desc[:3])}): {cnt}次 ({cnt/50*100:.0f}%)")

print("\n  加分组合频率:")
for bi, cnt in top50['bi'].value_counts().head(8).items():
    desc = bonus_combos[bi][0] if bonus_combos[bi] else '无加分'
    print(f"    B[{bi}] ({desc}): {cnt}次 ({cnt/50*100:.0f}%)")

# ============================================================
# 对比现有策略
# ============================================================
print("\n" + "="*70)
print("📊 现有策略 vs 最优组合对比")
print("="*70)

# 策略01原版
s01 = fast_backtest(['no_zt', 'r5_lt15', 'wr2_or_mistery'], 
                    make_weight_vec({'elite_base': 1.0}),
                    ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'], n=10)
print(f"\n  策略01(原版TOP10): 累计{s01['total_ret']:+.1f}% | 年化{s01['annual_ret']:+.1f}% | Sh={s01['sharpe']:.2f} | DD={s01['max_dd']:.1f}% | R/D={s01['ret_dd_ratio']:.2f}")

# 策略02原版
s02 = fast_backtest(['no_zt', 'r5_lt15', 'wr2_or_mistery', 'mainboard'],
                    make_weight_vec({'elite_base': 1.0}),
                    ['bonus_wr2_ge4', 'bonus_mistery_ge12', 'bonus_d4_ge10', 'bonus_nb_positive'], n=10)
print(f"  策略02(主板TOP10): 累计{s02['total_ret']:+.1f}% | 年化{s02['annual_ret']:+.1f}% | Sh={s02['sharpe']:.2f} | DD={s02['max_dd']:.1f}% | R/D={s02['ret_dd_ratio']:.2f}")

# 最优组合
best = result_df.iloc[0]
print(f"\n  最优组合(R/D):    累计{best['total_ret']:+.1f}% | 年化{best['annual_ret']:+.1f}% | Sh={best['sharpe']:.2f} | DD={best['max_dd']:.1f}% | R/D={best['ret_dd_ratio']:.2f}")
print(f"     过滤: {best['filters']}")
print(f"     打分: {best['scores']}")
print(f"     加分: {best['bonuses']}")

best_sharpe = result_df.nlargest(1, 'sharpe').iloc[0]
print(f"\n  最优组合(Sharpe): 累计{best_sharpe['total_ret']:+.1f}% | 年化{best_sharpe['annual_ret']:+.1f}% | Sh={best_sharpe['sharpe']:.2f} | DD={best_sharpe['max_dd']:.1f}% | R/D={best_sharpe['ret_dd_ratio']:.2f}")
print(f"     过滤: {best_sharpe['filters']}")
print(f"     打分: {best_sharpe['scores']}")
print(f"     加分: {best_sharpe['bonuses']}")

best_ret = result_df.nlargest(1, 'total_ret').iloc[0]
print(f"\n  最优组合(收益):   累计{best_ret['total_ret']:+.1f}% | 年化{best_ret['annual_ret']:+.1f}% | Sh={best_ret['sharpe']:.2f} | DD={best_ret['max_dd']:.1f}% | R/D={best_ret['ret_dd_ratio']:.2f}")
print(f"     过滤: {best_ret['filters']}")
print(f"     打分: {best_ret['scores']}")
print(f"     加分: {best_ret['bonuses']}")

# 保存
result_df.to_csv(f'{OUTPUT_DIR}/combo_search_full.csv', index=False)
print(f"\n\n✅ 结果已保存到 {OUTPUT_DIR}/combo_search_full.csv")
print("="*70)
