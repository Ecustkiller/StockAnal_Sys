#!/usr/bin/env python3
"""
精选模式回测 — 小资金只选5-10只股票的最优策略测试
================================================================
对比以下策略：
  A. 简单TOP5/TOP10（按总分排序取前N）
  B. 精选TOP5/TOP10（行业分散 + 非涨停优先 + 维度均衡 + 安全边际）
  C. 精选TOP5/TOP10 + WR2/Mistery过滤
  D. 精选TOP5/TOP10 + 主板筛选（00/60开头）

基于已有的TOP30全量回测detail数据，模拟每天从TOP30中精选5-10只
"""

import pandas as pd
import numpy as np
from collections import defaultdict

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_180427.csv'

# ============================================================
# 工具函数
# ============================================================

def calc_dimension_balance(row):
    """
    计算维度均衡度得分（0-100）
    5个维度都有分 > 某一维度极高其他很低
    """
    dims = []
    # 各维度归一化到0-1
    if 'd1' in row: dims.append(row['d1'] / 15)  # 多周期共振 满分15
    if 'd2' in row: dims.append(row['d2'] / 25)  # 主线热点 满分25（原20，但数据中可能是25）
    if 'd3' in row: dims.append(row['d3'] / 47)  # 三Skill 满分47
    if 'd4' in row: dims.append(row['d4'] / 15)  # 安全边际 满分15
    if 'd5' in row: dims.append(row['d5'] / 15)  # 基本面 满分15
    
    if not dims:
        return 0
    
    # 均衡度 = 1 - 标准差/均值（变异系数的反面）
    mean_val = np.mean(dims)
    if mean_val == 0:
        return 0
    cv = np.std(dims) / mean_val  # 变异系数
    balance = max(0, 1 - cv) * 100  # 越均衡越高
    return balance


def calc_elite_score(row):
    """
    计算精选得分 = 总分权重(50%) + 维度均衡度(20%) + 风险扣分少(15%) + 资金流入(15%)
    """
    total_norm = row['total'] / 150 * 100  # 归一化到0-100
    balance = calc_dimension_balance(row)
    
    # 风险得分：净风险=0最好，越高越差
    risk_score = max(0, 100 - row.get('net_risk', 0) * 10)
    
    # 资金得分：净流入越多越好
    nb = row.get('nb_yi', 0)
    if pd.isna(nb): nb = 0
    fund_score = min(100, max(0, 50 + nb * 10))  # 0亿=50分，+5亿=100分，-5亿=0分
    
    elite = total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15
    return elite


def select_simple_topn(day_df, n):
    """策略A：简单按总分排序取TOP-N"""
    return day_df.nlargest(n, 'total')


def select_elite_topn(day_df, n, max_per_ind=2):
    """
    策略B：精选模式
    1. 优先非涨停票
    2. 行业分散（同行业最多max_per_ind只）
    3. 按精选得分排序
    """
    # 计算精选得分
    day_df = day_df.copy()
    day_df['elite_score'] = day_df.apply(calc_elite_score, axis=1)
    
    # 非涨停优先：给非涨停票加分
    day_df['elite_score_adj'] = day_df['elite_score']
    day_df.loc[day_df['is_zt'] == False, 'elite_score_adj'] += 10  # 非涨停加10分
    
    # 按调整后的精选得分排序
    day_df = day_df.sort_values('elite_score_adj', ascending=False)
    
    # 行业分散选股
    selected = []
    ind_count = defaultdict(int)
    
    for _, row in day_df.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= max_per_ind:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= n:
            break
    
    return pd.DataFrame(selected)


def select_elite_wr2(day_df, n, max_per_ind=2):
    """
    策略C：精选模式 + WR2/Mistery过滤
    在精选模式基础上，优先选WR2≥3或Mistery≥12的票
    """
    day_df = day_df.copy()
    day_df['elite_score'] = day_df.apply(calc_elite_score, axis=1)
    
    # 非涨停优先
    day_df['elite_score_adj'] = day_df['elite_score']
    day_df.loc[day_df['is_zt'] == False, 'elite_score_adj'] += 10
    
    # WR2≥3或Mistery≥12加分
    day_df.loc[day_df['wr2'] >= 3, 'elite_score_adj'] += 15
    day_df.loc[day_df['wr2'] >= 4, 'elite_score_adj'] += 10  # 额外加分
    day_df.loc[day_df['mistery'] >= 12, 'elite_score_adj'] += 12
    day_df.loc[day_df['mistery'] >= 15, 'elite_score_adj'] += 8  # 额外加分
    
    # 安全边际加分
    day_df.loc[day_df['d4'] >= 10, 'elite_score_adj'] += 5
    
    day_df = day_df.sort_values('elite_score_adj', ascending=False)
    
    selected = []
    ind_count = defaultdict(int)
    
    for _, row in day_df.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= max_per_ind:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= n:
            break
    
    return pd.DataFrame(selected)


def select_elite_mainboard(day_df, n, max_per_ind=2):
    """
    策略D：精选模式 + 主板筛选（00/60开头）
    """
    # 先筛选主板
    main_mask = day_df['code'].str[:3].isin(['600', '601', '603', '605', '000', '001', '002', '003'])
    main_df = day_df[main_mask].copy()
    
    if len(main_df) == 0:
        return pd.DataFrame()
    
    return select_elite_wr2(main_df, n, max_per_ind)


def select_elite_strict(day_df, n, max_per_ind=2):
    """
    策略E：严格精选（非涨停 + WR2≥3或Mistery≥10 + 5日涨<15% + 行业分散）
    """
    day_df = day_df.copy()
    
    # 硬性过滤
    mask = (day_df['is_zt'] == False)  # 非涨停
    mask &= (day_df['r5'] < 15)  # 5日涨幅<15%
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))  # WR2≥3或Mistery≥10
    
    filtered = day_df[mask].copy()
    
    if len(filtered) == 0:
        # 放宽条件
        mask2 = (day_df['is_zt'] == False) & (day_df['r5'] < 20)
        filtered = day_df[mask2].copy()
    
    if len(filtered) == 0:
        return pd.DataFrame()
    
    filtered['elite_score'] = filtered.apply(calc_elite_score, axis=1)
    
    # 额外加分
    filtered['elite_score_adj'] = filtered['elite_score']
    filtered.loc[filtered['wr2'] >= 4, 'elite_score_adj'] += 15
    filtered.loc[filtered['mistery'] >= 12, 'elite_score_adj'] += 12
    filtered.loc[filtered['d4'] >= 10, 'elite_score_adj'] += 8
    filtered.loc[filtered['nb_yi'] > 0, 'elite_score_adj'] += 5
    
    filtered = filtered.sort_values('elite_score_adj', ascending=False)
    
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


def backtest_strategy(df, strategy_func, n, label, hold_period='ret_1d', hold_days=1):
    """
    对一个策略进行回测
    每天从TOP30中用strategy_func选出n只，计算等权收益
    
    hold_days: 持有天数，1=每天换仓，3=每3天换仓，5=每5天换仓
               当hold_days>1时，每隔hold_days天才重新选股换仓
               ret_Xd 是X天的累计收益，需要折算为日均收益来计算净值
    """
    dates = sorted(df['date'].unique())
    daily_returns = []  # 每个换仓周期的收益
    daily_counts = []
    daily_details = []
    
    i = 0
    while i < len(dates):
        date = dates[i]
        day_df = df[df['date'] == date].copy()
        if len(day_df) == 0:
            i += 1
            continue
        
        selected = strategy_func(day_df, n)
        
        if len(selected) == 0:
            i += 1
            continue
        
        # 计算等权平均收益
        valid_rets = selected[hold_period].dropna()
        if len(valid_rets) == 0:
            i += 1
            continue
        
        avg_ret = valid_rets.mean()
        win_count = (valid_rets > 0).sum()
        
        daily_returns.append(avg_ret)
        daily_counts.append(len(valid_rets))
        daily_details.append({
            'date': date,
            'n_stocks': len(valid_rets),
            'avg_ret': avg_ret,
            'win_count': win_count,
            'win_rate': win_count / len(valid_rets) * 100,
            'n_industries': selected['industry'].nunique() if 'industry' in selected.columns else 0,
            'n_non_zt': (selected['is_zt'] == False).sum(),
        })
        
        # 跳过hold_days天（模拟持有期间不换仓）
        i += hold_days
    
    if not daily_returns:
        return None
    
    # 计算累计收益
    cum = 1.0
    peak = 1.0
    max_dd = 0
    cum_series = []
    
    for r in daily_returns:
        cum *= (1 + r / 100)
        cum_series.append(cum)
        if cum > peak:
            peak = cum
        dd = (peak - cum) / peak * 100
        if dd > max_dd:
            max_dd = dd
    
    rets = np.array(daily_returns)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.01
    profit_ratio = avg_win / avg_loss if avg_loss > 0 else 99
    
    # 年化收益：n_periods个换仓周期，每个周期hold_days天
    n_periods = len(daily_returns)
    total_trading_days = n_periods * hold_days
    annual_return = ((cum) ** (250 / total_trading_days) - 1) * 100 if total_trading_days > 0 else 0
    
    # 月度统计
    details_df = pd.DataFrame(daily_details)
    details_df['month'] = details_df['date'].astype(str).str[:6]
    
    return {
        'label': label,
        'n_days': n_periods,  # 换仓次数
        'total_trading_days': total_trading_days,  # 总交易日
        'hold_days': hold_days,
        'avg_stocks': np.mean(daily_counts),
        'avg_ret': rets.mean(),
        'median_ret': np.median(rets),
        'win_rate': (rets > 0).sum() / len(rets) * 100,
        'profit_ratio': profit_ratio,
        'cum_return': (cum - 1) * 100,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'sharpe': rets.mean() / rets.std() * np.sqrt(250 / hold_days) if rets.std() > 0 else 0,
        'avg_industries': details_df['n_industries'].mean() if 'n_industries' in details_df else 0,
        'avg_non_zt_pct': details_df['n_non_zt'].mean() / details_df['n_stocks'].mean() * 100 if details_df['n_stocks'].mean() > 0 else 0,
        'positive_days': (rets > 0).sum(),
        'negative_days': (rets <= 0).sum(),
        'positive_pct': (rets > 0).sum() / len(rets) * 100,
        'best_day': rets.max(),
        'worst_day': rets.min(),
        'details': details_df,
    }


def print_result(r):
    """打印单个策略结果"""
    if r is None:
        print("  ❌ 无有效数据")
        return
    
    tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
    print(f"  {tag} {r['label']}")
    hold_info = f" | 持有{r.get('hold_days', 1)}天/次" if r.get('hold_days', 1) > 1 else ""
    print(f"     换仓次数: {r['n_days']}次{hold_info} | 日均持股: {r['avg_stocks']:.1f}只 | 覆盖行业: {r['avg_industries']:.1f}个 | 非涨停占比: {r['avg_non_zt_pct']:.0f}%")
    print(f"     日均收益: {r['avg_ret']:+.3f}% | 中位数: {r['median_ret']:+.3f}% | 胜率: {r['win_rate']:.1f}%")
    print(f"     盈亏比: {r['profit_ratio']:.2f} | Sharpe: {r['sharpe']:.2f}")
    print(f"     累计收益: {r['cum_return']:+.1f}% | 年化: {r['annual_return']:+.1f}% | 最大回撤: -{r['max_dd']:.1f}%")
    print(f"     正收益天数: {r['positive_days']}/{r['n_days']} ({r['positive_pct']:.1f}%)")
    print(f"     最好一天: {r['best_day']:+.2f}% | 最差一天: {r['worst_day']:+.2f}%")
    print()


# ============================================================
# 主程序
# ============================================================
def main():
    print("=" * 100)
    print("📊 精选模式回测 — 小资金5-10只股票最优策略测试")
    print("=" * 100)
    
    # 加载数据
    df = pd.read_csv(DETAIL_FILE)
    df = df[df['ret_1d'].notna()].copy()
    print(f"\n数据: {len(df)}条样本, {df['date'].nunique()}个交易日")
    print(f"区间: {df['date'].min()} ~ {df['date'].max()}")
    
    # ============================================================
    # 一、T+1持有期对比
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 一、T+1持有期 — 各策略对比")
    print(f"{'=' * 100}")
    
    strategies_5 = [
        (lambda d, n: select_simple_topn(d, n), 5, "A1. 简单TOP5（按总分）"),
        (lambda d, n: select_elite_topn(d, n), 5, "B1. 精选TOP5（行业分散+非涨停优先）"),
        (lambda d, n: select_elite_wr2(d, n), 5, "C1. 精选TOP5 + WR2/Mistery加权"),
        (lambda d, n: select_elite_mainboard(d, n), 5, "D1. 精选TOP5 + 主板筛选"),
        (lambda d, n: select_elite_strict(d, n), 5, "E1. 严格精选TOP5（非涨停+WR2/Mistery+5日<15%）"),
    ]
    
    strategies_10 = [
        (lambda d, n: select_simple_topn(d, n), 10, "A2. 简单TOP10（按总分）"),
        (lambda d, n: select_elite_topn(d, n), 10, "B2. 精选TOP10（行业分散+非涨停优先）"),
        (lambda d, n: select_elite_wr2(d, n), 10, "C2. 精选TOP10 + WR2/Mistery加权"),
        (lambda d, n: select_elite_mainboard(d, n), 10, "D2. 精选TOP10 + 主板筛选"),
        (lambda d, n: select_elite_strict(d, n), 10, "E2. 严格精选TOP10（非涨停+WR2/Mistery+5日<15%）"),
    ]
    
    # 额外对比：简单TOP20/TOP30作为基准
    strategies_base = [
        (lambda d, n: select_simple_topn(d, n), 20, "基准: 简单TOP20"),
        (lambda d, n: select_simple_topn(d, n), 30, "基准: 简单TOP30"),
    ]
    
    all_results = {}
    
    print("\n--- TOP5 策略组 ---")
    for func, n, label in strategies_5:
        r = backtest_strategy(df, func, n, label, 'ret_1d')
        print_result(r)
        if r: all_results[label] = r
    
    print("\n--- TOP10 策略组 ---")
    for func, n, label in strategies_10:
        r = backtest_strategy(df, func, n, label, 'ret_1d')
        print_result(r)
        if r: all_results[label] = r
    
    print("\n--- 基准对比 ---")
    for func, n, label in strategies_base:
        r = backtest_strategy(df, func, n, label, 'ret_1d')
        print_result(r)
        if r: all_results[label] = r
    
    # ============================================================
    # 二、T+2持有期对比（WR2最佳持有期）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 二、T+2持有期 — 各策略对比（每2天换仓）")
    print(f"{'=' * 100}")
    
    for func, n, label in strategies_5 + strategies_10:
        label_t2 = label.replace(".", "(T+2).")
        r = backtest_strategy(df, func, n, label_t2, 'ret_2d', hold_days=2)
        if r:
            tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
            print(f"  {tag} {label_t2}: 累计{r['cum_return']:+.1f}% 年化{r['annual_return']:+.1f}% 胜率{r['win_rate']:.1f}% 盈亏比{r['profit_ratio']:.2f} 回撤-{r['max_dd']:.1f}%")
    
    # ============================================================
    # 二-B、T+3持有期对比（持有3天）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 二-B、T+3持有期 — 各策略对比（每3天换仓）")
    print(f"{'=' * 100}")
    
    t3_results = {}
    for func, n, label in strategies_5 + strategies_10:
        label_t3 = label.replace(".", "(T+3).")
        r = backtest_strategy(df, func, n, label_t3, 'ret_3d', hold_days=3)
        if r:
            tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
            print(f"  {tag} {label_t3}: 累计{r['cum_return']:+.1f}% 年化{r['annual_return']:+.1f}% 胜率{r['win_rate']:.1f}% 盈亏比{r['profit_ratio']:.2f} 回撤-{r['max_dd']:.1f}% Sharpe{r['sharpe']:.2f}")
            t3_results[label_t3] = r
    
    # ============================================================
    # 二-C、T+5持有期对比（持有5天）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 二-C、T+5持有期 — 各策略对比（每5天换仓，周频换仓）")
    print(f"{'=' * 100}")
    
    t5_results = {}
    for func, n, label in strategies_5 + strategies_10:
        label_t5 = label.replace(".", "(T+5).")
        r = backtest_strategy(df, func, n, label_t5, 'ret_5d', hold_days=5)
        if r:
            tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
            print(f"  {tag} {label_t5}: 累计{r['cum_return']:+.1f}% 年化{r['annual_return']:+.1f}% 胜率{r['win_rate']:.1f}% 盈亏比{r['profit_ratio']:.2f} 回撤-{r['max_dd']:.1f}% Sharpe{r['sharpe']:.2f}")
            t5_results[label_t5] = r
    
    # ============================================================
    # 三、T+1汇总排名
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 三、T+1策略综合排名（按累计收益）")
    print(f"{'=' * 100}")
    
    sorted_results = sorted(all_results.values(), key=lambda x: x['cum_return'], reverse=True)
    
    print(f"\n{'排名':>4} {'策略':<50} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8} {'日均股':>6}")
    print("-" * 120)
    
    for i, r in enumerate(sorted_results, 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<50} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}% {r['avg_stocks']:>5.1f}")
    
    # ============================================================
    # 三-B、跨持有期综合排名（T+1 / T+3 / T+5 最优策略对比）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 三-B、⭐ 跨持有期综合排名 — T+1 vs T+3 vs T+5 最优策略对比")
    print(f"{'=' * 100}")
    
    # 收集所有持有期的结果
    cross_results = []
    
    # T+1 结果
    for func, n, label in strategies_5 + strategies_10:
        r = backtest_strategy(df, func, n, f"[T+1] {label}", 'ret_1d', hold_days=1)
        if r: cross_results.append(r)
    
    # T+2 结果
    for func, n, label in strategies_5 + strategies_10:
        r = backtest_strategy(df, func, n, f"[T+2] {label}", 'ret_2d', hold_days=2)
        if r: cross_results.append(r)
    
    # T+3 结果
    for func, n, label in strategies_5 + strategies_10:
        r = backtest_strategy(df, func, n, f"[T+3] {label}", 'ret_3d', hold_days=3)
        if r: cross_results.append(r)
    
    # T+5 结果
    for func, n, label in strategies_5 + strategies_10:
        r = backtest_strategy(df, func, n, f"[T+5] {label}", 'ret_5d', hold_days=5)
        if r: cross_results.append(r)
    
    # 按累计收益排名
    cross_sorted = sorted(cross_results, key=lambda x: x['cum_return'], reverse=True)
    
    print(f"\n{'排名':>4} {'策略':<60} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8} {'换仓频率':>8}")
    print("-" * 140)
    
    for i, r in enumerate(cross_sorted[:30], 1):  # 只显示前30
        tag = '🏆' if i <= 3 else ('⭐' if i <= 5 else '  ')
        freq = f"每{r['hold_days']}天" if r['hold_days'] > 1 else "每天"
        print(f"{tag}{i:>3} {r['label']:<60} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}% {freq:>8}")
    
    # 按Sharpe排名
    cross_sharpe = sorted(cross_results, key=lambda x: x['sharpe'], reverse=True)
    
    print(f"\n--- 按Sharpe排名（风险调整后收益）TOP15 ---")
    print(f"\n{'排名':>4} {'策略':<60} {'Sharpe':>7} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'回撤':>8}")
    print("-" * 130)
    
    for i, r in enumerate(cross_sharpe[:15], 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<60} {r['sharpe']:>6.2f} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} -{r['max_dd']:>6.1f}%")
    
    # 按收益/回撤比排名
    cross_ratio = sorted(cross_results, key=lambda x: x['cum_return'] / max(x['max_dd'], 0.1), reverse=True)
    
    print(f"\n--- 按收益/回撤比排名 TOP15 ---")
    print(f"\n{'排名':>4} {'策略':<60} {'收益/回撤':>10} {'累计收益':>10} {'回撤':>8} {'年化':>8} {'Sharpe':>7}")
    print("-" * 130)
    
    for i, r in enumerate(cross_ratio[:15], 1):
        tag = '🏆' if i <= 3 else '  '
        ratio = r['cum_return'] / max(r['max_dd'], 0.1)
        print(f"{tag}{i:>3} {r['label']:<60} {ratio:>9.2f} {r['cum_return']:>+9.1f}% -{r['max_dd']:>6.1f}% {r['annual_return']:>+7.1f}% {r['sharpe']:>6.2f}")
    
    # ============================================================
    # 三-C、同策略不同持有期对比表
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 三-C、⭐ 同策略不同持有期对比（核心对比表）")
    print(f"{'=' * 100}")
    
    # 按策略名分组
    strategy_labels_5 = [label for _, _, label in strategies_5]
    strategy_labels_10 = [label for _, _, label in strategies_10]
    hold_periods_map = {
        'T+1': ('ret_1d', 1),
        'T+2': ('ret_2d', 2),
        'T+3': ('ret_3d', 3),
        'T+5': ('ret_5d', 5),
    }
    
    for group_name, group_labels, group_strategies in [
        ('TOP5策略组', strategy_labels_5, strategies_5),
        ('TOP10策略组', strategy_labels_10, strategies_10),
    ]:
        print(f"\n--- {group_name} ---")
        print(f"{'策略':<45} {'持有期':>6} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8} {'收益/回撤':>10}")
        print("-" * 130)
        
        for func, n, label in group_strategies:
            for hp_name, (hp_col, hp_days) in hold_periods_map.items():
                r = backtest_strategy(df, func, n, label, hp_col, hold_days=hp_days)
                if r:
                    ratio = r['cum_return'] / max(r['max_dd'], 0.1)
                    best_marker = ''
                    print(f"{label:<45} {hp_name:>6} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}% {ratio:>9.2f}")
            print()  # 策略间空行
    
    # ============================================================
    # 四、月度对比（TOP5简单 vs TOP5精选WR2）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 四、月度收益对比（简单TOP5 vs 精选TOP5+WR2）")
    print(f"{'=' * 100}")
    
    key_a = "A1. 简单TOP5（按总分）"
    key_c = "C1. 精选TOP5 + WR2/Mistery加权"
    
    if key_a in all_results and key_c in all_results:
        da = all_results[key_a]['details']
        dc = all_results[key_c]['details']
        
        months_a = da.groupby('month')['avg_ret'].mean()
        months_c = dc.groupby('month')['avg_ret'].mean()
        
        all_months = sorted(set(months_a.index) | set(months_c.index))
        
        print(f"\n{'月份':<10} {'简单TOP5':>10} {'精选TOP5+WR2':>14} {'差值':>10} {'胜出':>8}")
        print("-" * 60)
        
        simple_wins = 0
        elite_wins = 0
        
        for m in all_months:
            a_val = months_a.get(m, 0)
            c_val = months_c.get(m, 0)
            diff = c_val - a_val
            winner = "精选✅" if diff > 0 else ("简单" if diff < 0 else "平")
            if diff > 0: elite_wins += 1
            elif diff < 0: simple_wins += 1
            print(f"{m:<10} {a_val:>+9.3f}% {c_val:>+13.3f}% {diff:>+9.3f}% {winner:>8}")
        
        print(f"\n精选胜出: {elite_wins}个月 | 简单胜出: {simple_wins}个月")
    
    # ============================================================
    # 五、行业分散度分析
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 五、行业分散度分析")
    print(f"{'=' * 100}")
    
    for label in ["A1. 简单TOP5（按总分）", "C1. 精选TOP5 + WR2/Mistery加权", 
                   "A2. 简单TOP10（按总分）", "C2. 精选TOP10 + WR2/Mistery加权"]:
        if label in all_results:
            r = all_results[label]
            d = r['details']
            print(f"\n  {label}:")
            print(f"    日均覆盖行业: {d['n_industries'].mean():.1f}个")
            print(f"    日均非涨停: {d['n_non_zt'].mean():.1f}只 ({d['n_non_zt'].mean()/d['n_stocks'].mean()*100:.0f}%)")
            print(f"    行业≥3个的天数: {(d['n_industries']>=3).sum()}/{len(d)} ({(d['n_industries']>=3).mean()*100:.1f}%)")
    
    # ============================================================
    # 六、不同行业上限的影响
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 六、行业集中度上限对比（精选TOP10）")
    print(f"{'=' * 100}")
    
    for max_ind in [1, 2, 3, 5]:
        func = lambda d, n, mi=max_ind: select_elite_wr2(d, n, max_per_ind=mi)
        r = backtest_strategy(df, func, 10, f"精选TOP10 同行业≤{max_ind}只", 'ret_1d')
        if r:
            tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
            print(f"  {tag} 同行业≤{max_ind}只: 累计{r['cum_return']:+.1f}% 年化{r['annual_return']:+.1f}% 胜率{r['win_rate']:.1f}% 盈亏比{r['profit_ratio']:.2f} 回撤-{r['max_dd']:.1f}% 日均行业{r['avg_industries']:.1f}个")
    
    # ============================================================
    # 七、核心结论
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 七、核心结论")
    print(f"{'=' * 100}")
    
    if sorted_results:
        best = sorted_results[0]
        print(f"\n  🥇 T+1最优策略: {best['label']}")
        print(f"     累计收益: {best['cum_return']:+.1f}% | 年化: {best['annual_return']:+.1f}% | 胜率: {best['win_rate']:.1f}% | 盈亏比: {best['profit_ratio']:.2f}")
        print(f"     最大回撤: -{best['max_dd']:.1f}% | Sharpe: {best['sharpe']:.2f}")
    
    # 找TOP5中最好的
    top5_results = [r for r in sorted_results if 'TOP5' in r['label'] and '基准' not in r['label']]
    top10_results = [r for r in sorted_results if 'TOP10' in r['label'] and '基准' not in r['label']]
    
    if top5_results:
        best5 = top5_results[0]
        simple5 = next((r for r in sorted_results if r['label'] == "A1. 简单TOP5（按总分）"), None)
        print(f"\n  📌 TOP5最优: {best5['label']}")
        if simple5:
            diff = best5['cum_return'] - simple5['cum_return']
            print(f"     vs 简单TOP5: 累计收益差 {diff:+.1f}%")
    
    if top10_results:
        best10 = top10_results[0]
        simple10 = next((r for r in sorted_results if r['label'] == "A2. 简单TOP10（按总分）"), None)
        print(f"\n  📌 TOP10最优: {best10['label']}")
        if simple10:
            diff = best10['cum_return'] - simple10['cum_return']
            print(f"     vs 简单TOP10: 累计收益差 {diff:+.1f}%")
    
    # 跨持有期最优
    if cross_sorted:
        print(f"\n  {'=' * 80}")
        print(f"  ⭐ 全局最优（跨持有期）:")
        
        best_all = cross_sorted[0]
        print(f"\n  🥇 累计收益最高: {best_all['label']}")
        print(f"     累计收益: {best_all['cum_return']:+.1f}% | 年化: {best_all['annual_return']:+.1f}% | Sharpe: {best_all['sharpe']:.2f} | 回撤: -{best_all['max_dd']:.1f}%")
        
        best_sharpe_all = cross_sharpe[0]
        print(f"\n  🥇 Sharpe最高: {best_sharpe_all['label']}")
        print(f"     Sharpe: {best_sharpe_all['sharpe']:.2f} | 累计收益: {best_sharpe_all['cum_return']:+.1f}% | 回撤: -{best_sharpe_all['max_dd']:.1f}%")
        
        best_ratio_all = cross_ratio[0]
        ratio_val = best_ratio_all['cum_return'] / max(best_ratio_all['max_dd'], 0.1)
        print(f"\n  🥇 收益/回撤比最高: {best_ratio_all['label']}")
        print(f"     收益/回撤: {ratio_val:.2f} | 累计收益: {best_ratio_all['cum_return']:+.1f}% | 回撤: -{best_ratio_all['max_dd']:.1f}%")
        
        # 找各持有期的最优
        for hp_tag in ['[T+1]', '[T+2]', '[T+3]', '[T+5]']:
            hp_results = [r for r in cross_sorted if r['label'].startswith(hp_tag)]
            if hp_results:
                best_hp = hp_results[0]
                print(f"\n  📌 {hp_tag}最优: {best_hp['label']}")
                print(f"     累计: {best_hp['cum_return']:+.1f}% | 年化: {best_hp['annual_return']:+.1f}% | 胜率: {best_hp['win_rate']:.1f}% | 盈亏比: {best_hp['profit_ratio']:.2f} | Sharpe: {best_hp['sharpe']:.2f} | 回撤: -{best_hp['max_dd']:.1f}%")
    
    # ============================================================
    # 八、🏛️ 主板票专项回测（仅00/60开头）
    # ============================================================
    print(f"\n{'=' * 100}")
    print("📊 八、🏛️ 主板票专项回测 — 仅保留00/60开头股票后各策略表现")
    print(f"{'=' * 100}")
    
    # 先过滤主板票
    main_mask = df['code'].str[:3].isin(['600', '601', '603', '605', '000', '001', '002', '003'])
    df_main = df[main_mask].copy()
    print(f"\n  主板票过滤: {len(df)}条 → {len(df_main)}条 ({len(df_main)/len(df)*100:.1f}%)")
    print(f"  主板票交易日: {df_main['date'].nunique()}个")
    
    # 在主板票池上定义策略（注意：这里所有策略都在主板票池上运行）
    # 策略A-E在主板票池上的表现
    mb_strategies_5 = [
        (lambda d, n: select_simple_topn(d, n), 5, "A1🏛️ 简单TOP5（主板池）"),
        (lambda d, n: select_elite_topn(d, n), 5, "B1🏛️ 精选TOP5（主板池+行业分散）"),
        (lambda d, n: select_elite_wr2(d, n), 5, "C1🏛️ 精选TOP5+WR2（主板池）"),
        (lambda d, n: select_elite_strict(d, n), 5, "E1🏛️ 严格精选TOP5（主板池）"),
    ]
    
    mb_strategies_10 = [
        (lambda d, n: select_simple_topn(d, n), 10, "A2🏛️ 简单TOP10（主板池）"),
        (lambda d, n: select_elite_topn(d, n), 10, "B2🏛️ 精选TOP10（主板池+行业分散）"),
        (lambda d, n: select_elite_wr2(d, n), 10, "C2🏛️ 精选TOP10+WR2（主板池）"),
        (lambda d, n: select_elite_strict(d, n), 10, "E2🏛️ 严格精选TOP10（主板池）"),
    ]
    
    hold_periods_map = {
        'T+1': ('ret_1d', 1),
        'T+2': ('ret_2d', 2),
        'T+3': ('ret_3d', 3),
        'T+5': ('ret_5d', 5),
    }
    
    # 八-A：主板票各策略 × 各持有期 完整对比表
    print(f"\n--- 八-A：主板票 TOP5策略组 × 各持有期 ---")
    print(f"{'策略':<45} {'持有期':>6} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8} {'收益/回撤':>10}")
    print("-" * 130)
    
    mb_all_results = []
    
    for func, n, label in mb_strategies_5:
        for hp_name, (hp_col, hp_days) in hold_periods_map.items():
            r = backtest_strategy(df_main, func, n, f"[{hp_name}] {label}", hp_col, hold_days=hp_days)
            if r:
                ratio = r['cum_return'] / max(r['max_dd'], 0.1)
                tag = '🏆' if r['cum_return'] > 100 else ('✅' if r['cum_return'] > 0 else '❌')
                print(f"{tag} {label:<43} {hp_name:>6} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}% {ratio:>9.2f}")
                mb_all_results.append(r)
        print()
    
    print(f"\n--- 八-B：主板票 TOP10策略组 × 各持有期 ---")
    print(f"{'策略':<45} {'持有期':>6} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8} {'收益/回撤':>10}")
    print("-" * 130)
    
    for func, n, label in mb_strategies_10:
        for hp_name, (hp_col, hp_days) in hold_periods_map.items():
            r = backtest_strategy(df_main, func, n, f"[{hp_name}] {label}", hp_col, hold_days=hp_days)
            if r:
                ratio = r['cum_return'] / max(r['max_dd'], 0.1)
                tag = '🏆' if r['cum_return'] > 100 else ('✅' if r['cum_return'] > 0 else '❌')
                print(f"{tag} {label:<43} {hp_name:>6} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}% {ratio:>9.2f}")
                mb_all_results.append(r)
        print()
    
    # 八-C：主板票综合排名
    print(f"\n--- 八-C：🏛️ 主板票综合排名（按累计收益）TOP20 ---")
    mb_sorted = sorted(mb_all_results, key=lambda x: x['cum_return'], reverse=True)
    
    print(f"\n{'排名':>4} {'策略':<60} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'回撤':>8}")
    print("-" * 130)
    
    for i, r in enumerate(mb_sorted[:20], 1):
        tag = '🏆' if i <= 3 else ('⭐' if i <= 5 else '  ')
        print(f"{tag}{i:>3} {r['label']:<60} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} {r['sharpe']:>6.2f} -{r['max_dd']:>6.1f}%")
    
    # 按Sharpe排名
    mb_sharpe = sorted(mb_all_results, key=lambda x: x['sharpe'], reverse=True)
    print(f"\n--- 八-D：🏛️ 主板票按Sharpe排名 TOP10 ---")
    print(f"\n{'排名':>4} {'策略':<60} {'Sharpe':>7} {'累计收益':>10} {'年化':>8} {'回撤':>8} {'收益/回撤':>10}")
    print("-" * 130)
    
    for i, r in enumerate(mb_sharpe[:10], 1):
        tag = '🏆' if i <= 3 else '  '
        ratio = r['cum_return'] / max(r['max_dd'], 0.1)
        print(f"{tag}{i:>3} {r['label']:<60} {r['sharpe']:>6.2f} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% -{r['max_dd']:>6.1f}% {ratio:>9.2f}")
    
    # 按收益/回撤比排名
    mb_ratio = sorted(mb_all_results, key=lambda x: x['cum_return'] / max(x['max_dd'], 0.1), reverse=True)
    print(f"\n--- 八-E：🏛️ 主板票按收益/回撤比排名 TOP10 ---")
    print(f"\n{'排名':>4} {'策略':<60} {'收益/回撤':>10} {'累计收益':>10} {'回撤':>8} {'Sharpe':>7}")
    print("-" * 130)
    
    for i, r in enumerate(mb_ratio[:10], 1):
        tag = '🏆' if i <= 3 else '  '
        ratio = r['cum_return'] / max(r['max_dd'], 0.1)
        print(f"{tag}{i:>3} {r['label']:<60} {ratio:>9.2f} {r['cum_return']:>+9.1f}% -{r['max_dd']:>6.1f}% {r['sharpe']:>6.2f}")
    
    # 八-F：主板 vs 全市场对比
    print(f"\n--- 八-F：🏛️ 主板 vs 全市场 关键策略对比 ---")
    print(f"{'策略':<35} {'市场':>6} {'持有期':>6} {'累计收益':>10} {'年化':>8} {'胜率':>7} {'Sharpe':>7} {'回撤':>8}")
    print("-" * 110)
    
    compare_strategies = [
        (select_simple_topn, 5, "简单TOP5"),
        (select_elite_wr2, 5, "精选TOP5+WR2"),
        (select_elite_strict, 5, "严格精选TOP5"),
        (select_simple_topn, 10, "简单TOP10"),
        (select_elite_wr2, 10, "精选TOP10+WR2"),
        (select_elite_strict, 10, "严格精选TOP10"),
    ]
    
    for func, n, short_label in compare_strategies:
        for hp_name, (hp_col, hp_days) in [('T+1', ('ret_1d', 1)), ('T+5', ('ret_5d', 5))]:
            # 全市场
            r_all = backtest_strategy(df, func, n, "全市场", hp_col, hold_days=hp_days)
            # 主板
            r_mb = backtest_strategy(df_main, func, n, "主板", hp_col, hold_days=hp_days)
            
            if r_all:
                print(f"{short_label:<35} {'全市场':>6} {hp_name:>6} {r_all['cum_return']:>+9.1f}% {r_all['annual_return']:>+7.1f}% {r_all['win_rate']:>6.1f}% {r_all['sharpe']:>6.2f} -{r_all['max_dd']:>6.1f}%")
            if r_mb:
                diff = r_mb['cum_return'] - (r_all['cum_return'] if r_all else 0)
                marker = '✅' if diff > 0 else '❌'
                print(f"{short_label:<35} {'主板🏛️':>6} {hp_name:>6} {r_mb['cum_return']:>+9.1f}% {r_mb['annual_return']:>+7.1f}% {r_mb['win_rate']:>6.1f}% {r_mb['sharpe']:>6.2f} -{r_mb['max_dd']:>6.1f}% {marker}({diff:+.1f}%)")
        print()
    
    # 八-G：主板票核心结论
    print(f"\n--- 八-G：🏛️ 主板票核心结论 ---")
    if mb_sorted:
        best_mb = mb_sorted[0]
        print(f"\n  🥇 主板票累计收益最高: {best_mb['label']}")
        print(f"     累计: {best_mb['cum_return']:+.1f}% | 年化: {best_mb['annual_return']:+.1f}% | Sharpe: {best_mb['sharpe']:.2f} | 回撤: -{best_mb['max_dd']:.1f}%")
    
    if mb_sharpe:
        best_mb_s = mb_sharpe[0]
        print(f"\n  🥇 主板票Sharpe最高: {best_mb_s['label']}")
        print(f"     Sharpe: {best_mb_s['sharpe']:.2f} | 累计: {best_mb_s['cum_return']:+.1f}% | 回撤: -{best_mb_s['max_dd']:.1f}%")
    
    if mb_ratio:
        best_mb_r = mb_ratio[0]
        ratio_val = best_mb_r['cum_return'] / max(best_mb_r['max_dd'], 0.1)
        print(f"\n  🥇 主板票收益/回撤比最高: {best_mb_r['label']}")
        print(f"     收益/回撤: {ratio_val:.2f} | 累计: {best_mb_r['cum_return']:+.1f}% | 回撤: -{best_mb_r['max_dd']:.1f}%")
    
    # 各持有期最优
    for hp_tag in ['[T+1]', '[T+2]', '[T+3]', '[T+5]']:
        hp_results = [r for r in mb_sorted if r['label'].startswith(hp_tag)]
        if hp_results:
            best_hp = hp_results[0]
            print(f"\n  📌 主板{hp_tag}最优: {best_hp['label']}")
            print(f"     累计: {best_hp['cum_return']:+.1f}% | 年化: {best_hp['annual_return']:+.1f}% | 胜率: {best_hp['win_rate']:.1f}% | Sharpe: {best_hp['sharpe']:.2f} | 回撤: -{best_hp['max_dd']:.1f}%")
    
    print(f"\n{'=' * 100}")
    print("✅ 精选模式回测完成！（含T+1/T+2/T+3/T+5持有期对比 + 主板票专项回测）")
    print(f"{'=' * 100}")


if __name__ == '__main__':
    main()
