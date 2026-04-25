#!/usr/bin/env python3
"""
backtest_v2 子策略独立回测
================================================================
基于 backtest_v2 已有的 detail CSV（每天TOP30的评分+收益数据），
测试各个子策略/维度的独立选股效果：

一、整体回测（TOP20/TOP30 基准）
二、6大维度独立选股（只按单一维度排序选TOP10）
三、8大子维度独立选股
四、组合策略（多维度加权）
五、条件过滤策略（硬性条件筛选）
六、净值曲线 & 最大回撤对比
七、月度收益对比
八、核心结论

数据来源: backtest_results/backtest_v2_detail_20260420_052244.csv
================================================================
"""

import pandas as pd
import numpy as np
from collections import defaultdict

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_052244.csv'


# ============================================================
# 回测引擎
# ============================================================

def backtest_by_selector(df, selector_func, label, hold_period='ret_1d', top_n=10):
    """
    通用回测函数
    selector_func(day_df) -> 返回选中的DataFrame（已排序）
    每天从TOP30中用selector选出top_n只，计算等权收益
    """
    dates = sorted(df['date'].unique())
    daily_returns = []
    daily_details = []

    for date in dates:
        day_df = df[df['date'] == date].copy()
        if len(day_df) == 0:
            continue

        selected = selector_func(day_df)
        if selected is None or len(selected) == 0:
            continue

        selected = selected.head(top_n)
        valid_rets = selected[hold_period].dropna()
        if len(valid_rets) == 0:
            continue

        avg_ret = valid_rets.mean()
        daily_returns.append(avg_ret)
        daily_details.append({
            'date': date,
            'n_stocks': len(valid_rets),
            'avg_ret': avg_ret,
            'win_rate': (valid_rets > 0).sum() / len(valid_rets) * 100,
        })

    if not daily_returns:
        return None

    rets = np.array(daily_returns)

    # 累计收益 & 最大回撤
    cum = 1.0
    peak = 1.0
    max_dd = 0
    nav_list = []
    for r in rets:
        cum *= (1 + r / 100)
        nav_list.append(cum)
        if cum > peak:
            peak = cum
        dd = (peak - cum) / peak * 100
        if dd > max_dd:
            max_dd = dd

    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.01
    profit_ratio = avg_win / avg_loss if avg_loss > 0 else 99

    n_days = len(rets)
    annual_return = ((cum) ** (250 / n_days) - 1) * 100 if n_days > 0 else 0

    # 月度收益
    details_df = pd.DataFrame(daily_details)
    details_df['month'] = details_df['date'].astype(str).str[:6]
    monthly_rets = details_df.groupby('month')['avg_ret'].sum().to_dict()

    return {
        'label': label,
        'n_days': n_days,
        'avg_stocks': np.mean([d['n_stocks'] for d in daily_details]),
        'cum_return': (cum - 1) * 100,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'win_rate': (rets > 0).sum() / len(rets) * 100,
        'profit_ratio': profit_ratio,
        'sharpe': rets.mean() / rets.std() * np.sqrt(250) if rets.std() > 0 else 0,
        'avg_daily_ret': rets.mean(),
        'median_daily_ret': np.median(rets),
        'best_day': rets.max(),
        'worst_day': rets.min(),
        'monthly_rets': monthly_rets,
        'nav_list': nav_list,
        'details_df': details_df,
    }


def print_result_line(r, rank=None):
    """打印单行结果"""
    if r is None:
        return
    tag = '🏆' if rank and rank <= 3 else '  '
    rk = f"{tag}{rank:>3}" if rank else "    "
    print(f"{rk} {r['label']:<55} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% "
          f"-{r['max_dd']:>6.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} "
          f"{r['sharpe']:>6.2f} {r['avg_daily_ret']:>+7.3f}% {r['avg_stocks']:>5.1f}")


def print_header():
    """打印表头"""
    print(f"{'排名':>4} {'策略':<55} {'累计收益':>10} {'年化':>8} {'回撤':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'日均收':>9} {'均股':>6}")
    print("-" * 145)


# ============================================================
# 选股函数定义
# ============================================================

# --- 基准策略 ---
def sel_total_top(day_df):
    """按总分排序"""
    return day_df.sort_values('total', ascending=False)

def sel_raw_total_top(day_df):
    """按原始总分（不含风险扣分）排序"""
    return day_df.sort_values('raw_total', ascending=False)

# --- 6大维度独立选股 ---
def sel_d1_top(day_df):
    """D1多周期共振"""
    return day_df.sort_values('d1', ascending=False)

def sel_d2_top(day_df):
    """D2主线热点"""
    return day_df.sort_values('d2', ascending=False)

def sel_d3_top(day_df):
    """D3三Skill"""
    return day_df.sort_values('d3', ascending=False)

def sel_d4_top(day_df):
    """D4安全边际"""
    return day_df.sort_values('d4', ascending=False)

def sel_d5_top(day_df):
    """D5基本面"""
    return day_df.sort_values('d5', ascending=False)

def sel_d9_top(day_df):
    """D9百胜WR"""
    return day_df.sort_values('d9', ascending=False)

# --- 8大子维度独立选股 ---
def sel_mistery_top(day_df):
    """Mistery"""
    return day_df.sort_values('mistery', ascending=False)

def sel_tds_top(day_df):
    """TDS"""
    return day_df.sort_values('tds', ascending=False)

def sel_yuanzi_top(day_df):
    """元子元"""
    return day_df.sort_values('yuanzi', ascending=False)

def sel_txcg_top(day_df):
    """TXCG六模型"""
    return day_df.sort_values('txcg', ascending=False)

def sel_wr1_top(day_df):
    """WR-1首板放量"""
    return day_df.sort_values('wr1', ascending=False)

def sel_wr2_top(day_df):
    """WR-2右侧起爆"""
    return day_df.sort_values('wr2', ascending=False)

def sel_wr3_top(day_df):
    """WR-3底倍量"""
    return day_df.sort_values('wr3', ascending=False)

def sel_bci_top(day_df):
    """BCI板块完整性"""
    return day_df.sort_values('bci', ascending=False)

# --- 组合策略 ---
def sel_d3_d9_combo(day_df):
    """D3+D9组合（三Skill+百胜WR）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d3'] / 47 * 50 + day_df['d9'] / 15 * 50
    return day_df.sort_values('combo', ascending=False)

def sel_mistery_wr2_combo(day_df):
    """Mistery+WR2组合"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['mistery'] / 20 * 50 + day_df['wr2'] / 5 * 50
    return day_df.sort_values('combo', ascending=False)

def sel_d2_d3_combo(day_df):
    """D2+D3组合（主线热点+三Skill）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d2'] / 25 * 50 + day_df['d3'] / 47 * 50
    return day_df.sort_values('combo', ascending=False)

def sel_d1_d3_combo(day_df):
    """D1+D3组合（多周期共振+三Skill）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d1'] / 15 * 50 + day_df['d3'] / 47 * 50
    return day_df.sort_values('combo', ascending=False)

def sel_d4_d3_combo(day_df):
    """D4+D3组合（安全边际+三Skill）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d4'] / 15 * 40 + day_df['d3'] / 47 * 60
    return day_df.sort_values('combo', ascending=False)

def sel_low_risk_high_skill(day_df):
    """低风险+高技术面（net_risk低+D3高）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d3'] / 47 * 60 + (1 - day_df['net_risk'] / 30) * 40
    return day_df.sort_values('combo', ascending=False)

def sel_balanced(day_df):
    """均衡策略（各维度归一化等权）"""
    day_df = day_df.copy()
    day_df['combo'] = (day_df['d1'] / 15 + day_df['d2'] / 25 + day_df['d3'] / 47 +
                       day_df['d4'] / 15 + day_df['d5'] / 15 + day_df['d9'] / 15) / 6 * 100
    return day_df.sort_values('combo', ascending=False)

def sel_momentum_quality(day_df):
    """动量+质量（D1趋势+D3技术+D5基本面）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d1'] / 15 * 30 + day_df['d3'] / 47 * 40 + day_df['d5'] / 15 * 30
    return day_df.sort_values('combo', ascending=False)

def sel_hotspot_skill(day_df):
    """热点+技术（D2主线+D3三Skill+D9百胜WR）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d2'] / 25 * 30 + day_df['d3'] / 47 * 40 + day_df['d9'] / 15 * 30
    return day_df.sort_values('combo', ascending=False)

# --- 条件过滤策略 ---
def sel_non_zt_top(day_df):
    """非涨停票按总分"""
    filtered = day_df[day_df['is_zt'] == False]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_zt_only(day_df):
    """仅涨停票按总分"""
    filtered = day_df[day_df['is_zt'] == True]
    if len(filtered) == 0:
        return None
    return filtered.sort_values('total', ascending=False)

def sel_high_bci(day_df):
    """BCI≥60的票按总分"""
    filtered = day_df[day_df['bci'] >= 60]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_low_risk(day_df):
    """低风险票（net_risk=0）按总分"""
    filtered = day_df[day_df['net_risk'] == 0]
    if len(filtered) < 3:
        filtered = day_df[day_df['net_risk'] <= 2]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_high_protect(day_df):
    """高保护因子（protect≥8）按总分"""
    filtered = day_df[day_df['protect'] >= 8]
    if len(filtered) < 3:
        filtered = day_df[day_df['protect'] >= 5]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_wr2_ge3(day_df):
    """WR2≥3的票按总分"""
    filtered = day_df[day_df['wr2'] >= 3]
    if len(filtered) < 3:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_mistery_ge12(day_df):
    """Mistery≥12的票按总分"""
    filtered = day_df[day_df['mistery'] >= 12]
    if len(filtered) < 3:
        filtered = day_df[day_df['mistery'] >= 10]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_safe_margin_high(day_df):
    """安全边际高（D4≥10）按总分"""
    filtered = day_df[day_df['d4'] >= 10]
    if len(filtered) < 3:
        filtered = day_df[day_df['d4'] >= 8]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_mainboard_only(day_df):
    """仅主板（00/60开头）按总分"""
    mask = day_df['code'].str[:3].isin(['600', '601', '603', '605', '000', '001', '002', '003'])
    filtered = day_df[mask]
    if len(filtered) == 0:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_fund_inflow(day_df):
    """资金净流入（nb_yi>0）按总分"""
    filtered = day_df[day_df['nb_yi'] > 0]
    if len(filtered) < 3:
        return day_df.sort_values('total', ascending=False)
    return filtered.sort_values('total', ascending=False)

def sel_elite_wr2_industry(day_df):
    """精选策略C（WR2/Mistery加权+行业分散）"""
    day_df = day_df.copy()
    # 计算精选得分
    total_norm = day_df['total'] / 150 * 100
    risk_score = (100 - day_df['net_risk'] * 10).clip(0, 100)
    nb = day_df['nb_yi'].fillna(0)
    fund_score = (50 + nb * 10).clip(0, 100)

    day_df['elite'] = total_norm * 0.50 + risk_score * 0.15 + fund_score * 0.15
    # 非涨停加分
    day_df.loc[day_df['is_zt'] == False, 'elite'] += 10
    # WR2/Mistery加权
    day_df.loc[day_df['wr2'] >= 3, 'elite'] += 15
    day_df.loc[day_df['wr2'] >= 4, 'elite'] += 10
    day_df.loc[day_df['mistery'] >= 12, 'elite'] += 12
    day_df.loc[day_df['mistery'] >= 15, 'elite'] += 8
    day_df.loc[day_df['d4'] >= 10, 'elite'] += 5

    day_df = day_df.sort_values('elite', ascending=False)

    # 行业分散
    selected = []
    ind_count = defaultdict(int)
    for _, row in day_df.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= 2:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= 30:
            break
    return pd.DataFrame(selected)


# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 145)
    print("📊 backtest_v2 子策略独立回测")
    print("=" * 145)

    # 加载数据
    df = pd.read_csv(DETAIL_FILE)
    df = df[df['ret_1d'].notna()].copy()
    print(f"\n数据: {len(df)}条样本, {df['date'].nunique()}个交易日")
    print(f"区间: {df['date'].min()} ~ {df['date'].max()}")
    print(f"每天TOP30中选TOP10进行回测\n")

    all_results = {}

    # ============================================================
    # 一、基准策略
    # ============================================================
    print(f"{'=' * 145}")
    print("📊 一、基准策略")
    print(f"{'=' * 145}")
    print_header()

    baselines = [
        (sel_total_top, 10, "基准: 总分TOP10"),
        (sel_total_top, 20, "基准: 总分TOP20"),
        (sel_total_top, 30, "基准: 总分TOP30（全量）"),
        (sel_raw_total_top, 10, "基准: 原始总分TOP10（不含风险扣分）"),
    ]

    for func, n, label in baselines:
        r = backtest_by_selector(df, func, label, top_n=n)
        if r:
            print_result_line(r)
            all_results[label] = r

    # ============================================================
    # 二、6大维度独立选股
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 二、6大维度独立选股（每天只按单一维度排序选TOP10）")
    print(f"{'=' * 145}")
    print_header()

    dim_strategies = [
        (sel_d1_top, "D1 多周期共振 TOP10"),
        (sel_d2_top, "D2 主线热点 TOP10"),
        (sel_d3_top, "D3 三Skill TOP10"),
        (sel_d4_top, "D4 安全边际 TOP10"),
        (sel_d5_top, "D5 基本面 TOP10"),
        (sel_d9_top, "D9 百胜WR TOP10"),
    ]

    for func, label in dim_strategies:
        r = backtest_by_selector(df, func, label, top_n=10)
        if r:
            print_result_line(r)
            all_results[label] = r

    # ============================================================
    # 三、8大子维度独立选股
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 三、8大子维度独立选股（每天只按单一子维度排序选TOP10）")
    print(f"{'=' * 145}")
    print_header()

    sub_strategies = [
        (sel_mistery_top, "Mistery TOP10"),
        (sel_tds_top, "TDS TOP10"),
        (sel_yuanzi_top, "元子元 TOP10"),
        (sel_txcg_top, "TXCG六模型 TOP10"),
        (sel_wr1_top, "WR-1首板放量 TOP10"),
        (sel_wr2_top, "WR-2右侧起爆 TOP10"),
        (sel_wr3_top, "WR-3底倍量 TOP10"),
        (sel_bci_top, "BCI板块完整性 TOP10"),
    ]

    for func, label in sub_strategies:
        r = backtest_by_selector(df, func, label, top_n=10)
        if r:
            print_result_line(r)
            all_results[label] = r

    # ============================================================
    # 四、组合策略
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 四、组合策略（多维度加权选股TOP10）")
    print(f"{'=' * 145}")
    print_header()

    combo_strategies = [
        (sel_d3_d9_combo, "D3+D9 三Skill+百胜WR"),
        (sel_mistery_wr2_combo, "Mistery+WR2 组合"),
        (sel_d2_d3_combo, "D2+D3 主线热点+三Skill"),
        (sel_d1_d3_combo, "D1+D3 多周期共振+三Skill"),
        (sel_d4_d3_combo, "D4+D3 安全边际+三Skill"),
        (sel_low_risk_high_skill, "低风险+高技术面"),
        (sel_balanced, "均衡策略（6维度等权）"),
        (sel_momentum_quality, "动量+质量（D1+D3+D5）"),
        (sel_hotspot_skill, "热点+技术（D2+D3+D9）"),
    ]

    for func, label in combo_strategies:
        r = backtest_by_selector(df, func, label, top_n=10)
        if r:
            print_result_line(r)
            all_results[label] = r

    # ============================================================
    # 五、条件过滤策略
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 五、条件过滤策略（先过滤再按总分排序选TOP10）")
    print(f"{'=' * 145}")
    print_header()

    filter_strategies = [
        (sel_non_zt_top, "非涨停票 按总分"),
        (sel_zt_only, "仅涨停票 按总分"),
        (sel_high_bci, "BCI≥60 按总分"),
        (sel_low_risk, "低风险(net_risk≤2) 按总分"),
        (sel_high_protect, "高保护因子(protect≥5) 按总分"),
        (sel_wr2_ge3, "WR2≥3 按总分"),
        (sel_mistery_ge12, "Mistery≥10 按总分"),
        (sel_safe_margin_high, "安全边际高(D4≥8) 按总分"),
        (sel_mainboard_only, "仅主板(00/60) 按总分"),
        (sel_fund_inflow, "资金净流入 按总分"),
        (sel_elite_wr2_industry, "精选策略C（WR2/Mistery加权+行业分散）"),
    ]

    for func, label in filter_strategies:
        r = backtest_by_selector(df, func, label, top_n=10)
        if r:
            print_result_line(r)
            all_results[label] = r

    # ============================================================
    # 六、不同持有期对比（TOP10总分基准）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 六、不同持有期对比（总分TOP10）")
    print(f"{'=' * 145}")
    print_header()

    for hp, hp_label in [('ret_1d', 'T+1'), ('ret_2d', 'T+2'), ('ret_3d', 'T+3'), ('ret_5d', 'T+5')]:
        r = backtest_by_selector(df, sel_total_top, f"总分TOP10 持有{hp_label}", hold_period=hp, top_n=10)
        if r:
            print_result_line(r)

    # ============================================================
    # 七、综合排名（按累计收益）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 七、综合排名（按累计收益排序）")
    print(f"{'=' * 145}")
    print_header()

    sorted_results = sorted(all_results.values(), key=lambda x: x['cum_return'], reverse=True)
    for i, r in enumerate(sorted_results, 1):
        print_result_line(r, rank=i)
        if i >= 25:
            print(f"  ... 共{len(sorted_results)}个策略，仅显示前25")
            break

    # ============================================================
    # 八、风险收益比排名（收益/回撤）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 八、风险收益比排名（累计收益/最大回撤）")
    print(f"{'=' * 145}")

    risk_reward = []
    for r in sorted_results:
        rr = r['cum_return'] / max(r['max_dd'], 0.1)
        risk_reward.append((r, rr))
    risk_reward.sort(key=lambda x: -x[1])

    print(f"{'排名':>4} {'策略':<55} {'收益/回撤':>10} {'累计收益':>10} {'最大回撤':>10} {'Sharpe':>7}")
    print("-" * 110)
    for i, (r, rr) in enumerate(risk_reward[:15], 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<55} {rr:>9.2f} {r['cum_return']:>+9.1f}% -{r['max_dd']:>8.1f}% {r['sharpe']:>6.2f}")

    # ============================================================
    # 九、Sharpe排名
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 九、Sharpe排名")
    print(f"{'=' * 145}")

    sharpe_sorted = sorted(all_results.values(), key=lambda x: x['sharpe'], reverse=True)
    print(f"{'排名':>4} {'策略':<55} {'Sharpe':>7} {'累计收益':>10} {'胜率':>7} {'盈亏比':>7}")
    print("-" * 100)
    for i, r in enumerate(sharpe_sorted[:15], 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<55} {r['sharpe']:>6.2f} {r['cum_return']:>+9.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f}")

    # ============================================================
    # 十、月度收益对比（TOP3策略 vs 基准）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 十、月度收益对比（TOP3策略 vs 基准）")
    print(f"{'=' * 145}")

    baseline_key = "基准: 总分TOP10"
    top3_keys = [r['label'] for r in sorted_results[:3]]
    compare_keys = [baseline_key] + [k for k in top3_keys if k != baseline_key][:3]

    # 收集所有月份
    all_months = set()
    for key in compare_keys:
        if key in all_results:
            all_months.update(all_results[key]['monthly_rets'].keys())
    all_months = sorted(all_months)

    # 打印表头
    header = f"{'月份':<10}"
    for key in compare_keys:
        short_name = key[:25]
        header += f" {short_name:>25}"
    print(header)
    print("-" * (10 + 26 * len(compare_keys)))

    for month in all_months:
        line = f"{month:<10}"
        for key in compare_keys:
            if key in all_results:
                val = all_results[key]['monthly_rets'].get(month, 0)
                tag = '🟢' if val > 0 else '🔴'
                line += f" {tag}{val:>+22.2f}%"
            else:
                line += f" {'N/A':>25}"
        print(line)

    # 月度汇总
    print()
    line = f"{'累计':<10}"
    for key in compare_keys:
        if key in all_results:
            total = sum(all_results[key]['monthly_rets'].values())
            line += f" {total:>+24.1f}%"
    print(line)

    # ============================================================
    # 十一、维度贡献度分析（高分vs低分）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 十一、维度贡献度分析（高分组 vs 低分组的T+1收益差）")
    print(f"{'=' * 145}")

    dims_config = [
        ('d1', '多周期共振', 15), ('d2', '主线热点', 25), ('d3', '三Skill', 47),
        ('d4', '安全边际', 15), ('d5', '基本面', 15), ('d9', '百胜WR', 15),
    ]
    sub_dims_config = [
        ('mistery', 'Mistery', 20), ('tds', 'TDS', 12), ('yuanzi', '元子元', 10),
        ('txcg', 'TXCG六模型', 5), ('wr1', 'WR-1首板', 7), ('wr2', 'WR-2起爆', 5),
        ('wr3', 'WR-3底倍量', 4), ('bci', 'BCI板块', 100),
    ]

    print(f"\n{'维度':<15} {'阈值':>6} | {'高分N':>6} {'高分收':>8} {'高分胜':>8} | {'低分N':>6} {'低分收':>8} {'低分胜':>8} | {'差值':>8} {'判定':>10}")
    print("-" * 115)

    print("\n  --- 6大维度 ---")
    for key, name, mx in dims_config:
        th = mx * 0.6
        hi = df[df[key] >= th]
        lo = df[df[key] < th]
        if len(hi) > 5 and len(lo) > 5:
            ah = hi['ret_1d'].mean()
            al = lo['ret_1d'].mean()
            wh = (hi['ret_1d'] > 0).mean() * 100
            wl = (lo['ret_1d'] > 0).mean() * 100
            d = ah - al
            v = "✅正贡献" if d > 0.3 else ("⚠️弱" if d > -0.3 else "❌负贡献")
            print(f"  {name:<13} ≥{th:>4.0f} | {len(hi):>5} {ah:>+7.2f}% {wh:>7.1f}% | {len(lo):>5} {al:>+7.2f}% {wl:>7.1f}% | {d:>+7.2f}% {v}")

    print("\n  --- 8大子维度 ---")
    for key, name, mx in sub_dims_config:
        th = mx * 0.6
        hi = df[df[key] >= th]
        lo = df[df[key] < th]
        if len(hi) > 5 and len(lo) > 5:
            ah = hi['ret_1d'].mean()
            al = lo['ret_1d'].mean()
            wh = (hi['ret_1d'] > 0).mean() * 100
            wl = (lo['ret_1d'] > 0).mean() * 100
            d = ah - al
            v = "✅正贡献" if d > 0.3 else ("⚠️弱" if d > -0.3 else "❌负贡献")
            print(f"  {name:<13} ≥{th:>4.0f} | {len(hi):>5} {ah:>+7.2f}% {wh:>7.1f}% | {len(lo):>5} {al:>+7.2f}% {wl:>7.1f}% | {d:>+7.2f}% {v}")

    # ============================================================
    # 十二、核心结论
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 十二、核心结论")
    print(f"{'=' * 145}")

    if sorted_results:
        best_return = sorted_results[0]
        best_rr_r, best_rr_val = risk_reward[0]
        best_sharpe = sharpe_sorted[0]

        print(f"\n  🥇 最高收益策略: {best_return['label']}")
        print(f"     累计{best_return['cum_return']:+.1f}% 年化{best_return['annual_return']:+.1f}% "
              f"回撤-{best_return['max_dd']:.1f}% 胜率{best_return['win_rate']:.1f}% Sharpe{best_return['sharpe']:.2f}")

        print(f"\n  🥇 最佳风险收益比: {best_rr_r['label']}")
        print(f"     收益/回撤={best_rr_val:.2f} 累计{best_rr_r['cum_return']:+.1f}% 回撤-{best_rr_r['max_dd']:.1f}%")

        print(f"\n  🥇 最佳Sharpe: {best_sharpe['label']}")
        print(f"     Sharpe={best_sharpe['sharpe']:.2f} 累计{best_sharpe['cum_return']:+.1f}%")

        # 分类最优
        print(f"\n  📌 分类最优:")

        # 6大维度中最优
        dim_results = [(k, v) for k, v in all_results.items() if k.startswith('D') and 'TOP10' in k and '+' not in k]
        if dim_results:
            best_dim = max(dim_results, key=lambda x: x[1]['cum_return'])
            print(f"     6大维度最优: {best_dim[0]} (累计{best_dim[1]['cum_return']:+.1f}%)")

        # 子维度中最优
        sub_results = [(k, v) for k, v in all_results.items()
                       if any(k.startswith(s) for s in ['Mistery', 'TDS', '元子元', 'TXCG', 'WR-', 'BCI'])]
        if sub_results:
            best_sub = max(sub_results, key=lambda x: x[1]['cum_return'])
            print(f"     子维度最优: {best_sub[0]} (累计{best_sub[1]['cum_return']:+.1f}%)")

        # 组合策略中最优
        combo_results = [(k, v) for k, v in all_results.items()
                         if any(s in k for s in ['组合', '均衡', '动量', '热点+技术', '低风险'])]
        if combo_results:
            best_combo = max(combo_results, key=lambda x: x[1]['cum_return'])
            print(f"     组合策略最优: {best_combo[0]} (累计{best_combo[1]['cum_return']:+.1f}%)")

        # 条件过滤中最优
        filter_results = [(k, v) for k, v in all_results.items()
                          if any(s in k for s in ['非涨停', '仅涨停', 'BCI≥', '低风险', '高保护', 'WR2≥', 'Mistery≥',
                                                  '安全边际高', '仅主板', '资金净流入', '精选策略'])]
        if filter_results:
            best_filter = max(filter_results, key=lambda x: x[1]['cum_return'])
            print(f"     条件过滤最优: {best_filter[0]} (累计{best_filter[1]['cum_return']:+.1f}%)")

        # vs 基准对比
        baseline = all_results.get(baseline_key)
        if baseline:
            print(f"\n  📊 TOP5策略 vs 基准({baseline_key}):")
            for r in sorted_results[:5]:
                if r['label'] == baseline_key:
                    continue
                diff_ret = r['cum_return'] - baseline['cum_return']
                diff_dd = baseline['max_dd'] - r['max_dd']
                print(f"     {r['label'][:50]}: 收益{diff_ret:+.1f}% 回撤改善{diff_dd:+.1f}%")

    print(f"\n{'=' * 145}")
    print("✅ 子策略回测完成！")
    print(f"{'=' * 145}")


if __name__ == '__main__':
    main()
