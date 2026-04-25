#!/usr/bin/env python3
"""
PositionManager 预热脚本
========================
用历史回测的每日收益序列 feed 给 PositionManager，
构建出"截至今日"的真实 PM 状态（nav/peak/loss_streak/returns_history），
这样今日的 get_position() 才能给出有意义的仓位建议。

数据来源：
  backtest_v2_detail_20260420_180427.csv（2021-01-04 ~ 2026-04-10，5年1275天）

预热策略（用于生成每日收益序列）：
  为了尽量贴近策略04（基于策略03），但又避免重新计算 aiTrader 因子：
  使用策略03的【硬性前置过滤】 + total降序 TOP10 作为代理。
  这相当于策略03 因子精选前的上游池，风险特征高度一致。

使用：
  from preheat_position_manager import build_preheated_pm
  pm = build_preheated_pm()
  today_position = pm.get_position()
"""
import os
import sys
import pandas as pd
import numpy as np
from collections import defaultdict

sys.path.insert(0, '/Users/ecustkiller/WorkBuddy/Claw')
from claw.strategies.strategy_04_risk_managed import PositionManager

DEFAULT_DETAIL_CSV = '/Users/ecustkiller/WorkBuddy/Claw/data/backtest_results/backtest_v2_detail_20260420_180427.csv'


def proxy_select_top10(day_df, n=10, max_per_ind=3):
    """策略03 上游代理：硬过滤 + total降序 + 行业分散"""
    mask = (day_df['is_zt'] == False)
    mask &= (day_df['r5'] < 10)
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
    mask &= (day_df['net_risk'] <= 2)
    f = day_df[mask].copy()

    if len(f) == 0:
        # 放宽兜底
        mask2 = (day_df['is_zt'] == False) & (day_df['r5'] < 15)
        mask2 &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
        f = day_df[mask2].copy()
    if len(f) == 0:
        mask3 = (day_df['is_zt'] == False) & (day_df['r5'] < 20)
        f = day_df[mask3].copy()
    if len(f) == 0:
        return pd.DataFrame()

    f = f.sort_values('total', ascending=False)

    selected = []
    ind_count = defaultdict(int)
    for _, row in f.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= max_per_ind:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= n:
            break
    return pd.DataFrame(selected)


def build_preheated_pm(detail_csv=DEFAULT_DETAIL_CSV, target_annual_vol=10.0,
                       verbose=False):
    """
    读取历史回测数据，逐日 feed PM，返回预热后的 PM 实例
    """
    df = pd.read_csv(detail_csv)
    df['date'] = df['date'].astype(str)
    dates = sorted(df['date'].unique())

    pm = PositionManager(target_annual_vol=target_annual_vol)

    daily_log = []
    for date in dates:
        day_df = df[df['date'] == date]
        if len(day_df) == 0:
            continue
        sel = proxy_select_top10(day_df, n=10, max_per_ind=3)
        if len(sel) == 0:
            continue
        rets = sel['ret_1d'].dropna()
        if len(rets) == 0:
            continue
        daily_ret = float(rets.mean())  # 等权
        pm.update(daily_ret)
        if verbose:
            daily_log.append({
                'date': date, 'daily_ret': daily_ret,
                'nav': pm.nav, 'peak': pm.peak,
                'loss_streak': pm.loss_streak,
            })

    return pm, daily_log


def print_pm_state(pm, title="PM 预热完成"):
    print(f"\n{'='*70}")
    print(f"🛡️  {title}")
    print(f"{'='*70}")
    n = len(pm.returns_history)
    print(f"  历史交易日数:   {n}")
    print(f"  当前净值 NAV:   {pm.nav:.4f}  (起始 1.0)")
    print(f"  历史峰值 Peak:  {pm.peak:.4f}")
    dd = (pm.peak - pm.nav) / pm.peak * 100 if pm.peak > 0 else 0
    print(f"  当前回撤:       {dd:.2f}%")
    print(f"  连续亏损天数:   {pm.loss_streak}")

    if n >= 20:
        recent20 = np.array(pm.returns_history[-20:])
        print(f"  近20日收益均值: {recent20.mean():+.3f}%")
        print(f"  近20日波动率:   {recent20.std():.3f}% (年化 {recent20.std()*np.sqrt(250):.1f}%)")
    if n >= 10:
        recent10 = np.array(pm.returns_history[-10:])
        win10 = (recent10 > 0).sum() / len(recent10) * 100
        print(f"  近10日胜率:     {win10:.0f}%")

    print(f"\n  📊 今日建议仓位: {pm.get_position()*100:.1f}%")

    # 各信号详情
    detail = pm.get_signal_detail()
    print(f"\n  🔍 信号分解:")
    from claw.strategies.strategy_04_risk_managed import PositionManager as _PM
    # 再算一遍各信号
    signals = {}
    # 1 loss_streak
    if pm.loss_streak >= 3: signals['连亏控制 (25%)'] = 0.2
    elif pm.loss_streak >= 2: signals['连亏控制 (25%)'] = 0.4
    elif pm.loss_streak >= 1: signals['连亏控制 (25%)'] = 0.6
    else: signals['连亏控制 (25%)'] = 1.0
    # 2 vol
    if n >= pm.lookback:
        recent = np.array(pm.returns_history[-pm.lookback:])
        vol = recent.std()
        if vol > 0: signals['波动率目标 (25%)'] = min(1.0, pm.target_daily_vol / vol)
        else: signals['波动率目标 (25%)'] = 1.0
    # 3 dd
    if dd > 18: signals['回撤控制 (25%)'] = 0.2
    elif dd > 10: signals['回撤控制 (25%)'] = 0.4
    elif dd > 5: signals['回撤控制 (25%)'] = 0.7
    else: signals['回撤控制 (25%)'] = 1.0
    # 4 momentum
    navs = np.array(pm.navs)
    if len(navs) >= 20:
        ma5 = navs[-5:].mean(); ma20 = navs[-20:].mean()
        if ma5 > ma20 * 1.02: signals['净值动量 (15%)'] = 1.0
        elif ma5 > ma20: signals['净值动量 (15%)'] = 0.7
        else: signals['净值动量 (15%)'] = 0.4
    # 5 win_rate
    if n >= 10:
        recent10 = np.array(pm.returns_history[-10:])
        wr = (recent10 > 0).sum() / len(recent10)
        if wr < 0.3: signals['近期胜率 (10%)'] = 0.2
        elif wr < 0.4: signals['近期胜率 (10%)'] = 0.5
        elif wr > 0.6: signals['近期胜率 (10%)'] = 1.0
        else: signals['近期胜率 (10%)'] = 0.7

    for name, v in signals.items():
        bar = '█' * int(v * 20)
        print(f"    {name:20s} → {v*100:5.1f}%  {bar}")

    print(f"{'='*70}\n")


if __name__ == '__main__':
    print("📥 读取历史回测并预热 PositionManager...")
    pm, log = build_preheated_pm(verbose=True)
    print(f"✅ 预热完成，处理了 {len(pm.returns_history)} 个交易日")
    print_pm_state(pm, title="PM 状态（基于2021-01 至 2026-04-10 历史回测）")

    # 同时输出最近20天的净值轨迹
    if log:
        print("\n📈 最近20个交易日 PM 状态:")
        print(f"  {'日期':<10} {'日收益%':>8} {'NAV':>8} {'回撤%':>7} {'连亏':>4}")
        for r in log[-20:]:
            dd = (r['peak'] - r['nav']) / r['peak'] * 100 if r['peak']>0 else 0
            print(f"  {r['date']:<10} {r['daily_ret']:>+8.3f} {r['nav']:>8.4f} {dd:>6.2f}% {r['loss_streak']:>4}")
