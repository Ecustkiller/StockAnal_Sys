#!/usr/bin/env python3
"""
PositionManager 每日维护脚本
==============================
【作用】
  维护一个"策略04专属"的 PM 状态文件（JSON），每天盘后增量 feed 一次，
  这样 run_4strategies.py 启动时就能直接加载最新状态，
  避免每次都从5年回测重新预热，也避免数据断档。

【状态文件结构】(data/json_results/pm_state_s04.json)
  {
    "last_date": "20260410",          # 最后一次 feed 的日期
    "nav": 3.1909,
    "peak": 3.1909,
    "loss_streak": 0,
    "navs": [1.0, 1.005, ...],        # 净值序列
    "returns_history": [0.5, -0.2...], # 每日收益率%序列
    "target_annual_vol": 10.0,
    "history_dates": ["20210104", ...]  # 对应每天的日期
  }

【三种使用模式】
  1. 全量初始化（首次运行，从5年回测 CSV 一次性预热）：
     python3 pm_daily.py --init
  
  2. 从 detail CSV 增量补齐到最新（推荐，backtest_v2 重跑后用）：
     python3 pm_daily.py --feed-from-detail <csv>
  
  3. 单日手工喂入（实盘执行后记录今日实际策略03等权收益）：
     python3 pm_daily.py --feed-daily 20260423 0.45
  
  4. 查看状态：
     python3 pm_daily.py --show
"""
import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, '/Users/ecustkiller/WorkBuddy/Claw')
from claw.strategies.strategy_04_risk_managed import PositionManager
try:
    from claw.strategies.strategy_03_optimized import select_optimized_elite
except ImportError:
    select_optimized_elite = None  # detail csv 中没有aiTrader因子时退化为代理

STATE_PATH = '/Users/ecustkiller/WorkBuddy/Claw/data/json_results/pm_state_s04.json'
DEFAULT_DETAIL_CSV = '/Users/ecustkiller/WorkBuddy/Claw/data/backtest_results/backtest_v2_detail_20260420_180427.csv'


# ============================================================
# 状态持久化
# ============================================================
def save_state(pm: PositionManager, history_dates: list):
    state = {
        'last_date': history_dates[-1] if history_dates else None,
        'nav': pm.nav,
        'peak': pm.peak,
        'loss_streak': pm.loss_streak,
        'navs': pm.navs,
        'returns_history': pm.returns_history,
        'target_annual_vol': pm.target_daily_vol * np.sqrt(250),
        'history_dates': history_dates,
        'n_days': len(pm.returns_history),
        'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    return state


def load_state():
    if not os.path.exists(STATE_PATH):
        return None
    with open(STATE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def pm_from_state(state):
    """从持久化 state dict 重建 PositionManager"""
    pm = PositionManager(target_annual_vol=state.get('target_annual_vol', 10.0))
    pm.returns_history = list(state['returns_history'])
    pm.navs = list(state['navs'])
    pm.nav = state['nav']
    pm.peak = state['peak']
    pm.loss_streak = state['loss_streak']
    return pm, list(state.get('history_dates', []))


def load_or_preheat():
    """
    对外的主接口：run_4strategies.py 启动时调用。
    - 有持久化状态 → 直接加载
    - 无状态 → 从5年回测预热，自动落盘
    返回: (pm, info_dict)
    """
    state = load_state()
    if state is not None:
        pm, dates = pm_from_state(state)
        return pm, {
            'source': 'persisted',
            'last_date': state['last_date'],
            'n_days': len(pm.returns_history),
            'saved_at': state.get('saved_at', 'N/A'),
        }
    # 无状态 → 预热并落盘
    print("  (首次运行，从5年历史回测预热 PM...)")
    pm, dates = _preheat_from_csv(DEFAULT_DETAIL_CSV)
    save_state(pm, dates)
    return pm, {
        'source': 'preheated',
        'last_date': dates[-1] if dates else None,
        'n_days': len(pm.returns_history),
        'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


# ============================================================
# 策略03上游代理（当CSV无因子时使用）
# ============================================================
def _proxy_select_top10(day_df, n=10, max_per_ind=3):
    mask = (day_df['is_zt'] == False)
    mask &= (day_df['r5'] < 10)
    mask &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
    mask &= (day_df['net_risk'] <= 2)
    f = day_df[mask].copy()
    if len(f) == 0:
        mask2 = (day_df['is_zt'] == False) & (day_df['r5'] < 15)
        mask2 &= ((day_df['wr2'] >= 3) | (day_df['mistery'] >= 10))
        f = day_df[mask2].copy()
    if len(f) == 0:
        f = day_df[(day_df['is_zt'] == False) & (day_df['r5'] < 20)].copy()
    if len(f) == 0:
        return pd.DataFrame()
    f = f.sort_values('total', ascending=False)
    selected, ind_count = [], defaultdict(int)
    for _, row in f.iterrows():
        ind = row.get('industry', '未知')
        if ind_count[ind] >= max_per_ind:
            continue
        selected.append(row)
        ind_count[ind] += 1
        if len(selected) >= n:
            break
    return pd.DataFrame(selected)


def _day_raw_return(day_df):
    """根据当日数据计算策略03的单日 raw_ret (%)"""
    # 优先用真正的策略03（如果 CSV 带因子列）
    if select_optimized_elite is not None and 'close_position' in day_df.columns:
        try:
            sel = select_optimized_elite(day_df, n=10, max_per_ind=3)
        except Exception:
            sel = _proxy_select_top10(day_df)
    else:
        sel = _proxy_select_top10(day_df)
    if len(sel) == 0 or 'ret_1d' not in sel.columns:
        return None
    rets = sel['ret_1d'].dropna()
    if len(rets) == 0:
        return None
    return float(rets.mean())


# ============================================================
# 从 detail CSV 预热/增量 feed
# ============================================================
def _preheat_from_csv(detail_csv, start_after_date=None):
    """
    把 detail CSV 里的每一天（可选在 start_after_date 之后）逐日 feed 给 PM
    返回全新的 PM 实例和对应的日期列表
    """
    df = pd.read_csv(detail_csv)
    df['date'] = df['date'].astype(str)
    all_dates = sorted(df['date'].unique())
    if start_after_date:
        all_dates = [d for d in all_dates if d > str(start_after_date)]
    pm = PositionManager(target_annual_vol=10.0)
    used_dates = []
    for d in all_dates:
        day_df = df[df['date'] == d]
        r = _day_raw_return(day_df)
        if r is None:
            continue
        pm.update(r)
        used_dates.append(d)
    return pm, used_dates


def feed_from_detail(detail_csv, force_reinit=False):
    """
    从 detail CSV 增量 feed：
    - 读取现有状态的 last_date，只 feed 之后的日期
    - 若无状态或 force_reinit → 全量预热
    """
    state = load_state()
    if state is None or force_reinit:
        pm, dates = _preheat_from_csv(detail_csv)
        save_state(pm, dates)
        return pm, dates, 'full_init'

    # 增量
    pm, existing_dates = pm_from_state(state)
    last_date = state['last_date']
    df = pd.read_csv(detail_csv)
    df['date'] = df['date'].astype(str)
    new_dates = sorted([d for d in df['date'].unique() if d > last_date])
    if not new_dates:
        return pm, existing_dates, 'no_new_data'
    
    added = []
    for d in new_dates:
        day_df = df[df['date'] == d]
        r = _day_raw_return(day_df)
        if r is None:
            continue
        pm.update(r)
        existing_dates.append(d)
        added.append((d, r))
    save_state(pm, existing_dates)
    return pm, existing_dates, ('incremental', added)


def feed_single_day(date_str, daily_return_pct):
    """
    手工喂入单日收益率（实盘执行后用）
    date_str: 'YYYYMMDD'
    daily_return_pct: 今日策略03等权收益率（%），例如 0.45
    """
    state = load_state()
    if state is None:
        raise RuntimeError("请先运行 --init 初始化 PM 状态")
    if state['last_date'] and str(date_str) <= str(state['last_date']):
        raise RuntimeError(f"日期 {date_str} 已经 feed 过（最后日期 {state['last_date']}），拒绝重复喂入")
    pm, dates = pm_from_state(state)
    pm.update(daily_return_pct)
    dates.append(str(date_str))
    save_state(pm, dates)
    return pm, dates


# ============================================================
# 展示
# ============================================================
def print_state(pm: PositionManager, dates: list, source_info=None):
    n = len(pm.returns_history)
    dd = (pm.peak - pm.nav) / pm.peak * 100 if pm.peak > 0 else 0
    print(f"\n{'='*70}")
    print(f"🛡️  PositionManager 状态")
    print(f"{'='*70}")
    if source_info:
        print(f"  数据来源:       {source_info}")
    print(f"  状态文件:       {STATE_PATH}")
    print(f"  历史交易日数:   {n}")
    if dates:
        print(f"  数据区间:       {dates[0]} → {dates[-1]}")
    print(f"  当前 NAV:       {pm.nav:.4f}")
    print(f"  历史峰值:       {pm.peak:.4f}")
    print(f"  当前回撤:       {dd:.2f}%")
    print(f"  连续亏损天数:   {pm.loss_streak}")
    if n >= 20:
        recent20 = np.array(pm.returns_history[-20:])
        print(f"  近20日均收益:   {recent20.mean():+.3f}%/day")
        print(f"  近20日波动率:   {recent20.std():.3f}% (年化 {recent20.std()*np.sqrt(250):.1f}%)")
    if n >= 10:
        win10 = (np.array(pm.returns_history[-10:]) > 0).sum() / 10 * 100
        print(f"  近10日胜率:     {win10:.0f}%")
    print(f"\n  📊 今日建议仓位: {pm.get_position()*100:.1f}%")
    print(f"{'='*70}\n")


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='PositionManager 每日维护')
    parser.add_argument('--init', action='store_true', help='从5年回测CSV全量初始化')
    parser.add_argument('--feed-from-detail', type=str, metavar='CSV',
                        help='从 detail CSV 增量补齐（自动跳过已feed日期）')
    parser.add_argument('--feed-daily', nargs=2, metavar=('DATE', 'RET_PCT'),
                        help='手工喂入单日收益率，例如: --feed-daily 20260423 0.45')
    parser.add_argument('--show', action='store_true', help='查看当前 PM 状态')
    parser.add_argument('--reset', action='store_true', help='删除状态文件（慎用）')
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(STATE_PATH):
            os.remove(STATE_PATH)
            print(f"✅ 已删除状态文件: {STATE_PATH}")
        else:
            print("  状态文件不存在")
        return

    if args.init:
        detail_csv = DEFAULT_DETAIL_CSV
        print(f"📥 从 {os.path.basename(detail_csv)} 全量预热 PM...")
        pm, dates = _preheat_from_csv(detail_csv)
        save_state(pm, dates)
        print(f"✅ 预热完成，共 {len(dates)} 个交易日")
        print_state(pm, dates, source_info='全量预热（--init）')
        return

    if args.feed_from_detail:
        detail_csv = args.feed_from_detail
        print(f"📥 从 {os.path.basename(detail_csv)} 增量 feed PM...")
        pm, dates, status = feed_from_detail(detail_csv)
        if isinstance(status, tuple) and status[0] == 'incremental':
            added = status[1]
            print(f"✅ 增量添加 {len(added)} 天:")
            for d, r in added:
                print(f"   {d}: {r:+.3f}%")
        elif status == 'no_new_data':
            print("  ⚠ 没有新日期可 feed（CSV 数据未超过 last_date）")
        else:
            print(f"  ✅ 全量初始化完成，共 {len(dates)} 天")
        print_state(pm, dates, source_info=f'增量来源: {os.path.basename(detail_csv)}')
        return

    if args.feed_daily:
        date_str, ret_str = args.feed_daily
        ret = float(ret_str)
        print(f"📥 手工喂入 {date_str}: {ret:+.3f}%")
        pm, dates = feed_single_day(date_str, ret)
        print("✅ 完成")
        print_state(pm, dates, source_info=f'手工喂入 {date_str}')
        return

    if args.show or len(sys.argv) == 1:
        state = load_state()
        if state is None:
            print("⚠ 状态文件不存在，请先运行:")
            print(f"  python3 {sys.argv[0]} --init")
            return
        pm, dates = pm_from_state(state)
        print_state(pm, dates, source_info=f"已持久化 ({state.get('saved_at','N/A')})")


if __name__ == '__main__':
    main()
