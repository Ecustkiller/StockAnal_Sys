#!/usr/bin/env python3
"""
持仓延续 + 趋势止损 回测引擎
================================================================
与 elite_backtest.py 的"每天清仓换股"不同，本引擎模拟真实交易：
  - 持仓延续：昨天持有的票如果今天还在TOP名单里，继续持有
  - 趋势止损：不在名单里的票，根据趋势指标决定是否卖出
  - 动态仓位：根据持仓情况动态分配资金

止损/止盈规则（可配置）：
  1. 硬止损：从买入价回撤超X% → 强制卖出
  2. 移动止盈：从持仓期间最高价回撤超Y% → 止盈卖出
  3. 跌出名单+亏损：连续N天不在TOP名单且收益为负 → 卖出
  4. 最大持仓天数：兜底，超过N天无论如何卖出

基于 backtest_v2 的 detail CSV（提供每日评分TOP30名单）
+ daily_snapshot（提供逐日行情用于止损判断）
================================================================
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_052244.csv'
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")

# 止损止盈参数（多组对比）
PARAM_SETS = {
    'conservative': {
        'label': '保守型（硬止损-5% 移动止盈-5% 最长5天）',
        'hard_stop_loss': -5.0,      # 从买入价亏损超5%强制止损
        'trailing_stop': -5.0,       # 从最高价回撤超5%止盈
        'out_list_days': 2,          # 连续2天不在名单且亏损→卖
        'max_hold_days': 5,          # 最大持仓5天
        'top_n': 10,                 # 每天选10只
        'max_positions': 10,         # 最大持仓数
    },
    'moderate': {
        'label': '稳健型（硬止损-7% 移动止盈-6% 最长8天）',
        'hard_stop_loss': -7.0,
        'trailing_stop': -6.0,
        'out_list_days': 3,
        'max_hold_days': 8,
        'top_n': 10,
        'max_positions': 10,
    },
    'aggressive': {
        'label': '激进型（硬止损-10% 移动止盈-8% 最长15天）',
        'hard_stop_loss': -10.0,
        'trailing_stop': -8.0,
        'out_list_days': 5,
        'max_hold_days': 15,
        'top_n': 10,
        'max_positions': 10,
    },
    'trend_follow': {
        'label': '趋势跟踪型（硬止损-8% 移动止盈-10% 最长20天）',
        'hard_stop_loss': -8.0,
        'trailing_stop': -10.0,      # 给更大的回撤空间，让趋势充分发展
        'out_list_days': 3,
        'max_hold_days': 20,
        'top_n': 10,
        'max_positions': 10,
    },
    'tight_5': {
        'label': '紧凑5只（硬止损-6% 移动止盈-5% 最长8天 5只）',
        'hard_stop_loss': -6.0,
        'trailing_stop': -5.0,
        'out_list_days': 2,
        'max_hold_days': 8,
        'top_n': 5,
        'max_positions': 5,
    },
}

# ============================================================
# 数据加载
# ============================================================
_snap_cache = {}

def load_snapshot(date_str):
    """加载某天的全市场快照"""
    date_str = str(date_str)
    if date_str in _snap_cache:
        return _snap_cache[date_str]
    path = os.path.join(SNAPSHOT_DIR, f"{date_str}.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'pct_chg',
                'vol', 'amount', 'pe_ttm', 'total_mv', 'circ_mv',
                'turnover_rate_f', 'net_mf_amount']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    _snap_cache[date_str] = df
    return df


def get_all_trading_dates():
    """获取所有交易日期（从snapshot目录）"""
    files = sorted([f.replace('.parquet', '') for f in os.listdir(SNAPSHOT_DIR)
                    if f.endswith('.parquet') and f[0].isdigit()])
    return files


def get_stock_price(date_str, code, field='close'):
    """获取某只股票某天的价格"""
    df = load_snapshot(date_str)
    if df is None:
        return None
    row = df[df['ts_code'] == code]
    if row.empty:
        return None
    val = row.iloc[0].get(field)
    if pd.isna(val):
        return None
    return float(val)


def get_stock_high(date_str, code):
    """获取某只股票某天的最高价"""
    return get_stock_price(date_str, code, 'high')


def get_stock_low(date_str, code):
    """获取某只股票某天的最低价"""
    return get_stock_price(date_str, code, 'low')


# ============================================================
# 选股函数（复用elite_backtest的逻辑）
# ============================================================

def calc_elite_score(row):
    """计算精选得分"""
    total_norm = row['total'] / 150 * 100
    # 维度均衡度
    dims = []
    if 'd1' in row and pd.notna(row['d1']): dims.append(row['d1'] / 15)
    if 'd2' in row and pd.notna(row['d2']): dims.append(row['d2'] / 25)
    if 'd3' in row and pd.notna(row['d3']): dims.append(row['d3'] / 47)
    if 'd4' in row and pd.notna(row['d4']): dims.append(row['d4'] / 15)
    if 'd5' in row and pd.notna(row['d5']): dims.append(row['d5'] / 15)
    if dims:
        mean_val = np.mean(dims)
        cv = np.std(dims) / mean_val if mean_val > 0 else 1
        balance = max(0, 1 - cv) * 100
    else:
        balance = 0
    risk_score = max(0, 100 - row.get('net_risk', 0) * 10)
    nb = row.get('nb_yi', 0)
    if pd.isna(nb): nb = 0
    fund_score = min(100, max(0, 50 + nb * 10))
    return total_norm * 0.50 + balance * 0.20 + risk_score * 0.15 + fund_score * 0.15


def select_stocks(day_df, n, max_per_ind=2):
    """
    从当天TOP30中精选n只（使用策略C的逻辑：精选+WR2/Mistery加权）
    """
    day_df = day_df.copy()
    day_df['elite_score'] = day_df.apply(calc_elite_score, axis=1)

    # 非涨停优先
    day_df['score_adj'] = day_df['elite_score']
    day_df.loc[day_df['is_zt'] == False, 'score_adj'] += 10

    # WR2/Mistery加权
    day_df.loc[day_df['wr2'] >= 3, 'score_adj'] += 15
    day_df.loc[day_df['wr2'] >= 4, 'score_adj'] += 10
    day_df.loc[day_df['mistery'] >= 12, 'score_adj'] += 12
    day_df.loc[day_df['mistery'] >= 15, 'score_adj'] += 8
    day_df.loc[day_df['d4'] >= 10, 'score_adj'] += 5

    day_df = day_df.sort_values('score_adj', ascending=False)

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


# ============================================================
# 持仓类
# ============================================================

class Position:
    """单个持仓"""
    def __init__(self, code, name, industry, buy_date, buy_price, score_info=None):
        self.code = code
        self.name = name
        self.industry = industry
        self.buy_date = buy_date        # 买入日期
        self.buy_price = buy_price      # 买入价格（开盘价）
        self.high_since_buy = buy_price # 买入以来最高价
        self.hold_days = 0              # 已持有天数
        self.out_list_count = 0         # 连续不在名单天数
        self.current_price = buy_price  # 当前价格
        self.score_info = score_info or {}  # 买入时的评分信息

    @property
    def pnl_pct(self):
        """当前盈亏百分比"""
        if self.buy_price <= 0:
            return 0
        return (self.current_price - self.buy_price) / self.buy_price * 100

    @property
    def drawdown_from_high(self):
        """从最高价的回撤百分比（负数）"""
        if self.high_since_buy <= 0:
            return 0
        return (self.current_price - self.high_since_buy) / self.high_since_buy * 100

    def update_price(self, close_price, high_price):
        """更新当日价格"""
        self.current_price = close_price
        if high_price > self.high_since_buy:
            self.high_since_buy = high_price
        self.hold_days += 1

    def __repr__(self):
        return f"<Pos {self.code} {self.name} 买入{self.buy_price:.2f} 现价{self.current_price:.2f} 盈亏{self.pnl_pct:+.2f}% 持{self.hold_days}天>"


# ============================================================
# 回测引擎
# ============================================================

def run_hold_backtest(detail_df, params, verbose=False):
    """
    持仓延续+趋势止损 回测

    参数:
        detail_df: backtest_v2的detail CSV数据（每天TOP30的评分结果）
        params: 止损止盈参数字典
        verbose: 是否打印每日明细

    返回:
        回测结果字典
    """
    hard_stop_loss = params['hard_stop_loss']
    trailing_stop = params['trailing_stop']
    out_list_days = params['out_list_days']
    max_hold_days = params['max_hold_days']
    top_n = params['top_n']
    max_positions = params['max_positions']
    label = params['label']

    # 获取所有交易日期
    all_trading_dates = get_all_trading_dates()
    # 获取detail数据中的日期范围
    detail_dates = sorted(detail_df['date'].unique())
    start_date = str(detail_dates[0])
    end_date = str(detail_dates[-1])

    # 过滤交易日期到回测区间
    # detail中的date是"评分日"，买入是在下一个交易日
    # 我们需要从评分日的下一个交易日开始，到最后一个评分日之后足够多的交易日
    trading_dates = [d for d in all_trading_dates if d >= start_date]

    # 构建评分日→候选名单的映射
    # key: 评分日期, value: 该日TOP-N的股票代码集合和详细信息
    daily_candidates = {}
    for date in detail_dates:
        day_data = detail_df[detail_df['date'] == date].copy()
        selected = select_stocks(day_data, top_n)
        if len(selected) > 0:
            daily_candidates[str(date)] = {
                'codes': set(selected['code'].tolist()),
                'details': {row['code']: row.to_dict() for _, row in selected.iterrows()},
            }

    # ============================================================
    # 逐日模拟（等权净值法）
    # 每个持仓占1/max_positions的权重，空仓部分不计收益
    # 组合日收益 = 持仓比例 × 持仓平均日收益
    # ============================================================
    positions = {}          # code -> Position
    nav = 1.0               # 组合净值（从1.0开始）
    daily_records = []      # 每日记录
    all_trades = []         # 所有交易记录

    # 找到第一个评分日的下一个交易日作为起始
    first_score_date = str(detail_dates[0])
    start_idx = None
    for i, d in enumerate(trading_dates):
        if d > first_score_date:
            start_idx = i
            break
    if start_idx is None:
        print(f"  ❌ 无法找到起始交易日")
        return None

    # 最后一个评分日
    last_score_date = str(detail_dates[-1])

    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        yesterday = trading_dates[day_idx - 1] if day_idx > 0 else None

        # 如果已经超过最后评分日太多天，停止
        if today > last_score_date and not positions:
            break
        # 给持仓清仓的缓冲期
        if today > last_score_date:
            days_after = sum(1 for d in trading_dates if last_score_date < d <= today)
            if days_after > max_hold_days + 5:
                break

        # 昨天的评分名单（昨天收盘后评分，今天执行）
        score_date = yesterday
        today_candidates = daily_candidates.get(score_date, {})
        today_candidate_codes = today_candidates.get('codes', set())
        today_candidate_details = today_candidates.get('details', {})

        # ============================================================
        # 1. 记录昨日收盘价（用于计算今日日收益）
        # ============================================================
        prev_prices = {}
        for code, pos in positions.items():
            prev_prices[code] = pos.current_price

        # ============================================================
        # 2. 更新持仓价格 & 判断止损止盈
        # ============================================================
        to_sell = []
        sell_reasons = {}

        for code, pos in list(positions.items()):
            close_price = get_stock_price(today, code, 'close')
            high_price = get_stock_price(today, code, 'high')
            low_price = get_stock_price(today, code, 'low')

            if close_price is None:
                continue

            if high_price is None:
                high_price = close_price
            if low_price is None:
                low_price = close_price

            # 更新价格
            pos.update_price(close_price, high_price)

            # 判断是否在今天的候选名单中
            if code in today_candidate_codes:
                pos.out_list_count = 0
            else:
                pos.out_list_count += 1

            # --- 止损止盈判断 ---
            sell_reason = None

            # 规则1: 硬止损（日内最低价触及止损线就卖）
            intraday_low_pnl = (low_price - pos.buy_price) / pos.buy_price * 100
            if intraday_low_pnl <= hard_stop_loss:
                sell_price = pos.buy_price * (1 + hard_stop_loss / 100)
                sell_price = max(sell_price, low_price)
                sell_reason = '硬止损'
                pos.current_price = sell_price

            # 规则2: 移动止盈（从最高价回撤超过阈值）
            elif pos.high_since_buy > pos.buy_price * 1.02:
                if pos.drawdown_from_high <= trailing_stop:
                    sell_reason = '移动止盈'

            # 规则3: 连续不在名单+亏损
            if sell_reason is None and pos.out_list_count >= out_list_days and pos.pnl_pct < 0:
                sell_reason = '出名单+亏损'

            # 规则4: 连续不在名单+盈利回吐
            if sell_reason is None and pos.out_list_count >= out_list_days + 1 and pos.pnl_pct < 2:
                sell_reason = '出名单+盈利不足'

            # 规则5: 最大持仓天数
            if sell_reason is None and pos.hold_days >= max_hold_days:
                sell_reason = '达到最大持仓天数'

            if sell_reason:
                to_sell.append(code)
                sell_reasons[code] = sell_reason

        # ============================================================
        # 3. 执行卖出
        # ============================================================
        for code in to_sell:
            pos = positions[code]
            all_trades.append({
                'code': code,
                'name': pos.name,
                'industry': pos.industry,
                'buy_date': pos.buy_date,
                'sell_date': today,
                'buy_price': pos.buy_price,
                'sell_price': pos.current_price,
                'hold_days': pos.hold_days,
                'pnl_pct': pos.pnl_pct,
                'high_since_buy': pos.high_since_buy,
                'sell_reason': sell_reasons[code],
            })

            if verbose:
                tag = '🟢' if pos.pnl_pct > 0 else '🔴'
                print(f"    {tag} 卖出 {pos.name}({code}) 持{pos.hold_days}天 "
                      f"盈亏{pos.pnl_pct:+.2f}% 原因:{sell_reasons[code]}")

            del positions[code]

        # ============================================================
        # 4. 执行买入（填补空仓位）
        # ============================================================
        available_slots = max_positions - len(positions)

        if available_slots > 0 and today_candidate_codes and today <= last_score_date:
            new_candidates = []
            for code in today_candidate_codes:
                if code not in positions:
                    detail = today_candidate_details.get(code, {})
                    new_candidates.append((code, detail))

            new_candidates.sort(key=lambda x: x[1].get('total', 0), reverse=True)

            bought_count = 0
            for code, detail in new_candidates[:available_slots]:
                open_price = get_stock_price(today, code, 'open')
                if open_price is None or open_price <= 0:
                    continue

                pos = Position(
                    code=code,
                    name=detail.get('name', ''),
                    industry=detail.get('industry', ''),
                    buy_date=today,
                    buy_price=open_price,
                    score_info=detail,
                )
                close_price = get_stock_price(today, code, 'close')
                high_price = get_stock_price(today, code, 'high')
                if close_price:
                    pos.update_price(close_price, high_price or close_price)

                positions[code] = pos
                bought_count += 1

                if verbose:
                    print(f"    🔵 买入 {pos.name}({code}) 开盘价{open_price:.2f}")

            if verbose and bought_count > 0:
                print(f"    新买入{bought_count}只，当前持仓{len(positions)}只")

        # ============================================================
        # 5. 计算当日组合收益（等权净值法）
        # 每个持仓占 1/max_positions 的权重
        # 日收益 = sum(每个持仓的日涨跌幅) / max_positions
        # ============================================================
        daily_stock_returns = []
        for code, pos in positions.items():
            prev_p = prev_prices.get(code)
            if prev_p and prev_p > 0:
                # 老持仓：用昨收→今收计算日收益
                stock_daily_ret = (pos.current_price - prev_p) / prev_p * 100
            else:
                # 新买入：用开盘→收盘计算当日收益
                stock_daily_ret = (pos.current_price - pos.buy_price) / pos.buy_price * 100
            daily_stock_returns.append(stock_daily_ret)

        # 也要算上已卖出的票的当日收益（卖出价 vs 昨收）
        for code in to_sell:
            trade = all_trades[-len(to_sell) + to_sell.index(code)]  # 找到对应的交易记录
            prev_p = prev_prices.get(code)
            if prev_p and prev_p > 0:
                stock_daily_ret = (trade['sell_price'] - prev_p) / prev_p * 100
                daily_stock_returns.append(stock_daily_ret)

        # 组合日收益 = 持仓股票的等权平均收益 × (持仓数/最大持仓数)
        n_active = len(daily_stock_returns)
        if n_active > 0:
            avg_stock_ret = np.mean(daily_stock_returns)
            # 仓位利用率：实际持仓/最大持仓
            position_ratio = min(n_active, max_positions) / max_positions
            daily_return = avg_stock_ret * position_ratio
        else:
            daily_return = 0
            avg_stock_ret = 0

        nav *= (1 + daily_return / 100)

        daily_records.append({
            'date': today,
            'n_positions': len(positions),
            'n_candidates': len(today_candidate_codes),
            'daily_return': daily_return,
            'nav': nav,
            'avg_stock_ret': avg_stock_ret,
            'n_sells': len(to_sell),
            'n_buys': bought_count if 'bought_count' in dir() else 0,
        })

        if verbose and (len(to_sell) > 0 or today <= last_score_date):
            print(f"  [{today}] 持仓{len(positions)}只 日收益{daily_return:+.3f}% "
                  f"净值{nav:.4f}")

    # ============================================================
    # 6. 强制清仓剩余持仓
    # ============================================================
    for code, pos in list(positions.items()):
        all_trades.append({
            'code': code,
            'name': pos.name,
            'industry': pos.industry,
            'buy_date': pos.buy_date,
            'sell_date': today if daily_records else 'N/A',
            'buy_price': pos.buy_price,
            'sell_price': pos.current_price,
            'hold_days': pos.hold_days,
            'pnl_pct': pos.pnl_pct,
            'high_since_buy': pos.high_since_buy,
            'sell_reason': '回测结束清仓',
        })

    # ============================================================
    # 7. 统计结果
    # ============================================================
    if not daily_records:
        return None

    daily_df = pd.DataFrame(daily_records)
    trades_df = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()

    # 净值曲线
    nav_series = daily_df['nav']
    cum_return = (nav_series.iloc[-1] - 1) * 100

    # 最大回撤
    peak = nav_series.expanding().max()
    drawdown = (nav_series - peak) / peak * 100
    max_dd = abs(drawdown.min())

    # 日收益统计
    daily_rets = daily_df['daily_return']
    # 只统计有持仓的日子
    active_days = daily_df[daily_df['n_positions'] > 0]
    active_rets = active_days['daily_return'] if len(active_days) > 0 else daily_rets

    # 交易统计
    if len(trades_df) > 0:
        trade_pnls = trades_df['pnl_pct']
        wins = trade_pnls[trade_pnls > 0]
        losses = trade_pnls[trade_pnls <= 0]
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.01
        profit_ratio = avg_win / avg_loss if avg_loss > 0 else 99
        avg_hold_days = trades_df['hold_days'].mean()

        # 止损原因统计
        reason_stats = trades_df['sell_reason'].value_counts().to_dict()
    else:
        trade_pnls = pd.Series(dtype=float)
        wins = pd.Series(dtype=float)
        losses = pd.Series(dtype=float)
        profit_ratio = 0
        avg_hold_days = 0
        reason_stats = {}

    n_trading_days = len(daily_df)
    annual_return = ((nav_series.iloc[-1]) ** (250 / n_trading_days) - 1) * 100 if n_trading_days > 0 else 0

    # Sharpe
    if active_rets.std() > 0:
        sharpe = active_rets.mean() / active_rets.std() * np.sqrt(250)
    else:
        sharpe = 0

    # 月度收益
    daily_df['month'] = daily_df['date'].astype(str).str[:6]
    monthly_rets = daily_df.groupby('month')['daily_return'].sum()

    result = {
        'label': label,
        'params': params,
        'n_trading_days': n_trading_days,
        'n_active_days': len(active_days),
        'total_trades': len(trades_df),
        'avg_hold_days': avg_hold_days,
        'cum_return': cum_return,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'win_rate': len(wins) / max(len(trades_df), 1) * 100,
        'profit_ratio': profit_ratio,
        'sharpe': sharpe,
        'avg_daily_return': active_rets.mean() if len(active_rets) > 0 else 0,
        'daily_win_rate': (active_rets > 0).sum() / max(len(active_rets), 1) * 100,
        'avg_positions': daily_df['n_positions'].mean(),
        'best_trade': trade_pnls.max() if len(trade_pnls) > 0 else 0,
        'worst_trade': trade_pnls.min() if len(trade_pnls) > 0 else 0,
        'best_day': active_rets.max() if len(active_rets) > 0 else 0,
        'worst_day': active_rets.min() if len(active_rets) > 0 else 0,
        'reason_stats': reason_stats,
        'monthly_rets': monthly_rets.to_dict(),
        'trades_df': trades_df,
        'daily_df': daily_df,
    }

    return result


# ============================================================
# 对比基准：每天清仓换股（原有逻辑）
# ============================================================

def run_daily_rotate_baseline(detail_df, top_n=10):
    """
    基准策略：每天清仓换股（和elite_backtest一样）
    用于和持仓延续策略做对比
    """
    detail_dates = sorted(detail_df['date'].unique())
    all_trading_dates = get_all_trading_dates()

    daily_returns = []
    daily_details = []

    for date in detail_dates:
        day_data = detail_df[detail_df['date'] == date].copy()
        selected = select_stocks(day_data, top_n)

        if len(selected) == 0:
            continue

        valid_rets = selected['ret_1d'].dropna()
        if len(valid_rets) == 0:
            continue

        avg_ret = valid_rets.mean()
        daily_returns.append(avg_ret)
        daily_details.append({
            'date': str(date),
            'n_stocks': len(valid_rets),
            'avg_ret': avg_ret,
            'win_rate': (valid_rets > 0).sum() / len(valid_rets) * 100,
        })

    if not daily_returns:
        return None

    rets = np.array(daily_returns)
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in rets:
        cum *= (1 + r / 100)
        if cum > peak: peak = cum
        dd = (peak - cum) / peak * 100
        if dd > max_dd: max_dd = dd

    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.01
    profit_ratio = avg_win / avg_loss if avg_loss > 0 else 99

    n_days = len(rets)
    annual_return = ((cum) ** (250 / n_days) - 1) * 100 if n_days > 0 else 0

    return {
        'label': '基准：每天清仓换股TOP10',
        'n_trading_days': n_days,
        'n_active_days': n_days,
        'total_trades': sum(d['n_stocks'] for d in daily_details),
        'avg_hold_days': 1.0,
        'cum_return': (cum - 1) * 100,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'win_rate': len(wins) / len(rets) * 100,
        'profit_ratio': profit_ratio,
        'sharpe': rets.mean() / rets.std() * np.sqrt(250) if rets.std() > 0 else 0,
        'avg_daily_return': rets.mean(),
        'daily_win_rate': (rets > 0).sum() / len(rets) * 100,
        'avg_positions': np.mean([d['n_stocks'] for d in daily_details]),
        'best_trade': rets.max(),
        'worst_trade': rets.min(),
        'best_day': rets.max(),
        'worst_day': rets.min(),
        'reason_stats': {'每日清仓': sum(d['n_stocks'] for d in daily_details)},
        'monthly_rets': {},
        'trades_df': pd.DataFrame(),
        'daily_df': pd.DataFrame(daily_details),
    }


# ============================================================
# 打印结果
# ============================================================

def print_result(r, show_monthly=False, show_reasons=True):
    """打印单个策略结果"""
    if r is None:
        print("  ❌ 无有效数据")
        return

    tag = '🏆' if r['cum_return'] > 50 else ('✅' if r['cum_return'] > 0 else '❌')
    print(f"\n  {tag} {r['label']}")
    print(f"     交易日: {r['n_trading_days']}天 | 有持仓天数: {r['n_active_days']}天 | 日均持仓: {r['avg_positions']:.1f}只")
    print(f"     总交易次数: {r['total_trades']}次 | 平均持仓天数: {r['avg_hold_days']:.1f}天")
    print(f"     累计收益: {r['cum_return']:+.1f}% | 年化: {r['annual_return']:+.1f}% | 最大回撤: -{r['max_dd']:.1f}%")
    print(f"     交易胜率: {r['win_rate']:.1f}% | 盈亏比: {r['profit_ratio']:.2f} | Sharpe: {r['sharpe']:.2f}")
    print(f"     日均收益: {r['avg_daily_return']:+.4f}% | 日胜率: {r['daily_win_rate']:.1f}%")
    print(f"     最佳交易: {r['best_trade']:+.2f}% | 最差交易: {r['worst_trade']:+.2f}%")
    print(f"     最好一天: {r['best_day']:+.2f}% | 最差一天: {r['worst_day']:+.2f}%")

    if show_reasons and r.get('reason_stats'):
        print(f"     卖出原因分布:")
        for reason, count in sorted(r['reason_stats'].items(), key=lambda x: -x[1]):
            pct = count / max(r['total_trades'], 1) * 100
            print(f"       {reason}: {count}次 ({pct:.1f}%)")

    if show_monthly and r.get('monthly_rets'):
        print(f"     月度收益:")
        for month, ret in sorted(r['monthly_rets'].items()):
            tag_m = '🟢' if ret > 0 else '🔴'
            print(f"       {tag_m} {month}: {ret:+.2f}%")


# ============================================================
# 主程序
# ============================================================

def main():
    t_start = time.time()
    print("=" * 110)
    print("📊 持仓延续 + 趋势止损 回测引擎")
    print("=" * 110)

    # 加载数据
    print(f"\n加载数据: {DETAIL_FILE}")
    df = pd.read_csv(DETAIL_FILE)
    df = df[df['ret_1d'].notna()].copy()
    print(f"数据: {len(df)}条样本, {df['date'].nunique()}个交易日")
    print(f"区间: {df['date'].min()} ~ {df['date'].max()}")

    all_results = {}

    # ============================================================
    # 一、基准策略（每天清仓换股）
    # ============================================================
    print(f"\n{'=' * 110}")
    print("📊 一、基准策略（每天清仓换股）")
    print(f"{'=' * 110}")

    baseline = run_daily_rotate_baseline(df, top_n=10)
    print_result(baseline)
    if baseline:
        all_results['baseline'] = baseline

    # ============================================================
    # 二、持仓延续+趋势止损（多组参数对比）
    # ============================================================
    print(f"\n{'=' * 110}")
    print("📊 二、持仓延续+趋势止损（多组参数对比）")
    print(f"{'=' * 110}")

    for key, params in PARAM_SETS.items():
        print(f"\n--- 正在回测: {params['label']} ---")
        t0 = time.time()
        result = run_hold_backtest(df, params, verbose=False)
        elapsed = time.time() - t0
        print(f"    耗时: {elapsed:.1f}秒")
        print_result(result, show_monthly=False, show_reasons=True)
        if result:
            all_results[key] = result

    # ============================================================
    # 三、综合排名对比
    # ============================================================
    print(f"\n{'=' * 110}")
    print("📊 三、综合排名对比")
    print(f"{'=' * 110}")

    sorted_results = sorted(all_results.values(), key=lambda x: x['cum_return'], reverse=True)

    print(f"\n{'排名':>4} {'策略':<55} {'累计收益':>10} {'年化':>8} {'回撤':>8} {'胜率':>7} {'盈亏比':>7} {'Sharpe':>7} {'均持天':>7} {'交易数':>7}")
    print("-" * 140)

    for i, r in enumerate(sorted_results, 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<55} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% "
              f"-{r['max_dd']:>6.1f}% {r['win_rate']:>6.1f}% {r['profit_ratio']:>6.2f} "
              f"{r['sharpe']:>6.2f} {r['avg_hold_days']:>6.1f} {r['total_trades']:>6}")

    # ============================================================
    # 四、风险收益比分析
    # ============================================================
    print(f"\n{'=' * 110}")
    print("📊 四、风险收益比分析（收益/回撤）")
    print(f"{'=' * 110}")

    risk_reward = []
    for r in sorted_results:
        rr = r['cum_return'] / max(r['max_dd'], 0.1)
        risk_reward.append((r['label'], rr, r['cum_return'], r['max_dd']))

    risk_reward.sort(key=lambda x: -x[1])
    print(f"\n{'排名':>4} {'策略':<55} {'收益/回撤':>10} {'累计收益':>10} {'最大回撤':>10}")
    print("-" * 100)
    for i, (label, rr, cum, dd) in enumerate(risk_reward, 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {label:<55} {rr:>9.2f} {cum:>+9.1f}% -{dd:>8.1f}%")

    # ============================================================
    # 五、核心结论
    # ============================================================
    print(f"\n{'=' * 110}")
    print("📊 五、核心结论")
    print(f"{'=' * 110}")

    if sorted_results:
        best_return = sorted_results[0]
        best_rr = max(all_results.values(), key=lambda x: x['cum_return'] / max(x['max_dd'], 0.1))
        best_sharpe = max(all_results.values(), key=lambda x: x['sharpe'])

        print(f"\n  🥇 最高收益: {best_return['label']}")
        print(f"     累计{best_return['cum_return']:+.1f}% 回撤-{best_return['max_dd']:.1f}%")

        print(f"\n  🥇 最佳风险收益比: {best_rr['label']}")
        rr = best_rr['cum_return'] / max(best_rr['max_dd'], 0.1)
        print(f"     收益/回撤={rr:.2f} 累计{best_rr['cum_return']:+.1f}% 回撤-{best_rr['max_dd']:.1f}%")

        print(f"\n  🥇 最佳Sharpe: {best_sharpe['label']}")
        print(f"     Sharpe={best_sharpe['sharpe']:.2f} 累计{best_sharpe['cum_return']:+.1f}%")

        # 和基准对比
        if baseline:
            print(f"\n  📊 vs 基准（每天清仓换股）:")
            for key, r in all_results.items():
                if key == 'baseline':
                    continue
                dd_improve = baseline['max_dd'] - r['max_dd']
                ret_diff = r['cum_return'] - baseline['cum_return']
                print(f"     {r['label'][:40]}: 收益{ret_diff:+.1f}% 回撤改善{dd_improve:+.1f}%")

    total_time = time.time() - t_start
    print(f"\n{'=' * 110}")
    print(f"✅ 回测完成！总耗时: {total_time:.1f}秒")
    print(f"{'=' * 110}")


if __name__ == '__main__':
    main()
