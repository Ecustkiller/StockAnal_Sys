#!/usr/bin/env python3
"""
最优选股+持仓管理 回测方案
================================================================
综合之前所有回测发现，设计最优方案：

【核心发现】
1. D4安全边际 是最稳维度（+174.6%，回撤仅-27.2%）
2. D1多周期共振 是最强维度（+199.0%）
3. D2主线热点 和 D5基本面 是反向指标（高分组收益反而低）
4. 非涨停票 远优于涨停票（+191.2% vs 基准+48.3%）
5. Mistery 和 TXCG 是最有效子维度
6. 持仓延续+趋势止损 能将回撤从-39%降至-18%
7. D4+D3组合 是最佳风险收益比（6.95）

【新方案设计】
A. 重新设计评分权重（降低D2/D5，提升D4/D1/Mistery）
B. 非涨停优先 + 行业分散
C. 持仓延续 + 趋势止损
D. 多组参数对比

数据来源: backtest_v2 的 detail CSV + daily_snapshot
================================================================
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from collections import defaultdict

# ============================================================
# 配置
# ============================================================
DETAIL_FILE = 'backtest_results/backtest_v2_detail_20260420_052244.csv'
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")

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
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'pct_chg']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    _snap_cache[date_str] = df
    return df


def get_all_trading_dates():
    """获取所有交易日期"""
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


# ============================================================
# 最优评分函数（基于回测发现重新设计权重）
# ============================================================

def calc_optimal_score(row):
    """
    最优评分 v3.0
    
    基于回测发现重新设计权重：
    - D4安全边际: 权重大幅提升（最稳维度）
    - D1多周期共振: 权重提升（最强维度）
    - D3三Skill中的Mistery/TXCG: 权重提升（最有效子维度）
    - D2主线热点: 权重大幅降低（反向指标）
    - D5基本面: 权重大幅降低（反向指标）
    - D9百胜WR: 保持（WR2有效）
    - 非涨停加分
    - 风险控制加强
    """
    # === 核心维度得分（归一化到0-100） ===
    d1_norm = row.get('d1', 0) / 15 * 100       # 多周期共振
    d2_norm = row.get('d2', 0) / 25 * 100       # 主线热点（反向指标，降权）
    d3_norm = row.get('d3', 0) / 47 * 100       # 三Skill
    d4_norm = row.get('d4', 0) / 15 * 100       # 安全边际
    d5_norm = row.get('d5', 0) / 15 * 100       # 基本面（反向指标，降权）
    d9_norm = row.get('d9', 0) / 15 * 100       # 百胜WR
    
    # === 子维度得分 ===
    mistery_norm = row.get('mistery', 0) / 20 * 100
    tds_norm = row.get('tds', 0) / 12 * 100
    txcg_norm = row.get('txcg', 0) / 5 * 100
    wr2_norm = row.get('wr2', 0) / 5 * 100
    
    # === 新权重分配（总计100分）===
    # 核心发现：D4最稳、D1最强、Mistery/TXCG最有效、D2/D5反向
    score = 0
    score += d4_norm * 0.22    # D4安全边际 22%（原10%→大幅提升）
    score += d1_norm * 0.18    # D1多周期共振 18%（原10%→提升）
    score += mistery_norm * 0.15  # Mistery 15%（从D3中独立出来）
    score += txcg_norm * 0.10  # TXCG 10%（从D3中独立出来）
    score += wr2_norm * 0.10   # WR2 10%（有效子维度）
    score += tds_norm * 0.08   # TDS 8%
    score += d9_norm * 0.07    # D9百胜WR 7%
    score += d2_norm * 0.05    # D2主线热点 5%（反向指标，大幅降权）
    score += d5_norm * 0.05    # D5基本面 5%（反向指标，大幅降权）
    
    # === 加分项 ===
    # 非涨停加分（回测发现非涨停票远优于涨停票）
    if not row.get('is_zt', True):
        score += 8
    
    # WR2高分加分（WR2≥3的票表现显著好）
    wr2 = row.get('wr2', 0)
    if wr2 >= 4:
        score += 10
    elif wr2 >= 3:
        score += 6
    
    # Mistery高分加分
    mistery = row.get('mistery', 0)
    if mistery >= 15:
        score += 8
    elif mistery >= 12:
        score += 5
    
    # TXCG有分加分
    txcg = row.get('txcg', 0)
    if txcg >= 3:
        score += 6
    elif txcg >= 2:
        score += 3
    
    # 安全边际高加分
    d4 = row.get('d4', 0)
    if d4 >= 12:
        score += 5
    elif d4 >= 10:
        score += 3
    
    # === 扣分项 ===
    # 风险扣分
    net_risk = row.get('net_risk', 0)
    if net_risk > 0:
        score -= net_risk * 2
    
    # 5日涨幅过大扣分（追高风险）
    r5 = row.get('r5', 0)
    if r5 > 20:
        score -= 10
    elif r5 > 15:
        score -= 5
    
    # 资金净流出扣分
    nb_yi = row.get('nb_yi', 0)
    if pd.isna(nb_yi):
        nb_yi = 0
    if nb_yi < -1:
        score -= 5
    elif nb_yi < -0.5:
        score -= 2
    
    # 资金净流入加分
    if nb_yi > 1:
        score += 3
    elif nb_yi > 0:
        score += 1
    
    return score


def select_optimal(day_df, n, max_per_ind=2):
    """
    最优选股：评分+行业分散
    """
    day_df = day_df.copy()
    day_df['opt_score'] = day_df.apply(calc_optimal_score, axis=1)
    day_df = day_df.sort_values('opt_score', ascending=False)
    
    # 行业分散选股
    selected = []
    ind_count = defaultdict(int)
    for _, row in day_df.iterrows():
        ind = row.get('industry', '未知')
        if pd.isna(ind):
            ind = '未知'
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
        self.buy_date = buy_date
        self.buy_price = buy_price
        self.high_since_buy = buy_price
        self.hold_days = 0
        self.out_list_count = 0
        self.current_price = buy_price
        self.prev_close = buy_price  # 昨日收盘价
        self.score_info = score_info or {}

    @property
    def pnl_pct(self):
        if self.buy_price <= 0:
            return 0
        return (self.current_price - self.buy_price) / self.buy_price * 100

    @property
    def drawdown_from_high(self):
        if self.high_since_buy <= 0:
            return 0
        return (self.current_price - self.high_since_buy) / self.high_since_buy * 100

    @property
    def daily_return(self):
        """当日收益率"""
        if self.prev_close <= 0:
            return 0
        return (self.current_price - self.prev_close) / self.prev_close * 100

    def update_price(self, close_price, high_price):
        self.prev_close = self.current_price
        self.current_price = close_price
        if high_price > self.high_since_buy:
            self.high_since_buy = high_price
        self.hold_days += 1


# ============================================================
# 回测引擎
# ============================================================

def run_optimal_backtest(detail_df, params, verbose=False):
    """
    最优方案回测引擎
    
    选股：使用最优评分函数 + 行业分散
    持仓：延续持有 + 趋势止损
    """
    hard_stop_loss = params['hard_stop_loss']
    trailing_stop = params['trailing_stop']
    trailing_activate = params.get('trailing_activate', 2.0)  # 移动止盈激活阈值
    out_list_days = params['out_list_days']
    max_hold_days = params['max_hold_days']
    top_n = params['top_n']
    max_positions = params['max_positions']
    max_per_ind = params.get('max_per_ind', 2)
    label = params['label']
    
    # 获取交易日期
    all_trading_dates = get_all_trading_dates()
    detail_dates = sorted(detail_df['date'].unique())
    
    # 构建评分日→候选名单
    daily_candidates = {}
    for date in detail_dates:
        day_data = detail_df[detail_df['date'] == date].copy()
        selected = select_optimal(day_data, top_n, max_per_ind)
        if len(selected) > 0:
            daily_candidates[str(date)] = {
                'codes': set(selected['code'].tolist()),
                'details': {row['code']: row.to_dict() for _, row in selected.iterrows()},
            }
    
    # 找到起始交易日
    first_score_date = str(detail_dates[0])
    last_score_date = str(detail_dates[-1])
    trading_dates = [d for d in all_trading_dates if d >= first_score_date]
    
    start_idx = None
    for i, d in enumerate(trading_dates):
        if d > first_score_date:
            start_idx = i
            break
    if start_idx is None:
        return None
    
    # 逐日模拟
    positions = {}
    nav = 1.0
    daily_records = []
    all_trades = []
    
    for day_idx in range(start_idx, len(trading_dates)):
        today = trading_dates[day_idx]
        yesterday = trading_dates[day_idx - 1] if day_idx > 0 else None
        
        if today > last_score_date and not positions:
            break
        if today > last_score_date:
            days_after = sum(1 for d in trading_dates if last_score_date < d <= today)
            if days_after > max_hold_days + 5:
                break
        
        score_date = yesterday
        today_candidates = daily_candidates.get(score_date, {})
        today_candidate_codes = today_candidates.get('codes', set())
        today_candidate_details = today_candidates.get('details', {})
        
        # 1. 记录昨日收盘价
        prev_prices = {}
        for code, pos in positions.items():
            prev_prices[code] = pos.current_price
        
        # 2. 更新持仓价格 & 判断止损止盈
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
            
            pos.update_price(close_price, high_price)
            
            if code in today_candidate_codes:
                pos.out_list_count = 0
            else:
                pos.out_list_count += 1
            
            sell_reason = None
            
            # 规则1: 硬止损
            intraday_low_pnl = (low_price - pos.buy_price) / pos.buy_price * 100
            if intraday_low_pnl <= hard_stop_loss:
                sell_price = pos.buy_price * (1 + hard_stop_loss / 100)
                sell_price = max(sell_price, low_price)
                sell_reason = '硬止损'
                pos.current_price = sell_price
            
            # 规则2: 移动止盈（从最高价回撤超过阈值）
            elif pos.high_since_buy > pos.buy_price * (1 + trailing_activate / 100):
                if pos.drawdown_from_high <= trailing_stop:
                    sell_reason = '移动止盈'
            
            # 规则3: 连续不在名单+亏损
            if sell_reason is None and pos.out_list_count >= out_list_days and pos.pnl_pct < 0:
                sell_reason = '出名单+亏损'
            
            # 规则4: 连续不在名单+盈利不足
            if sell_reason is None and pos.out_list_count >= out_list_days + 1 and pos.pnl_pct < 2:
                sell_reason = '出名单+盈利不足'
            
            # 规则5: 最大持仓天数
            if sell_reason is None and pos.hold_days >= max_hold_days:
                sell_reason = '达到最大持仓天数'
            
            if sell_reason:
                to_sell.append(code)
                sell_reasons[code] = sell_reason
        
        # 3. 执行卖出
        for code in to_sell:
            pos = positions[code]
            all_trades.append({
                'code': code, 'name': pos.name, 'industry': pos.industry,
                'buy_date': pos.buy_date, 'sell_date': today,
                'buy_price': pos.buy_price, 'sell_price': pos.current_price,
                'hold_days': pos.hold_days, 'pnl_pct': pos.pnl_pct,
                'high_since_buy': pos.high_since_buy,
                'sell_reason': sell_reasons[code],
            })
            if verbose:
                tag = '🟢' if pos.pnl_pct > 0 else '🔴'
                print(f"    {tag} 卖出 {pos.name}({code}) 持{pos.hold_days}天 "
                      f"盈亏{pos.pnl_pct:+.2f}% 原因:{sell_reasons[code]}")
            del positions[code]
        
        # 4. 执行买入
        available_slots = max_positions - len(positions)
        bought_count = 0
        
        if available_slots > 0 and today_candidate_codes and today <= last_score_date:
            new_candidates = []
            for code in today_candidate_codes:
                if code not in positions:
                    detail = today_candidate_details.get(code, {})
                    new_candidates.append((code, detail))
            
            # 按最优评分排序
            new_candidates.sort(key=lambda x: x[1].get('opt_score', x[1].get('total', 0)), reverse=True)
            
            for code, detail in new_candidates[:available_slots]:
                open_price = get_stock_price(today, code, 'open')
                if open_price is None or open_price <= 0:
                    continue
                
                pos = Position(
                    code=code, name=detail.get('name', ''),
                    industry=detail.get('industry', ''),
                    buy_date=today, buy_price=open_price,
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
        
        # 5. 计算当日组合收益
        daily_stock_returns = []
        
        for code, pos in positions.items():
            prev_p = prev_prices.get(code)
            if prev_p and prev_p > 0:
                stock_daily_ret = (pos.current_price - prev_p) / prev_p * 100
            else:
                stock_daily_ret = (pos.current_price - pos.buy_price) / pos.buy_price * 100
            daily_stock_returns.append(stock_daily_ret)
        
        # 已卖出的票的当日收益
        for code in to_sell:
            prev_p = prev_prices.get(code)
            if prev_p and prev_p > 0:
                trade = next(t for t in reversed(all_trades) if t['code'] == code and t['sell_date'] == today)
                stock_daily_ret = (trade['sell_price'] - prev_p) / prev_p * 100
                daily_stock_returns.append(stock_daily_ret)
        
        n_active = len(daily_stock_returns)
        if n_active > 0:
            avg_stock_ret = np.mean(daily_stock_returns)
            position_ratio = min(n_active, max_positions) / max_positions
            daily_return = avg_stock_ret * position_ratio
        else:
            daily_return = 0
        
        nav *= (1 + daily_return / 100)
        
        daily_records.append({
            'date': today,
            'n_positions': len(positions),
            'daily_return': daily_return,
            'nav': nav,
            'n_sells': len(to_sell),
            'n_buys': bought_count,
        })
    
    # 强制清仓
    for code, pos in list(positions.items()):
        all_trades.append({
            'code': code, 'name': pos.name, 'industry': pos.industry,
            'buy_date': pos.buy_date, 'sell_date': today if daily_records else 'N/A',
            'buy_price': pos.buy_price, 'sell_price': pos.current_price,
            'hold_days': pos.hold_days, 'pnl_pct': pos.pnl_pct,
            'high_since_buy': pos.high_since_buy, 'sell_reason': '回测结束清仓',
        })
    
    # 统计结果
    if not daily_records:
        return None
    
    daily_df = pd.DataFrame(daily_records)
    trades_df = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()
    
    nav_series = daily_df['nav']
    cum_return = (nav_series.iloc[-1] - 1) * 100
    
    peak = nav_series.expanding().max()
    drawdown = (nav_series - peak) / peak * 100
    max_dd = abs(drawdown.min())
    
    daily_rets = daily_df['daily_return']
    active_days = daily_df[daily_df['n_positions'] > 0]
    active_rets = active_days['daily_return'] if len(active_days) > 0 else daily_rets
    
    if len(trades_df) > 0:
        trade_pnls = trades_df['pnl_pct']
        wins = trade_pnls[trade_pnls > 0]
        losses = trade_pnls[trade_pnls <= 0]
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.01
        profit_ratio = avg_win / avg_loss if avg_loss > 0 else 99
        avg_hold_days = trades_df['hold_days'].mean()
        reason_stats = trades_df['sell_reason'].value_counts().to_dict()
    else:
        trade_pnls = pd.Series(dtype=float)
        wins = losses = pd.Series(dtype=float)
        profit_ratio = 0
        avg_hold_days = 0
        reason_stats = {}
    
    n_trading_days = len(daily_df)
    annual_return = ((nav_series.iloc[-1]) ** (250 / n_trading_days) - 1) * 100 if n_trading_days > 0 else 0
    sharpe = active_rets.mean() / active_rets.std() * np.sqrt(250) if len(active_rets) > 0 and active_rets.std() > 0 else 0
    
    # 月度收益
    daily_df['month'] = daily_df['date'].astype(str).str[:6]
    monthly_rets = daily_df.groupby('month')['daily_return'].sum()
    
    # 月度胜率
    monthly_wins = (monthly_rets > 0).sum()
    monthly_total = len(monthly_rets)
    
    return {
        'label': label,
        'params': params,
        'n_trading_days': n_trading_days,
        'n_active_days': len(active_days),
        'total_trades': len(trades_df),
        'avg_hold_days': avg_hold_days,
        'cum_return': cum_return,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'trade_win_rate': len(wins) / max(len(trades_df), 1) * 100,
        'daily_win_rate': (active_rets > 0).sum() / max(len(active_rets), 1) * 100,
        'profit_ratio': profit_ratio,
        'sharpe': sharpe,
        'avg_daily_return': active_rets.mean() if len(active_rets) > 0 else 0,
        'avg_positions': daily_df['n_positions'].mean(),
        'best_trade': trade_pnls.max() if len(trade_pnls) > 0 else 0,
        'worst_trade': trade_pnls.min() if len(trade_pnls) > 0 else 0,
        'best_day': active_rets.max() if len(active_rets) > 0 else 0,
        'worst_day': active_rets.min() if len(active_rets) > 0 else 0,
        'monthly_win_rate': monthly_wins / max(monthly_total, 1) * 100,
        'reason_stats': reason_stats,
        'monthly_rets': monthly_rets.to_dict(),
        'trades_df': trades_df,
        'daily_df': daily_df,
    }


# ============================================================
# 基准策略（每天清仓换股）
# ============================================================

def run_baseline(detail_df, selector_func, label, top_n=10, hold_period='ret_1d'):
    """基准策略：每天清仓换股"""
    dates = sorted(detail_df['date'].unique())
    daily_returns = []
    
    for date in dates:
        day_df = detail_df[detail_df['date'] == date].copy()
        if len(day_df) == 0:
            continue
        
        selected = selector_func(day_df, top_n)
        if selected is None or len(selected) == 0:
            continue
        
        valid_rets = selected[hold_period].dropna()
        if len(valid_rets) == 0:
            continue
        
        daily_returns.append(valid_rets.mean())
    
    if not daily_returns:
        return None
    
    rets = np.array(daily_returns)
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in rets:
        cum *= (1 + r / 100)
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
    
    return {
        'label': label,
        'n_trading_days': n_days,
        'n_active_days': n_days,
        'total_trades': n_days * top_n,
        'avg_hold_days': 1.0,
        'cum_return': (cum - 1) * 100,
        'annual_return': annual_return,
        'max_dd': max_dd,
        'trade_win_rate': len(wins) / len(rets) * 100,
        'daily_win_rate': (rets > 0).sum() / len(rets) * 100,
        'profit_ratio': profit_ratio,
        'sharpe': rets.mean() / rets.std() * np.sqrt(250) if rets.std() > 0 else 0,
        'avg_daily_return': rets.mean(),
        'avg_positions': top_n,
        'best_trade': rets.max(),
        'worst_trade': rets.min(),
        'best_day': rets.max(),
        'worst_day': rets.min(),
        'monthly_win_rate': 0,
        'reason_stats': {},
        'monthly_rets': {},
        'trades_df': pd.DataFrame(),
        'daily_df': pd.DataFrame(),
    }


# ============================================================
# 选股函数（用于基准对比）
# ============================================================

def sel_total_top(day_df, n):
    """原始总分TOP"""
    return day_df.nlargest(n, 'total')

def sel_d4_d3_combo(day_df, n):
    """D4+D3组合（之前回测最佳风险收益比）"""
    day_df = day_df.copy()
    day_df['combo'] = day_df['d4'] / 15 * 40 + day_df['d3'] / 47 * 60
    return day_df.nlargest(n, 'combo')

def sel_optimal_daily(day_df, n):
    """最优评分每日清仓"""
    return select_optimal(day_df, n)

def sel_non_zt_total(day_df, n):
    """非涨停按总分"""
    filtered = day_df[day_df['is_zt'] == False]
    if len(filtered) < 3:
        filtered = day_df
    return filtered.nlargest(n, 'total')


# ============================================================
# 打印函数
# ============================================================

def print_result(r, show_reasons=True, show_monthly=False):
    if r is None:
        print("  ❌ 无有效数据")
        return
    
    tag = '🏆' if r['cum_return'] > 100 else ('✅' if r['cum_return'] > 0 else '❌')
    print(f"\n  {tag} {r['label']}")
    print(f"     交易日: {r['n_trading_days']}天 | 有持仓: {r['n_active_days']}天 | 日均持仓: {r['avg_positions']:.1f}只")
    print(f"     总交易: {r['total_trades']}次 | 平均持仓: {r['avg_hold_days']:.1f}天")
    print(f"     累计收益: {r['cum_return']:+.1f}% | 年化: {r['annual_return']:+.1f}% | 最大回撤: -{r['max_dd']:.1f}%")
    print(f"     交易胜率: {r['trade_win_rate']:.1f}% | 日胜率: {r['daily_win_rate']:.1f}% | 盈亏比: {r['profit_ratio']:.2f} | Sharpe: {r['sharpe']:.2f}")
    print(f"     日均收益: {r['avg_daily_return']:+.4f}% | 最佳交易: {r['best_trade']:+.2f}% | 最差交易: {r['worst_trade']:+.2f}%")
    
    if r.get('monthly_win_rate'):
        print(f"     月度胜率: {r['monthly_win_rate']:.1f}%")
    
    rr = r['cum_return'] / max(r['max_dd'], 0.1)
    print(f"     收益/回撤比: {rr:.2f}")
    
    if show_reasons and r.get('reason_stats'):
        print(f"     卖出原因:")
        for reason, count in sorted(r['reason_stats'].items(), key=lambda x: -x[1]):
            pct = count / max(r['total_trades'], 1) * 100
            print(f"       {reason}: {count}次 ({pct:.1f}%)")
    
    if show_monthly and r.get('monthly_rets'):
        print(f"     月度收益:")
        for month, ret in sorted(r['monthly_rets'].items()):
            tag_m = '🟢' if ret > 0 else '🔴'
            print(f"       {tag_m} {month}: {ret:+.2f}%")


def print_compare_table(results):
    """打印对比表格"""
    print(f"\n{'排名':>4} {'策略':<50} {'累计收益':>10} {'年化':>8} {'回撤':>8} {'交易胜率':>8} {'盈亏比':>7} "
          f"{'Sharpe':>7} {'收益/回撤':>9} {'均持天':>7}")
    print("-" * 145)
    
    sorted_results = sorted(results, key=lambda x: x['cum_return'], reverse=True)
    for i, r in enumerate(sorted_results, 1):
        tag = '🏆' if i <= 3 else '  '
        rr = r['cum_return'] / max(r['max_dd'], 0.1)
        print(f"{tag}{i:>3} {r['label']:<50} {r['cum_return']:>+9.1f}% {r['annual_return']:>+7.1f}% "
              f"-{r['max_dd']:>6.1f}% {r['trade_win_rate']:>7.1f}% {r['profit_ratio']:>6.2f} "
              f"{r['sharpe']:>6.2f} {rr:>8.2f} {r['avg_hold_days']:>6.1f}")
    
    return sorted_results


# ============================================================
# 主程序
# ============================================================

def main():
    t_start = time.time()
    print("=" * 145)
    print("📊 最优选股+持仓管理 回测方案 v3.0")
    print("=" * 145)
    print("""
设计依据（基于之前回测发现）：
  ✅ D4安全边际 权重22%（最稳维度，原10%→大幅提升）
  ✅ D1多周期共振 权重18%（最强维度，原10%→提升）
  ✅ Mistery 权重15%（最有效子维度，独立出来）
  ✅ TXCG 权重10%（最有效子维度，独立出来）
  ✅ WR2 权重10%（有效子维度）
  ⚠️ D2主线热点 权重5%（反向指标，原17%→大幅降权）
  ⚠️ D5基本面 权重5%（反向指标，原10%→大幅降权）
  ✅ 非涨停优先（+8分加分）
  ✅ 持仓延续 + 趋势止损
""")
    
    # 加载数据
    df = pd.read_csv(DETAIL_FILE)
    df = df[df['ret_1d'].notna()].copy()
    print(f"数据: {len(df)}条样本, {df['date'].nunique()}个交易日")
    print(f"区间: {df['date'].min()} ~ {df['date'].max()}")
    
    all_results = []
    
    # ============================================================
    # 一、基准策略（每天清仓换股）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 一、基准策略（每天清仓换股，T+1持有期）")
    print(f"{'=' * 145}")
    
    baselines = [
        (sel_total_top, "基准A: 原始总分TOP10（每天清仓）"),
        (sel_d4_d3_combo, "基准B: D4+D3组合TOP10（每天清仓）"),
        (sel_non_zt_total, "基准C: 非涨停按总分TOP10（每天清仓）"),
        (sel_optimal_daily, "基准D: 最优评分TOP10（每天清仓）"),
    ]
    
    for func, label in baselines:
        r = run_baseline(df, func, label, top_n=10)
        if r:
            print_result(r, show_reasons=False)
            all_results.append(r)
    
    # ============================================================
    # 二、最优方案 + 持仓延续（多组参数）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 二、最优评分 + 持仓延续 + 趋势止损（多组参数对比）")
    print(f"{'=' * 145}")
    
    param_sets = {
        'opt_conservative': {
            'label': '最优+保守型（止损-5% 止盈-5% 出名单2天 最长5天）',
            'hard_stop_loss': -5.0,
            'trailing_stop': -5.0,
            'trailing_activate': 2.0,
            'out_list_days': 2,
            'max_hold_days': 5,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 2,
        },
        'opt_moderate': {
            'label': '最优+稳健型（止损-7% 止盈-6% 出名单3天 最长8天）',
            'hard_stop_loss': -7.0,
            'trailing_stop': -6.0,
            'trailing_activate': 2.0,
            'out_list_days': 3,
            'max_hold_days': 8,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 2,
        },
        'opt_moderate_5': {
            'label': '最优+稳健5只（止损-7% 止盈-6% 出名单3天 最长8天 5只）',
            'hard_stop_loss': -7.0,
            'trailing_stop': -6.0,
            'trailing_activate': 2.0,
            'out_list_days': 3,
            'max_hold_days': 8,
            'top_n': 5,
            'max_positions': 5,
            'max_per_ind': 1,
        },
        'opt_trend': {
            'label': '最优+趋势型（止损-8% 止盈-8% 出名单3天 最长12天）',
            'hard_stop_loss': -8.0,
            'trailing_stop': -8.0,
            'trailing_activate': 3.0,
            'out_list_days': 3,
            'max_hold_days': 12,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 2,
        },
        'opt_aggressive': {
            'label': '最优+激进型（止损-10% 止盈-10% 出名单5天 最长15天）',
            'hard_stop_loss': -10.0,
            'trailing_stop': -10.0,
            'trailing_activate': 3.0,
            'out_list_days': 5,
            'max_hold_days': 15,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 2,
        },
        'opt_tight_stop': {
            'label': '最优+紧止损（止损-4% 止盈-4% 出名单2天 最长5天）',
            'hard_stop_loss': -4.0,
            'trailing_stop': -4.0,
            'trailing_activate': 1.5,
            'out_list_days': 2,
            'max_hold_days': 5,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 2,
        },
        'opt_wide_ind': {
            'label': '最优+宽行业（止损-7% 止盈-6% 同行业≤3只 最长8天）',
            'hard_stop_loss': -7.0,
            'trailing_stop': -6.0,
            'trailing_activate': 2.0,
            'out_list_days': 3,
            'max_hold_days': 8,
            'top_n': 10,
            'max_positions': 10,
            'max_per_ind': 3,
        },
    }
    
    for key, params in param_sets.items():
        print(f"\n--- 正在回测: {params['label']} ---")
        t0 = time.time()
        result = run_optimal_backtest(df, params, verbose=False)
        elapsed = time.time() - t0
        print(f"    耗时: {elapsed:.1f}秒")
        print_result(result, show_reasons=True, show_monthly=False)
        if result:
            all_results.append(result)
    
    # ============================================================
    # 三、综合排名
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 三、综合排名（按累计收益）")
    print(f"{'=' * 145}")
    sorted_results = print_compare_table(all_results)
    
    # ============================================================
    # 四、风险收益比排名
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 四、风险收益比排名（收益/回撤）")
    print(f"{'=' * 145}")
    
    rr_list = [(r, r['cum_return'] / max(r['max_dd'], 0.1)) for r in all_results]
    rr_list.sort(key=lambda x: -x[1])
    
    print(f"\n{'排名':>4} {'策略':<50} {'收益/回撤':>10} {'累计收益':>10} {'最大回撤':>10} {'Sharpe':>7}")
    print("-" * 105)
    for i, (r, rr) in enumerate(rr_list, 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<50} {rr:>9.2f} {r['cum_return']:>+9.1f}% -{r['max_dd']:>8.1f}% {r['sharpe']:>6.2f}")
    
    # ============================================================
    # 五、Sharpe排名
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 五、Sharpe排名")
    print(f"{'=' * 145}")
    
    sharpe_sorted = sorted(all_results, key=lambda x: x['sharpe'], reverse=True)
    print(f"\n{'排名':>4} {'策略':<50} {'Sharpe':>7} {'累计收益':>10} {'回撤':>8} {'交易胜率':>8} {'盈亏比':>7}")
    print("-" * 105)
    for i, r in enumerate(sharpe_sorted, 1):
        tag = '🏆' if i <= 3 else '  '
        print(f"{tag}{i:>3} {r['label']:<50} {r['sharpe']:>6.2f} {r['cum_return']:>+9.1f}% -{r['max_dd']:>6.1f}% "
              f"{r['trade_win_rate']:>7.1f}% {r['profit_ratio']:>6.2f}")
    
    # ============================================================
    # 六、月度收益对比（TOP3 vs 基准）
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 六、月度收益对比")
    print(f"{'=' * 145}")
    
    # 选出有月度数据的策略
    monthly_results = [r for r in sorted_results if r.get('monthly_rets')]
    if monthly_results:
        compare_list = monthly_results[:4]  # TOP4
        all_months = set()
        for r in compare_list:
            all_months.update(r['monthly_rets'].keys())
        all_months = sorted(all_months)
        
        header = f"{'月份':<10}"
        for r in compare_list:
            short = r['label'][:22]
            header += f" {short:>24}"
        print(header)
        print("-" * (10 + 25 * len(compare_list)))
        
        for month in all_months:
            line = f"{month:<10}"
            for r in compare_list:
                val = r['monthly_rets'].get(month, 0)
                tag_m = '🟢' if val > 0 else '🔴'
                line += f" {tag_m}{val:>+21.2f}%"
            print(line)
        
        # 汇总
        print()
        line = f"{'累计':<10}"
        for r in compare_list:
            total = sum(r['monthly_rets'].values())
            line += f" {total:>+23.1f}%"
        print(line)
    
    # ============================================================
    # 七、核心结论
    # ============================================================
    print(f"\n{'=' * 145}")
    print("📊 七、核心结论")
    print(f"{'=' * 145}")
    
    if sorted_results:
        best_return = sorted_results[0]
        best_rr_r, best_rr_val = rr_list[0]
        best_sharpe = sharpe_sorted[0]
        
        print(f"\n  🥇 最高收益: {best_return['label']}")
        print(f"     累计{best_return['cum_return']:+.1f}% 年化{best_return['annual_return']:+.1f}% "
              f"回撤-{best_return['max_dd']:.1f}% Sharpe{best_return['sharpe']:.2f}")
        
        print(f"\n  🥇 最佳风险收益比: {best_rr_r['label']}")
        print(f"     收益/回撤={best_rr_val:.2f} 累计{best_rr_r['cum_return']:+.1f}% 回撤-{best_rr_r['max_dd']:.1f}%")
        
        print(f"\n  🥇 最佳Sharpe: {best_sharpe['label']}")
        print(f"     Sharpe={best_sharpe['sharpe']:.2f} 累计{best_sharpe['cum_return']:+.1f}%")
        
        # 持仓延续 vs 每天清仓
        hold_results = [r for r in all_results if '持仓' in r['label'] or '最优+' in r['label']]
        daily_results = [r for r in all_results if '清仓' in r['label']]
        
        if hold_results and daily_results:
            best_hold = max(hold_results, key=lambda x: x['cum_return'])
            best_daily = max(daily_results, key=lambda x: x['cum_return'])
            
            print(f"\n  📊 持仓延续 vs 每天清仓:")
            print(f"     最优持仓延续: {best_hold['label'][:40]}")
            print(f"       累计{best_hold['cum_return']:+.1f}% 回撤-{best_hold['max_dd']:.1f}% "
                  f"盈亏比{best_hold['profit_ratio']:.2f}")
            print(f"     最优每天清仓: {best_daily['label'][:40]}")
            print(f"       累计{best_daily['cum_return']:+.1f}% 回撤-{best_daily['max_dd']:.1f}% "
                  f"盈亏比{best_daily['profit_ratio']:.2f}")
            
            dd_improve = best_daily['max_dd'] - best_hold['max_dd']
            ret_diff = best_hold['cum_return'] - best_daily['cum_return']
            print(f"\n     持仓延续 vs 清仓: 收益{ret_diff:+.1f}% 回撤改善{dd_improve:+.1f}%")
        
        # 最优评分 vs 原始总分
        opt_daily = next((r for r in all_results if '最优评分TOP10' in r['label']), None)
        orig_daily = next((r for r in all_results if '原始总分TOP10' in r['label']), None)
        
        if opt_daily and orig_daily:
            print(f"\n  📊 最优评分 vs 原始总分（每天清仓模式）:")
            print(f"     最优评分: 累计{opt_daily['cum_return']:+.1f}% 回撤-{opt_daily['max_dd']:.1f}%")
            print(f"     原始总分: 累计{orig_daily['cum_return']:+.1f}% 回撤-{orig_daily['max_dd']:.1f}%")
            diff = opt_daily['cum_return'] - orig_daily['cum_return']
            print(f"     最优评分超额: {diff:+.1f}%")
    
    total_time = time.time() - t_start
    print(f"\n{'=' * 145}")
    print(f"✅ 回测完成！总耗时: {total_time:.1f}秒")
    print(f"{'=' * 145}")


if __name__ == '__main__':
    main()
