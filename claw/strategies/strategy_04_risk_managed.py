#!/usr/bin/env python3
"""
策略04：风控增强因子优化策略 — T+1 每日换仓 + 动态仓位管理
================================================================
【核心思路】
  选股逻辑沿用策略03（因子优化精选），在此基础上叠加"市场环境识别 + 动态仓位管理"
  来降低回撤。核心理念：在市场环境恶化时主动降低仓位，保护利润。

【仓位管理算法】（严格无前视偏差，所有信号基于T-1日及之前的数据）
  采用多信号融合的仓位决策系统，5个独立信号加权平均：

  信号1 - 连续亏损控制 (权重25%)：
    连亏≥3天 → 0.2 | 连亏≥2天 → 0.4 | 连亏≥1天 → 0.6 | 否则 → 1.0
    原理：连续亏损是市场环境恶化的最直接信号，灵敏度提高到1天即响应

  信号2 - 波动率目标 (权重25%)：
    目标日波动率 = 年化10% / √250 ≈ 0.63%
    仓位 = min(1.0, 目标波动率 / 近20日实际波动率)
    原理：高波动时降仓，低波动时加仓，目标波动率更保守

  信号3 - 回撤控制 (权重25%)：
    回撤>18% → 0.2 | 回撤>10% → 0.4 | 回撤>5% → 0.7 | 否则 → 1.0
    原理：更早介入回撤控制，5%即开始降仓

  信号4 - 净值动量 (权重15%)：
    5日均线 > 20日均线×1.02 → 1.0（趋势向上）
    5日均线 > 20日均线 → 0.7（温和上涨）
    5日均线 < 20日均线 → 0.4（趋势向下）
    原理：顺势而为，趋势向下时减仓

  信号5 - 近期胜率 (权重10%)：
    近10日胜率<30% → 0.2 | <40% → 0.5 | >60% → 1.0 | 否则 → 0.7
    原理：胜率骤降说明策略与当前市场不匹配

  最终仓位 = 加权平均(信号1~5)，限制在 [0.1, 1.0] 范围内

【回测表现】（2021-2026，5年+，严格无前视偏差）
  ┌─────────────────────────────────────────────────────────────┐
  │ 🛡️ 风控增强 TOP10 T+1（极保守配置）                        │
  │   累计收益: +233.3%  │ 年化: +26.6%   │ Sharpe: 1.13       │
  │   最大回撤: -25.4%   │ Calmar: 1.05   │ 平均仓位: 52%      │
  ├─────────────────────────────────────────────────────────────┤
  │ vs 策略03 满仓:                                             │
  │   回撤: -56.6% → -25.4% (降31个百分点！)                   │
  │   Sharpe: 0.82 → 1.13 (+38%)                               │
  │   Calmar: 0.53 → 1.05 (+98%)                               │
  │   收益: +285% → +233% (降18%，用收益换回撤)                │
  └─────────────────────────────────────────────────────────────┘

【适用场景】
  - 追求更低回撤，愿意牺牲部分收益
  - 不想在大跌中硬扛，希望系统自动降仓
  - 能每天操作换仓
  - 希望最大回撤控制在25%以内
"""

import pandas as pd
import numpy as np
from collections import defaultdict

try:
    # 新工程化路径（包内引用）
    from claw.strategies.strategy_03_optimized import (
        select_optimized_elite, calc_optimized_score,
    )
except ImportError:  # pragma: no cover - 兼容旧的直接脚本运行
    from strategy_03_optimized import select_optimized_elite, calc_optimized_score


# ============================================================
# 仓位管理器：市场环境识别 + 动态仓位
# ============================================================
class PositionManager:
    """
    多信号融合仓位管理器
    
    所有信号严格基于T-1日及之前的数据，无前视偏差。
    每个信号输出一个 [0, 1] 的仓位建议，加权平均后得到最终仓位。
    """
    
    # 信号权重（基于参数敏感性测试，极保守配置Calmar最高=1.05）
    WEIGHTS = {
        'loss_streak': 0.25,   # 连续亏损控制
        'vol_target':  0.25,   # 波动率目标
        'dd_control':  0.25,   # 回撤控制
        'momentum':    0.15,   # 净值动量
        'win_rate':    0.10,   # 近期胜率
    }
    
    def __init__(self, target_annual_vol=10.0, lookback=20):
        """
        参数:
            target_annual_vol: 目标年化波动率(%)，默认10%（极保守配置）
            lookback: 回看窗口天数，默认20
        """
        self.target_daily_vol = target_annual_vol / np.sqrt(250)
        self.lookback = lookback
        
        # 历史状态
        self.returns_history = []   # 历史每日收益
        self.nav = 1.0              # 当前净值
        self.peak = 1.0             # 历史最高净值
        self.loss_streak = 0        # 连续亏损天数
        self.navs = [1.0]           # 净值序列
    
    def update(self, daily_return):
        """
        更新历史状态（在当日收盘后调用）
        
        参数:
            daily_return: 当日收益率(%)
        """
        self.returns_history.append(daily_return)
        self.nav *= (1 + daily_return / 100)
        self.navs.append(self.nav)
        if self.nav > self.peak:
            self.peak = self.nav
        
        if daily_return < 0:
            self.loss_streak += 1
        else:
            self.loss_streak = 0
    
    def get_position(self):
        """
        计算今日建议仓位（基于截至昨日的所有信息）
        
        返回:
            float: 仓位比例 [0.1, 1.0]
        """
        n = len(self.returns_history)
        
        # 数据不足时满仓
        if n < self.lookback:
            return 1.0
        
        signals = {}
        
        # ---- 信号1: 连续亏损控制（极保守：1天即响应）----
        if self.loss_streak >= 3:
            signals['loss_streak'] = 0.2
        elif self.loss_streak >= 2:
            signals['loss_streak'] = 0.4
        elif self.loss_streak >= 1:
            signals['loss_streak'] = 0.6
        else:
            signals['loss_streak'] = 1.0
        
        # ---- 信号2: 波动率目标 ----
        recent_rets = np.array(self.returns_history[-self.lookback:])
        recent_vol = recent_rets.std()
        if recent_vol > 0:
            signals['vol_target'] = min(1.0, self.target_daily_vol / recent_vol)
        else:
            signals['vol_target'] = 1.0
        
        # ---- 信号3: 回撤控制（极保守：5%即开始降仓）----
        dd = (self.peak - self.nav) / self.peak * 100 if self.peak > 0 else 0
        if dd > 18:
            signals['dd_control'] = 0.2
        elif dd > 10:
            signals['dd_control'] = 0.4
        elif dd > 5:
            signals['dd_control'] = 0.7
        else:
            signals['dd_control'] = 1.0
        
        # ---- 信号4: 净值动量 ----
        nav_arr = np.array(self.navs)
        if len(nav_arr) >= 20:
            ma5 = nav_arr[-5:].mean()
            ma20 = nav_arr[-20:].mean()
            if ma5 > ma20 * 1.02:
                signals['momentum'] = 1.0
            elif ma5 > ma20:
                signals['momentum'] = 0.7
            else:
                signals['momentum'] = 0.4
        else:
            signals['momentum'] = 0.7
        
        # ---- 信号5: 近期胜率 ----
        recent_10 = np.array(self.returns_history[-10:])
        win_rate = (recent_10 > 0).sum() / len(recent_10)
        if win_rate < 0.3:
            signals['win_rate'] = 0.2
        elif win_rate < 0.4:
            signals['win_rate'] = 0.5
        elif win_rate > 0.6:
            signals['win_rate'] = 1.0
        else:
            signals['win_rate'] = 0.7
        
        # ---- 加权融合 ----
        weighted_pos = sum(
            signals[k] * self.WEIGHTS[k] 
            for k in self.WEIGHTS if k in signals
        )
        
        # 限制范围
        final_pos = min(1.0, max(0.1, weighted_pos))
        
        return final_pos
    
    def get_signal_detail(self):
        """返回各信号的详细值（用于调试/展示）"""
        pos = self.get_position()
        dd = (self.peak - self.nav) / self.peak * 100 if self.peak > 0 else 0
        n = len(self.returns_history)
        
        detail = {
            'position': pos,
            'loss_streak': self.loss_streak,
            'drawdown': dd,
            'nav': self.nav,
            'peak': self.peak,
        }
        
        if n >= self.lookback:
            recent = np.array(self.returns_history[-self.lookback:])
            detail['vol_20d'] = recent.std() * np.sqrt(250)
            detail['win_rate_10d'] = (np.array(self.returns_history[-10:]) > 0).sum() / 10
        
        return detail
    
    def reset(self):
        """重置状态"""
        self.returns_history = []
        self.nav = 1.0
        self.peak = 1.0
        self.loss_streak = 0
        self.navs = [1.0]


# ============================================================
# 策略04：选股 + 仓位管理
# ============================================================
def select_risk_managed(day_df, n=10, max_per_ind=3, position_manager=None):
    """
    风控增强因子优化策略
    
    选股逻辑与策略03完全相同，额外返回仓位建议。
    
    参数:
        day_df: 当日TOP30评分数据
        n: 选股数量（默认10）
        max_per_ind: 同行业最多几只
        position_manager: 仓位管理器实例（可选）
    
    返回:
        (selected_df, position): 选中的股票DataFrame 和 建议仓位(0~1)
    """
    # 选股逻辑完全复用策略03
    selected = select_optimized_elite(day_df, n, max_per_ind)
    
    # 获取仓位建议
    if position_manager is not None:
        position = position_manager.get_position()
    else:
        position = 1.0
    
    return selected, position


# ============================================================
# 快捷调用接口
# ============================================================
_default_pm = None

def get_position_manager():
    """获取全局仓位管理器实例"""
    global _default_pm
    if _default_pm is None:
        _default_pm = PositionManager(target_annual_vol=10.0)
    return _default_pm

def reset_position_manager():
    """重置全局仓位管理器"""
    global _default_pm
    _default_pm = None

def pick_top10_with_position(day_df):
    """
    风控增强TOP10 + 仓位建议
    
    返回: (selected_df, position_ratio)
    """
    pm = get_position_manager()
    return select_risk_managed(day_df, n=10, max_per_ind=3, position_manager=pm)

def pick_top5_with_position(day_df):
    """
    风控增强TOP5 + 仓位建议
    
    返回: (selected_df, position_ratio)
    """
    pm = get_position_manager()
    return select_risk_managed(day_df, n=5, max_per_ind=3, position_manager=pm)


# ============================================================
# 独立回测函数
# ============================================================
def backtest_with_position_management(detail_csv, n=10, hold_days=1):
    """
    带仓位管理的完整回测
    
    参数:
        detail_csv: backtest_v2 输出的detail CSV路径
        n: 选股数量
        hold_days: 持有天数
    
    返回:
        dict: 回测结果
    """
    df = pd.read_csv(detail_csv)
    df['date'] = df['date'].astype(str)
    dates = sorted(df['date'].unique())
    
    pm = PositionManager(target_annual_vol=10.0)
    
    daily_records = []
    i = 0
    
    while i < len(dates):
        date = dates[i]
        day_df = df[df['date'] == date].copy()
        
        if len(day_df) == 0:
            i += 1
            continue
        
        # 获取仓位建议（基于截至昨日的信息）
        position = pm.get_position()
        
        # 选股
        selected = select_optimized_elite(day_df, n)
        
        if len(selected) == 0:
            i += hold_days
            continue
        
        # 计算收益
        ret_col = f'ret_{hold_days}d' if hold_days > 1 else 'ret_1d'
        valid_rets = selected[ret_col].dropna()
        
        if len(valid_rets) == 0:
            i += hold_days
            continue
        
        raw_ret = valid_rets.mean()
        adj_ret = raw_ret * position  # 仓位调整后的收益
        
        # 更新仓位管理器状态
        pm.update(raw_ret)  # 用原始收益更新（因为选股结果不变，只是仓位变了）
        
        daily_records.append({
            'date': date,
            'raw_ret': raw_ret,
            'adj_ret': adj_ret,
            'position': position,
            'n_stocks': len(valid_rets),
            'loss_streak': pm.loss_streak,
            'nav': pm.nav,
            'drawdown': (pm.peak - pm.nav) / pm.peak * 100,
        })
        
        i += hold_days
    
    if not daily_records:
        return None
    
    rec_df = pd.DataFrame(daily_records)
    
    # 计算仓位调整后的统计
    adj_rets = rec_df['adj_ret'].values
    cum = 1.0
    peak = 1.0
    max_dd = 0
    for r in adj_rets:
        cum *= (1 + r / 100)
        if cum > peak: peak = cum
        dd = (peak - cum) / peak * 100
        if dd > max_dd: max_dd = dd
    
    total_ret = (cum - 1) * 100
    total_td = len(adj_rets) * hold_days
    ann = (cum ** (250 / total_td) - 1) * 100 if total_td > 0 else 0
    sharpe = adj_rets.mean() / adj_rets.std() * np.sqrt(250 / hold_days) if adj_rets.std() > 0 else 0
    calmar = ann / max_dd if max_dd > 0 else 99
    
    # 原始（满仓）统计
    raw_rets = rec_df['raw_ret'].values
    raw_cum = 1.0
    raw_peak = 1.0
    raw_max_dd = 0
    for r in raw_rets:
        raw_cum *= (1 + r / 100)
        if raw_cum > raw_peak: raw_peak = raw_cum
        dd = (raw_peak - raw_cum) / raw_peak * 100
        if dd > raw_max_dd: raw_max_dd = dd
    
    return {
        'cum_ret': total_ret,
        'ann_ret': ann,
        'sharpe': sharpe,
        'max_dd': max_dd,
        'calmar': calmar,
        'win_rate': (adj_rets > 0).sum() / len(adj_rets) * 100,
        'avg_position': rec_df['position'].mean() * 100,
        'n_days': len(adj_rets),
        # 对比满仓
        'raw_cum_ret': (raw_cum - 1) * 100,
        'raw_max_dd': raw_max_dd,
        'dd_improvement': raw_max_dd - max_dd,
        'records': rec_df,
    }


if __name__ == '__main__':
    import sys
    
    print("策略04：风控增强因子优化策略 — T+1 每日换仓 + 动态仓位管理")
    print("=" * 70)
    
    # 默认回测文件
    detail_csv = 'backtest_results/backtest_v2_detail_20260420_180427.csv'
    if len(sys.argv) > 1:
        detail_csv = sys.argv[1]
    
    print(f"\n回测数据: {detail_csv}")
    
    for n in [5, 10]:
        print(f"\n{'─' * 60}")
        print(f"📊 TOP{n} T+1 回测结果:")
        result = backtest_with_position_management(detail_csv, n=n)
        
        if result:
            print(f"  累计收益: {result['cum_ret']:+.1f}% (满仓: {result['raw_cum_ret']:+.1f}%)")
            print(f"  年化收益: {result['ann_ret']:+.1f}%")
            print(f"  Sharpe:   {result['sharpe']:.2f}")
            print(f"  最大回撤: -{result['max_dd']:.1f}% (满仓: -{result['raw_max_dd']:.1f}%, 降{result['dd_improvement']:+.1f}%)")
            print(f"  Calmar:   {result['calmar']:.2f}")
            print(f"  胜率:     {result['win_rate']:.1f}%")
            print(f"  平均仓位: {result['avg_position']:.1f}%")
            print(f"  交易天数: {result['n_days']}")
