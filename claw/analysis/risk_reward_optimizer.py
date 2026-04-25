#!/usr/bin/env python3
"""
风险收益比优化器 v4.1
====================
【优化目标】
将简单的风控扣分（-30分）升级为系统化的风险收益比计算

【核心指标】
1. 预期收益率 = (目标价-现价)/现价
2. 风险率 = 最大回撤预期
3. 风险收益比 = 预期收益率 / 风险率
4. 夏普比率 = 年化收益率 / 年化波动率

【预期效果】
- 夏普比率提升 +0.2
- 最大回撤降低 -5%
- 仓位分配更科学
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from scipy import stats

from claw.core.tushare_client import ts
from claw.core.config import settings
from claw.core.logging import get_logger

log = get_logger("risk_reward")


class RiskRewardOptimizer:
    """风险收益比优化器"""
    
    def __init__(self, lookback_days: int = 60):
        self.lookback_days = lookback_days
        self.cache = {}
    
    def calculate_risk_reward_profile(self, stock_code: str, trade_date: str) -> Dict[str, float]:
        """
        计算个股的风险收益比分析
        
        参数:
            stock_code: 股票代码
            trade_date: 交易日期
        
        返回:
            {
                'expected_return': 预期收益率（%）,
                'max_drawdown_risk': 最大回撤风险（%）,
                'risk_reward_ratio': 风险收益比,
                'sharpe_ratio': 夏普比率,
                'volatility': 波动率（%）,
                'position_weight': 建议仓位权重（0-1）
            }
        """
        cache_key = f"{stock_code}_{trade_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 1. 获取历史价格数据
        price_data = self._get_price_history(stock_code, trade_date, self.lookback_days)
        
        if price_data.empty:
            return self._get_default_profile()
        
        # 2. 计算预期收益率
        expected_return = self._calculate_expected_return(price_data)
        
        # 3. 计算风险指标
        volatility = self._calculate_volatility(price_data)
        max_drawdown = self._calculate_max_drawdown(price_data)
        
        # 4. 计算风险收益比
        risk_reward_ratio = self._calculate_risk_reward_ratio(expected_return, max_drawdown)
        
        # 5. 计算夏普比率
        sharpe_ratio = self._calculate_sharpe_ratio(expected_return, volatility)
        
        # 6. 计算建议仓位权重
        position_weight = self._calculate_position_weight(expected_return, risk_reward_ratio, sharpe_ratio)
        
        result = {
            'expected_return': expected_return * 100,  # 转换为百分比
            'max_drawdown_risk': max_drawdown * 100,
            'risk_reward_ratio': risk_reward_ratio,
            'sharpe_ratio': sharpe_ratio,
            'volatility': volatility * 100,
            'position_weight': position_weight,
            'risk_level': self._get_risk_level(risk_reward_ratio, sharpe_ratio)
        }
        
        self.cache[cache_key] = result
        return result
    
    def _get_price_history(self, stock_code: str, end_date: str, days: int) -> pd.DataFrame:
        """获取历史价格数据"""
        # 计算开始日期
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        start_dt = end_dt - timedelta(days=days * 2)  # 预留缓冲
        
        # 获取交易日历
        cal_data = ts("trade_cal", {
            "exchange": "SSE",
            "start_date": start_dt.strftime("%Y%m%d"),
            "end_date": end_date,
            "is_open": "1"
        }, "cal_date")
        
        if cal_data.empty:
            return pd.DataFrame()
        
        trade_dates = sorted(cal_data["cal_date"].tolist(), reverse=True)
        target_dates = trade_dates[:days]
        
        if not target_dates:
            return pd.DataFrame()
        
        # 获取价格数据
        price_data = ts("daily", {
            "ts_code": stock_code,
            "trade_date": {"$in": target_dates}
        }, fields="trade_date,close,pct_chg")
        
        return price_data.sort_values("trade_date")
    
    def _calculate_expected_return(self, price_data: pd.DataFrame) -> float:
        """计算预期收益率（基于历史收益率的统计预测）"""
        if len(price_data) < 10:
            return 0.0
        
        # 计算日收益率
        returns = price_data["pct_chg"] / 100.0
        
        # 方法1：简单平均
        simple_mean = returns.mean()
        
        # 方法2：加权平均（近期权重更高）
        weights = np.exp(np.linspace(0, 1, len(returns)))
        weighted_mean = np.average(returns, weights=weights)
        
        # 方法3：技术分析预测（简化版）
        tech_forecast = self._technical_forecast(price_data)
        
        # 综合预期收益率
        expected_return = (simple_mean * 0.3 + weighted_mean * 0.4 + tech_forecast * 0.3)
        
        # 年化收益率（假设250个交易日）
        annual_return = expected_return * 250
        
        return min(max(annual_return, -0.5), 2.0)  # 限制范围
    
    def _technical_forecast(self, price_data: pd.DataFrame) -> float:
        """技术分析预测（简化版）"""
        if len(price_data) < 20:
            return 0.0
        
        closes = price_data["close"].values
        
        # 计算移动平均线
        ma5 = closes[-5:].mean() if len(closes) >= 5 else closes.mean()
        ma20 = closes[-20:].mean() if len(closes) >= 20 else closes.mean()
        
        current_price = closes[-1]
        
        # 趋势判断
        if ma5 > ma20 and current_price > ma5:
            # 强势上升趋势
            return 0.0015  # 日收益率0.15%
        elif ma5 < ma20 and current_price < ma5:
            # 下降趋势
            return -0.001
        else:
            # 震荡趋势
            return 0.0005
    
    def _calculate_volatility(self, price_data: pd.DataFrame) -> float:
        """计算波动率（年化）"""
        if len(price_data) < 10:
            return 0.3  # 默认波动率
        
        returns = price_data["pct_chg"] / 100.0
        
        # 日波动率
        daily_volatility = returns.std()
        
        # 年化波动率
        annual_volatility = daily_volatility * np.sqrt(250)
        
        return max(0.1, min(annual_volatility, 1.0))  # 限制范围
    
    def _calculate_max_drawdown(self, price_data: pd.DataFrame) -> float:
        """计算最大回撤风险"""
        if len(price_data) < 10:
            return 0.2  # 默认最大回撤20%
        
        closes = price_data["close"].values
        
        # 计算累计收益率
        cumulative_returns = (closes / closes[0]) - 1
        
        # 计算最大回撤
        peak = cumulative_returns[0]
        max_dd = 0.0
        
        for ret in cumulative_returns:
            if ret > peak:
                peak = ret
            dd = peak - ret
            if dd > max_dd:
                max_dd = dd
        
        return max_dd
    
    def _calculate_risk_reward_ratio(self, expected_return: float, max_drawdown: float) -> float:
        """计算风险收益比"""
        if max_drawdown <= 0:
            return float('inf') if expected_return > 0 else 0.0
        
        ratio = expected_return / max_drawdown
        return min(ratio, 10.0)  # 限制最大比率
    
    def _calculate_sharpe_ratio(self, expected_return: float, volatility: float) -> float:
        """计算夏普比率（无风险利率假设为3%）"""
        if volatility <= 0:
            return float('inf') if expected_return > 0 else 0.0
        
        risk_free_rate = 0.03  # 无风险利率3%
        sharpe = (expected_return - risk_free_rate) / volatility
        return min(sharpe, 5.0)  # 限制最大夏普比率
    
    def _calculate_position_weight(self, expected_return: float, risk_reward_ratio: float, sharpe_ratio: float) -> float:
        """计算建议仓位权重（0-1）"""
        # 基于凯利公式的简化版
        if risk_reward_ratio <= 0:
            return 0.0
        
        # 基础权重（基于风险收益比）
        base_weight = min(risk_reward_ratio / 5.0, 0.2)  # 最大20%单票仓位
        
        # 夏普比率调整
        sharpe_adjustment = min(sharpe_ratio / 2.0, 1.0)
        
        # 预期收益率调整
        return_adjustment = min(expected_return / 0.5, 1.0)  # 年化50%为上限
        
        # 综合权重
        weight = base_weight * sharpe_adjustment * return_adjustment
        
        return max(0.0, min(weight, 0.2))  # 限制在0-20%
    
    def _get_risk_level(self, risk_reward_ratio: float, sharpe_ratio: float) -> str:
        """获取风险等级"""
        if risk_reward_ratio >= 3.0 and sharpe_ratio >= 1.5:
            return "低风险"
        elif risk_reward_ratio >= 2.0 and sharpe_ratio >= 1.0:
            return "中低风险"
        elif risk_reward_ratio >= 1.0 and sharpe_ratio >= 0.5:
            return "中等风险"
        elif risk_reward_ratio >= 0.5:
            return "中高风险"
        else:
            return "高风险"
    
    def _get_default_profile(self) -> Dict[str, float]:
        """获取默认风险收益配置"""
        return {
            'expected_return': 0.0,
            'max_drawdown_risk': 0.2,
            'risk_reward_ratio': 0.0,
            'sharpe_ratio': 0.0,
            'volatility': 0.3,
            'position_weight': 0.0,
            'risk_level': '高风险'
        }
    
    def optimize_portfolio_weights(self, stock_profiles: Dict[str, Dict]) -> Dict[str, float]:
        """
        优化投资组合权重（基于风险平价思想）
        
        参数:
            stock_profiles: {股票代码: 风险收益配置}
        
        返回:
            {股票代码: 优化后的权重}
        """
        if not stock_profiles:
            return {}
        
        # 计算每个股票的"风险贡献"
        risk_contributions = {}
        total_risk_contribution = 0.0
        
        for stock, profile in stock_profiles.items():
            # 风险贡献 = 1 / 波动率（风险平价思想）
            volatility = profile.get('volatility', 0.3) / 100.0  # 转换为小数
            risk_contribution = 1.0 / max(volatility, 0.01)  # 避免除零
            
            # 考虑夏普比率调整
            sharpe = profile.get('sharpe_ratio', 0.0)
            sharpe_adjustment = max(0.5, min(sharpe / 1.0, 2.0))
            
            risk_contributions[stock] = risk_contribution * sharpe_adjustment
            total_risk_contribution += risk_contributions[stock]
        
        # 计算权重
        weights = {}
        for stock, risk_contribution in risk_contributions.items():
            if total_risk_contribution > 0:
                weight = risk_contribution / total_risk_contribution
                # 限制单票最大权重
                weights[stock] = min(weight, 0.25)  # 最大25%
            else:
                weights[stock] = 1.0 / len(stock_profiles)  # 等权重
        
        # 归一化权重
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {k: v / total_weight for k, v in weights.items()}
        
        return weights


def integrate_with_scoring_system(risk_reward_ratio: float, sharpe_ratio: float) -> float:
    """
    将风险收益比分析整合到九维评分系统中
    
    原系统：风险扣分维度（-30分）较简单
    新系统：风险收益比（0-10分）和夏普比率（0-5分）额外加分
    
    映射规则：
    - 风险收益比 >= 3.0 → +8分
    - 风险收益比 >= 2.0 → +5分
    - 风险收益比 >= 1.0 → +2分
    - 风险收益比 < 1.0 → -5分
    
    - 夏普比率 >= 1.5 → +5分
    - 夏普比率 >= 1.0 → +3分
    - 夏普比率 >= 0.5 → +1分
    - 夏普比率 < 0.5 → -2分
    """
    # 风险收益比得分
    if risk_reward_ratio >= 3.0:
        rr_score = 8.0
    elif risk_reward_ratio >= 2.0:
        rr_score = 5.0
    elif risk_reward_ratio >= 1.0:
        rr_score = 2.0
    else:
        rr_score = -5.0
    
    # 夏普比率得分
    if sharpe_ratio >= 1.5:
        sharpe_score = 5.0
    elif sharpe_ratio >= 1.0:
        sharpe_score = 3.0
    elif sharpe_ratio >= 0.5:
        sharpe_score = 1.0
    else:
        sharpe_score = -2.0
    
    return rr_score + sharpe_score


# ============================================================
# 使用示例
# ============================================================
if __name__ == "__main__":
    optimizer = RiskRewardOptimizer(lookback_days=60)
    
    # 测试个股风险收益分析
    test_stock = "000001.SZ"  # 平安银行
    test_date = "20260420"
    
    profile = optimizer.calculate_risk_reward_profile(test_stock, test_date)
    
    print(f"=== 风险收益比分析报告 ===")
    print(f"股票: {test_stock}")
    print(f"日期: {test_date}")
    print(f"预期年化收益率: {profile['expected_return']:.2f}%")
    print(f"最大回撤风险: {profile['max_drawdown_risk']:.2f}%")
    print(f"风险收益比: {profile['risk_reward_ratio']:.2f}")
    print(f"夏普比率: {profile['sharpe_ratio']:.2f}")
    print(f"年化波动率: {profile['volatility']:.2f}%")
    print(f"建议仓位权重: {profile['position_weight']:.2%}")
    print(f"风险等级: {profile['risk_level']}")
    
    # 整合到评分系统
    scoring_adjustment = integrate_with_scoring_system(
        profile['risk_reward_ratio'], 
        profile['sharpe_ratio']
    )
    print(f"\n评分系统调整: {scoring_adjustment:+}分")
    
    # 投资组合优化示例
    portfolio = {
        "000001.SZ": profile,
        "000002.SZ": optimizer.calculate_risk_reward_profile("000002.SZ", test_date),
        "000858.SZ": optimizer.calculate_risk_reward_profile("000858.SZ", test_date)
    }
    
    optimized_weights = optimizer.optimize_portfolio_weights(portfolio)
    print(f"\n投资组合优化权重:")
    for stock, weight in optimized_weights.items():
        print(f"  {stock}: {weight:.2%}")
