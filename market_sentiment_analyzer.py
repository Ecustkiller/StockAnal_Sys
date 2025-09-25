# -*- coding: utf-8 -*-
"""
市场情绪分析模块
基于多维度指标的实时市场情绪监控和分析
"""

import pandas as pd
import numpy as np
import akshare as ak
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class MarketSentimentAnalyzer:
    """市场情绪分析器"""
    
    def __init__(self):
        self.sentiment_data = {}
        self.cache_timeout = 300  # 5分钟缓存
        self.last_update = None
        
    def get_market_sentiment(self) -> Dict[str, Any]:
        """获取综合市场情绪指标"""
        try:
            # 检查缓存
            if self._is_cache_valid():
                return self.sentiment_data
                
            sentiment_data = {}
            
            # 1. 涨跌停数据
            limit_data = self._get_limit_data()
            sentiment_data.update(limit_data)
            
            # 2. 市场广度指标
            breadth_data = self._get_market_breadth()
            sentiment_data.update(breadth_data)
            
            # 3. 资金流向数据
            money_flow = self._get_money_flow_sentiment()
            sentiment_data.update(money_flow)
            
            # 4. 计算综合情绪指数
            sentiment_index = self._calculate_sentiment_index(sentiment_data)
            sentiment_data['sentiment_index'] = sentiment_index
            
            # 5. 情绪等级评定
            sentiment_level = self._get_sentiment_level(sentiment_index)
            sentiment_data['sentiment_level'] = sentiment_level
            
            self.sentiment_data = sentiment_data
            self.last_update = datetime.now()
            
            return sentiment_data
            
        except Exception as e:
            logger.error(f"获取市场情绪数据失败: {e}")
            return self._get_default_sentiment()
    
    def _get_limit_data(self) -> Dict[str, Any]:
        """获取涨跌停数据"""
        try:
            # 获取当日涨停数据
            limit_up_df = ak.stock_zt_pool_em(date=datetime.now().strftime('%Y%m%d'))
            limit_up_count = len(limit_up_df) if not limit_up_df.empty else 0
            
            # 获取当日跌停数据  
            limit_down_df = ak.stock_dt_pool_em(date=datetime.now().strftime('%Y%m%d'))
            limit_down_count = len(limit_down_df) if not limit_down_df.empty else 0
            
            # 涨跌停比例
            total_limits = limit_up_count + limit_down_count
            limit_ratio = limit_up_count / max(total_limits, 1)
            
            return {
                'limit_up_count': limit_up_count,
                'limit_down_count': limit_down_count,
                'limit_ratio': limit_ratio,
                'total_limits': total_limits
            }
            
        except Exception as e:
            logger.error(f"获取涨跌停数据失败: {e}")
            return {
                'limit_up_count': 0,
                'limit_down_count': 0,
                'limit_ratio': 0.5,
                'total_limits': 0
            }
    
    def _get_market_breadth(self) -> Dict[str, Any]:
        """获取市场广度指标"""
        try:
            # 获取A股实时数据
            stock_zh_a_spot_df = ak.stock_zh_a_spot_em()
            
            if stock_zh_a_spot_df.empty:
                return self._get_default_breadth()
                
            # 计算涨跌家数
            up_count = len(stock_zh_a_spot_df[stock_zh_a_spot_df['涨跌幅'] > 0])
            down_count = len(stock_zh_a_spot_df[stock_zh_a_spot_df['涨跌幅'] < 0])
            flat_count = len(stock_zh_a_spot_df[stock_zh_a_spot_df['涨跌幅'] == 0])
            total_count = len(stock_zh_a_spot_df)
            
            # 计算上涨比例
            up_ratio = up_count / max(total_count, 1)
            
            # 计算平均涨跌幅
            avg_change = stock_zh_a_spot_df['涨跌幅'].mean()
            
            # 强势股比例（涨幅>3%）
            strong_count = len(stock_zh_a_spot_df[stock_zh_a_spot_df['涨跌幅'] > 3])
            strong_ratio = strong_count / max(total_count, 1)
            
            return {
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'total_count': total_count,
                'up_ratio': up_ratio,
                'avg_change': avg_change,
                'strong_count': strong_count,
                'strong_ratio': strong_ratio
            }
            
        except Exception as e:
            logger.error(f"获取市场广度数据失败: {e}")
            return self._get_default_breadth()
    
    def _get_money_flow_sentiment(self) -> Dict[str, Any]:
        """获取资金流向情绪指标"""
        try:
            # 获取大盘资金流向
            money_flow_df = ak.stock_market_fund_flow()
            
            if money_flow_df.empty:
                return {'money_flow_sentiment': 0.5}
                
            # 获取今日数据
            today_data = money_flow_df.iloc[0]
            
            # 主力净流入
            main_net_inflow = float(today_data.get('主力净流入-净额', 0))
            
            # 超大单净流入
            super_large_inflow = float(today_data.get('超大单净流入-净额', 0))
            
            # 计算资金流向情绪（基于净流入额度）
            if main_net_inflow > 0:
                money_sentiment = min(0.5 + main_net_inflow / 10000000000, 1.0)  # 100亿为满值
            else:
                money_sentiment = max(0.5 + main_net_inflow / 10000000000, 0.0)
                
            return {
                'main_net_inflow': main_net_inflow,
                'super_large_inflow': super_large_inflow,
                'money_flow_sentiment': money_sentiment
            }
            
        except Exception as e:
            logger.error(f"获取资金流向数据失败: {e}")
            return {'money_flow_sentiment': 0.5}
    
    def _calculate_sentiment_index(self, data: Dict[str, Any]) -> float:
        """计算综合情绪指数（0-100）"""
        try:
            # 各项指标权重
            weights = {
                'limit_ratio': 0.25,      # 涨跌停比例
                'up_ratio': 0.30,         # 上涨家数比例
                'strong_ratio': 0.20,     # 强势股比例
                'money_flow_sentiment': 0.25  # 资金流向情绪
            }
            
            # 计算加权平均
            sentiment_score = 0
            total_weight = 0
            
            for key, weight in weights.items():
                if key in data:
                    sentiment_score += data[key] * weight
                    total_weight += weight
            
            # 标准化到0-100
            if total_weight > 0:
                sentiment_index = (sentiment_score / total_weight) * 100
            else:
                sentiment_index = 50  # 默认中性
                
            return max(0, min(100, sentiment_index))
            
        except Exception as e:
            logger.error(f"计算情绪指数失败: {e}")
            return 50.0
    
    def _get_sentiment_level(self, sentiment_index: float) -> str:
        """根据情绪指数获取情绪等级"""
        if sentiment_index >= 80:
            return "极度乐观"
        elif sentiment_index >= 65:
            return "乐观"
        elif sentiment_index >= 50:
            return "偏乐观"
        elif sentiment_index >= 35:
            return "偏悲观"
        elif sentiment_index >= 20:
            return "悲观"
        else:
            return "极度悲观"
    
    def get_sentiment_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """获取历史情绪数据"""
        # 这里可以从数据库或缓存中获取历史数据
        # 暂时返回模拟数据
        history = []
        base_date = datetime.now() - timedelta(days=days)
        
        for i in range(days):
            date = base_date + timedelta(days=i)
            # 模拟历史情绪数据
            sentiment_index = 50 + np.random.normal(0, 15)
            sentiment_index = max(0, min(100, sentiment_index))
            
            history.append({
                'date': date.strftime('%Y-%m-%d'),
                'sentiment_index': sentiment_index,
                'sentiment_level': self._get_sentiment_level(sentiment_index)
            })
        
        return history
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self.last_update or not self.sentiment_data:
            return False
        
        time_diff = (datetime.now() - self.last_update).seconds
        return time_diff < self.cache_timeout
    
    def _get_default_sentiment(self) -> Dict[str, Any]:
        """获取默认情绪数据"""
        return {
            'limit_up_count': 0,
            'limit_down_count': 0,
            'limit_ratio': 0.5,
            'up_count': 0,
            'down_count': 0,
            'up_ratio': 0.5,
            'avg_change': 0.0,
            'strong_ratio': 0.1,
            'money_flow_sentiment': 0.5,
            'sentiment_index': 50.0,
            'sentiment_level': '中性'
        }
    
    def _get_default_breadth(self) -> Dict[str, Any]:
        """获取默认市场广度数据"""
        return {
            'up_count': 0,
            'down_count': 0,
            'flat_count': 0,
            'total_count': 0,
            'up_ratio': 0.5,
            'avg_change': 0.0,
            'strong_count': 0,
            'strong_ratio': 0.1
        }

# 创建全局实例
market_sentiment_analyzer = MarketSentimentAnalyzer()
