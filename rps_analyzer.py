# -*- coding: utf-8 -*-
"""
RPS相对强度分析模块
计算股票和行业的相对强度排名
"""

import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logger = logging.getLogger(__name__)

class RPSAnalyzer:
    """RPS相对强度分析器"""
    
    def __init__(self):
        self.rps_cache = {}
        self.cache_timeout = 3600  # 1小时缓存
        self.last_update = {}
        
    def calculate_stock_rps(self, stock_codes: List[str], periods: List[int] = [5, 10, 20, 60]) -> Dict[str, Any]:
        """计算股票RPS相对强度"""
        try:
            results = {}
            
            # 获取所有股票的历史数据
            stock_data = self._get_stocks_data(stock_codes, max(periods) + 10)
            
            if not stock_data:
                return {}
            
            # 计算各周期RPS
            for period in periods:
                period_rps = self._calculate_period_rps(stock_data, period)
                results[f'rps_{period}'] = period_rps
            
            # 计算综合RPS评分
            composite_rps = self._calculate_composite_rps(results, periods)
            results['composite_rps'] = composite_rps
            
            # 强度分级
            results['strength_level'] = self._get_strength_levels(composite_rps)
            
            return results
            
        except Exception as e:
            logger.error(f"计算股票RPS失败: {e}")
            return {}
    
    def calculate_industry_rps(self, periods: List[int] = [5, 10, 20, 60]) -> Dict[str, Any]:
        """计算行业RPS相对强度"""
        try:
            cache_key = f"industry_rps_{'-'.join(map(str, periods))}"
            if self._is_cache_valid(cache_key):
                return self.rps_cache[cache_key]
            
            # 获取行业数据
            industry_data = self._get_industry_data(max(periods) + 10)
            
            if not industry_data:
                return {}
            
            results = {}
            
            # 计算各周期行业RPS
            for period in periods:
                period_rps = self._calculate_industry_period_rps(industry_data, period)
                results[f'rps_{period}'] = period_rps
            
            # 计算综合RPS评分
            composite_rps = self._calculate_industry_composite_rps(results, periods)
            results['composite_rps'] = composite_rps
            
            # 行业轮动识别
            rotation_analysis = self._analyze_industry_rotation(industry_data, periods)
            results['rotation_analysis'] = rotation_analysis
            
            # 缓存结果
            self.rps_cache[cache_key] = results
            self.last_update[cache_key] = datetime.now()
            
            return results
            
        except Exception as e:
            logger.error(f"计算行业RPS失败: {e}")
            return {}
    
    def get_rps_ranking(self, rps_data: Dict[str, float], top_n: int = 50) -> List[Dict[str, Any]]:
        """获取RPS排名"""
        try:
            # 转换为列表并排序
            ranking = []
            for code, rps_score in rps_data.items():
                ranking.append({
                    'code': code,
                    'rps_score': rps_score,
                    'rank': 0  # 将在排序后填充
                })
            
            # 按RPS得分降序排序
            ranking.sort(key=lambda x: x['rps_score'], reverse=True)
            
            # 添加排名
            for i, item in enumerate(ranking[:top_n]):
                item['rank'] = i + 1
                item['strength_level'] = self._get_single_strength_level(item['rps_score'])
            
            return ranking[:top_n]
            
        except Exception as e:
            logger.error(f"获取RPS排名失败: {e}")
            return []
    
    def _get_stocks_data(self, stock_codes: List[str], days: int) -> Dict[str, pd.DataFrame]:
        """获取多只股票的历史数据"""
        stock_data = {}
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        def get_single_stock_data(code):
            try:
                # 清理股票代码
                clean_code = code.replace('.SZ', '').replace('.SH', '')
                
                df = ak.stock_zh_a_hist(symbol=clean_code, period="daily", 
                                       start_date=start_date, end_date=end_date, adjust="qfq")
                
                if not df.empty:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df = df.sort_values('日期')
                    return code, df
                return code, None
                
            except Exception as e:
                logger.warning(f"获取股票{code}数据失败: {e}")
                return code, None
        
        # 多线程获取数据
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_single_stock_data, code) for code in stock_codes[:100]]  # 限制数量
            
            for future in as_completed(futures):
                try:
                    code, data = future.result()
                    if data is not None:
                        stock_data[code] = data
                except Exception as e:
                    logger.error(f"获取股票数据异常: {e}")
        
        return stock_data
    
    def _calculate_period_rps(self, stock_data: Dict[str, pd.DataFrame], period: int) -> Dict[str, float]:
        """计算指定周期的RPS"""
        period_returns = {}
        
        # 计算各股票的周期收益率
        for code, df in stock_data.items():
            try:
                if len(df) >= period + 1:
                    current_price = df.iloc[-1]['收盘']
                    period_price = df.iloc[-(period+1)]['收盘']
                    period_return = (current_price - period_price) / period_price
                    period_returns[code] = period_return
            except Exception as e:
                logger.warning(f"计算{code}周期收益率失败: {e}")
                continue
        
        # 计算RPS排名
        if not period_returns:
            return {}
        
        # 转换为列表并排序
        sorted_returns = sorted(period_returns.items(), key=lambda x: x[1], reverse=True)
        total_stocks = len(sorted_returns)
        
        rps_scores = {}
        for i, (code, return_rate) in enumerate(sorted_returns):
            # RPS = (总数 - 排名) / 总数 * 100
            rps_score = (total_stocks - i) / total_stocks * 100
            rps_scores[code] = rps_score
        
        return rps_scores
    
    def _calculate_composite_rps(self, rps_results: Dict[str, Dict[str, float]], periods: List[int]) -> Dict[str, float]:
        """计算综合RPS评分"""
        composite_rps = {}
        
        # 获取所有股票代码
        all_codes = set()
        for period_rps in rps_results.values():
            if isinstance(period_rps, dict):
                all_codes.update(period_rps.keys())
        
        # 权重设置（短期权重大于长期）
        weights = {
            5: 0.4,
            10: 0.3,
            20: 0.2,
            60: 0.1
        }
        
        for code in all_codes:
            total_score = 0
            total_weight = 0
            
            for period in periods:
                period_key = f'rps_{period}'
                if period_key in rps_results and code in rps_results[period_key]:
                    weight = weights.get(period, 1.0 / len(periods))
                    total_score += rps_results[period_key][code] * weight
                    total_weight += weight
            
            if total_weight > 0:
                composite_rps[code] = total_score / total_weight
        
        return composite_rps
    
    def _get_industry_data(self, days: int) -> Dict[str, pd.DataFrame]:
        """获取行业指数数据"""
        try:
            # 获取申万一级行业数据
            industry_data = {}
            
            # 主要行业指数代码
            industry_codes = [
                "801010",  # 农林牧渔
                "801020",  # 采掘
                "801030",  # 化工
                "801040",  # 钢铁
                "801050",  # 有色金属
                "801080",  # 电子
                "801110",  # 家用电器
                "801120",  # 食品饮料
                "801130",  # 纺织服装
                "801140",  # 轻工制造
                "801150",  # 医药生物
                "801160",  # 公用事业
                "801170",  # 交通运输
                "801180",  # 房地产
                "801200",  # 商业贸易
                "801210",  # 休闲服务
                "801230",  # 综合
                "801710",  # 建筑材料
                "801720",  # 建筑装饰
                "801730",  # 电气设备
                "801740",  # 国防军工
                "801750",  # 计算机
                "801760",  # 传媒
                "801770",  # 通信
                "801780",  # 银行
                "801790",  # 非银金融
                "801880",  # 汽车
                "801890"   # 机械设备
            ]
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            for code in industry_codes:
                try:
                    df = ak.index_zh_a_hist(symbol=code, period="daily", 
                                          start_date=start_date, end_date=end_date)
                    if df is not None and not df.empty and '日期' in df.columns:
                        df['日期'] = pd.to_datetime(df['日期'])
                        df = df.sort_values('日期')
                        industry_data[code] = df
                        time.sleep(0.1)  # 避免请求过快
                except Exception as e:
                    logger.warning(f"获取行业{code}数据失败: {e}")
                    continue
            
            return industry_data
            
        except Exception as e:
            logger.error(f"获取行业数据失败: {e}")
            return {}
    
    def _calculate_industry_period_rps(self, industry_data: Dict[str, pd.DataFrame], period: int) -> Dict[str, float]:
        """计算行业指定周期RPS"""
        period_returns = {}
        
        # 计算各行业的周期收益率
        for code, df in industry_data.items():
            try:
                if len(df) >= period + 1:
                    current_price = df.iloc[-1]['收盘']
                    period_price = df.iloc[-(period+1)]['收盘']
                    period_return = (current_price - period_price) / period_price
                    period_returns[code] = period_return
            except Exception as e:
                logger.warning(f"计算行业{code}周期收益率失败: {e}")
                continue
        
        # 计算RPS排名
        if not period_returns:
            return {}
        
        sorted_returns = sorted(period_returns.items(), key=lambda x: x[1], reverse=True)
        total_industries = len(sorted_returns)
        
        rps_scores = {}
        for i, (code, return_rate) in enumerate(sorted_returns):
            rps_score = (total_industries - i) / total_industries * 100
            rps_scores[code] = rps_score
        
        return rps_scores
    
    def _calculate_industry_composite_rps(self, rps_results: Dict[str, Dict[str, float]], periods: List[int]) -> Dict[str, float]:
        """计算行业综合RPS评分"""
        return self._calculate_composite_rps(rps_results, periods)
    
    def _analyze_industry_rotation(self, industry_data: Dict[str, pd.DataFrame], periods: List[int]) -> Dict[str, Any]:
        """分析行业轮动"""
        try:
            rotation_analysis = {
                'hot_industries': [],
                'cold_industries': [],
                'rotation_signal': 'neutral'
            }
            
            # 计算短期和长期RPS对比
            short_rps = self._calculate_industry_period_rps(industry_data, periods[0])
            long_rps = self._calculate_industry_period_rps(industry_data, periods[-1])
            
            # 识别轮动机会
            rotation_opportunities = []
            for code in short_rps.keys():
                if code in long_rps:
                    short_score = short_rps[code]
                    long_score = long_rps[code]
                    momentum = short_score - long_score
                    
                    rotation_opportunities.append({
                        'industry': code,
                        'short_rps': short_score,
                        'long_rps': long_score,
                        'momentum': momentum
                    })
            
            # 排序并识别热点和冷门行业
            rotation_opportunities.sort(key=lambda x: x['momentum'], reverse=True)
            
            rotation_analysis['hot_industries'] = rotation_opportunities[:5]
            rotation_analysis['cold_industries'] = rotation_opportunities[-5:]
            
            # 判断轮动信号
            avg_momentum = np.mean([item['momentum'] for item in rotation_opportunities])
            if avg_momentum > 10:
                rotation_analysis['rotation_signal'] = 'strong_rotation'
            elif avg_momentum > 5:
                rotation_analysis['rotation_signal'] = 'mild_rotation'
            elif avg_momentum < -5:
                rotation_analysis['rotation_signal'] = 'trend_continuation'
            else:
                rotation_analysis['rotation_signal'] = 'neutral'
            
            return rotation_analysis
            
        except Exception as e:
            logger.error(f"分析行业轮动失败: {e}")
            return {'rotation_signal': 'neutral', 'hot_industries': [], 'cold_industries': []}
    
    def _get_strength_levels(self, composite_rps: Dict[str, float]) -> Dict[str, str]:
        """获取强度等级"""
        strength_levels = {}
        for code, rps_score in composite_rps.items():
            strength_levels[code] = self._get_single_strength_level(rps_score)
        return strength_levels
    
    def _get_single_strength_level(self, rps_score: float) -> str:
        """获取单个RPS强度等级"""
        if rps_score >= 90:
            return "极强"
        elif rps_score >= 80:
            return "强势"
        elif rps_score >= 60:
            return "较强"
        elif rps_score >= 40:
            return "中等"
        elif rps_score >= 20:
            return "较弱"
        else:
            return "弱势"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self.rps_cache or cache_key not in self.last_update:
            return False
        
        time_diff = (datetime.now() - self.last_update[cache_key]).seconds
        return time_diff < self.cache_timeout

# 创建全局实例
rps_analyzer = RPSAnalyzer()
