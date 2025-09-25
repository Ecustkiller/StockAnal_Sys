# -*- coding: utf-8 -*-
"""
涨停连板分析模块
提供涨停股票分析、连板统计、概念热度分析等功能
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZhangTingAnalyzer:
    """涨停连板分析器"""
    
    def __init__(self):
        self.cache = {}
        self.cache_timeout = 300  # 5分钟缓存
        
    def _get_cache_key(self, func_name: str, **kwargs) -> str:
        """生成缓存键"""
        key_parts = [func_name]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        return "_".join(key_parts)
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self.cache:
            return False
        
        cache_time = self.cache[cache_key].get('timestamp', 0)
        return (datetime.now().timestamp() - cache_time) < self.cache_timeout
    
    def _set_cache(self, cache_key: str, data):
        """设置缓存"""
        self.cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now().timestamp()
        }
    
    def _get_cache(self, cache_key: str):
        """获取缓存数据"""
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]['data']
        return None
    
    def get_trading_dates(self, days: int = 10) -> List[datetime.date]:
        """
        获取最近的交易日期
        
        Args:
            days: 获取最近几个交易日
            
        Returns:
            交易日期列表
        """
        cache_key = self._get_cache_key("trading_dates", days=days)
        cached_data = self._get_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        try:
            # 尝试多种方式获取交易日历
            trade_dates = None
            
            # 方法1：通过股票历史数据获取交易日期
            try:
                stock_data = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240901", end_date="20251231", adjust="")
                if stock_data is not None and not stock_data.empty:
                    dates = pd.to_datetime(stock_data['日期']).dt.date.unique()
                    dates = sorted(dates, reverse=True)  # 最新日期在前
                    recent_dates = dates[:days]
                    self._set_cache(cache_key, recent_dates)
                    logger.info(f"通过股票历史数据获取到{len(recent_dates)}个交易日期")
                    return recent_dates
            except Exception as e1:
                logger.warning(f"方法1获取交易日期失败: {e1}")
            
            # 方法2：尝试原始接口（如果可用）
            try:
                trade_dates = ak.tool_trade_date_hist_sina()
            except Exception as e2:
                logger.warning(f"原始交易日历接口失败: {e2}")
                trade_dates = None
            
            if trade_dates is not None and not trade_dates.empty:
                trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date']).dt.date
                recent_dates = trade_dates['trade_date'].tail(days).tolist()
                recent_dates.reverse()  # 最新日期在前
                
                self._set_cache(cache_key, recent_dates)
                return recent_dates
            else:
                # 方法3：生成近期工作日作为交易日
                logger.info("使用工作日回退方案生成交易日期")
                return self._get_fallback_dates(days)
                
        except Exception as e:
            logger.error(f"获取交易日期失败: {e}")
            # 生成默认日期列表
            today = datetime.now().date()
            dates = []
            current_date = today
            
            while len(dates) < days:
                if current_date.weekday() < 5:
                    dates.append(current_date)
                current_date -= timedelta(days=1)
            
            return dates
    
    def _get_fallback_dates(self, days: int) -> List[datetime.date]:
        """
        生成回退交易日期（工作日）
        
        Args:
            days: 需要的日期数量
            
        Returns:
            工作日日期列表
        """
        today = datetime.now().date()
        dates = []
        current_date = today
        
        # 如果今天是周末，先回退到最近的工作日
        while current_date.weekday() >= 5:  # 5=周六, 6=周日
            current_date -= timedelta(days=1)
        
        while len(dates) < days:
            # 只添加工作日
            if current_date.weekday() < 5:  # 0-4 是周一到周五
                dates.append(current_date)
            current_date -= timedelta(days=1)
        
        logger.info(f"生成了{len(dates)}个工作日作为交易日期")
        return dates
    
    def get_zhangting_data(self, trade_date: datetime.date) -> Optional[pd.DataFrame]:
        """
        获取指定日期的涨停数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            涨停数据DataFrame
        """
        cache_key = self._get_cache_key("zhangting_data", date=trade_date.strftime("%Y%m%d"))
        cached_data = self._get_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        try:
            # 使用问财查询涨停数据
            date_str = trade_date.strftime("%Y%m%d")
            query = f"涨停板 时间:{date_str}"
            
            # 这里我们用akshare的限售股解禁数据作为示例，实际项目中需要接入问财API
            # 由于问财需要特殊权限，我们先用模拟数据
            logger.info(f"获取{date_str}涨停数据")
            
            # 模拟涨停数据结构
            sample_data = {
                '股票代码': ['000001', '000002', '000003', '300001', '600001'],
                '股票名称': ['平安银行', '万科A', '国农科技', '特锐德', '邮储银行'],
                f'涨跌幅[{date_str}]': [10.02, 10.01, 9.99, 10.00, 10.03],
                f'最新价[{date_str}]': [12.34, 23.45, 15.67, 45.23, 6.78],
                f'成交额[{date_str}]': [123456789, 234567890, 156789012, 345678901, 98765432],
                f'连续涨停天数[{date_str}]': [1, 2, 1, 3, 1],
                f'几天几板[{date_str}]': ['1天1板', '2天2板', '1天1板', '3天3板', '1天1板'],
                f'涨停原因类别[{date_str}]': ['银行', '地产+基建', '农业', '新能源+汽车', '银行+金融'],
                f'a股市值(不含限售股)[{date_str}]': [1234567890, 2345678901, 567890123, 3456789012, 890123456]
            }
            
            df = pd.DataFrame(sample_data)
            
            # 数据清洗和处理
            df = self._clean_zhangting_data(df, date_str)
            
            self._set_cache(cache_key, df)
            return df
            
        except Exception as e:
            logger.error(f"获取涨停数据失败: {e}")
            return None
    
    def _clean_zhangting_data(self, df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        """
        清洗涨停数据
        
        Args:
            df: 原始数据
            date_str: 日期字符串
            
        Returns:
            清洗后的数据
        """
        if df is None or df.empty:
            return df
        
        try:
            # 标准化列名
            column_mapping = {
                f'涨跌幅[{date_str}]': '涨跌幅',
                f'最新价[{date_str}]': '最新价',
                f'成交额[{date_str}]': '成交额',
                f'连续涨停天数[{date_str}]': '连续涨停天数',
                f'几天几板[{date_str}]': '几天几板',
                f'涨停原因类别[{date_str}]': '涨停原因',
                f'a股市值(不含限售股)[{date_str}]': '总市值'
            }
            
            # 重命名存在的列
            final_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
            if final_mapping:
                df = df.rename(columns=final_mapping)
            
            # 数据类型转换
            numeric_cols = ['涨跌幅', '最新价', '成交额', '连续涨停天数', '总市值']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 排序：按连续涨停天数降序，成交额降序
            if '连续涨停天数' in df.columns and '成交额' in df.columns:
                df = df.sort_values(['连续涨停天数', '成交额'], ascending=[False, False])
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"清洗涨停数据失败: {e}")
            return df
    
    def get_concept_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        统计涨停概念分布
        
        Args:
            df: 涨停数据
            
        Returns:
            概念统计DataFrame
        """
        if df is None or df.empty or '涨停原因' not in df.columns:
            return pd.DataFrame()
        
        try:
            # 分解概念（多个概念用+分隔）
            concepts = df['涨停原因'].astype(str).str.split('+').explode().reset_index(drop=True)
            concepts = concepts[concepts != 'nan']  # 移除空值
            
            # 统计概念出现次数
            concept_counts = concepts.value_counts().reset_index()
            concept_counts.columns = ['概念', '涨停数量']
            
            # 计算概念占比
            total_count = len(df)
            concept_counts['占比(%)'] = (concept_counts['涨停数量'] / total_count * 100).round(2)
            
            return concept_counts.head(20)  # 返回前20个热门概念
            
        except Exception as e:
            logger.error(f"统计涨停概念失败: {e}")
            return pd.DataFrame()
    
    def get_lianban_statistics(self, df: pd.DataFrame) -> Dict:
        """
        获取连板统计信息
        
        Args:
            df: 涨停数据
            
        Returns:
            连板统计字典
        """
        if df is None or df.empty:
            return {}
        
        try:
            stats = {}
            
            # 总涨停数量
            stats['总涨停数量'] = int(len(df))
            
            # 连板分布统计
            if '连续涨停天数' in df.columns:
                lianban_counts = df['连续涨停天数'].value_counts().sort_index()
                # 转换为Python原生int类型，避免JSON序列化问题
                stats['连板分布'] = {int(k): int(v) for k, v in lianban_counts.to_dict().items()}
                
                # 最高连板
                stats['最高连板'] = int(df['连续涨停天数'].max()) if not df['连续涨停天数'].isna().all() else 0
                
                # 各种连板的数量
                stats['首板数量'] = int(lianban_counts.get(1, 0))
                stats['二板数量'] = int(lianban_counts.get(2, 0))
                stats['三板以上数量'] = int(lianban_counts[lianban_counts.index >= 3].sum()) if len(lianban_counts[lianban_counts.index >= 3]) > 0 else 0
            
            # 成交额统计
            if '成交额' in df.columns:
                total_amount = float(df['成交额'].sum())
                stats['总成交额'] = total_amount
                stats['平均成交额'] = float(df['成交额'].mean())
                
                # 大额成交（>10亿）
                big_amount = df[df['成交额'] > 1000000000]
                stats['大额成交数量'] = int(len(big_amount))
            
            return stats
            
        except Exception as e:
            logger.error(f"统计连板信息失败: {e}")
            return {}
    
    def calculate_promotion_rate(self, current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算连板晋级率
        
        Args:
            current_df: 今日涨停数据
            previous_df: 昨日涨停数据
            
        Returns:
            晋级率统计DataFrame
        """
        if current_df is None or previous_df is None or current_df.empty or previous_df.empty:
            return pd.DataFrame()
        
        try:
            promotion_data = []
            
            # 获取最大连板数
            max_days_current = current_df['连续涨停天数'].max() if '连续涨停天数' in current_df.columns else 0
            max_days_previous = previous_df['连续涨停天数'].max() if '连续涨停天数' in previous_df.columns else 0
            max_days = int(max(max_days_current, max_days_previous, 5))  # 至少统计到5板
            
            for days in range(1, max_days + 1):
                # 昨日该连板数的股票数量
                prev_count = len(previous_df[previous_df['连续涨停天数'] == days]) if '连续涨停天数' in previous_df.columns else 0
                
                # 今日晋级到下一板的数量（即今日连板数为days+1的股票）
                promoted_count = len(current_df[current_df['连续涨停天数'] == days + 1]) if '连续涨停天数' in current_df.columns else 0
                
                # 计算晋级率
                promotion_rate = (promoted_count / prev_count * 100) if prev_count > 0 else 0
                
                promotion_data.append({
                    '连板类型': f'{days}板 → {days+1}板',
                    '昨日数量': prev_count,
                    '今日晋级': promoted_count,
                    '晋级率(%)': round(promotion_rate, 2)
                })
            
            return pd.DataFrame(promotion_data)
            
        except Exception as e:
            logger.error(f"计算晋级率失败: {e}")
            return pd.DataFrame()
    
    def get_market_sentiment(self, df: pd.DataFrame) -> Dict:
        """
        分析市场情绪指标
        
        Args:
            df: 涨停数据
            
        Returns:
            情绪指标字典
        """
        if df is None or df.empty:
            return {}
        
        try:
            sentiment = {}
            
            # 涨停数量情绪
            total_count = len(df)
            if total_count >= 100:
                sentiment['市场热度'] = '极热'
                sentiment['热度评分'] = 5
            elif total_count >= 60:
                sentiment['市场热度'] = '较热'
                sentiment['热度评分'] = 4
            elif total_count >= 30:
                sentiment['市场热度'] = '一般'
                sentiment['热度评分'] = 3
            elif total_count >= 10:
                sentiment['市场热度'] = '偏冷'
                sentiment['热度评分'] = 2
            else:
                sentiment['市场热度'] = '冷淡'
                sentiment['热度评分'] = 1
            
            # 连板结构情绪
            if '连续涨停天数' in df.columns:
                max_lianban = df['连续涨停天数'].max()
                high_lianban_count = len(df[df['连续涨停天数'] >= 3])
                
                sentiment['最高连板'] = int(max_lianban) if not pd.isna(max_lianban) else 0
                sentiment['高度连板数量'] = high_lianban_count
                
                # 高度板占比
                high_board_ratio = (high_lianban_count / total_count * 100) if total_count > 0 else 0
                sentiment['高度板占比'] = round(high_board_ratio, 2)
                
                if high_board_ratio >= 15:
                    sentiment['连板结构'] = '优秀'
                elif high_board_ratio >= 8:
                    sentiment['连板结构'] = '良好'
                elif high_board_ratio >= 3:
                    sentiment['连板结构'] = '一般'
                else:
                    sentiment['连板结构'] = '较差'
            
            return sentiment
            
        except Exception as e:
            logger.error(f"分析市场情绪失败: {e}")
            return {}

    def get_leader_stocks(self, df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """
        获取龙头股票
        
        Args:
            df: 涨停数据
            top_n: 返回前N个龙头股
            
        Returns:
            龙头股DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        try:
            # 按连续涨停天数和成交额排序，筛选龙头
            leader_df = df.copy()
            
            # 计算龙头评分：连板天数权重70% + 成交额权重30%
            if '连续涨停天数' in leader_df.columns and '成交额' in leader_df.columns:
                # 标准化分数
                max_lianban = leader_df['连续涨停天数'].max()
                max_amount = leader_df['成交额'].max()
                
                if max_lianban > 0 and max_amount > 0:
                    lianban_score = leader_df['连续涨停天数'] / max_lianban
                    amount_score = leader_df['成交额'] / max_amount
                    
                    leader_df['龙头评分'] = (lianban_score * 0.7 + amount_score * 0.3) * 100
                    leader_df = leader_df.sort_values('龙头评分', ascending=False)
            
            # 选择关键列
            columns_to_show = ['股票代码', '股票名称', '连续涨停天数', '涨跌幅', '最新价', '成交额', '涨停原因']
            available_columns = [col for col in columns_to_show if col in leader_df.columns]
            
            result = leader_df[available_columns].head(top_n).copy()
            
            # 格式化显示
            if '成交额' in result.columns:
                result['成交额(万)'] = (result['成交额'] / 10000).round(0).astype(int)
                result.drop('成交额', axis=1, inplace=True)
            
            if '涨跌幅' in result.columns:
                result['涨跌幅(%)'] = result['涨跌幅'].round(2)
                result.drop('涨跌幅', axis=1, inplace=True)
            
            return result.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"获取龙头股失败: {e}")
            return df.head(top_n) if not df.empty else pd.DataFrame()
