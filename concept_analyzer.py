# -*- coding: utf-8 -*-
"""
概念板块分析模块
涨停概念统计、热门概念识别、概念轮动分析
"""

import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from collections import defaultdict, Counter
import re

logger = logging.getLogger(__name__)

class ConceptAnalyzer:
    """概念板块分析器"""
    
    def __init__(self):
        self.concept_cache = {}
        self.cache_timeout = 1800  # 30分钟缓存
        self.last_update = {}
        
    def analyze_daily_concepts(self, date: str = None) -> Dict[str, Any]:
        """分析当日概念板块表现"""
        try:
            if date is None:
                date = datetime.now().strftime('%Y%m%d')
            
            cache_key = f"daily_concepts_{date}"
            if self._is_cache_valid(cache_key):
                return self.concept_cache[cache_key]
            
            analysis_result = {
                'date': date,
                'limit_up_concepts': {},
                'hot_concepts': [],
                'concept_performance': {},
                'concept_trend': {}
            }
            
            # 1. 涨停概念统计
            limit_up_concepts = self._analyze_limit_up_concepts(date)
            analysis_result['limit_up_concepts'] = limit_up_concepts
            
            # 2. 概念板块整体表现
            concept_performance = self._get_concept_performance()
            analysis_result['concept_performance'] = concept_performance
            
            # 3. 热门概念识别
            hot_concepts = self._identify_hot_concepts(concept_performance, limit_up_concepts)
            analysis_result['hot_concepts'] = hot_concepts
            
            # 4. 概念趋势分析
            concept_trend = self._analyze_concept_trend(hot_concepts)
            analysis_result['concept_trend'] = concept_trend
            
            # 缓存结果
            self.concept_cache[cache_key] = analysis_result
            self.last_update[cache_key] = datetime.now()
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"分析概念板块失败: {e}")
            return self._get_default_concept_analysis(date)
    
    def get_concept_history(self, days: int = 30) -> Dict[str, Any]:
        """获取概念板块历史趋势"""
        try:
            cache_key = f"concept_history_{days}"
            if self._is_cache_valid(cache_key):
                return self.concept_cache[cache_key]
            
            history_result = {
                'time_series': [],
                'trending_concepts': [],
                'concept_rotation': {}
            }
            
            # 获取历史数据
            end_date = datetime.now()
            concept_counts = defaultdict(list)
            
            for i in range(days):
                target_date = end_date - timedelta(days=i)
                date_str = target_date.strftime('%Y%m%d')
                
                # 获取当日涨停概念
                daily_concepts = self._analyze_limit_up_concepts(date_str)
                
                # 记录时间序列
                history_result['time_series'].append({
                    'date': target_date.strftime('%Y-%m-%d'),
                    'total_limit_up': sum(daily_concepts.values()),
                    'concept_count': len(daily_concepts),
                    'top_concepts': dict(sorted(daily_concepts.items(), key=lambda x: x[1], reverse=True)[:5])
                })
                
                # 累计概念出现次数
                for concept, count in daily_concepts.items():
                    concept_counts[concept].append(count)
            
            # 识别趋势概念
            trending_concepts = self._identify_trending_concepts(concept_counts)
            history_result['trending_concepts'] = trending_concepts
            
            # 概念轮动分析
            concept_rotation = self._analyze_concept_rotation(concept_counts)
            history_result['concept_rotation'] = concept_rotation
            
            # 缓存结果
            self.concept_cache[cache_key] = history_result
            self.last_update[cache_key] = datetime.now()
            
            return history_result
            
        except Exception as e:
            logger.error(f"获取概念历史失败: {e}")
            return {'time_series': [], 'trending_concepts': [], 'concept_rotation': {}}
    
    def get_concept_stocks(self, concept_name: str) -> List[Dict[str, Any]]:
        """获取概念相关股票"""
        try:
            # 获取概念成分股
            concept_stocks = ak.stock_board_concept_cons_em(symbol=concept_name)
            
            if concept_stocks.empty:
                return []
            
            stocks_info = []
            for _, row in concept_stocks.head(20).iterrows():  # 限制数量
                stock_info = {
                    'code': row.get('代码', ''),
                    'name': row.get('名称', ''),
                    'latest_price': row.get('最新价', 0),
                    'change_pct': row.get('涨跌幅', 0),
                    'volume_ratio': row.get('量比', 0),
                    'turnover_rate': row.get('换手率', 0),
                    'pe_ratio': row.get('市盈率-动态', 0),
                    'market_cap': row.get('总市值', 0)
                }
                stocks_info.append(stock_info)
            
            # 按涨跌幅排序
            stocks_info.sort(key=lambda x: x['change_pct'], reverse=True)
            
            return stocks_info
            
        except Exception as e:
            logger.error(f"获取概念股票失败: {e}")
            return []
    
    def _analyze_limit_up_concepts(self, date: str) -> Dict[str, int]:
        """分析涨停股票的概念分布"""
        try:
            # 获取涨停股票
            limit_up_stocks = ak.stock_zt_pool_em(date=date)
            
            if limit_up_stocks.empty:
                return {}
            
            concept_counts = defaultdict(int)
            
            # 统计每只涨停股的概念
            for _, stock in limit_up_stocks.iterrows():
                stock_code = stock.get('代码', '')
                stock_name = stock.get('名称', '')
                
                # 获取股票所属概念
                try:
                    stock_concepts = self._get_stock_concepts(stock_code)
                    for concept in stock_concepts:
                        if concept and concept.strip():
                            concept_counts[concept.strip()] += 1
                except Exception as e:
                    logger.warning(f"获取股票{stock_code}概念失败: {e}")
                    continue
            
            # 过滤低频概念
            filtered_concepts = {k: v for k, v in concept_counts.items() if v >= 2}
            
            return dict(filtered_concepts)
            
        except Exception as e:
            logger.error(f"分析涨停概念失败: {e}")
            return {}
    
    def _get_stock_concepts(self, stock_code: str) -> List[str]:
        """获取单只股票的概念信息"""
        try:
            # 获取股票概念信息
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            
            concepts = []
            if not stock_info.empty:
                # 查找概念相关字段
                for _, row in stock_info.iterrows():
                    item = row.get('item', '')
                    value = row.get('value', '')
                    
                    if '概念' in item and value:
                        # 分割概念字符串
                        concept_list = re.split('[,，;；、]', str(value))
                        concepts.extend([c.strip() for c in concept_list if c.strip()])
            
            return concepts
            
        except Exception as e:
            logger.warning(f"获取股票{stock_code}概念信息失败: {e}")
            return []
    
    def _get_concept_performance(self) -> Dict[str, Dict[str, float]]:
        """获取概念板块整体表现"""
        try:
            # 获取概念板块数据
            try:
                concept_board = ak.stock_board_concept_name_em()
                if concept_board is None or concept_board.empty:
                    return {}
            except Exception as e:
                logger.warning(f"获取概念板块数据失败: {e}")
                return {}
            
            concept_performance = {}
            
            for _, row in concept_board.iterrows():
                concept_name = row.get('板块名称', '')
                if concept_name:
                    concept_performance[concept_name] = {
                        'change_pct': row.get('涨跌幅', 0),
                        'total_market_cap': row.get('总市值', 0),
                        'volume_ratio': row.get('量比', 0),
                        'up_count': row.get('上涨家数', 0),
                        'down_count': row.get('下跌家数', 0),
                        'leading_stock': row.get('领涨股票', ''),
                        'leading_change': row.get('领涨股票涨跌幅', 0)
                    }
            
            return concept_performance
            
        except Exception as e:
            logger.error(f"获取概念板块表现失败: {e}")
            return {}
    
    def _identify_hot_concepts(self, concept_performance: Dict[str, Dict[str, float]], 
                             limit_up_concepts: Dict[str, int]) -> List[Dict[str, Any]]:
        """识别热门概念"""
        try:
            hot_concepts = []
            
            # 综合评分算法
            for concept_name in set(list(concept_performance.keys()) + list(limit_up_concepts.keys())):
                score = 0
                concept_info = {
                    'name': concept_name,
                    'score': 0,
                    'limit_up_count': limit_up_concepts.get(concept_name, 0),
                    'change_pct': 0,
                    'volume_ratio': 0,
                    'heat_level': '一般'
                }
                
                # 涨停股数量得分（权重40%）
                limit_up_count = limit_up_concepts.get(concept_name, 0)
                score += limit_up_count * 10 * 0.4
                
                # 板块涨跌幅得分（权重30%）
                if concept_name in concept_performance:
                    perf = concept_performance[concept_name]
                    change_pct = perf.get('change_pct', 0)
                    volume_ratio = perf.get('volume_ratio', 1)
                    
                    concept_info['change_pct'] = change_pct
                    concept_info['volume_ratio'] = volume_ratio
                    
                    score += max(0, change_pct) * 0.3
                    
                    # 量比得分（权重20%）
                    if volume_ratio > 1:
                        score += (volume_ratio - 1) * 10 * 0.2
                    
                    # 上涨家数比例得分（权重10%）
                    up_count = perf.get('up_count', 0)
                    down_count = perf.get('down_count', 0)
                    total_count = up_count + down_count
                    if total_count > 0:
                        up_ratio = up_count / total_count
                        score += up_ratio * 10 * 0.1
                
                concept_info['score'] = round(score, 2)
                
                # 热度等级
                if score >= 50:
                    concept_info['heat_level'] = '极热'
                elif score >= 30:
                    concept_info['heat_level'] = '很热'
                elif score >= 20:
                    concept_info['heat_level'] = '较热'
                elif score >= 10:
                    concept_info['heat_level'] = '一般'
                else:
                    concept_info['heat_level'] = '冷淡'
                
                if score > 0:
                    hot_concepts.append(concept_info)
            
            # 按得分排序
            hot_concepts.sort(key=lambda x: x['score'], reverse=True)
            
            return hot_concepts[:20]  # 返回前20个
            
        except Exception as e:
            logger.error(f"识别热门概念失败: {e}")
            return []
    
    def _analyze_concept_trend(self, hot_concepts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析概念趋势"""
        try:
            if not hot_concepts:
                return {'trend': 'neutral', 'description': '无明显趋势'}
            
            # 统计热度分布
            heat_levels = [concept['heat_level'] for concept in hot_concepts]
            heat_counter = Counter(heat_levels)
            
            # 计算平均得分
            avg_score = np.mean([concept['score'] for concept in hot_concepts])
            
            trend_analysis = {
                'trend': 'neutral',
                'description': '',
                'avg_score': round(avg_score, 2),
                'heat_distribution': dict(heat_counter),
                'top_concepts': [concept['name'] for concept in hot_concepts[:5]]
            }
            
            # 判断市场趋势
            if heat_counter.get('极热', 0) >= 3:
                trend_analysis['trend'] = 'very_hot'
                trend_analysis['description'] = '市场概念炒作非常活跃，多个极热概念并存'
            elif heat_counter.get('很热', 0) + heat_counter.get('极热', 0) >= 5:
                trend_analysis['trend'] = 'hot'
                trend_analysis['description'] = '市场概念炒作活跃，建议关注热点轮动'
            elif avg_score >= 20:
                trend_analysis['trend'] = 'warm'
                trend_analysis['description'] = '市场有一定热点，概念表现温和'
            elif avg_score >= 10:
                trend_analysis['trend'] = 'neutral'
                trend_analysis['description'] = '市场概念表现平淡，缺乏明显热点'
            else:
                trend_analysis['trend'] = 'cold'
                trend_analysis['description'] = '市场概念整体偏冷，建议谨慎操作'
            
            return trend_analysis
            
        except Exception as e:
            logger.error(f"分析概念趋势失败: {e}")
            return {'trend': 'neutral', 'description': '趋势分析失败'}
    
    def _identify_trending_concepts(self, concept_counts: Dict[str, List[int]]) -> List[Dict[str, Any]]:
        """识别趋势概念"""
        trending_concepts = []
        
        for concept, counts in concept_counts.items():
            if len(counts) < 5:  # 数据不足
                continue
            
            # 计算趋势指标
            recent_avg = np.mean(counts[:7])  # 最近7天平均
            earlier_avg = np.mean(counts[7:14]) if len(counts) >= 14 else np.mean(counts[7:])  # 更早期平均
            
            if earlier_avg > 0:
                trend_ratio = recent_avg / earlier_avg
                total_appearances = sum(counts)
                
                trending_concepts.append({
                    'concept': concept,
                    'trend_ratio': round(trend_ratio, 2),
                    'recent_avg': round(recent_avg, 2),
                    'earlier_avg': round(earlier_avg, 2),
                    'total_appearances': total_appearances,
                    'trend_type': 'rising' if trend_ratio > 1.2 else 'falling' if trend_ratio < 0.8 else 'stable'
                })
        
        # 按趋势比例排序
        trending_concepts.sort(key=lambda x: x['trend_ratio'], reverse=True)
        
        return trending_concepts[:15]
    
    def _analyze_concept_rotation(self, concept_counts: Dict[str, List[int]]) -> Dict[str, Any]:
        """分析概念轮动"""
        try:
            rotation_analysis = {
                'rotation_speed': 'normal',
                'dominant_concepts': [],
                'emerging_concepts': [],
                'fading_concepts': []
            }
            
            # 识别主导概念（持续热门）
            dominant_concepts = []
            emerging_concepts = []
            fading_concepts = []
            
            for concept, counts in concept_counts.items():
                if len(counts) < 10:
                    continue
                
                recent_counts = counts[:5]
                middle_counts = counts[5:10]
                early_counts = counts[10:15] if len(counts) >= 15 else counts[10:]
                
                recent_avg = np.mean(recent_counts)
                middle_avg = np.mean(middle_counts) if middle_counts else 0
                early_avg = np.mean(early_counts) if early_counts else 0
                
                # 主导概念：持续高热度
                if recent_avg >= 3 and middle_avg >= 2:
                    dominant_concepts.append({
                        'concept': concept,
                        'recent_avg': round(recent_avg, 1),
                        'stability': round(min(recent_avg, middle_avg) / max(recent_avg, middle_avg, 0.1), 2)
                    })
                
                # 新兴概念：最近突然热门
                elif recent_avg >= 2 and middle_avg < 1:
                    emerging_concepts.append({
                        'concept': concept,
                        'recent_avg': round(recent_avg, 1),
                        'growth_rate': round((recent_avg - middle_avg) / max(middle_avg, 0.1), 2)
                    })
                
                # 衰落概念：热度下降
                elif middle_avg >= 2 and recent_avg < middle_avg * 0.5:
                    fading_concepts.append({
                        'concept': concept,
                        'recent_avg': round(recent_avg, 1),
                        'decline_rate': round((middle_avg - recent_avg) / max(middle_avg, 0.1), 2)
                    })
            
            # 排序
            dominant_concepts.sort(key=lambda x: x['recent_avg'], reverse=True)
            emerging_concepts.sort(key=lambda x: x['growth_rate'], reverse=True)
            fading_concepts.sort(key=lambda x: x['decline_rate'], reverse=True)
            
            rotation_analysis['dominant_concepts'] = dominant_concepts[:5]
            rotation_analysis['emerging_concepts'] = emerging_concepts[:5]
            rotation_analysis['fading_concepts'] = fading_concepts[:5]
            
            # 判断轮动速度
            if len(emerging_concepts) >= 3 and len(fading_concepts) >= 3:
                rotation_analysis['rotation_speed'] = 'fast'
            elif len(emerging_concepts) >= 2 or len(fading_concepts) >= 2:
                rotation_analysis['rotation_speed'] = 'normal'
            else:
                rotation_analysis['rotation_speed'] = 'slow'
            
            return rotation_analysis
            
        except Exception as e:
            logger.error(f"分析概念轮动失败: {e}")
            return {
                'rotation_speed': 'normal',
                'dominant_concepts': [],
                'emerging_concepts': [],
                'fading_concepts': []
            }
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self.concept_cache or cache_key not in self.last_update:
            return False
        
        time_diff = (datetime.now() - self.last_update[cache_key]).seconds
        return time_diff < self.cache_timeout
    
    def _get_default_concept_analysis(self, date: str = None) -> Dict[str, Any]:
        """获取默认概念分析结果"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        return {
            'date': date,
            'limit_up_concepts': {},
            'hot_concepts': [],
            'concept_performance': {},
            'concept_trend': {
                'trend': 'neutral',
                'description': '数据获取失败，无法分析趋势',
                'avg_score': 0,
                'heat_distribution': {},
                'top_concepts': []
            }
        }

# 创建全局实例
concept_analyzer = ConceptAnalyzer()
