#!/usr/bin/env python3
"""
市场情绪量化指标 v4.1
=====================
【优化目标】
将主观的"元子元情绪"维度（当前10分）升级为数据驱动的情绪量化评分（0-100分）

【核心指标】
1. 涨停跌停比率（40%权重）- 市场热度
2. 连板高度（20%权重）- 投机情绪
3. 量价背离检测（20%权重）- 风险预警
4. 板块轮动速度（20%权重）- 资金活跃度

【预期效果】
- 选股准确率提升 +5%
- 规避情绪退潮期的大幅回撤
- 更早识别情绪转折点
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

from claw.core.tushare_client import ts
from claw.core.config import settings
from claw.core.logging import get_logger

log = get_logger("sentiment")


class MarketSentimentAnalyzer:
    """市场情绪量化分析器"""
    
    def __init__(self):
        self.cache = {}
    
    def get_daily_market_data(self, trade_date: str) -> Dict:
        """获取当日市场基础数据"""
        if trade_date in self.cache:
            return self.cache[trade_date]
        
        # 获取涨停跌停数据
        daily_data = ts("daily", {"trade_date": trade_date}, 
                       fields="ts_code,trade_date,close,pct_chg,high,low,vol,amount")
        
        if daily_data.empty:
            return {}
        
        # 涨停跌停统计
        zt_count = len(daily_data[daily_data["pct_chg"] >= 9.5])
        dt_count = len(daily_data[daily_data["pct_chg"] <= -9.5])
        
        # 连板高度检测
        lb_height = self._detect_lianban_height(trade_date)
        
        # 量价背离检测
        divergence_score = self._detect_volume_price_divergence(trade_date)
        
        # 板块轮动速度
        rotation_speed = self._calculate_sector_rotation_speed(trade_date)
        
        result = {
            'trade_date': trade_date,
            'zt_count': zt_count,
            'dt_count': dt_count,
            'lianban_height': lb_height,
            'divergence_score': divergence_score,
            'rotation_speed': rotation_speed,
            'total_stocks': len(daily_data)
        }
        
        self.cache[trade_date] = result
        return result
    
    def _detect_lianban_height(self, trade_date: str) -> int:
        """检测市场最高连板高度"""
        # 获取最近5个交易日的涨停数据
        dates = self._get_recent_trade_dates(trade_date, 5)
        
        max_height = 0
        for date in dates:
            # 获取当日涨停股
            zt_stocks = ts("daily", {"trade_date": date, "pct_chg": {"$gte": 9.5}}, 
                          fields="ts_code,pct_chg")
            
            for stock in zt_stocks.itertuples():
                height = self._get_stock_lianban_height(stock.ts_code, date)
                max_height = max(max_height, height)
        
        return min(max_height, 10)  # 限制最大高度为10
    
    def _get_stock_lianban_height(self, ts_code: str, end_date: str) -> int:
        """获取个股连板高度"""
        # 获取最近10个交易日数据
        dates = self._get_recent_trade_dates(end_date, 10)
        
        height = 0
        for date in dates:
            daily = ts("daily", {"ts_code": ts_code, "trade_date": date}, 
                      fields="pct_chg")
            if not daily.empty and daily.iloc[0]["pct_chg"] >= 9.5:
                height += 1
            else:
                break
        
        return height
    
    def _detect_volume_price_divergence(self, trade_date: str) -> float:
        """检测市场整体量价背离程度（0-10分，越高风险越大）"""
        # 获取最近3个交易日数据
        dates = self._get_recent_trade_dates(trade_date, 3)
        
        if len(dates) < 3:
            return 0.0
        
        # 计算指数变化和成交量变化
        index_data = []
        for date in dates:
            daily = ts("index_daily", {"ts_code": "000001.SH", "trade_date": date}, 
                      fields="close,pct_chg,vol")
            if not daily.empty:
                index_data.append(daily.iloc[0])
        
        if len(index_data) < 3:
            return 0.0
        
        # 计算量价背离
        price_change = (index_data[-1]["close"] - index_data[0]["close"]) / index_data[0]["close"] * 100
        volume_change = (index_data[-1]["vol"] - index_data[0]["vol"]) / index_data[0]["vol"] * 100
        
        # 背离检测：价格上涨但成交量下降，或价格下跌但成交量上升
        divergence = 0.0
        if price_change > 0 and volume_change < -10:  # 价升量缩
            divergence = min(abs(volume_change) / 10, 10.0)
        elif price_change < 0 and volume_change > 10:  # 价跌量增
            divergence = min(abs(volume_change) / 10, 10.0)
        
        return divergence
    
    def _calculate_sector_rotation_speed(self, trade_date: str) -> float:
        """计算板块轮动速度（0-10分，越高轮动越快）"""
        # 获取最近2个交易日的板块涨停数据
        dates = self._get_recent_trade_dates(trade_date, 2)
        
        if len(dates) < 2:
            return 0.0
        
        # 获取各板块涨停数变化
        sector_zt_counts = {}
        for date in dates:
            # 获取当日涨停股及其行业
            zt_stocks = ts("daily", {"trade_date": date, "pct_chg": {"$gte": 9.5}}, 
                          fields="ts_code")
            
            sector_count = {}
            for stock in zt_stocks.itertuples():
                # 获取个股行业信息（简化版）
                basic_info = ts("stock_basic", {"ts_code": stock.ts_code}, 
                               fields="industry")
                if not basic_info.empty:
                    industry = basic_info.iloc[0]["industry"]
                    sector_count[industry] = sector_count.get(industry, 0) + 1
            
            sector_zt_counts[date] = sector_count
        
        # 计算板块轮动速度
        if len(sector_zt_counts) < 2:
            return 0.0
        
        prev_date, curr_date = dates[-2], dates[-1]
        prev_sectors = set(sector_zt_counts[prev_date].keys())
        curr_sectors = set(sector_zt_counts[curr_date].keys())
        
        # 新出现板块数 + 消失板块数
        new_sectors = len(curr_sectors - prev_sectors)
        disappeared_sectors = len(prev_sectors - curr_sectors)
        
        rotation_speed = (new_sectors + disappeared_sectors) / max(len(prev_sectors), 1)
        return min(rotation_speed * 10, 10.0)
    
    def _get_recent_trade_dates(self, end_date: str, days: int) -> List[str]:
        """获取最近的交易日列表"""
        cal_data = ts("trade_cal", {
            "exchange": "SSE", 
            "start_date": (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=days*2)).strftime("%Y%m%d"),
            "end_date": end_date,
            "is_open": "1"
        }, "cal_date")
        
        if cal_data.empty:
            return []
        
        dates = sorted(cal_data["cal_date"].tolist(), reverse=True)
        return dates[:days]
    
    def calculate_sentiment_score(self, trade_date: str) -> Dict[str, float]:
        """计算综合情绪得分（0-100分）"""
        market_data = self.get_daily_market_data(trade_date)
        
        if not market_data:
            return {'total_score': 50.0, 'components': {}}
        
        # 1. 涨停跌停比率得分（0-40分）
        zt_ratio = market_data['zt_count'] / max(market_data['zt_count'] + market_data['dt_count'], 1)
        zt_score = zt_ratio * 40
        
        # 2. 连板高度得分（0-20分）
        lb_score = min(market_data['lianban_height'] * 2, 20)
        
        # 3. 量价背离得分（0-20分，负向指标）
        divergence_score = max(0, 20 - market_data['divergence_score'] * 2)
        
        # 4. 板块轮动速度得分（0-20分，适度轮动最好）
        rotation_score = 10 - abs(market_data['rotation_speed'] - 5)  # 5分左右最佳
        rotation_score = max(0, min(20, rotation_score))
        
        # 综合得分
        total_score = zt_score + lb_score + divergence_score + rotation_score
        
        return {
            'total_score': total_score,
            'components': {
                '涨停跌停比率': zt_score,
                '连板高度': lb_score,
                '量价背离': divergence_score,
                '板块轮动': rotation_score
            },
            'market_data': market_data
        }
    
    def get_sentiment_level(self, score: float) -> Tuple[str, str]:
        """根据得分返回情绪等级和建议"""
        if score >= 80:
            return "过热", "⚠️ 情绪过热，注意风险控制，避免追高"
        elif score >= 70:
            return "活跃", "✅ 情绪活跃，适合龙头股操作"
        elif score >= 60:
            return "温和", "🟡 情绪温和，可适度参与"
        elif score >= 50:
            return "平淡", "🔵 情绪平淡，控制仓位"
        elif score >= 40:
            return "谨慎", "🟠 情绪谨慎，减少操作"
        else:
            return "冰点", "🔴 情绪冰点，观望为主"


def integrate_with_scoring_system(sentiment_score: float) -> float:
    """
    将情绪得分整合到九维评分系统中
    
    原系统：元子元情绪维度（10分）
    新系统：情绪量化得分（0-100分）映射到 0-15 分
    
    映射规则：
    - 80+分（过热）→ 扣分（-5分）
    - 70-80分（活跃）→ 加分（+5分）
    - 60-70分（温和）→ 正常（+2分）
    - 50-60分（平淡）→ 正常（0分）
    - 40-50分（谨慎）→ 扣分（-3分）
    - <40分（冰点）→ 扣分（-8分）
    """
    if sentiment_score >= 80:
        return -5.0  # 过热期风险大
    elif sentiment_score >= 70:
        return 5.0   # 活跃期机会多
    elif sentiment_score >= 60:
        return 2.0   # 温和期正常参与
    elif sentiment_score >= 50:
        return 0.0   # 平淡期中性
    elif sentiment_score >= 40:
        return -3.0  # 谨慎期控制风险
    else:
        return -8.0  # 冰点期观望


# ============================================================
# 使用示例
# ============================================================
if __name__ == "__main__":
    analyzer = MarketSentimentAnalyzer()
    
    # 测试今日情绪
    today = datetime.now().strftime("%Y%m%d")
    result = analyzer.calculate_sentiment_score(today)
    
    level, advice = analyzer.get_sentiment_level(result['total_score'])
    
    print(f"=== 市场情绪分析报告 {today} ===")
    print(f"综合情绪得分: {result['total_score']:.1f}/100")
    print(f"情绪等级: {level}")
    print(f"操作建议: {advice}")
    print("\n分项得分:")
    for comp, score in result['components'].items():
        print(f"  {comp}: {score:.1f}")
    
    print(f"\n市场数据:")
    print(f"  涨停家数: {result['market_data']['zt_count']}")
    print(f"  跌停家数: {result['market_data']['dt_count']}")
    print(f"  最高连板: {result['market_data']['lianban_height']}板")
    print(f"  量价背离: {result['market_data']['divergence_score']:.1f}")
    print(f"  板块轮动: {result['market_data']['rotation_speed']:.1f}")
    
    # 整合到评分系统
    scoring_adjustment = integrate_with_scoring_system(result['total_score'])
    print(f"\n评分系统调整: {scoring_adjustment:+}分")
