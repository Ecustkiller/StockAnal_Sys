# -*- coding: utf-8 -*-
"""
增强版数据收集器 - 为AI分析提供全面的多维度数据
==============================================
整合以下数据源：
1. 技术面数据（价格、均线、RSI、MACD、布林带等）
2. 基本面数据（PE、PB、ROE、营收增长、财务健康等）
3. 资金流数据（主力资金、北向资金、大单小单等）
4. 市场情绪数据（涨停跌停、封板率、赚钱效应、连板统计等）
5. 宏观环境数据（利率、成交额分位、中美利差等）
6. 行业对比数据（行业排名、板块资金流向等）
7. 新闻舆情数据（最新财经新闻摘要）

创建时间: 2026-04-25
"""

import json
import logging
import os
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class EnhancedDataCollector:
    """增强版数据收集器 - 为AI分析提供全面的多维度数据"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._cache = {}
        self._cache_ttl = 1800  # 缓存30分钟

        # 初始化各数据源
        self._init_data_sources()

    def _init_data_sources(self):
        """初始化各数据源连接"""
        # 基础分析器
        try:
            from stock_analyzer import StockAnalyzer
            self.stock_analyzer = StockAnalyzer()
        except Exception as e:
            self.logger.warning(f"StockAnalyzer初始化失败: {e}")
            self.stock_analyzer = None

        # 基本面分析器
        try:
            from fundamental_analyzer import FundamentalAnalyzer
            self.fundamental_analyzer = FundamentalAnalyzer()
        except Exception as e:
            self.logger.warning(f"FundamentalAnalyzer初始化失败: {e}")
            self.fundamental_analyzer = None

        # 资金流分析器
        try:
            from capital_flow_analyzer import CapitalFlowAnalyzer
            self.capital_flow_analyzer = CapitalFlowAnalyzer()
        except Exception as e:
            self.logger.warning(f"CapitalFlowAnalyzer初始化失败: {e}")
            self.capital_flow_analyzer = None

        # Tushare客户端
        try:
            from claw.core.tushare_client import TushareClient
            self.ts_client = TushareClient()
        except Exception as e:
            self.logger.warning(f"TushareClient初始化失败: {e}")
            self.ts_client = None

        # 新闻获取器
        try:
            from news_fetcher import NewsFetcher
            self.news_fetcher = NewsFetcher()
        except Exception as e:
            self.logger.warning(f"NewsFetcher初始化失败: {e}")
            self.news_fetcher = None

    def _get_cache(self, key: str) -> Optional[Any]:
        """获取缓存数据"""
        if key in self._cache:
            ts, data = self._cache[key]
            if time.time() - ts < self._cache_ttl:
                return data
        return None

    def _set_cache(self, key: str, data: Any):
        """设置缓存数据"""
        self._cache[key] = (time.time(), data)

    # ============================================================
    # 1. 技术面数据（增强版）
    # ============================================================
    def get_technical_data(self, stock_code: str, market_type: str = 'A') -> Dict:
        """获取增强版技术面数据，包含多周期指标"""
        cache_key = f"tech_{stock_code}_{market_type}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            if not self.stock_analyzer:
                return result

            # 获取较长时间的数据用于计算更多指标
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

            df = self.stock_analyzer.get_stock_data(stock_code, market_type, start_date, end_date)
            if df.empty:
                return result

            df = self.stock_analyzer.calculate_indicators(df)
            latest = df.iloc[-1]

            # 基础技术指标
            result['current_price'] = float(latest['close'])
            result['change_pct'] = float(latest.get('change_pct', latest.get('涨跌幅', 0)))
            result['volume'] = int(latest['volume'])
            result['turnover'] = float(latest.get('turnover', latest.get('换手率', 0)))

            # 均线系统
            for ma_col in ['MA5', 'MA10', 'MA20', 'MA60', 'ma_5', 'ma_10', 'ma_20', 'ma_60']:
                if ma_col in df.columns and pd.notna(latest.get(ma_col)):
                    clean_name = ma_col.lower().replace('ma_', 'ma')
                    result[clean_name] = float(latest[ma_col])

            # 趋势判断
            price = result.get('current_price', 0)
            ma5 = result.get('ma5', 0)
            ma20 = result.get('ma20', 0)
            ma60 = result.get('ma60', 0)
            if ma5 and ma20 and ma60:
                if price > ma5 > ma20 > ma60:
                    result['trend'] = '多头排列（强势上涨）'
                elif price < ma5 < ma20 < ma60:
                    result['trend'] = '空头排列（弱势下跌）'
                elif ma5 > ma20 and price > ma20:
                    result['trend'] = '偏多震荡'
                elif ma5 < ma20 and price < ma20:
                    result['trend'] = '偏空震荡'
                else:
                    result['trend'] = '盘整'

            # 动量指标
            for col in ['RSI', 'rsi']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['rsi'] = float(latest[col])
                    if result['rsi'] > 80:
                        result['rsi_signal'] = '严重超买'
                    elif result['rsi'] > 70:
                        result['rsi_signal'] = '超买'
                    elif result['rsi'] < 20:
                        result['rsi_signal'] = '严重超卖'
                    elif result['rsi'] < 30:
                        result['rsi_signal'] = '超卖'
                    else:
                        result['rsi_signal'] = '中性'
                    break

            for col in ['MACD', 'macd']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['macd'] = float(latest[col])
                    break
            for col in ['Signal', 'signal']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['macd_signal'] = float(latest[col])
                    break

            # MACD金叉/死叉判断
            if 'macd' in result and 'macd_signal' in result and len(df) > 2:
                prev = df.iloc[-2]
                macd_col = 'MACD' if 'MACD' in df.columns else 'macd'
                signal_col = 'Signal' if 'Signal' in df.columns else 'signal'
                if macd_col in df.columns and signal_col in df.columns:
                    prev_macd = prev.get(macd_col, 0)
                    prev_signal = prev.get(signal_col, 0)
                    if pd.notna(prev_macd) and pd.notna(prev_signal):
                        if result['macd'] > result['macd_signal'] and prev_macd <= prev_signal:
                            result['macd_cross'] = 'MACD金叉（买入信号）'
                        elif result['macd'] < result['macd_signal'] and prev_macd >= prev_signal:
                            result['macd_cross'] = 'MACD死叉（卖出信号）'

            # 布林带
            for col in ['BB_upper', 'bollinger_upper']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['bollinger_upper'] = float(latest[col])
                    break
            for col in ['BB_lower', 'bollinger_lower']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['bollinger_lower'] = float(latest[col])
                    break
            for col in ['BB_middle', 'bollinger_middle']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['bollinger_middle'] = float(latest[col])
                    break

            # 布林带位置判断
            if 'bollinger_upper' in result and 'bollinger_lower' in result:
                bb_width = result['bollinger_upper'] - result['bollinger_lower']
                if bb_width > 0:
                    bb_pos = (price - result['bollinger_lower']) / bb_width
                    result['bollinger_position'] = f"{bb_pos:.1%}"
                    if bb_pos > 1:
                        result['bollinger_signal'] = '突破上轨（超强/超买）'
                    elif bb_pos > 0.8:
                        result['bollinger_signal'] = '接近上轨（偏强）'
                    elif bb_pos < 0:
                        result['bollinger_signal'] = '跌破下轨（超弱/超卖）'
                    elif bb_pos < 0.2:
                        result['bollinger_signal'] = '接近下轨（偏弱）'
                    else:
                        result['bollinger_signal'] = '中轨附近'

            # 波动率
            for col in ['ATR', 'atr', 'Volatility']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['atr'] = float(latest[col])
                    break

            # 量比
            for col in ['Volume_Ratio', 'volume_ratio']:
                if col in df.columns and pd.notna(latest.get(col)):
                    result['volume_ratio'] = float(latest[col])
                    if result['volume_ratio'] > 3:
                        result['volume_signal'] = '巨量（异常放量）'
                    elif result['volume_ratio'] > 2:
                        result['volume_signal'] = '显著放量'
                    elif result['volume_ratio'] > 1.5:
                        result['volume_signal'] = '温和放量'
                    elif result['volume_ratio'] < 0.5:
                        result['volume_signal'] = '严重缩量'
                    elif result['volume_ratio'] < 0.8:
                        result['volume_signal'] = '缩量'
                    else:
                        result['volume_signal'] = '正常'
                    break

            # 近期涨跌幅统计
            if len(df) >= 5:
                result['change_5d'] = round(
                    (float(df.iloc[-1]['close']) / float(df.iloc[-5]['close']) - 1) * 100, 2)
            if len(df) >= 20:
                result['change_20d'] = round(
                    (float(df.iloc[-1]['close']) / float(df.iloc[-20]['close']) - 1) * 100, 2)
            if len(df) >= 60:
                result['change_60d'] = round(
                    (float(df.iloc[-1]['close']) / float(df.iloc[-60]['close']) - 1) * 100, 2)

            # 支撑压力位
            try:
                sr_levels = self.stock_analyzer.identify_support_resistance(df)
                if sr_levels:
                    supports = sr_levels.get('support_levels', {})
                    resistances = sr_levels.get('resistance_levels', {})
                    if supports.get('short_term'):
                        result['support_short'] = [round(float(x), 2) for x in supports['short_term'][:3]]
                    if resistances.get('short_term'):
                        result['resistance_short'] = [round(float(x), 2) for x in resistances['short_term'][:3]]
            except Exception:
                pass

        except Exception as e:
            self.logger.error(f"获取技术面数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 2. 基本面数据
    # ============================================================
    def get_fundamental_data(self, stock_code: str) -> Dict:
        """获取基本面数据：估值、盈利能力、成长性、财务健康"""
        cache_key = f"fundamental_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            # 方式1：通过FundamentalAnalyzer获取
            if self.fundamental_analyzer:
                try:
                    indicators = self.fundamental_analyzer.get_financial_indicators(stock_code)
                    if indicators:
                        result['pe_ttm'] = indicators.get('pe_ttm')
                        result['pb'] = indicators.get('pb')
                        result['ps_ttm'] = indicators.get('ps_ttm')
                        result['roe'] = indicators.get('roe')
                        result['gross_margin'] = indicators.get('gross_margin')
                        result['net_profit_margin'] = indicators.get('net_profit_margin')
                        result['debt_ratio'] = indicators.get('debt_ratio')

                        # 估值判断
                        pe = result.get('pe_ttm')
                        if pe and pe > 0:
                            if pe < 15:
                                result['valuation_signal'] = '低估值（PE<15）'
                            elif pe < 25:
                                result['valuation_signal'] = '合理估值（15<PE<25）'
                            elif pe < 40:
                                result['valuation_signal'] = '偏高估值（25<PE<40）'
                            else:
                                result['valuation_signal'] = '高估值（PE>40）'

                        # 盈利能力判断
                        roe = result.get('roe')
                        if roe:
                            if roe > 20:
                                result['profitability_signal'] = '优秀盈利能力（ROE>20%）'
                            elif roe > 15:
                                result['profitability_signal'] = '良好盈利能力（ROE>15%）'
                            elif roe > 10:
                                result['profitability_signal'] = '一般盈利能力（ROE>10%）'
                            else:
                                result['profitability_signal'] = '较弱盈利能力（ROE<10%）'

                        # 财务健康判断
                        debt = result.get('debt_ratio')
                        if debt:
                            if debt < 30:
                                result['financial_health'] = '财务非常健康（负债率<30%）'
                            elif debt < 50:
                                result['financial_health'] = '财务健康（负债率<50%）'
                            elif debt < 70:
                                result['financial_health'] = '财务一般（负债率<70%）'
                            else:
                                result['financial_health'] = '财务风险较高（负债率>70%）'
                except Exception as e:
                    self.logger.warning(f"FundamentalAnalyzer获取数据失败: {e}")

            # 方式2：通过Tushare获取补充数据
            if self.ts_client:
                try:
                    ts_code = self._to_ts_code(stock_code)
                    # 获取每日基本面指标
                    today = datetime.now().strftime('%Y%m%d')
                    daily_basic = self.ts_client.call("daily_basic",
                                                      ts_code=ts_code,
                                                      start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                                                      end_date=today,
                                                      fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate_f")
                    if not daily_basic.empty:
                        latest_basic = daily_basic.sort_values('trade_date', ascending=False).iloc[0]
                        if 'pe_ttm' not in result or result['pe_ttm'] is None:
                            result['pe_ttm'] = float(latest_basic.get('pe_ttm', 0)) if pd.notna(latest_basic.get('pe_ttm')) else None
                        if 'pb' not in result or result['pb'] is None:
                            result['pb'] = float(latest_basic.get('pb', 0)) if pd.notna(latest_basic.get('pb')) else None
                        result['total_mv'] = round(float(latest_basic.get('total_mv', 0)) / 10000, 2) if pd.notna(latest_basic.get('total_mv')) else None  # 亿元
                        result['circ_mv'] = round(float(latest_basic.get('circ_mv', 0)) / 10000, 2) if pd.notna(latest_basic.get('circ_mv')) else None  # 亿元
                        result['turnover_rate'] = float(latest_basic.get('turnover_rate_f', 0)) if pd.notna(latest_basic.get('turnover_rate_f')) else None

                        # 市值规模判断
                        mv = result.get('total_mv')
                        if mv:
                            if mv > 1000:
                                result['market_cap_level'] = '超大盘股（>1000亿）'
                            elif mv > 300:
                                result['market_cap_level'] = '大盘股（300-1000亿）'
                            elif mv > 100:
                                result['market_cap_level'] = '中盘股（100-300亿）'
                            elif mv > 30:
                                result['market_cap_level'] = '小盘股（30-100亿）'
                            else:
                                result['market_cap_level'] = '微盘股（<30亿）'
                    time.sleep(0.3)
                except Exception as e:
                    self.logger.warning(f"Tushare基本面数据获取失败: {e}")

            # 获取成长性数据
            if self.fundamental_analyzer:
                try:
                    growth = self.fundamental_analyzer.get_growth_data(stock_code)
                    if growth:
                        result['revenue_growth_3y'] = growth.get('revenue_growth_3y')
                        result['profit_growth_3y'] = growth.get('profit_growth_3y')

                        rev_g = result.get('revenue_growth_3y')
                        if rev_g is not None:
                            if rev_g > 30:
                                result['growth_signal'] = '高速成长（营收3年CAGR>30%）'
                            elif rev_g > 15:
                                result['growth_signal'] = '稳健成长（营收3年CAGR>15%）'
                            elif rev_g > 0:
                                result['growth_signal'] = '低速成长（营收3年CAGR>0%）'
                            else:
                                result['growth_signal'] = '营收萎缩（营收3年CAGR<0%）'
                except Exception as e:
                    self.logger.warning(f"成长性数据获取失败: {e}")

        except Exception as e:
            self.logger.error(f"获取基本面数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 3. 资金流数据
    # ============================================================
    def get_capital_flow_data(self, stock_code: str, market_type: str = '') -> Dict:
        """获取资金流数据：主力资金、大单小单、北向资金"""
        cache_key = f"capital_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            if self.capital_flow_analyzer:
                flow_data = self.capital_flow_analyzer.get_individual_fund_flow(stock_code, market_type)
                if flow_data and flow_data.get('data') and flow_data.get('summary'):
                    summary = flow_data['summary']
                    result['main_net_inflow_total'] = round(summary.get('total_main_net_inflow', 0) / 10000, 2)  # 万元转亿
                    result['avg_main_net_inflow_pct'] = round(summary.get('avg_main_net_inflow_percent', 0), 2)
                    result['positive_days'] = summary.get('positive_days', 0)
                    result['negative_days'] = summary.get('negative_days', 0)
                    result['recent_days'] = summary.get('recent_days', 0)

                    # 最近3天的资金流向
                    recent_3d = flow_data['data'][:3]
                    if recent_3d:
                        result['recent_3d_main_flow'] = [
                            {
                                'date': item.get('date', ''),
                                'main_net': round(item.get('main_net_inflow', 0) / 10000, 2),
                                'super_large_net': round(item.get('super_large_net_inflow', 0) / 10000, 2),
                            }
                            for item in recent_3d
                        ]

                    # 资金流向判断
                    pos_days = result.get('positive_days', 0)
                    neg_days = result.get('negative_days', 0)
                    total_flow = result.get('main_net_inflow_total', 0)
                    if pos_days > neg_days and total_flow > 0:
                        result['capital_signal'] = f'主力持续流入（近{result["recent_days"]}日{pos_days}天净流入）'
                    elif neg_days > pos_days and total_flow < 0:
                        result['capital_signal'] = f'主力持续流出（近{result["recent_days"]}日{neg_days}天净流出）'
                    else:
                        result['capital_signal'] = '资金流向不明确'

                # 资金流评分
                score = self.capital_flow_analyzer.calculate_capital_flow_score(stock_code, market_type)
                if score:
                    result['capital_score'] = score.get('total', 0)
                    result['main_force_score'] = score.get('main_force', 0)

        except Exception as e:
            self.logger.error(f"获取资金流数据失败: {e}")

        # 北向资金（全市场）
        try:
            if self.ts_client:
                today = datetime.now().strftime('%Y%m%d')
                start = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
                hsgt = self.ts_client.call("moneyflow_hsgt",
                                           start_date=start, end_date=today)
                if not hsgt.empty:
                    hsgt = hsgt.sort_values('trade_date', ascending=False)
                    latest_hsgt = hsgt.iloc[0]
                    result['north_money_today'] = round(float(latest_hsgt.get('north_money', 0)), 2)
                    # 近5日北向累计
                    recent_5 = hsgt.head(5)
                    result['north_money_5d'] = round(recent_5['north_money'].astype(float).sum(), 2)

                    if result['north_money_today'] > 50:
                        result['north_signal'] = '北向资金大幅流入'
                    elif result['north_money_today'] > 0:
                        result['north_signal'] = '北向资金小幅流入'
                    elif result['north_money_today'] > -50:
                        result['north_signal'] = '北向资金小幅流出'
                    else:
                        result['north_signal'] = '北向资金大幅流出'
                time.sleep(0.3)
        except Exception as e:
            self.logger.warning(f"北向资金数据获取失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 4. 市场情绪数据
    # ============================================================
    def get_market_sentiment_data(self) -> Dict:
        """获取市场情绪数据：涨停跌停、封板率、赚钱效应等"""
        cache_key = "market_sentiment"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            from claw.analysis.market_sentiment import get_market_sentiment, get_trade_dates
            trade_dates = get_trade_dates(
                (datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))
            if trade_dates:
                latest_date = trade_dates[-1]
                sentiment = get_market_sentiment(latest_date)
                if sentiment:
                    result['date'] = sentiment.get('date', '')
                    result['zt_cnt'] = sentiment.get('zt_cnt', 0)
                    result['zb_cnt'] = sentiment.get('zb_cnt', 0)
                    result['dt_cnt'] = sentiment.get('dt_cnt', 0)
                    result['fbl'] = sentiment.get('fbl', 0)
                    result['earn_rate'] = sentiment.get('earn_rate', 0)
                    result['total_amount'] = sentiment.get('total_amount', 0)
                    result['total_stocks'] = sentiment.get('total_stocks', 0)
                    result['up_cnt'] = sentiment.get('up_cnt', 0)
                    result['down_cnt'] = sentiment.get('down_cnt', 0)
                    result['max_board'] = sentiment.get('max_board', 0)
                    result['board_dist'] = sentiment.get('board_dist', {})
                    result['ind_zt_top10'] = sentiment.get('ind_zt_top10', [])

                    # BJCJ情绪判定
                    result['emotion_phase'] = sentiment.get('bjcj3_phase', '未知')
                    result['suggested_position'] = sentiment.get('bjcj3_pos', '未知')
                    result['max_position_pct'] = sentiment.get('bjcj3_max_pct', 0)

                    # 综合情绪判断
                    fbl = result.get('fbl', 0)
                    earn = result.get('earn_rate', 0)
                    if fbl > 80 and earn > 60:
                        result['sentiment_signal'] = '市场极度亢奋（封板率>80%，赚钱效应>60%）'
                    elif fbl > 60 and earn > 50:
                        result['sentiment_signal'] = '市场情绪偏热（封板率>60%，赚钱效应>50%）'
                    elif fbl > 40 and earn > 40:
                        result['sentiment_signal'] = '市场情绪正常'
                    elif fbl < 40 or earn < 30:
                        result['sentiment_signal'] = '市场情绪偏冷（封板率<40%或赚钱效应<30%）'
                    else:
                        result['sentiment_signal'] = '市场情绪低迷'

        except ImportError:
            self.logger.warning("claw.analysis.market_sentiment模块不可用，跳过市场情绪数据")
        except Exception as e:
            self.logger.warning(f"获取市场情绪数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 5. 宏观环境数据
    # ============================================================
    def get_macro_data(self) -> Dict:
        """获取宏观环境数据：利率、成交额分位、中美利差等"""
        cache_key = "macro_data"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            if self.ts_client:
                today = datetime.now().strftime('%Y%m%d')
                start_1y = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

                # 获取沪深300指数数据用于计算成交额分位
                idx_data = self.ts_client.call("index_daily",
                                                ts_code="399300.SZ",
                                                start_date=start_1y,
                                                end_date=today,
                                                fields="trade_date,close,amount,vol")
                if not idx_data.empty:
                    idx_data = idx_data.sort_values('trade_date')
                    idx_data['amount'] = pd.to_numeric(idx_data['amount'], errors='coerce')

                    # 成交额252日分位
                    latest_amount = idx_data['amount'].iloc[-1]
                    amount_pct = (idx_data['amount'] < latest_amount).sum() / len(idx_data)
                    result['market_amount_percentile'] = round(amount_pct * 100, 1)

                    if amount_pct > 0.8:
                        result['amount_signal'] = '成交额处于历史高位（>80%分位），市场活跃'
                    elif amount_pct > 0.5:
                        result['amount_signal'] = '成交额处于中等水平'
                    elif amount_pct > 0.2:
                        result['amount_signal'] = '成交额偏低，市场较冷清'
                    else:
                        result['amount_signal'] = '成交额处于历史低位（<20%分位），市场极度冷清'

                    # 沪深300近期表现
                    idx_data['close'] = pd.to_numeric(idx_data['close'], errors='coerce')
                    if len(idx_data) >= 5:
                        result['hs300_change_5d'] = round(
                            (float(idx_data['close'].iloc[-1]) / float(idx_data['close'].iloc[-5]) - 1) * 100, 2)
                    if len(idx_data) >= 20:
                        result['hs300_change_20d'] = round(
                            (float(idx_data['close'].iloc[-1]) / float(idx_data['close'].iloc[-20]) - 1) * 100, 2)

                time.sleep(0.5)

                # 中美利差数据
                try:
                    from claw.timing.factors.macro import _load_us10y, _load_cn10y
                    start_3m = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
                    us10y = _load_us10y(start_3m, today)
                    cn10y = _load_cn10y(start_3m, today)
                    if not us10y.empty and not cn10y.empty:
                        latest_us = us10y.sort_values('trade_date').iloc[-1]
                        latest_cn = cn10y.sort_values('trade_date').iloc[-1]
                        us_rate = float(latest_us.get('us_y10', 0))
                        cn_rate = float(latest_cn.get('cn_y10', 0))
                        spread = us_rate - cn_rate
                        result['us_10y_rate'] = round(us_rate, 3)
                        result['cn_10y_rate'] = round(cn_rate, 3)
                        result['us_cn_spread'] = round(spread, 3)

                        if spread > 2:
                            result['spread_signal'] = '中美利差较大，资金外流压力较大'
                        elif spread > 1:
                            result['spread_signal'] = '中美利差适中'
                        elif spread > 0:
                            result['spread_signal'] = '中美利差收窄，资金回流预期'
                        else:
                            result['spread_signal'] = '中美利差倒挂，有利于资金回流'
                except Exception as e:
                    self.logger.warning(f"利差数据获取失败: {e}")

        except Exception as e:
            self.logger.error(f"获取宏观数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 6. 行业对比数据
    # ============================================================
    def get_industry_data(self, stock_code: str) -> Dict:
        """获取行业对比数据"""
        cache_key = f"industry_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            # 获取股票所属行业
            if self.stock_analyzer:
                stock_info = self.stock_analyzer.get_stock_info(stock_code)
                if stock_info:
                    result['industry'] = stock_info.get('行业', '未知')
                    result['stock_name'] = stock_info.get('股票名称', '未知')

            # 获取行业资金流向
            if self.capital_flow_analyzer:
                try:
                    concept_flow = self.capital_flow_analyzer.get_concept_fund_flow("今日排行")
                    if concept_flow:
                        # 找到该股票所属行业的资金流向
                        industry = result.get('industry', '')
                        for item in concept_flow[:10]:
                            if industry and industry in str(item.get('sector', '')):
                                result['industry_net_flow'] = item.get('net_flow', 0)
                                result['industry_change_pct'] = item.get('change_percent', 0)
                                break

                        # 资金流入TOP5行业
                        top5 = concept_flow[:5]
                        result['hot_sectors'] = [
                            f"{item.get('sector', '')}(净流入{item.get('net_flow', 0):.1f}亿)"
                            for item in top5 if item.get('net_flow', 0) > 0
                        ]
                except Exception as e:
                    self.logger.warning(f"行业资金流向获取失败: {e}")

        except Exception as e:
            self.logger.error(f"获取行业数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 7. 新闻舆情数据
    # ============================================================
    def get_news_data(self, stock_code: str = '', stock_name: str = '') -> Dict:
        """获取最新新闻摘要"""
        cache_key = f"news_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = {}
        try:
            if self.news_fetcher:
                news_list = self.news_fetcher.get_latest_news(days=1, limit=20)
                if news_list:
                    # 筛选与该股票相关的新闻
                    related_news = []
                    general_news = []
                    for news in news_list:
                        content = news.get('content', '') + news.get('title', '')
                        if stock_code in content or (stock_name and stock_name in content):
                            related_news.append({
                                'title': news.get('title', ''),
                                'time': news.get('time', ''),
                                'summary': content[:100]
                            })
                        else:
                            general_news.append({
                                'title': news.get('title', ''),
                                'time': news.get('time', ''),
                            })

                    if related_news:
                        result['related_news'] = related_news[:5]
                    result['market_news'] = [n['title'] for n in general_news[:10]]
                    result['news_count'] = len(news_list)

        except Exception as e:
            self.logger.warning(f"获取新闻数据失败: {e}")

        self._set_cache(cache_key, result)
        return result

    # ============================================================
    # 综合数据收集
    # ============================================================
    def collect_comprehensive_data(self, stock_code: str, market_type: str = 'A') -> Dict:
        """
        收集全面的多维度数据，用于AI分析

        返回包含以下维度的完整数据字典：
        - technical: 技术面数据
        - fundamental: 基本面数据
        - capital_flow: 资金流数据
        - market_sentiment: 市场情绪数据
        - macro: 宏观环境数据
        - industry: 行业对比数据
        - news: 新闻舆情数据
        """
        self.logger.info(f"开始收集 {stock_code} 的全面数据...")
        start_time = time.time()

        data = {
            'stock_code': stock_code,
            'market_type': market_type,
            'collection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        # 1. 技术面数据（必须）
        data['technical'] = self.get_technical_data(stock_code, market_type)

        # 2. 基本面数据
        data['fundamental'] = self.get_fundamental_data(stock_code)

        # 3. 资金流数据
        data['capital_flow'] = self.get_capital_flow_data(stock_code)

        # 4. 市场情绪数据
        data['market_sentiment'] = self.get_market_sentiment_data()

        # 5. 宏观环境数据
        data['macro'] = self.get_macro_data()

        # 6. 行业对比数据
        data['industry'] = self.get_industry_data(stock_code)

        # 7. 新闻舆情数据
        stock_name = data['industry'].get('stock_name', '')
        data['news'] = self.get_news_data(stock_code, stock_name)

        data['collection_duration'] = round(time.time() - start_time, 2)
        self.logger.info(f"数据收集完成，耗时 {data['collection_duration']}秒")

        return data

    def format_data_for_ai(self, data: Dict) -> str:
        """
        将收集的数据格式化为AI可读的文本，用于注入prompt

        这是核心方法，将多维度数据转化为结构化的分析材料
        """
        sections = []
        stock_code = data.get('stock_code', 'N/A')

        # === 股票基本信息 ===
        industry = data.get('industry', {})
        sections.append(f"📊 股票: {industry.get('stock_name', 'N/A')} ({stock_code})")
        sections.append(f"   行业: {industry.get('industry', 'N/A')}")

        # === 技术面分析 ===
        tech = data.get('technical', {})
        if tech:
            sections.append("\n═══ 一、技术面数据 ═══")
            sections.append(f"  当前价格: {tech.get('current_price', 'N/A')}")
            sections.append(f"  今日涨跌: {tech.get('change_pct', 0):.2f}%")
            if tech.get('change_5d') is not None:
                sections.append(f"  近5日涨跌: {tech['change_5d']:.2f}%")
            if tech.get('change_20d') is not None:
                sections.append(f"  近20日涨跌: {tech['change_20d']:.2f}%")
            if tech.get('change_60d') is not None:
                sections.append(f"  近60日涨跌: {tech['change_60d']:.2f}%")
            if tech.get('trend'):
                sections.append(f"  均线趋势: {tech['trend']}")
            sections.append(f"  均线: MA5={tech.get('ma5', 'N/A')}, MA20={tech.get('ma20', 'N/A')}, MA60={tech.get('ma60', 'N/A')}")
            if tech.get('rsi'):
                sections.append(f"  RSI(14): {tech['rsi']:.1f} → {tech.get('rsi_signal', '')}")
            if tech.get('macd') is not None:
                macd_info = f"  MACD: {tech['macd']:.4f}"
                if tech.get('macd_cross'):
                    macd_info += f" → {tech['macd_cross']}"
                sections.append(macd_info)
            if tech.get('bollinger_signal'):
                sections.append(f"  布林带: {tech['bollinger_signal']} (位置{tech.get('bollinger_position', '')})")
            if tech.get('volume_ratio'):
                sections.append(f"  量比: {tech['volume_ratio']:.2f} → {tech.get('volume_signal', '')}")
            if tech.get('support_short'):
                sections.append(f"  短期支撑位: {tech['support_short']}")
            if tech.get('resistance_short'):
                sections.append(f"  短期压力位: {tech['resistance_short']}")

        # === 基本面分析 ===
        fund = data.get('fundamental', {})
        if fund:
            sections.append("\n═══ 二、基本面数据 ═══")
            if fund.get('pe_ttm') is not None:
                sections.append(f"  PE(TTM): {fund['pe_ttm']:.2f} → {fund.get('valuation_signal', '')}")
            if fund.get('pb') is not None:
                sections.append(f"  PB: {fund['pb']:.2f}")
            if fund.get('roe') is not None:
                sections.append(f"  ROE: {fund['roe']:.2f}% → {fund.get('profitability_signal', '')}")
            if fund.get('gross_margin') is not None:
                sections.append(f"  毛利率: {fund['gross_margin']:.2f}%")
            if fund.get('debt_ratio') is not None:
                sections.append(f"  资产负债率: {fund['debt_ratio']:.2f}% → {fund.get('financial_health', '')}")
            if fund.get('total_mv') is not None:
                sections.append(f"  总市值: {fund['total_mv']:.2f}亿 → {fund.get('market_cap_level', '')}")
            if fund.get('revenue_growth_3y') is not None:
                sections.append(f"  营收3年CAGR: {fund['revenue_growth_3y']:.2f}% → {fund.get('growth_signal', '')}")
            if fund.get('profit_growth_3y') is not None:
                sections.append(f"  净利润3年CAGR: {fund['profit_growth_3y']:.2f}%")

        # === 资金流分析 ===
        capital = data.get('capital_flow', {})
        if capital:
            sections.append("\n═══ 三、资金流向数据 ═══")
            if capital.get('capital_signal'):
                sections.append(f"  资金流向: {capital['capital_signal']}")
            if capital.get('main_net_inflow_total') is not None:
                sections.append(f"  近期主力净流入: {capital['main_net_inflow_total']:.2f}亿")
            if capital.get('capital_score') is not None:
                sections.append(f"  资金流评分: {capital['capital_score']}/100")
            if capital.get('north_money_today') is not None:
                sections.append(f"  今日北向资金: {capital['north_money_today']:.2f}亿 → {capital.get('north_signal', '')}")
            if capital.get('north_money_5d') is not None:
                sections.append(f"  近5日北向累计: {capital['north_money_5d']:.2f}亿")

        # === 市场情绪 ===
        sentiment = data.get('market_sentiment', {})
        if sentiment:
            sections.append("\n═══ 四、市场情绪数据 ═══")
            if sentiment.get('sentiment_signal'):
                sections.append(f"  情绪判断: {sentiment['sentiment_signal']}")
            if sentiment.get('emotion_phase'):
                sections.append(f"  BJCJ情绪阶段: 【{sentiment['emotion_phase']}】 建议仓位: {sentiment.get('suggested_position', '')}")
            if sentiment.get('zt_cnt') is not None:
                sections.append(f"  涨停: {sentiment['zt_cnt']}只, 跌停: {sentiment.get('dt_cnt', 0)}只, 封板率: {sentiment.get('fbl', 0):.0f}%")
            if sentiment.get('earn_rate') is not None:
                sections.append(f"  赚钱效应: {sentiment['earn_rate']:.0f}% (上涨{sentiment.get('up_cnt', 0)}/{sentiment.get('total_stocks', 0)})")
            if sentiment.get('total_amount') is not None:
                sections.append(f"  全市场成交额: {sentiment['total_amount']:.0f}亿")
            if sentiment.get('max_board'):
                sections.append(f"  最高连板: {sentiment['max_board']}板")
            if sentiment.get('ind_zt_top10'):
                top_sectors = [f"{s[0]}({s[1]})" for s in sentiment['ind_zt_top10'][:5]]
                sections.append(f"  涨停行业TOP5: {', '.join(top_sectors)}")

        # === 宏观环境 ===
        macro = data.get('macro', {})
        if macro:
            sections.append("\n═══ 五、宏观环境数据 ═══")
            if macro.get('market_amount_percentile') is not None:
                sections.append(f"  成交额分位: {macro['market_amount_percentile']:.1f}% → {macro.get('amount_signal', '')}")
            if macro.get('hs300_change_5d') is not None:
                sections.append(f"  沪深300近5日: {macro['hs300_change_5d']:.2f}%")
            if macro.get('hs300_change_20d') is not None:
                sections.append(f"  沪深300近20日: {macro['hs300_change_20d']:.2f}%")
            if macro.get('us_cn_spread') is not None:
                sections.append(f"  中美10Y利差: {macro['us_cn_spread']:.3f}% → {macro.get('spread_signal', '')}")
                sections.append(f"  (美国10Y: {macro.get('us_10y_rate', 'N/A')}%, 中国10Y: {macro.get('cn_10y_rate', 'N/A')}%)")

        # === 行业数据 ===
        if industry.get('hot_sectors'):
            sections.append("\n═══ 六、行业热点 ═══")
            sections.append(f"  今日资金流入热门板块: {', '.join(industry['hot_sectors'][:5])}")
            if industry.get('industry_net_flow') is not None:
                sections.append(f"  所属行业资金净流入: {industry['industry_net_flow']:.2f}亿")

        # === 新闻舆情 ===
        news = data.get('news', {})
        if news:
            sections.append("\n═══ 七、新闻舆情 ═══")
            if news.get('related_news'):
                sections.append("  【个股相关新闻】")
                for n in news['related_news'][:3]:
                    sections.append(f"  · {n.get('title', '')} ({n.get('time', '')})")
            if news.get('market_news'):
                sections.append("  【市场要闻】")
                for title in news['market_news'][:5]:
                    sections.append(f"  · {title}")

        return "\n".join(sections)

    # ============================================================
    # 辅助方法
    # ============================================================
    def _to_ts_code(self, stock_code: str) -> str:
        """将股票代码转换为Tushare格式"""
        code = stock_code.strip()
        if '.' in code:
            return code
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('8') or code.startswith('4'):
            return f"{code}.BJ"
        return f"{code}.SH"


# 全局单例
_collector_instance = None

def get_collector() -> EnhancedDataCollector:
    """获取全局数据收集器实例"""
    global _collector_instance
    if _collector_instance is None:
        _collector_instance = EnhancedDataCollector()
    return _collector_instance
