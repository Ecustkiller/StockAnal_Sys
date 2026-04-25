# -*- coding: utf-8 -*-
"""
多因子选股系统
==============
6大因子模型：价值 + 质量 + 动量 + 资金 + 技术 + 情绪
支持自定义因子权重和多种预设策略。

创建时间: 2026-04-25
"""

import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import traceback

logger = logging.getLogger(__name__)

# 预设策略模板
STRATEGY_TEMPLATES = {
    'balanced': {
        'name': '均衡策略',
        'description': '各因子均衡配置，适合大多数市场环境',
        'weights': {'value': 0.20, 'quality': 0.20, 'momentum': 0.15, 'capital': 0.20, 'technical': 0.15, 'sentiment': 0.10}
    },
    'value': {
        'name': '价值投资策略',
        'description': '侧重低估值+高质量，适合震荡市和熊市',
        'weights': {'value': 0.35, 'quality': 0.30, 'momentum': 0.05, 'capital': 0.10, 'technical': 0.10, 'sentiment': 0.10}
    },
    'growth': {
        'name': '成长动量策略',
        'description': '侧重高成长+强动量，适合牛市和结构性行情',
        'weights': {'value': 0.05, 'quality': 0.15, 'momentum': 0.30, 'capital': 0.25, 'technical': 0.20, 'sentiment': 0.05}
    },
    'momentum': {
        'name': '趋势跟踪策略',
        'description': '侧重技术面+资金面+动量，适合趋势行情',
        'weights': {'value': 0.05, 'quality': 0.05, 'momentum': 0.25, 'capital': 0.30, 'technical': 0.30, 'sentiment': 0.05}
    },
    'conservative': {
        'name': '保守防御策略',
        'description': '侧重质量+价值+低波动，适合弱势市场',
        'weights': {'value': 0.30, 'quality': 0.35, 'momentum': 0.05, 'capital': 0.10, 'technical': 0.10, 'sentiment': 0.10}
    },
    'hot_sector': {
        'name': '热点板块策略',
        'description': '侧重市场情绪+资金+动量，适合短线操作',
        'weights': {'value': 0.00, 'quality': 0.05, 'momentum': 0.25, 'capital': 0.30, 'technical': 0.15, 'sentiment': 0.25}
    }
}


class MultiFactorSelector:
    """多因子选股系统"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._tushare_api = None
        self._market_sentiment_cache = None
        self._market_sentiment_time = None

    def _get_tushare_api(self):
        """延迟初始化tushare API"""
        if self._tushare_api is None:
            try:
                import tushare as ts
                token = os.getenv('TUSHARE_TOKEN', '')
                if token:
                    self._tushare_api = ts.pro_api(token)
            except Exception as e:
                self.logger.warning(f"Tushare初始化失败: {e}")
        return self._tushare_api

    def select_stocks(self, strategy: str = 'balanced', custom_weights: Dict = None,
                      filters: Dict = None, top_n: int = 20) -> Dict:
        """
        执行多因子选股

        参数:
            strategy: 预设策略名称（balanced/value/growth/momentum/conservative/hot_sector）
            custom_weights: 自定义因子权重（覆盖预设策略）
            filters: 过滤条件 {'min_market_cap': 50, 'max_pe': 100, 'exclude_st': True, ...}
            top_n: 返回前N只股票

        返回:
            包含选股结果和因子详情的字典
        """
        start_time = datetime.now()

        # 确定因子权重
        if custom_weights:
            weights = custom_weights
            strategy_name = '自定义策略'
            strategy_desc = '用户自定义因子权重'
        elif strategy in STRATEGY_TEMPLATES:
            tmpl = STRATEGY_TEMPLATES[strategy]
            weights = tmpl['weights']
            strategy_name = tmpl['name']
            strategy_desc = tmpl['description']
        else:
            weights = STRATEGY_TEMPLATES['balanced']['weights']
            strategy_name = '均衡策略'
            strategy_desc = '默认均衡配置'

        # 归一化权重
        total_w = sum(weights.values())
        if total_w > 0:
            weights = {k: v / total_w for k, v in weights.items()}

        # 默认过滤条件
        if filters is None:
            filters = {}
        filters.setdefault('exclude_st', True)
        filters.setdefault('min_market_cap', 20)  # 最小市值20亿
        filters.setdefault('min_volume', 5000)  # 最小成交量5000手
        filters.setdefault('max_pe', 200)  # 最大PE 200

        self.logger.info(f"开始多因子选股: 策略={strategy_name}, 权重={weights}")

        # 1. 获取股票池
        stock_pool = self._get_stock_pool(filters)
        if not stock_pool:
            return {'error': '无法获取股票池', 'stocks': []}

        self.logger.info(f"股票池大小: {len(stock_pool)}")

        # 2. 获取市场情绪数据（全局，只获取一次）
        market_sentiment = self._get_market_sentiment()

        # 3. 计算每只股票的因子得分
        scored_stocks = []
        for i, stock in enumerate(stock_pool):
            if i % 50 == 0:
                self.logger.info(f"因子计算进度: {i}/{len(stock_pool)}")
            try:
                factor_scores = self._calculate_all_factors(stock, market_sentiment)
                if factor_scores:
                    # 加权总分
                    total_score = sum(
                        factor_scores.get(f, {}).get('score', 50) * weights.get(f, 0)
                        for f in weights.keys()
                    )
                    stock['factor_scores'] = factor_scores
                    stock['total_score'] = round(total_score, 2)
                    scored_stocks.append(stock)
            except Exception as e:
                self.logger.debug(f"计算 {stock.get('code', '?')} 因子得分失败: {e}")

        # 4. 排序并取前N
        scored_stocks.sort(key=lambda x: x['total_score'], reverse=True)
        top_stocks = scored_stocks[:top_n]

        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"多因子选股完成: 共{len(scored_stocks)}只评分, 耗时{elapsed:.1f}秒")

        return {
            'strategy': {
                'name': strategy_name,
                'description': strategy_desc,
                'weights': weights
            },
            'filters': filters,
            'total_screened': len(stock_pool),
            'total_scored': len(scored_stocks),
            'top_n': top_n,
            'elapsed_seconds': round(elapsed, 1),
            'stocks': [self._format_stock_result(s) for s in top_stocks],
            'available_strategies': list(STRATEGY_TEMPLATES.keys())
        }

    def _get_stock_pool(self, filters: Dict) -> List[Dict]:
        """获取股票池 - 从tushare获取基础数据"""
        try:
            api = self._get_tushare_api()
            if not api:
                return self._get_stock_pool_akshare(filters)

            # 获取A股日线基础数据
            today = datetime.now().strftime('%Y%m%d')
            df = api.daily_basic(
                ts_code='',
                trade_date=today,
                fields='ts_code,close,turnover_rate,volume_ratio,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv'
            )

            if df is None or df.empty:
                # 尝试前一个交易日
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
                df = api.daily_basic(ts_code='', trade_date=yesterday,
                                     fields='ts_code,close,turnover_rate,volume_ratio,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv')

            if df is None or df.empty:
                return self._get_stock_pool_akshare(filters)

            stocks = []
            for _, row in df.iterrows():
                code = row['ts_code']
                # 过滤ST
                if filters.get('exclude_st') and ('ST' in str(code)):
                    continue
                # 过滤市值
                mv = row.get('total_mv', 0)
                if mv and mv > 0:
                    mv_yi = mv / 10000  # 转为亿
                    if mv_yi < filters.get('min_market_cap', 0):
                        continue
                # 过滤PE
                pe = row.get('pe_ttm')
                if pe and pe > 0 and pe > filters.get('max_pe', 9999):
                    continue

                # 转换代码格式 000001.SZ -> 000001
                stock_code = code.split('.')[0]

                stocks.append({
                    'code': stock_code,
                    'ts_code': code,
                    'close': float(row.get('close', 0) or 0),
                    'pe_ttm': float(row.get('pe_ttm', 0) or 0),
                    'pb': float(row.get('pb', 0) or 0),
                    'ps_ttm': float(row.get('ps_ttm', 0) or 0),
                    'dv_ratio': float(row.get('dv_ratio', 0) or 0),
                    'total_mv': float(row.get('total_mv', 0) or 0) / 10000,  # 亿
                    'circ_mv': float(row.get('circ_mv', 0) or 0) / 10000,
                    'turnover_rate': float(row.get('turnover_rate', 0) or 0),
                    'volume_ratio': float(row.get('volume_ratio', 0) or 0),
                })

            return stocks

        except Exception as e:
            self.logger.warning(f"Tushare获取股票池失败: {e}")
            return self._get_stock_pool_akshare(filters)

    def _get_stock_pool_akshare(self, filters: Dict) -> List[Dict]:
        """备用方案：从akshare获取股票池"""
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                return []

            stocks = []
            for _, row in df.iterrows():
                name = str(row.get('名称', ''))
                code = str(row.get('代码', ''))

                # 过滤ST
                if filters.get('exclude_st') and ('ST' in name or 'st' in name):
                    continue

                # 过滤市值
                mv = float(row.get('总市值', 0) or 0) / 1e8  # 转为亿
                if mv < filters.get('min_market_cap', 0):
                    continue

                pe = float(row.get('市盈率-动态', 0) or 0)
                if pe > 0 and pe > filters.get('max_pe', 9999):
                    continue

                close = float(row.get('最新价', 0) or 0)
                if close <= 0:
                    continue

                stocks.append({
                    'code': code,
                    'name': name,
                    'close': close,
                    'pe_ttm': pe,
                    'pb': float(row.get('市净率', 0) or 0),
                    'ps_ttm': 0,
                    'dv_ratio': 0,
                    'total_mv': mv,
                    'circ_mv': float(row.get('流通市值', 0) or 0) / 1e8,
                    'turnover_rate': float(row.get('换手率', 0) or 0),
                    'volume_ratio': float(row.get('量比', 0) or 0),
                    'change_pct': float(row.get('涨跌幅', 0) or 0),
                    'change_5d': float(row.get('5日涨跌幅', 0) or 0) if '5日涨跌幅' in row else None,
                    'change_20d': float(row.get('20日涨跌幅', 0) or 0) if '20日涨跌幅' in row else None,
                    'volume': float(row.get('成交量', 0) or 0),
                    'amount': float(row.get('成交额', 0) or 0),
                })

            return stocks

        except Exception as e:
            self.logger.error(f"AKShare获取股票池失败: {e}")
            return []

    def _get_market_sentiment(self) -> Dict:
        """获取市场情绪数据（缓存30分钟）"""
        now = datetime.now()
        if (self._market_sentiment_cache and self._market_sentiment_time and
                (now - self._market_sentiment_time).total_seconds() < 1800):
            return self._market_sentiment_cache

        sentiment = {}
        try:
            from enhanced_data_collector import get_collector, ENHANCED_COLLECTOR_AVAILABLE
            if ENHANCED_COLLECTOR_AVAILABLE:
                collector = get_collector()
                sentiment = collector.get_market_sentiment_data() or {}
        except Exception as e:
            self.logger.warning(f"获取市场情绪失败: {e}")

        self._market_sentiment_cache = sentiment
        self._market_sentiment_time = now
        return sentiment

    def _calculate_all_factors(self, stock: Dict, market_sentiment: Dict) -> Dict:
        """计算一只股票的所有因子得分"""
        scores = {}

        # 因子1：价值因子
        scores['value'] = self._calc_value_factor(stock)

        # 因子2：质量因子
        scores['quality'] = self._calc_quality_factor(stock)

        # 因子3：动量因子
        scores['momentum'] = self._calc_momentum_factor(stock)

        # 因子4：资金因子
        scores['capital'] = self._calc_capital_factor(stock)

        # 因子5：技术因子
        scores['technical'] = self._calc_technical_factor(stock)

        # 因子6：情绪因子
        scores['sentiment'] = self._calc_sentiment_factor(stock, market_sentiment)

        return scores

    def _calc_value_factor(self, stock: Dict) -> Dict:
        """
        价值因子（满分100）
        - PE估值（40分）
        - PB估值（30分）
        - 股息率（30分）
        """
        score = 0
        details = []

        # PE估值
        pe = stock.get('pe_ttm', 0)
        if pe and pe > 0:
            if pe < 10:
                pe_score = 40
                details.append(f'PE={pe:.1f}，极度低估')
            elif pe < 15:
                pe_score = 35
                details.append(f'PE={pe:.1f}，低估')
            elif pe < 25:
                pe_score = 25
                details.append(f'PE={pe:.1f}，合理')
            elif pe < 40:
                pe_score = 15
                details.append(f'PE={pe:.1f}，偏高')
            elif pe < 60:
                pe_score = 5
                details.append(f'PE={pe:.1f}，高估')
            else:
                pe_score = 0
                details.append(f'PE={pe:.1f}，严重高估')
            score += pe_score
        else:
            score += 10  # 亏损股给中性分
            details.append('PE为负(亏损)')

        # PB估值
        pb = stock.get('pb', 0)
        if pb and pb > 0:
            if pb < 1:
                pb_score = 30
                details.append(f'PB={pb:.2f}，破净')
            elif pb < 2:
                pb_score = 25
                details.append(f'PB={pb:.2f}，低估')
            elif pb < 4:
                pb_score = 15
                details.append(f'PB={pb:.2f}，合理')
            else:
                pb_score = 5
                details.append(f'PB={pb:.2f}，偏高')
            score += pb_score
        else:
            score += 10

        # 股息率
        dv = stock.get('dv_ratio', 0)
        if dv and dv > 0:
            if dv > 5:
                dv_score = 30
                details.append(f'股息率={dv:.2f}%，高分红')
            elif dv > 3:
                dv_score = 25
                details.append(f'股息率={dv:.2f}%，较好')
            elif dv > 1:
                dv_score = 15
            else:
                dv_score = 5
            score += dv_score
        else:
            score += 5

        return {'score': min(100, score), 'details': details}

    def _calc_quality_factor(self, stock: Dict) -> Dict:
        """
        质量因子（满分100）
        通过tushare获取ROE、负债率、毛利率等
        """
        score = 50  # 默认中性分
        details = []

        try:
            api = self._get_tushare_api()
            if api and stock.get('ts_code'):
                # 获取财务指标
                fina = api.fina_indicator(ts_code=stock['ts_code'], limit=1,
                                          fields='ts_code,roe,debt_to_assets,grossprofit_margin,netprofit_yoy')
                if fina is not None and not fina.empty:
                    row = fina.iloc[0]

                    score = 0
                    # ROE（40分）
                    roe = row.get('roe')
                    if roe is not None and not pd.isna(roe):
                        roe = float(roe)
                        if roe > 20:
                            score += 40
                            details.append(f'ROE={roe:.1f}%，优秀')
                        elif roe > 15:
                            score += 35
                            details.append(f'ROE={roe:.1f}%，良好')
                        elif roe > 10:
                            score += 25
                        elif roe > 5:
                            score += 15
                        elif roe > 0:
                            score += 5
                        else:
                            score += 0
                            details.append(f'ROE={roe:.1f}%，亏损')
                        stock['roe'] = roe

                    # 负债率（30分）
                    debt = row.get('debt_to_assets')
                    if debt is not None and not pd.isna(debt):
                        debt = float(debt)
                        if debt < 30:
                            score += 30
                            details.append(f'负债率={debt:.0f}%，健康')
                        elif debt < 50:
                            score += 20
                        elif debt < 70:
                            score += 10
                        else:
                            score += 0
                            details.append(f'负债率={debt:.0f}%，偏高')
                        stock['debt_ratio'] = debt

                    # 净利润增速（30分）
                    np_yoy = row.get('netprofit_yoy')
                    if np_yoy is not None and not pd.isna(np_yoy):
                        np_yoy = float(np_yoy)
                        if np_yoy > 50:
                            score += 30
                            details.append(f'净利润增速={np_yoy:.0f}%，高增长')
                        elif np_yoy > 20:
                            score += 25
                        elif np_yoy > 0:
                            score += 15
                        elif np_yoy > -20:
                            score += 5
                        else:
                            score += 0
                            details.append(f'净利润增速={np_yoy:.0f}%，下滑')
                        stock['np_yoy'] = np_yoy

        except Exception as e:
            self.logger.debug(f"质量因子计算失败: {e}")

        return {'score': min(100, score), 'details': details}

    def _calc_momentum_factor(self, stock: Dict) -> Dict:
        """
        动量因子（满分100）
        - 近期涨幅（50分）
        - 量比/换手率（30分）
        - 价格位置（20分）
        """
        score = 0
        details = []

        # 近期涨幅
        change = stock.get('change_pct', 0)
        change_5d = stock.get('change_5d')
        change_20d = stock.get('change_20d')

        # 今日涨幅（20分）
        if change:
            if 2 <= change <= 7:
                score += 20
                details.append(f'今日+{change:.1f}%，强势')
            elif 0 < change < 2:
                score += 12
            elif -2 <= change <= 0:
                score += 8
            elif change > 7:
                score += 10  # 涨太多有追高风险
                details.append(f'今日+{change:.1f}%，注意追高')
            else:
                score += 0

        # 5日涨幅（15分）
        if change_5d is not None:
            if 3 <= change_5d <= 15:
                score += 15
                details.append(f'5日+{change_5d:.1f}%')
            elif 0 < change_5d < 3:
                score += 10
            elif change_5d > 15:
                score += 5  # 短期涨幅过大
            else:
                score += 0

        # 20日涨幅（15分）
        if change_20d is not None:
            if 5 <= change_20d <= 30:
                score += 15
            elif 0 < change_20d < 5:
                score += 10
            elif change_20d > 30:
                score += 3
            else:
                score += 0

        # 量比（30分）
        vol_ratio = stock.get('volume_ratio', 1)
        if vol_ratio:
            if 1.5 <= vol_ratio <= 3:
                score += 30
                details.append(f'量比={vol_ratio:.2f}，温和放量')
            elif 1.2 <= vol_ratio < 1.5:
                score += 20
            elif 3 < vol_ratio <= 5:
                score += 15
                details.append(f'量比={vol_ratio:.2f}，大幅放量')
            elif vol_ratio > 5:
                score += 5  # 异常放量
            else:
                score += 10

        # 换手率（20分）- 适度换手最佳
        turnover = stock.get('turnover_rate', 0)
        if turnover:
            if 3 <= turnover <= 8:
                score += 20
            elif 1 <= turnover < 3:
                score += 12
            elif 8 < turnover <= 15:
                score += 10
            else:
                score += 5

        return {'score': min(100, score), 'details': details}

    def _calc_capital_factor(self, stock: Dict) -> Dict:
        """
        资金因子（满分100）
        通过enhanced_data_collector获取主力资金和北向资金数据
        """
        score = 50  # 默认中性
        details = []

        try:
            from enhanced_data_collector import get_collector, ENHANCED_COLLECTOR_AVAILABLE
            if ENHANCED_COLLECTOR_AVAILABLE:
                collector = get_collector()
                cap_data = collector.get_capital_flow_data(stock['code'])
                if cap_data:
                    score = 0

                    # 主力资金方向（50分）
                    pos_days = cap_data.get('positive_days', 0)
                    neg_days = cap_data.get('negative_days', 0)
                    total_flow = cap_data.get('main_net_inflow_total', 0)

                    if pos_days >= 7 and total_flow > 0:
                        score += 50
                        details.append(f'主力连续{pos_days}天净流入')
                    elif pos_days > neg_days and total_flow > 0:
                        score += 35
                        details.append('主力资金偏向流入')
                    elif pos_days == neg_days:
                        score += 20
                    elif neg_days >= 7:
                        score += 0
                        details.append(f'主力连续{neg_days}天净流出')
                    else:
                        score += 10

                    # 北向资金（30分）
                    north_5d = cap_data.get('north_money_5d', 0)
                    if north_5d is not None:
                        if north_5d > 100:
                            score += 30
                            details.append(f'北向5日净买入{north_5d:.0f}亿')
                        elif north_5d > 0:
                            score += 20
                        elif north_5d > -100:
                            score += 10
                        else:
                            score += 0

                    # 资金流评分（20分）
                    cap_score = cap_data.get('capital_score', 50)
                    score += int(cap_score / 100 * 20)

                    stock['capital_data'] = cap_data
        except Exception as e:
            self.logger.debug(f"资金因子计算失败: {e}")

        return {'score': min(100, score), 'details': details}

    def _calc_technical_factor(self, stock: Dict) -> Dict:
        """
        技术因子（满分100）
        - 均线排列（40分）
        - MACD信号（30分）
        - RSI位置（30分）
        """
        score = 50  # 默认中性
        details = []

        try:
            # 尝试获取技术指标数据
            api = self._get_tushare_api()
            if api and stock.get('ts_code'):
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')

                df = api.daily(ts_code=stock['ts_code'], start_date=start_date, end_date=end_date)
                if df is not None and len(df) >= 60:
                    df = df.sort_values('trade_date').reset_index(drop=True)
                    close = df['close']

                    score = 0

                    # 均线排列（40分）
                    ma5 = close.rolling(5).mean().iloc[-1]
                    ma20 = close.rolling(20).mean().iloc[-1]
                    ma60 = close.rolling(60).mean().iloc[-1]
                    current = close.iloc[-1]

                    if ma5 > ma20 > ma60:
                        score += 40
                        details.append('完美多头排列')
                    elif ma5 > ma20:
                        score += 25
                        details.append('短期多头')
                    elif current > ma20:
                        score += 15
                        details.append('站上20日均线')
                    elif ma5 < ma20 < ma60:
                        score += 0
                        details.append('空头排列')
                    else:
                        score += 10

                    # MACD信号（30分）
                    ema12 = close.ewm(span=12).mean()
                    ema26 = close.ewm(span=26).mean()
                    dif = ema12 - ema26
                    dea = dif.ewm(span=9).mean()
                    macd_hist = (dif - dea) * 2

                    if dif.iloc[-1] > dea.iloc[-1] and macd_hist.iloc[-1] > 0:
                        score += 30
                        details.append('MACD金叉+红柱')
                    elif dif.iloc[-1] > dea.iloc[-1]:
                        score += 20
                        details.append('MACD金叉')
                    elif macd_hist.iloc[-1] > macd_hist.iloc[-2]:
                        score += 10
                        details.append('MACD柱状图收窄')
                    else:
                        score += 0

                    # RSI（30分）
                    delta = close.diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss.replace(0, np.nan)
                    rsi = 100 - (100 / (1 + rs))
                    rsi_val = rsi.iloc[-1]

                    if 30 <= rsi_val <= 50:
                        score += 30
                        details.append(f'RSI={rsi_val:.0f}，超卖反弹区')
                    elif 50 < rsi_val <= 65:
                        score += 25
                        details.append(f'RSI={rsi_val:.0f}，强势区')
                    elif 65 < rsi_val <= 75:
                        score += 15
                    elif rsi_val < 30:
                        score += 20
                        details.append(f'RSI={rsi_val:.0f}，深度超卖')
                    elif rsi_val > 75:
                        score += 5
                        details.append(f'RSI={rsi_val:.0f}，超买')
                    else:
                        score += 10

                    stock['ma5'] = ma5
                    stock['ma20'] = ma20
                    stock['ma60'] = ma60
                    stock['rsi'] = rsi_val

        except Exception as e:
            self.logger.debug(f"技术因子计算失败: {e}")

        return {'score': min(100, score), 'details': details}

    def _calc_sentiment_factor(self, stock: Dict, market_sentiment: Dict) -> Dict:
        """
        情绪因子（满分100）
        基于全市场情绪数据
        """
        score = 50  # 默认中性
        details = []

        if not market_sentiment:
            return {'score': score, 'details': ['市场情绪数据不可用']}

        score = 0

        # 封板率（25分）
        fbl = market_sentiment.get('fbl', 50)
        if fbl is not None:
            if fbl > 60:
                score += 25
                details.append(f'封板率{fbl:.0f}%，市场活跃')
            elif fbl > 40:
                score += 15
            elif fbl > 20:
                score += 8
            else:
                score += 0
                details.append(f'封板率{fbl:.0f}%，市场冷淡')

        # 赚钱效应（25分）
        earn = market_sentiment.get('earn_rate', 50)
        if earn is not None:
            if earn > 60:
                score += 25
                details.append(f'赚钱效应{earn:.0f}%')
            elif earn > 45:
                score += 15
            elif earn > 30:
                score += 8
            else:
                score += 0

        # BJCJ情绪阶段（30分）
        phase = market_sentiment.get('emotion_phase', '')
        if phase:
            phase_scores = {
                '重仓期': 30, '加仓期': 25, '轻仓期': 15,
                '观望期': 10, '防御期': 5, '空仓期': 0
            }
            ps = phase_scores.get(phase, 10)
            score += ps
            details.append(f'BJCJ阶段: {phase}')

        # 涨跌停比（20分）
        zt = market_sentiment.get('zt_cnt', 0)
        dt = market_sentiment.get('dt_cnt', 0)
        if zt is not None and dt is not None:
            if zt > 0 and dt >= 0:
                ratio = zt / max(dt, 1)
                if ratio > 3:
                    score += 20
                    details.append(f'涨停{zt}只/跌停{dt}只')
                elif ratio > 1.5:
                    score += 12
                elif ratio > 0.5:
                    score += 5
                else:
                    score += 0

        return {'score': min(100, score), 'details': details}

    def _format_stock_result(self, stock: Dict) -> Dict:
        """格式化单只股票的选股结果"""
        factor_scores = stock.get('factor_scores', {})

        # 找出最强和最弱因子
        factor_names = {'value': '价值', 'quality': '质量', 'momentum': '动量',
                        'capital': '资金', 'technical': '技术', 'sentiment': '情绪'}
        sorted_factors = sorted(factor_scores.items(), key=lambda x: x[1].get('score', 0), reverse=True)
        strengths = [factor_names.get(f[0], f[0]) for f in sorted_factors[:2] if f[1].get('score', 0) >= 60]
        weaknesses = [factor_names.get(f[0], f[0]) for f in sorted_factors[-2:] if f[1].get('score', 0) < 40]

        # 收集所有亮点
        all_details = []
        for f_name, f_data in factor_scores.items():
            all_details.extend(f_data.get('details', []))

        return {
            'code': stock.get('code', ''),
            'name': stock.get('name', ''),
            'close': stock.get('close', 0),
            'total_score': stock.get('total_score', 0),
            'pe_ttm': stock.get('pe_ttm', 0),
            'pb': stock.get('pb', 0),
            'roe': stock.get('roe'),
            'total_mv': stock.get('total_mv', 0),
            'turnover_rate': stock.get('turnover_rate', 0),
            'factor_scores': {
                k: {'score': v.get('score', 50), 'details': v.get('details', [])}
                for k, v in factor_scores.items()
            },
            'strengths': strengths,
            'weaknesses': weaknesses,
            'highlights': all_details[:5],  # 最多5条亮点
        }

    def get_strategy_templates(self) -> Dict:
        """获取所有预设策略模板"""
        return STRATEGY_TEMPLATES
