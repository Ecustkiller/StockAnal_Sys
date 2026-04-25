# -*- coding: utf-8 -*-
"""
结构化分析报告引擎
==================
基于数据直接生成高质量结构化分析报告，不依赖AI。
速度快（<1秒）、质量稳定、数据驱动。

创建时间: 2026-04-25
"""

import logging
from datetime import datetime
from typing import Dict, Optional


class StructuredReportGenerator:
    """结构化分析报告生成器 - 纯代码驱动，不依赖AI"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def generate_report(self, score_details: Dict, enhanced_data: Dict = None,
                        stock_info: Dict = None, price_data: Dict = None,
                        technical_analysis: Dict = None) -> Dict:
        """
        生成完整的结构化分析报告

        参数:
            score_details: 8维度评分详情（来自calculate_score）
            enhanced_data: 增强版多维度数据（来自EnhancedDataCollector）
            stock_info: 股票基本信息
            price_data: 价格数据
            technical_analysis: 技术分析数据

        返回:
            包含各维度诊断和操作建议的完整报告
        """
        if not score_details:
            return {'error': '评分数据不可用'}

        report = {
            'report_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_code': stock_info.get('stock_code', '') if stock_info else '',
            'stock_name': stock_info.get('股票名称', '未知') if stock_info else '未知',
            'industry': stock_info.get('行业', '未知') if stock_info else '未知',
        }

        # 1. 综合评级
        report['overall_rating'] = self._generate_overall_rating(score_details)

        # 2. 技术面诊断
        report['technical_diagnosis'] = self._generate_technical_diagnosis(
            score_details, enhanced_data, technical_analysis)

        # 3. 基本面诊断
        report['fundamental_diagnosis'] = self._generate_fundamental_diagnosis(
            score_details, enhanced_data)

        # 4. 资金面诊断
        report['capital_flow_diagnosis'] = self._generate_capital_flow_diagnosis(
            score_details, enhanced_data)

        # 5. 市场环境诊断
        report['market_environment'] = self._generate_market_environment(
            score_details, enhanced_data)

        # 6. 风险评估
        report['risk_assessment'] = self._generate_risk_assessment(
            score_details, enhanced_data)

        # 7. 操作建议
        report['trading_advice'] = self._generate_trading_advice(
            score_details, enhanced_data, price_data)

        # 8. 生成文本摘要
        report['text_summary'] = self._generate_text_summary(report)

        return report

    def _generate_overall_rating(self, score_details: Dict) -> Dict:
        """生成综合评级"""
        total = score_details.get('total', 50)
        max_scores = score_details.get('max_scores', {})

        # 评级
        if total >= 80:
            rating = 'A+'
            rating_text = '强烈推荐'
            rating_color = '#e74c3c'
        elif total >= 70:
            rating = 'A'
            rating_text = '推荐买入'
            rating_color = '#e67e22'
        elif total >= 60:
            rating = 'B+'
            rating_text = '谨慎买入'
            rating_color = '#f39c12'
        elif total >= 50:
            rating = 'B'
            rating_text = '中性观望'
            rating_color = '#3498db'
        elif total >= 40:
            rating = 'C+'
            rating_text = '谨慎持有'
            rating_color = '#9b59b6'
        elif total >= 30:
            rating = 'C'
            rating_text = '建议减仓'
            rating_color = '#8e44ad'
        else:
            rating = 'D'
            rating_text = '建议卖出'
            rating_color = '#2c3e50'

        # 各维度得分率
        dimension_rates = {}
        for dim, max_val in max_scores.items():
            actual = score_details.get(dim, 0)
            rate = round(actual / max_val * 100, 1) if max_val > 0 else 0
            dimension_rates[dim] = {
                'score': actual,
                'max': max_val,
                'rate': rate,
                'level': '优' if rate >= 80 else '良' if rate >= 60 else '中' if rate >= 40 else '差'
            }

        # 找出优势和劣势维度
        sorted_dims = sorted(dimension_rates.items(), key=lambda x: x[1]['rate'], reverse=True)
        strengths = [d[0] for d in sorted_dims[:3] if d[1]['rate'] >= 60]
        weaknesses = [d[0] for d in sorted_dims[-3:] if d[1]['rate'] < 40]

        dim_names = {
            'trend': '趋势', 'technical': '技术指标', 'volume': '成交量',
            'momentum': '动量', 'fundamental': '基本面', 'capital_flow': '资金流',
            'sentiment': '市场情绪', 'industry': '行业强度'
        }

        return {
            'total_score': total,
            'rating': rating,
            'rating_text': rating_text,
            'rating_color': rating_color,
            'dimension_rates': dimension_rates,
            'strengths': [dim_names.get(s, s) for s in strengths],
            'weaknesses': [dim_names.get(w, w) for w in weaknesses],
        }

    def _generate_technical_diagnosis(self, score_details: Dict,
                                       enhanced_data: Dict = None,
                                       technical_analysis: Dict = None) -> Dict:
        """生成技术面诊断"""
        tech = enhanced_data.get('technical', {}) if enhanced_data else {}
        diagnosis_points = []
        signals = []

        # 趋势诊断
        trend = tech.get('trend', '')
        if trend:
            diagnosis_points.append(f"均线系统: {trend}")
            if '多头' in trend:
                signals.append({'type': 'bullish', 'text': '均线多头排列，趋势向上'})
            elif '空头' in trend:
                signals.append({'type': 'bearish', 'text': '均线空头排列，趋势向下'})

        # RSI诊断
        rsi = tech.get('rsi')
        rsi_signal = tech.get('rsi_signal', '')
        if rsi:
            diagnosis_points.append(f"RSI(14): {rsi:.1f} ({rsi_signal})")
            if rsi < 30:
                signals.append({'type': 'bullish', 'text': f'RSI={rsi:.1f}处于超卖区域，存在反弹机会'})
            elif rsi > 70:
                signals.append({'type': 'bearish', 'text': f'RSI={rsi:.1f}处于超买区域，注意回调风险'})

        # MACD诊断
        macd_cross = tech.get('macd_cross', '')
        if macd_cross:
            diagnosis_points.append(f"MACD: {macd_cross}")
            if '金叉' in macd_cross:
                signals.append({'type': 'bullish', 'text': macd_cross})
            elif '死叉' in macd_cross:
                signals.append({'type': 'bearish', 'text': macd_cross})

        # 布林带诊断
        bb_signal = tech.get('bollinger_signal', '')
        if bb_signal:
            diagnosis_points.append(f"布林带: {bb_signal}")
            if '超卖' in bb_signal or '下轨' in bb_signal:
                signals.append({'type': 'bullish', 'text': f'布林带{bb_signal}，可能存在反弹机会'})
            elif '超买' in bb_signal or '上轨' in bb_signal:
                signals.append({'type': 'bearish', 'text': f'布林带{bb_signal}，注意回调风险'})

        # 量能诊断
        vol_signal = tech.get('volume_signal', '')
        vol_ratio = tech.get('volume_ratio', 1)
        if vol_signal:
            diagnosis_points.append(f"量比: {vol_ratio:.2f} ({vol_signal})")

        # 近期涨跌
        change_5d = tech.get('change_5d')
        change_20d = tech.get('change_20d')
        if change_5d is not None:
            diagnosis_points.append(f"近5日涨跌: {change_5d:+.2f}%")
        if change_20d is not None:
            diagnosis_points.append(f"近20日涨跌: {change_20d:+.2f}%")

        # 支撑压力位
        support = tech.get('support_short', [])
        resistance = tech.get('resistance_short', [])
        if support:
            diagnosis_points.append(f"短期支撑位: {support}")
        if resistance:
            diagnosis_points.append(f"短期压力位: {resistance}")

        # 技术面综合判断
        trend_score = score_details.get('trend', 0)
        tech_score = score_details.get('technical', 0)
        max_trend = score_details.get('max_scores', {}).get('trend', 15)
        max_tech = score_details.get('max_scores', {}).get('technical', 15)
        combined_rate = (trend_score + tech_score) / (max_trend + max_tech) * 100 if (max_trend + max_tech) > 0 else 50

        if combined_rate >= 70:
            conclusion = '技术面整体偏强，多头信号明显'
        elif combined_rate >= 50:
            conclusion = '技术面中性偏强，关注关键位置突破'
        elif combined_rate >= 30:
            conclusion = '技术面偏弱，建议等待企稳信号'
        else:
            conclusion = '技术面明显偏弱，空头趋势明显'

        return {
            'diagnosis_points': diagnosis_points,
            'signals': signals,
            'conclusion': conclusion,
            'score_rate': round(combined_rate, 1)
        }

    def _generate_fundamental_diagnosis(self, score_details: Dict,
                                         enhanced_data: Dict = None) -> Dict:
        """生成基本面诊断"""
        fund = enhanced_data.get('fundamental', {}) if enhanced_data else {}
        diagnosis_points = []
        highlights = []
        risks = []

        # 估值诊断
        pe = fund.get('pe_ttm')
        pb = fund.get('pb')
        valuation_signal = fund.get('valuation_signal', '')
        if pe is not None:
            diagnosis_points.append(f"PE(TTM): {pe:.2f} → {valuation_signal}")
            if pe < 15:
                highlights.append('估值处于低位，具有安全边际')
            elif pe > 50:
                risks.append('估值偏高，需要高增长支撑')
        if pb is not None:
            diagnosis_points.append(f"PB: {pb:.2f}")
            if pb < 1:
                highlights.append('破净股，可能存在价值低估')

        # 盈利能力诊断
        roe = fund.get('roe')
        profitability = fund.get('profitability_signal', '')
        if roe is not None:
            diagnosis_points.append(f"ROE: {roe:.2f}% → {profitability}")
            if roe > 20:
                highlights.append(f'ROE={roe:.1f}%，盈利能力优秀')
            elif roe < 5:
                risks.append(f'ROE={roe:.1f}%，盈利能力较弱')

        # 毛利率
        gm = fund.get('gross_margin')
        if gm is not None:
            diagnosis_points.append(f"毛利率: {gm:.2f}%")
            if gm > 50:
                highlights.append('高毛利率，具有较强定价权')

        # 财务健康
        debt = fund.get('debt_ratio')
        health = fund.get('financial_health', '')
        if debt is not None:
            diagnosis_points.append(f"资产负债率: {debt:.2f}% → {health}")
            if debt > 70:
                risks.append(f'负债率{debt:.0f}%偏高，财务风险需关注')

        # 成长性
        rev_g = fund.get('revenue_growth_3y')
        growth_signal = fund.get('growth_signal', '')
        if rev_g is not None:
            diagnosis_points.append(f"营收3年CAGR: {rev_g:.2f}% → {growth_signal}")
            if rev_g > 30:
                highlights.append(f'营收高速增长({rev_g:.0f}%CAGR)')
            elif rev_g < 0:
                risks.append('营收持续萎缩')

        # 市值
        mv = fund.get('total_mv')
        cap_level = fund.get('market_cap_level', '')
        if mv is not None:
            diagnosis_points.append(f"总市值: {mv:.2f}亿 → {cap_level}")

        # 基本面综合判断
        fund_score = score_details.get('fundamental', 10)
        max_fund = score_details.get('max_scores', {}).get('fundamental', 20)
        fund_rate = fund_score / max_fund * 100 if max_fund > 0 else 50

        if fund_rate >= 70:
            conclusion = '基本面优秀，估值合理、盈利能力强、成长性好'
        elif fund_rate >= 50:
            conclusion = '基本面良好，整体财务状况健康'
        elif fund_rate >= 30:
            conclusion = '基本面一般，部分指标需要关注'
        else:
            conclusion = '基本面偏弱，存在财务或估值风险'

        return {
            'diagnosis_points': diagnosis_points,
            'highlights': highlights,
            'risks': risks,
            'conclusion': conclusion,
            'score_rate': round(fund_rate, 1),
            'data_available': bool(fund)
        }

    def _generate_capital_flow_diagnosis(self, score_details: Dict,
                                          enhanced_data: Dict = None) -> Dict:
        """生成资金面诊断"""
        capital = enhanced_data.get('capital_flow', {}) if enhanced_data else {}
        diagnosis_points = []
        signals = []

        # 主力资金
        capital_signal = capital.get('capital_signal', '')
        if capital_signal:
            diagnosis_points.append(f"主力资金: {capital_signal}")
            if '流入' in capital_signal:
                signals.append({'type': 'bullish', 'text': capital_signal})
            elif '流出' in capital_signal:
                signals.append({'type': 'bearish', 'text': capital_signal})

        total_flow = capital.get('main_net_inflow_total')
        if total_flow is not None:
            diagnosis_points.append(f"近期主力净流入: {total_flow:.2f}亿")

        pos_days = capital.get('positive_days', 0)
        neg_days = capital.get('negative_days', 0)
        if pos_days or neg_days:
            diagnosis_points.append(f"净流入天数: {pos_days}天 / 净流出天数: {neg_days}天")

        # 北向资金
        north_signal = capital.get('north_signal', '')
        north_today = capital.get('north_money_today')
        north_5d = capital.get('north_money_5d')
        if north_signal:
            diagnosis_points.append(f"北向资金: {north_signal}")
            if '流入' in north_signal:
                signals.append({'type': 'bullish', 'text': f'北向资金{north_signal}'})
            elif '流出' in north_signal:
                signals.append({'type': 'bearish', 'text': f'北向资金{north_signal}'})
        if north_today is not None:
            diagnosis_points.append(f"今日北向: {north_today:.2f}亿")
        if north_5d is not None:
            diagnosis_points.append(f"近5日北向累计: {north_5d:.2f}亿")

        # 资金流评分
        cap_score_val = capital.get('capital_score')
        if cap_score_val is not None:
            diagnosis_points.append(f"资金流评分: {cap_score_val}/100")

        # 资金面综合判断
        cap_score = score_details.get('capital_flow', 7)
        max_cap = score_details.get('max_scores', {}).get('capital_flow', 15)
        cap_rate = cap_score / max_cap * 100 if max_cap > 0 else 50

        if cap_rate >= 70:
            conclusion = '资金面强势，主力和北向资金持续流入，做多意愿明确'
        elif cap_rate >= 50:
            conclusion = '资金面偏正面，整体资金流向有利'
        elif cap_rate >= 30:
            conclusion = '资金面中性，资金流向不明确'
        else:
            conclusion = '资金面偏弱，主力资金流出明显，需警惕'

        return {
            'diagnosis_points': diagnosis_points,
            'signals': signals,
            'conclusion': conclusion,
            'score_rate': round(cap_rate, 1),
            'data_available': bool(capital)
        }

    def _generate_market_environment(self, score_details: Dict,
                                      enhanced_data: Dict = None) -> Dict:
        """生成市场环境诊断"""
        sentiment = enhanced_data.get('market_sentiment', {}) if enhanced_data else {}
        macro = enhanced_data.get('macro', {}) if enhanced_data else {}
        industry = enhanced_data.get('industry', {}) if enhanced_data else {}
        diagnosis_points = []

        # 市场情绪
        sent_signal = sentiment.get('sentiment_signal', '')
        if sent_signal:
            diagnosis_points.append(f"市场情绪: {sent_signal}")

        phase = sentiment.get('emotion_phase', '')
        position = sentiment.get('suggested_position', '')
        if phase:
            diagnosis_points.append(f"BJCJ情绪阶段: 【{phase}】 建议仓位: {position}")

        fbl = sentiment.get('fbl')
        earn = sentiment.get('earn_rate')
        if fbl is not None:
            diagnosis_points.append(f"封板率: {fbl:.0f}%, 赚钱效应: {earn:.0f}%")

        zt = sentiment.get('zt_cnt')
        dt = sentiment.get('dt_cnt')
        if zt is not None:
            diagnosis_points.append(f"涨停: {zt}只, 跌停: {dt}只")

        # 宏观环境
        amount_signal = macro.get('amount_signal', '')
        if amount_signal:
            diagnosis_points.append(f"成交额: {amount_signal}")

        hs300_20d = macro.get('hs300_change_20d')
        if hs300_20d is not None:
            diagnosis_points.append(f"沪深300近20日: {hs300_20d:+.2f}%")

        spread_signal = macro.get('spread_signal', '')
        if spread_signal:
            diagnosis_points.append(f"中美利差: {spread_signal}")

        # 行业热度
        hot_sectors = industry.get('hot_sectors', [])
        if hot_sectors:
            diagnosis_points.append(f"今日热门板块: {', '.join(hot_sectors[:3])}")

        ind_flow = industry.get('industry_net_flow')
        ind_name = industry.get('industry', '')
        if ind_flow is not None and ind_name:
            diagnosis_points.append(f"所属行业({ind_name})资金净流入: {ind_flow:.2f}亿")

        # 综合判断
        sent_score = score_details.get('sentiment', 5)
        ind_score = score_details.get('industry', 2)
        max_sent = score_details.get('max_scores', {}).get('sentiment', 10)
        max_ind = score_details.get('max_scores', {}).get('industry', 5)
        env_rate = (sent_score + ind_score) / (max_sent + max_ind) * 100 if (max_sent + max_ind) > 0 else 50

        if env_rate >= 70:
            conclusion = '市场环境有利，情绪积极、行业热度高，适合积极操作'
        elif env_rate >= 50:
            conclusion = '市场环境中性偏好，可正常操作'
        elif env_rate >= 30:
            conclusion = '市场环境偏冷，建议控制仓位'
        else:
            conclusion = '市场环境不利，情绪低迷，建议防守为主'

        return {
            'diagnosis_points': diagnosis_points,
            'conclusion': conclusion,
            'score_rate': round(env_rate, 1)
        }

    def _generate_risk_assessment(self, score_details: Dict,
                                   enhanced_data: Dict = None) -> Dict:
        """生成风险评估"""
        risks = []
        risk_level = '低'

        total = score_details.get('total', 50)

        # 基于各维度评分识别风险
        max_scores = score_details.get('max_scores', {})
        for dim, max_val in max_scores.items():
            actual = score_details.get(dim, 0)
            rate = actual / max_val * 100 if max_val > 0 else 50
            dim_names = {
                'trend': '趋势', 'technical': '技术指标', 'volume': '成交量',
                'momentum': '动量', 'fundamental': '基本面', 'capital_flow': '资金流',
                'sentiment': '市场情绪', 'industry': '行业强度'
            }
            if rate < 30:
                risks.append({
                    'dimension': dim_names.get(dim, dim),
                    'level': '高',
                    'detail': f'{dim_names.get(dim, dim)}评分仅{actual}/{max_val}，风险较高'
                })

        # 基于增强数据识别具体风险
        if enhanced_data:
            tech = enhanced_data.get('technical', {})
            fund = enhanced_data.get('fundamental', {})
            capital = enhanced_data.get('capital_flow', {})
            sentiment = enhanced_data.get('market_sentiment', {})

            # RSI极端值风险
            rsi = tech.get('rsi')
            if rsi and rsi > 80:
                risks.append({'dimension': '技术面', 'level': '高',
                              'detail': f'RSI={rsi:.1f}严重超买，短期回调风险大'})
            elif rsi and rsi < 20:
                risks.append({'dimension': '技术面', 'level': '中',
                              'detail': f'RSI={rsi:.1f}严重超卖，可能继续下探'})

            # 高负债风险
            debt = fund.get('debt_ratio')
            if debt and debt > 70:
                risks.append({'dimension': '基本面', 'level': '高',
                              'detail': f'资产负债率{debt:.0f}%偏高，财务风险需关注'})

            # 主力资金持续流出
            neg_days = capital.get('negative_days', 0)
            if neg_days >= 7:
                risks.append({'dimension': '资金面', 'level': '高',
                              'detail': f'主力资金连续{neg_days}天净流出'})

            # 市场情绪极端
            phase = sentiment.get('emotion_phase', '')
            if phase == '空仓期':
                risks.append({'dimension': '市场环境', 'level': '高',
                              'detail': 'BJCJ判定为空仓期，系统性风险高'})

        # 确定整体风险等级
        high_risks = [r for r in risks if r['level'] == '高']
        if len(high_risks) >= 3:
            risk_level = '极高'
        elif len(high_risks) >= 2:
            risk_level = '高'
        elif len(high_risks) >= 1:
            risk_level = '中高'
        elif total < 40:
            risk_level = '中'
        else:
            risk_level = '低'

        return {
            'risk_level': risk_level,
            'risk_count': len(risks),
            'high_risk_count': len(high_risks),
            'risks': risks
        }

    def _generate_trading_advice(self, score_details: Dict,
                                  enhanced_data: Dict = None,
                                  price_data: Dict = None) -> Dict:
        """生成操作建议"""
        total = score_details.get('total', 50)
        tech = enhanced_data.get('technical', {}) if enhanced_data else {}
        fund = enhanced_data.get('fundamental', {}) if enhanced_data else {}
        sentiment = enhanced_data.get('market_sentiment', {}) if enhanced_data else {}

        current_price = price_data.get('current_price', 0) if price_data else tech.get('current_price', 0)

        # 操作建议
        if total >= 75:
            action = '买入'
            action_detail = '综合评分较高，多维度信号偏多，建议积极买入'
            position_pct = '30-50%'
        elif total >= 60:
            action = '谨慎买入'
            action_detail = '综合评分中上，可适当建仓，注意控制仓位'
            position_pct = '20-30%'
        elif total >= 50:
            action = '观望'
            action_detail = '综合评分中性，建议观望等待更明确信号'
            position_pct = '0-10%'
        elif total >= 40:
            action = '谨慎持有'
            action_detail = '综合评分偏低，已持有可暂时持有，不建议加仓'
            position_pct = '减仓至10-20%'
        elif total >= 30:
            action = '减仓'
            action_detail = '综合评分较低，建议逐步减仓'
            position_pct = '减仓至5-10%'
        else:
            action = '卖出'
            action_detail = '综合评分很低，多维度信号偏空，建议清仓'
            position_pct = '清仓'

        # BJCJ情绪阶段调整
        phase = sentiment.get('emotion_phase', '')
        if phase == '空仓期' and action in ['买入', '谨慎买入']:
            action = '观望'
            action_detail += '（注意：BJCJ判定为空仓期，建议暂缓买入）'
            position_pct = '0-5%'
        elif phase in ['加仓期', '重仓期'] and action == '观望':
            action_detail += '（BJCJ判定为加仓期，可适当提高仓位）'

        # 止损止盈建议
        support = tech.get('support_short', [])
        resistance = tech.get('resistance_short', [])

        stop_loss = None
        target_price = None
        if current_price > 0:
            if support:
                stop_loss = round(min(support) * 0.97, 2)  # 支撑位下方3%
            else:
                stop_loss = round(current_price * 0.93, 2)  # 默认7%止损

            if resistance:
                target_price = round(max(resistance) * 1.02, 2)  # 压力位上方2%
            else:
                target_price = round(current_price * 1.15, 2)  # 默认15%目标

        # 时间维度建议
        time_advice = {}
        if total >= 60:
            time_advice = {
                'short_term': '短期（1周）: 可逢低买入，关注支撑位',
                'medium_term': '中期（1-3月）: 持有为主，关注基本面变化',
                'long_term': '长期（半年）: 若基本面持续向好，可长期持有'
            }
        elif total >= 40:
            time_advice = {
                'short_term': '短期（1周）: 观望为主，等待方向明确',
                'medium_term': '中期（1-3月）: 关注趋势变化，择机操作',
                'long_term': '长期（半年）: 需观察基本面是否改善'
            }
        else:
            time_advice = {
                'short_term': '短期（1周）: 建议减仓或清仓',
                'medium_term': '中期（1-3月）: 等待底部信号确认',
                'long_term': '长期（半年）: 需等待基本面明显改善'
            }

        return {
            'action': action,
            'action_detail': action_detail,
            'position_pct': position_pct,
            'stop_loss': stop_loss,
            'target_price': target_price,
            'current_price': current_price,
            'time_advice': time_advice
        }

    def _generate_text_summary(self, report: Dict) -> str:
        """生成文本摘要（用于快速展示）"""
        parts = []

        # 综合评级
        rating = report.get('overall_rating', {})
        parts.append(f"【综合评级】{rating.get('rating', 'N/A')} ({rating.get('rating_text', '')}) "
                     f"总分: {rating.get('total_score', 0)}/100")

        # 优势和劣势
        strengths = rating.get('strengths', [])
        weaknesses = rating.get('weaknesses', [])
        if strengths:
            parts.append(f"优势维度: {', '.join(strengths)}")
        if weaknesses:
            parts.append(f"薄弱维度: {', '.join(weaknesses)}")

        # 技术面
        tech = report.get('technical_diagnosis', {})
        if tech.get('conclusion'):
            parts.append(f"\n【技术面】{tech['conclusion']}")

        # 基本面
        fund = report.get('fundamental_diagnosis', {})
        if fund.get('conclusion'):
            parts.append(f"【基本面】{fund['conclusion']}")
        if fund.get('highlights'):
            parts.append(f"  亮点: {'; '.join(fund['highlights'][:3])}")

        # 资金面
        cap = report.get('capital_flow_diagnosis', {})
        if cap.get('conclusion'):
            parts.append(f"【资金面】{cap['conclusion']}")

        # 市场环境
        env = report.get('market_environment', {})
        if env.get('conclusion'):
            parts.append(f"【市场环境】{env['conclusion']}")

        # 风险
        risk = report.get('risk_assessment', {})
        parts.append(f"\n【风险等级】{risk.get('risk_level', '未知')} "
                     f"(共{risk.get('risk_count', 0)}个风险点，"
                     f"其中{risk.get('high_risk_count', 0)}个高风险)")

        # 操作建议
        advice = report.get('trading_advice', {})
        if advice.get('action'):
            parts.append(f"\n【操作建议】{advice['action']} - {advice.get('action_detail', '')}")
            parts.append(f"  建议仓位: {advice.get('position_pct', 'N/A')}")
            if advice.get('stop_loss'):
                parts.append(f"  止损位: {advice['stop_loss']}")
            if advice.get('target_price'):
                parts.append(f"  目标位: {advice['target_price']}")

        return '\n'.join(parts)
