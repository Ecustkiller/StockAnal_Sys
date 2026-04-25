# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 基本面分析器（增强版）
开发者：熊猫大侠
版本：v3.0.0
许可证：MIT License

增强内容：
- 现金流分析
- 杜邦分析分解
- 同行业PE/PB/ROE对比
- 财务趋势（近4季度对比）
- 分红历史
- 股权质押比例
"""
# fundamental_analyzer.py
import akshare as ak
import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class FundamentalAnalyzer:
    def __init__(self):
        """初始化基础分析类"""
        self.data_cache = {}

    def get_financial_indicators(self, stock_code):
        """获取财务指标数据"""
        try:
            # 获取基本财务指标
            financial_data = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2022")

            # 获取最新估值指标
            valuation = ak.stock_value_em(symbol=stock_code)

            # 整合数据
            indicators = {
                'pe_ttm': float(valuation['PE(TTM)'].iloc[0]),
                'pb': float(valuation['市净率'].iloc[0]),
                'ps_ttm': float(valuation['市销率'].iloc[0]),
                'roe': float(financial_data['加权净资产收益率(%)'].iloc[0]),
                'gross_margin': float(financial_data['销售毛利率(%)'].iloc[0]),
                'net_profit_margin': float(financial_data['总资产净利润率(%)'].iloc[0]),
                'debt_ratio': float(financial_data['资产负债率(%)'].iloc[0])
            }

            return indicators
        except Exception as e:
            logger.warning(f"获取财务指标出错: {str(e)}")
            return {}

    def get_growth_data(self, stock_code):
        """获取成长性数据"""
        try:
            # 获取历年财务数据
            financial_data = ak.stock_financial_abstract(symbol=stock_code)

            # 计算各项成长率
            revenue = financial_data['营业收入'].astype(float)
            net_profit = financial_data['净利润'].astype(float)

            growth = {
                'revenue_growth_3y': self._calculate_cagr(revenue, 3),
                'profit_growth_3y': self._calculate_cagr(net_profit, 3),
                'revenue_growth_5y': self._calculate_cagr(revenue, 5),
                'profit_growth_5y': self._calculate_cagr(net_profit, 5)
            }

            return growth
        except Exception as e:
            logger.warning(f"获取成长数据出错: {str(e)}")
            return {}

    def _calculate_cagr(self, series, years):
        """计算复合年增长率"""
        if len(series) < years:
            return None

        latest = series.iloc[0]
        earlier = series.iloc[min(years, len(series) - 1)]

        if earlier <= 0:
            return None

        return ((latest / earlier) ** (1 / years) - 1) * 100

    # ==================== 新增功能 ====================

    def get_cash_flow_analysis(self, stock_code):
        """
        现金流分析 - 分析经营/投资/筹资三大现金流
        """
        try:
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
            if df is None or df.empty:
                return {}

            # 取最近4期数据
            df = df.head(4)

            result = {
                'periods': [],
                'operating_cf': [],  # 经营活动现金流
                'investing_cf': [],  # 投资活动现金流
                'financing_cf': [],  # 筹资活动现金流
                'free_cf': [],       # 自由现金流（经营-资本支出）
            }

            for _, row in df.iterrows():
                period = str(row.get('REPORT_DATE_NAME', row.get('报告期', '')))
                op_cf = self._safe_float(row.get('NETCASH_OPERATE', row.get('经营活动产生的现金流量净额', 0)))
                inv_cf = self._safe_float(row.get('NETCASH_INVEST', row.get('投资活动产生的现金流量净额', 0)))
                fin_cf = self._safe_float(row.get('NETCASH_FINANCE', row.get('筹资活动产生的现金流量净额', 0)))

                # 自由现金流 = 经营现金流 - 资本支出（简化为投资现金流的绝对值的一部分）
                free_cf = op_cf + inv_cf  # 简化计算

                result['periods'].append(period)
                result['operating_cf'].append(round(op_cf / 1e8, 2))  # 转为亿元
                result['investing_cf'].append(round(inv_cf / 1e8, 2))
                result['financing_cf'].append(round(fin_cf / 1e8, 2))
                result['free_cf'].append(round(free_cf / 1e8, 2))

            # 现金流质量评估
            latest_op = result['operating_cf'][0] if result['operating_cf'] else 0
            latest_free = result['free_cf'][0] if result['free_cf'] else 0

            if latest_op > 0 and latest_free > 0:
                result['quality'] = '优秀'
                result['quality_detail'] = '经营现金流和自由现金流均为正，造血能力强'
            elif latest_op > 0:
                result['quality'] = '良好'
                result['quality_detail'] = '经营现金流为正，但投资支出较大'
            elif latest_op < 0:
                result['quality'] = '较差'
                result['quality_detail'] = '经营现金流为负，需关注经营质量'
            else:
                result['quality'] = '一般'
                result['quality_detail'] = '现金流数据需进一步分析'

            return result
        except Exception as e:
            logger.warning(f"现金流分析出错: {str(e)}")
            return {}

    def get_dupont_analysis(self, stock_code):
        """
        杜邦分析 - 将ROE分解为三个驱动因子
        ROE = 净利润率 × 资产周转率 × 权益乘数
        """
        try:
            df = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2022")
            if df is None or df.empty:
                return {}

            latest = df.iloc[0]

            # 提取杜邦分析所需指标
            roe = self._safe_float(latest.get('加权净资产收益率(%)', 0))
            net_margin = self._safe_float(latest.get('总资产净利润率(%)', 0))
            # 销售净利率
            sales_net_margin = self._safe_float(latest.get('销售净利率(%)',
                                                           latest.get('主营业务利润率(%)', 0)))
            # 资产负债率
            debt_ratio = self._safe_float(latest.get('资产负债率(%)', 0))

            # 权益乘数 = 1 / (1 - 资产负债率)
            equity_multiplier = 1 / (1 - debt_ratio / 100) if debt_ratio < 100 else 0

            # 资产周转率 = ROE / (净利润率 × 权益乘数)
            if sales_net_margin > 0 and equity_multiplier > 0:
                asset_turnover = roe / (sales_net_margin * equity_multiplier) if roe > 0 else 0
            else:
                asset_turnover = 0

            # 评估各因子
            factors = []
            if sales_net_margin > 15:
                factors.append({'name': '净利润率', 'value': f'{sales_net_margin:.2f}%',
                                'level': '优秀', 'detail': '盈利能力强，产品附加值高'})
            elif sales_net_margin > 8:
                factors.append({'name': '净利润率', 'value': f'{sales_net_margin:.2f}%',
                                'level': '良好', 'detail': '盈利能力正常'})
            else:
                factors.append({'name': '净利润率', 'value': f'{sales_net_margin:.2f}%',
                                'level': '偏低', 'detail': '盈利能力较弱，需关注成本控制'})

            if asset_turnover > 1.0:
                factors.append({'name': '资产周转率', 'value': f'{asset_turnover:.2f}',
                                'level': '优秀', 'detail': '资产运营效率高'})
            elif asset_turnover > 0.5:
                factors.append({'name': '资产周转率', 'value': f'{asset_turnover:.2f}',
                                'level': '良好', 'detail': '资产运营效率正常'})
            else:
                factors.append({'name': '资产周转率', 'value': f'{asset_turnover:.2f}',
                                'level': '偏低', 'detail': '资产运营效率较低'})

            if equity_multiplier < 2:
                factors.append({'name': '权益乘数', 'value': f'{equity_multiplier:.2f}',
                                'level': '稳健', 'detail': '杠杆水平低，财务风险小'})
            elif equity_multiplier < 4:
                factors.append({'name': '权益乘数', 'value': f'{equity_multiplier:.2f}',
                                'level': '适中', 'detail': '杠杆水平适中'})
            else:
                factors.append({'name': '权益乘数', 'value': f'{equity_multiplier:.2f}',
                                'level': '偏高', 'detail': '杠杆水平高，财务风险较大'})

            # ROE驱动力判断
            driver = '均衡型'
            if sales_net_margin > 15 and asset_turnover < 0.5:
                driver = '高利润率驱动型（如白酒、医药）'
            elif asset_turnover > 1.0 and sales_net_margin < 8:
                driver = '高周转驱动型（如零售、快消）'
            elif equity_multiplier > 4:
                driver = '高杠杆驱动型（如银行、地产）'

            return {
                'roe': round(roe, 2),
                'net_profit_margin': round(sales_net_margin, 2),
                'asset_turnover': round(asset_turnover, 2),
                'equity_multiplier': round(equity_multiplier, 2),
                'debt_ratio': round(debt_ratio, 2),
                'factors': factors,
                'driver': driver,
                'formula': f'ROE({roe:.1f}%) = 净利润率({sales_net_margin:.1f}%) × 资产周转率({asset_turnover:.2f}) × 权益乘数({equity_multiplier:.2f})'
            }
        except Exception as e:
            logger.warning(f"杜邦分析出错: {str(e)}")
            return {}

    def get_financial_trend(self, stock_code):
        """
        财务趋势分析 - 近4季度/年度关键指标对比
        """
        try:
            df = ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2021")
            if df is None or df.empty:
                return {}

            # 取最近8期数据（约2年）
            df = df.head(8)

            result = {
                'periods': [],
                'roe_trend': [],
                'gross_margin_trend': [],
                'debt_ratio_trend': [],
                'net_margin_trend': [],
            }

            for _, row in df.iterrows():
                period = str(row.get('日期', ''))
                result['periods'].append(period)
                result['roe_trend'].append(self._safe_float(row.get('加权净资产收益率(%)', 0)))
                result['gross_margin_trend'].append(self._safe_float(row.get('销售毛利率(%)', 0)))
                result['debt_ratio_trend'].append(self._safe_float(row.get('资产负债率(%)', 0)))
                result['net_margin_trend'].append(self._safe_float(row.get('总资产净利润率(%)', 0)))

            # 趋势判断
            if len(result['roe_trend']) >= 2:
                roe_change = result['roe_trend'][0] - result['roe_trend'][-1]
                if roe_change > 3:
                    result['roe_trend_signal'] = '上升趋势，盈利能力持续改善'
                elif roe_change < -3:
                    result['roe_trend_signal'] = '下降趋势，盈利能力恶化'
                else:
                    result['roe_trend_signal'] = '基本稳定'

            if len(result['gross_margin_trend']) >= 2:
                gm_change = result['gross_margin_trend'][0] - result['gross_margin_trend'][-1]
                if gm_change > 2:
                    result['margin_trend_signal'] = '毛利率上升，竞争力增强'
                elif gm_change < -2:
                    result['margin_trend_signal'] = '毛利率下降，可能面临竞争压力'
                else:
                    result['margin_trend_signal'] = '毛利率基本稳定'

            return result
        except Exception as e:
            logger.warning(f"财务趋势分析出错: {str(e)}")
            return {}

    def get_dividend_history(self, stock_code):
        """
        分红历史 - 获取近年分红记录
        """
        try:
            df = ak.stock_history_dividend_detail(symbol=stock_code, indicator="分红")
            if df is None or df.empty:
                return {'records': [], 'summary': '暂无分红记录'}

            records = []
            total_dividend = 0
            for _, row in df.head(10).iterrows():  # 最近10次
                record = {
                    'date': str(row.get('公告日期', row.get('除权除息日', ''))),
                    'plan': str(row.get('分红方案', '')),
                    'ex_date': str(row.get('除权除息日', '')),
                }
                records.append(record)

            # 分红频率评估
            years_count = len(set([r['date'][:4] for r in records if len(r['date']) >= 4]))
            if years_count >= 5:
                summary = f'近年持续分红（{years_count}年），分红记录良好'
                dividend_quality = '优秀'
            elif years_count >= 3:
                summary = f'近年有分红记录（{years_count}年），分红较为稳定'
                dividend_quality = '良好'
            elif years_count >= 1:
                summary = f'有分红记录（{years_count}年），但不够稳定'
                dividend_quality = '一般'
            else:
                summary = '近年无分红记录'
                dividend_quality = '较差'

            return {
                'records': records,
                'summary': summary,
                'dividend_quality': dividend_quality,
                'total_records': len(records)
            }
        except Exception as e:
            logger.warning(f"获取分红历史出错: {str(e)}")
            return {'records': [], 'summary': '分红数据获取失败'}

    def get_industry_comparison(self, stock_code):
        """
        同行业对比 - 获取同行业公司的估值和盈利对比
        """
        try:
            # 获取个股所属行业
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if stock_info is None or stock_info.empty:
                return {}

            industry = ''
            for _, row in stock_info.iterrows():
                if '行业' in str(row.get('item', '')):
                    industry = str(row.get('value', ''))
                    break

            if not industry:
                return {'error': '无法获取行业信息'}

            # 获取行业内公司列表
            try:
                industry_stocks = ak.stock_board_industry_cons_em(symbol=industry)
            except Exception:
                return {'industry': industry, 'peers': [], 'note': '行业对比数据暂不可用'}

            if industry_stocks is None or industry_stocks.empty:
                return {'industry': industry, 'peers': []}

            peers = []
            for _, row in industry_stocks.head(15).iterrows():  # 取前15只
                peer = {
                    'code': str(row.get('代码', '')),
                    'name': str(row.get('名称', '')),
                    'price': self._safe_float(row.get('最新价', 0)),
                    'change_pct': self._safe_float(row.get('涨跌幅', 0)),
                    'pe': self._safe_float(row.get('市盈率-动态', 0)),
                    'pb': self._safe_float(row.get('市净率', 0)),
                    'total_mv': self._safe_float(row.get('总市值', 0)) / 1e8,  # 转为亿
                }
                peers.append(peer)

            # 计算行业中位数
            pe_values = [p['pe'] for p in peers if p['pe'] and p['pe'] > 0]
            pb_values = [p['pb'] for p in peers if p['pb'] and p['pb'] > 0]

            industry_median_pe = round(np.median(pe_values), 2) if pe_values else None
            industry_median_pb = round(np.median(pb_values), 2) if pb_values else None

            # 找到当前股票在行业中的排名
            current_peer = next((p for p in peers if p['code'] == stock_code), None)
            pe_rank = None
            if current_peer and current_peer['pe'] and pe_values:
                pe_rank = sorted(pe_values).index(
                    min(pe_values, key=lambda x: abs(x - current_peer['pe']))) + 1

            return {
                'industry': industry,
                'peer_count': len(peers),
                'peers': peers,
                'industry_median_pe': industry_median_pe,
                'industry_median_pb': industry_median_pb,
                'current_stock_pe_rank': pe_rank,
                'total_in_industry': len(pe_values),
                'valuation_position': self._get_valuation_position(
                    current_peer.get('pe') if current_peer else None,
                    industry_median_pe
                )
            }
        except Exception as e:
            logger.warning(f"同行业对比出错: {str(e)}")
            return {}

    def get_pledge_info(self, stock_code):
        """
        股权质押信息
        """
        try:
            df = ak.stock_gpzy_pledge_ratio_em()
            if df is None or df.empty:
                return {}

            # 查找当前股票
            stock_row = df[df['股票代码'] == stock_code]
            if stock_row.empty:
                return {'pledge_ratio': 0, 'risk_level': '低', 'detail': '未查到质押信息'}

            row = stock_row.iloc[0]
            pledge_ratio = self._safe_float(row.get('质押比例(%)', 0))

            if pledge_ratio > 50:
                risk_level = '极高'
                detail = f'质押比例{pledge_ratio:.1f}%，超过50%，存在平仓风险'
            elif pledge_ratio > 30:
                risk_level = '高'
                detail = f'质押比例{pledge_ratio:.1f}%，偏高，需关注'
            elif pledge_ratio > 10:
                risk_level = '中'
                detail = f'质押比例{pledge_ratio:.1f}%，处于正常范围'
            else:
                risk_level = '低'
                detail = f'质押比例{pledge_ratio:.1f}%，质押风险低'

            return {
                'pledge_ratio': round(pledge_ratio, 2),
                'risk_level': risk_level,
                'detail': detail
            }
        except Exception as e:
            logger.warning(f"获取质押信息出错: {str(e)}")
            return {}

    def get_comprehensive_fundamental(self, stock_code):
        """
        获取全面的基本面分析数据（整合所有维度）
        """
        result = {
            'stock_code': stock_code,
            'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        # 基础财务指标
        result['indicators'] = self.get_financial_indicators(stock_code)

        # 成长性数据
        result['growth'] = self.get_growth_data(stock_code)

        # 现金流分析
        result['cash_flow'] = self.get_cash_flow_analysis(stock_code)

        # 杜邦分析
        result['dupont'] = self.get_dupont_analysis(stock_code)

        # 财务趋势
        result['financial_trend'] = self.get_financial_trend(stock_code)

        # 分红历史
        result['dividend'] = self.get_dividend_history(stock_code)

        # 同行业对比
        result['industry_comparison'] = self.get_industry_comparison(stock_code)

        # 股权质押
        result['pledge'] = self.get_pledge_info(stock_code)

        # 综合评分
        result['score'] = self.calculate_fundamental_score(stock_code)

        return result

    # ==================== 辅助方法 ====================

    def _safe_float(self, value, default=0):
        """安全转换为float"""
        try:
            if value is None or value == '' or value == '--' or value == 'nan':
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _get_valuation_position(self, current_pe, median_pe):
        """判断估值在行业中的位置"""
        if not current_pe or not median_pe or median_pe <= 0:
            return '无法判断'
        ratio = current_pe / median_pe
        if ratio < 0.7:
            return '明显低于行业中位数，可能被低估'
        elif ratio < 0.9:
            return '略低于行业中位数'
        elif ratio < 1.1:
            return '接近行业中位数'
        elif ratio < 1.3:
            return '略高于行业中位数'
        else:
            return '明显高于行业中位数，估值偏高'

    def calculate_fundamental_score(self, stock_code):
        """计算基本面综合评分（增强版）"""
        indicators = self.get_financial_indicators(stock_code)
        growth = self.get_growth_data(stock_code)

        # 估值评分 (25分)
        valuation_score = 0
        if 'pe_ttm' in indicators and indicators['pe_ttm'] > 0:
            pe = indicators['pe_ttm']
            if pe < 15:
                valuation_score += 25
            elif pe < 25:
                valuation_score += 20
            elif pe < 35:
                valuation_score += 15
            elif pe < 50:
                valuation_score += 10
            else:
                valuation_score += 5

        # 财务健康评分 (35分)
        financial_score = 0
        if 'roe' in indicators:
            roe = indicators['roe']
            if roe > 20:
                financial_score += 15
            elif roe > 15:
                financial_score += 12
            elif roe > 10:
                financial_score += 8
            elif roe > 5:
                financial_score += 4

        if 'debt_ratio' in indicators:
            debt_ratio = indicators['debt_ratio']
            if debt_ratio < 30:
                financial_score += 12
            elif debt_ratio < 50:
                financial_score += 8
            elif debt_ratio < 70:
                financial_score += 4

        if 'gross_margin' in indicators:
            gm = indicators['gross_margin']
            if gm > 50:
                financial_score += 8
            elif gm > 30:
                financial_score += 6
            elif gm > 15:
                financial_score += 3

        # 成长性评分 (25分)
        growth_score = 0
        if 'revenue_growth_3y' in growth and growth['revenue_growth_3y']:
            rev_growth = growth['revenue_growth_3y']
            if rev_growth > 30:
                growth_score += 13
            elif rev_growth > 20:
                growth_score += 10
            elif rev_growth > 10:
                growth_score += 7
            elif rev_growth > 0:
                growth_score += 3

        if 'profit_growth_3y' in growth and growth['profit_growth_3y']:
            profit_growth = growth['profit_growth_3y']
            if profit_growth > 30:
                growth_score += 12
            elif profit_growth > 20:
                growth_score += 9
            elif profit_growth > 10:
                growth_score += 6
            elif profit_growth > 0:
                growth_score += 3

        # 现金流质量评分 (15分) - 新增
        cashflow_score = 0
        try:
            cf = self.get_cash_flow_analysis(stock_code)
            if cf:
                quality = cf.get('quality', '')
                if quality == '优秀':
                    cashflow_score = 15
                elif quality == '良好':
                    cashflow_score = 10
                elif quality == '一般':
                    cashflow_score = 5
                else:
                    cashflow_score = 2
        except Exception:
            cashflow_score = 7  # 数据不可用给中性分

        # 计算总分
        total_score = valuation_score + financial_score + growth_score + cashflow_score

        return {
            'total': total_score,
            'valuation': valuation_score,
            'financial_health': financial_score,
            'growth': growth_score,
            'cashflow': cashflow_score,
            'max_scores': {
                'valuation': 25,
                'financial_health': 35,
                'growth': 25,
                'cashflow': 15
            },
            'details': {
                'indicators': indicators,
                'growth': growth
            }
        }