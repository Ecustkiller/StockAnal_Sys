# -*- coding: utf-8 -*-
"""
同行业横向对比模块
==================
输入一只股票代码，自动识别所属行业，找出同行业TOP10公司，
从估值、盈利、成长、资金流、技术面5个维度进行横向对比。

创建时间: 2026-04-25
"""

import logging
import traceback
import numpy as np
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class IndustryComparator:
    """同行业横向对比分析器"""

    def __init__(self):
        self.data_cache = {}
        self.cache_ttl = 3600  # 缓存1小时

    def compare(self, stock_code: str, top_n: int = 10) -> Dict:
        """
        同行业横向对比主入口

        参数:
            stock_code: 股票代码（如 '000001'）
            top_n: 对比的同行业公司数量（默认10）

        返回:
            包含5维度对比数据的完整报告
        """
        try:
            # 1. 识别行业并获取成分股
            industry_info = self._get_industry_info(stock_code)
            if not industry_info or not industry_info.get('industry'):
                return {'error': '无法识别股票所属行业', 'stock_code': stock_code}

            industry = industry_info['industry']
            logger.info(f"股票 {stock_code} 所属行业: {industry}")

            # 2. 获取同行业成分股列表
            peers = self._get_industry_peers(industry, stock_code, top_n)
            if not peers:
                return {
                    'error': '无法获取同行业公司数据',
                    'stock_code': stock_code,
                    'industry': industry
                }

            # 3. 收集5维度数据
            peer_codes = [p['code'] for p in peers]
            dimension_data = self._collect_dimension_data(peer_codes, peers)

            # 4. 计算5维度评分
            scored_peers = self._calculate_dimension_scores(peers, dimension_data)

            # 5. 找到目标股票的数据
            target = next((p for p in scored_peers if p['code'] == stock_code), None)

            # 6. 计算行业统计
            industry_stats = self._calculate_industry_stats(scored_peers)

            # 7. 生成对比结论
            conclusion = self._generate_conclusion(target, scored_peers, industry_stats, industry)

            return {
                'stock_code': stock_code,
                'stock_name': industry_info.get('stock_name', ''),
                'industry': industry,
                'peer_count': len(scored_peers),
                'peers': scored_peers,
                'target_stock': target,
                'industry_stats': industry_stats,
                'conclusion': conclusion,
                'dimensions': ['估值', '盈利', '成长', '资金流', '技术面'],
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"同行业对比出错: {traceback.format_exc()}")
            return {'error': f'同行业对比出错: {str(e)}', 'stock_code': stock_code}

    def _get_industry_info(self, stock_code: str) -> Dict:
        """获取股票的行业信息"""
        cache_key = f"industry_info_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        try:
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if stock_info is None or stock_info.empty:
                return {}

            result = {'stock_code': stock_code}
            for _, row in stock_info.iterrows():
                item = str(row.get('item', ''))
                value = str(row.get('value', ''))
                if '行业' in item:
                    result['industry'] = value
                elif '股票简称' in item or '名称' in item:
                    result['stock_name'] = value
                elif '总市值' in item:
                    try:
                        result['total_mv'] = float(value) / 1e8
                    except (ValueError, TypeError):
                        pass

            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.warning(f"获取行业信息失败 {stock_code}: {e}")
            return {}

    def _get_industry_peers(self, industry: str, stock_code: str, top_n: int) -> List[Dict]:
        """获取同行业成分股，按市值排序取TOP N（使用 Tushare 数据源）"""
        cache_key = f"industry_peers_{industry}_{top_n}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        try:
            from tushare_industry_helper import get_industry_stocks_ts
            stocks_list = get_industry_stocks_ts(industry)

            if not stocks_list:
                return []

            peers = []
            for item in stocks_list:
                try:
                    total_mv = item.get('total_mv', 0)
                    peer = {
                        'code': item.get('code', ''),
                        'name': item.get('name', ''),
                        'price': item.get('price', 0),
                        'change_pct': item.get('change', 0),
                        'pe': item.get('pe', 0),
                        'pb': item.get('pb', 0),
                        'total_mv': round(total_mv, 2) if total_mv else 0,
                        'turnover_rate': item.get('turnover_rate', 0),
                        'volume_ratio': 0,  # Tushare 需要额外接口获取量比
                    }
                    peers.append(peer)
                except Exception:
                    continue

            # 按市值排序，取TOP N（确保目标股票在内）
            peers.sort(key=lambda x: x.get('total_mv', 0), reverse=True)

            # 确保目标股票在列表中
            target_in_list = any(p['code'] == stock_code for p in peers[:top_n])
            top_peers = peers[:top_n]

            if not target_in_list:
                target = next((p for p in peers if p['code'] == stock_code), None)
                if target:
                    top_peers = peers[:top_n - 1] + [target]

            self._set_cache(cache_key, top_peers)
            return top_peers

        except Exception as e:
            logger.warning(f"获取行业成分股失败 {industry}: {e}")
            return []

    def _collect_dimension_data(self, peer_codes: List[str], peers: List[Dict]) -> Dict:
        """收集5维度的原始数据"""
        dimension_data = {
            'valuation': {},   # 估值
            'profitability': {},  # 盈利
            'growth': {},      # 成长
            'capital_flow': {},  # 资金流
            'technical': {},   # 技术面
        }

        # 批量获取财务数据
        for code in peer_codes:
            try:
                self._collect_single_stock_data(code, dimension_data)
            except Exception as e:
                logger.debug(f"收集 {code} 数据失败: {e}")

        # 从peers中补充已有的估值数据
        for peer in peers:
            code = peer['code']
            if code not in dimension_data['valuation']:
                dimension_data['valuation'][code] = {}
            val = dimension_data['valuation'][code]
            if not val.get('pe') and peer.get('pe'):
                val['pe'] = peer['pe']
            if not val.get('pb') and peer.get('pb'):
                val['pb'] = peer['pb']

            # 技术面数据从peers补充
            if code not in dimension_data['technical']:
                dimension_data['technical'][code] = {}
            tech = dimension_data['technical'][code]
            if not tech.get('change_pct'):
                tech['change_pct'] = peer.get('change_pct', 0)
            if not tech.get('turnover_rate'):
                tech['turnover_rate'] = peer.get('turnover_rate', 0)

        return dimension_data

    def _collect_single_stock_data(self, stock_code: str, dimension_data: Dict):
        """收集单只股票的多维度数据"""
        cache_key = f"stock_dims_{stock_code}"
        cached = self._get_cache(cache_key)
        if cached:
            for dim in dimension_data:
                if stock_code in cached.get(dim, {}):
                    dimension_data[dim][stock_code] = cached[dim][stock_code]
            return

        stock_dims = {}

        # === 估值 + 盈利 + 成长：从财务指标获取 ===
        try:
            fin_df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
            if fin_df is not None and not fin_df.empty:
                latest = fin_df.iloc[0]

                # 估值数据
                val_data = {}
                val_data['eps'] = self._safe_float(latest.get('基本每股收益', 0))
                dimension_data['valuation'][stock_code] = val_data
                stock_dims['valuation'] = {stock_code: val_data}

                # 盈利数据
                prof_data = {}
                prof_data['roe'] = self._safe_float(latest.get('净资产收益率', 0))
                prof_data['gross_margin'] = self._safe_float(latest.get('销售毛利率', 0))
                prof_data['net_margin'] = self._safe_float(latest.get('销售净利率', 0))
                dimension_data['profitability'][stock_code] = prof_data
                stock_dims['profitability'] = {stock_code: prof_data}

                # 成长数据
                growth_data = {}
                growth_data['revenue_growth'] = self._safe_float(latest.get('营业总收入同比增长率', 0))
                growth_data['profit_growth'] = self._safe_float(latest.get('归属净利润同比增长率', 0))
                dimension_data['growth'][stock_code] = growth_data
                stock_dims['growth'] = {stock_code: growth_data}

        except Exception as e:
            logger.debug(f"获取 {stock_code} 财务数据失败: {e}")

        # === 资金流 ===
        try:
            flow_df = ak.stock_individual_fund_flow(stock=stock_code, market="")
            if flow_df is not None and not flow_df.empty:
                recent = flow_df.tail(5)
                cap_data = {}

                # 尝试获取主力净流入
                net_col = None
                for col in flow_df.columns:
                    if '主力' in str(col) and '净' in str(col):
                        net_col = col
                        break

                if net_col:
                    cap_data['main_net_5d'] = round(recent[net_col].sum() / 1e4, 2)  # 万元转亿
                    pos_days = (recent[net_col] > 0).sum()
                    cap_data['positive_days'] = int(pos_days)
                else:
                    cap_data['main_net_5d'] = 0
                    cap_data['positive_days'] = 0

                dimension_data['capital_flow'][stock_code] = cap_data
                stock_dims['capital_flow'] = {stock_code: cap_data}

        except Exception as e:
            logger.debug(f"获取 {stock_code} 资金流数据失败: {e}")

        # === 技术面 ===
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
            hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily",
                                          start_date=start_date, end_date=end_date, adjust="qfq")
            if hist_df is not None and not hist_df.empty and len(hist_df) >= 20:
                tech_data = dimension_data.get('technical', {}).get(stock_code, {})

                closes = hist_df['收盘'].values
                # 近5日涨幅
                if len(closes) >= 5:
                    tech_data['change_5d'] = round((closes[-1] / closes[-5] - 1) * 100, 2)
                # 近20日涨幅
                if len(closes) >= 20:
                    tech_data['change_20d'] = round((closes[-1] / closes[-20] - 1) * 100, 2)

                # MA20位置
                ma20 = np.mean(closes[-20:])
                tech_data['above_ma20'] = 1 if closes[-1] > ma20 else 0
                tech_data['ma20_distance'] = round((closes[-1] / ma20 - 1) * 100, 2)

                # RSI(14)
                if len(closes) >= 15:
                    deltas = np.diff(closes[-15:])
                    gains = np.where(deltas > 0, deltas, 0)
                    losses = np.where(deltas < 0, -deltas, 0)
                    avg_gain = np.mean(gains)
                    avg_loss = np.mean(losses)
                    if avg_loss > 0:
                        rs = avg_gain / avg_loss
                        tech_data['rsi'] = round(100 - 100 / (1 + rs), 2)
                    else:
                        tech_data['rsi'] = 100

                # 波动率（20日年化）
                if len(closes) >= 20:
                    returns = np.diff(np.log(closes[-21:]))
                    tech_data['volatility'] = round(np.std(returns) * np.sqrt(252) * 100, 2)

                dimension_data['technical'][stock_code] = tech_data
                stock_dims['technical'] = {stock_code: tech_data}

        except Exception as e:
            logger.debug(f"获取 {stock_code} 技术面数据失败: {e}")

        # 缓存
        self._set_cache(cache_key, stock_dims)

    def _calculate_dimension_scores(self, peers: List[Dict], dimension_data: Dict) -> List[Dict]:
        """计算每只股票的5维度评分（0-100）"""
        scored_peers = []

        for peer in peers:
            code = peer['code']
            scores = {}

            # === 估值评分（越低越好，逆向评分）===
            val = dimension_data.get('valuation', {}).get(code, {})
            pe = val.get('pe') or peer.get('pe', 0)
            pb = val.get('pb') or peer.get('pb', 0)
            val_score = 50  # 默认中性
            if pe and pe > 0:
                if pe < 15:
                    val_score = 90
                elif pe < 25:
                    val_score = 75
                elif pe < 40:
                    val_score = 55
                elif pe < 60:
                    val_score = 35
                else:
                    val_score = 15
            if pb and pb > 0:
                pb_adj = 0
                if pb < 1:
                    pb_adj = 15
                elif pb < 2:
                    pb_adj = 10
                elif pb < 4:
                    pb_adj = 0
                else:
                    pb_adj = -10
                val_score = max(0, min(100, val_score + pb_adj))
            scores['valuation'] = val_score

            # === 盈利评分 ===
            prof = dimension_data.get('profitability', {}).get(code, {})
            roe = prof.get('roe', 0)
            gm = prof.get('gross_margin', 0)
            nm = prof.get('net_margin', 0)
            prof_score = 50
            if roe:
                if roe > 20:
                    prof_score = 90
                elif roe > 15:
                    prof_score = 75
                elif roe > 10:
                    prof_score = 60
                elif roe > 5:
                    prof_score = 40
                else:
                    prof_score = 20
            if gm and gm > 50:
                prof_score = min(100, prof_score + 10)
            if nm and nm > 20:
                prof_score = min(100, prof_score + 5)
            scores['profitability'] = prof_score

            # === 成长评分 ===
            growth = dimension_data.get('growth', {}).get(code, {})
            rev_g = growth.get('revenue_growth', 0)
            profit_g = growth.get('profit_growth', 0)
            growth_score = 50
            if rev_g:
                if rev_g > 30:
                    growth_score = 90
                elif rev_g > 15:
                    growth_score = 70
                elif rev_g > 0:
                    growth_score = 50
                elif rev_g > -10:
                    growth_score = 30
                else:
                    growth_score = 10
            if profit_g and profit_g > 30:
                growth_score = min(100, growth_score + 10)
            elif profit_g and profit_g < -20:
                growth_score = max(0, growth_score - 15)
            scores['growth'] = growth_score

            # === 资金流评分 ===
            cap = dimension_data.get('capital_flow', {}).get(code, {})
            main_net = cap.get('main_net_5d', 0)
            pos_days = cap.get('positive_days', 0)
            cap_score = 50
            if main_net > 1:
                cap_score = 85
            elif main_net > 0:
                cap_score = 70
            elif main_net > -0.5:
                cap_score = 50
            elif main_net > -2:
                cap_score = 30
            else:
                cap_score = 15
            if pos_days >= 4:
                cap_score = min(100, cap_score + 10)
            elif pos_days <= 1:
                cap_score = max(0, cap_score - 10)
            scores['capital_flow'] = cap_score

            # === 技术面评分 ===
            tech = dimension_data.get('technical', {}).get(code, {})
            change_5d = tech.get('change_5d', 0)
            change_20d = tech.get('change_20d', 0)
            above_ma20 = tech.get('above_ma20', 0)
            rsi = tech.get('rsi', 50)
            tech_score = 50
            # 近期涨幅
            if change_5d and change_5d > 5:
                tech_score = 80
            elif change_5d and change_5d > 0:
                tech_score = 65
            elif change_5d and change_5d > -3:
                tech_score = 45
            else:
                tech_score = 25
            # MA20位置
            if above_ma20:
                tech_score = min(100, tech_score + 10)
            else:
                tech_score = max(0, tech_score - 5)
            # RSI
            if rsi and 40 <= rsi <= 60:
                tech_score = min(100, tech_score + 5)
            elif rsi and (rsi < 30 or rsi > 70):
                tech_score = max(0, tech_score - 5)
            scores['technical'] = tech_score

            # === 综合评分 ===
            weights = {'valuation': 0.25, 'profitability': 0.25, 'growth': 0.20,
                       'capital_flow': 0.15, 'technical': 0.15}
            total = sum(scores[d] * weights[d] for d in weights)
            scores['total'] = round(total, 1)

            # 构建完整的peer数据
            scored_peer = {**peer, 'scores': scores}
            # 附加详细数据
            scored_peer['detail'] = {
                'roe': prof.get('roe'),
                'gross_margin': prof.get('gross_margin'),
                'net_margin': prof.get('net_margin'),
                'revenue_growth': growth.get('revenue_growth'),
                'profit_growth': growth.get('profit_growth'),
                'main_net_5d': cap.get('main_net_5d'),
                'change_5d': tech.get('change_5d'),
                'change_20d': tech.get('change_20d'),
                'rsi': tech.get('rsi'),
                'volatility': tech.get('volatility'),
            }
            scored_peers.append(scored_peer)

        # 按综合评分排序
        scored_peers.sort(key=lambda x: x['scores']['total'], reverse=True)

        # 添加排名
        for i, p in enumerate(scored_peers):
            p['rank'] = i + 1

        return scored_peers

    def _calculate_industry_stats(self, scored_peers: List[Dict]) -> Dict:
        """计算行业统计数据"""
        if not scored_peers:
            return {}

        stats = {}

        # 各维度的行业中位数和均值
        dimensions = ['valuation', 'profitability', 'growth', 'capital_flow', 'technical', 'total']
        dim_names = {'valuation': '估值', 'profitability': '盈利', 'growth': '成长',
                     'capital_flow': '资金流', 'technical': '技术面', 'total': '综合'}

        for dim in dimensions:
            values = [p['scores'].get(dim, 50) for p in scored_peers]
            stats[dim] = {
                'name': dim_names.get(dim, dim),
                'median': round(np.median(values), 1),
                'mean': round(np.mean(values), 1),
                'max': round(max(values), 1),
                'min': round(min(values), 1),
                'std': round(np.std(values), 1),
            }

        # 关键财务指标的行业中位数
        pe_values = [p.get('pe', 0) for p in scored_peers if p.get('pe') and p['pe'] > 0]
        pb_values = [p.get('pb', 0) for p in scored_peers if p.get('pb') and p['pb'] > 0]
        roe_values = [p['detail'].get('roe', 0) for p in scored_peers if p['detail'].get('roe')]
        mv_values = [p.get('total_mv', 0) for p in scored_peers if p.get('total_mv')]

        stats['key_metrics'] = {
            'median_pe': round(np.median(pe_values), 2) if pe_values else None,
            'median_pb': round(np.median(pb_values), 2) if pb_values else None,
            'median_roe': round(np.median(roe_values), 2) if roe_values else None,
            'median_mv': round(np.median(mv_values), 2) if mv_values else None,
            'total_mv': round(sum(mv_values), 2) if mv_values else None,
        }

        return stats

    def _generate_conclusion(self, target: Optional[Dict], scored_peers: List[Dict],
                              industry_stats: Dict, industry: str) -> Dict:
        """生成对比结论"""
        if not target:
            return {'summary': '目标股票数据不可用', 'highlights': [], 'risks': []}

        rank = target.get('rank', 0)
        total_count = len(scored_peers)
        scores = target.get('scores', {})
        total_score = scores.get('total', 50)

        highlights = []
        risks = []
        dim_names = {'valuation': '估值', 'profitability': '盈利', 'growth': '成长',
                     'capital_flow': '资金流', 'technical': '技术面'}

        # 分析各维度在行业中的位置
        for dim, name in dim_names.items():
            dim_score = scores.get(dim, 50)
            dim_median = industry_stats.get(dim, {}).get('median', 50)

            if dim_score >= dim_median + 15:
                highlights.append(f'{name}维度({dim_score}分)显著优于行业中位数({dim_median}分)')
            elif dim_score <= dim_median - 15:
                risks.append(f'{name}维度({dim_score}分)明显低于行业中位数({dim_median}分)')

        # 综合评价
        if rank <= 3:
            position = '行业领先'
            advice = '综合实力在行业中处于领先地位，可重点关注'
        elif rank <= total_count * 0.3:
            position = '行业中上'
            advice = '综合表现优于多数同行，具有一定竞争优势'
        elif rank <= total_count * 0.7:
            position = '行业中游'
            advice = '综合表现处于行业中等水平，需关注边际变化'
        else:
            position = '行业落后'
            advice = '综合表现在行业中偏弱，需谨慎评估投资价值'

        summary = (f"{target.get('name', '')}({target.get('code', '')}) "
                   f"在{industry}行业{total_count}家公司中排名第{rank}，"
                   f"综合评分{total_score}分，处于{position}水平。{advice}")

        return {
            'summary': summary,
            'rank': rank,
            'total_count': total_count,
            'position': position,
            'total_score': total_score,
            'highlights': highlights,
            'risks': risks,
            'advice': advice,
        }

    # ========== 工具方法 ==========

    def _safe_float(self, value, default=0.0):
        """安全浮点数转换"""
        try:
            if pd.isna(value):
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _get_cache(self, key):
        """获取缓存"""
        if key in self.data_cache:
            cache_time, data = self.data_cache[key]
            if (datetime.now() - cache_time).total_seconds() < self.cache_ttl:
                return data
        return None

    def _set_cache(self, key, data):
        """设置缓存"""
        self.data_cache[key] = (datetime.now(), data)
