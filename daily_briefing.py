# -*- coding: utf-8 -*-
"""
每日市场简报系统
收盘后自动生成：大盘总结 + 板块轮动 + 资金流向 + 涨停分析 + 自选股异动
支持定时生成和手动触发
"""
import threading
import time
import traceback
from datetime import datetime, timedelta
from database import get_session, DailyBrief, WatchlistStock, init_db
import logging
import akshare as ak
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DailyBriefing:
    """每日市场简报生成器"""

    def __init__(self, analyzer=None):
        self.analyzer = analyzer
        self._scheduler_thread = None
        self._running = False
        init_db()

    # ==================== 简报生成 ====================

    def generate_brief(self, target_date=None):
        """生成每日市场简报"""
        if not target_date:
            target_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"开始生成 {target_date} 市场简报...")

        try:
            # 1. 大盘总结
            market_summary = self._generate_market_summary()

            # 2. 板块轮动
            sector_rotation = self._generate_sector_rotation()

            # 3. 资金流向
            capital_flow = self._generate_capital_flow()

            # 4. 涨停分析
            limit_up_analysis = self._generate_limit_up_analysis()

            # 5. 自选股异动
            watchlist_alerts = self._generate_watchlist_alerts()

            # 6. 生成完整简报文本
            full_report = self._compose_full_report(
                target_date, market_summary, sector_rotation,
                capital_flow, limit_up_analysis, watchlist_alerts
            )

            # 保存到数据库
            session = get_session()
            try:
                existing = session.query(DailyBrief).filter_by(brief_date=target_date).first()
                if existing:
                    existing.market_summary = market_summary
                    existing.sector_rotation = sector_rotation
                    existing.capital_flow = capital_flow
                    existing.limit_up_analysis = limit_up_analysis
                    existing.watchlist_alerts = watchlist_alerts
                    existing.full_report = full_report
                else:
                    brief = DailyBrief(
                        brief_date=target_date,
                        market_summary=market_summary,
                        sector_rotation=sector_rotation,
                        capital_flow=capital_flow,
                        limit_up_analysis=limit_up_analysis,
                        watchlist_alerts=watchlist_alerts,
                        full_report=full_report
                    )
                    session.add(brief)
                session.commit()
                logger.info(f"✅ {target_date} 市场简报已生成并保存")
            except Exception as e:
                session.rollback()
                logger.error(f"保存简报失败: {e}")
            finally:
                session.close()

            return {
                'success': True,
                'brief_date': target_date,
                'market_summary': market_summary,
                'sector_rotation': sector_rotation,
                'capital_flow': capital_flow,
                'limit_up_analysis': limit_up_analysis,
                'watchlist_alerts': watchlist_alerts,
                'full_report': full_report
            }

        except Exception as e:
            logger.error(f"生成简报失败: {traceback.format_exc()}")
            return {'error': f'生成简报失败: {str(e)}'}

    def _generate_market_summary(self):
        """生成大盘总结"""
        summary = {
            'indices': [],
            'market_breadth': {},
            'volume_analysis': {},
            'overall_sentiment': '中性'
        }

        try:
            # 获取主要指数行情
            indices_map = {
                'sh000001': '上证指数',
                'sz399001': '深证成指',
                'sz399006': '创业板指',
                'sh000688': '科创50',
                'sh000300': '沪深300',
                'sh000905': '中证500'
            }

            for code, name in indices_map.items():
                try:
                    if self.analyzer:
                        df = self.analyzer.get_stock_data(code, 'A')
                        if df is not None and len(df) >= 2:
                            latest = df.iloc[-1]
                            prev = df.iloc[-2]
                            close = float(latest['close'])
                            prev_close = float(prev['close'])
                            change = close - prev_close
                            change_pct = change / prev_close * 100
                            volume = float(latest.get('volume', 0))

                            summary['indices'].append({
                                'code': code,
                                'name': name,
                                'close': round(close, 2),
                                'change': round(change, 2),
                                'change_pct': round(change_pct, 2),
                                'volume': volume
                            })
                except Exception as e:
                    logger.warning(f"获取指数 {code} 失败: {e}")

            # 市场涨跌家数
            try:
                df_market = ak.stock_zh_a_spot_em()
                if df_market is not None and len(df_market) > 0:
                    up_count = len(df_market[df_market['涨跌幅'] > 0])
                    down_count = len(df_market[df_market['涨跌幅'] < 0])
                    flat_count = len(df_market[df_market['涨跌幅'] == 0])
                    limit_up = len(df_market[df_market['涨跌幅'] >= 9.9])
                    limit_down = len(df_market[df_market['涨跌幅'] <= -9.9])
                    total = len(df_market)
                    avg_change = float(df_market['涨跌幅'].mean())

                    summary['market_breadth'] = {
                        'total': total,
                        'up_count': up_count,
                        'down_count': down_count,
                        'flat_count': flat_count,
                        'limit_up': limit_up,
                        'limit_down': limit_down,
                        'up_ratio': round(up_count / total * 100, 1) if total > 0 else 0,
                        'avg_change': round(avg_change, 2)
                    }

                    # 判断整体情绪
                    up_ratio = up_count / total * 100 if total > 0 else 50
                    if up_ratio > 70 and limit_up > 30:
                        summary['overall_sentiment'] = '极度乐观'
                    elif up_ratio > 55:
                        summary['overall_sentiment'] = '偏多'
                    elif up_ratio < 30 and limit_down > 20:
                        summary['overall_sentiment'] = '极度悲观'
                    elif up_ratio < 45:
                        summary['overall_sentiment'] = '偏空'
                    else:
                        summary['overall_sentiment'] = '中性震荡'

                    # 成交额分析
                    total_amount = float(df_market['成交额'].sum()) / 1e8  # 亿元
                    summary['volume_analysis'] = {
                        'total_amount': round(total_amount, 2),
                        'amount_level': '放量' if total_amount > 12000 else '缩量' if total_amount < 7000 else '正常'
                    }
            except Exception as e:
                logger.warning(f"获取市场涨跌数据失败: {e}")

        except Exception as e:
            logger.error(f"生成大盘总结失败: {e}")

        return summary

    def _generate_sector_rotation(self):
        """生成板块轮动分析"""
        rotation = {'top_sectors': [], 'bottom_sectors': [], 'hot_concepts': []}

        try:
            # 行业板块涨跌
            try:
                df_sector = ak.stock_board_industry_name_em()
                if df_sector is not None and len(df_sector) > 0:
                    # 获取行业板块行情
                    sector_data = []
                    for _, row in df_sector.head(50).iterrows():
                        try:
                            name = row.get('板块名称', '')
                            change = row.get('涨跌幅', 0)
                            if name and change is not None:
                                sector_data.append({
                                    'name': name,
                                    'change_pct': round(float(change), 2)
                                })
                        except:
                            continue

                    if sector_data:
                        sector_data.sort(key=lambda x: x['change_pct'], reverse=True)
                        rotation['top_sectors'] = sector_data[:10]
                        rotation['bottom_sectors'] = sector_data[-10:][::-1]
            except Exception as e:
                logger.warning(f"获取行业板块失败: {e}")

            # 概念板块热点
            try:
                df_concept = ak.stock_board_concept_name_em()
                if df_concept is not None and len(df_concept) > 0:
                    concept_data = []
                    for _, row in df_concept.head(30).iterrows():
                        try:
                            name = row.get('板块名称', '')
                            change = row.get('涨跌幅', 0)
                            if name and change is not None:
                                concept_data.append({
                                    'name': name,
                                    'change_pct': round(float(change), 2)
                                })
                        except:
                            continue

                    if concept_data:
                        concept_data.sort(key=lambda x: x['change_pct'], reverse=True)
                        rotation['hot_concepts'] = concept_data[:10]
            except Exception as e:
                logger.warning(f"获取概念板块失败: {e}")

        except Exception as e:
            logger.error(f"生成板块轮动失败: {e}")

        return rotation

    def _generate_capital_flow(self):
        """生成资金流向分析"""
        flow = {'north_money': {}, 'sector_flow': [], 'main_flow': {}}

        try:
            # 北向资金
            try:
                df_north = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
                if df_north is not None and len(df_north) > 0:
                    latest = df_north.iloc[-1]
                    flow['north_money'] = {
                        'date': str(latest.get('date', '')),
                        'net_inflow': round(float(latest.get('value', 0)), 2),
                        'trend': '流入' if float(latest.get('value', 0)) > 0 else '流出'
                    }
                    # 近5日累计
                    if len(df_north) >= 5:
                        recent_5d = float(df_north.tail(5)['value'].sum())
                        flow['north_money']['recent_5d'] = round(recent_5d, 2)
                        flow['north_money']['recent_5d_trend'] = '持续流入' if recent_5d > 0 else '持续流出'
            except Exception as e:
                logger.warning(f"获取北向资金失败: {e}")

            # 行业资金流向
            try:
                df_flow = ak.stock_individual_fund_flow_rank(indicator="今日")
                if df_flow is not None and len(df_flow) > 0:
                    # 主力净流入TOP10
                    top_inflow = df_flow.nlargest(10, '主力净流入-净额')
                    for _, row in top_inflow.iterrows():
                        try:
                            flow['sector_flow'].append({
                                'name': row.get('名称', ''),
                                'code': row.get('代码', ''),
                                'main_net_inflow': round(float(row.get('主力净流入-净额', 0)) / 1e4, 2),  # 万元转亿
                                'change_pct': round(float(row.get('涨跌幅', 0)), 2)
                            })
                        except:
                            continue

                    # 整体主力资金统计
                    total_main_in = float(df_flow['主力净流入-净额'].sum()) / 1e8
                    flow['main_flow'] = {
                        'total_main_net': round(total_main_in, 2),
                        'trend': '净流入' if total_main_in > 0 else '净流出'
                    }
            except Exception as e:
                logger.warning(f"获取资金流向失败: {e}")

        except Exception as e:
            logger.error(f"生成资金流向失败: {e}")

        return flow

    def _generate_limit_up_analysis(self):
        """生成涨停分析"""
        analysis = {'limit_up_count': 0, 'limit_down_count': 0, 'top_limit_up': [], 'themes': []}

        try:
            df_market = ak.stock_zh_a_spot_em()
            if df_market is not None and len(df_market) > 0:
                # 涨停股
                limit_up_df = df_market[df_market['涨跌幅'] >= 9.9].copy()
                limit_down_df = df_market[df_market['涨跌幅'] <= -9.9].copy()

                analysis['limit_up_count'] = len(limit_up_df)
                analysis['limit_down_count'] = len(limit_down_df)

                # 涨停股详情（按成交额排序）
                if len(limit_up_df) > 0:
                    limit_up_df = limit_up_df.sort_values('成交额', ascending=False)
                    for _, row in limit_up_df.head(20).iterrows():
                        try:
                            analysis['top_limit_up'].append({
                                'code': str(row.get('代码', '')),
                                'name': str(row.get('名称', '')),
                                'close': round(float(row.get('最新价', 0)), 2),
                                'amount': round(float(row.get('成交额', 0)) / 1e8, 2),  # 亿元
                                'turnover': round(float(row.get('换手率', 0)), 2)
                            })
                        except:
                            continue

                # 涨停封板率估算
                if analysis['limit_up_count'] > 0:
                    # 简单估算：涨停数 / (涨停数 + 曾涨停但炸板数)
                    analysis['seal_rate'] = f"约{analysis['limit_up_count']}只涨停"

        except Exception as e:
            logger.error(f"生成涨停分析失败: {e}")

        return analysis

    def _generate_watchlist_alerts(self):
        """生成自选股异动提醒"""
        alerts = []

        session = get_session()
        try:
            stocks = session.query(WatchlistStock).all()
            if not stocks or not self.analyzer:
                return alerts

            for stock in stocks:
                try:
                    df = self.analyzer.get_stock_data(stock.stock_code, stock.market_type)
                    if df is None or len(df) < 2:
                        continue

                    latest = df.iloc[-1]
                    prev = df.iloc[-2]
                    current_price = float(latest['close'])
                    prev_close = float(prev['close'])
                    change_pct = (current_price - prev_close) / prev_close * 100
                    volume = float(latest.get('volume', 0))
                    avg_vol = float(df['volume'].tail(20).mean()) if len(df) >= 20 else float(df['volume'].mean())
                    vol_ratio = volume / avg_vol if avg_vol > 0 else 1

                    alert_reasons = []

                    # 大幅波动
                    if abs(change_pct) >= 5:
                        alert_reasons.append(f"{'大涨' if change_pct > 0 else '大跌'} {change_pct:.2f}%")

                    # 成交量异常
                    if vol_ratio >= 2.5:
                        alert_reasons.append(f"成交量暴增 {vol_ratio:.1f}倍")

                    # 触及止损/目标价
                    if stock.stop_loss_price > 0 and current_price <= stock.stop_loss_price:
                        alert_reasons.append(f"触及止损价 {stock.stop_loss_price}")
                    if stock.target_price > 0 and current_price >= stock.target_price:
                        alert_reasons.append(f"达到目标价 {stock.target_price}")

                    if alert_reasons:
                        alerts.append({
                            'stock_code': stock.stock_code,
                            'stock_name': stock.stock_name,
                            'current_price': round(current_price, 2),
                            'change_pct': round(change_pct, 2),
                            'vol_ratio': round(vol_ratio, 1),
                            'reasons': alert_reasons
                        })

                except Exception as e:
                    logger.warning(f"检查自选股 {stock.stock_code} 异动失败: {e}")

        except Exception as e:
            logger.error(f"生成自选股异动失败: {e}")
        finally:
            session.close()

        # 按涨跌幅绝对值排序
        alerts.sort(key=lambda x: abs(x.get('change_pct', 0)), reverse=True)
        return alerts

    def _compose_full_report(self, date, market_summary, sector_rotation,
                              capital_flow, limit_up_analysis, watchlist_alerts):
        """组合生成完整简报文本"""
        lines = []
        lines.append(f"📊 {date} 市场简报")
        lines.append("=" * 50)

        # 大盘总结
        lines.append("\n【一、大盘总结】")
        sentiment = market_summary.get('overall_sentiment', '中性')
        lines.append(f"市场情绪：{sentiment}")

        for idx in market_summary.get('indices', []):
            arrow = '↑' if idx['change_pct'] > 0 else '↓' if idx['change_pct'] < 0 else '→'
            color_sign = '+' if idx['change_pct'] > 0 else ''
            lines.append(f"  {idx['name']}: {idx['close']} {arrow} {color_sign}{idx['change_pct']}%")

        breadth = market_summary.get('market_breadth', {})
        if breadth:
            lines.append(f"  涨跌家数: 上涨{breadth.get('up_count', 0)} / 下跌{breadth.get('down_count', 0)} / 平盘{breadth.get('flat_count', 0)}")
            lines.append(f"  涨停{breadth.get('limit_up', 0)}家 / 跌停{breadth.get('limit_down', 0)}家")
            lines.append(f"  个股平均涨幅: {breadth.get('avg_change', 0)}%")

        vol = market_summary.get('volume_analysis', {})
        if vol:
            lines.append(f"  两市成交额: {vol.get('total_amount', 0)}亿（{vol.get('amount_level', '正常')}）")

        # 板块轮动
        lines.append("\n【二、板块轮动】")
        top_sectors = sector_rotation.get('top_sectors', [])
        if top_sectors:
            lines.append("  领涨行业:")
            for s in top_sectors[:5]:
                lines.append(f"    🔴 {s['name']}: +{s['change_pct']}%")

        bottom_sectors = sector_rotation.get('bottom_sectors', [])
        if bottom_sectors:
            lines.append("  领跌行业:")
            for s in bottom_sectors[:5]:
                lines.append(f"    🟢 {s['name']}: {s['change_pct']}%")

        hot_concepts = sector_rotation.get('hot_concepts', [])
        if hot_concepts:
            lines.append("  热门概念:")
            for c in hot_concepts[:5]:
                lines.append(f"    🔥 {c['name']}: +{c['change_pct']}%")

        # 资金流向
        lines.append("\n【三、资金流向】")
        north = capital_flow.get('north_money', {})
        if north:
            lines.append(f"  北向资金: 今日净{north.get('trend', '流入')} {abs(north.get('net_inflow', 0))}亿")
            if 'recent_5d' in north:
                lines.append(f"  近5日累计: {north.get('recent_5d_trend', '')} {abs(north.get('recent_5d', 0))}亿")

        main_flow = capital_flow.get('main_flow', {})
        if main_flow:
            lines.append(f"  主力资金: {main_flow.get('trend', '')} {abs(main_flow.get('total_main_net', 0))}亿")

        # 涨停分析
        lines.append("\n【四、涨停分析】")
        lines.append(f"  涨停: {limit_up_analysis.get('limit_up_count', 0)}家 / 跌停: {limit_up_analysis.get('limit_down_count', 0)}家")
        top_lu = limit_up_analysis.get('top_limit_up', [])
        if top_lu:
            lines.append("  涨停龙头（按成交额）:")
            for s in top_lu[:10]:
                lines.append(f"    {s['code']} {s['name']} 成交{s['amount']}亿 换手{s['turnover']}%")

        # 自选股异动
        if watchlist_alerts:
            lines.append("\n【五、自选股异动】")
            for a in watchlist_alerts:
                reasons = '、'.join(a['reasons'])
                lines.append(f"  ⚡ {a['stock_code']}({a['stock_name']}) {a['current_price']} ({a['change_pct']:+.2f}%) - {reasons}")

        lines.append("\n" + "=" * 50)
        lines.append("以上数据仅供参考，不构成投资建议。")

        return '\n'.join(lines)

    # ==================== 查询历史简报 ====================

    def get_brief(self, brief_date):
        """获取指定日期的简报"""
        session = get_session()
        try:
            brief = session.query(DailyBrief).filter_by(brief_date=brief_date).first()
            if brief:
                return brief.to_dict()
            return {'error': f'{brief_date} 的简报不存在'}
        except Exception as e:
            logger.error(f"获取简报失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def get_recent_briefs(self, limit=7):
        """获取最近的简报列表"""
        session = get_session()
        try:
            briefs = session.query(DailyBrief).order_by(
                DailyBrief.brief_date.desc()
            ).limit(limit).all()
            return [b.to_dict() for b in briefs]
        except Exception as e:
            logger.error(f"获取简报列表失败: {e}")
            return []
        finally:
            session.close()

    # ==================== 定时任务 ====================

    def start_scheduler(self):
        """启动定时简报生成（每日15:30自动生成）"""
        if self._running:
            logger.info("简报定时任务已在运行中")
            return

        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        logger.info("✅ 每日简报定时任务已启动（每日15:30自动生成）")

    def stop_scheduler(self):
        """停止定时任务"""
        self._running = False
        logger.info("每日简报定时任务已停止")

    def _scheduler_loop(self):
        """定时任务循环"""
        last_generated_date = None
        while self._running:
            try:
                now = datetime.now()
                today = now.strftime('%Y-%m-%d')
                weekday = now.weekday()

                # 周一到周五，15:30后生成
                if (weekday < 5 and now.hour == 15 and now.minute >= 30
                        and last_generated_date != today):
                    logger.info(f"定时生成 {today} 市场简报...")
                    result = self.generate_brief(today)
                    if result.get('success'):
                        last_generated_date = today
                        logger.info(f"✅ {today} 市场简报已自动生成")
                    else:
                        logger.error(f"自动生成简报失败: {result.get('error')}")

            except Exception as e:
                logger.error(f"简报定时任务异常: {traceback.format_exc()}")

            time.sleep(60)  # 每分钟检查一次
