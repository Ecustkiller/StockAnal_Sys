# -*- coding: utf-8 -*-
"""
自选股管理系统
支持分组管理、批量监控、一键分析、实时行情刷新
"""
import traceback
from datetime import datetime
from database import get_session, WatchlistGroup, WatchlistStock, init_db
import logging

logger = logging.getLogger(__name__)


class WatchlistManager:
    """自选股管理器"""

    def __init__(self, analyzer=None):
        self.analyzer = analyzer
        init_db()

    # ==================== 分组管理 ====================

    def create_group(self, name, description='', color='#4e73df'):
        """创建自选股分组"""
        session = get_session()
        try:
            # 检查同名分组
            existing = session.query(WatchlistGroup).filter_by(name=name).first()
            if existing:
                return {'error': f'分组"{name}"已存在', 'group': existing.to_dict()}

            max_order = session.query(WatchlistGroup).count()
            group = WatchlistGroup(
                name=name,
                description=description,
                color=color,
                sort_order=max_order
            )
            session.add(group)
            session.commit()
            return {'success': True, 'group': group.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"创建分组失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def get_groups(self):
        """获取所有分组"""
        session = get_session()
        try:
            groups = session.query(WatchlistGroup).order_by(WatchlistGroup.sort_order).all()
            return [g.to_dict() for g in groups]
        except Exception as e:
            logger.error(f"获取分组失败: {e}")
            return []
        finally:
            session.close()

    def update_group(self, group_id, **kwargs):
        """更新分组信息"""
        session = get_session()
        try:
            group = session.query(WatchlistGroup).get(group_id)
            if not group:
                return {'error': '分组不存在'}
            for key in ['name', 'description', 'color', 'sort_order']:
                if key in kwargs:
                    setattr(group, key, kwargs[key])
            session.commit()
            return {'success': True, 'group': group.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"更新分组失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def delete_group(self, group_id):
        """删除分组（级联删除其中的股票）"""
        session = get_session()
        try:
            group = session.query(WatchlistGroup).get(group_id)
            if not group:
                return {'error': '分组不存在'}
            name = group.name
            session.delete(group)
            session.commit()
            return {'success': True, 'message': f'分组"{name}"已删除'}
        except Exception as e:
            session.rollback()
            logger.error(f"删除分组失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 股票管理 ====================

    def add_stock(self, group_id, stock_code, stock_name='', market_type='A',
                  cost_price=0, target_price=0, stop_loss_price=0, notes=''):
        """添加股票到分组"""
        session = get_session()
        try:
            group = session.query(WatchlistGroup).get(group_id)
            if not group:
                return {'error': '分组不存在'}

            # 检查是否已在该分组中
            existing = session.query(WatchlistStock).filter_by(
                group_id=group_id, stock_code=stock_code
            ).first()
            if existing:
                return {'error': f'{stock_code}已在分组"{group.name}"中', 'stock': existing.to_dict()}

            # 如果没有提供股票名称，尝试获取
            if not stock_name and self.analyzer:
                try:
                    info = self.analyzer.get_stock_info(stock_code)
                    stock_name = info.get('股票名称', stock_code)
                except:
                    stock_name = stock_code

            max_order = session.query(WatchlistStock).filter_by(group_id=group_id).count()
            stock = WatchlistStock(
                group_id=group_id,
                stock_code=stock_code,
                stock_name=stock_name,
                market_type=market_type,
                cost_price=cost_price,
                target_price=target_price,
                stop_loss_price=stop_loss_price,
                notes=notes,
                sort_order=max_order
            )
            session.add(stock)
            session.commit()
            return {'success': True, 'stock': stock.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"添加股票失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def remove_stock(self, stock_id):
        """从分组中移除股票"""
        session = get_session()
        try:
            stock = session.query(WatchlistStock).get(stock_id)
            if not stock:
                return {'error': '股票不存在'}
            code = stock.stock_code
            session.delete(stock)
            session.commit()
            return {'success': True, 'message': f'{code}已移除'}
        except Exception as e:
            session.rollback()
            logger.error(f"移除股票失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def update_stock(self, stock_id, **kwargs):
        """更新股票信息（成本价、目标价、止损价、备注等）"""
        session = get_session()
        try:
            stock = session.query(WatchlistStock).get(stock_id)
            if not stock:
                return {'error': '股票不存在'}
            for key in ['cost_price', 'target_price', 'stop_loss_price', 'notes', 'sort_order', 'group_id']:
                if key in kwargs:
                    setattr(stock, key, kwargs[key])
            session.commit()
            return {'success': True, 'stock': stock.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"更新股票失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def move_stock(self, stock_id, target_group_id):
        """移动股票到另一个分组"""
        return self.update_stock(stock_id, group_id=target_group_id)

    def get_stocks_by_group(self, group_id):
        """获取分组中的所有股票"""
        session = get_session()
        try:
            stocks = session.query(WatchlistStock).filter_by(
                group_id=group_id
            ).order_by(WatchlistStock.sort_order).all()
            return [s.to_dict() for s in stocks]
        except Exception as e:
            logger.error(f"获取分组股票失败: {e}")
            return []
        finally:
            session.close()

    def get_all_stocks(self):
        """获取所有自选股"""
        session = get_session()
        try:
            stocks = session.query(WatchlistStock).order_by(
                WatchlistStock.group_id, WatchlistStock.sort_order
            ).all()
            return [s.to_dict() for s in stocks]
        except Exception as e:
            logger.error(f"获取所有自选股失败: {e}")
            return []
        finally:
            session.close()

    def search_stocks(self, keyword):
        """搜索自选股"""
        session = get_session()
        try:
            stocks = session.query(WatchlistStock).filter(
                (WatchlistStock.stock_code.like(f'%{keyword}%')) |
                (WatchlistStock.stock_name.like(f'%{keyword}%'))
            ).all()
            return [s.to_dict() for s in stocks]
        except Exception as e:
            logger.error(f"搜索自选股失败: {e}")
            return []
        finally:
            session.close()

    # ==================== 批量监控 ====================

    def batch_get_realtime(self, group_id=None):
        """批量获取自选股实时行情"""
        session = get_session()
        try:
            if group_id:
                stocks = session.query(WatchlistStock).filter_by(group_id=group_id).all()
            else:
                stocks = session.query(WatchlistStock).all()

            if not stocks:
                return []

            results = []
            for stock in stocks:
                item = stock.to_dict()
                # 获取实时行情
                if self.analyzer:
                    try:
                        df = self.analyzer.get_stock_data(stock.stock_code, stock.market_type)
                        if df is not None and len(df) > 0:
                            latest = df.iloc[-1]
                            prev_close = df.iloc[-2]['close'] if len(df) > 1 else latest['close']
                            current_price = float(latest['close'])
                            change_pct = (current_price - prev_close) / prev_close * 100

                            item['current_price'] = round(current_price, 2)
                            item['change_pct'] = round(change_pct, 2)
                            item['volume'] = float(latest.get('volume', 0))
                            item['high'] = float(latest.get('high', 0))
                            item['low'] = float(latest.get('low', 0))
                            item['open'] = float(latest.get('open', 0))

                            # 计算盈亏（如果有成本价）
                            if stock.cost_price and stock.cost_price > 0:
                                item['profit_pct'] = round(
                                    (current_price - stock.cost_price) / stock.cost_price * 100, 2
                                )
                            else:
                                item['profit_pct'] = None

                            # 检查是否触及目标价/止损价
                            item['hit_target'] = (stock.target_price > 0 and current_price >= stock.target_price)
                            item['hit_stop_loss'] = (stock.stop_loss_price > 0 and current_price <= stock.stop_loss_price)
                        else:
                            item['current_price'] = None
                            item['change_pct'] = None
                            item['error'] = '数据获取失败'
                    except Exception as e:
                        item['current_price'] = None
                        item['change_pct'] = None
                        item['error'] = str(e)
                results.append(item)

            # 按涨跌幅排序
            results.sort(key=lambda x: x.get('change_pct') or 0, reverse=True)
            return results
        except Exception as e:
            logger.error(f"批量获取行情失败: {traceback.format_exc()}")
            return []
        finally:
            session.close()

    def batch_quick_score(self, group_id=None):
        """批量快速评分"""
        session = get_session()
        try:
            if group_id:
                stocks = session.query(WatchlistStock).filter_by(group_id=group_id).all()
            else:
                stocks = session.query(WatchlistStock).all()

            if not stocks or not self.analyzer:
                return []

            results = []
            for stock in stocks:
                item = {'stock_code': stock.stock_code, 'stock_name': stock.stock_name}
                try:
                    df = self.analyzer.get_stock_data(stock.stock_code, stock.market_type)
                    df = self.analyzer.calculate_indicators(df)
                    score_details = self.analyzer.calculate_score(df, stock.stock_code)
                    item['total_score'] = score_details.get('total_score', 0)
                    item['recommendation'] = score_details.get('recommendation', '观望')
                    item['score_details'] = score_details
                except Exception as e:
                    item['total_score'] = 0
                    item['recommendation'] = '数据异常'
                    item['error'] = str(e)
                results.append(item)

            # 按评分排序
            results.sort(key=lambda x: x.get('total_score', 0), reverse=True)
            return results
        except Exception as e:
            logger.error(f"批量评分失败: {traceback.format_exc()}")
            return []
        finally:
            session.close()

    # ==================== 统计信息 ====================

    def get_overview(self):
        """获取自选股概览统计"""
        session = get_session()
        try:
            total_groups = session.query(WatchlistGroup).count()
            total_stocks = session.query(WatchlistStock).count()
            groups = session.query(WatchlistGroup).order_by(WatchlistGroup.sort_order).all()

            group_stats = []
            for g in groups:
                stock_count = session.query(WatchlistStock).filter_by(group_id=g.id).count()
                group_stats.append({
                    'id': g.id,
                    'name': g.name,
                    'color': g.color,
                    'stock_count': stock_count
                })

            return {
                'total_groups': total_groups,
                'total_stocks': total_stocks,
                'groups': group_stats
            }
        except Exception as e:
            logger.error(f"获取概览失败: {e}")
            return {'total_groups': 0, 'total_stocks': 0, 'groups': []}
        finally:
            session.close()

    def ensure_default_group(self):
        """确保存在默认分组"""
        session = get_session()
        try:
            default = session.query(WatchlistGroup).filter_by(name='默认分组').first()
            if not default:
                default = WatchlistGroup(
                    name='默认分组',
                    description='默认自选股分组',
                    color='#4e73df',
                    sort_order=0
                )
                session.add(default)
                session.commit()
            return default.id
        except Exception as e:
            session.rollback()
            logger.error(f"创建默认分组失败: {e}")
            return None
        finally:
            session.close()
