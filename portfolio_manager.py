# -*- coding: utf-8 -*-
"""
投资组合后端管理系统
支持：账户管理、持仓管理、交易记录、盈亏计算、风险归因、再平衡建议
"""
import traceback
from datetime import datetime, timedelta
from database import get_session, PortfolioAccount, PortfolioHolding, PortfolioTransaction, init_db
import logging
import numpy as np

logger = logging.getLogger(__name__)


class PortfolioManager:
    """投资组合管理器"""

    def __init__(self, analyzer=None):
        self.analyzer = analyzer
        init_db()

    # ==================== 账户管理 ====================

    def create_account(self, name, initial_capital=0, description=''):
        """创建投资组合账户"""
        session = get_session()
        try:
            existing = session.query(PortfolioAccount).filter_by(name=name).first()
            if existing:
                return {'error': f'账户"{name}"已存在', 'account': existing.to_dict()}

            account = PortfolioAccount(
                name=name,
                description=description,
                initial_capital=initial_capital,
                cash_balance=initial_capital
            )
            session.add(account)
            session.commit()
            return {'success': True, 'account': account.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"创建账户失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def get_accounts(self):
        """获取所有账户"""
        session = get_session()
        try:
            accounts = session.query(PortfolioAccount).order_by(PortfolioAccount.created_at).all()
            return [a.to_dict() for a in accounts]
        except Exception as e:
            logger.error(f"获取账户失败: {e}")
            return []
        finally:
            session.close()

    def get_account(self, account_id):
        """获取单个账户详情"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}
            return account.to_dict()
        except Exception as e:
            logger.error(f"获取账户失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def delete_account(self, account_id):
        """删除账户（级联删除持仓和交易记录）"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}
            name = account.name
            session.delete(account)
            session.commit()
            return {'success': True, 'message': f'账户"{name}"已删除'}
        except Exception as e:
            session.rollback()
            logger.error(f"删除账户失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 交易操作 ====================

    def buy(self, account_id, stock_code, quantity, price, stock_name='',
            market_type='A', commission=0, tax=0, notes='', trade_date=None):
        """买入股票"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}

            total_cost = quantity * price + commission + tax
            if account.cash_balance < total_cost:
                return {'error': f'现金余额不足，需要 {total_cost:.2f}，当前余额 {account.cash_balance:.2f}'}

            # 如果没有提供股票名称，尝试获取
            if not stock_name and self.analyzer:
                try:
                    info = self.analyzer.get_stock_info(stock_code)
                    stock_name = info.get('股票名称', stock_code)
                except:
                    pass

            # 记录交易
            tx = PortfolioTransaction(
                account_id=account_id,
                stock_code=stock_code,
                stock_name=stock_name,
                market_type=market_type,
                action='buy',
                quantity=quantity,
                price=price,
                commission=commission,
                tax=tax,
                notes=notes,
                trade_date=trade_date or datetime.now()
            )
            session.add(tx)

            # 更新持仓
            holding = session.query(PortfolioHolding).filter_by(
                account_id=account_id, stock_code=stock_code
            ).first()

            if holding:
                # 加仓：计算新的平均成本
                total_qty = holding.quantity + quantity
                total_cost_val = holding.quantity * holding.avg_cost + quantity * price
                holding.avg_cost = total_cost_val / total_qty if total_qty > 0 else 0
                holding.quantity = total_qty
                holding.stock_name = stock_name or holding.stock_name
            else:
                # 新建持仓
                holding = PortfolioHolding(
                    account_id=account_id,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    market_type=market_type,
                    quantity=quantity,
                    avg_cost=price
                )
                session.add(holding)

            # 扣减现金
            account.cash_balance -= total_cost

            session.commit()
            return {'success': True, 'message': f'买入 {stock_code} {quantity}股 @ {price}', 'holding': holding.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"买入失败: {traceback.format_exc()}")
            return {'error': str(e)}
        finally:
            session.close()

    def sell(self, account_id, stock_code, quantity, price,
             commission=0, tax=0, notes='', trade_date=None):
        """卖出股票"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}

            holding = session.query(PortfolioHolding).filter_by(
                account_id=account_id, stock_code=stock_code
            ).first()

            if not holding:
                return {'error': f'未持有 {stock_code}'}
            if holding.quantity < quantity:
                return {'error': f'持仓不足，当前持有 {holding.quantity}股，尝试卖出 {quantity}股'}

            # 记录交易
            tx = PortfolioTransaction(
                account_id=account_id,
                stock_code=stock_code,
                stock_name=holding.stock_name,
                market_type=holding.market_type,
                action='sell',
                quantity=quantity,
                price=price,
                commission=commission,
                tax=tax,
                notes=notes,
                trade_date=trade_date or datetime.now()
            )
            session.add(tx)

            # 更新持仓
            holding.quantity -= quantity
            if holding.quantity == 0:
                session.delete(holding)

            # 增加现金（卖出所得 - 手续费 - 印花税）
            proceeds = quantity * price - commission - tax
            account.cash_balance += proceeds

            # 计算本次交易盈亏
            profit = (price - holding.avg_cost) * quantity - commission - tax
            profit_pct = (price - holding.avg_cost) / holding.avg_cost * 100 if holding.avg_cost > 0 else 0

            session.commit()
            return {
                'success': True,
                'message': f'卖出 {stock_code} {quantity}股 @ {price}',
                'profit': round(profit, 2),
                'profit_pct': round(profit_pct, 2)
            }
        except Exception as e:
            session.rollback()
            logger.error(f"卖出失败: {traceback.format_exc()}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 持仓查询 ====================

    def get_holdings(self, account_id, refresh_price=True):
        """获取账户持仓（可选刷新实时价格）"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}

            holdings = session.query(PortfolioHolding).filter_by(account_id=account_id).all()

            results = []
            total_market_value = 0
            total_cost_value = 0
            total_profit = 0

            for h in holdings:
                # 刷新实时价格
                if refresh_price and self.analyzer:
                    try:
                        df = self.analyzer.get_stock_data(h.stock_code, h.market_type)
                        if df is not None and len(df) > 0:
                            h.current_price = float(df.iloc[-1]['close'])
                            h.price_updated_at = datetime.now()
                    except Exception as e:
                        logger.warning(f"刷新 {h.stock_code} 价格失败: {e}")

                item = h.to_dict()
                total_market_value += item['market_value']
                total_cost_value += item['cost_value']
                total_profit += item['profit']
                results.append(item)

            if refresh_price:
                session.commit()

            # 计算权重
            for item in results:
                item['weight'] = round(item['market_value'] / total_market_value * 100, 2) if total_market_value > 0 else 0

            total_assets = account.cash_balance + total_market_value
            total_return = total_assets - account.initial_capital
            total_return_pct = (total_return / account.initial_capital * 100) if account.initial_capital > 0 else 0

            return {
                'account': account.to_dict(),
                'holdings': results,
                'summary': {
                    'total_assets': round(total_assets, 2),
                    'cash_balance': round(account.cash_balance, 2),
                    'total_market_value': round(total_market_value, 2),
                    'total_cost_value': round(total_cost_value, 2),
                    'total_profit': round(total_profit, 2),
                    'total_profit_pct': round(total_profit / total_cost_value * 100, 2) if total_cost_value > 0 else 0,
                    'total_return': round(total_return, 2),
                    'total_return_pct': round(total_return_pct, 2),
                    'cash_ratio': round(account.cash_balance / total_assets * 100, 2) if total_assets > 0 else 100,
                    'holding_count': len(results)
                }
            }
        except Exception as e:
            logger.error(f"获取持仓失败: {traceback.format_exc()}")
            return {'error': str(e)}
        finally:
            session.close()

    def get_transactions(self, account_id, limit=100, stock_code=None):
        """获取交易记录"""
        session = get_session()
        try:
            query = session.query(PortfolioTransaction).filter_by(account_id=account_id)
            if stock_code:
                query = query.filter_by(stock_code=stock_code)
            txs = query.order_by(PortfolioTransaction.trade_date.desc()).limit(limit).all()
            return [t.to_dict() for t in txs]
        except Exception as e:
            logger.error(f"获取交易记录失败: {e}")
            return []
        finally:
            session.close()

    # ==================== 风险归因分析 ====================

    def analyze_risk_attribution(self, account_id):
        """投资组合风险归因分析"""
        session = get_session()
        try:
            holdings_data = self.get_holdings(account_id, refresh_price=True)
            if 'error' in holdings_data:
                return holdings_data

            holdings = holdings_data['holdings']
            summary = holdings_data['summary']

            if not holdings:
                return {'error': '持仓为空，无法进行风险归因'}

            # 行业分布
            industry_dist = {}
            # 个股集中度
            stock_weights = []
            # 盈亏贡献
            profit_attribution = []

            for h in holdings:
                weight = h.get('weight', 0)
                stock_weights.append({'stock_code': h['stock_code'], 'stock_name': h['stock_name'], 'weight': weight})

                # 盈亏贡献
                if summary['total_profit'] != 0:
                    contribution = h['profit'] / abs(summary['total_profit']) * 100 if summary['total_profit'] != 0 else 0
                else:
                    contribution = 0
                profit_attribution.append({
                    'stock_code': h['stock_code'],
                    'stock_name': h['stock_name'],
                    'profit': h['profit'],
                    'profit_pct': h['profit_pct'],
                    'contribution': round(contribution, 2)
                })

                # 行业分布
                if self.analyzer:
                    try:
                        info = self.analyzer.get_stock_info(h['stock_code'])
                        industry = info.get('行业', '未知')
                    except:
                        industry = '未知'
                else:
                    industry = '未知'

                if industry not in industry_dist:
                    industry_dist[industry] = {'weight': 0, 'count': 0, 'stocks': []}
                industry_dist[industry]['weight'] += weight
                industry_dist[industry]['count'] += 1
                industry_dist[industry]['stocks'].append(h['stock_code'])

            # 集中度风险评估
            stock_weights.sort(key=lambda x: x['weight'], reverse=True)
            top1_weight = stock_weights[0]['weight'] if stock_weights else 0
            top3_weight = sum(s['weight'] for s in stock_weights[:3])
            hhi = sum((s['weight'] / 100) ** 2 for s in stock_weights) * 10000  # 赫芬达尔指数

            concentration_risk = '低'
            if top1_weight > 40 or hhi > 3000:
                concentration_risk = '高'
            elif top1_weight > 25 or hhi > 2000:
                concentration_risk = '中'

            # 行业集中度
            industry_list = [{'industry': k, **v} for k, v in industry_dist.items()]
            industry_list.sort(key=lambda x: x['weight'], reverse=True)
            max_industry_weight = industry_list[0]['weight'] if industry_list else 0

            industry_risk = '低'
            if max_industry_weight > 50:
                industry_risk = '高'
            elif max_industry_weight > 35:
                industry_risk = '中'

            # 现金比例风险
            cash_ratio = summary['cash_ratio']
            cash_risk = '适中'
            if cash_ratio < 5:
                cash_risk = '偏低（满仓风险）'
            elif cash_ratio > 50:
                cash_risk = '偏高（资金利用率低）'

            return {
                'summary': summary,
                'stock_weights': stock_weights,
                'profit_attribution': sorted(profit_attribution, key=lambda x: x['profit'], reverse=True),
                'industry_distribution': industry_list,
                'risk_metrics': {
                    'top1_weight': round(top1_weight, 2),
                    'top3_weight': round(top3_weight, 2),
                    'hhi': round(hhi, 2),
                    'concentration_risk': concentration_risk,
                    'max_industry': industry_list[0]['industry'] if industry_list else '无',
                    'max_industry_weight': round(max_industry_weight, 2),
                    'industry_risk': industry_risk,
                    'industry_count': len(industry_list),
                    'cash_ratio': round(cash_ratio, 2),
                    'cash_risk': cash_risk
                }
            }
        except Exception as e:
            logger.error(f"风险归因分析失败: {traceback.format_exc()}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 再平衡建议 ====================

    def get_rebalance_suggestions(self, account_id, target_weights=None):
        """生成再平衡建议"""
        session = get_session()
        try:
            holdings_data = self.get_holdings(account_id, refresh_price=True)
            if 'error' in holdings_data:
                return holdings_data

            holdings = holdings_data['holdings']
            summary = holdings_data['summary']

            if not holdings:
                return {'error': '持仓为空'}

            suggestions = []
            total_assets = summary['total_assets']

            if target_weights:
                # 用户指定目标权重
                for h in holdings:
                    code = h['stock_code']
                    current_weight = h['weight']
                    target_weight = target_weights.get(code, current_weight)
                    diff = target_weight - current_weight

                    if abs(diff) > 2:  # 偏差超过2%才建议调整
                        target_value = total_assets * target_weight / 100
                        current_value = h['market_value']
                        adjust_value = target_value - current_value
                        adjust_shares = int(adjust_value / h['current_price'] / 100) * 100 if h['current_price'] > 0 else 0

                        suggestions.append({
                            'stock_code': code,
                            'stock_name': h['stock_name'],
                            'current_weight': round(current_weight, 2),
                            'target_weight': round(target_weight, 2),
                            'diff': round(diff, 2),
                            'action': '加仓' if diff > 0 else '减仓',
                            'adjust_value': round(abs(adjust_value), 2),
                            'adjust_shares': abs(adjust_shares),
                            'current_price': h['current_price']
                        })
            else:
                # 自动生成建议（等权重策略）
                n = len(holdings)
                equal_weight = 100.0 / n if n > 0 else 0
                # 预留10%现金
                stock_weight = 90.0 / n if n > 0 else 0

                for h in holdings:
                    current_weight = h['weight'] * (1 - summary['cash_ratio'] / 100)  # 相对于总资产的权重
                    diff = stock_weight - h['weight']

                    if abs(diff) > 3:
                        target_value = total_assets * stock_weight / 100
                        adjust_value = target_value - h['market_value']
                        adjust_shares = int(adjust_value / h['current_price'] / 100) * 100 if h['current_price'] > 0 else 0

                        suggestions.append({
                            'stock_code': h['stock_code'],
                            'stock_name': h['stock_name'],
                            'current_weight': round(h['weight'], 2),
                            'target_weight': round(stock_weight, 2),
                            'diff': round(diff, 2),
                            'action': '加仓' if diff > 0 else '减仓',
                            'adjust_value': round(abs(adjust_value), 2),
                            'adjust_shares': abs(adjust_shares),
                            'current_price': h['current_price']
                        })

            # 额外建议
            extra_suggestions = []
            risk_data = self.analyze_risk_attribution(account_id)
            if 'risk_metrics' in risk_data:
                metrics = risk_data['risk_metrics']
                if metrics['concentration_risk'] == '高':
                    extra_suggestions.append('⚠️ 个股集中度过高，建议分散持仓，降低单一股票风险')
                if metrics['industry_risk'] == '高':
                    extra_suggestions.append(f'⚠️ 行业集中度过高（{metrics["max_industry"]}占比{metrics["max_industry_weight"]:.0f}%），建议跨行业配置')
                if metrics['cash_risk'] == '偏低（满仓风险）':
                    extra_suggestions.append('⚠️ 现金比例过低，建议保留5-15%现金应对市场波动')
                if metrics['cash_risk'] == '偏高（资金利用率低）':
                    extra_suggestions.append('💡 现金比例偏高，可适当增加持仓提高资金利用率')

            suggestions.sort(key=lambda x: abs(x['diff']), reverse=True)

            return {
                'suggestions': suggestions,
                'extra_suggestions': extra_suggestions,
                'strategy': '等权重平衡' if not target_weights else '自定义权重平衡',
                'total_assets': round(total_assets, 2)
            }
        except Exception as e:
            logger.error(f"再平衡建议失败: {traceback.format_exc()}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 账户资金操作 ====================

    def deposit(self, account_id, amount):
        """入金"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}
            account.cash_balance += amount
            account.initial_capital += amount
            session.commit()
            return {'success': True, 'cash_balance': round(account.cash_balance, 2)}
        except Exception as e:
            session.rollback()
            return {'error': str(e)}
        finally:
            session.close()

    def withdraw(self, account_id, amount):
        """出金"""
        session = get_session()
        try:
            account = session.query(PortfolioAccount).get(account_id)
            if not account:
                return {'error': '账户不存在'}
            if account.cash_balance < amount:
                return {'error': f'现金余额不足，当前余额 {account.cash_balance:.2f}'}
            account.cash_balance -= amount
            account.initial_capital -= amount
            session.commit()
            return {'success': True, 'cash_balance': round(account.cash_balance, 2)}
        except Exception as e:
            session.rollback()
            return {'error': str(e)}
        finally:
            session.close()

    def ensure_default_account(self):
        """确保存在默认账户"""
        session = get_session()
        try:
            default = session.query(PortfolioAccount).filter_by(name='默认账户').first()
            if not default:
                default = PortfolioAccount(
                    name='默认账户',
                    description='系统默认投资组合账户',
                    initial_capital=1000000,
                    cash_balance=1000000
                )
                session.add(default)
                session.commit()
            return default.id
        except Exception as e:
            session.rollback()
            logger.error(f"创建默认账户失败: {e}")
            return None
        finally:
            session.close()
