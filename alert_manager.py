# -*- coding: utf-8 -*-
"""
实时风险预警系统
定时扫描自选股，触发预设条件时记录预警日志
支持多种预警规则：价格突破、技术指标异常、成交量异常、均线突破等
"""
import threading
import time
import traceback
from datetime import datetime
from database import get_session, AlertRule, AlertLog, WatchlistStock, init_db
import logging

logger = logging.getLogger(__name__)

# 预警规则类型定义
RULE_TYPES = {
    'price_above': {'name': '价格上穿', 'desc': '当前价格高于设定值时触发', 'icon': '📈'},
    'price_below': {'name': '价格下穿', 'desc': '当前价格低于设定值时触发', 'icon': '📉'},
    'change_above': {'name': '涨幅超过', 'desc': '日涨幅超过设定百分比时触发', 'icon': '🔺'},
    'change_below': {'name': '跌幅超过', 'desc': '日跌幅超过设定百分比时触发（输入正数）', 'icon': '🔻'},
    'rsi_overbought': {'name': 'RSI超买', 'desc': 'RSI超过设定值（默认70）时触发', 'icon': '⚠️'},
    'rsi_oversold': {'name': 'RSI超卖', 'desc': 'RSI低于设定值（默认30）时触发', 'icon': '💡'},
    'volume_surge': {'name': '成交量暴增', 'desc': '成交量超过20日均量的设定倍数时触发', 'icon': '🔊'},
    'macd_golden': {'name': 'MACD金叉', 'desc': 'MACD发生金叉时触发', 'icon': '✨'},
    'macd_death': {'name': 'MACD死叉', 'desc': 'MACD发生死叉时触发', 'icon': '💀'},
    'ma_break_up': {'name': '突破均线', 'desc': '价格向上突破指定均线（5/10/20/60）时触发', 'icon': '🚀'},
    'ma_break_down': {'name': '跌破均线', 'desc': '价格向下跌破指定均线（5/10/20/60）时触发', 'icon': '⬇️'},
    'stop_loss': {'name': '止损预警', 'desc': '价格跌至自选股设定的止损价时触发', 'icon': '🛑'},
    'target_reach': {'name': '目标价到达', 'desc': '价格达到自选股设定的目标价时触发', 'icon': '🎯'},
}


class AlertManager:
    """风险预警管理器"""

    def __init__(self, analyzer=None):
        self.analyzer = analyzer
        self._scan_thread = None
        self._running = False
        self._scan_interval = 1800  # 默认30分钟扫描一次
        init_db()

    # ==================== 预警规则管理 ====================

    def get_rule_types(self):
        """获取所有支持的预警规则类型"""
        return RULE_TYPES

    def create_rule(self, stock_code, rule_type, condition_value=0, stock_name='', description=''):
        """创建预警规则"""
        session = get_session()
        try:
            if rule_type not in RULE_TYPES:
                return {'error': f'不支持的规则类型: {rule_type}'}

            # 自动生成描述
            if not description:
                rt = RULE_TYPES[rule_type]
                if rule_type in ('price_above', 'price_below'):
                    description = f'{stock_code} {rt["name"]} {condition_value}元'
                elif rule_type in ('change_above', 'change_below'):
                    description = f'{stock_code} {rt["name"]} {condition_value}%'
                elif rule_type in ('rsi_overbought', 'rsi_oversold'):
                    description = f'{stock_code} {rt["name"]} (阈值{condition_value})'
                elif rule_type == 'volume_surge':
                    description = f'{stock_code} 成交量超过均量{condition_value}倍'
                elif rule_type in ('ma_break_up', 'ma_break_down'):
                    description = f'{stock_code} {rt["name"]} MA{int(condition_value)}'
                else:
                    description = f'{stock_code} {rt["name"]}'

            rule = AlertRule(
                stock_code=stock_code,
                stock_name=stock_name,
                rule_type=rule_type,
                condition_value=condition_value,
                description=description,
                is_active=True,
                is_triggered=False
            )
            session.add(rule)
            session.commit()
            return {'success': True, 'rule': rule.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"创建预警规则失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def get_rules(self, stock_code=None, active_only=True):
        """获取预警规则列表"""
        session = get_session()
        try:
            query = session.query(AlertRule)
            if stock_code:
                query = query.filter_by(stock_code=stock_code)
            if active_only:
                query = query.filter_by(is_active=True)
            rules = query.order_by(AlertRule.created_at.desc()).all()
            return [r.to_dict() for r in rules]
        except Exception as e:
            logger.error(f"获取预警规则失败: {e}")
            return []
        finally:
            session.close()

    def update_rule(self, rule_id, **kwargs):
        """更新预警规则"""
        session = get_session()
        try:
            rule = session.query(AlertRule).get(rule_id)
            if not rule:
                return {'error': '规则不存在'}
            for key in ['condition_value', 'is_active', 'description']:
                if key in kwargs:
                    setattr(rule, key, kwargs[key])
            session.commit()
            return {'success': True, 'rule': rule.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"更新预警规则失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def delete_rule(self, rule_id):
        """删除预警规则"""
        session = get_session()
        try:
            rule = session.query(AlertRule).get(rule_id)
            if not rule:
                return {'error': '规则不存在'}
            session.delete(rule)
            session.commit()
            return {'success': True, 'message': '规则已删除'}
        except Exception as e:
            session.rollback()
            logger.error(f"删除预警规则失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def reset_rule(self, rule_id):
        """重置规则触发状态（允许再次触发）"""
        session = get_session()
        try:
            rule = session.query(AlertRule).get(rule_id)
            if not rule:
                return {'error': '规则不存在'}
            rule.is_triggered = False
            session.commit()
            return {'success': True, 'rule': rule.to_dict()}
        except Exception as e:
            session.rollback()
            logger.error(f"重置规则失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    # ==================== 预警日志 ====================

    def get_alert_logs(self, limit=50, unread_only=False, stock_code=None):
        """获取预警日志"""
        session = get_session()
        try:
            query = session.query(AlertLog)
            if unread_only:
                query = query.filter_by(is_read=False)
            if stock_code:
                query = query.filter_by(stock_code=stock_code)
            logs = query.order_by(AlertLog.created_at.desc()).limit(limit).all()
            return [l.to_dict() for l in logs]
        except Exception as e:
            logger.error(f"获取预警日志失败: {e}")
            return []
        finally:
            session.close()

    def get_unread_count(self):
        """获取未读预警数量"""
        session = get_session()
        try:
            count = session.query(AlertLog).filter_by(is_read=False).count()
            return count
        except Exception as e:
            logger.error(f"获取未读数量失败: {e}")
            return 0
        finally:
            session.close()

    def mark_read(self, log_id=None):
        """标记预警为已读（不传log_id则全部标记）"""
        session = get_session()
        try:
            if log_id:
                log = session.query(AlertLog).get(log_id)
                if log:
                    log.is_read = True
            else:
                session.query(AlertLog).filter_by(is_read=False).update({'is_read': True})
            session.commit()
            return {'success': True}
        except Exception as e:
            session.rollback()
            logger.error(f"标记已读失败: {e}")
            return {'error': str(e)}
        finally:
            session.close()

    def _create_alert_log(self, session, rule, alert_type, level, message, current_value=None, threshold_value=None):
        """创建预警日志记录"""
        log = AlertLog(
            rule_id=rule.id if rule else None,
            stock_code=rule.stock_code if rule else '',
            stock_name=rule.stock_name if rule else '',
            alert_type=alert_type,
            alert_level=level,
            message=message,
            current_value=current_value,
            threshold_value=threshold_value,
            is_read=False
        )
        session.add(log)
        # 标记规则已触发
        if rule:
            rule.is_triggered = True
            rule.last_triggered_at = datetime.now()

    # ==================== 扫描引擎 ====================

    def scan_all_rules(self):
        """扫描所有活跃的预警规则"""
        session = get_session()
        triggered_count = 0
        try:
            rules = session.query(AlertRule).filter_by(is_active=True, is_triggered=False).all()
            if not rules:
                logger.info("没有需要扫描的预警规则")
                return 0

            # 按股票代码分组，减少数据获取次数
            stock_rules = {}
            for rule in rules:
                if rule.stock_code not in stock_rules:
                    stock_rules[rule.stock_code] = []
                stock_rules[rule.stock_code].append(rule)

            for stock_code, code_rules in stock_rules.items():
                try:
                    triggered_count += self._check_stock_rules(session, stock_code, code_rules)
                except Exception as e:
                    logger.error(f"扫描 {stock_code} 规则失败: {e}")

            # 同时扫描自选股的止损/目标价
            triggered_count += self._check_watchlist_alerts(session)

            session.commit()
            logger.info(f"预警扫描完成，触发 {triggered_count} 条预警")
            return triggered_count
        except Exception as e:
            session.rollback()
            logger.error(f"预警扫描失败: {traceback.format_exc()}")
            return 0
        finally:
            session.close()

    def _check_stock_rules(self, session, stock_code, rules):
        """检查单只股票的所有规则"""
        if not self.analyzer:
            return 0

        triggered = 0
        try:
            df = self.analyzer.get_stock_data(stock_code, 'A')
            df = self.analyzer.calculate_indicators(df)
            if df is None or len(df) < 2:
                return 0

            latest = df.iloc[-1]
            prev = df.iloc[-2]
            current_price = float(latest['close'])
            prev_close = float(prev['close'])
            change_pct = (current_price - prev_close) / prev_close * 100

            for rule in rules:
                try:
                    is_triggered = False
                    message = ''
                    level = 'info'
                    current_val = None
                    threshold_val = rule.condition_value

                    if rule.rule_type == 'price_above':
                        if current_price >= rule.condition_value:
                            is_triggered = True
                            level = 'warning'
                            current_val = current_price
                            message = f'📈 {stock_code}({rule.stock_name}) 当前价 {current_price:.2f} 已上穿 {rule.condition_value:.2f}'

                    elif rule.rule_type == 'price_below':
                        if current_price <= rule.condition_value:
                            is_triggered = True
                            level = 'danger'
                            current_val = current_price
                            message = f'📉 {stock_code}({rule.stock_name}) 当前价 {current_price:.2f} 已下穿 {rule.condition_value:.2f}'

                    elif rule.rule_type == 'change_above':
                        if change_pct >= rule.condition_value:
                            is_triggered = True
                            level = 'warning'
                            current_val = change_pct
                            message = f'🔺 {stock_code}({rule.stock_name}) 涨幅 {change_pct:.2f}% 超过 {rule.condition_value}%'

                    elif rule.rule_type == 'change_below':
                        if change_pct <= -abs(rule.condition_value):
                            is_triggered = True
                            level = 'danger'
                            current_val = change_pct
                            message = f'🔻 {stock_code}({rule.stock_name}) 跌幅 {abs(change_pct):.2f}% 超过 {rule.condition_value}%'

                    elif rule.rule_type == 'rsi_overbought':
                        rsi = float(latest.get('RSI', 50))
                        threshold = rule.condition_value or 70
                        if rsi >= threshold:
                            is_triggered = True
                            level = 'warning'
                            current_val = rsi
                            threshold_val = threshold
                            message = f'⚠️ {stock_code}({rule.stock_name}) RSI={rsi:.1f} 进入超买区域（阈值{threshold}）'

                    elif rule.rule_type == 'rsi_oversold':
                        rsi = float(latest.get('RSI', 50))
                        threshold = rule.condition_value or 30
                        if rsi <= threshold:
                            is_triggered = True
                            level = 'info'
                            current_val = rsi
                            threshold_val = threshold
                            message = f'💡 {stock_code}({rule.stock_name}) RSI={rsi:.1f} 进入超卖区域（阈值{threshold}），可能存在反弹机会'

                    elif rule.rule_type == 'volume_surge':
                        volume = float(latest.get('volume', 0))
                        avg_vol = float(df['volume'].rolling(20).mean().iloc[-1]) if len(df) >= 20 else float(df['volume'].mean())
                        ratio = volume / avg_vol if avg_vol > 0 else 0
                        threshold = rule.condition_value or 2.0
                        if ratio >= threshold:
                            is_triggered = True
                            level = 'warning'
                            current_val = ratio
                            threshold_val = threshold
                            message = f'🔊 {stock_code}({rule.stock_name}) 成交量暴增，量比 {ratio:.1f} 倍（阈值{threshold}倍）'

                    elif rule.rule_type == 'macd_golden':
                        macd_now = float(latest.get('MACD', 0))
                        signal_now = float(latest.get('Signal', 0))
                        macd_prev = float(prev.get('MACD', 0))
                        signal_prev = float(prev.get('Signal', 0))
                        if macd_now > signal_now and macd_prev <= signal_prev:
                            is_triggered = True
                            level = 'info'
                            message = f'✨ {stock_code}({rule.stock_name}) MACD金叉，可能开启上涨趋势'

                    elif rule.rule_type == 'macd_death':
                        macd_now = float(latest.get('MACD', 0))
                        signal_now = float(latest.get('Signal', 0))
                        macd_prev = float(prev.get('MACD', 0))
                        signal_prev = float(prev.get('Signal', 0))
                        if macd_now < signal_now and macd_prev >= signal_prev:
                            is_triggered = True
                            level = 'danger'
                            message = f'💀 {stock_code}({rule.stock_name}) MACD死叉，注意下跌风险'

                    elif rule.rule_type == 'ma_break_up':
                        ma_period = int(rule.condition_value) if rule.condition_value else 20
                        ma_col = f'MA{ma_period}'
                        if ma_col in latest and ma_col in prev:
                            ma_val = float(latest[ma_col])
                            prev_price = float(prev['close'])
                            if current_price > ma_val and prev_price <= float(prev[ma_col]):
                                is_triggered = True
                                level = 'info'
                                current_val = current_price
                                threshold_val = ma_val
                                message = f'🚀 {stock_code}({rule.stock_name}) 突破MA{ma_period}（{ma_val:.2f}），当前价 {current_price:.2f}'

                    elif rule.rule_type == 'ma_break_down':
                        ma_period = int(rule.condition_value) if rule.condition_value else 20
                        ma_col = f'MA{ma_period}'
                        if ma_col in latest and ma_col in prev:
                            ma_val = float(latest[ma_col])
                            prev_price = float(prev['close'])
                            if current_price < ma_val and prev_price >= float(prev[ma_col]):
                                is_triggered = True
                                level = 'danger'
                                current_val = current_price
                                threshold_val = ma_val
                                message = f'⬇️ {stock_code}({rule.stock_name}) 跌破MA{ma_period}（{ma_val:.2f}），当前价 {current_price:.2f}'

                    if is_triggered:
                        self._create_alert_log(session, rule, rule.rule_type, level, message, current_val, threshold_val)
                        triggered += 1
                        logger.info(f"预警触发: {message}")

                except Exception as e:
                    logger.error(f"检查规则 {rule.id} 失败: {e}")

        except Exception as e:
            logger.error(f"获取 {stock_code} 数据失败: {e}")

        return triggered

    def _check_watchlist_alerts(self, session):
        """检查自选股的止损价和目标价"""
        if not self.analyzer:
            return 0

        triggered = 0
        try:
            # 获取设置了止损价或目标价的自选股
            stocks = session.query(WatchlistStock).filter(
                (WatchlistStock.stop_loss_price > 0) | (WatchlistStock.target_price > 0)
            ).all()

            for stock in stocks:
                try:
                    df = self.analyzer.get_stock_data(stock.stock_code, stock.market_type)
                    if df is None or len(df) < 1:
                        continue
                    current_price = float(df.iloc[-1]['close'])

                    # 检查止损
                    if stock.stop_loss_price > 0 and current_price <= stock.stop_loss_price:
                        # 检查是否已有未读的同类预警
                        existing = session.query(AlertLog).filter_by(
                            stock_code=stock.stock_code,
                            alert_type='stop_loss',
                            is_read=False
                        ).first()
                        if not existing:
                            log = AlertLog(
                                stock_code=stock.stock_code,
                                stock_name=stock.stock_name,
                                alert_type='stop_loss',
                                alert_level='danger',
                                message=f'🛑 {stock.stock_code}({stock.stock_name}) 当前价 {current_price:.2f} 已触及止损价 {stock.stop_loss_price:.2f}',
                                current_value=current_price,
                                threshold_value=stock.stop_loss_price,
                                is_read=False
                            )
                            session.add(log)
                            triggered += 1

                    # 检查目标价
                    if stock.target_price > 0 and current_price >= stock.target_price:
                        existing = session.query(AlertLog).filter_by(
                            stock_code=stock.stock_code,
                            alert_type='target_reach',
                            is_read=False
                        ).first()
                        if not existing:
                            log = AlertLog(
                                stock_code=stock.stock_code,
                                stock_name=stock.stock_name,
                                alert_type='target_reach',
                                alert_level='warning',
                                message=f'🎯 {stock.stock_code}({stock.stock_name}) 当前价 {current_price:.2f} 已达到目标价 {stock.target_price:.2f}',
                                current_value=current_price,
                                threshold_value=stock.target_price,
                                is_read=False
                            )
                            session.add(log)
                            triggered += 1

                except Exception as e:
                    logger.error(f"检查自选股 {stock.stock_code} 止损/目标价失败: {e}")

        except Exception as e:
            logger.error(f"检查自选股预警失败: {e}")

        return triggered

    # ==================== 定时扫描 ====================

    def start_scheduler(self, interval=1800):
        """启动定时扫描（默认30分钟）"""
        self._scan_interval = interval
        if self._running:
            logger.info("预警扫描器已在运行中")
            return

        self._running = True
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()
        logger.info(f"✅ 预警扫描器已启动，间隔 {interval} 秒")

    def stop_scheduler(self):
        """停止定时扫描"""
        self._running = False
        logger.info("预警扫描器已停止")

    def _scan_loop(self):
        """扫描循环"""
        while self._running:
            try:
                # 只在交易时间扫描（9:30-15:00）
                now = datetime.now()
                hour = now.hour
                minute = now.minute
                weekday = now.weekday()

                # 周一到周五，9:25-15:05
                if weekday < 5 and ((hour == 9 and minute >= 25) or (10 <= hour <= 14) or (hour == 15 and minute <= 5)):
                    logger.info("开始预警扫描...")
                    count = self.scan_all_rules()
                    logger.info(f"预警扫描完成，触发 {count} 条")
                else:
                    logger.debug("非交易时间，跳过扫描")

            except Exception as e:
                logger.error(f"预警扫描循环异常: {traceback.format_exc()}")

            time.sleep(self._scan_interval)

    def get_status(self):
        """获取扫描器状态"""
        return {
            'running': self._running,
            'interval': self._scan_interval,
            'unread_count': self.get_unread_count()
        }

    # ==================== 快捷创建 ====================

    def create_watchlist_rules(self, stock_code, stock_name=''):
        """为自选股快速创建一组常用预警规则"""
        results = []
        # RSI超买
        results.append(self.create_rule(stock_code, 'rsi_overbought', 75, stock_name))
        # RSI超卖
        results.append(self.create_rule(stock_code, 'rsi_oversold', 25, stock_name))
        # 成交量暴增
        results.append(self.create_rule(stock_code, 'volume_surge', 2.5, stock_name))
        # MACD金叉
        results.append(self.create_rule(stock_code, 'macd_golden', 0, stock_name))
        # MACD死叉
        results.append(self.create_rule(stock_code, 'macd_death', 0, stock_name))
        # 跌幅超5%
        results.append(self.create_rule(stock_code, 'change_below', 5, stock_name))

        success_count = sum(1 for r in results if r.get('success'))
        return {'success': True, 'message': f'已创建 {success_count} 条预警规则', 'rules': results}
