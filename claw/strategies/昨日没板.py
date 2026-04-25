# -*- coding: utf-8 -*-
"""
弱转强策略（前日涨停+昨日不涨停） - PTrade版本
基于 ay弱转强 pt 策略 V2 框架改写

选股逻辑：
1. 从中小综指(399101)成分股中筛选，过滤ST股
2. 核心筛选：前日涨停 + 昨日不涨停（弱转强形态）
3. 国九条财务筛选（净利润>0，营业收入>1亿）
4. 技术指标筛选（收盘>MA10、收盘>前低、放量不过量、股价>1元）
5. 开盘价筛选 + 集合竞价BS筛选 + 换手率×开盘比排序

卖出逻辑（与V2完全一致）：
- 13:00 仅执行止损（不止盈，减少卖飞）
- 14:55 执行完整卖出（止损+止盈+常规卖出）
- 增强版动态止损：日内快速止损、短期止损、ATR动态止损、大盘环境止损
- 移动止盈：盈利>5%回撤≥3%，盈利>10%回撤≥4%
- 常规卖出：未涨停 且 (跌破MA7 或 盈利>0% 或 昨日涨停)

实盘订单管理（与V2完全一致）：
- 每3秒检查订单状态
- 两阶段撤单确认机制
- 防重复买入，涨停不撤买单，跌停不撤卖单
- 最多重试10次
"""
import datetime
import numpy as np
import pandas as pd

# ================================== 策略函数 ===================================

def initialize(context):
    """
    策略初始化函数，只在策略启动时运行一次。
    """
    # --- 授权验证 ---
    if is_trade():
        auth_result = permission_test()
        if not auth_result:
            log.error("="*50)
            log.error("【授权失败】策略无权在当前账户或时间运行！")
            log.error("请检查：1. 账户是否匹配  2. 是否已过有效期")
            log.error("="*50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，策略启动成功")
    
    # --- 策略参数 ---
    g.stock_num = 4                # 持仓股票数量
    g.avoid_jan_apr_dec = False     # 是否开启1、4、12月空仓规则
    g.open_down_threshold = 0.97   # 开盘价筛选下限（聚宽原版0.95）
    g.open_up_threshold = 1.03     # 开盘价筛选上限（聚宽原版1.01）
    g.ma_period = 10               # 均线周期
    g.volume_ratio_threshold = 10  # 成交量倍数上限
    g.stop_loss_ma_period = 7      # 卖出止损均线周期
    g.max_single_stock_amount = 100000  # 单股最大买入金额限制

    # 国九条筛选参数
    g.min_operating_revenue = 1e8  # 最小营业收入（元）
    g.min_net_profit = 0           # 最小净利润

    # ========== 【退学炒股思想增强】（已关闭，经回测反而拖累策略）==========
    # 回滚原因：
    # 1. 弱转强本质是"低吸超跌反弹"，冰点/崩溃反而是最佳买点，不应空仓
    # 2. 同题材共振（如AI、储能）才是主升浪来源，强制分散砍掉板块效应α
    # 3. 次日低开-2%对弱转强股只是正常洗盘，提前止损严重伤害盈亏比
    # 如需重新启用请把对应 enable_* 改为 True 并回测验证

    # 1. 情绪周期过滤（关闭：弱转强不适合在冰点空仓）
    g.enable_market_regime_filter = False  # 关闭情绪周期过滤
    g.ice_regime_skip_buy = False          # 关闭冰点跳过买入
    g.regime_ice_up_ratio = 0.20           # 保留参数定义以防报错
    g.regime_crash_index_drop = -0.025     # 若启用需更极端才触发(-2.5%)
    g.regime_crash_ma20_break = False      # 关闭MA20破位判断

    # 2. 板块分散（关闭：不限制同行业数量，允许题材共振）
    g.max_per_industry = 99                # 设为99等同于不限制

    # 3. 次日弱势必走（关闭：弱转强低开洗盘是正常现象）
    g.enable_t1_weak_exit = False          # 关闭次日弱势必走
    g.t1_weak_open_threshold = -0.05       # 若启用需更极端(-5%)
    g.t1_weak_price_threshold = 0.97       # 若启用需跌破昨收-3%
    # ================================================================

    # --- 内部状态变量 ---
    g.today_list = []
    g.buy_records = {}
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    g.last_total_value = 0.0
    
    # --- 订单管理全局变量 ---
    g.buy_orders = {}
    g.sell_orders = {}
    g.pending_buy_stocks = {}
    g.stock_retry_count = {}
    g.max_retry_count = 10
    g.order_check_interval = 3
    
    # --- 注册所有定时任务 ---
    run_daily(context, real_trade_buy_task, time='09:25:10')
    run_interval(context, check_and_retry_orders, seconds=3)
    run_daily(context, backtest_buy_task, time='09:31')
    run_daily(context, sell_task, time='13:00')
    run_daily(context, sell_task, time='14:55')


def before_trading_start(context, data):
    """
    每日开盘前运行函数。
    """
    # 1. 重置每日状态变量
    g.today_list = []
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    
    # 2. [实盘专用] 重置订单追踪记录
    if is_trade():
        g.buy_orders.clear()
        g.sell_orders.clear()
        g.pending_buy_stocks.clear()
        g.stock_retry_count.clear()
        log.info("[Ptrade实盘] 订单追踪记录已清空")
    
    log.info("=" * 50)
    log.info("新交易日开始，重置每日状态变量。")

    # 2. 判断是否在空仓期
    if g.avoid_jan_apr_dec and is_avoid_period(context):
        log.info("当前处于1、4、12月空仓期，今日不进行选股和交易。")
        set_universe([])
        return

    # 2.5 【退学炒股思想】情绪周期判断 - 冰点/崩溃期强制空仓
    if g.enable_market_regime_filter:
        regime, detail = get_market_regime(context)
        log.info("📊 【周期判断】当前市场状态: %s | 详情: %s" % (regime, detail))
        if regime in ('ICE', 'CRASH') and g.ice_regime_skip_buy:
            log.warning("⛔ 市场处于[%s]状态，按退学思想空仓等待！今日不选股。" % regime)
            set_universe(list(context.portfolio.positions.keys()))
            return

    # 3. 执行选股逻辑
    log.info(">>> 开始执行盘前选股任务...")
    
    # 3.1 获取基础股票池：中小综指成分股，过滤ST
    stock_pool = get_index_pool_excluding_st(context)
    log.info("中小综指成分股(去ST)数量: %d" % len(stock_pool))

    # 3.2 核心筛选：前日涨停 + 昨日不涨停（弱转强）
    stock_pool = rzq_filter(context, stock_pool)
    log.info("弱转强筛选后数量: %d" % len(stock_pool))

    if not stock_pool:
        log.info("弱转强筛选后无股票，结束选股。")
        set_universe(list(context.portfolio.positions.keys()))
        return

    # 3.3 国九条筛选
    stock_pool = gjt_filter(context, stock_pool)
    log.info("国九条筛选后数量: %d" % len(stock_pool))

    if not stock_pool:
        log.info("国九条筛选后无股票，结束选股。")
        set_universe(list(context.portfolio.positions.keys()))
        return

    # 3.4 技术指标筛选
    stock_pool = technical_filter(context, stock_pool)
    log.info("技术指标筛选后数量: %d" % len(stock_pool))

    # 3.5 将初选结果存入全局变量
    g.today_list = stock_pool
    log.info("盘前选股完成，待买池数量: %d" % len(g.today_list))

    # 4. 更新股票池
    current_positions = list(context.portfolio.positions.keys())
    universe = list(set(g.today_list + current_positions))
    if universe:
        set_universe(universe)
        log.info("已更新universe，共订阅 %d 只股票的行情。" % len(universe))
        
    log.info(">>> 盘前选股任务执行完毕。")


def handle_data(context, data):
    """
    策略主逻辑函数，本策略中为空，所有逻辑由定时任务驱动。
    """
    pass


def after_trading_end(context, data):
    """
    每日收盘后运行函数，用于生成交易报告。
    """
    daily_trading_report(context)
    update_buy_records(context)


# ================================== 业务逻辑函数 ===================================

def real_trade_buy_task(context):
    """
    [实盘专用] 在集合竞价期间执行买入操作的函数。
    """
    if not is_trade():
        return
        
    log.info("--- %s: [实盘]触发集合竞价买入任务 ---" % context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))
    if not g.today_list:
        log.info("[实盘]今日无候选股票，不执行买入。")
        return

    # 1. 使用snapshot数据进行筛选和排序
    snapshots = get_snapshot(g.today_list)
    if not snapshots:
        log.warning("[实盘]无法获取快照数据，取消本次买入。")
        return
        
    buy_list = real_trade_opening_filter_and_rank(context, snapshots, g.today_list)
    log.info("[实盘]经集合竞价筛选和排序后，买入池数量: %d" % len(buy_list))

    if not buy_list:
        log.info("[实盘]无满足条件的股票。")
        return

    # 2. 计算可买入数量和资金
    current_positions = [code for code, pos in context.portfolio.positions.items() if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        return

    buy_list = [s for s in buy_list if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[实盘]候选股票均已持仓，无新的可买入股票。")
        return

    # 3. 执行买入（按空仓位数分配资金，而非按实际买入数量，避免候选票少时全仓买入）
    cash_per_stock = context.portfolio.cash / num_to_buy
    for stock in buy_list:
        if cash_per_stock > 0:
            actual_cash = min(g.max_single_stock_amount, cash_per_stock)
            price_ref = snapshots.get(stock, {}).get('last_px', 0)
            if price_ref > 0:
                limit_price = price_ref * 1.01
                up_px = snapshots.get(stock, {}).get('up_px', price_ref * 1.1)
                limit_price = min(limit_price, up_px)
                limit_price = round(limit_price, 2)
                
                log.info("[实盘]买入 %s, 分配资金: %.2f, 实际使用: %.2f, 现价: %.2f, 委托价: %.2f (+1%%)" % 
                        (stock, cash_per_stock, actual_cash, price_ref, limit_price))
                order_id = order_value(stock, actual_cash, limit_price=limit_price)
                if order_id:
                    g.today_bought_stocks.add(stock)
                    g.buy_orders[order_id] = {
                        'stock': stock,
                        'cash': actual_cash,
                        'limit_price': limit_price,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0
                    }
                    log.info("[实盘]订单已提交: order_id=%s, stock=%s, limit_price=%.2f" % (order_id, stock, limit_price))
    
    g.order_check_start_time = context.blotter.current_dt

def backtest_buy_task(context):
    """
    [回测专用] 在开盘后执行买入操作的函数。
    """
    if is_trade():
        return
        
    log.info("--- %s: [回测]触发买入任务 ---" % context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))
    if not g.today_list:
        log.info("[回测]今日无候选股票，不执行买入。")
        return

    # 1. 开盘价筛选和排序
    buy_list = backtest_opening_filter_and_rank(context, g.today_list)
    log.info("[回测]经开盘价筛选和排序后，买入池数量: %d" % len(buy_list))

    if not buy_list:
        log.info("[回测]无满足开盘价条件的股票。")
        return

    # 2. 计算可买入数量和资金
    current_positions = [code for code, pos in context.portfolio.positions.items() if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[回测]持仓已满，无需买入。")
        return

    buy_list = [s for s in buy_list if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[回测]候选股票均已持仓，无新的可买入股票。")
        return

    # 3. 执行买入（按空仓位数分配资金，而非按实际买入数量，避免候选票少时全仓买入）
    cash_per_stock = context.portfolio.cash / num_to_buy
    for stock in buy_list:
        if cash_per_stock > 0:
            log.info("[回测]买入 %s, 分配资金: %.2f" % (stock, cash_per_stock))
            order_id = order_value(stock, cash_per_stock)
            if order_id:
                g.today_bought_stocks.add(stock)


def check_and_retry_orders(context):
    """
    [实盘专用] 统一的订单检查与补单函数。
    由run_interval每3秒执行一次，最多重试10次。
    """
    if not is_trade():
        return
    
    if g.buy_orders:
        check_and_retry_buy_orders(context)
    
    if g.sell_orders:
        check_and_retry_sell_orders(context)


def check_and_retry_buy_orders(context):
    """
    [实盘专用] 检查买入订单状态并补单的函数。
    """
    if not is_trade():
        return
    
    if not g.buy_orders:
        return
    
    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]买入订单检查执行 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    log.info("[实盘买入补单] 待检查订单数量: %d" % len(g.buy_orders))
    
    # 获取当日所有成交记录，用于检测是否已成交（避免重复买入）
    try:
        today_trades = get_trades()
        stock_filled_amounts = {}
        for trade in today_trades:
            if trade.is_buy:
                stock_code = trade.security
                if stock_code not in stock_filled_amounts:
                    stock_filled_amounts[stock_code] = 0
                stock_filled_amounts[stock_code] += trade.amount
        log.info("[实盘买入补单] 当日买入成交统计: %s" % stock_filled_amounts)
    except Exception as e:
        log.warning("[实盘买入补单] 获取成交记录失败: %s" % e)
        stock_filled_amounts = {}
    
    orders_to_remove = []
    orders_to_retry = []
    
    for order_id, order_info in list(g.buy_orders.items()):
        stock = order_info['stock']
        cash_allocated = order_info['cash']
        retry_count = order_info.get('retry_count', 0)
        
        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count:
            log.warning("[实盘买入补单] %s 已达到最大重试次数(%d)，停止追踪" % (stock, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        if retry_count >= g.max_retry_count:
            log.warning("[实盘买入补单] %s 订单%s已达到最大重试次数(%d)，停止追踪" % (stock, order_id, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        try:
            order_list = get_order(order_id)
            
            if order_list and len(order_list) > 0:
                order_status = order_list[0]
                filled_amount = order_status.filled
                order_amount = order_status.amount
                status = order_status.status
                
                log.info("[实盘买入补单] %s 订单状态: order_id=%s, status=%s, 已成交=%d/总量=%d, 重试次数=%d/%d" % 
                        (stock, order_id, status, filled_amount, order_amount, retry_count, g.max_retry_count))
                
                # 检查该股票是否已有成交记录（防止重复买入）
                if stock in stock_filled_amounts and stock_filled_amounts[stock] > 0:
                    log.warning("[实盘买入补单] %s 检测到当日已有成交记录（%d股），移除订单追踪避免重复买入" % 
                               (stock, stock_filled_amounts[stock]))
                    orders_to_remove.append(order_id)
                    continue
                
                if status == '8':  # 已成交
                    is_cancelling = order_info.get('is_cancelling', False)
                    if is_cancelling:
                        log.warning("[实盘买入补单] %s 撤单期间订单已成交（撤单请求未生效），移除追踪" % stock)
                    else:
                        log.info("[实盘买入补单] %s 已全部成交，移除追踪" % stock)
                    orders_to_remove.append(order_id)
                    
                elif status == '7':  # 部成
                    if filled_amount > 0 and filled_amount < order_amount:
                        limit_price = order_info.get('limit_price', cash_allocated / order_amount)
                        filled_cash = filled_amount * limit_price
                        remaining_cash = cash_allocated - filled_cash
                        
                        log.info("[实盘买入补单] %s 部分成交: 已成交%d股(约%.2f元), 剩余%.2f元" % 
                                (stock, filled_amount, filled_cash, remaining_cash))
                        
                        is_cancelling = order_info.get('is_cancelling', False)
                        
                        if not is_cancelling:
                            try:
                                cancel_order(order_id)
                                log.info("[实盘买入补单] 部成订单撤销请求已发送: order_id=%s" % order_id)
                                g.buy_orders[order_id]['is_cancelling'] = True
                                g.buy_orders[order_id]['remaining_cash'] = remaining_cash
                                log.info("[实盘买入补单] %s 部成订单已标记撤单中，等待下轮确认撤单状态" % stock)
                            except Exception as e:
                                log.warning("[实盘买入补单] 部成订单撤销请求失败: order_id=%s, error=%s" % (order_id, e))
                                orders_to_remove.append(order_id)
                        else:
                            old_remaining = order_info.get('remaining_cash', 0)
                            if abs(remaining_cash - old_remaining) > 100:
                                log.info("[实盘买入补单] %s 部成订单撤单期间又成交，剩余资金从%.2f更新为%.2f" % 
                                        (stock, old_remaining, remaining_cash))
                                g.buy_orders[order_id]['remaining_cash'] = remaining_cash
                            else:
                                log.info("[实盘买入补单] %s 部成订单撤单处理中，等待确认..." % stock)
                    else:
                        log.warning("[实盘买入补单] %s 部成状态异常，移除追踪" % stock)
                        orders_to_remove.append(order_id)
                    
                elif status in ['0', '1', '2', '+', '-', 'C', 'V']:  # 完全未成交
                    snapshot = get_snapshot(stock)
                    if snapshot:
                        current_price = snapshot.get('last_px', 0)
                        up_px = snapshot.get('up_px', 0)
                        is_limit_up = (up_px > 0 and abs(current_price - up_px) < 0.01)
                        
                        if is_limit_up:
                            log.info("[实盘买入补单] %s 已涨停（%.2f），不撤单，等待打开涨停" % (stock, current_price))
                            continue
                    
                    is_cancelling = order_info.get('is_cancelling', False)
                    
                    if not is_cancelling:
                        try:
                            cancel_order(order_id)
                            log.info("[实盘买入补单] 撤单请求已发送: order_id=%s, stock=%s" % (order_id, stock))
                            g.buy_orders[order_id]['is_cancelling'] = True
                            log.info("[实盘买入补单] %s 已标记撤单中，等待下轮确认撤单状态" % stock)
                        except Exception as e:
                            log.warning("[实盘买入补单] 撤单请求失败: order_id=%s, error=%s" % (order_id, e))
                    else:
                        log.info("[实盘买入补单] %s 撤单处理中，等待确认..." % stock)
                
                elif status == '6':  # 已撤
                    remaining_cash = order_info.get('remaining_cash', 0)
                    
                    if remaining_cash > 0:
                        log.info("[实盘买入补单] %s 部成订单撤单已确认（status=6），准备补充剩余资金%.2f" % (stock, remaining_cash))
                        orders_to_remove.append(order_id)
                        if remaining_cash > 100:
                            actual_remaining_cash = min(g.max_single_stock_amount, remaining_cash)
                            orders_to_retry.append({
                                'stock': stock,
                                'cash': actual_remaining_cash,
                                'retry_count': retry_count + 1,
                                'original_order_id': order_id
                            })
                        else:
                            log.info("[实盘买入补单] %s 剩余资金不足100元，放弃补单" % stock)
                    else:
                        log.info("[实盘买入补单] %s 撤单已确认（status=6），准备补单" % stock)
                        orders_to_remove.append(order_id)
                        actual_cash = min(g.max_single_stock_amount, cash_allocated)
                        orders_to_retry.append({
                            'stock': stock,
                            'cash': actual_cash,
                            'retry_count': retry_count + 1,
                            'original_order_id': order_id
                        })
                
                elif status == '9':  # 废单
                    log.warning("[实盘买入补单] %s 订单被拒绝（status=9 废单），准备补单" % stock)
                    orders_to_remove.append(order_id)
                    actual_cash = min(g.max_single_stock_amount, cash_allocated)
                    orders_to_retry.append({
                        'stock': stock,
                        'cash': actual_cash,
                        'retry_count': retry_count + 1,
                        'original_order_id': order_id
                    })
            else:
                log.warning("[实盘买入补单] 无法获取订单状态: order_id=%s" % order_id)
                
        except Exception as e:
            log.error("[实盘买入补单] 检查订单异常: order_id=%s, error=%s" % (order_id, e))
    
    # 移除已处理的订单
    for order_id in orders_to_remove:
        if order_id in g.buy_orders:
            del g.buy_orders[order_id]
    
    # 执行补单并追踪新订单
    if orders_to_retry:
        # 过滤掉已经有成交记录的股票
        filtered_orders = []
        for item in orders_to_retry:
            stock = item['stock']
            if stock in stock_filled_amounts and stock_filled_amounts[stock] > 0:
                log.warning("[实盘买入补单] %s 当日已有成交记录（%d股），跳过补单避免重复买入" % 
                           (stock, stock_filled_amounts[stock]))
                continue
            filtered_orders.append(item)
        
        orders_to_retry = filtered_orders
        if not orders_to_retry:
            log.info("[实盘买入补单] 所有需要补单的股票都已有成交记录，无需补单")
            return
        
        log.info("[实盘买入补单] 需要补单的股票数量: %d" % len(orders_to_retry))
        
        stocks_to_retry = [item['stock'] for item in orders_to_retry]
        snapshots = get_snapshot(stocks_to_retry)
        
        if not snapshots:
            log.warning("[实盘买入补单] 无法获取快照数据，本轮放弃补单")
            return
        
        for item in orders_to_retry:
            stock = item['stock']
            cash_allocated = item['cash']
            retry_count = item['retry_count']
            
            snapshot = snapshots.get(stock)
            if not snapshot:
                log.warning("[实盘买入补单] %s 无法获取快照数据，跳过" % stock)
                continue
            
            current_price = snapshot.get('last_px', 0)
            if current_price <= 0:
                log.warning("[实盘买入补单] %s 当前价格无效，跳过" % stock)
                continue
            
            limit_price = current_price * 1.01
            up_px = snapshot.get('up_px', current_price * 1.1)
            limit_price = min(limit_price, up_px)
            limit_price = round(limit_price, 2)
            
            log.info("[实盘买入补单] 重新下单(第%d次): %s, 资金=%.2f, 当前价=%.2f, 限价=%.2f (+1%%)" % 
                    (retry_count, stock, cash_allocated, current_price, limit_price))
            
            try:
                actual_cash = min(g.max_single_stock_amount, cash_allocated)
                new_order_id = order_value(stock, actual_cash, limit_price=limit_price)
                if new_order_id:
                    g.buy_orders[new_order_id] = {
                        'stock': stock,
                        'cash': actual_cash,
                        'limit_price': limit_price,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': retry_count
                    }
                    g.stock_retry_count[stock] = retry_count
                    
                    log.info("[实盘买入补单] 补单成功，新订单ID=%s，将继续追踪（%s总重试%d/%d）" % 
                            (new_order_id, stock, retry_count, g.max_retry_count))
                else:
                    log.warning("[实盘买入补单] %s 补单失败，order_value返回空" % stock)
            except Exception as e:
                log.error("[实盘买入补单] 补单异常: stock=%s, error=%s" % (stock, e))
    
    if g.buy_orders:
        log.info("[实盘买入补单] 当前仍在追踪的订单数: %d" % len(g.buy_orders))
    else:
        log.info("[实盘买入补单] 所有订单已处理完成，无需继续追踪")


def check_and_retry_sell_orders(context):
    """
    [实盘专用] 检查卖出订单状态并补单的函数。
    """
    if not is_trade():
        return
    
    if not g.sell_orders:
        return
    
    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]卖出订单检查执行 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    log.info("[实盘卖出补单] 待检查订单数量: %d" % len(g.sell_orders))
    
    orders_to_remove = []
    orders_to_retry = []
    
    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '未知原因')
        retry_count = order_info.get('retry_count', 0)
        
        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count:
            log.warning("[实盘卖出补单] %s 已达到最大重试次数(%d)，停止追踪" % (stock, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        if retry_count >= g.max_retry_count:
            log.warning("[实盘卖出补单] %s 订单%s已达到最大重试次数(%d)，停止追踪" % (stock, order_id, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        try:
            order_list = get_order(order_id)
            
            if order_list and len(order_list) > 0:
                order_status = order_list[0]
                filled_amount = order_status.filled
                order_amount = order_status.amount
                status = order_status.status
                
                log.info("[实盘卖出补单] %s 订单状态: order_id=%s, status=%s, 已成交=%d/总量=%d, 重试次数=%d/%d, 原因=%s" % 
                        (stock, order_id, status, filled_amount, order_amount, retry_count, g.max_retry_count, reason))
                
                if status == '8':  # 已成
                    log.info("[实盘卖出补单] %s 已全部成交，移除追踪" % stock)
                    orders_to_remove.append(order_id)
                    
                elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:  # 未成交或部分成交
                    snapshot = get_snapshot(stock)
                    if snapshot:
                        current_price = snapshot.get('last_px', 0)
                        down_px = snapshot.get('down_px', 0)
                        is_limit_down = (down_px > 0 and abs(current_price - down_px) < 0.01)
                        
                        if is_limit_down:
                            log.info("[实盘卖出补单] %s 已跌停（%.2f），不撤单，等待打开跌停" % (stock, current_price))
                            continue
                    
                    cancel_success = False
                    try:
                        cancel_result = cancel_order(order_id)
                        log.info("[实盘卖出补单] 撤单请求已发送: order_id=%s, stock=%s" % (order_id, stock))
                        cancel_success = True
                    except Exception as e:
                        log.warning("[实盘卖出补单] 撤单失败: order_id=%s, error=%s" % (order_id, e))
                        cancel_success = False
                    
                    if cancel_success:
                        orders_to_remove.append(order_id)
                        orders_to_retry.append({
                            'stock': stock,
                            'reason': reason,
                            'retry_count': retry_count + 1,
                            'original_order_id': order_id
                        })
                    else:
                        log.warning("[实盘卖出补单] %s 撤单未成功，本轮不补单，下轮继续检查" % stock)
                
                elif status in ['6', '9']:  # 已撤 / 废单
                    log.warning("[实盘卖出补单] %s 订单已被撤销或拒绝: status=%s" % (stock, status))
                    orders_to_remove.append(order_id)
                    orders_to_retry.append({
                        'stock': stock,
                        'reason': reason,
                        'retry_count': retry_count + 1,
                        'original_order_id': order_id
                    })
            else:
                log.warning("[实盘卖出补单] 无法获取订单状态: order_id=%s" % order_id)
                
        except Exception as e:
            log.error("[实盘卖出补单] 检查订单异常: order_id=%s, error=%s" % (order_id, e))
    
    # 移除已处理的订单
    for order_id in orders_to_remove:
        if order_id in g.sell_orders:
            del g.sell_orders[order_id]
    
    # 执行补单并追踪新订单
    if orders_to_retry:
        log.info("[实盘卖出补单] 需要补单的股票数量: %d" % len(orders_to_retry))
        
        stocks_to_retry = [item['stock'] for item in orders_to_retry]
        snapshots = get_snapshot(stocks_to_retry)
        
        if not snapshots:
            log.warning("[实盘卖出补单] 无法获取快照数据，本轮放弃补单")
            return
        
        for item in orders_to_retry:
            stock = item['stock']
            reason = item['reason']
            retry_count = item['retry_count']
            
            snapshot = snapshots.get(stock)
            if not snapshot:
                log.warning("[实盘卖出补单] %s 无法获取快照数据，跳过" % stock)
                continue
            
            position = context.portfolio.positions.get(stock)
            if not position or position.amount <= 0:
                log.info("[实盘卖出补单] %s 已无持仓，无需补单" % stock)
                continue
            
            current_price = snapshot.get('last_px', 0)
            if current_price <= 0:
                log.warning("[实盘卖出补单] %s 当前价格无效，跳过" % stock)
                continue
            
            limit_price = current_price * 0.99
            down_px = snapshot.get('down_px', current_price * 0.9)
            limit_price = max(limit_price, down_px)
            limit_price = round(limit_price, 2)
            
            log.info("[实盘卖出补单] 重新卖出(第%d次): %s, 持仓=%d股, 当前价=%.2f, 限价=%.2f (-1%%), 原因=%s" % 
                    (retry_count, stock, position.amount, current_price, limit_price, reason))
            
            try:
                new_order_id = order_target(stock, 0, limit_price=limit_price)
                if new_order_id:
                    g.sell_orders[new_order_id] = {
                        'stock': stock,
                        'reason': reason,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': retry_count
                    }
                    g.stock_retry_count[stock] = retry_count
                    
                    log.info("[实盘卖出补单] 补单成功，新订单ID=%s，将继续追踪（%s总重试%d/%d）" % 
                            (new_order_id, stock, retry_count, g.max_retry_count))
                else:
                    log.warning("[实盘卖出补单] %s 补单失败，order_target返回空" % stock)
            except Exception as e:
                log.error("[实盘卖出补单] 补单异常: stock=%s, error=%s" % (stock, e))
    
    if g.sell_orders:
        log.info("[实盘卖出补单] 当前仍在追踪的订单数: %d" % len(g.sell_orders))
    else:
        log.info("[实盘卖出补单] 所有订单已处理完成，无需继续追踪")


def sell_task(context):
    """
    执行卖出操作的函数。
    13:00 只做止损（不止盈），14:55 做止损+止盈+常规卖出，减少卖飞。
    """
    current_time = context.blotter.current_dt
    log.info("--- %s: 触发卖出任务 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    
    is_afternoon_session = current_time.hour >= 14
    if is_afternoon_session:
        log.info("[14:55时段] 执行完整卖出逻辑（止损+止盈+常规卖出）")
    else:
        log.info("[13:00时段] 仅执行止损逻辑（不止盈，减少卖飞）")
    
    positions = context.portfolio.positions
    if not positions:
        log.info("当前无持仓，无需卖出。")
        return

    sellable_stocks = [
        code for code in positions
        if code not in g.today_bought_stocks and code not in g.today_sold_stocks
    ]
    if not sellable_stocks:
        log.info("无符合T+1规则的可卖出持仓。")
        return

    log.info("当前可卖出持仓股票: %s" % sellable_stocks)
    
    # 主动获取当前价格数据
    price_data = {}
    if is_trade():
        snapshots = get_snapshot(sellable_stocks)
        for code in sellable_stocks:
            if code in snapshots:
                price_data[code] = {
                    'price': snapshots[code].get('last_px', 0),
                    'preclose': snapshots[code].get('preclose_px', 0),
                    'open_px': snapshots[code].get('open_px', 0),
                }
    else:
        df_price = get_history(count=1, frequency='1m', field='price', security_list=sellable_stocks, include=True)
        df_preclose = get_history(count=1, frequency='1d', field='close', security_list=sellable_stocks)
        df_open = get_history(count=1, frequency='1d', field='open', security_list=sellable_stocks, include=True)
        for code in sellable_stocks:
            price_series = df_price.query('code == "%s"' % code)
            preclose_series = df_preclose.query('code == "%s"' % code)
            open_val = 0
            try:
                os = df_open.query('code == "%s"' % code)
                if not os.empty:
                    open_val = os['open'].iloc[0]
            except Exception:
                open_val = 0
            if not price_series.empty and not preclose_series.empty:
                price_data[code] = {
                    'price': price_series['price'].iloc[0],
                    'preclose': preclose_series['close'].iloc[0],
                    'open_px': open_val,
                }

    # 1. 执行增强版动态止损
    stop_only = not is_afternoon_session
    remaining_stocks = []
    for code in sellable_stocks:
        if code not in price_data: continue
        should_sell, reason = enhanced_dynamic_stop_loss(context, code, price_data, stop_only=stop_only)
        if should_sell:
            log.info("触发动态止损卖出 %s: %s" % (code, reason))
            order_id = order_target(code, 0)
            if order_id:
                g.today_sold_stocks.add(code)
                if is_trade():
                    g.sell_orders[order_id] = {
                        'stock': code,
                        'reason': '动态止损: ' + reason,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0
                    }
        else:
            remaining_stocks.append(code)

    if not remaining_stocks:
        log.info("动态止损已处理所有可卖出持仓。")
        return

    # 2. 对剩余持仓执行常规卖出逻辑（仅14:55时段执行）
    if not is_afternoon_session:
        log.info("[13:00时段] 跳过常规卖出逻辑，等待14:55再判断止盈")
        return
    
    log.info("[14:55时段] 对剩余持仓执行常规卖出逻辑: %s" % remaining_stocks)
    for code in remaining_stocks:
        if code not in price_data: continue
        should_sell, reason = regular_sell_condition(context, code, price_data)
        if should_sell:
            log.info("满足常规卖出条件，执行卖出 %s: %s" % (code, reason))
            order_id = order_target(code, 0)
            if order_id:
                g.today_sold_stocks.add(code)
                if is_trade():
                    g.sell_orders[order_id] = {
                        'stock': code,
                        'reason': '常规卖出: ' + reason,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0
                    }


# ================================== 选股与过滤函数 =================================

def get_index_pool_excluding_st(context):
    """获取中小综指(399101)成分股并过滤ST股"""
    current_date = context.blotter.current_dt.strftime('%Y%m%d')
    
    # 获取中小综指成分股
    stocks = get_index_stocks('399101.SZ', date=current_date)
    
    if not stocks:
        log.warning("获取中小综指成分股失败")
        return []
    
    # 批量获取股票状态，过滤ST
    st_status = get_stock_status(stocks, 'ST', current_date)
    
    final_list = [code for code in stocks if not st_status.get(code, False)]
    return final_list


def rzq_filter(context, stocks):
    """核心筛选：前日涨停 + 昨日不涨停（弱转强形态）
    
    逻辑与聚宽原版rzq_list完全一致：
    1. 获取近2天的收盘价和涨停价
    2. 按code分组，每组2行：第1行=前日，第2行=昨日
    3. 前日收盘==涨停价 且 昨日收盘!=涨停价 → 弱转强
    """
    if not stocks:
        return []

    try:
        # 获取近2天的收盘价和涨停价
        hist_df = get_history(count=2, frequency='1d', field=['close', 'high_limit'], security_list=stocks)
        
        if hist_df is None or hist_df.empty:
            log.warning("无法获取历史数据，跳过弱转强筛选")
            return []

        # 按code分组，逐只判断
        valid_stocks = []
        for code in stocks:
            stock_data = hist_df[hist_df['code'] == code] if 'code' in hist_df.columns else hist_df
            
            if len(stock_data) < 2:
                continue
            
            # get_history按时间升序排列：第1行=前日，第2行=昨日
            day_before_close = stock_data['close'].iloc[0]
            day_before_hl = stock_data['high_limit'].iloc[0]
            yesterday_close = stock_data['close'].iloc[1]
            yesterday_hl = stock_data['high_limit'].iloc[1]
            
            # 前日涨停 且 昨日不涨停
            if day_before_close == day_before_hl and yesterday_close != yesterday_hl:
                valid_stocks.append(code)
        
        return valid_stocks
        
    except Exception as e:
        log.error("rzq_filter error: %s" % e)
        return []


def gjt_filter(context, stocks):
    """国九条筛选：净利润>0，营业收入>1亿
    
    与聚宽原版GJT_filter_stocks逻辑一致
    """
    if not stocks:
        return []

    try:
        yesterday_str = context.previous_date.strftime('%Y-%m-%d')
        
        # PTrade用法: get_fundamentals(security, table, fields, date)
        df = get_fundamentals(stocks, 'income_statement',
                              fields=['net_profit', 'operating_revenue'],
                              date=yesterday_str)
        
        if df is None or df.empty:
            log.warning("国九条筛选：获取财务数据失败，跳过筛选")
            return stocks
        
        # 处理code列 - PTrade可能返回不同列名
        df = df.reset_index()
        code_col = None
        for c in ['secu_code', 'code', 'index', 'level_0']:
            if c in df.columns:
                code_col = c
                break
        if code_col and code_col != 'code':
            df = df.rename(columns={code_col: 'code'})
        
        # 筛选条件：净利润>0 且 营业收入>1亿
        df = df[(df['net_profit'] > g.min_net_profit) & 
                (df['operating_revenue'] > g.min_operating_revenue)]
        
        if 'code' in df.columns:
            return df['code'].tolist()
        else:
            return stocks
        
    except Exception as e:
        log.warning("国九条筛选异常: %s" % e)
        return stocks


def is_avoid_period(context):
    """判断是否在1、4、12月空仓期"""
    current_date = context.blotter.current_dt
    month = current_date.month
    day = current_date.day
    
    if month in [1, 4, 12] and day >= 15:
        return True
    return False


def technical_filter(context, stocks):
    """技术指标筛选
    
    条件（与聚宽原版filter_stocks一致）：
    1. 收盘价 > MA均线
    2. 收盘价 > 前日最低价
    3. 成交量 > 前日成交量（放量）
    4. 成交量 < 前日成交量 * N倍（不过量）
    5. 股价 > 1元
    """
    if not stocks:
        return []

    try:
        hist_df = get_history(g.ma_period + 1, '1d', ['close', 'low', 'volume'], security_list=stocks)
        if hist_df.empty:
            return []

        valid_stocks = []
        for code in stocks:
            df = hist_df.query('code == "%s"' % code)
            if len(df) < g.ma_period + 1:
                continue

            ma = df['close'].rolling(g.ma_period).mean().iloc[-1]
            last_close = df['close'].iloc[-1]
            prev_low = df['low'].iloc[-2]
            last_volume = df['volume'].iloc[-1]
            prev_volume = df['volume'].iloc[-2]

            if (last_close > ma and
                last_close > prev_low and
                last_volume > prev_volume and
                last_volume < g.volume_ratio_threshold * prev_volume and
                last_close > 1):
                valid_stocks.append(code)
        return valid_stocks
    except Exception as e:
        log.error("技术指标筛选失败: %s" % e)
        return []


def _apply_industry_diversification(context, ranked_codes, max_per_ind=None):
    """【退学炒股思想】同行业分散限制：同一行业最多 max_per_ind 只
    
    结合当前持仓一起计算，避免新买+已持同行业过度集中。
    """
    if not ranked_codes:
        return ranked_codes
    
    if max_per_ind is None:
        max_per_ind = getattr(g, 'max_per_industry', 2)
    
    try:
        # 获取当前持仓行业计数
        ind_count = {}
        current_positions = [c for c, p in context.portfolio.positions.items() if getattr(p, 'amount', 0) > 0]
        all_codes_for_query = list(set(ranked_codes + current_positions))
        
        ind_map = {}
        try:
            info = get_stock_info(all_codes_for_query, field=['industry'])
            if isinstance(info, dict):
                for c, v in info.items():
                    if isinstance(v, dict):
                        ind_map[c] = v.get('industry', '未知')
                    else:
                        ind_map[c] = str(v) if v else '未知'
        except Exception as e:
            log.warning("[行业分散] 获取行业信息失败: %s，跳过行业分散" % e)
            return ranked_codes
        
        # 已持仓股票占用行业名额
        for code in current_positions:
            ind = ind_map.get(code, '未知')
            ind_count[ind] = ind_count.get(ind, 0) + 1
        
        # 按排序贪心选取
        diversified = []
        skipped = []
        for code in ranked_codes:
            ind = ind_map.get(code, '未知')
            if ind_count.get(ind, 0) >= max_per_ind:
                skipped.append((code, ind))
                continue
            diversified.append(code)
            ind_count[ind] = ind_count.get(ind, 0) + 1
        
        if skipped:
            log.info("[行业分散] 已过滤 %d 只同行业超限股票: %s" % 
                    (len(skipped), ', '.join(['%s(%s)' % (c, i) for c, i in skipped[:5]])))
        return diversified
    except Exception as e:
        log.warning("[行业分散] 异常，跳过: %s" % e)
        return ranked_codes


def backtest_opening_filter_and_rank(context, stocks):
    """[回测专用] 开盘价筛选和排序"""
    if not stocks:
        return []

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
    turnover_dict = get_turnover_ratio_yesterday(stocks)

    # 1. 集合竞价数据
    try:
        trend_data = get_trend_data(stocks=stocks)
    except Exception:
        trend_data = {}

    # 2. 首分钟K线和昨日收盘价
    df_open_data = get_history(count=1, frequency='1m', field='open', security_list=stocks, include=True)
    df_preclose_data = get_history(count=1, frequency='1d', field='close', security_list=stocks)

    if not isinstance(df_preclose_data, pd.DataFrame) or df_preclose_data.empty:
        log.warning("[回测]无法获取昨日收盘价数据。")
        return []

    results = []
    for code in stocks:
        try:
            stock_preclose_data = df_preclose_data.query('code == "%s"' % code)
            if stock_preclose_data.empty:
                continue
            prev_close = stock_preclose_data['close'].iloc[0]
            if prev_close <= 0:
                continue

            # 优先使用集合竞价价格
            open_now = 0.0
            td = trend_data.get(code) if isinstance(trend_data, dict) else None
            if td:
                open_now = td.get('hq_px') or td.get('wavg_px') or 0.0

            # 退回到首分钟K线open
            if open_now <= 0:
                if not isinstance(df_open_data, pd.DataFrame) or df_open_data.empty:
                    continue
                stock_open_data = df_open_data.query('code == "%s"' % code)
                if stock_open_data.empty:
                    continue
                open_now = stock_open_data['open'].iloc[0]

            open_ratio = open_now / prev_close
            if not (g.open_down_threshold < open_ratio < g.open_up_threshold):
                continue

            turnover = turnover_dict.get(code, 0.0)
            factor = turnover * open_ratio
            log.info("[回测]排序因子: %s open_ratio=%.4f turnover=%.4f factor=%.6f" % (code, open_ratio, turnover, factor))
            results.append({'code': code, 'factor': factor})
        except Exception as e:
            log.warning("[回测]计算因子失败 for %s: %s" % (code, e))
            continue

    if not results:
        return []

    df_sorted = pd.DataFrame(results).sort_values(by='factor', ascending=False)
    ranked = df_sorted['code'].tolist()
    # 【退学炒股思想】行业分散
    ranked = _apply_industry_diversification(context, ranked)
    return ranked

def real_trade_opening_filter_and_rank(context, snapshots, stocks):
    """[实盘专用] 集合竞价数据筛选和排序，并增加BS(买卖盘)过滤"""
    if not stocks:
        return []

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
    turnover_dict = get_turnover_ratio_yesterday(stocks)

    results = []
    for code in stocks:
        try:
            snapshot = snapshots.get(code)
            if not snapshot:
                continue
            
            open_now = snapshot.get('open_px', 0)
            if open_now == 0:
                open_now = snapshot.get('last_px', 0)
            
            prev_close = snapshot.get('preclose_px', 0)
            if prev_close <= 0 or open_now <= 0:
                continue

            open_ratio = open_now / prev_close
            if not (g.open_down_threshold < open_ratio < g.open_up_threshold):
                continue

            # BS(买卖盘)过滤
            bid_grp = snapshot.get('bid_grp', {})
            offer_grp = snapshot.get('offer_grp', {})
            entrust_rate = snapshot.get('entrust_rate', 0)
            
            bid_qty_total = 0
            offer_qty_total = 0
            if isinstance(bid_grp, dict):
                for level in range(1, 6):
                    info = bid_grp.get(level)
                    if isinstance(info, list) and len(info) >= 2:
                        bid_qty_total += info[1]
            if isinstance(offer_grp, dict):
                for level in range(1, 6):
                    info = offer_grp.get(level)
                    if isinstance(info, list) and len(info) >= 2:
                        offer_qty_total += info[1]
            
            bs_filter_passed = False
            bs_data_available = (bid_qty_total > 0 or offer_qty_total > 0 or entrust_rate != 0)
            
            if bs_data_available:
                if bid_qty_total > offer_qty_total:
                    bs_filter_passed = True
                if entrust_rate > 0:
                    bs_filter_passed = True
                
                if not bs_filter_passed:
                    log.info("[实盘]BS过滤: %s bid_total=%d offer_total=%d entrust_rate=%.2f 卖压过大被过滤" % 
                            (code, bid_qty_total, offer_qty_total, entrust_rate))
                    continue
            else:
                log.warning("[实盘]BS数据缺失: %s 跳过BS过滤" % code)
                bs_filter_passed = True

            turnover = turnover_dict.get(code, 0.0)
            factor = turnover * open_ratio
            log.info("[实盘]排序因子: %s open_ratio=%.4f turnover=%.4f factor=%.6f bid_total=%d offer_total=%d entrust_rate=%.2f" % 
                    (code, open_ratio, turnover, factor, bid_qty_total, offer_qty_total, entrust_rate))
            results.append({'code': code, 'factor': factor})
        except Exception as e:
            log.warning("[实盘]计算因子失败 for %s: %s" % (code, e))
            continue

    if not results:
        return []
    
    df_sorted = pd.DataFrame(results).sort_values(by='factor', ascending=False)
    ranked = df_sorted['code'].tolist()
    # 【退学炒股思想】行业分散
    ranked = _apply_industry_diversification(context, ranked)
    return ranked


def get_turnover_ratio_yesterday(stocks):
    """获取上一交易日的换手率字典"""
    if not stocks:
        return {}

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')

    try:
        valuation_df = get_fundamentals(
            stocks,
            'valuation',
            'turnover_rate',
            yesterday_str
        )
    except Exception as e:
        log.error("[换手率] get_fundamentals异常: %s" % str(e))
        valuation_df = pd.DataFrame()

    result = {}
    for code in stocks:
        turnover = 0.0
        try:
            if isinstance(valuation_df, pd.DataFrame) and not valuation_df.empty:
                if code in valuation_df.index and 'turnover_rate' in valuation_df.columns:
                    raw = valuation_df.loc[code, 'turnover_rate']
                    if isinstance(raw, str):
                        if '%' in raw:
                            turnover = float(raw.strip('%')) / 100.0
                        elif raw != '' and raw != 'nan':
                            try:
                                turnover = float(raw)
                            except:
                                turnover = 0.0
                    elif raw is not None and not pd.isna(raw):
                        try:
                            turnover = float(raw)
                        except:
                            turnover = 0.0
        except Exception as e:
            log.error("[换手率] %s 解析异常: %s" % (code, str(e)))
            turnover = 0.0
        
        result[code] = turnover

    return result


def enhanced_dynamic_stop_loss(context, stock, price_data, stop_only=False):
    """增强版动态止损系统
    
    Args:
        stop_only: 为True时只执行止损逻辑，不执行移动止盈（用于13:00时段减少卖飞）
    """
    position = context.portfolio.positions.get(stock)
    if not position:
        return False, ""

    cost_price = position.cost_basis
    current_price = price_data[stock]['price']
    pre_close = price_data[stock].get('preclose', 0)
    if cost_price <= 0 or current_price <= 0:
        return False, ""
    profit_rate = (current_price - cost_price) / cost_price

    # 0. 【退学炒股思想】T+1次日弱势必走 - "次日不及预期必走"
    if getattr(g, 'enable_t1_weak_exit', False):
        hold_days_tmp = get_hold_days(context, stock)
        if hold_days_tmp == 1 and pre_close > 0:
            # 次日开盘低开超阈值 → 立即走（不扛跌）
            open_px = price_data[stock].get('open_px', current_price)
            if open_px > 0:
                open_chg = (open_px - pre_close) / pre_close
                if open_chg <= g.t1_weak_open_threshold:
                    return True, "T+1次日低开%.2f%%不及预期必走" % (open_chg * 100)
            # 次日早盘跌破昨收*阈值 → 立即走
            price_ratio = current_price / pre_close
            if price_ratio < g.t1_weak_price_threshold:
                return True, "T+1次日跌破昨收%.2f%% 不及预期必走" % ((price_ratio - 1) * 100)

    # 更新最高价
    record = g.buy_records.get(stock, {})
    highest_price = record.get('highest_price', cost_price)
    if current_price > highest_price:
        record['highest_price'] = current_price
        g.buy_records[stock] = record
        highest_price = current_price

    hold_days = get_hold_days(context, stock)

    # 1. 基于持仓时间的阶梯止损
    if hold_days <= 1 and profit_rate <= -0.015:
        return True, "日内快速止损(%.1f%%)" % (profit_rate * 100)
    if hold_days <= 3 and profit_rate <= -0.025:
        return True, "短期止损(%.1f%%)" % (profit_rate * 100)

    # 2. 基于ATR的动态止损
    atr_stop_price = calculate_atr_stop_loss(context, stock, cost_price)
    if current_price <= atr_stop_price:
        return True, "ATR动态止损 (止损价: %.2f)" % atr_stop_price

    # 3. 基于大盘环境的止损
    if market_condition_stop_loss(context, profit_rate):
        return True, "大盘环境恶化止损"

    # 4. 移动止盈（13:00时段stop_only=True时跳过）
    if not stop_only:
        if profit_rate > 0.05:
            if (highest_price - current_price) / highest_price >= 0.03:
                return True, "盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (profit_rate * 100, (highest_price - current_price) / highest_price * 100)
        if profit_rate > 0.10:
            if (highest_price - current_price) / highest_price >= 0.04:
                return True, "高盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (profit_rate * 100, (highest_price - current_price) / highest_price * 100)

    return False, ""


def regular_sell_condition(context, code, price_data):
    """常规卖出条件：未涨停 且 (跌破MA7 或 盈利>0% 或 昨日涨停)"""
    try:
        position = context.portfolio.positions.get(code)
        if not position:
            return False, ""

        last_price = price_data[code]['price']
        pre_close = price_data[code]['preclose']
        avg_cost = position.cost_basis
        
        if last_price <= 0 or pre_close <= 0:
            return False, "价格数据无效"

        # 条件1: 当日未涨停
        high_limit_today, _ = calculate_limit_prices(code, pre_close)
        if high_limit_today is None or last_price >= high_limit_today * 0.999:
            return False, "已涨停或接近涨停"

        # 均线计算
        hist_df = get_history(g.stop_loss_ma_period, '1d', 'close', security_list=code, include=False)
        if len(hist_df) < g.stop_loss_ma_period:
            ma_stop = -1
        else:
            ma_stop = hist_df['close'].mean()

        cond2_1 = (ma_stop > 0) and (last_price < ma_stop)

        # 条件2.2: 盈利>0
        cond2_2 = last_price > avg_cost

        # 条件2.3: 昨日是涨停板
        yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
        day_before_str = get_trading_day(-2).strftime('%Y-%m-%d')
        close_dby_df = get_price(code, end_date=day_before_str, frequency='1d', fields='close', count=1)
        close_y_df = get_price(code, end_date=yesterday_str, frequency='1d', fields='close', count=1)
        
        cond2_3 = False
        if not close_dby_df.empty and not close_y_df.empty:
            close_dby = close_dby_df['close'].iloc[0]
            close_y = close_y_df['close'].iloc[0]
            high_limit_yesterday, _ = calculate_limit_prices(code, close_dby)
            if high_limit_yesterday is not None and abs(close_y - high_limit_yesterday) < 0.01:
                cond2_3 = True

        if cond2_1: return True, "跌破%d日线(MA=%.2f)" % (g.stop_loss_ma_period, ma_stop)
        if cond2_2: return True, "盈利卖出"
        if cond2_3: return True, "昨日涨停卖出"

        return False, ""
    except Exception as e:
        log.error("常规卖出条件判断异常 for %s: %s" % (code, e))
        return False, ""


# ================================== 辅助与工具函数 =================================

def get_hold_days(context, stock):
    """计算持仓天数"""
    record = g.buy_records.get(stock)
    if record and 'buy_date' in record:
        try:
            buy_date = pd.to_datetime(record['buy_date']).date()
            current_date = context.blotter.current_dt.date()
            trade_days = get_trade_days(start_date=buy_date.strftime('%Y%m%d'), end_date=current_date.strftime('%Y%m%d'))
            return len(trade_days)
        except:
            return 999
    return 999


def calculate_atr_stop_loss(context, stock, cost_price):
    """基于ATR计算动态止损价"""
    try:
        hist_df = get_history(15, '1d', ['high', 'low', 'close'], security_list=stock)
        if len(hist_df) < 15:
            return cost_price * 0.97

        df = hist_df.copy()
        df['tr1'] = df['high'] - df['low']
        df['tr2'] = abs(df['high'] - df['close'].shift(1))
        df['tr3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        atr = df['tr'].rolling(14).mean().iloc[-1]

        return cost_price - (atr * 2)
    except Exception:
        return cost_price * 0.97


def market_condition_stop_loss(context, profit_rate):
    """基于市场整体状况的止损"""
    try:
        index_df = get_history(4, '1d', 'close', security_list='000001.SS')
        if len(index_df) < 4:
            return False

        pct_change = index_df['close'].pct_change().dropna()
        if (pct_change < -0.005).all() and profit_rate < 0.02:
            return True
        return False
    except Exception:
        return False


def calculate_limit_prices(stock_code, pre_close):
    """计算股票的涨跌停价"""
    if pre_close <= 0:
        return None, None
    
    stock_info = get_stock_info(stock_code)
    stock_name = stock_info.get(stock_code, {}).get('stock_name', '')
    
    if stock_code.startswith('688') or stock_code.startswith('300'):
        limit_pct = 0.20
    elif 'ST' in stock_name:
        limit_pct = 0.05
    else:
        limit_pct = 0.10
        
    high_limit = round(pre_close * (1 + limit_pct), 2)
    low_limit = round(pre_close * (1 - limit_pct), 2)
    return high_limit, low_limit


def daily_trading_report(context):
    """每日交易报告"""
    log.info("=" * 80)
    current_date = context.blotter.current_dt.strftime('%Y-%m-%d')
    portfolio = context.portfolio
    total_value = portfolio.portfolio_value
    cash = portfolio.cash
    
    if g.last_total_value > 0:
        daily_return = total_value - g.last_total_value
        daily_return_pct = (daily_return / g.last_total_value * 100) if g.last_total_value > 0 else 0
    else:
        g.last_total_value = context.capital_base
        daily_return, daily_return_pct = 0, 0
    
    g.last_total_value = total_value
    
    log.info("【%s 交易报告】" % current_date)
    log.info("总资产: %.2f元 | 现金: %.2f元" % (total_value, cash))
    log.info("当日收益: %+.2f元 (%+.2f%%)" % (daily_return, daily_return_pct))
    log.info("【当日买入】%d只: %s" % (len(g.today_bought_stocks), ', '.join(g.today_bought_stocks) or '无'))
    log.info("【当日卖出】%d只: %s" % (len(g.today_sold_stocks), ', '.join(g.today_sold_stocks) or '无'))
    
    if portfolio.positions:
        log.info("【持仓详情】共%d只股票" % len(portfolio.positions))
        position_codes = list(portfolio.positions.keys())
        stock_names = get_stock_name(position_codes)
        for code, pos in portfolio.positions.items():
            name = stock_names.get(code, code)
            profit_loss = (pos.last_sale_price - pos.cost_basis) * pos.amount
            cost_value = pos.cost_basis * pos.amount
            profit_loss_pct = (profit_loss / cost_value * 100) if cost_value != 0 else 0
            profit_loss_str = "%+.2f元 (%+.2f%%)" % (profit_loss, profit_loss_pct)
            log.info("  %s(%s): %d股 | 成本:%.2f | 现价:%.2f | 市值:%.2f元 | 盈亏:%s" % (
                name, code, pos.amount, pos.cost_basis, pos.last_sale_price, pos.amount * pos.last_sale_price, profit_loss_str
            ))
    else:
        log.info("【持仓详情】当前无持仓")
    log.info("=" * 80)


def update_buy_records(context):
    """收盘后更新或清理持仓记录"""
    current_positions = list(context.portfolio.positions.keys())
    
    for code in g.today_bought_stocks:
        if code in current_positions and code not in g.buy_records:
            position = context.portfolio.positions[code]
            g.buy_records[code] = {
                'buy_date': context.blotter.current_dt.strftime('%Y-%m-%d'),
                'highest_price': position.last_sale_price
            }
            
    current_records = list(g.buy_records.keys())
    for code in current_records:
        if code not in current_positions:
            del g.buy_records[code]
            
    log.info("收盘后更新持仓记录: %s" % g.buy_records)


# ================================== 【退学炒股思想】情绪周期判断 =================================

def get_market_regime(context):
    """【退学炒股思想】判断当前市场情绪周期状态
    
    根据退学炒股理论，返回市场状态：
    - 'CRASH' 崩溃/系统性风险 → 强制空仓
    - 'ICE' 冰点 → 空仓等待（跳过买入）
    - 'DIVERGENCE' 分歧期 → 竞仱持仓
    - 'REPAIR' 修复期 → 小仓试水
    - 'CONSENSUS' 一致期 → 满仓出击
    - 'NORMAL' 无法判断 → 正常交易
    
    基于两个可获取的指标：
    1. 上证指数走势（000001.SS）
    2. 全市场涨跌家数比例（换代涉股涨跌家数/涨停家数指标）
    """
    try:
        # 1. 获取上证指数近20日走势
        index_df = get_history(25, '1d', ['close', 'open', 'high', 'low'], security_list='000001.SS')
        if index_df is None or len(index_df) < 20:
            return 'NORMAL', {'reason': '指数数据不足'}
        
        close = index_df['close'].values.astype(float)
        last_close = close[-1]
        prev_close = close[-2]
        ma20 = np.mean(close[-20:])
        ma5 = np.mean(close[-5:])
        
        # 昨日涨跌幅
        index_chg = (last_close - prev_close) / prev_close if prev_close > 0 else 0
        
        detail = {
            'index_close': float(last_close),
            'index_chg': float(index_chg * 100),
            'ma5': float(ma5),
            'ma20': float(ma20),
            'ma5_vs_ma20': float((ma5 / ma20 - 1) * 100) if ma20 > 0 else 0,
        }
        
        # ---- 判断1: 崩溃信号（指数跳水破ma20 or 单日暴跌）----
        # 上证跌破MA20 + 近3日至少2天阴线 → 崩溃
        if g.regime_crash_ma20_break:
            if last_close < ma20:
                # 近3日有几天阴线
                recent_3 = close[-3:]
                down_days = sum(1 for i in range(1, 3) if recent_3[i] < recent_3[i-1])
                if down_days >= 2 or index_chg < g.regime_crash_index_drop:
                    detail['signal'] = 'MA20破位+连续阴线'
                    return 'CRASH', detail
        
        # 单日暴跌
        if index_chg < g.regime_crash_index_drop:
            detail['signal'] = '单日指数大跌%.2f%%' % (index_chg * 100)
            return 'CRASH', detail
        
        # ---- 判断2: 冰点信号（全市场涨家占比過低）----
        # 尝试用中小综指成分股估算涨家占比
        try:
            today_str = context.blotter.current_dt.strftime('%Y%m%d')
            sample_stocks = get_index_stocks('399101.SZ', date=today_str)[:500]  # 采样500只
            if sample_stocks:
                df_chg = get_history(2, '1d', 'close', security_list=sample_stocks)
                if df_chg is not None and not df_chg.empty:
                    up_count = 0
                    total_count = 0
                    for sc in sample_stocks:
                        sdf = df_chg.query('code == "%s"' % sc)
                        if len(sdf) >= 2:
                            c_prev = sdf['close'].iloc[0]
                            c_now = sdf['close'].iloc[1]
                            if c_prev > 0:
                                total_count += 1
                                if c_now > c_prev:
                                    up_count += 1
                    if total_count > 50:
                        up_ratio = up_count / total_count
                        detail['up_ratio'] = float(up_ratio)
                        # 涨家占比過低 → 冰点
                        if up_ratio < g.regime_ice_up_ratio:
                            detail['signal'] = '市场涨家占比仅%.1f%% → 冰点' % (up_ratio * 100)
                            return 'ICE', detail
                        elif up_ratio > 0.65:
                            detail['signal'] = '市场涨家占比%.1f%% → 一致期' % (up_ratio * 100)
                            return 'CONSENSUS', detail
        except Exception as e:
            detail['up_ratio_err'] = str(e)
        
        # ---- 判断3: 分歧/修复/正常 ----
        if ma5 > ma20 * 1.01:
            detail['signal'] = 'MA5在MA20上方→分歧期'
            return 'DIVERGENCE', detail
        elif ma5 > ma20:
            detail['signal'] = 'MA5略强于MA20→修复期'
            return 'REPAIR', detail
        
        detail['signal'] = '正常运行'
        return 'NORMAL', detail
        
    except Exception as e:
        log.warning("get_market_regime 异常: %s" % e)
        return 'NORMAL', {'err': str(e)}