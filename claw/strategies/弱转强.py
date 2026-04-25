# -*- coding: utf-8 -*-
import datetime
import numpy as np
import pandas as pd

# ================================== 策略函数 ===================================

def initialize(context):
    """
    策略初始化函数，只在策略启动时运行一次。
    """
    # --- 授权验证 ---
    # 在实盘交易中验证授权（回测不执行）
    if is_trade():
        # 【方式1：授权模式下载（加密保护）】★ 推荐
        # 直接调用 permission_test()，不传入任何参数
        # 使用 PTrade 授权工具对策略进行加密，授权信息会自动嵌入策略文件
        auth_result = permission_test()
        
        # 【方式2：普通授权验证（不加密）】
        # 手动指定账户和有效期，策略文件不加密
        # auth_result = permission_test(account='YOUR_ACCOUNT', end_date='20251231')
        
        # 授权失败则终止策略
        if not auth_result:
            log.error("="*50)
            log.error("【授权失败】策略无权在当前账户或时间运行！")
            log.error("请检查：1. 账户是否匹配  2. 是否已过有效期")
            log.error("="*50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，策略启动成功")
    
    # --- 策略参数 ---
    # 持仓股票数量
    g.stock_num = 4
    # 是否开启1、4、12月空仓规则
    g.avoid_jan_apr_dec = False
    # 开盘价筛选阈值
    g.open_down_threshold = 0.97
    g.open_up_threshold = 1.03
    # 技术指标参数
    g.ma_period = 10               # 均线周期
    g.volume_ratio_threshold = 10  # 成交量倍数上限
    # 卖出均线周期
    g.stop_loss_ma_period = 7
    # 单股最大买入金额限制
    g.max_single_stock_amount = 100000  # 1万元

    # --- 内部状态变量 ---
    # 当日待选股票池
    g.today_list = []
    # 记录持仓股的买入信息，用于动态止损 {code: {'buy_date': 'YYYY-MM-DD', 'highest_price': 10.0}}
    g.buy_records = {}
    # 当日已买入/卖出的股票，用于T+1和防止重复操作
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    # 上一交易日的总资产，用于计算每日收益
    g.last_total_value = 0.0
    
    # --- 订单管理全局变量 ---
    # 实盘买入订单记录 {order_id: {'stock': code, 'cash': amount, 'time': 'HH:MM:SS', 'retry_count': 0}}
    g.buy_orders = {}
    # 实盘卖出订单记录 {order_id: {'stock': code, 'reason': '卖出原因', 'time': 'HH:MM:SS', 'retry_count': 0}}
    g.sell_orders = {}
    # 当日待买入股票及重试记录 {stock: {'retry_count': 0, 'cash': 10000, 'last_try_time': None}}
    g.pending_buy_stocks = {}
    # 每个股票的总重试次数追踪 {stock: total_retry_count}
    g.stock_retry_count = {}  # 新增：按股票代码追踪总重试次数
    # 订单检查重试配置
    g.max_retry_count = 10  # 最多重试10次（增加到10次）
    g.order_check_interval = 3  # 每3秒检查一次
    
    # --- 注册所有定时任务 ---
    # 实盘在集合竞价结束后买入（9:25:10，留10秒缓冲时间确保数据推送完成）
    run_daily(context, real_trade_buy_task, time='09:25:10')
    # 实盘订单检查与补单（每3秒执行一次，覆盖全天交易时段）
    run_interval(context, check_and_retry_orders, seconds=3)
    # 回测在开盘后第一个bar模拟买入
    run_daily(context, backtest_buy_task, time='09:31')
    # 注册两个卖出时间点的任务
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
        g.stock_retry_count.clear()  # 重置每个股票的重试次数
        log.info("[Ptrade实盘] 订单追踪记录已清空")
    
    log.info("=" * 50)
    log.info("新交易日开始，重置每日状态变量。")

    # 2. 判断是否在空仓期
    if g.avoid_jan_apr_dec and is_avoid_period(context):
        log.info("当前处于1、4、12月空仓期，今日不进行选股和交易。")
        set_universe([]) # 清空股票池
        return

    # 3. 执行选股逻辑
    log.info(">>> 开始执行盘前选股任务...")
    # 3.1 获取基础股票池：全A股，剔除创业板和ST
    stock_pool = get_all_stocks_excluding_gem(context)
    log.info("全A股(去创业板/ST)数量: %d" % len(stock_pool))

    # 3.2 核心筛选：昨日炸板
    stock_pool = yesterday_bomb_filter(context, stock_pool)
    log.info("昨日炸板筛选后数量: %d" % len(stock_pool))

    # 3.3 技术指标筛选
    stock_pool = technical_filter(context, stock_pool)
    log.info("技术指标筛选后数量: %d" % len(stock_pool))

    # 3.4 将初选结果存入全局变量
    g.today_list = stock_pool
    log.info("盘前选股完成，待买池数量: %d" % len(g.today_list))

    # 4. 更新股票池，以便在handle_data或实盘任务中接收这些股票的行情
    # 同时订阅持仓股，以便卖出逻辑能获取行情
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
    # 持久化买入记录
    update_buy_records(context)


# ================================== 业务逻辑函数 ===================================

def real_trade_buy_task(context):
    """
    [实盘专用] 在集合竞价期间执行买入操作的函数。
    """
    if not is_trade(): # 确保此函数只在实盘运行
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
    # 只统计持仓量大于0的标的，避免把已归零的逆回购等算作一个持仓
    current_positions = [code for code, pos in context.portfolio.positions.items() if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        return

    # 过滤已持仓的股票
    buy_list = [s for s in buy_list if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[实盘]候选股票均已持仓，无新的可买入股票。")
        return

    # 3. 执行买入
    cash_per_stock = context.portfolio.cash / len(buy_list)
    for stock in buy_list:
        if cash_per_stock > 0:
            # 应用单股最大买入金额限制
            actual_cash = min(g.max_single_stock_amount, cash_per_stock)
            # 使用快照中的最新价作为委托价参考
            price_ref = snapshots.get(stock, {}).get('last_px', 0)
            if price_ref > 0:
                # 使用现价+1%下单，但不超过涨停价
                limit_price = price_ref * 1.01
                up_px = snapshots.get(stock, {}).get('up_px', price_ref * 1.1)
                limit_price = min(limit_price, up_px)  # 不超过涨停价
                
                # 价格取整：A股最小变动单位0.01元，保留2位小数
                limit_price = round(limit_price, 2)
                
                log.info("[实盘]买入 %s, 分配资金: %.2f, 实际使用: %.2f, 现价: %.2f, 委托价: %.2f (+1%%)" % 
                        (stock, cash_per_stock, actual_cash, price_ref, limit_price))
                order_id = order_value(stock, actual_cash, limit_price=limit_price)
                if order_id:
                    g.today_bought_stocks.add(stock)
                    # 记录订单信息，用于后续检查
                    g.buy_orders[order_id] = {
                        'stock': stock,
                        'cash': actual_cash,
                        'limit_price': limit_price,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0  # 初始重试次数为0
                    }
                    log.info("[实盘]订单已提交: order_id=%s, stock=%s, limit_price=%.2f" % (order_id, stock, limit_price))
    
    # 设置订单检查开始标志
    g.order_check_start_time = context.blotter.current_dt

def backtest_buy_task(context):
    """
    [回测专用] 在开盘后执行买入操作的函数。
    """
    if is_trade(): # 确保此函数只在回测运行
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
    # 只统计持仓量大于0的标的，避免把已归零的逆回购等算作一个持仓
    current_positions = [code for code, pos in context.portfolio.positions.items() if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[回测]持仓已满，无需买入。")
        return

    # 过滤已持仓的股票
    buy_list = [s for s in buy_list if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[回测]候选股票均已持仓，无新的可买入股票。")
        return

    # 3. 执行买入
    cash_per_stock = context.portfolio.cash / len(buy_list)
    for stock in buy_list:
        if cash_per_stock > 0:
            log.info("[回测]买入 %s, 分配资金: %.2f" % (stock, cash_per_stock))
            order_id = order_value(stock, cash_per_stock)
            if order_id:
                g.today_bought_stocks.add(stock)




def check_and_retry_orders(context):
    """
    [实盘专用] 统一的订单检查与补单函数。
    由run_interval每3秒执行一次（自动限制在交易时段09:30-11:30, 13:00-15:00），最多重试10次。
    """
    if not is_trade():  # 确保此函数只在实盘运行
        return
    
    
    # 检查买入订单
    if g.buy_orders:
        check_and_retry_buy_orders(context)
    
    # 检查卖出订单
    if g.sell_orders:
        check_and_retry_sell_orders(context)


def check_and_retry_buy_orders(context):
    """
    [实盘专用] 检查买入订单状态并补单的函数。
    由check_and_retry_orders调用，每3秒执行一次，最多重试10次。
    撤单成功后再补单，补单后继续追踪新订单ID。
    """
    if not is_trade():  # 确保此函数只在实盘运行
        return
    
    if not g.buy_orders:
        return  # 无待检查订单，静默返回
    
    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]买入订单检查执行 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    log.info("[实盘买入补单] 待检查订单数量: %d" % len(g.buy_orders))
    
    # 【关键修复】获取当日所有成交记录，用于检测是否已成交（避免重复买入）
    try:
        today_trades = get_trades()  # 获取当日所有成交记录
        # 统计每只股票的总买入成交数量
        stock_filled_amounts = {}
        for trade in today_trades:
            if trade.is_buy:  # 只统计买入成交
                stock_code = trade.security
                if stock_code not in stock_filled_amounts:
                    stock_filled_amounts[stock_code] = 0
                stock_filled_amounts[stock_code] += trade.amount
        log.info("[实盘买入补单] 当日买入成交统计: %s" % stock_filled_amounts)
    except Exception as e:
        log.warning("[实盘买入补单] 获取成交记录失败: %s" % e)
        stock_filled_amounts = {}
    
    # 记录需要从订单列表中移除的订单（已成交或达到重试上限）
    orders_to_remove = []
    # 记录需要补单的股票
    orders_to_retry = []
    
    for order_id, order_info in list(g.buy_orders.items()):
        stock = order_info['stock']
        cash_allocated = order_info['cash']
        retry_count = order_info.get('retry_count', 0)
        
        # 检查该股票的总重试次数（优先级更高）
        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count:
            log.warning("[实盘买入补单] %s 已达到最大重试次数(%d)，停止追踪" % (stock, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        # 也检查单个订单的重试次数（双重保障）
        if retry_count >= g.max_retry_count:
            log.warning("[实盘买入补单] %s 订单%s已达到最大重试次数(%d)，停止追踪" % (stock, order_id, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        try:
            # 检查订单状态（get_order返回list，取第一个元素）
            order_list = get_order(order_id)
            
            if order_list and len(order_list) > 0:
                order_status = order_list[0]
                filled_amount = order_status.filled  # 已成交量
                order_amount = order_status.amount   # 订单总量
                status = order_status.status         # 订单状态
                
                log.info("[实盘买入补单] %s 订单状态: order_id=%s, status=%s, 已成交=%d/总量=%d, 重试次数=%d/%d" % 
                        (stock, order_id, status, filled_amount, order_amount, retry_count, g.max_retry_count))
                
                # 判断订单状态（PTrade状态为数字字符串）
                
                # 【关键修复】检查该股票是否已有成交记录（防止get_order返回错误订单状态导致重复买入）
                if stock in stock_filled_amounts and stock_filled_amounts[stock] > 0:
                    log.warning("[实盘买入补单] %s 检测到当日已有成交记录（%d股），移除订单追踪避免重复买入" % 
                               (stock, stock_filled_amounts[stock]))
                    orders_to_remove.append(order_id)
                    continue
                
                if status == '8':  # '8' = 已成交
                    # ✅ 检查是否在撤单中就成交了
                    is_cancelling = order_info.get('is_cancelling', False)
                    if is_cancelling:
                        # 撤单期间订单成交了，说明撤单请求未生效
                        log.warning("[实盘买入补单] %s 撤单期间订单已成交（撤单请求未生效），移除追踪" % stock)
                    else:
                        log.info("[实盘买入补单] %s 已全部成交，移除追踪" % stock)
                    orders_to_remove.append(order_id)
                    
                elif status == '7':  # '7' = 部成（特殊处理）
                    # 部分成交：需要撤销未成交部分，但要等待撤单确认后再补单
                    if filled_amount > 0 and filled_amount < order_amount:
                        # 计算已成交金额（使用限价估算）
                        limit_price = order_info.get('limit_price', cash_allocated / order_amount)
                        filled_cash = filled_amount * limit_price
                        remaining_cash = cash_allocated - filled_cash  # 剩余资金
                        
                        log.info("[实盘买入补单] %s 部分成交: 已成交%d股(约%.2f元), 剩余%.2f元" % 
                                (stock, filled_amount, filled_cash, remaining_cash))
                        
                        # ✅ 两阶段撤单确认：检查是否已经提交过撤单请求
                        is_cancelling = order_info.get('is_cancelling', False)
                        
                        if not is_cancelling:
                            # 第一阶段：首次发现部成，提交撤单请求
                            try:
                                cancel_order(order_id)
                                log.info("[实盘买入补单] 部成订单撤销请求已发送: order_id=%s" % order_id)
                                # 标记"撤单中"，记录剩余资金，等待下轮确认
                                g.buy_orders[order_id]['is_cancelling'] = True
                                g.buy_orders[order_id]['remaining_cash'] = remaining_cash
                                log.info("[实盘买入补单] %s 部成订单已标记撤单中，等待下轮确认撤单状态" % stock)
                            except Exception as e:
                                log.warning("[实盘买入补单] 部成订单撤销请求失败: order_id=%s, error=%s" % (order_id, e))
                                # 撤单失败，移除追踪，避免资金计算混乱
                                orders_to_remove.append(order_id)
                        else:
                            # 已经提交过撤单，但订单状态还是部成
                            # 可能：1)撤单还没生效 2)撤单期间又成交了
                            # 重新计算剩余资金（应对撤单期间继续成交的情况）
                            old_remaining = order_info.get('remaining_cash', 0)
                            if abs(remaining_cash - old_remaining) > 100:  # 资金变化超过100元
                                log.info("[实盘买入补单] %s 部成订单撤单期间又成交，剩余资金从%.2f更新为%.2f" % 
                                        (stock, old_remaining, remaining_cash))
                                g.buy_orders[order_id]['remaining_cash'] = remaining_cash
                            else:
                                log.info("[实盘买入补单] %s 部成订单撤单处理中，等待确认..." % stock)
                    else:
                        # 异常情况：状态为部成但filled_amount不合理
                        log.warning("[实盘买入补单] %s 部成状态异常，移除追踪" % stock)
                        orders_to_remove.append(order_id)
                    
                elif status in ['0', '1', '2', '+', '-', 'C', 'V']:  # 完全未成交
                    # 检查是否涨停：如果涨停则不撤单，等待打开涨停
                    snapshot = get_snapshot(stock)
                    if snapshot:
                        current_price = snapshot.get('last_px', 0)
                        up_px = snapshot.get('up_px', 0)
                        
                        # 判断是否涨停（容忍0.01元误差）
                        is_limit_up = (up_px > 0 and abs(current_price - up_px) < 0.01)
                        
                        if is_limit_up:
                            log.info("[实盘买入补单] %s 已涨停（%.2f），不撤单，等待打开涨停" % (stock, current_price))
                            # 不撤单，继续保留在追踪列表，下轮继续检查
                            continue  # 跳过此订单，不移除也不补单
                    
                    # ✅ 两阶段撤单确认机制：检查是否已经提交过撤单请求
                    is_cancelling = order_info.get('is_cancelling', False)
                    
                    if not is_cancelling:
                        # 第一阶段：首次发现未成交，提交撤单请求
                        try:
                            cancel_order(order_id)
                            log.info("[实盘买入补单] 撤单请求已发送: order_id=%s, stock=%s" % (order_id, stock))
                            # 标记"撤单中"，但不移除订单，下轮再检查撤单结果
                            g.buy_orders[order_id]['is_cancelling'] = True
                            log.info("[实盘买入补单] %s 已标记撤单中，等待下轮确认撤单状态" % stock)
                        except Exception as e:
                            log.warning("[实盘买入补单] 撤单请求失败: order_id=%s, error=%s" % (order_id, e))
                    else:
                        # 已经提交过撤单，但订单状态还是未成交
                        # 说明撤单还没生效，继续等待下一轮
                        log.info("[实盘买入补单] %s 撤单处理中，等待确认..." % stock)
                
                elif status == '6':  # '6' = 已撤
                    # ✅ 第二阶段：撤单已确认，现在才真正补单
                    # 检查是否是部成订单的撤单确认
                    remaining_cash = order_info.get('remaining_cash', 0)
                    
                    if remaining_cash > 0:
                        # 这是部成订单的撤单确认，用剩余资金补单
                        log.info("[实盘买入补单] %s 部成订单撤单已确认（status=6），准备补充剩余资金%.2f" % (stock, remaining_cash))
                        orders_to_remove.append(order_id)
                        if remaining_cash > 100:  # 剩余资金足够买100股才补单
                            # 应用单股最大买入金额限制
                            actual_remaining_cash = min(g.max_single_stock_amount, remaining_cash)
                            orders_to_retry.append({
                                'stock': stock,
                                'cash': actual_remaining_cash,  # 使用限制后的剩余资金
                                'retry_count': retry_count + 1,
                                'original_order_id': order_id
                            })
                        else:
                            log.info("[实盘买入补单] %s 剩余资金不足100元，放弃补单" % stock)
                    else:
                        # 完全未成交订单的撤单确认
                        log.info("[实盘买入补单] %s 撤单已确认（status=6），准备补单" % stock)
                        orders_to_remove.append(order_id)
                        # 应用单股最大买入金额限制
                        actual_cash = min(g.max_single_stock_amount, cash_allocated)
                        orders_to_retry.append({
                            'stock': stock,
                            'cash': actual_cash,
                            'retry_count': retry_count + 1,
                            'original_order_id': order_id
                        })
                
                elif status == '9':  # '9' = 废单
                    log.warning("[实盘买入补单] %s 订单被拒绝（status=9 废单），准备补单" % stock)
                    orders_to_remove.append(order_id)
                    # 应用单股最大买入金额限制
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
        # 【关键修复】过滤掉已经有成交记录的股票，避免重复买入
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
        
        # 获取最新行情
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
            
            # 使用当前市场价重新下单
            current_price = snapshot.get('last_px', 0)
            if current_price <= 0:
                log.warning("[实盘买入补单] %s 当前价格无效，跳过" % stock)
                continue
            
            # 固定1%加价，避免触发价格笼子限制
            limit_price = current_price * 1.01
            up_px = snapshot.get('up_px', current_price * 1.1)
            limit_price = min(limit_price, up_px)  # 不超过涨停价
            
            # 价格取整：A股最小变动单位0.01元，保留2位小数
            limit_price = round(limit_price, 2)
            
            log.info("[实盘买入补单] 重新下单(第%d次): %s, 资金=%.2f, 当前价=%.2f, 限价=%.2f (+1%%)" % 
                    (retry_count, stock, cash_allocated, current_price, limit_price))
            
            try:
                # 应用单股最大买入金额限制
                actual_cash = min(g.max_single_stock_amount, cash_allocated)
                new_order_id = order_value(stock, actual_cash, limit_price=limit_price)
                if new_order_id:
                    # 追踪新订单ID，继续下轮检查
                    g.buy_orders[new_order_id] = {
                        'stock': stock,
                        'cash': actual_cash,
                        'limit_price': limit_price,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': retry_count
                    }
                    # 更新该股票的总重试次数
                    g.stock_retry_count[stock] = retry_count
                    
                    log.info("[实盘买入补单] 补单成功，新订单ID=%s，将继续追踪（%s总重试%d/%d）" % 
                            (new_order_id, stock, retry_count, g.max_retry_count))
                else:
                    log.warning("[实盘买入补单] %s 补单失败，order_value返回空" % stock)
            except Exception as e:
                log.error("[实盘买入补单] 补单异常: stock=%s, error=%s" % (stock, e))
    
    # 输出当前追踪状态
    if g.buy_orders:
        log.info("[实盘买入补单] 当前仍在追踪的订单数: %d" % len(g.buy_orders))
    else:
        log.info("[实盘买入补单] 所有订单已处理完成，无需继续追踪")


def check_and_retry_sell_orders(context):
    """
    [实盘专用] 检查卖出订单状态并补单的函数。
    由check_and_retry_orders调用，每3秒执行一次，最多重试10次。
    撤单成功后再补单，补单后继续追踪新订单ID。
    """
    if not is_trade():  # 确保此函数只在实盘运行
        return
    
    if not g.sell_orders:
        return  # 无待检查订单，静默返回
    
    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]卖出订单检查执行 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    log.info("[实盘卖出补单] 待检查订单数量: %d" % len(g.sell_orders))
    
    # 记录需要从订单列表中移除的订单（已成交或达到重试上限）
    orders_to_remove = []
    # 记录需要补单的股票
    orders_to_retry = []
    
    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '未知原因')
        retry_count = order_info.get('retry_count', 0)
        
        # 检查该股票的总重试次数（优先级更高）
        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count:
            log.warning("[实盘卖出补单] %s 已达到最大重试次数(%d)，停止追踪" % (stock, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        # 也检查单个订单的重试次数（双重保障）
        if retry_count >= g.max_retry_count:
            log.warning("[实盘卖出补单] %s 订单%s已达到最大重试次数(%d)，停止追踪" % (stock, order_id, g.max_retry_count))
            orders_to_remove.append(order_id)
            continue
        
        try:
            # 检查订单状态（get_order返回list，取第一个元素）
            order_list = get_order(order_id)
            
            if order_list and len(order_list) > 0:
                order_status = order_list[0]
                filled_amount = order_status.filled  # 已成交量
                order_amount = order_status.amount   # 订单总量
                status = order_status.status         # 订单状态
                
                log.info("[实盘卖出补单] %s 订单状态: order_id=%s, status=%s, 已成交=%d/总量=%d, 重试次数=%d/%d, 原因=%s" % 
                        (stock, order_id, status, filled_amount, order_amount, retry_count, g.max_retry_count, reason))
                
                # 判断订单状态（PTrade状态为数字字符串）
                if status == '8':  # '8' = 已成
                    log.info("[实盘卖出补单] %s 已全部成交，移除追踪" % stock)
                    orders_to_remove.append(order_id)
                    
                elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:  # 未成交或部分成交
                    # 检查是否跌停：如果跌停则不撤单，等待打开跌停
                    snapshot = get_snapshot(stock)
                    if snapshot:
                        current_price = snapshot.get('last_px', 0)
                        down_px = snapshot.get('down_px', 0)
                        
                        # 判断是否跌停（容忍0.01元误差）
                        is_limit_down = (down_px > 0 and abs(current_price - down_px) < 0.01)
                        
                        if is_limit_down:
                            log.info("[实盘卖出补单] %s 已跌停（%.2f），不撤单，等待打开跌停" % (stock, current_price))
                            # 不撤单，继续保留在追踪列表，下轮继续检查
                            continue  # 跳过此订单，不移除也不补单
                    
                    # 未跌停，正常撤单重下
                    cancel_success = False
                    try:
                        cancel_result = cancel_order(order_id)
                        log.info("[实盘卖出补单] 撤单请求已发送: order_id=%s, stock=%s" % (order_id, stock))
                        cancel_success = True
                    except Exception as e:
                        log.warning("[实盘卖出补单] 撤单失败: order_id=%s, error=%s" % (order_id, e))
                        cancel_success = False
                    
                    # 只有撤单成功才补单
                    if cancel_success:
                        orders_to_remove.append(order_id)  # 从追踪列表移除原订单
                        orders_to_retry.append({
                            'stock': stock,
                            'reason': reason,
                            'retry_count': retry_count + 1,
                            'original_order_id': order_id
                        })
                    else:
                        log.warning("[实盘卖出补单] %s 撤单未成功，本轮不补单，下轮继续检查" % stock)
                
                elif status in ['6', '9']:  # '6' = 已撤, '9' = 废单
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
        
        # 获取最新行情
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
            
            # 检查是否还持有该股票
            position = context.portfolio.positions.get(stock)
            if not position or position.amount <= 0:
                log.info("[实盘卖出补单] %s 已无持仓，无需补单" % stock)
                continue
            
            # 使用当前市场价重新下单
            current_price = snapshot.get('last_px', 0)
            if current_price <= 0:
                log.warning("[实盘卖出补单] %s 当前价格无效，跳过" % stock)
                continue
            
            # 卖出价格略低于市价，提高成交率（-1%）
            limit_price = current_price * 0.99
            down_px = snapshot.get('down_px', current_price * 0.9)
            limit_price = max(limit_price, down_px)  # 不低于跌停价
            
            # 价格取整：A股最小变动单位0.01元，保留2位小数
            limit_price = round(limit_price, 2)
            
            log.info("[实盘卖出补单] 重新卖出(第%d次): %s, 持仓=%d股, 当前价=%.2f, 限价=%.2f (-1%%), 原因=%s" % 
                    (retry_count, stock, position.amount, current_price, limit_price, reason))
            
            try:
                new_order_id = order_target(stock, 0, limit_price=limit_price)
                if new_order_id:
                    # 追踪新订单ID，继续下轮检查
                    g.sell_orders[new_order_id] = {
                        'stock': stock,
                        'reason': reason,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': retry_count
                    }
                    # 更新该股票的总重试次数
                    g.stock_retry_count[stock] = retry_count
                    
                    log.info("[实盘卖出补单] 补单成功，新订单ID=%s，将继续追踪（%s总重试%d/%d）" % 
                            (new_order_id, stock, retry_count, g.max_retry_count))
                else:
                    log.warning("[实盘卖出补单] %s 补单失败，order_target返回空" % stock)
            except Exception as e:
                log.error("[实盘卖出补单] 补单异常: stock=%s, error=%s" % (stock, e))
    
    # 输出当前追踪状态
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
    
    # 判断当前是13:00还是14:55时段
    is_afternoon_session = current_time.hour >= 14  # 14:55属于下午时段
    if is_afternoon_session:
        log.info("[14:55时段] 执行完整卖出逻辑（止损+止盈+常规卖出）")
    else:
        log.info("[13:00时段] 仅执行止损逻辑（不止盈，减少卖飞）")
    
    positions = context.portfolio.positions
    if not positions:
        log.info("当前无持仓，无需卖出。")
        return

    # 过滤掉当日买入的股票 (T+1) 和当日已卖出的股票
    sellable_stocks = [
        code for code in positions
        if code not in g.today_bought_stocks and code not in g.today_sold_stocks
    ]
    if not sellable_stocks:
        log.info("无符合T+1规则的可卖出持仓。")
        return

    log.info("当前可卖出持仓股票: %s" % sellable_stocks)
    
    # 【关键修改】主动获取当前价格数据
    price_data = {}
    if is_trade():
        snapshots = get_snapshot(sellable_stocks)
        for code in sellable_stocks:
            if code in snapshots:
                price_data[code] = {
                    'price': snapshots[code].get('last_px', 0),
                    'preclose': snapshots[code].get('preclose_px', 0)
                }
    else: # 回测
        df_price = get_history(count=1, frequency='1m', field='price', security_list=sellable_stocks, include=True)
        df_preclose = get_history(count=1, frequency='1d', field='close', security_list=sellable_stocks)
        for code in sellable_stocks:
            price_series = df_price.query('code == "%s"' % code)
            preclose_series = df_preclose.query('code == "%s"' % code)
            if not price_series.empty and not preclose_series.empty:
                price_data[code] = {
                    'price': price_series['price'].iloc[0],
                    'preclose': preclose_series['close'].iloc[0]
                }

    # 1. 执行增强版动态止损
    # 13:00时段：stop_only=True，只做止损不做移动止盈
    # 14:55时段：stop_only=False，止损+移动止盈全部执行
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
                # 实盘记录卖出订单，用于后续检查
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

    # 2. 对剩余持仓执行常规卖出逻辑（仅14:55时段执行，13:00不做止盈/常规卖出）
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
                # 实盘记录卖出订单，用于后续检查
                if is_trade():
                    g.sell_orders[order_id] = {
                        'stock': code,
                        'reason': '常规卖出: ' + reason,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0
                    }


# ================================== 选股与过滤函数 =================================

def get_all_stocks_excluding_gem(context):
    """获取全A股并过滤创业板和ST"""
    current_date = context.blotter.current_dt.strftime('%Y%m%d')
    all_stocks = get_Ashares(current_date)
    
    # 批量获取股票状态
    st_status = get_stock_status(all_stocks, 'ST', current_date)
    
    final_list = []
    for stock_code in all_stocks:
        if stock_code.startswith('30'):  # 剔除创业板
            continue
        if st_status.get(stock_code, False): # 剔除ST
            continue
        final_list.append(stock_code)
    return final_list


def is_avoid_period(context):
    """判断是否在1、4、12月空仓期"""
    current_date = context.blotter.current_dt
    month = current_date.month
    day = current_date.day
    
    if month in [1, 4, 12] and day >= 15:
        return True
    return False


def yesterday_bomb_filter(context, stocks):
    """核心筛选：昨日炸板（盘中触及涨停但收盘未封住）"""
    if not stocks:
        return []

    try:
        # 使用更安全的方式获取历史数据，避免非交易日问题
        # 获取昨日和前日的数据，使用get_history替代get_price
        yesterday_df = get_history(count=2, frequency='1d', field=['close', 'high'], security_list=stocks)
        
        if yesterday_df.empty:
            log.warning("无法获取历史数据，跳过昨日炸板筛选")
            return []

        valid_stocks = []
        for code in stocks:
            try:
                # 获取该股票的数据
                stock_data = yesterday_df.query('code == "%s"' % code)
                if len(stock_data) < 2:  # 需要至少2天的数据
                    continue
                
                # 由于get_history返回的数据按时间升序排列，所以倒数第二天是[-2]，倒数第一天是[-1]
                # 前日收盘价（倒数第二天）
                close_before = stock_data['close'].iloc[-2]
                # 昨日数据
                high_yesterday = stock_data['high'].iloc[-1]
                close_yesterday = stock_data['close'].iloc[-1]

                high_limit_yesterday, _ = calculate_limit_prices(code, close_before)
                if high_limit_yesterday is None:
                    continue

                # 判断是否炸板：昨日最高价触及涨停，但收盘价未涨停
                if abs(high_yesterday - high_limit_yesterday) < 0.001 and close_yesterday < high_limit_yesterday:
                    valid_stocks.append(code)
            except Exception as e:
                log.debug("处理股票%s炸板筛选异常: %s" % (code, str(e)))
                continue
        return valid_stocks
    except Exception as e:
        log.error("yesterday_bomb_filter error: %s" % e)
        return []


def technical_filter(context, stocks):
    """技术指标筛选"""
    if not stocks:
        return []

    try:
        # 获取过去 MA_PERIOD+1 天的数据
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


def backtest_opening_filter_and_rank(context, stocks):
    """[回测专用] 开盘价筛选和排序
    回测中开盘价优先使用集合竞价数据(get_trend_data 的 hq_px)，
    若获取失败再回退到首分钟K线open，以贴近实盘集合竞价逻辑"""
    if not stocks:
        return []

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
    turnover_dict = get_turnover_ratio_yesterday(stocks)

    # 1. 集合竞价数据（集中竞价期间价格）
    try:
        trend_data = get_trend_data(stocks=stocks)
    except Exception:
        trend_data = {}

    # 2. 首分钟K线和昨日收盘价，作为兜底数据
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

            # 优先使用集合竞价价格（hq_px，其次 wavg_px）
            open_now = 0.0
            td = trend_data.get(code) if isinstance(trend_data, dict) else None
            if td:
                open_now = td.get('hq_px') or td.get('wavg_px') or 0.0

            # 如果集合竞价数据不可用，则退回到首分钟K线open
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
    return df_sorted['code'].tolist()

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
            # 集合竞价期间，last_px是虚拟匹配价
            if open_now == 0:
                open_now = snapshot.get('last_px', 0)
            
            prev_close = snapshot.get('preclose_px', 0)
            if prev_close <= 0 or open_now <= 0:
                continue

            open_ratio = open_now / prev_close
            if not (g.open_down_threshold < open_ratio < g.open_up_threshold):
                continue

            # BS(买卖盘)过滤：使用买1-买5和卖1-卖5的总量来衡量买卖力量
            bid_grp = snapshot.get('bid_grp', {})
            offer_grp = snapshot.get('offer_grp', {})
            entrust_rate = snapshot.get('entrust_rate', 0)
            
            # 计算买1-买5总量和卖1-卖5总量
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
            
            # BS过滤条件：买盘总量 > 卖盘总量 或 委比 > 0
            # 如果BS数据缺失（总量都为0且委比为0），则跳过BS过滤
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
                # BS数据缺失，跳过BS过滤
                log.warning("[实盘]BS数据缺失: %s 跳过BS过滤" % code)
                bs_filter_passed = True  # 允许通过

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
    return df_sorted['code'].tolist()


def get_turnover_ratio_yesterday(stocks):
    """获取上一交易日的换手率字典，使用valuation表的turnover_rate字段"""
    if not stocks:
        return {}

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')

    # 使用 get_fundamentals 获取昨日换手率
    try:
        valuation_df = get_fundamentals(
            stocks,
            'valuation',
            'turnover_rate',
            yesterday_str
        )

    except Exception as e:
        log.error("[换手率调试] get_fundamentals异常: %s" % str(e))
        valuation_df = pd.DataFrame()

    result = {}
    for code in stocks:
        turnover = 0.0
        try:
            if isinstance(valuation_df, pd.DataFrame) and not valuation_df.empty:
                # valuation表返回的索引是secu_code，需要用index查询
                if code in valuation_df.index and 'turnover_rate' in valuation_df.columns:
                    raw = valuation_df.loc[code, 'turnover_rate']
                    # 文档说明：turnover_rate是带%的字符串，如"20%"，需要转换成0.2
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
                    
                    #log.info("[换手率调试] %s raw='%s' parsed=%.6f" % (code, str(raw), turnover))
                else:
                    log.warning("[换手率调试] %s 不在返回数据中或缺少turnover_rate字段" % code)
        except Exception as e:
            log.error("[换手率调试] %s 解析异常: %s" % (code, str(e)))
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
    if cost_price <= 0 or current_price <= 0:
        return False, ""
    profit_rate = (current_price - cost_price) / cost_price

    # 更新最高价（无论是否止盈，都要持续追踪最高价）
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

    # 4. 移动止盈（13:00时段stop_only=True时跳过，仅14:55执行，减少卖飞）
    if not stop_only:
        # 盈利超过5%，从最高点回撤3%则止盈
        if profit_rate > 0.05:
            if (highest_price - current_price) / highest_price >= 0.03:
                return True, "盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (profit_rate * 100, (highest_price - current_price) / highest_price * 100)
        # 盈利超过10%，从最高点回撤4%则止盈
        if profit_rate > 0.10:
            if (highest_price - current_price) / highest_price >= 0.04:
                return True, "高盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (profit_rate * 100, (highest_price - current_price) / highest_price * 100)

    return False, ""


def regular_sell_condition(context, code, price_data):
    """常规卖出条件"""
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

        # 均线计算：使用过去 g.stop_loss_ma_period 个交易日的收盘均价（不含当日），以贴近QMT逻辑
        hist_df = get_history(g.stop_loss_ma_period, '1d', 'close', security_list=code, include=False)
        if len(hist_df) < g.stop_loss_ma_period:
            ma_stop = -1  # 数据不足，不触发
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
            # 使用get_trade_days计算交易日数
            trade_days = get_trade_days(start_date=buy_date.strftime('%Y%m%d'), end_date=current_date.strftime('%Y%m%d'))
            return len(trade_days)
        except:
            return 999 # 解析失败返回一个大数
    return 999


def calculate_atr_stop_loss(context, stock, cost_price):
    """基于ATR计算动态止损价"""
    try:
        hist_df = get_history(15, '1d', ['high', 'low', 'close'], security_list=stock)
        if len(hist_df) < 15:
            return cost_price * 0.97  # 数据不足，默认3%止损

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
        # 使用上证指数作为大盘指数，保持与QMT逻辑一致
        index_df = get_history(4, '1d', 'close', security_list='000001.SS')
        if len(index_df) < 4:
            return False

        # 如果大盘连续下跌3天，且个股盈利小于2%，则止损
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
    
    # 科创板和创业板（虽然已过滤，但为代码健壮性保留）
    if stock_code.startswith('688') or stock_code.startswith('300'):
        limit_pct = 0.20
    # ST股票
    elif 'ST' in stock_name:
        limit_pct = 0.05
    # 其他
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
        # 首次运行时，使用初始资金
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
        # 批量获取股票名称
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
    
    # 添加新买入的记录
    for code in g.today_bought_stocks:
        if code in current_positions and code not in g.buy_records:
            position = context.portfolio.positions[code]
            g.buy_records[code] = {
                'buy_date': context.blotter.current_dt.strftime('%Y-%m-%d'),
                'highest_price': position.last_sale_price
            }
            
    # 移除已卖出的记录
    current_records = list(g.buy_records.keys())
    for code in current_records:
        if code not in current_positions:
            del g.buy_records[code]
            
    log.info("收盘后更新持仓记录: %s" % g.buy_records)