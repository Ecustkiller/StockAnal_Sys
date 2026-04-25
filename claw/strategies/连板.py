# -*- coding: utf-8 -*-
"""
PTrade策略: 连板策略（1进2打板）
参考弱转强V2框架完善，保留原始盘中打板核心逻辑。

策略逻辑：
- 盘前筛选：昨日首板涨停的股票（前日非涨停，保证"首板"）
- 盘中打板：handle_data逐bar扫描，触板/涨停时买入
- 卖出：止盈15% / 止损5% / 超期3天
- 风控：日内回撤>3%停止买入
- 实盘增强(V2框架)：授权验证、卖出补单、交易报告、buy_records持久化

运行建议:
1. 业务类型: 股票
2. 频率: 分钟
3. 实盘/回测均支持
"""


# ================================== 策略函数 ===================================

def initialize(context):
    """策略初始化函数，只在策略启动时运行一次。"""
    # --- 授权验证（参考V2） ---
    if is_trade():
        auth_result = permission_test()
        if not auth_result:
            log.error("=" * 50)
            log.error("【授权失败】策略无权在当前账户或时间运行！")
            log.error("请检查：1. 账户是否匹配  2. 是否已过有效期")
            log.error("=" * 50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，策略启动成功")

    # --- 基础仓位参数 ---
    g.max_positions = 3
    g.single_position_ratio = 0.9
    g.min_cash_per_order = 2000.0
    g.max_available_cash = 100000.0  # 最大可用资金，参数可调整

    # --- 风控参数 ---
    g.take_profit_ratio = 0.15
    g.stop_loss_ratio = -0.05
    g.max_hold_days = 3
    g.max_daily_loss_pct = -0.03  # 日内回撤阈值

    # --- 打板识别与候选池约束参数 ---
    g.limit_up_tol = 0.001
    g.min_yesterday_turnover = 200000000.0
    g.max_watchlist = 80
    g.fallback_security = "000001.SZ"
    g.only_touch_board = True
    g.require_trade_status = True
    g.min_offer1_qty = 1000
    g.max_price_gap_to_up = 0.001
    g.cancel_buy_time = "14:56"
    g.buy_cutoff_time = "14:30"
    g.backtest_buy_start_time = "09:33"
    g.order_timeout_minutes = 2
    g.max_buy_retries = 2
    g.max_daily_buy_submits = 6
    g.backtest_follow_live = True
    g.backtest_allow_limitup_as_touch = True

    # --- 运行期状态变量 ---
    g.candidates = []
    g.today_bought = []
    g.hold_days = {}
    g.last_trade_date = ""
    g.day_start_value = None
    g.day_start_cash = None
    g.prev_day_end_value = None
    g.prev_day_end_cash = None
    g.stop_buy_for_today = False
    g.daily_buy_submits = 0
    g.buy_order_meta = {}
    g.resubmit_queue = {}

    # --- 新增(V2)：卖出订单管理与buy_records ---
    g.sell_orders = {}           # 卖出订单追踪 {order_id: {'stock':, 'reason':, 'time':, 'retry_count':}}
    g.sell_retry_count = {}      # 每只股票卖出重试次数
    g.max_sell_retry = 10        # 卖出最大重试次数
    g.buy_records = {}           # 持仓记录 {code: {'buy_date': ..., 'highest_price': ...}}
    g.today_sold_stocks = set()  # 当日已卖出的股票

    # --- 股票池 ---
    set_universe([g.fallback_security])

    # --- PTrade实盘/回测设置 ---
    if is_trade():
        set_parameters(holiday_not_do_before="1", server_restart_not_do_before="1")
    else:
        set_volume_ratio(0.9)
        set_limit_mode("LIMIT")

    # --- 定时任务 ---
    # 原始：尾盘撤销未成交买单
    run_daily(context, _cancel_pending_buy_orders, time=g.cancel_buy_time)
    # 新增(V2)：卖出订单补单检查，每3秒执行一次
    run_interval(context, check_and_retry_sell_orders, seconds=3)

    log.info(
        "策略初始化完成，频率=%s，模式=%s"
        % (get_frequency(), "实盘" if is_trade() else "回测")
    )


def before_trading_start(context, data):
    """每日开盘前运行函数。"""
    # 新交易日切换：仓龄+1，并同步持仓与仓龄字典
    _roll_trade_day(context)
    _sync_hold_days_with_positions(context)
    g.today_bought = []
    g.today_sold_stocks = set()
    g.stop_buy_for_today = False
    g.daily_buy_submits = 0
    g.buy_order_meta = {}
    g.resubmit_queue = {}

    # 新增(V2)：重置卖出订单追踪
    if is_trade():
        g.sell_orders.clear()
        g.sell_retry_count.clear()
        log.info("[Ptrade实盘] 卖出订单追踪记录已清空")

    _save_day_start_snapshot(context)

    log.info("=" * 50)
    log.info(">>> 开始执行盘前选股任务...")

    # 获取全A并做基础状态过滤（ST/停牌/退市等）
    stocks = get_Ashares()
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        log.info("未获取到A股列表")
        return

    stocks = filter_stock_by_status(stocks, ["ST", "HALT", "DELISTING", "DELISTING_SORTING"])
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        log.info("状态过滤后无股票")
        return

    # 进一步剔除创业板、科创板，仅保留主板标的
    stocks = [sec for sec in stocks if _is_main_board_stock(sec)]
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        log.info("板块过滤后无股票")
        return

    # 盘前生成"昨日首板"候选池
    g.candidates = _pick_yesterday_first_board(stocks)
    _refresh_universe(context)
    log.info("今日候选股票数量: %d" % len(g.candidates))
    log.info(">>> 盘前选股任务执行完毕。")


def handle_data(context, data):
    """策略主逻辑函数：盘中逐bar扫描，先卖出后买入。"""
    # 先执行卖出管理，再执行买入逻辑（保留原始handle_data驱动模式）
    _manage_exit(context, data)
    _update_intraday_risk_flags(context)
    _manage_pending_buy_orders(context, data)
    if not _is_buy_time(context):
        return
    _manage_entry(context, data)


def after_trading_end(context, data):
    """每日收盘后运行函数。"""
    # 新增(V2)：增强版交易报告
    _log_daily_summary(context)
    # 新增(V2)：持久化buy_records
    _update_buy_records(context)


# ================================== 盘前选股 ===================================

def _pick_yesterday_first_board(stocks):
    """盘前筛选：昨日首板涨停（前日非涨停，保证"首板"）"""
    fields = ["close", "high_limit", "volume", "money", "is_open", "unlimited"]
    hist = get_history(2, "1d", fields, security_list=stocks, fq=None, include=False)
    if hist is None:
        return []
    if len(hist) == 0:
        return []
    if "code" not in hist.columns:
        return []

    result = []
    grouped = hist.groupby("code")
    for sec, df in grouped:
        if len(df) < 2:
            continue
        df = df.sort_index()
        pre_bar = df.iloc[-2]
        y_bar = df.iloc[-1]

        # 昨日必须正常交易且有成交额，剔除无涨跌停限制标的
        if int(_as_float(y_bar.get("is_open", 1))) != 1:
            continue
        if _as_float(y_bar.get("volume", 0)) <= 0:
            continue
        if _as_float(y_bar.get("money", 0)) < g.min_yesterday_turnover:
            continue
        if int(_as_float(y_bar.get("unlimited", 0))) == 1:
            continue

        # 昨日收盘贴近涨停价，视为涨停
        y_is_limit = _is_limit_up(
            _as_float(y_bar.get("close", 0)),
            _as_float(y_bar.get("high_limit", 0))
        )
        if not y_is_limit:
            continue

        # 前一交易日不能涨停，保证"首板"
        pre_is_limit = False
        if int(_as_float(pre_bar.get("unlimited", 0))) == 0:
            pre_is_limit = _is_limit_up(
                _as_float(pre_bar.get("close", 0)),
                _as_float(pre_bar.get("high_limit", 0))
            )
        if pre_is_limit:
            continue

        result.append((sec, _as_float(y_bar.get("money", 0))))

    # 候选按昨日成交额降序，控制池子规模避免盘中过载
    result.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in result[:g.max_watchlist]]


# ================================== 盘中买入（打板） ===================================

def _manage_entry(context, data):
    """盘中打板买入：逐bar扫描候选池，触板/涨停时买入。"""
    if not g.candidates:
        return
    if g.stop_buy_for_today:
        return
    if g.daily_buy_submits >= g.max_daily_buy_submits:
        return

    held = _get_held_stocks(context)
    if len(held) >= g.max_positions:
        return

    # 动态计算单票预算：受现金、单票仓位、最大可用资金三重约束
    available_slots = g.max_positions - len(held)
    cash = _as_float(context.portfolio.cash)
    if cash < g.min_cash_per_order:
        return

    cap = _as_float(context.portfolio.portfolio_value) * g.single_position_ratio
    budget = min(cash / max(available_slots, 1), cap)
    max_per_stock = g.max_available_cash / max(available_slots, 1)
    budget = min(budget, max_per_stock)
    if budget < g.min_cash_per_order:
        return

    # 优先处理撤单后的重提队列
    used_slots = _process_resubmit_queue(context, data, available_slots)
    available_slots -= used_slots
    if available_slots <= 0:
        return

    for sec in g.candidates:
        if available_slots <= 0:
            break
        if sec in held:
            continue
        if sec in g.today_bought:
            continue
        if sec not in data:
            continue
        if _has_open_order(sec, side="buy"):
            continue

        # 仅在触板/涨停时买入（1=涨停，2=触板涨停）
        limit_state = _get_limit_state(sec)
        if limit_state not in (1, 2):
            continue
        # 触板严格条件仅用于实盘
        if is_trade() and g.only_touch_board and limit_state != 2:
            continue

        snapshot = None
        limit_px = None
        amount = 0
        if is_trade():
            # 实盘：使用快照做交易状态、盘口和价格一致性校验
            snapshot = _get_snapshot_safe(sec)
            pass_check, reject_reason = _check_live_entry(snapshot, sec)
            if not pass_check:
                log.info("跳过买入 %s 原因=%s" % (sec, reject_reason))
                continue

            limit_px = _get_prefer_order_price(sec, data, snapshot=snapshot)
            if limit_px is None or limit_px <= 0:
                log.info("跳过买入 %s 原因=无有效委托价格" % sec)
                continue
            amount = _calc_stock_amount(budget, limit_px)
            if amount < 100:
                log.info("跳过买入 %s 原因=委托量太小 预算=%.2f 价格=%.3f" % (sec, budget, limit_px))
                continue
            order_id = order(sec, amount, limit_price=limit_px)
        else:
            # 回测尽量贴近实盘
            if g.backtest_follow_live and not _is_backtest_entry_time(context):
                continue
            if g.backtest_follow_live and g.only_touch_board:
                if limit_state == 2:
                    pass
                elif limit_state == 1 and g.backtest_allow_limitup_as_touch:
                    pass
                else:
                    continue

            limit_px = _get_prefer_order_price(sec, data, snapshot=None)
            if limit_px is None or _as_float(limit_px) <= 0:
                log.info("跳过买入(回测) %s 原因=无有效委托价格" % sec)
                continue
            amount = _calc_stock_amount(budget, limit_px)
            if amount < 100:
                log.info(
                    "跳过买入(回测) %s 原因=委托量太小 预算=%.2f 价格=%.3f"
                    % (sec, budget, _as_float(limit_px))
                )
                continue
            order_id = order(sec, amount, limit_price=limit_px)

        if order_id:
            if is_trade():
                _register_buy_order(
                    order_id, sec, amount, budget,
                    retry_count=0, dt=context.blotter.current_dt
                )
            g.today_bought.append(sec)
            g.hold_days[sec] = 0
            available_slots -= 1
            g.daily_buy_submits += 1
            trigger_text = "触板涨停" if limit_state == 2 else "涨停封板"
            if is_trade():
                offer1_qty = _get_offer1_qty(snapshot)
                price_text = "%.3f" % _as_float(limit_px)
                buy_reason = (
                    "1进2打板买入|原因=%s|预算=%.2f|委托量=%s|委托价=%s|卖一量=%s|当日提交=%s"
                    % (trigger_text, budget, amount, price_text, offer1_qty, g.daily_buy_submits)
                )
            else:
                buy_reason = (
                    "1进2打板买入(回测)|原因=%s|预算=%.2f|委托量=%s|委托价=%.3f|"
                    "limit_state=%s|当日提交=%s|allow_limit1=%s"
                    % (
                        trigger_text, budget, amount, _as_float(limit_px),
                        limit_state, g.daily_buy_submits, g.backtest_allow_limitup_as_touch
                    )
                )
            log.info("买入 %s 委托号=%s 原因=%s" % (sec, order_id, buy_reason))
        else:
            log.info("买入失败 %s 原因=委托API返回None" % sec)


# ================================== 盘中卖出 ===================================

def _manage_exit(context, data):
    """盘中逐bar评估持仓：止盈、止损、超期卖出。"""
    positions = context.portfolio.positions
    for sec, pos in positions.items():
        amount = int(_as_float(getattr(pos, "amount", 0)))
        if amount <= 0:
            continue
        if sec not in data:
            continue
        if _has_open_order(sec, side="sell"):
            continue

        cost = _as_float(getattr(pos, "cost_basis", 0))
        last_price = _as_float(data[sec].close)
        if cost <= 0 or last_price <= 0:
            continue

        pnl_ratio = (last_price - cost) / cost
        hold_days = int(g.hold_days.get(sec, 0))
        sell_reasons = []
        if pnl_ratio >= g.take_profit_ratio:
            sell_reasons.append("止盈")
        if pnl_ratio <= g.stop_loss_ratio:
            sell_reasons.append("止损")
        if hold_days >= g.max_hold_days:
            sell_reasons.append("超期")
        should_exit = (
            pnl_ratio >= g.take_profit_ratio
            or pnl_ratio <= g.stop_loss_ratio
            or hold_days >= g.max_hold_days
        )
        if not should_exit:
            continue

        # 优先用持仓对象可卖数量，不足时再查一次持仓
        enable_amount = int(_as_float(getattr(pos, "enable_amount", 0)))
        if enable_amount <= 0:
            p = get_position(sec)
            enable_amount = int(_as_float(getattr(p, "enable_amount", 0)))
        if enable_amount <= 0:
            continue

        order_id = order(sec, -enable_amount)
        if order_id:
            reason_text = "|".join(sell_reasons) if sell_reasons else "条件卖出"
            log.info(
                "卖出 %s 委托号=%s 原因=%s 持仓天数=%s 盈亏比例=%.2f%% 成本价=%.3f 现价=%.3f"
                % (sec, order_id, reason_text, hold_days, pnl_ratio * 100, cost, last_price)
            )
            g.today_sold_stocks.add(sec)
            # 新增(V2)：实盘记录卖出订单，用于后续补单追踪
            if is_trade():
                g.sell_orders[order_id] = {
                    'stock': sec,
                    'reason': reason_text,
                    'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                    'retry_count': 0
                }


# ================================== 卖出补单（V2新增） ===================================

def check_and_retry_sell_orders(context):
    """
    [实盘专用] 卖出订单补单检查，由run_interval每3秒执行一次。
    确保止盈/止损/超期的卖出订单确实成交，未成交则撤单重下。
    跌停时不撤单，等待打开跌停。
    """
    if not is_trade():
        return
    if not g.sell_orders:
        return

    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]卖出订单检查 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))
    log.info("[实盘卖出补单] 待检查订单数量: %d" % len(g.sell_orders))

    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '未知')
        retry_count = order_info.get('retry_count', 0)

        # 检查重试上限
        stock_total_retry = g.sell_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_sell_retry:
            log.warning("[实盘卖出补单] %s 已达最大重试次数(%d)，停止追踪" % (stock, g.max_sell_retry))
            orders_to_remove.append(order_id)
            continue
        if retry_count >= g.max_sell_retry:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if not order_list or len(order_list) == 0:
                continue

            order_status = order_list[0]
            filled_amount = order_status.filled
            order_amount = order_status.amount
            status = order_status.status

            log.info("[实盘卖出补单] %s 状态: id=%s, status=%s, 成交=%d/%d, 重试=%d/%d, 原因=%s" %
                     (stock, order_id, status, filled_amount, order_amount, retry_count, g.max_sell_retry, reason))

            if status == '8':  # 已成交
                log.info("[实盘卖出补单] %s 已全部成交" % stock)
                orders_to_remove.append(order_id)

            elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:  # 未成交或部分成交
                # 检查是否跌停：跌停时不撤单，等待打开
                snapshot = _get_snapshot_safe(stock)
                if snapshot:
                    current_px = _as_float(snapshot.get("last_px", 0))
                    down_px = _as_float(snapshot.get("down_px", 0))
                    if down_px > 0 and abs(current_px - down_px) < 0.01:
                        log.info("[实盘卖出补单] %s 跌停(%.2f)，不撤单，等待" % (stock, current_px))
                        continue

                # 非跌停，撤单重下
                cancel_success = False
                try:
                    cancel_order(order_id)
                    log.info("[实盘卖出补单] 撤单请求已发送: %s %s" % (order_id, stock))
                    cancel_success = True
                except Exception as e:
                    log.warning("[实盘卖出补单] 撤单失败: %s, %s" % (order_id, e))

                if cancel_success:
                    orders_to_remove.append(order_id)
                    orders_to_retry.append({
                        'stock': stock,
                        'reason': reason,
                        'retry_count': retry_count + 1
                    })

            elif status in ['6', '9']:  # 已撤或废单
                orders_to_remove.append(order_id)
                orders_to_retry.append({
                    'stock': stock,
                    'reason': reason,
                    'retry_count': retry_count + 1
                })

        except Exception as e:
            log.error("[实盘卖出补单] 检查异常: %s, %s" % (order_id, e))

    # 移除已处理的订单
    for oid in orders_to_remove:
        if oid in g.sell_orders:
            del g.sell_orders[oid]

    # 执行补单
    if orders_to_retry:
        log.info("[实盘卖出补单] 需要补单数量: %d" % len(orders_to_retry))

        stocks_to_snap = [item['stock'] for item in orders_to_retry]
        snapshots = get_snapshot(stocks_to_snap)
        if not snapshots:
            log.warning("[实盘卖出补单] 无法获取快照，放弃补单")
            return

        for item in orders_to_retry:
            stock = item['stock']
            reason = item['reason']
            r_count = item['retry_count']

            # 检查是否还持有
            position = context.portfolio.positions.get(stock)
            if not position or _as_float(getattr(position, "amount", 0)) <= 0:
                log.info("[实盘卖出补单] %s 已无持仓，无需补单" % stock)
                continue

            snapshot = snapshots.get(stock)
            if not snapshot:
                continue
            current_price = _as_float(snapshot.get('last_px', 0))
            if current_price <= 0:
                continue

            # 卖出价格略低于市价(-1%)，提高成交率
            limit_price = current_price * 0.99
            down_px = _as_float(snapshot.get('down_px', current_price * 0.9))
            limit_price = max(limit_price, down_px)  # 不低于跌停价
            limit_price = _round_stock_price(limit_price)

            enable_amount = int(_as_float(getattr(position, "enable_amount", 0)))
            if enable_amount <= 0:
                p = get_position(stock)
                enable_amount = int(_as_float(getattr(p, "enable_amount", 0)))
            if enable_amount <= 0:
                continue

            log.info("[实盘卖出补单] 重新卖出(第%d次): %s, 可卖=%d股, 限价=%.2f, 原因=%s" %
                     (r_count, stock, enable_amount, limit_price, reason))
            try:
                new_oid = order(stock, -enable_amount, limit_price=limit_price)
                if new_oid:
                    g.sell_orders[new_oid] = {
                        'stock': stock,
                        'reason': reason,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': r_count
                    }
                    g.sell_retry_count[stock] = r_count
                    log.info("[实盘卖出补单] 补单成功 new_id=%s (%s 总重试%d/%d)" %
                             (new_oid, stock, r_count, g.max_sell_retry))
            except Exception as e:
                log.error("[实盘卖出补单] 异常: %s, %s" % (stock, e))

    if g.sell_orders:
        log.info("[实盘卖出补单] 仍在追踪: %d" % len(g.sell_orders))


# ================================== 风控与订单管理 ===================================

def _update_intraday_risk_flags(context):
    """日内权益回撤超阈值后，停止当日新开仓并撤销在途买单。"""
    if not is_trade():
        return
    start_value = _as_float(g.day_start_value)
    if start_value <= 0:
        return
    cur_value = _as_float(context.portfolio.portfolio_value)
    day_ret = (cur_value - start_value) / start_value
    if (not g.stop_buy_for_today) and day_ret <= g.max_daily_loss_pct:
        g.stop_buy_for_today = True
        log.info(
            "风险止损: 当日回撤=%.2f%% <= %.2f%%, 今日停止买入"
            % (day_ret * 100, g.max_daily_loss_pct * 100)
        )
        _cancel_pending_buy_orders(context, reason="daily_loss_stop")


def _manage_pending_buy_orders(context, data):
    """盘中处理买单超时：撤单并加入重提队列。"""
    if not is_trade():
        return
    if not g.buy_order_meta:
        return

    open_ids = _get_open_order_ids()
    now_dt = context.blotter.current_dt

    for oid in list(g.buy_order_meta.keys()):
        meta = g.buy_order_meta.get(oid)
        if meta is None:
            continue
        if oid not in open_ids:
            del g.buy_order_meta[oid]
            continue

        submit_dt = meta.get("submit_dt")
        elapsed = _minutes_diff(submit_dt, now_dt)
        if elapsed < g.order_timeout_minutes:
            continue

        sec = meta.get("security")
        retry_count = int(meta.get("retry_count", 0))
        try:
            cancel_order(oid)
            log.info(
                "撤销超时买单: 股票=%s 委托号=%s 已耗时=%d分钟 重试=%d"
                % (sec, oid, elapsed, retry_count)
            )
        except Exception as e:
            log.info("撤销超时买单失败: 股票=%s 委托号=%s 错误=%s" % (sec, oid, e))
            continue

        if (not g.stop_buy_for_today) and retry_count < g.max_buy_retries:
            g.resubmit_queue[sec] = {
                "budget": _as_float(meta.get("budget", 0)),
                "retry_count": retry_count + 1,
                "last_order_id": oid,
            }
            log.info(
                "加入重提队列: 股票=%s 重试=%d/%d"
                % (sec, retry_count + 1, g.max_buy_retries)
            )
        del g.buy_order_meta[oid]


def _process_resubmit_queue(context, data, available_slots):
    """按队列优先重提撤单的买单。"""
    if not is_trade():
        return 0
    if available_slots <= 0:
        return 0
    if not g.resubmit_queue:
        return 0
    if g.stop_buy_for_today:
        return 0

    used_slots = 0
    held = set(_get_held_stocks(context))

    for sec in list(g.resubmit_queue.keys()):
        if used_slots >= available_slots:
            break
        if g.daily_buy_submits >= g.max_daily_buy_submits:
            break
        if sec in held:
            del g.resubmit_queue[sec]
            continue
        if _has_open_order(sec, side="buy"):
            continue
        if sec not in data:
            continue

        limit_state = _get_limit_state(sec)
        if limit_state not in (1, 2):
            del g.resubmit_queue[sec]
            continue
        if g.only_touch_board and limit_state != 2:
            del g.resubmit_queue[sec]
            continue

        snapshot = _get_snapshot_safe(sec)
        pass_check, reject_reason = _check_live_entry(snapshot, sec)
        if not pass_check:
            log.info("跳过重提 %s 原因=%s" % (sec, reject_reason))
            del g.resubmit_queue[sec]
            continue

        task = g.resubmit_queue[sec]
        budget = _as_float(task.get("budget", 0))
        retry_count = int(task.get("retry_count", 1))
        if budget < g.min_cash_per_order:
            del g.resubmit_queue[sec]
            continue

        limit_px = _get_prefer_order_price(sec, data, snapshot=snapshot)
        if limit_px is None or _as_float(limit_px) <= 0:
            log.info("跳过重提 %s 原因=无有效委托价格" % sec)
            del g.resubmit_queue[sec]
            continue
        amount = _calc_stock_amount(budget, limit_px)
        if amount < 100:
            log.info(
                "跳过重提 %s 原因=委托量太小 预算=%.2f 价格=%.3f"
                % (sec, budget, _as_float(limit_px))
            )
            del g.resubmit_queue[sec]
            continue

        order_id = order(sec, amount, limit_price=limit_px)
        if order_id:
            _register_buy_order(
                order_id, sec, amount, budget,
                retry_count=retry_count, dt=context.blotter.current_dt
            )
            g.today_bought.append(sec)
            g.hold_days[sec] = 0
            g.daily_buy_submits += 1
            used_slots += 1
            offer1_qty = _get_offer1_qty(snapshot)
            log.info(
                "重提买入 %s 委托号=%d 重试=%d/%d 股数=%d 价格=%.3f 卖一量=%d"
                % (sec, order_id, retry_count, g.max_buy_retries, amount, _as_float(limit_px), offer1_qty)
            )
            del g.resubmit_queue[sec]
        else:
            log.info("重提买入失败 %s 重试=%d" % (sec, retry_count))

    return used_slots


def _cancel_pending_buy_orders(context, reason="tail_risk_control"):
    """盘尾撤销所有在途买单，降低无效挂单风险。"""
    orders = get_open_orders()
    if not orders:
        log.info("撤销在途买单: 无在途订单")
        return

    if isinstance(orders, dict):
        iterator = orders.values()
    else:
        iterator = orders

    cancel_count = 0
    for od in iterator:
        amount = _as_float(getattr(od, "amount", 0))
        if amount <= 0:
            continue
        oid = getattr(od, "id", None)
        sid = getattr(od, "sid", "")
        if oid is None:
            continue
        try:
            cancel_order(oid)
            cancel_count += 1
            if oid in g.buy_order_meta:
                del g.buy_order_meta[oid]
            if sid in g.resubmit_queue:
                del g.resubmit_queue[sid]
            log.info("撤销买单: 股票=%s 委托号=%s 原因=%s" % (sid, oid, reason))
        except Exception as e:
            log.info("撤销买单失败: 股票=%s 委托号=%s 错误=%s" % (sid, oid, e))
    log.info("撤销买单完成，共撤销 %d 个订单" % cancel_count)


# ================================== 辅助函数 ===================================

def _refresh_universe(context):
    """股票池动态维护为：候选池 + 当前持仓。"""
    universe = []
    if g.candidates:
        universe.extend(g.candidates)
    universe.extend(_get_held_stocks(context))
    universe = list(set(universe))
    if not universe:
        universe = [g.fallback_security]
    set_universe(universe)


def _roll_trade_day(context):
    """每个新交易日将仓龄统一+1。"""
    trade_day = context.blotter.current_dt.strftime("%Y%m%d")
    if g.last_trade_date == trade_day:
        return
    for sec in list(g.hold_days.keys()):
        g.hold_days[sec] = int(g.hold_days.get(sec, 0)) + 1
    g.last_trade_date = trade_day


def _sync_hold_days_with_positions(context):
    """持仓有而仓龄无：补0；仓龄有而持仓无：删除。"""
    held = _get_held_stocks(context)
    held_set = set(held)
    for sec in held:
        if sec not in g.hold_days:
            g.hold_days[sec] = 0
    for sec in list(g.hold_days.keys()):
        if sec not in held_set:
            del g.hold_days[sec]


def _get_held_stocks(context):
    """获取当前有效持仓列表。"""
    held = []
    for sec, pos in context.portfolio.positions.items():
        if _as_float(getattr(pos, "amount", 0)) > 0:
            held.append(sec)
    return held


def _save_day_start_snapshot(context):
    """记录当日开盘前资产快照。"""
    g.day_start_value = _as_float(context.portfolio.portfolio_value)
    g.day_start_cash = _as_float(context.portfolio.cash)
    log.info(
        "【日初资金快照】总资产=%.2f元 可用资金=%.2f元"
        % (g.day_start_value, g.day_start_cash)
    )


def _is_main_board_stock(security):
    """排除创业板(300/301)与科创板(688)。"""
    try:
        code, market = security.split(".")
    except Exception:
        return False
    if code.startswith("300"):
        return False
    if market == "SZ" and code.startswith("301"):
        return False
    if market == "SS" and code.startswith("688"):
        return False
    return True


def _get_limit_state(security):
    """使用手册推荐接口判断涨跌停状态。"""
    try:
        res = check_limit(security)
        if not res or security not in res:
            return 0
        return int(res[security])
    except Exception as e:
        log.info("涨跌停状态查询错误 %s: %s" % (security, e))
        return 0


def _get_prefer_order_price(security, data, snapshot=None):
    """获取最优委托价：实盘用快照涨停价/最新价，回测用bar close。"""
    if is_trade():
        snap = snapshot
        if snap is None:
            snap = _get_snapshot_safe(security)
        if snap:
            up_px = _as_float(snap.get("up_px", 0))
            if up_px > 0:
                return _round_stock_price(up_px)
            last_px = _as_float(snap.get("last_px", 0))
            if last_px > 0:
                return _round_stock_price(last_px)
    try:
        px = _as_float(data[security].close)
        if px > 0:
            return _round_stock_price(px)
    except Exception:
        return None
    return None


def _is_buy_time(context):
    """买入时间窗：避开集合竞价与尾盘最后几分钟。"""
    dt = context.blotter.current_dt
    hm = dt.hour * 100 + dt.minute
    pm_cutoff = _time_to_hm(g.buy_cutoff_time, 1430)
    if pm_cutoff > 1459:
        pm_cutoff = 1459
    return (931 <= hm <= 1130) or (1300 <= hm <= pm_cutoff)


def _has_open_order(security, side=None):
    """查询是否已有在途委托，防止重复下单。"""
    orders = get_open_orders()
    if not orders:
        return False

    if isinstance(orders, dict):
        iterator = orders.values()
    else:
        iterator = orders

    for od in iterator:
        sid = getattr(od, "sid", None)
        if sid is None:
            sid = getattr(od, "stock_code", None)
        if sid != security:
            continue

        amount = _as_float(getattr(od, "amount", 0))
        if side == "buy" and amount <= 0:
            continue
        if side == "sell" and amount >= 0:
            continue
        return True
    return False


def _get_snapshot_safe(security):
    """安全获取单只股票快照。"""
    try:
        snap = get_snapshot(security)
        if snap and security in snap:
            return snap[security]
    except Exception as e:
        log.info("快照获取错误 %s: %s" % (security, e))
    return None


def _check_live_entry(snapshot, security):
    """实盘优化入口检查：交易状态、涨停一致性、卖一量。"""
    if snapshot is None:
        return False, "no_snapshot"

    if g.require_trade_status:
        trade_status = str(snapshot.get("trade_status", ""))
        if trade_status != "TRADE":
            return False, "trade_status_%s" % trade_status

    up_px = _as_float(snapshot.get("up_px", 0))
    last_px = _as_float(snapshot.get("last_px", 0))
    if up_px <= 0 or last_px <= 0:
        return False, "invalid_price"

    gap = abs(last_px - up_px) / up_px
    if gap > g.max_price_gap_to_up:
        return False, "not_near_up_px gap=%.5f" % gap

    offer1_qty = _get_offer1_qty(snapshot)
    if offer1_qty < g.min_offer1_qty:
        return False, "offer1_qty_too_small=%s" % offer1_qty

    return True, "ok"


def _get_offer1_qty(snapshot):
    """获取卖一量。"""
    try:
        offer_grp = snapshot.get("offer_grp", {})
        level1 = offer_grp.get(1, [])
        if len(level1) >= 2:
            return int(_as_float(level1[1]))
    except Exception:
        return 0
    return 0


def _calc_stock_amount(budget, price):
    """A股按100股一手取整。"""
    price = _as_float(price)
    if budget <= 0 or price <= 0:
        return 0
    return int(budget / price / 100) * 100


def _register_buy_order(order_id, security, amount, budget, retry_count, dt):
    """注册买单元数据，用于超时管理。"""
    if order_id is None:
        return
    g.buy_order_meta[order_id] = {
        "security": security,
        "amount": int(amount),
        "budget": float(budget),
        "retry_count": int(retry_count),
        "submit_dt": dt,
    }


def _get_open_order_ids():
    """获取当前所有在途委托ID集合。"""
    orders = get_open_orders()
    if not orders:
        return set()
    if isinstance(orders, dict):
        iterator = orders.values()
    else:
        iterator = orders
    ids = set()
    for od in iterator:
        oid = getattr(od, "id", None)
        if oid is not None:
            ids.add(oid)
    return ids


def _minutes_diff(old_dt, new_dt):
    """计算两个时间的分钟差。"""
    if old_dt is None or new_dt is None:
        return 9999
    try:
        return int((new_dt - old_dt).total_seconds() / 60)
    except Exception:
        return 9999


def _time_to_hm(hhmm_str, default_hm):
    """将HH:MM字符串转为HHMM整数。"""
    try:
        parts = str(hhmm_str).split(":")
        h = int(parts[0])
        m = int(parts[1])
        return h * 100 + m
    except Exception:
        return int(default_hm)


def _is_backtest_entry_time(context):
    """回测下尽量贴近实盘，避开开盘最初几分钟。"""
    dt = context.blotter.current_dt
    hm = dt.hour * 100 + dt.minute
    start_hm = _time_to_hm(g.backtest_buy_start_time, 933)
    pm_cutoff = _time_to_hm(g.buy_cutoff_time, 1430)
    if pm_cutoff > 1459:
        pm_cutoff = 1459
    return (start_hm <= hm <= 1130) or (1300 <= hm <= pm_cutoff)


def _is_limit_up(close_px, high_limit_px):
    """用容差判断"收盘是否等于涨停价"。"""
    if close_px <= 0 or high_limit_px <= 0:
        return False
    diff = abs(close_px - high_limit_px) / high_limit_px
    return diff <= g.limit_up_tol


def _round_stock_price(px):
    """A股价格保留2位小数。"""
    return round(float(px) + 1e-8, 2)


def _as_float(v):
    """安全转换为float。"""
    try:
        return float(v)
    except Exception:
        return 0.0


def _get_stock_name(security):
    """获取股票中文名称。"""
    try:
        name_data = get_stock_name(security)
        result = None
        if isinstance(name_data, dict):
            if security in name_data:
                result = name_data.get(security)
            if result is None:
                switched = get_switch_code(security)
                if switched in name_data:
                    result = name_data.get(switched)
            if result is None and name_data:
                result = list(name_data.values())[0]
        elif isinstance(name_data, str):
            result = name_data

        if result is None:
            return ""
        result = str(result).strip()
        if not result:
            return ""
        return result
    except Exception:
        return ""


# ================================== 新增(V2)：buy_records持久化 ===================================

def _update_buy_records(context):
    """收盘后更新或清理持仓记录，用于持仓天数追踪。"""
    current_positions = list(context.portfolio.positions.keys())

    # 添加新买入的记录
    for code in g.today_bought:
        if code in current_positions and code not in g.buy_records:
            position = context.portfolio.positions[code]
            g.buy_records[code] = {
                'buy_date': context.blotter.current_dt.strftime('%Y-%m-%d'),
                'highest_price': _as_float(getattr(position, 'last_sale_price', 0))
            }

    # 更新最高价
    for code in list(g.buy_records.keys()):
        if code in current_positions:
            pos = context.portfolio.positions.get(code)
            if pos:
                last = _as_float(getattr(pos, 'last_sale_price', 0))
                if last > g.buy_records[code].get('highest_price', 0):
                    g.buy_records[code]['highest_price'] = last

    # 移除已卖出的记录
    for code in list(g.buy_records.keys()):
        if code not in current_positions:
            del g.buy_records[code]

    log.info("收盘后更新持仓记录: %s" % g.buy_records)


# ================================== 日终报告（V2增强） ===================================

def _log_daily_summary(context):
    """增强版日终资金汇总与持仓明细报告。"""
    end_value = _as_float(context.portfolio.portfolio_value)
    end_cash = _as_float(context.portfolio.cash)
    start_value = _as_float(g.day_start_value)
    start_cash = _as_float(g.day_start_cash)

    day_change = end_value - start_value
    day_change_pct = day_change / start_value if start_value > 0 else 0.0
    cash_change = end_cash - start_cash

    # 持仓统计
    position_count = 0
    position_market_value = 0.0
    for sec, pos in context.portfolio.positions.items():
        amount = int(_as_float(getattr(pos, "amount", 0)))
        if amount > 0:
            position_count += 1
            last = _as_float(getattr(pos, "last_sale_price", 0))
            cost = _as_float(getattr(pos, "cost_basis", 0))
            if last <= 0:
                last = cost
            position_market_value += amount * last

    # 与上一交易日比较
    prev_change = None
    prev_change_pct = None
    if g.prev_day_end_value is not None and _as_float(g.prev_day_end_value) > 0:
        prev_end = _as_float(g.prev_day_end_value)
        prev_change = end_value - prev_end
        prev_change_pct = prev_change / prev_end

    # 基准收益率
    base_capital = g.max_available_cash
    base_profit = end_value - base_capital
    base_profit_pct = (base_profit / base_capital) * 100 if base_capital > 0 else 0.0

    # 资金汇总表格
    log.info("=" * 60)
    log.info("【每日资金账户变化】(基准: %.0f元)", base_capital)
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %-27s |" % ("项目", "金额(元)"))
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %-27.2f |" % ("日初总资产", start_value))
    log.info("| %-28s | %-27.2f |" % ("日末总资产", end_value))
    log.info("| %-28s | %+27.2f |" % ("当日盈亏", day_change))
    log.info("| %-28s | %+27.2f%% |" % ("当日盈亏比例", day_change_pct * 100))
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %-27.2f |" % ("日初可用资金", start_cash))
    log.info("| %-28s | %-27.2f |" % ("日末可用资金", end_cash))
    log.info("| %-28s | %+27.2f |" % ("资金变化", cash_change))
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %-27.2f |" % ("持仓数量(只)", position_count))
    log.info("| %-28s | %-27.2f |" % ("持仓市值", position_market_value))
    log.info("| %-28s | %-27s |" % ("今日是否停止买入", "是" if g.stop_buy_for_today else "否"))
    log.info("| %-28s | %-27d |" % ("今日买入提交次数", g.daily_buy_submits))
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %+27.2f |" % ("基准收益(%.0f)" % base_capital, base_profit))
    log.info("| %-28s | %+27.2f%% |" % ("基准收益率", base_profit_pct))
    log.info("+" + "-" * 58 + "+")
    # 新增(V2)：显示当日买入/卖出股票
    log.info("【当日买入】%d只: %s" % (len(g.today_bought), ', '.join(g.today_bought) or '无'))
    log.info("【当日卖出】%d只: %s" % (len(g.today_sold_stocks), ', '.join(g.today_sold_stocks) or '无'))

    if prev_change is not None:
        log.info("【与上一交易日比较】")
        log.info("  昨日收盘: %.2f元 -> 今日收盘: %.2f元, 变化: %+.2f元 (%+.2f%%)"
            % (_as_float(g.prev_day_end_value), end_value, prev_change, prev_change_pct * 100))

    # 持仓明细（增强：含股票名称）
    hold_details = []
    for sec, pos in context.portfolio.positions.items():
        amount = int(_as_float(getattr(pos, "amount", 0)))
        if amount <= 0:
            continue
        enable_amount = int(_as_float(getattr(pos, "enable_amount", 0)))
        cost = _as_float(getattr(pos, "cost_basis", 0))
        last = _as_float(getattr(pos, "last_sale_price", 0))
        if last <= 0:
            last = cost
        market_value = amount * last
        pnl_ratio = (last - cost) / cost if cost > 0 else 0.0
        pnl_amount = market_value - (amount * cost)
        hold_day = int(g.hold_days.get(sec, 0))
        stock_name = _get_stock_name(sec)
        hold_details.append({
            "code": sec,
            "name": stock_name,
            "amount": amount,
            "enable": enable_amount,
            "cost": cost,
            "last": last,
            "market_value": market_value,
            "pnl_ratio": pnl_ratio * 100,
            "pnl_amount": pnl_amount,
            "hold_days": hold_day
        })

    log.info("=" * 60)
    if hold_details:
        log.info("【每日持仓明细】共 %d 只股票" % len(hold_details))
        log.info("+" + "-" * 14 + "+" + "-" * 12 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 10 + "+" + "-" * 10 + "+" + "-" * 8 + "+")
        log.info("| %-12s | %-10s | %-6s | %-6s | %-6s | %-6s | %-8s | %-8s | %-6s |" % ("股票代码", "股票名称", "持仓", "可用", "成本价", "现价", "盈亏金额", "盈亏比例", "天数"))
        log.info("+" + "-" * 14 + "+" + "-" * 12 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 10 + "+" + "-" * 10 + "+" + "-" * 8 + "+")
        for h in hold_details:
            name = h["name"] if h["name"] else "-"
            log.info("| %-12s | %-10s | %-6d | %-6d | %-6.2f | %-6.2f | %+8.2f | %+8.2f%% | %-6d |"
                % (h["code"], name, h["amount"], h["enable"], h["cost"], h["last"], h["pnl_amount"], h["pnl_ratio"], h["hold_days"]))
        log.info("+" + "-" * 14 + "+" + "-" * 12 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 8 + "+" + "-" * 10 + "+" + "-" * 10 + "+" + "-" * 8 + "+")
    else:
        log.info("【每日持仓明细】无持仓")
    log.info("=" * 60)

    # 保存当日收盘资金，供次日比较
    g.prev_day_end_value = end_value
    g.prev_day_end_cash = end_cash