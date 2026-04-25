# -*- coding: utf-8 -*-
"""
PTrade策略: 一进二打板（1进2）
参考 龙牛居《1进2打板战法》 + 打板四合一/弱转强V2框架。

战法核心思路（原文摘录）：
------------------------------------------------------------------
  "1进2 顾名思义就是昨天已经涨停，今天再板上瞬间买入"
  1、日线形态：5日均线向上攻击最佳 走平也行 下跌趋势不要
  2、分时形态：换手板最佳，分时有洗盘动作 先冲高回落 再上板最佳
              忌一字板、无量封板
  3、股性：以前有人气的股最好，不要庄股、不要坑人股
  4、热点：近期热点板块 大大提高胜率
  5、仓位：10w分5仓 每只2w 每天打2-3只
  6、止盈：T+1早盘冲高不板/10点左右不连板 → 高点出局 5-8%；
          分批出局 1/4留到下午14点左右不板再走
  7、止损：都是T+1必卖 打板进去炸了的找分时高点出 一般亏~10%
  8、大环境：昨日连板在5日线上方 30度角向上 最好有8-13板妖股
------------------------------------------------------------------

本策略工程化实现：
1. 盘前选股（before_trading_start）
   - 全A剔除ST/停牌/退市 + 仅保留主板(60/00开头，剔除创业板/科创板/北交)
   - 过滤新股（上市<50天）
   - 【昨日首板】昨日涨停封板 且 前日、大前日均未曾涨停（确保是首板）
   - 【5日均线过滤】昨日收盘 ≥ MA5(即今天的5日线)，禁止做5日线下跌趋势的票
   - 【换手板过滤】剔除一字板：昨日 open ≠ high_limit（不是一开盘就涨停）
   - 【非龙头高位】昨日收盘 距60日最高价 ≥ 0（涨停后成新高允许）但 20日涨幅 < 60%
   - 【流动性】昨日成交额 ≥ 2 亿

2. 盘中买入（触板打板，非竞价集合）
   - handle_data 分钟级扫描候选池，check_limit 识别触板(2)/封板(1)
   - 实盘：only_touch_board=True，只在触板瞬间下单（避开已封死的板）
   - 校验：trade_status=TRADE、最新价贴涨停价(≤0.1%)、卖一量 ≥ 1000 股
   - 以涨停价挂单，2 分钟未成交自动撤单+最多 2 次重试
   - 日内提交上限 3 次（战法要求"每天打2-3只"）
   - 日内回撤 > 3% 触发熔断，停止新开仓

3. T+1 卖出（战法规则硬实现）
   - 10:00 分批卖：如果未涨停 → 卖 3/4，保留 1/4
   - 14:00 尾盘再扫：如果仍未涨停 → 全部卖出
   - 任意时刻浮亏 ≤ -8% 即全部止损（战法口径 ~10% 放宽到 -8%）
   - run_interval 每 3 秒卖单补单（跌停不撤，非跌停撤单+市价99%重挂）

4. 仓位
   - 最多 3 票，单票 20% 仓位，总体控制在 60% 以内

5. V2 实盘护城河
   - 授权验证 permission_test
   - buy_records 持久化持仓天数
   - 卖单补单 run_interval 每 3 秒检查
   - 日终增强报告

运行建议：
  业务类型: 股票 | 频率: 分钟 | 实盘/回测均支持
"""

import datetime


# ================================== 策略函数 ===================================

def initialize(context):
    """策略初始化函数，只在策略启动时运行一次。"""
    # --- 授权验证（V2框架） ---
    if is_trade():
        auth_result = permission_test()
        if not auth_result:
            log.error("=" * 50)
            log.error("【授权失败】一进二打板策略无权在当前账户或时间运行！")
            log.error("=" * 50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，一进二打板策略启动成功")

    # --- 仓位参数（战法：10w分5仓，每只2w；这里改为主账户5仓×20%，最多同时持3只）---
    g.max_positions = 3
    g.single_position_ratio = 0.20      # 单票20%（战法：2w/10w）
    g.min_cash_per_order = 2000.0
    g.max_available_cash = 100000.0     # 基准资金

    # --- 止盈止损（T+1 硬卖）---
    g.stop_loss_ratio = -0.08           # 盘中浮亏-8%立即止损
    g.max_hold_days = 3                 # 超过3天强制清仓兜底
    g.max_daily_loss_pct = -0.03        # 日内回撤3%停止新开仓

    # --- T+1 分批卖时点（战法原文："10点左右不连板就出" + "1/4留到14点"）---
    g.sell_partial_time = "10:00"       # 早盘分批卖（未涨停卖3/4）
    g.sell_final_time = "14:00"         # 尾盘总清仓（未涨停全卖）
    g.sell_partial_ratio = 0.75         # 早盘卖出比例

    # --- 盘前选股参数 ---
    g.min_yesterday_turnover = 2e8      # 昨日成交额≥2亿
    g.ma5_days = 5                      # 5日均线
    g.max_20d_gain = 0.60               # 20日涨幅<60%（非高位龙头）
    g.exclude_one_word_board = True     # 剔除一字板（open==high_limit）
    g.max_watchlist = 60                # 候选池上限
    g.fallback_security = "000001.SZ"

    # --- 打板识别参数 ---
    g.limit_up_tol = 0.001              # 涨停价判定容差
    g.only_touch_board = True           # 实盘：只在触板瞬间下单
    g.require_trade_status = True       # 实盘：必须 trade_status=TRADE
    g.min_offer1_qty = 1000             # 实盘：卖一量最低 1000 股
    g.max_price_gap_to_up = 0.001       # 最新价到涨停价的最大偏离(0.1%)

    # --- 买入/撤单时间 ---
    g.buy_start_time = "09:31"          # 避开集合竞价
    g.buy_cutoff_time = "14:30"         # 打板截止
    g.cancel_buy_time = "14:56"         # 尾盘撤销所有在途买单

    # --- 订单管理 ---
    g.order_timeout_minutes = 2
    g.max_buy_retries = 2
    g.max_daily_buy_submits = 3         # 战法：每天打2-3只

    # --- 回测开关 ---
    g.backtest_follow_live = True
    g.backtest_allow_limitup_as_touch = True

    # --- 运行期状态变量 ---
    g.candidates = []
    g.today_bought = []
    g.today_sold_stocks = set()
    g.hold_days = {}
    g.partial_sold = set()              # 记录早盘已部分卖出的股票
    g.last_trade_date = ""
    g.day_start_value = None
    g.day_start_cash = None
    g.prev_day_end_value = None
    g.prev_day_end_cash = None
    g.stop_buy_for_today = False
    g.daily_buy_submits = 0
    g.buy_order_meta = {}
    g.resubmit_queue = {}

    # --- V2 ---
    g.sell_orders = {}
    g.sell_retry_count = {}
    g.max_sell_retry = 10
    g.buy_records = {}

    # --- 股票池 ---
    set_universe([g.fallback_security])

    # --- PTrade实盘/回测设置 ---
    if is_trade():
        set_parameters(holiday_not_do_before="1", server_restart_not_do_before="1")
    else:
        set_volume_ratio(0.9)
        set_limit_mode("LIMIT")

    # --- 定时任务 ---
    # T+1 分批卖出：10:00 卖 3/4，14:00 全清
    run_daily(context, sell_partial_task, time=g.sell_partial_time)
    run_daily(context, sell_final_task, time=g.sell_final_time)
    # 尾盘撤销在途买单
    run_daily(context, _cancel_pending_buy_orders_task, time=g.cancel_buy_time)
    # 实盘卖出补单（每3秒）
    run_interval(context, check_and_retry_sell_orders, seconds=3)

    log.info("一进二打板策略初始化完成，频率=%s，模式=%s" %
             (get_frequency(), "实盘" if is_trade() else "回测"))


def before_trading_start(context, data):
    """每日开盘前：1进2 候选池筛选。"""
    _roll_trade_day(context)
    _sync_hold_days_with_positions(context)
    g.today_bought = []
    g.today_sold_stocks = set()
    g.partial_sold = set()
    g.stop_buy_for_today = False
    g.daily_buy_submits = 0
    g.buy_order_meta = {}
    g.resubmit_queue = {}

    if is_trade():
        g.sell_orders.clear()
        g.sell_retry_count.clear()
        log.info("[Ptrade实盘] 卖出订单追踪记录已清空")

    _save_day_start_snapshot(context)

    log.info("=" * 50)
    log.info(">>> [一进二打板] 开始执行盘前选股任务...")

    stocks = get_Ashares()
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        log.info("未获取到A股列表")
        return

    stocks = filter_stock_by_status(stocks, ["ST", "HALT", "DELISTING", "DELISTING_SORTING"])
    stocks = [s for s in stocks if _is_main_board_stock(s)]
    stocks = _filter_new_stocks(stocks, min_days=50)
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        log.info("基础过滤后无股票")
        return

    g.candidates = _pick_yijin_er_candidates(stocks)
    _refresh_universe(context)
    log.info("[一进二打板] 今日候选股票数量: %d" % len(g.candidates))
    if g.candidates:
        log.info("[一进二打板] 候选: %s" % g.candidates[:20])
    log.info(">>> 盘前选股任务执行完毕。")


def handle_data(context, data):
    """盘中逐bar扫描，只负责买入（卖出由定时任务+盘中止损负责）。"""
    # 盘中止损：浮亏-8%立即全卖
    _manage_stop_loss(context, data)
    _update_intraday_risk_flags(context)
    _manage_pending_buy_orders(context, data)
    if not _is_buy_time(context):
        return
    _manage_entry(context, data)


def after_trading_end(context, data):
    """每日收盘后。"""
    _log_daily_summary(context)
    _update_buy_records(context)


# ================================== 盘前选股 ===================================

def _pick_yijin_er_candidates(stocks):
    """
    盘前筛选"昨日首板"候选池，强化战法要求：
    1. 昨日涨停封板（收盘=涨停价）
    2. 前日+大前日均未曾涨停（保证是"首板"而非连板延续）
    3. 剔除一字板（昨日 open ≠ high_limit）
    4. 5日均线向上或走平：昨日收盘 ≥ 5日均线
    5. 非高位：20日涨幅 < 60%
    6. 昨日成交额 ≥ 2 亿
    """
    fields = ["open", "close", "high", "low", "high_limit", "volume", "money", "is_open", "unlimited"]
    # 取近 20 根日K，既用来判定"前日/大前日是否涨停"，也用来算MA5和20日涨幅
    hist = get_history(20, "1d", fields, security_list=stocks, fq=None, include=False)
    if hist is None or len(hist) == 0 or "code" not in hist.columns:
        return []

    result = []
    for sec, df in hist.groupby("code"):
        if len(df) < 6:
            continue
        df = df.sort_index()
        y_bar = df.iloc[-1]          # 昨日
        pre1 = df.iloc[-2]           # 前日
        pre2 = df.iloc[-3]           # 大前日

        # ---- 昨日正常交易 ----
        if int(_as_float(y_bar.get("is_open", 1))) != 1:
            continue
        if _as_float(y_bar.get("volume", 0)) <= 0:
            continue
        if int(_as_float(y_bar.get("unlimited", 0))) == 1:
            continue

        y_close = _as_float(y_bar.get("close", 0))
        y_open = _as_float(y_bar.get("open", 0))
        y_high_limit = _as_float(y_bar.get("high_limit", 0))
        y_money = _as_float(y_bar.get("money", 0))

        # ---- 流动性：≥2亿 ----
        if y_money < g.min_yesterday_turnover:
            continue

        # ---- 昨日收盘=涨停价 ----
        if not _is_limit_up(y_close, y_high_limit):
            continue

        # ---- 剔除一字板：open≠high_limit，即开盘就没一字封死 ----
        if g.exclude_one_word_board:
            if y_open > 0 and y_high_limit > 0 and _is_limit_up(y_open, y_high_limit):
                continue

        # ---- 前日、大前日均未曾涨停（最高价<涨停价）保证是真·首板 ----
        def _ever_limit(bar):
            if int(_as_float(bar.get("unlimited", 0))) == 1:
                return False
            return _is_limit_up(_as_float(bar.get("high", 0)),
                                _as_float(bar.get("high_limit", 0)))
        if _ever_limit(pre1) or _ever_limit(pre2):
            continue

        # ---- 5日均线过滤：昨日收盘 ≥ MA5 (向上或走平) ----
        closes = df["close"].astype(float).values
        if len(closes) >= g.ma5_days:
            ma5 = float(sum(closes[-g.ma5_days:])) / g.ma5_days
            if y_close < ma5:
                continue

        # ---- 非高位：20日涨幅 < 60% ----
        if len(closes) >= 20:
            c20 = float(closes[-20])
            if c20 > 0:
                gain20 = (y_close - c20) / c20
                if gain20 > g.max_20d_gain:
                    continue

        result.append((sec, y_money))

    # 按昨日成交额降序，控制池子规模
    result.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in result[:g.max_watchlist]]


def _filter_new_stocks(stock_list, min_days=50):
    """过滤上市不足min_days天的新股。"""
    if not stock_list:
        return []
    try:
        info_dict = get_stock_info(stock_list)
        if not info_dict:
            return stock_list
        result = []
        today = datetime.datetime.now().date()
        for code in stock_list:
            info = info_dict.get(code, {})
            list_date = info.get('list_date', '')
            if not list_date:
                result.append(code)
                continue
            try:
                if isinstance(list_date, str):
                    ld = datetime.datetime.strptime(list_date[:10], '%Y-%m-%d').date()
                else:
                    ld = list_date
                if (today - ld).days >= min_days:
                    result.append(code)
            except Exception:
                result.append(code)
        return result
    except Exception as e:
        log.info("filter_new_stocks error: %s" % e)
        return stock_list


# ================================== 盘中买入（打板） ===================================

def _manage_entry(context, data):
    """盘中打板买入：触板/封板即入。"""
    if not g.candidates:
        return
    if g.stop_buy_for_today:
        return
    if g.daily_buy_submits >= g.max_daily_buy_submits:
        return

    held = _get_held_stocks(context)
    if len(held) >= g.max_positions:
        return

    available_slots = g.max_positions - len(held)
    cash = _as_float(context.portfolio.cash)
    if cash < g.min_cash_per_order:
        return

    # 战法仓位：单票20% portfolio_value，但不超过可用现金/剩余仓位
    cap_by_value = _as_float(context.portfolio.portfolio_value) * g.single_position_ratio
    budget = min(cash / max(available_slots, 1), cap_by_value)
    max_per_stock = g.max_available_cash * g.single_position_ratio
    budget = min(budget, max_per_stock)
    if budget < g.min_cash_per_order:
        return

    # 先处理重提队列
    used_slots = _process_resubmit_queue(context, data, available_slots, budget)
    available_slots -= used_slots
    if available_slots <= 0:
        return

    for sec in g.candidates:
        if available_slots <= 0:
            break
        if g.daily_buy_submits >= g.max_daily_buy_submits:
            break
        if sec in held:
            continue
        if sec in g.today_bought:
            continue
        if sec not in data:
            continue
        if _has_open_order(sec, side="buy"):
            continue

        limit_state = _get_limit_state(sec)
        if limit_state not in (1, 2):
            continue
        if is_trade() and g.only_touch_board and limit_state != 2:
            continue

        snapshot = None
        if is_trade():
            snapshot = _get_snapshot_safe(sec)
            pass_check, reject_reason = _check_live_entry(snapshot, sec)
            if not pass_check:
                log.info("跳过买入 %s 原因=%s" % (sec, reject_reason))
                continue
        else:
            if g.backtest_follow_live and not _is_backtest_entry_time(context):
                continue
            if g.backtest_follow_live and g.only_touch_board:
                if limit_state == 2:
                    pass
                elif limit_state == 1 and g.backtest_allow_limitup_as_touch:
                    pass
                else:
                    continue

        limit_px = _get_prefer_order_price(sec, data, snapshot=snapshot)
        if limit_px is None or _as_float(limit_px) <= 0:
            log.info("跳过买入 %s 原因=无有效委托价格" % sec)
            continue

        amount = _calc_stock_amount(budget, limit_px)
        if amount < 100:
            log.info("跳过买入 %s 原因=委托量太小 预算=%.2f 价格=%.3f" %
                     (sec, budget, _as_float(limit_px)))
            continue

        order_id = order(sec, amount, limit_price=limit_px)
        if order_id:
            if is_trade():
                _register_buy_order(order_id, sec, amount, budget,
                                    retry_count=0, dt=context.blotter.current_dt)
            g.today_bought.append(sec)
            g.hold_days[sec] = 0
            available_slots -= 1
            g.daily_buy_submits += 1
            trigger = "触板涨停" if limit_state == 2 else "涨停封板"
            log.info("[一进二]买入 %s 委托号=%s 触发=%s 预算=%.2f 委托价=%.3f 股数=%d 今日提交=%d/%d" %
                     (sec, order_id, trigger, budget, _as_float(limit_px), amount,
                      g.daily_buy_submits, g.max_daily_buy_submits))
        else:
            log.info("买入失败 %s 原因=委托API返回None" % sec)


# ================================== 盘中止损 ===================================

def _manage_stop_loss(context, data):
    """盘中止损：浮亏≤-8%即全卖（战法：T+1炸板亏~10% 放宽到-8%）。"""
    positions = context.portfolio.positions
    for sec, pos in positions.items():
        amount = int(_as_float(getattr(pos, "amount", 0)))
        if amount <= 0:
            continue
        if sec not in data:
            continue
        # T+1 限制：当日买入不可卖
        if sec in g.today_bought:
            continue
        if sec in g.today_sold_stocks:
            continue
        if _has_open_order(sec, side="sell"):
            continue

        cost = _as_float(getattr(pos, "cost_basis", 0))
        last_price = _as_float(data[sec].close)
        if cost <= 0 or last_price <= 0:
            continue

        pnl_ratio = (last_price - cost) / cost
        hold_days = int(g.hold_days.get(sec, 0))
        should_stop_loss = (pnl_ratio <= g.stop_loss_ratio) or (hold_days >= g.max_hold_days)
        if not should_stop_loss:
            continue

        enable_amount = int(_as_float(getattr(pos, "enable_amount", 0)))
        if enable_amount <= 0:
            p = get_position(sec)
            enable_amount = int(_as_float(getattr(p, "enable_amount", 0)))
        if enable_amount <= 0:
            continue

        reason = "止损-8%%" if pnl_ratio <= g.stop_loss_ratio else "超期%d天" % hold_days
        order_id = order(sec, -enable_amount)
        if order_id:
            g.today_sold_stocks.add(sec)
            log.info("[一进二盘中止损]卖出 %s 委托号=%s 原因=%s 持仓%d天 收益=%.2f%% 成本=%.3f 现价=%.3f" %
                     (sec, order_id, reason, hold_days, pnl_ratio * 100, cost, last_price))
            if is_trade():
                g.sell_orders[order_id] = {
                    'stock': sec, 'reason': reason,
                    'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                    'retry_count': 0
                }


# ================================== T+1 定时卖出 ===================================

def sell_partial_task(context):
    """
    10:00 早盘分批卖：
    战法原文："10点左右不能连板 就高点出局 5-8% 最少出掉一半以上"
    → 对昨日买入的持仓：如果未涨停，卖出 3/4
    """
    _sell_positions_if_not_limit_up(context, ratio=g.sell_partial_ratio,
                                     tag="早盘分批", add_to_partial=True)


def sell_final_task(context):
    """
    14:00 尾盘全清：
    战法原文："打板封住，但是第二天不给溢价，最多拿到下午14点左右高点也要走"
    → 如果未涨停，全部卖出
    """
    _sell_positions_if_not_limit_up(context, ratio=1.0, tag="尾盘清仓",
                                     add_to_partial=False)


def _sell_positions_if_not_limit_up(context, ratio, tag, add_to_partial):
    """通用卖出：对非当日买入、非涨停的持仓，按比例卖出。"""
    current_time = context.blotter.current_dt
    log.info("--- %s: [一进二]触发%s任务 ---" %
             (current_time.strftime('%Y-%m-%d %H:%M:%S'), tag))

    positions = context.portfolio.positions
    if not positions:
        log.info("当前无持仓，无需卖出")
        return

    sellable = [code for code in positions
                if code not in g.today_bought
                and code not in g.today_sold_stocks
                and _as_float(getattr(positions[code], 'amount', 0)) > 0]
    if not sellable:
        log.info("无符合T+1规则的可卖持仓")
        return

    for code in sellable:
        pos = positions[code]
        # 早盘分批：如果已经部分卖过，就跳过（只卖一次）
        if add_to_partial and code in g.partial_sold:
            continue

        # 获取当前价和涨停价
        last_px, high_limit = _get_current_price_and_limit(code, is_trade_mode=is_trade())
        if last_px <= 0:
            continue
        # 涨停不卖
        if high_limit > 0 and last_px >= high_limit * 0.999:
            log.info("[%s]%s 涨停中(%.3f)，不卖" % (tag, code, last_px))
            continue

        cost = _as_float(getattr(pos, 'cost_basis', 0))
        ret = (last_px / cost - 1) * 100 if cost > 0 else 0

        # 计算要卖多少股
        enable_amount = int(_as_float(getattr(pos, 'enable_amount', 0)))
        if enable_amount <= 0:
            p = get_position(code)
            enable_amount = int(_as_float(getattr(p, 'enable_amount', 0)))
        if enable_amount <= 0:
            continue

        sell_amount = int(enable_amount * ratio / 100) * 100  # 按100股向下取整
        if sell_amount <= 0:
            sell_amount = enable_amount  # 小仓位直接全卖
        if sell_amount > enable_amount:
            sell_amount = enable_amount

        log.info("[%s]%s 现价=%.3f 成本=%.3f 收益=%+.2f%% 卖出=%d/%d股" %
                 (tag, code, last_px, cost, ret, sell_amount, enable_amount))

        order_id = order(code, -sell_amount)
        if order_id:
            if ratio >= 1.0 or sell_amount >= enable_amount:
                g.today_sold_stocks.add(code)
            if add_to_partial:
                g.partial_sold.add(code)
            if is_trade():
                g.sell_orders[order_id] = {
                    'stock': code,
                    'reason': '%s(收益%.2f%%)' % (tag, ret),
                    'time': current_time.strftime('%H:%M:%S'),
                    'retry_count': 0
                }


def _get_current_price_and_limit(code, is_trade_mode):
    """获取当前价和涨停价，实盘用快照，回测用1分钟K线+1日涨停价。"""
    if is_trade_mode:
        snap = _get_snapshot_safe(code)
        if snap:
            last_px = _as_float(snap.get('last_px', 0))
            up_px = _as_float(snap.get('up_px', 0))
            return last_px, up_px
        return 0.0, 0.0
    else:
        try:
            h1m = get_history(1, '1m', ['close'], security_list=code, include=True)
            h1d = get_history(1, '1d', ['high_limit'], security_list=code)
            if h1m is None or h1m.empty or h1d is None or h1d.empty:
                return 0.0, 0.0
            if 'code' in h1m.columns:
                h1m = h1m[h1m['code'] == code]
            if 'code' in h1d.columns:
                h1d = h1d[h1d['code'] == code]
            last_px = _as_float(h1m['close'].iloc[-1]) if not h1m.empty else 0
            high_limit = _as_float(h1d['high_limit'].iloc[-1]) if not h1d.empty else 0
            return last_px, high_limit
        except Exception as e:
            log.info("回测价格获取失败 %s: %s" % (code, e))
            return 0.0, 0.0


# ================================== 卖出补单（V2） ===================================

def check_and_retry_sell_orders(context):
    """[实盘专用] 卖出订单补单检查，每3秒执行一次。"""
    if not is_trade():
        return
    if not g.sell_orders:
        return

    current_time = context.blotter.current_dt

    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '未知')
        retry_count = order_info.get('retry_count', 0)

        stock_total_retry = g.sell_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_sell_retry or retry_count >= g.max_sell_retry:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if not order_list or len(order_list) == 0:
                continue

            order_status = order_list[0]
            status = order_status.status

            if status == '8':
                log.info("[卖出补单] %s 已全部成交" % stock)
                orders_to_remove.append(order_id)

            elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:
                snapshot = _get_snapshot_safe(stock)
                if snapshot:
                    current_px = _as_float(snapshot.get("last_px", 0))
                    down_px = _as_float(snapshot.get("down_px", 0))
                    if down_px > 0 and abs(current_px - down_px) < 0.01:
                        log.info("[卖出补单] %s 跌停(%.3f)，不撤单" % (stock, current_px))
                        continue
                try:
                    cancel_order(order_id)
                    orders_to_remove.append(order_id)
                    orders_to_retry.append({
                        'stock': stock, 'reason': reason,
                        'retry_count': retry_count + 1
                    })
                except Exception as e:
                    log.info("[卖出补单] 撤单失败 %s: %s" % (order_id, e))

            elif status in ['6', '9']:
                orders_to_remove.append(order_id)
                orders_to_retry.append({
                    'stock': stock, 'reason': reason,
                    'retry_count': retry_count + 1
                })

        except Exception as e:
            log.error("[卖出补单] 异常 %s: %s" % (order_id, e))

    for oid in orders_to_remove:
        if oid in g.sell_orders:
            del g.sell_orders[oid]

    if orders_to_retry:
        stocks_to_snap = [item['stock'] for item in orders_to_retry]
        snapshots = get_snapshot(stocks_to_snap)
        if not snapshots:
            return

        for item in orders_to_retry:
            stock = item['stock']
            reason = item['reason']
            r_count = item['retry_count']

            position = context.portfolio.positions.get(stock)
            if not position or _as_float(getattr(position, "amount", 0)) <= 0:
                continue

            snapshot = snapshots.get(stock)
            if not snapshot:
                continue
            current_price = _as_float(snapshot.get('last_px', 0))
            if current_price <= 0:
                continue

            limit_price = current_price * 0.99
            down_px = _as_float(snapshot.get('down_px', current_price * 0.9))
            limit_price = max(limit_price, down_px)
            limit_price = _round_stock_price(limit_price)

            enable_amount = int(_as_float(getattr(position, "enable_amount", 0)))
            if enable_amount <= 0:
                p = get_position(stock)
                enable_amount = int(_as_float(getattr(p, "enable_amount", 0)))
            if enable_amount <= 0:
                continue

            try:
                new_oid = order(stock, -enable_amount, limit_price=limit_price)
                if new_oid:
                    g.sell_orders[new_oid] = {
                        'stock': stock, 'reason': reason,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': r_count
                    }
                    g.sell_retry_count[stock] = r_count
                    log.info("[卖出补单] 补单成功 %s (第%d次)" % (stock, r_count))
            except Exception as e:
                log.error("[卖出补单] 补单异常 %s: %s" % (stock, e))


# ================================== 风控与买单管理 ===================================

def _update_intraday_risk_flags(context):
    """日内回撤>3%停止新开仓。"""
    if not is_trade():
        return
    start_value = _as_float(g.day_start_value)
    if start_value <= 0:
        return
    cur_value = _as_float(context.portfolio.portfolio_value)
    day_ret = (cur_value - start_value) / start_value
    if (not g.stop_buy_for_today) and day_ret <= g.max_daily_loss_pct:
        g.stop_buy_for_today = True
        log.info("【风控】当日回撤=%.2f%%≤%.2f%%，今日停止买入" %
                 (day_ret * 100, g.max_daily_loss_pct * 100))
        _cancel_pending_buy_orders(context, reason="daily_loss_stop")


def _manage_pending_buy_orders(context, data):
    """买单超时撤单+加入重提队列。"""
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
            log.info("撤销超时买单 %s 委托号=%s 耗时=%d分钟 重试=%d" %
                     (sec, oid, elapsed, retry_count))
        except Exception as e:
            log.info("撤销超时买单失败 %s 委托号=%s: %s" % (sec, oid, e))
            continue

        if (not g.stop_buy_for_today) and retry_count < g.max_buy_retries:
            g.resubmit_queue[sec] = {
                "budget": _as_float(meta.get("budget", 0)),
                "retry_count": retry_count + 1,
                "last_order_id": oid,
            }
            log.info("加入重提队列 %s 重试=%d/%d" %
                     (sec, retry_count + 1, g.max_buy_retries))
        del g.buy_order_meta[oid]


def _process_resubmit_queue(context, data, available_slots, default_budget):
    """重提撤单的买单。"""
    if not is_trade():
        return 0
    if available_slots <= 0 or not g.resubmit_queue:
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
        budget = _as_float(task.get("budget", default_budget))
        retry_count = int(task.get("retry_count", 1))
        if budget < g.min_cash_per_order:
            del g.resubmit_queue[sec]
            continue

        limit_px = _get_prefer_order_price(sec, data, snapshot=snapshot)
        if limit_px is None or _as_float(limit_px) <= 0:
            del g.resubmit_queue[sec]
            continue
        amount = _calc_stock_amount(budget, limit_px)
        if amount < 100:
            del g.resubmit_queue[sec]
            continue

        order_id = order(sec, amount, limit_price=limit_px)
        if order_id:
            _register_buy_order(order_id, sec, amount, budget,
                                retry_count=retry_count, dt=context.blotter.current_dt)
            g.today_bought.append(sec)
            g.hold_days[sec] = 0
            g.daily_buy_submits += 1
            used_slots += 1
            log.info("[一进二重提]买入 %s 委托号=%s 重试=%d/%d 股数=%d 价格=%.3f" %
                     (sec, order_id, retry_count, g.max_buy_retries, amount,
                      _as_float(limit_px)))
            del g.resubmit_queue[sec]
        else:
            log.info("[一进二重提]买入失败 %s 重试=%d" % (sec, retry_count))

    return used_slots


def _cancel_pending_buy_orders_task(context):
    """尾盘定时撤销在途买单（run_daily 入口）。"""
    _cancel_pending_buy_orders(context, reason="tail_risk_control")


def _cancel_pending_buy_orders(context, reason="tail_risk_control"):
    """撤销所有在途买单。"""
    orders = get_open_orders()
    if not orders:
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
            log.info("撤销买单 %s 委托号=%s 原因=%s" % (sid, oid, reason))
        except Exception as e:
            log.info("撤销买单失败 %s 委托号=%s: %s" % (sid, oid, e))
    if cancel_count > 0:
        log.info("撤销在途买单完成 共%d个" % cancel_count)


# ================================== 辅助函数 ===================================

def _refresh_universe(context):
    universe = []
    if g.candidates:
        universe.extend(g.candidates)
    universe.extend(_get_held_stocks(context))
    universe = list(set(universe))
    if not universe:
        universe = [g.fallback_security]
    set_universe(universe)


def _roll_trade_day(context):
    trade_day = context.blotter.current_dt.strftime("%Y%m%d")
    if g.last_trade_date == trade_day:
        return
    for sec in list(g.hold_days.keys()):
        g.hold_days[sec] = int(g.hold_days.get(sec, 0)) + 1
    g.last_trade_date = trade_day


def _sync_hold_days_with_positions(context):
    held = _get_held_stocks(context)
    held_set = set(held)
    for sec in held:
        if sec not in g.hold_days:
            g.hold_days[sec] = 0
    for sec in list(g.hold_days.keys()):
        if sec not in held_set:
            del g.hold_days[sec]


def _get_held_stocks(context):
    return [sec for sec, pos in context.portfolio.positions.items()
            if _as_float(getattr(pos, "amount", 0)) > 0]


def _save_day_start_snapshot(context):
    g.day_start_value = _as_float(context.portfolio.portfolio_value)
    g.day_start_cash = _as_float(context.portfolio.cash)
    log.info("【日初资金快照】总资产=%.2f元 可用资金=%.2f元" %
             (g.day_start_value, g.day_start_cash))


def _is_main_board_stock(security):
    """仅保留主板（60/00开头），排除创业板(300/301)、科创板(688)、北交。"""
    try:
        code, market = security.split(".")
    except Exception:
        return False
    if code[:2] not in ('60', '00'):
        return False
    return True


def _get_limit_state(security):
    try:
        res = check_limit(security)
        if not res or security not in res:
            return 0
        return int(res[security])
    except Exception as e:
        log.info("check_limit 错误 %s: %s" % (security, e))
        return 0


def _get_prefer_order_price(security, data, snapshot=None):
    """实盘：快照up_px > last_px；回测：bar.close。"""
    if is_trade():
        snap = snapshot if snapshot is not None else _get_snapshot_safe(security)
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
    dt = context.blotter.current_dt
    hm = dt.hour * 100 + dt.minute
    start_hm = _time_to_hm(g.buy_start_time, 931)
    pm_cutoff = _time_to_hm(g.buy_cutoff_time, 1430)
    return (start_hm <= hm <= 1130) or (1300 <= hm <= pm_cutoff)


def _is_backtest_entry_time(context):
    return _is_buy_time(context)


def _has_open_order(security, side=None):
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
    try:
        snap = get_snapshot(security)
        if snap and security in snap:
            return snap[security]
    except Exception as e:
        log.info("快照获取错误 %s: %s" % (security, e))
    return None


def _check_live_entry(snapshot, security):
    """实盘买入校验。"""
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
        return False, "offer1_qty=%s<min" % offer1_qty

    return True, "ok"


def _get_offer1_qty(snapshot):
    try:
        offer_grp = snapshot.get("offer_grp", {})
        level1 = offer_grp.get(1, [])
        if len(level1) >= 2:
            return int(_as_float(level1[1]))
    except Exception:
        return 0
    return 0


def _calc_stock_amount(budget, price):
    price = _as_float(price)
    if budget <= 0 or price <= 0:
        return 0
    return int(budget / price / 100) * 100


def _register_buy_order(order_id, security, amount, budget, retry_count, dt):
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
    if old_dt is None or new_dt is None:
        return 9999
    try:
        return int((new_dt - old_dt).total_seconds() / 60)
    except Exception:
        return 9999


def _time_to_hm(hhmm_str, default_hm):
    try:
        parts = str(hhmm_str).split(":")
        return int(parts[0]) * 100 + int(parts[1])
    except Exception:
        return int(default_hm)


def _is_limit_up(close_px, high_limit_px):
    if close_px <= 0 or high_limit_px <= 0:
        return False
    return abs(close_px - high_limit_px) / high_limit_px <= g.limit_up_tol


def _round_stock_price(px):
    return round(float(px) + 1e-8, 2)


def _as_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _get_stock_name(security):
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
        return str(result).strip() if result else ""
    except Exception:
        return ""


# ================================== buy_records 持久化 ===================================

def _update_buy_records(context):
    current_positions = list(context.portfolio.positions.keys())
    for code in g.today_bought:
        if code in current_positions and code not in g.buy_records:
            position = context.portfolio.positions[code]
            g.buy_records[code] = {
                'buy_date': context.blotter.current_dt.strftime('%Y-%m-%d'),
                'highest_price': _as_float(getattr(position, 'last_sale_price', 0))
            }
    for code in list(g.buy_records.keys()):
        if code in current_positions:
            pos = context.portfolio.positions.get(code)
            if pos:
                last = _as_float(getattr(pos, 'last_sale_price', 0))
                if last > g.buy_records[code].get('highest_price', 0):
                    g.buy_records[code]['highest_price'] = last
    for code in list(g.buy_records.keys()):
        if code not in current_positions:
            del g.buy_records[code]


# ================================== 日终报告 ===================================

def _log_daily_summary(context):
    end_value = _as_float(context.portfolio.portfolio_value)
    end_cash = _as_float(context.portfolio.cash)
    start_value = _as_float(g.day_start_value)
    start_cash = _as_float(g.day_start_cash)

    day_change = end_value - start_value
    day_change_pct = day_change / start_value if start_value > 0 else 0.0
    cash_change = end_cash - start_cash

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

    prev_change = None
    prev_change_pct = None
    if g.prev_day_end_value is not None and _as_float(g.prev_day_end_value) > 0:
        prev_end = _as_float(g.prev_day_end_value)
        prev_change = end_value - prev_end
        prev_change_pct = prev_change / prev_end

    base_capital = g.max_available_cash
    base_profit = end_value - base_capital
    base_profit_pct = (base_profit / base_capital) * 100 if base_capital > 0 else 0.0

    log.info("=" * 60)
    log.info("【一进二打板 日终报告】基准=%.0f元" % base_capital)
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
    log.info("| %-28s | %-27d |" % ("持仓数量(只)", position_count))
    log.info("| %-28s | %-27.2f |" % ("持仓市值", position_market_value))
    log.info("| %-28s | %-27s |" % ("今日是否停止买入", "是" if g.stop_buy_for_today else "否"))
    log.info("| %-28s | %-27d |" % ("今日买入提交次数", g.daily_buy_submits))
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %+27.2f |" % ("基准收益(%.0f)" % base_capital, base_profit))
    log.info("| %-28s | %+27.2f%% |" % ("基准收益率", base_profit_pct))
    log.info("+" + "-" * 58 + "+")
    log.info("【当日买入】%d只: %s" % (len(g.today_bought), ', '.join(g.today_bought) or '无'))
    log.info("【当日卖出】%d只: %s" % (len(g.today_sold_stocks), ', '.join(g.today_sold_stocks) or '无'))
    log.info("【候选池】%d只" % len(g.candidates))

    if prev_change is not None:
        log.info("【与上一交易日】%.2f → %.2f 变化=%+.2f(%+.2f%%)" %
                 (_as_float(g.prev_day_end_value), end_value, prev_change, prev_change_pct * 100))

    if position_count > 0:
        log.info("【持仓明细】")
        for sec, pos in context.portfolio.positions.items():
            amount = int(_as_float(getattr(pos, "amount", 0)))
            if amount <= 0:
                continue
            cost = _as_float(getattr(pos, "cost_basis", 0))
            last = _as_float(getattr(pos, "last_sale_price", 0))
            if last <= 0:
                last = cost
            pnl = (last - cost) / cost * 100 if cost > 0 else 0
            hold = int(g.hold_days.get(sec, 0))
            name = _get_stock_name(sec)
            log.info("  %s(%s) 持仓%d股 成本=%.3f 现价=%.3f 盈亏=%+.2f%% 持仓=%d天" %
                     (name or "-", sec, amount, cost, last, pnl, hold))
    log.info("=" * 60)

    g.prev_day_end_value = end_value
    g.prev_day_end_cash = end_cash
