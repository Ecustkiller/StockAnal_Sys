# -*- coding: utf-8 -*-
"""
PTrade策略: 二进三打板（2进3）
参考 龙牛居《2进3 打板》 + 打板四合一V2 实盘框架。

战法核心思路（原文摘录）：
------------------------------------------------------------------
  "2进3称之为柚子必争之位，大部分游资都会选择2进3板去接力"
  "2进3是非常好的上车位置，盈亏比划算，小龙吃5-30个点，大头50个点以上"

  第一步 选股：
    - 软件输入昨日连板+昨日涨停 → 几分钟选出昨天2板
    - 2连板公式: COUNT(C/REF(C,1)>1.097 AND C=H, 2)=2
    - 图形太差不要、位置太高不要、龙头高位不要
    - 以前妖过的2板不要、超跌反弹2板不要、历史股性太差不要
    - 只做低位
    - 结合近期题材，必须有一定的持续性

  第二步 买点（三选一）：
    1. 隔夜顶一字（大题材才用）
    2. 盘中打板：开盘快速拉板，要求竞价高开≥4%，首次封板1成
    3. 换手板长腿图形：小高开/小低开快速拉升，2%内顺势介入1-2成（不打板）
    打板时使用同花顺条件单

  第三步 仓位：≤2成内，新手0.5成/只起步
  第四步 止盈：T+1 10点前不封板 最少卖2/3
        止损：高开远离5日线直接出，低开贴近5日线等反抽再出
             触碰跌停直接割，亏5%内接受

  核心要点：2板图形要好 不要龙头反抽板 竞价开盘再4个点以上最好
           前一日已经大长腿的一般不要 开盘低开下杀绿盘的不要
------------------------------------------------------------------

本策略工程化实现：
1. 盘前选股（before_trading_start）
   - 全A剔除ST/停牌/退市 + 仅保留主板 + 过滤新股
   - 【2连板筛选】昨日涨停封板 且 前日涨停 且 大前日未涨停（纯2连板）
   - 【低位过滤】
       * 20日涨幅 < 50%（排除已经启动过的票）
       * 60日相对位置 (close-low)/(high-low) ≤ 0.7（非高位）
   - 【非大长腿】昨日最高-最低 的振幅 < 12%
   - 【流动性】昨日成交额 ≥ 2 亿

2. 盘中买入（9:25 竞价结束后 real_trade_buy_task）
   - 实盘：取集合竞价快照，要求 open_px / preclose ≥ 1.04（竞价高开≥4%）
   - 回测：取 9:31 首根分钟K线的open，同样要求 open/preclose ≥ 1.04
   - 同板块/同概念分仓优先级：按昨日成交额从大到小（龙一优先）
   - 限价=现价+1%（不超过涨停价）挂单
   - run_interval 3 秒检查，未成交撤单重挂（最多 10 次）

3. T+1 止盈止损（更激进）
   - 09:45：不涨停即卖 2/3（战法："10点前不封板 最少卖2/3"）
   - 10:00：未涨停即全卖
   - 盘中任意时刻：浮亏 ≤ -5% 立即止损（战法："亏5%内接受"，这里硬止损）
   - 触碰跌停立即割仓

4. 仓位
   - 单票 10% 仓位（战法："仓位控制2成内"+"0.5成一只"→折中10%）
   - 最多 2 票（同板块分仓"龙一+龙二"）
   - 总仓位上限 20%

5. V2 实盘护城河
   - 授权验证 permission_test
   - 买/卖订单 run_interval 3 秒补单
   - buy_records 持久化
   - 日终增强报告

运行建议：
  业务类型: 股票 | 频率: 分钟 | 实盘/回测均支持
"""

import datetime
import pandas as pd


# ================================== 策略函数 ===================================

def initialize(context):
    """策略初始化函数，只在策略启动时运行一次。"""
    # --- 授权验证 ---
    if is_trade():
        auth_result = permission_test()
        if not auth_result:
            log.error("=" * 50)
            log.error("【授权失败】二进三打板策略无权运行！")
            log.error("=" * 50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，二进三打板策略启动成功")

    # --- 仓位参数 ---
    g.stock_num = 2                     # 最多2票（龙一+龙二）
    g.max_single_stock_amount = 10000   # 单票10% = 10w总资产 × 10%
    g.max_available_cash = 100000.0     # 基准资金
    g.min_cash_ratio = 0.2              # 可用资金低于20%时停止买入

    # --- 选股参数（已放宽以保证候选池非空）---
    g.min_yesterday_turnover = 1e8      # 昨日成交额≥1亿 [2亿→1亿]
    g.max_20d_gain = 0.80               # 20日涨幅<80%（2板本身有涨幅，此处放宽）
    g.max_relative_pos = 0.90           # 60日相对位置≤0.90（仅过滤极端高位）
    g.max_long_leg_amplitude = 0.18     # 昨日振幅<18%（2板振幅常>12%）
    g.max_watchlist = 30

    # --- 买点参数（战法：竞价高开≥4%，放宽到2%以提高成交率）---
    g.auction_gap_min = 1.02            # 竞价高开下限 [4%→2%]
    g.auction_gap_max = 1.095           # 竞价高开上限（避开一字板 9.5%）

    # --- 止盈止损时点（战法：T+1 10点前不封板卖2/3）---
    g.sell_partial_time = "09:45"       # 分批卖点
    g.sell_final_time = "10:00"         # 尾盘总清仓点
    g.sell_partial_ratio = 0.67         # 早盘卖2/3
    g.stop_loss_ratio = -0.05           # 浮亏-5%止损（战法硬口径）
    g.max_hold_days = 2                 # 超过2天强制兜底
    g.max_daily_loss_pct = -0.03        # 日内回撤3%停止新开仓

    # --- 内部状态 ---
    g.candidates = []                    # 2连板候选池（带昨日成交额）
    g.today_list = []                    # 最终买入列表
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    g.partial_sold = set()
    g.hold_days = {}
    g.last_trade_date = ""
    g.day_start_value = None
    g.day_start_cash = None
    g.prev_day_end_value = None
    g.stop_buy_for_today = False
    g.buy_records = {}

    # --- 订单管理 ---
    g.buy_orders = {}
    g.sell_orders = {}
    g.stock_retry_count = {}
    g.max_retry_count = 10

    # --- 兜底标的 ---
    g.fallback_security = "000001.SZ"
    set_universe([g.fallback_security])

    # --- PTrade实盘/回测 ---
    if is_trade():
        set_parameters(holiday_not_do_before="1", server_restart_not_do_before="1")
    else:
        set_volume_ratio(0.9)
        set_limit_mode("LIMIT")

    # --- 定时任务 ---
    # 实盘：集合竞价结束后买入（9:25:10）
    run_daily(context, real_trade_buy_task, time='09:25:10')
    # 回测：开盘后第一个bar
    run_daily(context, backtest_buy_task, time='09:31')
    # T+1 止盈
    run_daily(context, sell_partial_task, time=g.sell_partial_time)
    run_daily(context, sell_final_task, time=g.sell_final_time)
    # 尾盘撤单
    run_daily(context, cancel_remaining_buy_orders, time='14:56')
    # 实盘订单补单（每3秒）
    run_interval(context, check_and_retry_orders, seconds=3)

    log.info("二进三打板策略初始化完成，模式=%s" % ("实盘" if is_trade() else "回测"))


def before_trading_start(context, data):
    """每日开盘前：2连板候选池筛选。"""
    _roll_trade_day(context)
    _sync_hold_days_with_positions(context)
    g.today_list = []
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    g.partial_sold = set()
    g.stop_buy_for_today = False

    if is_trade():
        g.buy_orders.clear()
        g.sell_orders.clear()
        g.stock_retry_count.clear()
        log.info("[Ptrade实盘] 订单追踪记录已清空")

    _save_day_start_snapshot(context)

    log.info("=" * 50)
    log.info(">>> [二进三打板] 开始执行盘前选股任务...")

    stocks = get_Ashares()
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        return

    stocks = filter_stock_by_status(stocks, ["ST", "HALT", "DELISTING", "DELISTING_SORTING"])
    stocks = [s for s in stocks if _is_main_board_stock(s)]
    stocks = _filter_new_stocks(stocks, min_days=50)
    if not stocks:
        g.candidates = []
        _refresh_universe(context)
        return

    # 2连板筛选
    g.candidates = _pick_erjin_san_candidates(stocks)
    _refresh_universe(context)

    log.info("[二进三打板] 2连板候选数量: %d" % len(g.candidates))
    if g.candidates:
        log.info("[二进三打板] 候选: %s" % g.candidates[:20])
    log.info(">>> 盘前选股任务执行完毕。")


def handle_data(context, data):
    """盘中逐bar：只负责盘中止损/跌停割仓，买入由定时任务驱动。"""
    _update_intraday_risk_flags(context)
    _manage_stop_loss(context, data)


def after_trading_end(context, data):
    """每日收盘后。"""
    _log_daily_summary(context)
    _update_buy_records(context)


# ================================== 盘前选股 ===================================

def _pick_erjin_san_candidates(stocks):
    """
    盘前筛选"昨日2连板"候选池（非3板+、非首板）：
    1. 昨日收盘=涨停价（第2板）
    2. 前日收盘=涨停价（第1板）
    3. 大前日收盘≠涨停价（保证只是2板，非3板+）
    4. 20日涨幅 < 80%（低位）    [已放宽: 50%→80%]
    5. 60日相对位置 ≤ 0.9（非极端高位）  [已放宽: 0.7→0.9]
    6. 昨日振幅 < 18%（排除大长腿）  [已放宽: 12%→18%]
    7. 昨日成交额 ≥ 1亿（流动性）    [已放宽: 2亿→1亿]
    返回: [(code, yesterday_money), ...] 已按成交额降序
    """
    fields = ["open", "close", "high", "low", "high_limit",
              "volume", "money", "is_open", "unlimited"]
    hist = get_history(70, "1d", fields, security_list=stocks, fq=None, include=False)
    if hist is None or len(hist) == 0 or "code" not in hist.columns:
        log.info("[诊断] 历史数据获取失败")
        return []

    # 分层计数，用于诊断
    cnt_total = 0
    cnt_y_limit = 0          # 昨日涨停
    cnt_pre1_limit = 0       # 前日也涨停(形成2板)
    cnt_not_3plus = 0        # 大前日未涨停(纯2板)
    cnt_money_ok = 0         # 成交额达标
    cnt_amp_ok = 0           # 非大长腿
    cnt_20d_ok = 0           # 20日涨幅低位
    cnt_60d_ok = 0           # 60日位置非高
    cnt_final = 0

    result = []
    for sec, df in hist.groupby("code"):
        if len(df) < 21:
            continue
        cnt_total += 1
        df = df.sort_index()
        y_bar = df.iloc[-1]       # 昨日（第2板）
        pre1 = df.iloc[-2]        # 前日（第1板）
        pre2 = df.iloc[-3]        # 大前日（不应涨停）

        # 昨日正常交易
        if int(_as_float(y_bar.get("is_open", 1))) != 1:
            continue
        if _as_float(y_bar.get("volume", 0)) <= 0:
            continue
        if int(_as_float(y_bar.get("unlimited", 0))) == 1:
            continue

        y_close = _as_float(y_bar.get("close", 0))
        y_high = _as_float(y_bar.get("high", 0))
        y_low = _as_float(y_bar.get("low", 0))
        y_high_limit = _as_float(y_bar.get("high_limit", 0))
        y_money = _as_float(y_bar.get("money", 0))

        # 昨日涨停封板（第2板）— 先判涨停，再判流动性
        if not _is_limit_up(y_close, y_high_limit):
            continue
        cnt_y_limit += 1

        # 前日涨停（第1板）
        pre1_close = _as_float(pre1.get("close", 0))
        pre1_high_limit = _as_float(pre1.get("high_limit", 0))
        if int(_as_float(pre1.get("unlimited", 0))) == 1:
            continue
        if not _is_limit_up(pre1_close, pre1_high_limit):
            continue
        cnt_pre1_limit += 1

        # 大前日不能涨停（保证是纯2板，而非3板+）
        pre2_close = _as_float(pre2.get("close", 0))
        pre2_high_limit = _as_float(pre2.get("high_limit", 0))
        if int(_as_float(pre2.get("unlimited", 0))) == 0:
            if _is_limit_up(pre2_close, pre2_high_limit):
                continue
        cnt_not_3plus += 1

        # 流动性过滤
        if y_money < g.min_yesterday_turnover:
            continue
        cnt_money_ok += 1

        # 大长腿过滤：昨日振幅 < 阈值
        if y_low > 0:
            amplitude = (y_high - y_low) / y_low
            if amplitude >= g.max_long_leg_amplitude:
                continue
        cnt_amp_ok += 1

        # 20日涨幅低位判定
        closes = df["close"].astype(float).values
        if len(closes) >= 20:
            c20 = float(closes[-20])
            if c20 > 0:
                gain20 = (y_close - c20) / c20
                if gain20 >= g.max_20d_gain:
                    continue
        cnt_20d_ok += 1

        # 60日相对位置
        if len(df) >= 60:
            df60 = df.iloc[-60:]
            h60 = _as_float(df60["high"].max())
            l60 = _as_float(df60["low"].min())
            if h60 > l60:
                rp = (y_close - l60) / (h60 - l60)
                if rp > g.max_relative_pos:
                    continue
        cnt_60d_ok += 1

        cnt_final += 1
        result.append((sec, y_money))

    # 分层诊断日志（每日盘前输出一次）
    log.info("[2进3选股漏斗] 样本=%d | 昨涨停=%d | 2板=%d | 纯2板(非3+)=%d | 成交额≥%.1f亿=%d | 振幅<%.0f%%=%d | 20d涨幅<%.0f%%=%d | 60d位置≤%.1f=%d | 最终=%d" % (
        cnt_total, cnt_y_limit, cnt_pre1_limit, cnt_not_3plus,
        g.min_yesterday_turnover / 1e8, cnt_money_ok,
        g.max_long_leg_amplitude * 100, cnt_amp_ok,
        g.max_20d_gain * 100, cnt_20d_ok,
        g.max_relative_pos, cnt_60d_ok,
        cnt_final
    ))

    # 按昨日成交额降序（"龙一优先"）
    result.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in result[:g.max_watchlist]]


def _filter_new_stocks(stock_list, min_days=50):
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


# ================================== 买入逻辑 ===================================

def real_trade_buy_task(context):
    """
    [实盘] 9:25:10 集合竞价结束后买入：
    - 取候选股快照，要求 open_px / preclose ≥ 4%
    - 限价=现价+1%（不超过涨停价），挂单买入
    """
    if not is_trade():
        return
    if g.stop_buy_for_today:
        log.info("[实盘]日内已触发风控，不执行买入")
        return
    if not g.candidates:
        log.info("[实盘]今日无2连板候选，不执行买入")
        return

    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘二进三]触发集合竞价买入 ---" %
             current_time.strftime('%Y-%m-%d %H:%M:%S'))

    snapshots = get_snapshot(g.candidates)
    if not snapshots:
        log.warning("[实盘]无法获取快照，取消本次买入")
        return

    # 筛选：竞价高开满足区间
    qualified = []
    reject_stats = {"no_snap": 0, "no_price": 0, "gap_low": 0, "gap_high": 0}
    for sec in g.candidates:
        snapshot = snapshots.get(sec)
        if not snapshot:
            reject_stats["no_snap"] += 1
            continue
        open_px = _as_float(snapshot.get('open_px', 0))
        if open_px <= 0:
            # 9:25 竞价结束后 last_px 就是竞价成交价
            open_px = _as_float(snapshot.get('last_px', 0))
        preclose = _as_float(snapshot.get('preclose_px', 0))
        if open_px <= 0 or preclose <= 0:
            reject_stats["no_price"] += 1
            continue
        gap = open_px / preclose
        if gap < g.auction_gap_min:
            reject_stats["gap_low"] += 1
            log.info("[实盘]跳过 %s 竞价高开=%.4f 低于下限%.2f" %
                     (sec, gap, g.auction_gap_min))
            continue
        if gap > g.auction_gap_max:
            reject_stats["gap_high"] += 1
            log.info("[实盘]跳过 %s 竞价高开=%.4f 高于上限%.2f" %
                     (sec, gap, g.auction_gap_max))
            continue
        qualified.append((sec, gap, open_px, snapshot))
        log.info("[实盘]2进3入选 %s 竞价高开=%.4f open=%.3f" % (sec, gap, open_px))

    log.info("[实盘竞价诊断] 候选=%d 无快照=%d 无价=%d 低开=%d 超开=%d 入选=%d" % (
        len(g.candidates), reject_stats["no_snap"], reject_stats["no_price"],
        reject_stats["gap_low"], reject_stats["gap_high"], len(qualified)
    ))

    if not qualified:
        log.info("[实盘]无股票满足竞价高开[%.0f%%,%.0f%%]" %
                 (g.auction_gap_min*100-100, g.auction_gap_max*100-100))
        return

    # 持仓与资金检查
    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if _as_float(getattr(pos, 'amount', 0)) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[实盘]持仓已满(%d只)，不再买入" % len(current_positions))
        return

    buy_list = [q for q in qualified if q[0] not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[实盘]候选均已持仓")
        return

    available_cash = _as_float(context.portfolio.cash)
    total_value = _as_float(context.portfolio.portfolio_value)
    if available_cash / total_value < g.min_cash_ratio:
        log.info("[实盘]可用资金<%.0f%%，不执行买入" % (g.min_cash_ratio * 100))
        return

    # 逐只下单
    g.today_list = [q[0] for q in buy_list]
    for sec, gap, open_px, snapshot in buy_list:
        # 单票 10% 仓位
        cash_per_stock = min(g.max_single_stock_amount, total_value * 0.10)
        cash_per_stock = min(cash_per_stock, available_cash)

        price_ref = _as_float(snapshot.get('last_px', open_px))
        if price_ref <= 0:
            price_ref = open_px
        up_px = _as_float(snapshot.get('up_px', price_ref * 1.1))
        limit_price = min(price_ref * 1.01, up_px)
        limit_price = round(limit_price, 2)

        if cash_per_stock / limit_price < 100:
            log.info("[实盘]资金不足100股 %s 资金=%.2f 价格=%.2f" %
                     (sec, cash_per_stock, limit_price))
            continue

        log.info("[实盘二进三]买入 %s 分配=%.2f 委托价=%.2f 竞价高开=%.4f" %
                 (sec, cash_per_stock, limit_price, gap))
        order_id = order_value(sec, cash_per_stock, limit_price=limit_price)
        if order_id:
            g.today_bought_stocks.add(sec)
            g.hold_days[sec] = 0
            g.buy_orders[order_id] = {
                'stock': sec,
                'cash': cash_per_stock,
                'limit_price': limit_price,
                'time': current_time.strftime('%H:%M:%S'),
                'retry_count': 0
            }


def backtest_buy_task(context):
    """
    [回测] 9:31 开盘后第一个bar：
    - 取 9:31 分钟K线 open 作为开盘价
    - 同样要求 open/preclose ≥ 4%
    """
    if is_trade():
        return
    if g.stop_buy_for_today:
        return
    if not g.candidates:
        log.info("[回测]今日无2连板候选")
        return

    current_time = context.blotter.current_dt
    log.info("--- %s: [回测二进三]触发买入 ---" %
             current_time.strftime('%Y-%m-%d %H:%M:%S'))

    # 取 1分钟 open 和昨日 close
    try:
        df_open = get_history(count=1, frequency='1m', field='open',
                              security_list=g.candidates, include=True)
    except Exception as e:
        log.warning("[回测]get_history(1m, open) 失败: %s" % e)
        return
    try:
        df_preclose = get_history(count=1, frequency='1d', field='close',
                                  security_list=g.candidates)
    except Exception as e:
        log.warning("[回测]get_history(1d, close) 失败: %s" % e)
        return

    if not isinstance(df_preclose, pd.DataFrame) or df_preclose.empty:
        log.warning("[回测]昨日收盘数据缺失")
        return

    qualified = []
    reject_stats = {"no_pc": 0, "no_open": 0, "gap_low": 0, "gap_high": 0}
    for sec in g.candidates:
        try:
            pc_data = df_preclose[df_preclose['code'] == sec] if 'code' in df_preclose.columns else df_preclose
            if pc_data is None or pc_data.empty:
                reject_stats["no_pc"] += 1
                continue
            preclose = _as_float(pc_data['close'].iloc[-1])
            if preclose <= 0:
                reject_stats["no_pc"] += 1
                continue

            open_px = 0.0
            if isinstance(df_open, pd.DataFrame) and not df_open.empty:
                op_data = df_open[df_open['code'] == sec] if 'code' in df_open.columns else df_open
                if op_data is not None and not op_data.empty:
                    open_px = _as_float(op_data['open'].iloc[-1])
            if open_px <= 0:
                reject_stats["no_open"] += 1
                continue

            gap = open_px / preclose
            if gap < g.auction_gap_min:
                reject_stats["gap_low"] += 1
                log.info("[回测]跳过 %s 高开=%.4f 低于%.2f" % (sec, gap, g.auction_gap_min))
                continue
            if gap > g.auction_gap_max:
                reject_stats["gap_high"] += 1
                log.info("[回测]跳过 %s 高开=%.4f 高于%.2f" % (sec, gap, g.auction_gap_max))
                continue
            qualified.append((sec, gap, open_px))
            log.info("[回测]2进3入选 %s 高开=%.4f open=%.3f" % (sec, gap, open_px))
        except Exception as e:
            log.info("[回测]筛选异常 %s: %s" % (sec, e))

    log.info("[回测竞价诊断] 候选=%d 无昨收=%d 无开盘=%d 低开=%d 超开=%d 入选=%d" % (
        len(g.candidates), reject_stats["no_pc"], reject_stats["no_open"],
        reject_stats["gap_low"], reject_stats["gap_high"], len(qualified)
    ))

    if not qualified:
        log.info("[回测]无股票满足高开[%.0f%%,%.0f%%]" %
                 (g.auction_gap_min*100-100, g.auction_gap_max*100-100))
        return

    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if _as_float(getattr(pos, 'amount', 0)) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[回测]持仓已满")
        return

    buy_list = [q for q in qualified if q[0] not in current_positions][:num_to_buy]
    if not buy_list:
        return

    available_cash = _as_float(context.portfolio.cash)
    total_value = _as_float(context.portfolio.portfolio_value)
    if available_cash / total_value < g.min_cash_ratio:
        log.info("[回测]可用资金不足%.0f%%" % (g.min_cash_ratio * 100))
        return

    g.today_list = [q[0] for q in buy_list]
    for sec, gap, open_px in buy_list:
        cash_per_stock = min(g.max_single_stock_amount, total_value * 0.10)
        cash_per_stock = min(cash_per_stock, available_cash)
        if cash_per_stock / open_px < 100:
            continue

        log.info("[回测二进三]买入 %s 分配=%.2f 竞价高开=%.4f" %
                 (sec, cash_per_stock, gap))
        order_id = order_value(sec, cash_per_stock)
        if order_id:
            g.today_bought_stocks.add(sec)
            g.hold_days[sec] = 0


# ================================== 盘中止损 ===================================

def _manage_stop_loss(context, data):
    """
    盘中扫描止损：
    - 浮亏 ≤ -5% 立即全卖
    - 触碰跌停直接割（跌停价±0.01）
    - 超期>2天强制兜底
    """
    positions = context.portfolio.positions
    for sec, pos in positions.items():
        amount = int(_as_float(getattr(pos, "amount", 0)))
        if amount <= 0:
            continue
        if sec not in data:
            continue
        if sec in g.today_bought_stocks:
            continue  # T+1
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

        # 获取跌停价（实盘用快照，回测用日线low_limit）
        down_hit = False
        if is_trade():
            snap = _get_snapshot_safe(sec)
            if snap:
                down_px = _as_float(snap.get("down_px", 0))
                if down_px > 0 and abs(last_price - down_px) < 0.01:
                    down_hit = True

        should_stop = down_hit or (pnl_ratio <= g.stop_loss_ratio) or (hold_days >= g.max_hold_days)
        if not should_stop:
            continue

        enable_amount = int(_as_float(getattr(pos, "enable_amount", 0)))
        if enable_amount <= 0:
            p = get_position(sec)
            enable_amount = int(_as_float(getattr(p, "enable_amount", 0)))
        if enable_amount <= 0:
            continue

        if down_hit:
            reason = "触碰跌停"
        elif pnl_ratio <= g.stop_loss_ratio:
            reason = "止损-5%%"
        else:
            reason = "超期%d天" % hold_days

        order_id = order(sec, -enable_amount)
        if order_id:
            g.today_sold_stocks.add(sec)
            log.info("[二进三盘中止损]卖出 %s 委托号=%s 原因=%s 持仓%d天 收益=%.2f%% 成本=%.3f 现价=%.3f" %
                     (sec, order_id, reason, hold_days, pnl_ratio * 100, cost, last_price))
            if is_trade():
                g.sell_orders[order_id] = {
                    'stock': sec, 'reason': reason,
                    'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                    'retry_count': 0
                }


# ================================== T+1 止盈 ===================================

def sell_partial_task(context):
    """09:45 分批卖：未涨停卖 2/3（战法："T+1 10点前不封板 最少卖2/3"）"""
    _sell_positions_if_not_limit_up(context, ratio=g.sell_partial_ratio,
                                     tag="早盘分批2/3", add_to_partial=True)


def sell_final_task(context):
    """10:00 全清：未涨停全卖"""
    _sell_positions_if_not_limit_up(context, ratio=1.0, tag="10点全清",
                                     add_to_partial=False)


def _sell_positions_if_not_limit_up(context, ratio, tag, add_to_partial):
    """对非当日买入、非涨停的持仓，按比例卖。"""
    current_time = context.blotter.current_dt
    log.info("--- %s: [二进三]触发%s任务 ---" %
             (current_time.strftime('%Y-%m-%d %H:%M:%S'), tag))

    positions = context.portfolio.positions
    if not positions:
        log.info("当前无持仓")
        return

    sellable = [code for code in positions
                if code not in g.today_bought_stocks
                and code not in g.today_sold_stocks
                and _as_float(getattr(positions[code], 'amount', 0)) > 0]
    if not sellable:
        log.info("无可卖持仓(T+1限制)")
        return

    for code in sellable:
        pos = positions[code]
        if add_to_partial and code in g.partial_sold:
            continue

        last_px, high_limit = _get_current_price_and_limit(code, is_trade_mode=is_trade())
        if last_px <= 0:
            continue
        if high_limit > 0 and last_px >= high_limit * 0.999:
            log.info("[%s]%s 涨停中(%.3f)，不卖" % (tag, code, last_px))
            continue

        cost = _as_float(getattr(pos, 'cost_basis', 0))
        ret = (last_px / cost - 1) * 100 if cost > 0 else 0

        enable_amount = int(_as_float(getattr(pos, 'enable_amount', 0)))
        if enable_amount <= 0:
            p = get_position(code)
            enable_amount = int(_as_float(getattr(p, 'enable_amount', 0)))
        if enable_amount <= 0:
            continue

        sell_amount = int(enable_amount * ratio / 100) * 100
        if sell_amount <= 0:
            sell_amount = enable_amount
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
    """获取当前价和涨停价。"""
    if is_trade_mode:
        snap = _get_snapshot_safe(code)
        if snap:
            return _as_float(snap.get('last_px', 0)), _as_float(snap.get('up_px', 0))
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


# ================================== 订单管理（V2） ===================================

def check_and_retry_orders(context):
    """统一的订单检查与补单，run_interval 每 3 秒调用。"""
    if not is_trade():
        return
    if g.buy_orders:
        _check_and_retry_buy_orders(context)
    if g.sell_orders:
        _check_and_retry_sell_orders(context)


def _check_and_retry_buy_orders(context):
    """买入订单补单。"""
    if not g.buy_orders:
        return

    current_time = context.blotter.current_dt

    # 防重复：查成交记录
    stock_filled_amounts = {}
    try:
        today_trades = get_trades()
        for trade in today_trades:
            if trade.is_buy:
                stock_code = trade.security
                stock_filled_amounts[stock_code] = stock_filled_amounts.get(stock_code, 0) + trade.amount
    except Exception as e:
        log.warning("[买入补单]获取成交记录失败: %s" % e)

    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.buy_orders.items()):
        stock = order_info['stock']
        cash_allocated = order_info['cash']
        retry_count = order_info.get('retry_count', 0)

        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count or retry_count >= g.max_retry_count:
            orders_to_remove.append(order_id)
            continue

        if stock in stock_filled_amounts and stock_filled_amounts[stock] > 0:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if not order_list or len(order_list) == 0:
                continue

            order_status = order_list[0]
            status = order_status.status

            if status == '8':
                orders_to_remove.append(order_id)

            elif status in ['0', '1', '2', '+', '-', 'C', 'V']:
                snap = get_snapshot(stock)
                if snap:
                    snap_data = snap.get(stock, snap) if isinstance(snap, dict) else snap
                    cur_px = _as_float(snap_data.get('last_px', 0)) if isinstance(snap_data, dict) else 0
                    up_px_val = _as_float(snap_data.get('up_px', 0)) if isinstance(snap_data, dict) else 0
                    # 涨停不撤单（让它在涨停板排队）
                    if up_px_val > 0 and abs(cur_px - up_px_val) < 0.01:
                        continue
                is_cancelling = order_info.get('is_cancelling', False)
                if not is_cancelling:
                    try:
                        cancel_order(order_id)
                        g.buy_orders[order_id]['is_cancelling'] = True
                    except Exception:
                        pass

            elif status == '6':  # 已撤
                orders_to_remove.append(order_id)
                actual_cash = min(g.max_single_stock_amount, cash_allocated)
                orders_to_retry.append({
                    'stock': stock, 'cash': actual_cash,
                    'retry_count': retry_count + 1
                })
            elif status == '9':  # 废单
                orders_to_remove.append(order_id)
                actual_cash = min(g.max_single_stock_amount, cash_allocated)
                orders_to_retry.append({
                    'stock': stock, 'cash': actual_cash,
                    'retry_count': retry_count + 1
                })
            elif status == '7':  # 部成
                orders_to_remove.append(order_id)

        except Exception as e:
            log.error("[买入补单]异常 %s: %s" % (order_id, e))

    for oid in orders_to_remove:
        if oid in g.buy_orders:
            del g.buy_orders[oid]

    if orders_to_retry:
        filtered = [item for item in orders_to_retry
                    if not (item['stock'] in stock_filled_amounts and
                            stock_filled_amounts[item['stock']] > 0)]
        if not filtered:
            return

        stocks_to_snap = [item['stock'] for item in filtered]
        snapshots = get_snapshot(stocks_to_snap)
        if not snapshots:
            return

        for item in filtered:
            stock = item['stock']
            cash_alloc = item['cash']
            r_count = item['retry_count']

            snapshot = snapshots.get(stock)
            if not snapshot:
                continue
            current_price = _as_float(snapshot.get('last_px', 0))
            if current_price <= 0:
                continue

            limit_price = current_price * 1.01
            up_px = _as_float(snapshot.get('up_px', current_price * 1.1))
            limit_price = min(limit_price, up_px)
            limit_price = round(limit_price, 2)

            try:
                new_oid = order_value(stock, cash_alloc, limit_price=limit_price)
                if new_oid:
                    g.buy_orders[new_oid] = {
                        'stock': stock, 'cash': cash_alloc,
                        'limit_price': limit_price,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': r_count
                    }
                    g.stock_retry_count[stock] = r_count
                    log.info("[买入补单]补单成功 %s (第%d次)" % (stock, r_count))
            except Exception as e:
                log.error("[买入补单]异常 %s: %s" % (stock, e))


def _check_and_retry_sell_orders(context):
    """卖出订单补单。"""
    if not g.sell_orders:
        return

    current_time = context.blotter.current_dt

    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '')
        retry_count = order_info.get('retry_count', 0)

        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count or retry_count >= g.max_retry_count:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if not order_list or len(order_list) == 0:
                continue

            order_status = order_list[0]
            status = order_status.status

            if status == '8':
                orders_to_remove.append(order_id)

            elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:
                snap = get_snapshot(stock)
                if snap:
                    snap_data = snap.get(stock, snap) if isinstance(snap, dict) else snap
                    cur_px = _as_float(snap_data.get('last_px', 0)) if isinstance(snap_data, dict) else 0
                    down_px = _as_float(snap_data.get('down_px', 0)) if isinstance(snap_data, dict) else 0
                    if down_px > 0 and abs(cur_px - down_px) < 0.01:
                        continue
                try:
                    cancel_order(order_id)
                    orders_to_remove.append(order_id)
                    orders_to_retry.append({
                        'stock': stock, 'reason': reason,
                        'retry_count': retry_count + 1
                    })
                except Exception:
                    pass

            elif status in ['6', '9']:
                orders_to_remove.append(order_id)
                orders_to_retry.append({
                    'stock': stock, 'reason': reason,
                    'retry_count': retry_count + 1
                })

        except Exception as e:
            log.error("[卖出补单]异常 %s: %s" % (order_id, e))

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
            if not position or _as_float(getattr(position, 'amount', 0)) <= 0:
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
            limit_price = round(limit_price, 2)

            try:
                new_oid = order_target(stock, 0, limit_price=limit_price)
                if new_oid:
                    g.sell_orders[new_oid] = {
                        'stock': stock, 'reason': reason,
                        'time': current_time.strftime('%H:%M:%S'),
                        'retry_count': r_count
                    }
                    g.stock_retry_count[stock] = r_count
                    log.info("[卖出补单]补单成功 %s (第%d次)" % (stock, r_count))
            except Exception as e:
                log.error("[卖出补单]异常 %s: %s" % (stock, e))


def cancel_remaining_buy_orders(context):
    """14:56 撤销所有剩余买单。"""
    orders = get_open_orders()
    if not orders:
        return
    iterator = orders.values() if isinstance(orders, dict) else orders
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
            if oid in g.buy_orders:
                del g.buy_orders[oid]
            log.info("尾盘撤销买单 %s 委托号=%s" % (sid, oid))
        except Exception as e:
            log.info("尾盘撤单失败 %s: %s" % (oid, e))
    if cancel_count > 0:
        log.info("尾盘撤销在途买单 共%d个" % cancel_count)


# ================================== 风控 ===================================

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
        log.info("【风控】当日回撤=%.2f%%≤%.2f%%，停止买入" %
                 (day_ret * 100, g.max_daily_loss_pct * 100))


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
    log.info("【日初资金】总资产=%.2f 可用=%.2f" % (g.day_start_value, g.day_start_cash))


def _is_main_board_stock(security):
    try:
        code, market = security.split(".")
    except Exception:
        return False
    return code[:2] in ('60', '00')


def _has_open_order(security, side=None):
    orders = get_open_orders()
    if not orders:
        return False
    iterator = orders.values() if isinstance(orders, dict) else orders
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


def _is_limit_up(close_px, high_limit_px):
    if close_px <= 0 or high_limit_px <= 0:
        return False
    return abs(close_px - high_limit_px) / high_limit_px <= 0.001


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
    for code in g.today_bought_stocks:
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
    log.info("【二进三打板 日终报告】基准=%.0f元" % base_capital)
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
    log.info("+" + "-" * 58 + "+")
    log.info("| %-28s | %+27.2f |" % ("基准收益(%.0f)" % base_capital, base_profit))
    log.info("| %-28s | %+27.2f%% |" % ("基准收益率", base_profit_pct))
    log.info("+" + "-" * 58 + "+")
    log.info("【当日买入】%d只: %s" % (len(g.today_bought_stocks),
                                      ', '.join(g.today_bought_stocks) or '无'))
    log.info("【当日卖出】%d只: %s" % (len(g.today_sold_stocks),
                                      ', '.join(g.today_sold_stocks) or '无'))
    log.info("【2连板候选池】%d只" % len(g.candidates))
    log.info("【实际入选】%d只: %s" % (len(g.today_list), ', '.join(g.today_list) or '无'))

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
            log.info("  %s(%s) %d股 成本=%.3f 现价=%.3f 盈亏=%+.2f%% 持仓=%d天" %
                     (name or "-", sec, amount, cost, last, pnl, hold))
    log.info("=" * 60)

    g.prev_day_end_value = end_value
