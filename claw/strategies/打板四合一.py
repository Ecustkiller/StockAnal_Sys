# -*- coding: utf-8 -*-
"""
PTrade策略: 打板四合一（从聚宽版本转换）
参考弱转强V2框架，保证实盘可用。

策略逻辑（四种子策略）：
1. 一进二高开(gap_up)：昨日首板涨停(前日/大前日未涨停) + 高开买入
2. 首板低开(gap_down)：昨日涨停(前日未涨停) + 低开3-4%买入
3. 弱转强(reversal)：昨日炸板(触板未封) + 竞价高开买入
4. 反向首板低开(fxsbdk)：昨日跌停(非连板) + 高开4-10%买入

运行建议:
1. 业务类型: 股票
2. 频率: 分钟
3. 实盘/回测均支持
"""

import datetime
import numpy as np
import pandas as pd


# ================================== 策略函数 ===================================

def initialize(context):
    """策略初始化函数，只在策略启动时运行一次。"""
    # --- 授权验证（V2框架） ---
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

    # --- 策略参数 ---
    g.stock_num = 4                     # 最大持仓数量
    g.max_single_stock_amount = 100000  # 单股最大买入金额
    g.min_money_threshold = 1e8         # 最低成交额过滤(1亿)

    # 开盘价筛选阈值（各子策略独立阈值在选股中硬编码）
    g.open_down_threshold = 0.97
    g.open_up_threshold = 1.03

    # 一进二高开参数
    g.gk_min_money = 5.5e8             # 高开最低成交额(5.5亿)
    g.gk_max_money = 20e8              # 高开最高成交额(20亿)
    g.gk_min_avg_price_ratio = 0.07    # 均价涨幅最低阈值
    g.gk_min_market_cap = 20           # 最低总市值(亿)
    g.gk_max_circ_cap = 520            # 最高流通市值(亿)
    g.gk_min_auction_vol_ratio = 0.04  # 竞价成交量/昨日成交量 最低阈值
    g.gk_open_ratio_min = 1.0          # 开盘价/昨日收盘 下限
    g.gk_open_ratio_max = 1.06         # 开盘价/昨日收盘 上限

    # 首板低开参数
    g.dk_low_open_min = 0.96           # 低开下限
    g.dk_low_open_max = 0.97           # 低开上限
    g.dk_relative_pos_max = 0.6        # 60日相对位置上限
    g.dk_min_money = 1e8               # 最低成交额(1亿)

    # 弱转强参数
    g.rzq_max_3day_increase = 0.15     # 前3天最大涨幅
    g.rzq_min_open_close_ratio = -0.05 # 前日开收盘比最小值
    g.rzq_min_avg_price_ratio = -0.04  # 均价涨幅最低
    g.rzq_min_money = 3e8              # 最低成交额(3亿)
    g.rzq_max_money = 19e8             # 最高成交额(19亿)
    g.rzq_min_market_cap = 70          # 最低总市值(亿)
    g.rzq_max_circ_cap = 520           # 最高流通市值(亿)
    g.rzq_min_auction_vol_ratio = 0.04 # 竞价成交量占比
    g.rzq_open_ratio_min = 0.98        # 开盘价/昨收 下限
    g.rzq_open_ratio_max = 1.09        # 开盘价/昨收 上限

    # 反向首板低开参数
    g.fxsbdk_relative_pos_max = 0.5    # 60日相对位置上限
    g.fxsbdk_open_ratio_min = 1.04     # 高开下限
    g.fxsbdk_open_ratio_max = 1.10     # 高开上限

    # --- 内部状态变量 ---
    g.gap_up = []       # 一进二高开候选池
    g.gap_down = []     # 首板低开候选池
    g.reversal = []     # 弱转强候选池
    g.fxsbdk = []       # 反向首板低开候选池
    g.today_list = []   # 当日综合买入池

    g.today_bought_stocks = set()
    g.today_sold_stocks = set()
    g.buy_records = {}
    g.last_total_value = 0.0

    # --- 订单管理（V2框架） ---
    g.buy_orders = {}
    g.sell_orders = {}
    g.pending_buy_stocks = {}
    g.stock_retry_count = {}
    g.max_retry_count = 10
    g.order_check_interval = 3

    # --- 兜底标的 ---
    g.fallback_security = "000001.SZ"
    set_universe([g.fallback_security])

    # --- PTrade实盘/回测设置 ---
    if is_trade():
        set_parameters(holiday_not_do_before="1", server_restart_not_do_before="1")
    else:
        set_volume_ratio(0.9)
        set_limit_mode("LIMIT")

    # --- 注册定时任务 ---
    # 实盘：集合竞价结束后买入
    run_daily(context, real_trade_buy_task, time='09:25:10')
    # 实盘：订单检查与补单（每3秒）
    run_interval(context, check_and_retry_orders, seconds=3)
    # 回测：开盘后第一个bar模拟买入
    run_daily(context, backtest_buy_task, time='09:31')
    # 卖出：11:25 和 14:50 两个时点
    run_daily(context, sell_task, time='11:25')
    run_daily(context, sell_task, time='14:50')

    log.info("策略初始化完成，模式=%s" % ("实盘" if is_trade() else "回测"))


def before_trading_start(context, data):
    """每日开盘前运行函数：四合一选股。"""
    # 1. 重置每日状态
    g.gap_up = []
    g.gap_down = []
    g.reversal = []
    g.fxsbdk = []
    g.today_list = []
    g.today_bought_stocks = set()
    g.today_sold_stocks = set()

    if is_trade():
        g.buy_orders.clear()
        g.sell_orders.clear()
        g.pending_buy_stocks.clear()
        g.stock_retry_count.clear()
        log.info("[Ptrade实盘] 订单追踪记录已清空")

    log.info("=" * 50)
    log.info(">>> 开始执行盘前选股任务（打板四合一）...")

    # 2. 获取基础股票池
    initial_list = prepare_stock_list(context)
    log.info("基础股票池数量: %d" % len(initial_list))
    if not initial_list:
        _refresh_universe(context)
        return

    # 3. 获取最近3个交易日
    trade_days = get_trade_days(count=4)
    if len(trade_days) < 4:
        log.warning("交易日数据不足")
        _refresh_universe(context)
        return
    # trade_days[-1]是当天(或最近交易日)，[-2]昨日，[-3]前日，[-4]大前日
    date_today = trade_days[-1]
    date_y = trade_days[-2]     # 昨日
    date_1 = trade_days[-3]     # 前日
    date_2 = trade_days[-4]     # 大前日

    date_y_str = date_y.strftime('%Y-%m-%d') if hasattr(date_y, 'strftime') else str(date_y)
    date_1_str = date_1.strftime('%Y-%m-%d') if hasattr(date_1, 'strftime') else str(date_1)
    date_2_str = date_2.strftime('%Y-%m-%d') if hasattr(date_2, 'strftime') else str(date_2)

    # 4. 四合一选股
    # 4.1 昨日涨停（封住涨停）
    hl0_list = get_limit_up_stocks(initial_list, date_y_str)
    log.info("昨日涨停封板数量: %d" % len(hl0_list))

    # 4.2 前日曾涨停
    hl1_list = get_ever_limit_up_stocks(initial_list, date_1_str)
    # 4.3 大前日曾涨停
    hl2_list = get_ever_limit_up_stocks(initial_list, date_2_str)

    # --- 一进二高开候选：昨日涨停 且 前日+大前日都没涨停 ---
    remove_set = set(hl1_list + hl2_list)
    g.gap_up = [s for s in hl0_list if s not in remove_set]
    log.info("一进二高开候选(首板涨停): %d" % len(g.gap_up))

    # --- 首板低开候选：昨日涨停 且 前日没涨停 ---
    g.gap_down = [s for s in hl0_list if s not in hl1_list]
    log.info("首板低开候选: %d" % len(g.gap_down))

    # --- 弱转强候选：昨日炸板（触板未封） ---
    h1_list = get_bomb_board_stocks(initial_list, date_y_str)
    # 过滤掉前日涨停的
    hl1_remove = get_limit_up_stocks(initial_list, date_1_str)
    g.reversal = [s for s in h1_list if s not in hl1_remove]
    log.info("弱转强候选(昨日炸板): %d" % len(g.reversal))

    # --- 反向首板低开候选：昨日跌停 ---
    g.fxsbdk = get_limit_down_stocks(initial_list, date_y_str)
    log.info("反向首板低开候选(昨日跌停): %d" % len(g.fxsbdk))

    # 5. 更新universe
    all_candidates = list(set(g.gap_up + g.gap_down + g.reversal + g.fxsbdk))
    log.info("四合一总候选数量: %d" % len(all_candidates))
    _refresh_universe(context, all_candidates)
    log.info(">>> 盘前选股任务执行完毕。")


def handle_data(context, data):
    """策略主逻辑函数，所有逻辑由定时任务驱动。"""
    pass


def after_trading_end(context, data):
    """每日收盘后运行函数。"""
    daily_trading_report(context)
    update_buy_records(context)


# ================================== 盘前选股辅助 ===================================

def prepare_stock_list(context):
    """每日初始股票池：全A股，去创业板/科创板/北交/ST/停牌/退市"""
    all_stocks = get_Ashares()
    if not all_stocks:
        return []

    # 批量状态过滤
    stocks = filter_stock_by_status(all_stocks, ["ST", "HALT", "DELISTING", "DELISTING_SORTING"])
    if not stocks:
        return []

    # 仅保留主板+创业板（60/00/30开头），排除科创/北交
    final = [s for s in stocks if _is_valid_board(s)]

    # 过滤新股（上市不足50个交易日）
    final = filter_new_stocks(final, min_days=50)

    return final


def get_limit_up_stocks(stock_list, date_str):
    """获取某日涨停且封住的股票（收盘价=涨停价）"""
    if not stock_list:
        return []
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['close', 'high_limit'], count=1)
        if df is None or df.empty:
            return []
        df = df.dropna()
        # 收盘价等于涨停价(容差0.01)
        result = df[abs(df['close'] - df['high_limit']) < 0.01]
        if 'code' in result.columns:
            return list(result['code'].unique())
        return list(result.index) if hasattr(result.index, 'tolist') else []
    except Exception as e:
        log.error("get_limit_up_stocks error: %s" % e)
        return []


def get_ever_limit_up_stocks(stock_list, date_str):
    """获取某日曾涨停的股票（最高价=涨停价，不管是否封住）"""
    if not stock_list:
        return []
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['high', 'high_limit'], count=1)
        if df is None or df.empty:
            return []
        df = df.dropna()
        result = df[abs(df['high'] - df['high_limit']) < 0.01]
        if 'code' in result.columns:
            return list(result['code'].unique())
        return list(result.index) if hasattr(result.index, 'tolist') else []
    except Exception as e:
        log.error("get_ever_limit_up_stocks error: %s" % e)
        return []


def get_bomb_board_stocks(stock_list, date_str):
    """获取某日炸板的股票（最高价=涨停价 但 收盘价<涨停价）"""
    if not stock_list:
        return []
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['close', 'high', 'high_limit'], count=1)
        if df is None or df.empty:
            return []
        df = df.dropna()
        cd1 = abs(df['high'] - df['high_limit']) < 0.01  # 最高触板
        cd2 = df['close'] < df['high_limit'] - 0.01      # 收盘未封
        result = df[cd1 & cd2]
        if 'code' in result.columns:
            return list(result['code'].unique())
        return list(result.index) if hasattr(result.index, 'tolist') else []
    except Exception as e:
        log.error("get_bomb_board_stocks error: %s" % e)
        return []


def get_limit_down_stocks(stock_list, date_str):
    """获取某日跌停的股票（收盘价=跌停价）"""
    if not stock_list:
        return []
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['close', 'low_limit'], count=1)
        if df is None or df.empty:
            return []
        df = df.dropna()
        result = df[abs(df['close'] - df['low_limit']) < 0.01]
        if 'code' in result.columns:
            return list(result['code'].unique())
        return list(result.index) if hasattr(result.index, 'tolist') else []
    except Exception as e:
        log.error("get_limit_down_stocks error: %s" % e)
        return []


def filter_new_stocks(stock_list, min_days=50):
    """过滤上市不足min_days天的新股"""
    if not stock_list:
        return []
    try:
        result = []
        info_dict = get_stock_info(stock_list)
        if not info_dict:
            return stock_list
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
                today = datetime.datetime.now().date()
                if (today - ld).days >= min_days:
                    result.append(code)
            except:
                result.append(code)
        return result
    except Exception as e:
        log.error("filter_new_stocks error: %s" % e)
        return stock_list


def rise_low_volume(stock, n_days=106):
    """
    左压结构判断：如果近期高点左侧有更高的高点，且最近成交量不够大，
    说明上方有套牢盘压力，返回True表示存在左压。
    """
    try:
        hist = get_history(n_days, '1d', ['high', 'volume'], security_list=stock)
        if hist is None or len(hist) < n_days:
            return False
        # 只取该股票的数据
        if 'code' in hist.columns:
            hist = hist[hist['code'] == stock]
        if len(hist) < n_days:
            return False

        high_prices = hist['high'].values[:102]
        volumes = hist['volume'].values

        prev_high = high_prices[-1]
        # 从倒数第3天开始向前找，找到第一个比prev_high高的点
        zyts_0 = 100
        for i in range(len(high_prices) - 3, -1, -1):
            if high_prices[i] >= prev_high:
                zyts_0 = len(high_prices) - 1 - i
                break
        zyts = zyts_0 + 5

        if zyts_0 < 20:
            threshold = 0.9
        elif zyts_0 < 50:
            threshold = 0.88
        else:
            threshold = 0.85

        recent_vol = volumes[-1]
        if zyts < len(volumes):
            max_vol_range = max(volumes[-zyts:-1]) if zyts > 1 else volumes[-2]
        else:
            max_vol_range = max(volumes[:-1])

        if recent_vol <= max_vol_range * threshold:
            return True
        return False
    except Exception as e:
        log.error("rise_low_volume error for %s: %s" % (stock, e))
        return False


def get_relative_position(stock_list, date_str, watch_days=60):
    """
    计算股票在watch_days内的相对位置：(close-low)/(high-low)
    返回dict: {code: rp}
    """
    if not stock_list:
        return {}
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['high', 'low', 'close'], count=watch_days)
        if df is None or df.empty:
            return {}

        result = {}
        if 'code' in df.columns:
            for code in stock_list:
                sub = df[df['code'] == code]
                if sub.empty:
                    continue
                close_val = sub['close'].iloc[-1]
                high_val = sub['high'].max()
                low_val = sub['low'].min()
                if high_val > low_val:
                    result[code] = (close_val - low_val) / (high_val - low_val)
                else:
                    result[code] = 0.5
        return result
    except Exception as e:
        log.error("get_relative_position error: %s" % e)
        return {}


def get_continue_limit_down_count(stock_list, date_str, watch_days=10):
    """
    计算连续跌停天数，返回连板股票列表（连跌停>=2天的股票）
    用于反向首板低开中过滤连板跌停。
    """
    if not stock_list:
        return []
    try:
        df = get_price(stock_list, end_date=date_str, frequency='1d',
                       fields=['close', 'low_limit'], count=watch_days)
        if df is None or df.empty:
            return []

        lb_stocks = []
        if 'code' in df.columns:
            for code in stock_list:
                sub = df[df['code'] == code].dropna()
                if sub.empty:
                    continue
                ld_count = sum(abs(sub['close'] - sub['low_limit']) < 0.01)
                if ld_count >= 2:
                    lb_stocks.append(code)
        return lb_stocks
    except Exception as e:
        log.error("get_continue_limit_down_count error: %s" % e)
        return []


def _is_valid_board(security):
    """检查是否是有效板块的股票（60/00/30开头，排除科创688和北交8/4）"""
    try:
        code = security.split(".")[0]
        return code[:2] in ('60', '00', '30')
    except:
        return False


def _refresh_universe(context, candidates=None):
    """股票池动态维护"""
    universe = []
    if candidates:
        universe.extend(candidates)
    held = [code for code, pos in context.portfolio.positions.items()
            if _as_float(getattr(pos, 'amount', 0)) > 0]
    universe.extend(held)
    universe = list(set(universe))
    if not universe:
        universe = [g.fallback_security]
    set_universe(universe)
    log.info("已更新universe，共订阅 %d 只股票的行情。" % len(universe))


# ================================== 买入逻辑 ===================================

def real_trade_buy_task(context):
    """
    [实盘专用] 集合竞价结束后执行四合一买入。
    使用snapshot获取集合竞价价格，对四个子策略分别筛选后合并买入。
    """
    if not is_trade():
        return

    log.info("--- %s: [实盘]触发集合竞价买入任务 ---" % context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))

    all_candidates = list(set(g.gap_up + g.gap_down + g.reversal + g.fxsbdk))
    if not all_candidates:
        log.info("[实盘]今日无候选股票，不执行买入。")
        return

    # 获取所有候选的快照
    snapshots = get_snapshot(all_candidates)
    if not snapshots:
        log.warning("[实盘]无法获取快照数据，取消本次买入。")
        return

    # 获取昨日数据（用于各子策略筛选）
    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')

    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []
    fxsbdk_stocks = []

    # === 子策略1：一进二高开 ===
    gk_stocks = _filter_gap_up_realtime(context, g.gap_up, snapshots, yesterday_str)
    log.info("[实盘]一进二高开入选: %d只 %s" % (len(gk_stocks), gk_stocks))

    # === 子策略2：首板低开 ===
    dk_stocks = _filter_gap_down_realtime(context, g.gap_down, snapshots, yesterday_str)
    log.info("[实盘]首板低开入选: %d只 %s" % (len(dk_stocks), dk_stocks))

    # === 子策略3：弱转强 ===
    rzq_stocks = _filter_reversal_realtime(context, g.reversal, snapshots, yesterday_str)
    log.info("[实盘]弱转强入选: %d只 %s" % (len(rzq_stocks), rzq_stocks))

    # === 子策略4：反向首板低开 ===
    fxsbdk_stocks = _filter_fxsbdk_realtime(context, g.fxsbdk, snapshots, yesterday_str)
    log.info("[实盘]反向首板低开入选: %d只 %s" % (len(fxsbdk_stocks), fxsbdk_stocks))

    # 合并
    qualified_stocks = list(set(gk_stocks + dk_stocks + rzq_stocks + fxsbdk_stocks))
    g.today_list = qualified_stocks
    log.info("[实盘]四合一最终入选: %d只 %s" % (len(qualified_stocks), qualified_stocks))

    if not qualified_stocks:
        log.info("[实盘]无满足条件的股票。")
        return

    # 检查持仓和资金
    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[实盘]持仓已满，无需买入。")
        return

    buy_list = [s for s in qualified_stocks if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[实盘]候选股票均已持仓。")
        return

    # 资金检查
    available_cash = _as_float(context.portfolio.cash)
    if available_cash / _as_float(context.portfolio.portfolio_value) < 0.3:
        log.info("[实盘]可用资金不足30%%，不执行买入。")
        return

    # 执行买入
    cash_per_stock = available_cash / len(buy_list)
    for stock in buy_list:
        if cash_per_stock <= 0:
            continue
        actual_cash = min(g.max_single_stock_amount, cash_per_stock)

        snapshot = snapshots.get(stock, {})
        price_ref = _as_float(snapshot.get('last_px', 0))
        if price_ref <= 0:
            continue

        # 限价 = 现价+1%，不超过涨停价
        limit_price = price_ref * 1.01
        up_px = _as_float(snapshot.get('up_px', price_ref * 1.1))
        limit_price = min(limit_price, up_px)
        limit_price = round(limit_price, 2)

        # 检查够不够买100股
        if actual_cash / limit_price < 100:
            log.info("[实盘]资金不足100股: %s 资金=%.2f 价格=%.2f" % (stock, actual_cash, limit_price))
            continue

        log.info("[实盘]买入 %s, 分配资金: %.2f, 实际使用: %.2f, 现价: %.2f, 委托价: %.2f" %
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
            log.info("[实盘]订单已提交: order_id=%s, stock=%s, limit_price=%.2f" %
                     (order_id, stock, limit_price))

    g.order_check_start_time = context.blotter.current_dt


def backtest_buy_task(context):
    """
    [回测专用] 开盘后执行四合一买入。
    使用首分钟K线open作为开盘价进行筛选。
    """
    if is_trade():
        return

    log.info("--- %s: [回测]触发买入任务 ---" % context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))

    all_candidates = list(set(g.gap_up + g.gap_down + g.reversal + g.fxsbdk))
    if not all_candidates:
        log.info("[回测]今日无候选股票，不执行买入。")
        return

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')

    # 获取开盘数据
    df_open = get_history(count=1, frequency='1m', field='open', security_list=all_candidates, include=True)
    df_preclose = get_history(count=1, frequency='1d', field='close', security_list=all_candidates)

    if not isinstance(df_preclose, pd.DataFrame) or df_preclose.empty:
        log.warning("[回测]无法获取昨日收盘价数据。")
        return

    # 构造price_dict: {code: {'open': xx, 'preclose': xx}}
    price_dict = {}
    for code in all_candidates:
        try:
            pc_data = df_preclose.query('code == "%s"' % code)
            if pc_data.empty:
                continue
            preclose = pc_data['close'].iloc[0]
            if preclose <= 0:
                continue

            open_px = 0.0
            if isinstance(df_open, pd.DataFrame) and not df_open.empty:
                op_data = df_open.query('code == "%s"' % code)
                if not op_data.empty:
                    open_px = op_data['open'].iloc[0]

            if open_px <= 0:
                continue

            price_dict[code] = {'open': open_px, 'preclose': preclose, 'open_ratio': open_px / preclose}
        except:
            continue

    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []
    fxsbdk_stocks = []

    # === 子策略1：一进二高开（回测简化版） ===
    gk_stocks = _filter_gap_up_backtest(context, g.gap_up, price_dict, yesterday_str)
    log.info("[回测]一进二高开入选: %d只" % len(gk_stocks))

    # === 子策略2：首板低开 ===
    dk_stocks = _filter_gap_down_backtest(context, g.gap_down, price_dict, yesterday_str)
    log.info("[回测]首板低开入选: %d只" % len(dk_stocks))

    # === 子策略3：弱转强 ===
    rzq_stocks = _filter_reversal_backtest(context, g.reversal, price_dict, yesterday_str)
    log.info("[回测]弱转强入选: %d只" % len(rzq_stocks))

    # === 子策略4：反向首板低开 ===
    fxsbdk_stocks = _filter_fxsbdk_backtest(context, g.fxsbdk, price_dict, yesterday_str)
    log.info("[回测]反向首板低开入选: %d只" % len(fxsbdk_stocks))

    qualified_stocks = list(set(gk_stocks + dk_stocks + rzq_stocks + fxsbdk_stocks))
    g.today_list = qualified_stocks
    log.info("[回测]四合一最终入选: %d只 %s" % (len(qualified_stocks), qualified_stocks))

    if not qualified_stocks:
        log.info("[回测]无满足条件的股票。")
        return

    # 持仓和资金检查
    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if getattr(pos, 'amount', 0) > 0]
    num_to_buy = g.stock_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[回测]持仓已满，无需买入。")
        return

    buy_list = [s for s in qualified_stocks if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[回测]候选股票均已持仓。")
        return

    available_cash = _as_float(context.portfolio.cash)
    if available_cash / _as_float(context.portfolio.portfolio_value) < 0.3:
        log.info("[回测]可用资金不足30%%，不执行买入。")
        return

    cash_per_stock = available_cash / len(buy_list)
    for stock in buy_list:
        if cash_per_stock <= 0:
            continue
        actual_cash = min(g.max_single_stock_amount, cash_per_stock)
        log.info("[回测]买入 %s, 分配资金: %.2f, 实际使用: %.2f" % (stock, cash_per_stock, actual_cash))
        order_id = order_value(stock, actual_cash)
        if order_id:
            g.today_bought_stocks.add(stock)


# ================================== 四合一子策略筛选 ===================================

def _filter_gap_up_realtime(context, stocks, snapshots, yesterday_str):
    """[实盘] 一进二高开筛选：竞价数据+成交额+市值+左压+开盘价"""
    if not stocks:
        return []
    result = []
    for s in stocks:
        try:
            snapshot = snapshots.get(s)
            if not snapshot:
                continue

            # 获取昨日数据
            hist = get_history(1, '1d', ['close', 'volume', 'money'], security_list=s)
            if hist is None or hist.empty:
                continue
            if 'code' in hist.columns:
                hist = hist[hist['code'] == s]
            if hist.empty:
                continue

            y_close = _as_float(hist['close'].iloc[-1])
            y_volume = _as_float(hist['volume'].iloc[-1])
            y_money = _as_float(hist['money'].iloc[-1])
            if y_close <= 0 or y_volume <= 0 or y_money <= 0:
                continue

            # 均价涨幅过滤
            avg_price_ratio = y_money / y_volume / y_close * 1.1 - 1
            if avg_price_ratio < g.gk_min_avg_price_ratio:
                continue
            # 成交额过滤
            if y_money < g.gk_min_money or y_money > g.gk_max_money:
                continue

            # 市值过滤（使用get_fundamentals）
            if not _check_market_cap(s, yesterday_str, g.gk_min_market_cap, g.gk_max_circ_cap):
                continue

            # 左压结构判断
            if rise_low_volume(s):
                continue

            # 竞价数据筛选（使用snapshot）
            open_px = _as_float(snapshot.get('open_px', 0))
            if open_px <= 0:
                open_px = _as_float(snapshot.get('last_px', 0))
            auction_vol = _as_float(snapshot.get('business_amount', 0))

            # 竞价成交量占比
            if y_volume > 0 and auction_vol > 0:
                if auction_vol / y_volume < g.gk_min_auction_vol_ratio:
                    continue

            # 竞价均价涨幅 >= 0 (即不低开)
            auction_money = _as_float(snapshot.get('business_balance', 0))
            if auction_vol > 0 and auction_money > 0:
                auction_avg = auction_money / auction_vol
                if (auction_avg - y_close) / y_close < 0:
                    continue

            # 开盘价区间
            open_ratio = open_px / y_close
            if open_ratio <= g.gk_open_ratio_min or open_ratio >= g.gk_open_ratio_max:
                continue

            result.append(s)
            log.info("[实盘高开]入选: %s open_ratio=%.4f avg_price_ratio=%.4f money=%.0f" %
                     (s, open_ratio, avg_price_ratio, y_money))
        except Exception as e:
            log.error("[实盘高开]筛选异常 %s: %s" % (s, e))
            continue
    return result


def _filter_gap_down_realtime(context, stocks, snapshots, yesterday_str):
    """[实盘] 首板低开筛选：低开3-4% + 相对位置 + 成交额"""
    if not stocks:
        return []

    # 计算60日相对位置
    rp_dict = get_relative_position(stocks, yesterday_str, 60)
    # 过滤相对位置
    stocks = [s for s in stocks if rp_dict.get(s, 1.0) <= g.dk_relative_pos_max]

    result = []
    for s in stocks:
        try:
            snapshot = snapshots.get(s)
            if not snapshot:
                continue

            open_px = _as_float(snapshot.get('open_px', 0))
            if open_px <= 0:
                open_px = _as_float(snapshot.get('last_px', 0))
            preclose = _as_float(snapshot.get('preclose_px', 0))
            if preclose <= 0 or open_px <= 0:
                continue

            open_ratio = open_px / preclose
            if not (g.dk_low_open_min <= open_ratio <= g.dk_low_open_max):
                continue

            # 成交额过滤
            hist = get_history(1, '1d', ['money'], security_list=s)
            if hist is None or hist.empty:
                continue
            if 'code' in hist.columns:
                hist = hist[hist['code'] == s]
            y_money = _as_float(hist['money'].iloc[-1]) if not hist.empty else 0
            if y_money < g.dk_min_money:
                continue

            result.append(s)
            log.info("[实盘低开]入选: %s open_ratio=%.4f rp=%.4f money=%.0f" %
                     (s, open_ratio, rp_dict.get(s, 0), y_money))
        except Exception as e:
            log.error("[实盘低开]筛选异常 %s: %s" % (s, e))
            continue
    return result


def _filter_reversal_realtime(context, stocks, snapshots, yesterday_str):
    """[实盘] 弱转强筛选：3日涨幅+开收比+成交额+市值+左压+竞价"""
    if not stocks:
        return []
    result = []
    for s in stocks:
        try:
            # 前3天涨幅过滤
            hist4 = get_history(4, '1d', ['close'], security_list=s)
            if hist4 is None or len(hist4) < 4:
                continue
            if 'code' in hist4.columns:
                hist4 = hist4[hist4['code'] == s]
            if len(hist4) < 4:
                continue
            increase_ratio = (hist4['close'].iloc[-1] - hist4['close'].iloc[0]) / hist4['close'].iloc[0]
            if increase_ratio > g.rzq_max_3day_increase:
                continue

            # 前日开收比过滤
            hist1 = get_history(1, '1d', ['open', 'close', 'volume', 'money'], security_list=s)
            if hist1 is None or hist1.empty:
                continue
            if 'code' in hist1.columns:
                hist1 = hist1[hist1['code'] == s]
            if hist1.empty:
                continue

            y_open = _as_float(hist1['open'].iloc[-1])
            y_close = _as_float(hist1['close'].iloc[-1])
            y_volume = _as_float(hist1['volume'].iloc[-1])
            y_money = _as_float(hist1['money'].iloc[-1])

            if y_open > 0:
                oc_ratio = (y_close - y_open) / y_open
                if oc_ratio < g.rzq_min_open_close_ratio:
                    continue

            # 均价涨幅过滤
            if y_close > 0 and y_volume > 0:
                avg_ratio = y_money / y_volume / y_close - 1
                if avg_ratio < g.rzq_min_avg_price_ratio:
                    continue

            # 成交额过滤
            if y_money < g.rzq_min_money or y_money > g.rzq_max_money:
                continue

            # 市值过滤
            if not _check_market_cap(s, yesterday_str, g.rzq_min_market_cap, g.rzq_max_circ_cap):
                continue

            # 左压结构
            if rise_low_volume(s):
                continue

            # 竞价数据筛选
            snapshot = snapshots.get(s)
            if not snapshot:
                continue

            auction_vol = _as_float(snapshot.get('business_amount', 0))
            if y_volume > 0 and auction_vol > 0:
                if auction_vol / y_volume < g.rzq_min_auction_vol_ratio:
                    continue

            open_px = _as_float(snapshot.get('open_px', 0))
            if open_px <= 0:
                open_px = _as_float(snapshot.get('last_px', 0))
            if y_close <= 0 or open_px <= 0:
                continue

            open_ratio = open_px / y_close
            if open_ratio <= g.rzq_open_ratio_min or open_ratio >= g.rzq_open_ratio_max:
                continue

            result.append(s)
            log.info("[实盘弱转强]入选: %s open_ratio=%.4f 3d_incr=%.4f oc_ratio=%.4f" %
                     (s, open_ratio, increase_ratio, oc_ratio if y_open > 0 else 0))
        except Exception as e:
            log.error("[实盘弱转强]筛选异常 %s: %s" % (s, e))
            continue
    return result


def _filter_fxsbdk_realtime(context, stocks, snapshots, yesterday_str):
    """[实盘] 反向首板低开筛选：非连板跌停 + 相对位置 + 高开4-10%"""
    if not stocks:
        return []

    # 过滤连板跌停
    lb_stocks = get_continue_limit_down_count(stocks, yesterday_str, 10)
    stocks = [s for s in stocks if s not in lb_stocks]

    # 相对位置过滤
    rp_dict = get_relative_position(stocks, yesterday_str, 60)
    stocks = [s for s in stocks if rp_dict.get(s, 1.0) <= g.fxsbdk_relative_pos_max]

    result = []
    for s in stocks:
        try:
            snapshot = snapshots.get(s)
            if not snapshot:
                continue

            open_px = _as_float(snapshot.get('open_px', 0))
            if open_px <= 0:
                open_px = _as_float(snapshot.get('last_px', 0))
            preclose = _as_float(snapshot.get('preclose_px', 0))
            if preclose <= 0 or open_px <= 0:
                continue

            open_ratio = open_px / preclose
            if not (g.fxsbdk_open_ratio_min <= open_ratio < g.fxsbdk_open_ratio_max):
                continue

            result.append(s)
            log.info("[实盘反向低开]入选: %s open_ratio=%.4f rp=%.4f" %
                     (s, open_ratio, rp_dict.get(s, 0)))
        except Exception as e:
            log.error("[实盘反向低开]筛选异常 %s: %s" % (s, e))
            continue
    return result


# ---------- 回测版筛选 ----------

def _filter_gap_up_backtest(context, stocks, price_dict, yesterday_str):
    """[回测] 一进二高开筛选（简化版，不含竞价成交量）"""
    if not stocks:
        return []
    result = []
    for s in stocks:
        try:
            pd_info = price_dict.get(s)
            if not pd_info:
                continue
            open_ratio = pd_info['open_ratio']

            # 开盘价区间
            if open_ratio <= g.gk_open_ratio_min or open_ratio >= g.gk_open_ratio_max:
                continue

            # 成交额过滤
            hist = get_history(1, '1d', ['close', 'volume', 'money'], security_list=s)
            if hist is None or hist.empty:
                continue
            if 'code' in hist.columns:
                hist = hist[hist['code'] == s]
            if hist.empty:
                continue

            y_close = _as_float(hist['close'].iloc[-1])
            y_volume = _as_float(hist['volume'].iloc[-1])
            y_money = _as_float(hist['money'].iloc[-1])

            avg_price_ratio = y_money / y_volume / y_close * 1.1 - 1 if y_close > 0 and y_volume > 0 else 0
            if avg_price_ratio < g.gk_min_avg_price_ratio:
                continue
            if y_money < g.gk_min_money or y_money > g.gk_max_money:
                continue

            # 市值过滤
            if not _check_market_cap(s, yesterday_str, g.gk_min_market_cap, g.gk_max_circ_cap):
                continue

            # 左压结构
            if rise_low_volume(s):
                continue

            result.append(s)
            log.info("[回测高开]入选: %s open_ratio=%.4f" % (s, open_ratio))
        except Exception as e:
            log.error("[回测高开]筛选异常 %s: %s" % (s, e))
    return result


def _filter_gap_down_backtest(context, stocks, price_dict, yesterday_str):
    """[回测] 首板低开筛选"""
    if not stocks:
        return []

    rp_dict = get_relative_position(stocks, yesterday_str, 60)
    stocks = [s for s in stocks if rp_dict.get(s, 1.0) <= g.dk_relative_pos_max]

    result = []
    for s in stocks:
        try:
            pd_info = price_dict.get(s)
            if not pd_info:
                continue
            open_ratio = pd_info['open_ratio']
            if not (g.dk_low_open_min <= open_ratio <= g.dk_low_open_max):
                continue

            hist = get_history(1, '1d', ['money'], security_list=s)
            if hist is None or hist.empty:
                continue
            if 'code' in hist.columns:
                hist = hist[hist['code'] == s]
            y_money = _as_float(hist['money'].iloc[-1]) if not hist.empty else 0
            if y_money < g.dk_min_money:
                continue

            result.append(s)
            log.info("[回测低开]入选: %s open_ratio=%.4f" % (s, open_ratio))
        except Exception as e:
            log.error("[回测低开]筛选异常 %s: %s" % (s, e))
    return result


def _filter_reversal_backtest(context, stocks, price_dict, yesterday_str):
    """[回测] 弱转强筛选"""
    if not stocks:
        return []
    result = []
    for s in stocks:
        try:
            pd_info = price_dict.get(s)
            if not pd_info:
                continue
            open_ratio = pd_info['open_ratio']
            if open_ratio <= g.rzq_open_ratio_min or open_ratio >= g.rzq_open_ratio_max:
                continue

            hist4 = get_history(4, '1d', ['close'], security_list=s)
            if hist4 is None or len(hist4) < 4:
                continue
            if 'code' in hist4.columns:
                hist4 = hist4[hist4['code'] == s]
            if len(hist4) < 4:
                continue
            increase_ratio = (hist4['close'].iloc[-1] - hist4['close'].iloc[0]) / hist4['close'].iloc[0]
            if increase_ratio > g.rzq_max_3day_increase:
                continue

            hist1 = get_history(1, '1d', ['open', 'close', 'volume', 'money'], security_list=s)
            if hist1 is None or hist1.empty:
                continue
            if 'code' in hist1.columns:
                hist1 = hist1[hist1['code'] == s]
            if hist1.empty:
                continue

            y_open = _as_float(hist1['open'].iloc[-1])
            y_close = _as_float(hist1['close'].iloc[-1])
            y_volume = _as_float(hist1['volume'].iloc[-1])
            y_money = _as_float(hist1['money'].iloc[-1])

            if y_open > 0:
                oc_ratio = (y_close - y_open) / y_open
                if oc_ratio < g.rzq_min_open_close_ratio:
                    continue
            if y_close > 0 and y_volume > 0:
                avg_ratio = y_money / y_volume / y_close - 1
                if avg_ratio < g.rzq_min_avg_price_ratio:
                    continue
            if y_money < g.rzq_min_money or y_money > g.rzq_max_money:
                continue
            if not _check_market_cap(s, yesterday_str, g.rzq_min_market_cap, g.rzq_max_circ_cap):
                continue
            if rise_low_volume(s):
                continue

            result.append(s)
            log.info("[回测弱转强]入选: %s open_ratio=%.4f" % (s, open_ratio))
        except Exception as e:
            log.error("[回测弱转强]筛选异常 %s: %s" % (s, e))
    return result


def _filter_fxsbdk_backtest(context, stocks, price_dict, yesterday_str):
    """[回测] 反向首板低开筛选"""
    if not stocks:
        return []

    lb_stocks = get_continue_limit_down_count(stocks, yesterday_str, 10)
    stocks = [s for s in stocks if s not in lb_stocks]
    rp_dict = get_relative_position(stocks, yesterday_str, 60)
    stocks = [s for s in stocks if rp_dict.get(s, 1.0) <= g.fxsbdk_relative_pos_max]

    result = []
    for s in stocks:
        try:
            pd_info = price_dict.get(s)
            if not pd_info:
                continue
            open_ratio = pd_info['open_ratio']
            if not (g.fxsbdk_open_ratio_min <= open_ratio < g.fxsbdk_open_ratio_max):
                continue

            result.append(s)
            log.info("[回测反向低开]入选: %s open_ratio=%.4f" % (s, open_ratio))
        except Exception as e:
            log.error("[回测反向低开]筛选异常 %s: %s" % (s, e))
    return result


# ================================== 卖出逻辑 ===================================

def sell_task(context):
    """
    执行卖出操作：未涨停的持仓全部卖出（保留原始聚宽逻辑）。
    11:25 和 14:50 两个时点执行。
    """
    current_time = context.blotter.current_dt
    log.info("--- %s: 触发卖出任务 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))

    positions = context.portfolio.positions
    if not positions:
        log.info("当前无持仓，无需卖出。")
        return

    # 过滤掉当日买入(T+1)和当日已卖出的
    sellable_stocks = [
        code for code in positions
        if code not in g.today_bought_stocks and code not in g.today_sold_stocks
        and _as_float(getattr(positions[code], 'amount', 0)) > 0
    ]
    if not sellable_stocks:
        log.info("无符合T+1规则的可卖出持仓。")
        return

    log.info("当前可卖出持仓: %s" % sellable_stocks)

    # 获取当前价格数据
    if is_trade():
        snapshots = get_snapshot(sellable_stocks)
        for code in sellable_stocks:
            snapshot = snapshots.get(code, {}) if snapshots else {}
            last_px = _as_float(snapshot.get('last_px', 0))
            up_px = _as_float(snapshot.get('up_px', 0))

            pos = positions[code]
            enable_amount = int(_as_float(getattr(pos, 'enable_amount', 0)))
            if enable_amount <= 0:
                p = get_position(code)
                enable_amount = int(_as_float(getattr(p, 'enable_amount', 0)))
            if enable_amount <= 0:
                continue

            # 核心卖出条件：未涨停就卖（保留原始逻辑）
            if up_px > 0 and last_px >= up_px * 0.999:
                log.info("[卖出]%s 涨停中(%.2f)，不卖" % (code, last_px))
                continue

            cost = _as_float(getattr(pos, 'cost_basis', 0))
            ret = (last_px / cost - 1) * 100 if cost > 0 else 0

            log.info("[卖出]%s 未涨停, 现价=%.2f, 成本=%.2f, 收益=%.2f%%" % (code, last_px, cost, ret))
            order_id = order_target(code, 0)
            if order_id:
                g.today_sold_stocks.add(code)
                # 记录卖出订单用于补单
                g.sell_orders[order_id] = {
                    'stock': code,
                    'reason': '未涨停卖出(收益%.2f%%)' % ret,
                    'time': current_time.strftime('%H:%M:%S'),
                    'retry_count': 0
                }
    else:
        # 回测模式
        for code in sellable_stocks:
            pos = positions[code]
            try:
                # 获取当前价格和涨停价
                hist = get_history(1, '1m', ['close'], security_list=code, include=True)
                preclose_hist = get_history(1, '1d', ['close', 'high_limit'], security_list=code)
                if hist is None or hist.empty or preclose_hist is None or preclose_hist.empty:
                    continue
                if 'code' in hist.columns:
                    hist = hist[hist['code'] == code]
                if 'code' in preclose_hist.columns:
                    preclose_hist = preclose_hist[preclose_hist['code'] == code]

                last_px = _as_float(hist['close'].iloc[-1])
                high_limit = _as_float(preclose_hist['high_limit'].iloc[-1]) if 'high_limit' in preclose_hist.columns else 0

                # 涨停不卖
                if high_limit > 0 and last_px >= high_limit * 0.999:
                    log.info("[回测卖出]%s 涨停中(%.2f)，不卖" % (code, last_px))
                    continue

                cost = _as_float(getattr(pos, 'cost_basis', 0))
                ret = (last_px / cost - 1) * 100 if cost > 0 else 0
                log.info("[回测卖出]%s 未涨停, 现价=%.2f, 成本=%.2f, 收益=%.2f%%" % (code, last_px, cost, ret))
                order_id = order_target(code, 0)
                if order_id:
                    g.today_sold_stocks.add(code)
            except Exception as e:
                log.error("[回测卖出]%s 异常: %s" % (code, e))


# ================================== 订单管理（V2框架） ===================================

def check_and_retry_orders(context):
    """[实盘专用] 统一的订单检查与补单函数，由run_interval每3秒执行一次。"""
    if not is_trade():
        return
    if g.buy_orders:
        check_and_retry_buy_orders(context)
    if g.sell_orders:
        check_and_retry_sell_orders(context)


def check_and_retry_buy_orders(context):
    """[实盘专用] 检查买入订单状态并补单。"""
    if not is_trade():
        return
    if not g.buy_orders:
        return

    current_time = context.blotter.current_dt
    log.info("--- %s: [实盘]买入订单检查 ---" % current_time.strftime('%Y-%m-%d %H:%M:%S'))

    # 获取当日成交记录防重复
    stock_filled_amounts = {}
    try:
        today_trades = get_trades()
        for trade in today_trades:
            if trade.is_buy:
                stock_code = trade.security
                if stock_code not in stock_filled_amounts:
                    stock_filled_amounts[stock_code] = 0
                stock_filled_amounts[stock_code] += trade.amount
    except Exception as e:
        log.warning("[实盘买入补单] 获取成交记录失败: %s" % e)

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

            if status == '8':  # 已成交
                orders_to_remove.append(order_id)

            elif status in ['0', '1', '2', '+', '-', 'C', 'V']:  # 未成交
                # 涨停不撤
                snap = get_snapshot(stock)
                if snap:
                    snap_data = snap.get(stock, snap) if isinstance(snap, dict) else snap
                    cur_px = _as_float(snap_data.get('last_px', 0)) if isinstance(snap_data, dict) else 0
                    up_px_val = _as_float(snap_data.get('up_px', 0)) if isinstance(snap_data, dict) else 0
                    if up_px_val > 0 and abs(cur_px - up_px_val) < 0.01:
                        continue

                is_cancelling = order_info.get('is_cancelling', False)
                if not is_cancelling:
                    try:
                        cancel_order(order_id)
                        g.buy_orders[order_id]['is_cancelling'] = True
                    except:
                        pass
                # 等待下轮确认

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
            log.error("[实盘买入补单] 异常: %s, %s" % (order_id, e))

    for oid in orders_to_remove:
        if oid in g.buy_orders:
            del g.buy_orders[oid]

    # 补单
    if orders_to_retry:
        filtered = [item for item in orders_to_retry
                    if not (item['stock'] in stock_filled_amounts and stock_filled_amounts[item['stock']] > 0)]
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
                    log.info("[实盘买入补单] 补单成功 %s (第%d次)" % (stock, r_count))
            except Exception as e:
                log.error("[实盘买入补单] 异常: %s, %s" % (stock, e))


def check_and_retry_sell_orders(context):
    """[实盘专用] 检查卖出订单状态并补单。"""
    if not is_trade():
        return
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
                # 跌停不撤
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
                except:
                    pass

            elif status in ['6', '9']:
                orders_to_remove.append(order_id)
                orders_to_retry.append({
                    'stock': stock, 'reason': reason,
                    'retry_count': retry_count + 1
                })

        except Exception as e:
            log.error("[实盘卖出补单] 异常: %s, %s" % (order_id, e))

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
                    log.info("[实盘卖出补单] 补单成功 %s (第%d次)" % (stock, r_count))
            except Exception as e:
                log.error("[实盘卖出补单] 异常: %s, %s" % (stock, e))


# ================================== 辅助函数 ===================================

def _check_market_cap(stock, date_str, min_total_cap, max_circ_cap):
    """
    市值检查（PTrade兼容版）：
    PTrade的valuation表不包含聚宽的market_cap/circulating_market_cap字段，
    改用 get_price 获取的成交额(money)和收盘价(close)×成交量(volume) 来近似估算。
    
    近似逻辑：
    - 总市值下限(min_total_cap亿)：用日均成交额的倒数近似判断（成交额过低说明市值小）
      成交额 < 总市值下限 * 0.005(千分之五换手)，则认为市值不足
    - 流通市值上限(max_circ_cap亿)：用日均成交额的上限近似判断
      成交额 > 流通市值上限 * 0.1(10%换手)，则认为是超大盘，但这种情况极少发生
    实际上通过成交额阈值过滤已经在各子策略中完成了（如gk_min_money等），
    这里只做粗略的补充验证。
    """
    try:
        # 获取最近5日成交额的均值来估算
        hist = get_price(stock, end_date=date_str, frequency='1d', fields=['money', 'close', 'volume'], count=5)
        if hist is None or hist.empty:
            return True  # 数据缺失时放行

        if 'code' in hist.columns:
            hist = hist[hist['code'] == stock]
        if hist.empty:
            return True

        avg_money = hist['money'].mean()
        if avg_money <= 0:
            return True

        # 粗略估算：假设平均换手率约0.5%，则 市值 ≈ 成交额 / 0.005
        estimated_cap_yi = avg_money / 1e8 / 0.005  # 转换为"亿"

        # 总市值下限检查
        if min_total_cap > 0 and estimated_cap_yi < min_total_cap:
            return False

        # 流通市值上限检查（粗略）
        if max_circ_cap > 0 and estimated_cap_yi > max_circ_cap * 5:
            # 估算市值远超上限才过滤（因为换手率假设不精确，放宽5倍）
            return False

        return True
    except Exception as e:
        log.error("_check_market_cap error for %s: %s" % (stock, e))
        return True  # 异常时放行


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
        return result if result else ""
    except Exception:
        return ""


# ================================== 交易报告（V2框架） ===================================

def daily_trading_report(context):
    """每日交易报告（含股票名称）"""
    log.info("=" * 80)
    current_date = context.blotter.current_dt.strftime('%Y-%m-%d')
    portfolio = context.portfolio
    total_value = _as_float(portfolio.portfolio_value)
    cash = _as_float(portfolio.cash)

    if g.last_total_value > 0:
        daily_return = total_value - g.last_total_value
        daily_return_pct = (daily_return / g.last_total_value * 100) if g.last_total_value > 0 else 0
    else:
        g.last_total_value = _as_float(context.capital_base)
        daily_return, daily_return_pct = 0, 0

    g.last_total_value = total_value

    log.info("【%s 交易报告 - 打板四合一】" % current_date)
    log.info("总资产: %.2f元 | 现金: %.2f元" % (total_value, cash))
    log.info("当日收益: %+.2f元 (%+.2f%%)" % (daily_return, daily_return_pct))
    log.info("【当日买入】%d只: %s" % (len(g.today_bought_stocks), ', '.join(g.today_bought_stocks) or '无'))
    log.info("【当日卖出】%d只: %s" % (len(g.today_sold_stocks), ', '.join(g.today_sold_stocks) or '无'))

    # 子策略入选统计
    log.info("【子策略入选】一进二高开=%d | 首板低开=%d | 弱转强=%d | 反向首板低开=%d" %
             (len([s for s in g.today_list if s in g.gap_up]),
              len([s for s in g.today_list if s in g.gap_down and s not in g.gap_up]),
              len([s for s in g.today_list if s in g.reversal]),
              len([s for s in g.today_list if s in g.fxsbdk])))

    if portfolio.positions:
        log.info("【持仓详情】共%d只股票" % len(portfolio.positions))
        position_codes = [code for code, pos in portfolio.positions.items()
                          if _as_float(getattr(pos, 'amount', 0)) > 0]
        stock_names = {}
        try:
            name_data = get_stock_name(position_codes)
            if isinstance(name_data, dict):
                stock_names = name_data
        except:
            pass

        for code, pos in portfolio.positions.items():
            amount = int(_as_float(getattr(pos, 'amount', 0)))
            if amount <= 0:
                continue
            name = stock_names.get(code, code)
            cost = _as_float(getattr(pos, 'cost_basis', 0))
            last = _as_float(getattr(pos, 'last_sale_price', 0))
            if last <= 0:
                last = cost
            market_value = amount * last
            profit_loss = (last - cost) * amount
            cost_value = cost * amount
            profit_loss_pct = (profit_loss / cost_value * 100) if cost_value != 0 else 0
            log.info("  %s(%s): %d股 | 成本:%.2f | 现价:%.2f | 市值:%.2f元 | 盈亏:%+.2f元(%+.2f%%)" % (
                name, code, amount, cost, last, market_value, profit_loss, profit_loss_pct
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
                'highest_price': _as_float(getattr(position, 'last_sale_price', 0))
            }

    for code in list(g.buy_records.keys()):
        if code not in current_positions:
            del g.buy_records[code]

    log.info("收盘后更新持仓记录: %s" % g.buy_records)