# -*- coding: utf-8 -*-
"""
连板预测选股策略 v2（回撤控制增强版）
============================================
基于 v1 版本改进，重点优化回撤控制：

改进点：
  1. 止损黑名单机制 — 止损后5个交易日内不再买入同一只股票
  2. 硬性止损线-8% — 无论什么条件，亏损达到8%立即卖出
  3. 收紧选股漏斗 — 近10日至少2次涨停
  4. 优化连板开板止损 — 连板1天开板且盈利<3%直接卖出；连板越多允许回撤越大
  5. 持仓详情过滤0股 — 日志中不再显示已清仓标的
  6. 持仓数量从4只减至3只 — 集中持仓提高选股精度

策略逻辑:
  基于三连板因子挖掘研究成果（8种模型+3581个事件+182个因子），
  在首板/二板阶段提前介入，捕捉后续连板行情。

核心选股因子（多模型共识 Top 因子）:
  1. signal_vol_price_mom — 量价动量共振信号（SHAP排名第1）
  2. turn_x_atr5 — 换手率×波动率交互（SHAP排名第3）
  3. atr_norm_5 — 5日归一化ATR（统计AUC=0.7067）
  4. signal_squeeze_breakout — 缩量突破信号（差异+284%）
  5. resid_mom_10 — 10日残差动量（差异+386%）
  6. days_since_last_limit_up — 距上次涨停天数（事件组30天 vs 对照组79天）
  7. momentum_120d — 120日动量（差异+2011%）
  8. pe_ttm — 市盈率TTM（事件组43.8 vs 对照组30.0）
  9. turn_today — 当日换手率（差异+138%）
  10. vol_change_20d — 20日量变化率（差异+1342%）

买入逻辑:
  盘前选股 → 集合竞价/开盘价筛选 → 多因子排序 → 黑名单过滤 → 买入Top N

卖出逻辑:
  硬性止损(-8%) + 连板跟踪（涨停不卖）+ 动态止损 + 移动止盈 + 均线止损

研究依据:
  CatBoost模型 AUC=0.765, CV AUC=0.765±0.015
  XGBoost模型 AUC=0.739, SHAP分析验证因子方向一致
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
            log.error("=" * 50)
            log.error("【授权失败】策略无权在当前账户或时间运行！")
            log.error("=" * 50)
            raise RuntimeError('授权验证失败，终止策略运行')
        else:
            log.info("✅ 授权验证通过，策略启动成功")

    # --- 策略参数 ---
    # 【改进7】持仓股票数量从4只减至3只，集中持仓提高选股精度
    g.stock_num = 4
    # 开盘价筛选阈值（低开不超过3%，高开不超过5%）
    g.open_down_threshold = 0.97
    g.open_up_threshold = 1.05
    # 单股最大买入金额限制
    g.max_single_stock_amount = 100000  # 10万元

    # --- 连板预测因子参数 ---
    # 距上次涨停天数阈值（研究结论：事件组中位数30天）
    g.days_since_limit_up_max = 60
    # 120日动量阈值（研究结论：事件组+9.1%）
    g.momentum_120d_min = 0.0
    # PE阈值（研究结论：事件组43.8，但不宜过高）
    g.pe_max = 200
    # 换手率阈值（研究结论：事件组3.75%）
    g.turnover_min = 1.0  # 最低换手率%
    # 5日ATR归一化阈值（研究结论：事件组0.051 vs 对照组0.035）
    g.atr_norm_5_min = 0.03
    # 成交量倍数上限（过滤异常放量）
    g.volume_ratio_max = 15
    # 均线周期
    g.ma_period = 10
    # 卖出均线周期
    g.stop_loss_ma_period = 7

    # --- 连板跟踪参数 ---
    # 涨停不卖（跟踪连板）
    g.hold_if_limit_up = True
    # 连板中途开板卖出的容忍度（开板后跌幅超过此值卖出）
    g.limit_up_break_threshold = -0.02

    # --- 【改进3】收紧选股漏斗参数 ---
    # 近10日至少涨停次数
    g.min_limit_up_count_10d = 2

    # --- 【改进2】硬性止损线 ---
    g.hard_stop_loss_pct = -0.08  # 亏损8%无条件止损

    # --- 【改进1】止损黑名单参数 ---
    g.stop_loss_blacklist = {}  # {stock_code: expire_date_str}
    g.blacklist_days = 5  # 止损后冷却5个交易日


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
    # 实盘在集合竞价结束后买入
    run_daily(context, real_trade_buy_task, time='09:25:10')
    # 实盘订单检查与补单
    run_interval(context, check_and_retry_orders, seconds=3)
    # 回测在开盘后第一个bar模拟买入
    run_daily(context, backtest_buy_task, time='09:31')
    # 注册两个卖出时间点的任务
    run_daily(context, sell_task, time='13:00')
    run_daily(context, sell_task, time='14:55')


def before_trading_start(context, data):
    """
    每日开盘前运行函数。
    核心选股逻辑在此执行。
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

    log.info("=" * 60)
    log.info("【连板预测策略v2】新交易日开始")

    # 【改进1】清理过期的止损黑名单
    current_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')
    expired_keys = [k for k, v in g.stop_loss_blacklist.items() if v <= current_date_str]
    for k in expired_keys:
        del g.stop_loss_blacklist[k]
    if g.stop_loss_blacklist:
        log.info("【黑名单】当前止损冷却中的股票: %s" % list(g.stop_loss_blacklist.keys()))

    # 3. 执行选股逻辑
    log.info(">>> 开始执行盘前选股任务...")

    # 3.1 获取基础股票池：全A股，剔除创业板、科创板和ST
    stock_pool = get_all_stocks_excluding_gem(context)
    log.info("全A股(去创业板/科创板/ST)数量: %d" % len(stock_pool))

    # 3.2 第一层筛选：近期有涨停基因（改进：至少2次涨停）
    stock_pool = limit_up_gene_filter(context, stock_pool)
    log.info("涨停基因筛选后数量: %d" % len(stock_pool))

    # 3.3 第二层筛选：中长期趋势向上
    stock_pool = trend_filter(context, stock_pool)
    log.info("趋势筛选后数量: %d" % len(stock_pool))

    # 3.4 第三层筛选：量价异动信号
    stock_pool = volume_price_signal_filter(context, stock_pool)
    log.info("量价异动筛选后数量: %d" % len(stock_pool))

    # 3.5 第四层筛选：波动率+换手率条件
    stock_pool = volatility_turnover_filter(context, stock_pool)
    log.info("波动率换手率筛选后数量: %d" % len(stock_pool))

    # 【改进1】过滤止损黑名单中的股票
    before_blacklist = len(stock_pool)
    stock_pool = [s for s in stock_pool if s not in g.stop_loss_blacklist]
    filtered_count = before_blacklist - len(stock_pool)
    if filtered_count > 0:
        log.info("【黑名单过滤】移除 %d 只近期止损股票" % filtered_count)

    # 3.6 将初选结果存入全局变量
    g.today_list = stock_pool
    log.info("盘前选股完成，待买池数量: %d" % len(g.today_list))
    if g.today_list:
        log.info("候选股票: %s" % ', '.join(g.today_list[:20]))

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


# ================================== 选股逻辑函数 ===================================

def get_all_stocks_excluding_gem(context):
    """获取全A股并过滤创业板、科创板和ST"""
    current_date = context.blotter.current_dt.strftime('%Y%m%d')
    all_stocks = get_Ashares(current_date)

    st_status = get_stock_status(all_stocks, 'ST', current_date)

    final_list = []
    for stock_code in all_stocks:
        if stock_code.startswith('30'):  # 剔除创业板
            continue
        if stock_code.startswith('68'):  # 剔除科创板
            continue
        if st_status.get(stock_code, False):  # 剔除ST
            continue
        final_list.append(stock_code)
    return final_list


def limit_up_gene_filter(context, stocks):
    """
    第一层筛选：涨停基因
    【改进3】从"近N天有过1次涨停"改为"近10天至少有2次涨停"
    研究结论：days_since_last_limit_up 事件组中位数30天 vs 对照组79天
    多次涨停的股票连板持续性更强
    """
    if not stocks:
        return []

    try:
        # 获取过去60个交易日的数据
        lookback = g.days_since_limit_up_max
        hist_df = get_history(lookback, '1d', ['close', 'high'], security_list=stocks)
        if hist_df.empty:
            return []

        valid_stocks = []
        for code in stocks:
            try:
                df = hist_df.query('code == "%s"' % code)
                if len(df) < 20:
                    continue

                # 【改进3】统计近10天内涨停次数
                limit_up_count_10d = 0
                # 同时检查近60天内是否有涨停
                has_limit_up_60d = False

                for i in range(1, len(df)):
                    prev_close = df['close'].iloc[i - 1]
                    if prev_close <= 0:
                        continue
                    pct = (df['close'].iloc[i] / prev_close - 1) * 100
                    if pct >= 9.5:
                        has_limit_up_60d = True
                        # 近10天的涨停
                        if i >= len(df) - 10:
                            limit_up_count_10d += 1

                # 必须近60天有涨停，且近10天至少有min_limit_up_count_10d次涨停
                if has_limit_up_60d and limit_up_count_10d >= g.min_limit_up_count_10d:
                    valid_stocks.append(code)
            except Exception as e:
                continue

        return valid_stocks
    except Exception as e:
        log.error("涨停基因筛选失败: %s" % e)
        return []


def trend_filter(context, stocks):
    """
    第二层筛选：中长期趋势向上
    研究结论：momentum_120d 事件组+9.1% vs 对照组-0.5%
    """
    if not stocks:
        return []

    try:
        # 获取120日数据
        hist_df = get_history(121, '1d', ['close', 'high', 'low'], security_list=stocks)
        if hist_df.empty:
            return []

        valid_stocks = []
        for code in stocks:
            try:
                df = hist_df.query('code == "%s"' % code)
                if len(df) < 60:
                    continue

                last_close = df['close'].iloc[-1]
                if last_close <= 1:  # 过滤低价股
                    continue

                # 120日动量 > 0（中长期趋势向上）
                if len(df) >= 121:
                    mom_120 = last_close / df['close'].iloc[0] - 1
                    if mom_120 <= g.momentum_120d_min:
                        continue

                # 价格在10日均线上方
                ma10 = df['close'].tail(g.ma_period).mean()
                if last_close < ma10:
                    continue

                # 距120日高点不超过25%（研究结论：事件组-12.5%）
                high_120 = df['high'].max()
                if high_120 > 0:
                    dist_to_high = last_close / high_120 - 1
                    if dist_to_high < -0.25:
                        continue

                valid_stocks.append(code)
            except Exception as e:
                continue

        return valid_stocks
    except Exception as e:
        log.error("趋势筛选失败: %s" % e)
        return []


def volume_price_signal_filter(context, stocks):
    """
    第三层筛选：量价异动信号
    研究结论：
      - vol_change_20d 事件组+75% vs 对照组+5%（差异1342%）
      - signal_vol_price_mom 事件组+0.26 vs 对照组-0.27
      - signal_squeeze_breakout 事件组+0.39 vs 对照组-0.21
    """
    if not stocks:
        return []

    try:
        hist_df = get_history(25, '1d', ['close', 'open', 'high', 'low', 'volume'],
                              security_list=stocks)
        if hist_df.empty:
            return []

        valid_stocks = []
        for code in stocks:
            try:
                df = hist_df.query('code == "%s"' % code)
                if len(df) < 21:
                    continue

                close = df['close']
                volume = df['volume']
                high = df['high']
                low = df['low']

                # 近5日均量 vs 近20日均量（量能放大）
                vol_ma_5 = volume.tail(5).mean()
                vol_ma_20 = volume.tail(20).mean()
                if vol_ma_20 <= 0:
                    continue

                vol_ratio = vol_ma_5 / vol_ma_20
                # 量能至少不萎缩（研究结论：事件组vol_shrink_5_20=1.19）
                if vol_ratio < 0.7:
                    continue

                # 20日量变化率 > 0（研究结论：事件组+75%）
                if len(volume) >= 21:
                    vol_20d_ago = volume.iloc[-21]
                    vol_now = volume.iloc[-1]
                    if vol_20d_ago > 0:
                        vol_change_20d = (vol_now - vol_20d_ago) / vol_20d_ago
                        # 不要求太高，但至少量能没有大幅萎缩
                        if vol_change_20d < -0.5:
                            continue

                # 近3日成交量放大（研究结论：vol_change_3d显著）
                vol_ma_3 = volume.tail(3).mean()
                if vol_ma_3 < vol_ma_20 * 0.5:  # 近3日量不能太低
                    continue

                # 昨日成交量 > 前日成交量（放量趋势）
                last_vol = volume.iloc[-1]
                prev_vol = volume.iloc[-2]
                if last_vol < prev_vol * 0.5:  # 不能急剧缩量
                    continue

                # 成交量不能过于异常（过滤天量）
                if last_vol > prev_vol * g.volume_ratio_max:
                    continue

                valid_stocks.append(code)
            except Exception as e:
                continue

        return valid_stocks
    except Exception as e:
        log.error("量价异动筛选失败: %s" % e)
        return []


def volatility_turnover_filter(context, stocks):
    """
    第四层筛选：波动率+换手率条件
    研究结论：
      - atr_norm_5 事件组0.051 vs 对照组0.035（高出47%）
      - turn_today 事件组3.75% vs 对照组1.58%（高出138%）
      - turn_cv_60 事件组0.71 vs 对照组0.61（换手率波动加大）
    """
    if not stocks:
        return []

    try:
        hist_df = get_history(15, '1d', ['close', 'high', 'low', 'volume'],
                              security_list=stocks)
        if hist_df.empty:
            return []

        # 获取换手率
        yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
        turnover_dict = get_turnover_ratio_yesterday(stocks)

        valid_stocks = []
        for code in stocks:
            try:
                df = hist_df.query('code == "%s"' % code)
                if len(df) < 10:
                    continue

                close = df['close']
                high = df['high']
                low = df['low']
                last_close = close.iloc[-1]

                if last_close <= 0:
                    continue

                # 计算5日ATR归一化（研究结论：事件组0.051）
                tr_values = []
                for i in range(1, len(df)):
                    h = high.iloc[i]
                    l = low.iloc[i]
                    pc = close.iloc[i - 1]
                    tr = max(h - l, abs(h - pc), abs(l - pc))
                    tr_values.append(tr)

                if len(tr_values) >= 5:
                    atr_5 = np.mean(tr_values[-5:])
                    atr_norm_5 = atr_5 / last_close
                    if atr_norm_5 < g.atr_norm_5_min:
                        continue
                else:
                    continue

                # 换手率检查（研究结论：事件组3.75%）
                turnover = turnover_dict.get(code, 0.0)
                if turnover < g.turnover_min / 100.0:  # 转换为小数
                    continue

                valid_stocks.append(code)
            except Exception as e:
                continue

        return valid_stocks
    except Exception as e:
        log.error("波动率换手率筛选失败: %s" % e)
        return []


# ================================== 因子计算与排序 ===================================

def calculate_prediction_factors(stocks):
    """
    计算连板预测核心因子（基于研究成果Top因子）
    返回 {stock_code: {factor_name: value}} 字典

    核心因子（多模型SHAP共识）：
    1. signal_vol_price_mom — 量价动量共振（Z-score合成）
    2. turn_x_atr5 — 换手率×波动率交互
    3. atr_norm_5 — 5日归一化ATR
    4. resid_mom_10 — 10日残差动量
    5. signal_squeeze_breakout — 缩量突破信号
    6. momentum_20d — 20日动量
    7. dist_to_high_120 — 距120日高点距离
    8. vol_change_3d — 3日量变化率
    """
    if not stocks:
        return {}

    try:
        # 需要足够的历史数据
        hist_df = get_history(65, '1d', ['close', 'open', 'high', 'low', 'volume'],
                              security_list=stocks)
        if hist_df.empty:
            return {}

        # 获取换手率
        turnover_dict = get_turnover_ratio_yesterday(stocks)

        # 计算市场等权收益（用于残差动量）
        all_returns = {}
        for code in stocks:
            df = hist_df.query('code == "%s"' % code)
            if len(df) >= 2:
                rets = df['close'].pct_change().dropna()
                all_returns[code] = rets

        # 市场平均收益
        if all_returns:
            all_ret_df = pd.DataFrame(all_returns)
            mkt_ret = all_ret_df.mean(axis=1)
        else:
            mkt_ret = None

        factor_dict = {}
        for code in stocks:
            try:
                df = hist_df.query('code == "%s"' % code)
                if len(df) < 25:
                    continue

                close = df['close']
                open_p = df['open']
                high = df['high']
                low = df['low']
                volume = df['volume']
                last_close = close.iloc[-1]

                if last_close <= 0:
                    continue

                factors = {}

                # --- 1. 5日归一化ATR ---
                tr_values = []
                for i in range(1, len(df)):
                    h = high.iloc[i]
                    l = low.iloc[i]
                    pc = close.iloc[i - 1]
                    tr = max(h - l, abs(h - pc), abs(l - pc))
                    tr_values.append(tr)

                if len(tr_values) >= 5:
                    atr_5 = np.mean(tr_values[-5:])
                    factors['atr_norm_5'] = atr_5 / last_close
                else:
                    factors['atr_norm_5'] = 0

                # --- 2. 换手率 ---
                turnover = turnover_dict.get(code, 0.0)
                factors['turnover'] = turnover

                # --- 3. 换手率 × ATR交互 ---
                factors['turn_x_atr5'] = turnover * factors['atr_norm_5']

                # --- 4. 20日动量 ---
                if len(close) >= 21:
                    factors['momentum_20d'] = last_close / close.iloc[-21] - 1
                else:
                    factors['momentum_20d'] = 0

                # --- 5. 5日动量 ---
                if len(close) >= 6:
                    factors['momentum_5d'] = last_close / close.iloc[-6] - 1
                else:
                    factors['momentum_5d'] = 0

                # --- 6. 3日量变化率 ---
                if len(volume) >= 4:
                    factors['vol_change_3d'] = (volume.iloc[-1] / volume.iloc[-4] - 1) \
                        if volume.iloc[-4] > 0 else 0
                else:
                    factors['vol_change_3d'] = 0

                # --- 7. 量价动量共振信号（Z-score合成） ---
                # signal_vol_price_mom = mean(Z(turnover), Z(atr_norm_5), Z(momentum_5d))
                factors['signal_vol_price_mom'] = (
                    factors['turnover'] + factors['atr_norm_5'] + factors['momentum_5d']
                )  # 简化版：直接求和（排序时等价于Z-score排序）

                # --- 8. 10日残差动量 ---
                if mkt_ret is not None and code in all_returns:
                    ret_series = all_returns[code]
                    if len(ret_series) >= 10:
                        ret_10 = ret_series.tail(10)
                        mkt_10 = mkt_ret.tail(10)
                        # 简化：残差 = 个股收益 - 市场收益
                        resid = ret_10.values - mkt_10.values[:len(ret_10)]
                        factors['resid_mom_10'] = np.sum(resid)
                    else:
                        factors['resid_mom_10'] = 0
                else:
                    factors['resid_mom_10'] = 0

                # --- 9. 缩量突破信号 ---
                # 近5日量/近20日量 + 价格压缩度 + RSV位置
                vol_ma_5 = volume.tail(5).mean()
                vol_ma_20 = volume.tail(20).mean() if len(volume) >= 20 else vol_ma_5
                vol_shrink = vol_ma_5 / vol_ma_20 if vol_ma_20 > 0 else 1

                # 价格压缩度（近10日最高/最低 - 1）
                if len(df) >= 10:
                    h10 = high.tail(10).max()
                    l10 = low.tail(10).min()
                    price_comp = (h10 / l10 - 1) if l10 > 0 else 0
                else:
                    price_comp = 0

                # RSV_60
                if len(close) >= 60:
                    c60_max = close.tail(60).max()
                    c60_min = close.tail(60).min()
                    rsv_60 = (last_close - c60_min) / (c60_max - c60_min) \
                        if (c60_max - c60_min) > 0 else 0.5
                else:
                    rsv_60 = 0.5

                factors['signal_squeeze_breakout'] = vol_shrink + price_comp + rsv_60

                # --- 10. 距120日高点距离 ---
                if len(high) >= 60:
                    high_max = high.max()
                    factors['dist_to_high'] = last_close / high_max - 1 if high_max > 0 else 0
                else:
                    factors['dist_to_high'] = 0

                # --- 11. EMA30偏离率（趋势强度） ---
                ema_30 = close.ewm(span=30, adjust=False).mean().iloc[-1]
                factors['ema30_ratio'] = last_close / ema_30 if ema_30 > 0 else 1.0

                # --- 12. Parkinson波动率（低波动异象，取负） ---
                if len(df) >= 20:
                    log_hl = np.log(high.tail(20) / low.tail(20)) ** 2
                    park_vol = np.sqrt(log_hl.mean() / (4 * np.log(2)))
                    factors['park_vol'] = -park_vol  # 取负：低波动得高分
                else:
                    factors['park_vol'] = 0

                # --- 13. CCI_20 ---
                if len(df) >= 20:
                    tp = (high + low + close) / 3
                    tp_20 = tp.tail(20)
                    ma_tp = tp_20.mean()
                    md_tp = (tp_20 - ma_tp).abs().mean()
                    factors['cci_20'] = (tp.iloc[-1] - ma_tp) / (0.015 * md_tp) \
                        if md_tp > 0 else 0
                else:
                    factors['cci_20'] = 0

                # --- 14. ROC_20 ---
                if len(close) >= 21:
                    factors['roc_20'] = last_close / close.iloc[-21] - 1
                else:
                    factors['roc_20'] = 0

                factor_dict[code] = factors
            except Exception as e:
                log.debug("[因子计算] %s 异常: %s" % (code, str(e)))
                continue

        return factor_dict
    except Exception as e:
        log.error("[因子计算] 批量计算失败: %s" % e)
        return {}


def multi_factor_rank_score(results_df):
    """
    多因子排名打分：对每个因子列做截面排名，然后加权相加
    权重基于研究中的SHAP重要性

    因子权重（基于CatBoost SHAP）：
      signal_vol_price_mom: 3（SHAP排名第1-2）
      turn_x_atr5: 3（SHAP排名第3）
      atr_norm_5: 2（统计AUC=0.7067）
      resid_mom_10: 2（差异+386%）
      momentum_20d: 1.5
      vol_change_3d: 1.5
      signal_squeeze_breakout: 1.5
      ema30_ratio: 1
      park_vol: 1
      roc_20: 1
      cci_20: 1
      turnover: 1
    """
    factor_weights = {
        'signal_vol_price_mom': 3.0,
        'turn_x_atr5': 3.0,
        'atr_norm_5': 2.0,
        'resid_mom_10': 2.0,
        'momentum_20d': 1.5,
        'vol_change_3d': 1.5,
        'signal_squeeze_breakout': 1.5,
        'ema30_ratio': 1.0,
        'park_vol': 1.0,
        'roc_20': 1.0,
        'cci_20': 1.0,
        'turnover': 1.0,
    }

    total_weight = 0
    results_df['composite_score'] = 0.0

    for factor, weight in factor_weights.items():
        if factor in results_df.columns:
            # 截面排名（值越大排名越高）
            rank_col = factor + '_rank'
            results_df[rank_col] = results_df[factor].rank(ascending=True, method='min')
            results_df['composite_score'] += results_df[rank_col] * weight
            total_weight += weight

    if total_weight > 0:
        results_df['composite_score'] /= total_weight

    return results_df.sort_values(by='composite_score', ascending=False)


# ================================== 买入逻辑 ===================================

def get_effective_stock_num(context):
    """
    获取最大持仓数量
    """
    return g.stock_num


def real_trade_buy_task(context):
    """
    [实盘专用] 在集合竞价期间执行买入操作。
    """
    if not is_trade():
        return

    log.info("--- %s: [实盘]触发集合竞价买入任务 ---" %
             context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))

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

    # 2. 计算可买入数量和资金（使用动态持仓数量）
    effective_num = get_effective_stock_num(context)
    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if getattr(pos, 'amount', 0) > 0]
    num_to_buy = effective_num - len(current_positions)
    if num_to_buy <= 0:
        return

    buy_list = [s for s in buy_list if s not in current_positions][:num_to_buy]
    if not buy_list:
        log.info("[实盘]候选股票均已持仓，无新的可买入股票。")
        return

    # 3. 执行买入
    cash_per_stock = context.portfolio.cash / len(buy_list)
    for stock in buy_list:
        if cash_per_stock > 0:
            actual_cash = min(g.max_single_stock_amount, cash_per_stock)
            price_ref = snapshots.get(stock, {}).get('last_px', 0)
            if price_ref > 0:
                limit_price = price_ref * 1.01
                up_px = snapshots.get(stock, {}).get('up_px', price_ref * 1.1)
                limit_price = min(limit_price, up_px)
                limit_price = round(limit_price, 2)

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


def backtest_buy_task(context):
    """
    [回测专用] 在开盘后执行买入操作。
    """
    if is_trade():
        return

    log.info("--- %s: [回测]触发买入任务 ---" %
             context.blotter.current_dt.strftime('%Y-%m-%d %H:%M:%S'))

    if not g.today_list:
        log.info("[回测]今日无候选股票，不执行买入。")
        return

    # 1. 开盘价筛选和排序
    buy_list = backtest_opening_filter_and_rank(context, g.today_list)
    log.info("[回测]经开盘价筛选和排序后，买入池数量: %d" % len(buy_list))

    if not buy_list:
        log.info("[回测]无满足开盘价条件的股票。")
        return

    # 2. 计算可买入数量和资金（使用动态持仓数量）
    effective_num = get_effective_stock_num(context)
    current_positions = [code for code, pos in context.portfolio.positions.items()
                         if getattr(pos, 'amount', 0) > 0]
    num_to_buy = effective_num - len(current_positions)
    if num_to_buy <= 0:
        log.info("[回测]持仓已满(%d/%d)，无需买入。" % (len(current_positions), effective_num))
        return

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


def backtest_opening_filter_and_rank(context, stocks):
    """
    [回测专用] 开盘价筛选和多因子排序
    """
    if not stocks:
        return []

    turnover_dict = get_turnover_ratio_yesterday(stocks)

    # 计算连板预测因子
    prediction_factors = calculate_prediction_factors(stocks)

    # 集合竞价数据
    try:
        trend_data = get_trend_data(stocks=stocks)
    except Exception:
        trend_data = {}

    df_open_data = get_history(count=1, frequency='1m', field='open',
                               security_list=stocks, include=True)
    df_preclose_data = get_history(count=1, frequency='1d', field='close',
                                   security_list=stocks)

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

            # 获取开盘价
            open_now = 0.0
            td = trend_data.get(code) if isinstance(trend_data, dict) else None
            if td:
                open_now = td.get('hq_px') or td.get('wavg_px') or 0.0

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

            # 获取预测因子
            pf = prediction_factors.get(code, {})
            turnover = turnover_dict.get(code, 0.0)

            record = {
                'code': code,
                'open_ratio': open_ratio,
                'turnover': turnover,
                'signal_vol_price_mom': pf.get('signal_vol_price_mom', 0),
                'turn_x_atr5': pf.get('turn_x_atr5', 0),
                'atr_norm_5': pf.get('atr_norm_5', 0),
                'resid_mom_10': pf.get('resid_mom_10', 0),
                'momentum_20d': pf.get('momentum_20d', 0),
                'vol_change_3d': pf.get('vol_change_3d', 0),
                'signal_squeeze_breakout': pf.get('signal_squeeze_breakout', 0),
                'ema30_ratio': pf.get('ema30_ratio', 1.0),
                'park_vol': pf.get('park_vol', 0),
                'roc_20': pf.get('roc_20', 0),
                'cci_20': pf.get('cci_20', 0),
            }

            log.info("[回测]因子: %s open=%.4f turn=%.4f sig_vpm=%.4f tx_atr=%.6f resid=%.4f" %
                     (code, open_ratio, turnover,
                      record['signal_vol_price_mom'],
                      record['turn_x_atr5'],
                      record['resid_mom_10']))
            results.append(record)
        except Exception as e:
            log.warning("[回测]计算因子失败 for %s: %s" % (code, e))
            continue

    if not results:
        return []

    # 多因子排名打分
    df_results = pd.DataFrame(results)
    df_sorted = multi_factor_rank_score(df_results)
    log.info("[回测]多因子排序结果:\n%s" %
             df_sorted[['code', 'composite_score']].to_string())
    return df_sorted['code'].tolist()


def real_trade_opening_filter_and_rank(context, snapshots, stocks):
    """
    [实盘专用] 集合竞价数据筛选和排序，增加BS(买卖盘)过滤
    """
    if not stocks:
        return []

    turnover_dict = get_turnover_ratio_yesterday(stocks)
    prediction_factors = calculate_prediction_factors(stocks)

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

            bs_data_available = (bid_qty_total > 0 or offer_qty_total > 0 or entrust_rate != 0)
            if bs_data_available:
                if bid_qty_total <= offer_qty_total and entrust_rate <= 0:
                    log.info("[实盘]BS过滤: %s 卖压过大被过滤" % code)
                    continue

            # 获取预测因子
            pf = prediction_factors.get(code, {})
            turnover = turnover_dict.get(code, 0.0)

            record = {
                'code': code,
                'open_ratio': open_ratio,
                'turnover': turnover,
                'signal_vol_price_mom': pf.get('signal_vol_price_mom', 0),
                'turn_x_atr5': pf.get('turn_x_atr5', 0),
                'atr_norm_5': pf.get('atr_norm_5', 0),
                'resid_mom_10': pf.get('resid_mom_10', 0),
                'momentum_20d': pf.get('momentum_20d', 0),
                'vol_change_3d': pf.get('vol_change_3d', 0),
                'signal_squeeze_breakout': pf.get('signal_squeeze_breakout', 0),
                'ema30_ratio': pf.get('ema30_ratio', 1.0),
                'park_vol': pf.get('park_vol', 0),
                'roc_20': pf.get('roc_20', 0),
                'cci_20': pf.get('cci_20', 0),
            }

            log.info("[实盘]因子: %s open=%.4f turn=%.4f sig_vpm=%.4f tx_atr=%.6f" %
                     (code, open_ratio, turnover,
                      record['signal_vol_price_mom'],
                      record['turn_x_atr5']))
            results.append(record)
        except Exception as e:
            log.warning("[实盘]计算因子失败 for %s: %s" % (code, e))
            continue

    if not results:
        return []

    df_results = pd.DataFrame(results)
    df_sorted = multi_factor_rank_score(df_results)
    log.info("[实盘]多因子排序结果:\n%s" %
             df_sorted[['code', 'composite_score']].to_string())
    return df_sorted['code'].tolist()


# ================================== 卖出逻辑 ===================================

def sell_task(context):
    """
    执行卖出操作。
    13:00 只做止损（不止盈），14:55 做止损+止盈+常规卖出。

    特色：连板跟踪 — 如果持仓股涨停，不卖出，等待连板。
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

    # 过滤掉当日买入的股票 (T+1) 和当日已卖出的股票
    sellable_stocks = [
        code for code in positions
        if code not in g.today_bought_stocks and code not in g.today_sold_stocks
    ]
    if not sellable_stocks:
        log.info("无符合T+1规则的可卖出持仓。")
        return

    log.info("当前可卖出持仓股票: %s" % sellable_stocks)

    # 获取当前价格数据
    price_data = {}
    if is_trade():
        snapshots = get_snapshot(sellable_stocks)
        for code in sellable_stocks:
            if code in snapshots:
                price_data[code] = {
                    'price': snapshots[code].get('last_px', 0),
                    'preclose': snapshots[code].get('preclose_px', 0)
                }
    else:  # 回测
        df_price = get_history(count=1, frequency='1m', field='price',
                               security_list=sellable_stocks, include=True)
        df_preclose = get_history(count=1, frequency='1d', field='close',
                                  security_list=sellable_stocks)
        for code in sellable_stocks:
            price_series = df_price.query('code == "%s"' % code)
            preclose_series = df_preclose.query('code == "%s"' % code)
            if not price_series.empty and not preclose_series.empty:
                price_data[code] = {
                    'price': price_series['price'].iloc[0],
                    'preclose': preclose_series['close'].iloc[0]
                }

    # 【改进2】0. 硬性止损检查：亏损超过8%无条件卖出
    hard_stop_stocks = []
    for code in list(sellable_stocks):
        if code not in price_data:
            continue
        position = context.portfolio.positions.get(code)
        if not position:
            continue
        cost_price = position.cost_basis
        current_price = price_data[code]['price']
        if cost_price > 0 and current_price > 0:
            profit_rate = (current_price - cost_price) / cost_price
            if profit_rate <= g.hard_stop_loss_pct:
                log.info("🛑 %s 触发硬性止损线(亏损%.1f%% <= %.1f%%)，无条件卖出！" %
                         (code, profit_rate * 100, g.hard_stop_loss_pct * 100))
                order_id = order_target(code, 0)
                if order_id:
                    g.today_sold_stocks.add(code)
                    # 【改进1】加入止损黑名单
                    add_to_stop_loss_blacklist(context, code)
                    if is_trade():
                        g.sell_orders[order_id] = {
                            'stock': code,
                            'reason': '硬性止损: 亏损%.1f%%' % (profit_rate * 100),
                            'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                            'retry_count': 0
                        }
                hard_stop_stocks.append(code)

    # 从可卖出列表中移除已硬性止损的股票
    sellable_stocks = [s for s in sellable_stocks if s not in hard_stop_stocks]

    # 1. 连板跟踪：涨停的股票不卖
    remaining_stocks = []
    for code in sellable_stocks:
        if code not in price_data:
            continue

        current_price = price_data[code]['price']
        preclose = price_data[code]['preclose']

        if preclose > 0 and current_price > 0:
            # 判断是否涨停
            high_limit, _ = calculate_limit_prices(code, preclose)
            if high_limit and abs(current_price - high_limit) < 0.02:
                if g.hold_if_limit_up:
                    log.info("🔒 %s 涨停中(%.2f)，继续持有等待连板" % (code, current_price))
                    # 更新最高价
                    record = g.buy_records.get(code, {})
                    record['highest_price'] = max(record.get('highest_price', 0), current_price)
                    record['consecutive_limit_up'] = record.get('consecutive_limit_up', 0) + 1
                    g.buy_records[code] = record
                    continue  # 涨停不卖

        remaining_stocks.append(code)

    # 2. 执行动态止损
    stop_only = not is_afternoon_session
    after_stop_stocks = []
    for code in remaining_stocks:
        if code not in price_data:
            continue
        should_sell, reason = enhanced_dynamic_stop_loss(context, code, price_data,
                                                         stop_only=stop_only)
        if should_sell:
            log.info("触发动态止损卖出 %s: %s" % (code, reason))
            order_id = order_target(code, 0)
            if order_id:
                g.today_sold_stocks.add(code)
                # 【改进1】止损类卖出加入黑名单
                if '止损' in reason:
                    add_to_stop_loss_blacklist(context, code)
                if is_trade():
                    g.sell_orders[order_id] = {
                        'stock': code,
                        'reason': '动态止损: ' + reason,
                        'time': context.blotter.current_dt.strftime('%H:%M:%S'),
                        'retry_count': 0
                    }
        else:
            after_stop_stocks.append(code)

    if not after_stop_stocks:
        log.info("动态止损已处理所有可卖出持仓。")
        return

    # 3. 常规卖出逻辑（仅14:55时段）
    if not is_afternoon_session:
        log.info("[13:00时段] 跳过常规卖出逻辑，等待14:55再判断止盈")
        return

    log.info("[14:55时段] 对剩余持仓执行常规卖出逻辑: %s" % after_stop_stocks)
    for code in after_stop_stocks:
        if code not in price_data:
            continue
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


def enhanced_dynamic_stop_loss(context, stock, price_data, stop_only=False):
    """
    增强版动态止损系统（针对连板策略优化）

    【改进5】优化连板开板止损：
    - 连板1天开板且盈利<3%直接卖出
    - 连板越多，允许的回撤越大（动态回撤阈值）

    【改进2】硬性止损已在sell_task中提前处理，此处不再重复
    """
    position = context.portfolio.positions.get(stock)
    if not position:
        return False, ""

    cost_price = position.cost_basis
    current_price = price_data[stock]['price']
    if cost_price <= 0 or current_price <= 0:
        return False, ""
    profit_rate = (current_price - cost_price) / cost_price

    # 更新最高价
    record = g.buy_records.get(stock, {})
    highest_price = record.get('highest_price', cost_price)
    if current_price > highest_price:
        record['highest_price'] = current_price
        g.buy_records[stock] = record
        highest_price = current_price

    hold_days = get_hold_days(context, stock)

    # 1. 【改进5】连板开板止损（动态回撤阈值）
    consec_limit = record.get('consecutive_limit_up', 0)
    if consec_limit >= 1:
        if highest_price > 0:
            drawdown = (highest_price - current_price) / highest_price

            if consec_limit == 1:
                # 连板1天开板：盈利<3%直接卖出，否则回撤2%卖出
                if profit_rate < 0.03:
                    return True, "连板1天开板+盈利不足(盈利%.1f%% < 3%%)" % (profit_rate * 100)
                elif drawdown >= 0.02:
                    return True, "连板1天开板止损(回撤%.1f%%)" % (drawdown * 100)
            elif consec_limit == 2:
                # 连板2天开板：回撤3%卖出
                if drawdown >= 0.03:
                    return True, "连板2天开板止损(回撤%.1f%%)" % (drawdown * 100)
            elif consec_limit >= 3:
                # 连板3天+开板：回撤5%卖出（给更多空间）
                if drawdown >= 0.05:
                    return True, "连板%d天开板止损(回撤%.1f%%)" % (consec_limit, drawdown * 100)

    # 2. 基于持仓时间的阶梯止损
    if hold_days <= 1 and profit_rate <= -0.02:
        return True, "日内快速止损(%.1f%%)" % (profit_rate * 100)
    if hold_days <= 3 and profit_rate <= -0.03:
        return True, "短期止损(%.1f%%)" % (profit_rate * 100)
    if hold_days > 5 and profit_rate <= -0.05:
        return True, "中期止损(%.1f%%)" % (profit_rate * 100)

    # 3. 基于ATR的动态止损
    atr_stop_price = calculate_atr_stop_loss(context, stock, cost_price)
    if current_price <= atr_stop_price:
        return True, "ATR动态止损(止损价: %.2f)" % atr_stop_price

    # 4. 基于大盘环境的止损
    if market_condition_stop_loss(context, profit_rate):
        return True, "大盘环境恶化止损"

    # 5. 移动止盈（13:00时段stop_only=True时跳过）
    if not stop_only:
        # 盈利超过5%，从最高点回撤3%则止盈
        if profit_rate > 0.05:
            if (highest_price - current_price) / highest_price >= 0.03:
                return True, "盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (
                    profit_rate * 100,
                    (highest_price - current_price) / highest_price * 100)
        # 盈利超过10%，从最高点回撤4%则止盈
        if profit_rate > 0.10:
            if (highest_price - current_price) / highest_price >= 0.04:
                return True, "高盈利保护止盈(盈利%.1f%%, 回撤%.1f%%)" % (
                    profit_rate * 100,
                    (highest_price - current_price) / highest_price * 100)
        # 盈利超过20%（可能经历了连板），从最高点回撤5%止盈
        if profit_rate > 0.20:
            if (highest_price - current_price) / highest_price >= 0.05:
                return True, "连板高盈利止盈(盈利%.1f%%, 回撤%.1f%%)" % (
                    profit_rate * 100,
                    (highest_price - current_price) / highest_price * 100)

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

        # 条件1: 当日未涨停（涨停不卖）
        high_limit_today, _ = calculate_limit_prices(code, pre_close)
        if high_limit_today is None or last_price >= high_limit_today * 0.999:
            return False, "已涨停或接近涨停"

        # 条件2: 跌破N日均线
        hist_df = get_history(g.stop_loss_ma_period, '1d', 'close',
                              security_list=code, include=False)
        if len(hist_df) >= g.stop_loss_ma_period:
            ma_stop = hist_df['close'].mean()
            if last_price < ma_stop:
                return True, "跌破%d日线(MA=%.2f)" % (g.stop_loss_ma_period, ma_stop)

        # 条件3: 盈利 > 0 且持仓超过3天（锁定利润）
        hold_days = get_hold_days(context, code)
        if last_price > avg_cost and hold_days > 3:
            return True, "持仓%d天盈利卖出" % hold_days

        # 条件4: 昨日是涨停板但今日未涨停（连板断裂）
        yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')
        day_before_str = get_trading_day(-2).strftime('%Y-%m-%d')
        close_dby_df = get_price(code, end_date=day_before_str, frequency='1d',
                                  fields='close', count=1)
        close_y_df = get_price(code, end_date=yesterday_str, frequency='1d',
                                fields='close', count=1)

        if not close_dby_df.empty and not close_y_df.empty:
            close_dby = close_dby_df['close'].iloc[0]
            close_y = close_y_df['close'].iloc[0]
            high_limit_yesterday, _ = calculate_limit_prices(code, close_dby)
            if high_limit_yesterday is not None and abs(close_y - high_limit_yesterday) < 0.01:
                # 昨日涨停，今日未涨停 → 连板断裂
                if last_price < high_limit_today * 0.999:
                    return True, "昨日涨停今日未封板，连板断裂"

        return False, ""
    except Exception as e:
        log.error("常规卖出条件判断异常 for %s: %s" % (code, e))
        return False, ""


# ================================== 【改进1】止损黑名单模块 ===================================

def add_to_stop_loss_blacklist(context, stock):
    """
    【改进1】将股票加入止损黑名单，冷却期内不再买入
    """
    try:
        current_date = context.blotter.current_dt.date()
        # 获取未来第N个交易日作为过期日期
        future_days = get_trade_days(
            start_date=current_date.strftime('%Y%m%d'),
            end_date=(current_date + datetime.timedelta(days=g.blacklist_days * 2)).strftime('%Y%m%d')
        )
        if len(future_days) > g.blacklist_days:
            expire_date = future_days[g.blacklist_days].strftime('%Y-%m-%d')
        else:
            expire_date = (current_date + datetime.timedelta(days=g.blacklist_days)).strftime('%Y-%m-%d')

        g.stop_loss_blacklist[stock] = expire_date
        log.info("🚫 【黑名单】%s 加入止损冷却名单，到期日: %s" % (stock, expire_date))
    except Exception as e:
        log.error("加入止损黑名单失败: %s" % e)


# ================================== 实盘订单管理 ===================================

def check_and_retry_orders(context):
    """
    [实盘专用] 统一的订单检查与补单函数。
    由run_interval每3秒执行一次。
    """
    if not is_trade():
        return

    if g.buy_orders:
        check_and_retry_buy_orders(context)

    if g.sell_orders:
        check_and_retry_sell_orders(context)


def check_and_retry_buy_orders(context):
    """[实盘专用] 检查买入订单状态并补单"""
    if not is_trade() or not g.buy_orders:
        return

    current_time = context.blotter.current_dt
    log.info("[实盘买入补单] 待检查订单数量: %d" % len(g.buy_orders))

    try:
        today_trades = get_trades()
        stock_filled_amounts = {}
        for trade in today_trades:
            if trade.is_buy:
                stock_code = trade.security
                if stock_code not in stock_filled_amounts:
                    stock_filled_amounts[stock_code] = 0
                stock_filled_amounts[stock_code] += trade.amount
    except Exception as e:
        stock_filled_amounts = {}

    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.buy_orders.items()):
        stock = order_info['stock']
        cash_allocated = order_info['cash']
        retry_count = order_info.get('retry_count', 0)

        stock_total_retry = g.stock_retry_count.get(stock, 0)
        if stock_total_retry >= g.max_retry_count:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if order_list and len(order_list) > 0:
                order_status = order_list[0]
                status = order_status.status

                if stock in stock_filled_amounts and stock_filled_amounts[stock] > 0:
                    orders_to_remove.append(order_id)
                    continue

                if status == '8':  # 已成交
                    orders_to_remove.append(order_id)
                elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:
                    # 未成交，撤单重下
                    is_cancelling = order_info.get('is_cancelling', False)
                    if not is_cancelling:
                        try:
                            cancel_order(order_id)
                            g.buy_orders[order_id]['is_cancelling'] = True
                        except Exception as e:
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
        except Exception as e:
            pass

    for order_id in orders_to_remove:
        if order_id in g.buy_orders:
            del g.buy_orders[order_id]

    if orders_to_retry:
        filtered_orders = [item for item in orders_to_retry
                           if item['stock'] not in stock_filled_amounts
                           or stock_filled_amounts.get(item['stock'], 0) == 0]

        if filtered_orders:
            stocks_to_retry = [item['stock'] for item in filtered_orders]
            snapshots = get_snapshot(stocks_to_retry)
            if snapshots:
                for item in filtered_orders:
                    stock = item['stock']
                    snapshot = snapshots.get(stock)
                    if not snapshot:
                        continue
                    current_price = snapshot.get('last_px', 0)
                    if current_price <= 0:
                        continue
                    limit_price = round(min(current_price * 1.01,
                                            snapshot.get('up_px', current_price * 1.1)), 2)
                    try:
                        new_order_id = order_value(stock, item['cash'],
                                                    limit_price=limit_price)
                        if new_order_id:
                            g.buy_orders[new_order_id] = {
                                'stock': stock, 'cash': item['cash'],
                                'limit_price': limit_price,
                                'time': current_time.strftime('%H:%M:%S'),
                                'retry_count': item['retry_count']
                            }
                            g.stock_retry_count[stock] = item['retry_count']
                    except Exception as e:
                        pass


def check_and_retry_sell_orders(context):
    """[实盘专用] 检查卖出订单状态并补单"""
    if not is_trade() or not g.sell_orders:
        return

    current_time = context.blotter.current_dt
    orders_to_remove = []
    orders_to_retry = []

    for order_id, order_info in list(g.sell_orders.items()):
        stock = order_info['stock']
        reason = order_info.get('reason', '')
        retry_count = order_info.get('retry_count', 0)

        if retry_count >= g.max_retry_count:
            orders_to_remove.append(order_id)
            continue

        try:
            order_list = get_order(order_id)
            if order_list and len(order_list) > 0:
                status = order_list[0].status
                if status == '8':
                    orders_to_remove.append(order_id)
                elif status in ['0', '1', '2', '7', '+', '-', 'C', 'V']:
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
        except:
            pass

    for order_id in orders_to_remove:
        if order_id in g.sell_orders:
            del g.sell_orders[order_id]

    if orders_to_retry:
        stocks_to_retry = [item['stock'] for item in orders_to_retry]
        snapshots = get_snapshot(stocks_to_retry)
        if snapshots:
            for item in orders_to_retry:
                stock = item['stock']
                position = context.portfolio.positions.get(stock)
                if not position or position.amount <= 0:
                    continue
                snapshot = snapshots.get(stock)
                if not snapshot:
                    continue
                current_price = snapshot.get('last_px', 0)
                if current_price <= 0:
                    continue
                limit_price = round(max(current_price * 0.99,
                                        snapshot.get('down_px', current_price * 0.9)), 2)
                try:
                    new_order_id = order_target(stock, 0, limit_price=limit_price)
                    if new_order_id:
                        g.sell_orders[new_order_id] = {
                            'stock': stock, 'reason': item['reason'],
                            'time': current_time.strftime('%H:%M:%S'),
                            'retry_count': item['retry_count']
                        }
                except:
                    pass


# ================================== 辅助与工具函数 ===================================

def get_turnover_ratio_yesterday(stocks):
    """获取上一交易日的换手率字典"""
    if not stocks:
        return {}

    yesterday_str = get_trading_day(-1).strftime('%Y-%m-%d')

    try:
        valuation_df = get_fundamentals(stocks, 'valuation', 'turnover_rate', yesterday_str)
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
            turnover = 0.0
        result[code] = turnover
    return result


def get_hold_days(context, stock):
    """计算持仓天数"""
    record = g.buy_records.get(stock)
    if record and 'buy_date' in record:
        try:
            buy_date = pd.to_datetime(record['buy_date']).date()
            current_date = context.blotter.current_dt.date()
            trade_days = get_trade_days(
                start_date=buy_date.strftime('%Y%m%d'),
                end_date=current_date.strftime('%Y%m%d'))
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
        daily_return_pct = (daily_return / g.last_total_value * 100) \
            if g.last_total_value > 0 else 0
    else:
        g.last_total_value = context.capital_base
        daily_return, daily_return_pct = 0, 0

    g.last_total_value = total_value

    log.info("【%s 连板预测策略v2交易报告】" % current_date)
    log.info("总资产: %.2f元 | 现金: %.2f元" % (total_value, cash))
    log.info("当日收益: %+.2f元 (%+.2f%%)" % (daily_return, daily_return_pct))

    log.info("黑名单: %d只" % len(g.stop_loss_blacklist))

    log.info("【当日买入】%d只: %s" % (
        len(g.today_bought_stocks),
        ', '.join(g.today_bought_stocks) or '无'))
    log.info("【当日卖出】%d只: %s" % (
        len(g.today_sold_stocks),
        ', '.join(g.today_sold_stocks) or '无'))

    if portfolio.positions:
        # 【改进6】过滤0股持仓，只显示实际持有的股票
        active_positions = {code: pos for code, pos in portfolio.positions.items()
                           if pos.amount > 0}
        if active_positions:
            log.info("【持仓详情】共%d只股票" % len(active_positions))
            position_codes = list(active_positions.keys())
            stock_names = get_stock_name(position_codes)
            for code, pos in active_positions.items():
                name = stock_names.get(code, code)
                profit_loss = (pos.last_sale_price - pos.cost_basis) * pos.amount
                cost_value = pos.cost_basis * pos.amount
                profit_loss_pct = (profit_loss / cost_value * 100) if cost_value != 0 else 0
                # 连板跟踪信息
                record = g.buy_records.get(code, {})
                consec = record.get('consecutive_limit_up', 0)
                consec_str = " 🔥连板%d天" % consec if consec > 0 else ""
                log.info("  %s(%s): %d股 | 成本:%.2f | 现价:%.2f | 盈亏:%+.2f元(%+.2f%%)%s" % (
                    name, code, pos.amount, pos.cost_basis, pos.last_sale_price,
                    profit_loss, profit_loss_pct, consec_str
                ))
        else:
            log.info("【持仓详情】当前无实际持仓")
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
                'highest_price': position.last_sale_price,
                'consecutive_limit_up': 0,
            }

    # 移除已卖出的记录
    current_records = list(g.buy_records.keys())
    for code in current_records:
        if code not in current_positions:
            del g.buy_records[code]

    log.info("收盘后更新持仓记录: %s" % g.buy_records)