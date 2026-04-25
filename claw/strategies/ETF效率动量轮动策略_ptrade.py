# -*- coding: utf-8 -*-
"""
ETF效率动量轮动策略 - PTrade回测版
================================================================
【策略来源】
  基于微信群分享的 ETF效率动量轮动策略V1 改写为 PTrade 回测格式
  核心思想：在候选ETF池中，每日选出"动量最强且路径最高效"的N只，全仓轮动持有

【打分公式】
  ETF得分 = 动量(Momentum) × 效率系数(Efficiency Ratio)
  
  - Pivot = (Open + High + Low + Close) / 4  （四价均值，比单纯收盘价更稳定）
  - Momentum = 100 × ln(Pivot_today / Pivot_N天前)  （对数收益率）
  - Efficiency Ratio = |ln(Pivot_end) - ln(Pivot_start)| / Σ|ln(Pivot_t) - ln(Pivot_{t-1})|
    → ER接近1：趋势清晰，价格走得"直"
    → ER接近0：来回震荡，价格走得"弯"
  - 最终得分 = Momentum × ER  （不仅要涨得多，还要涨得"顺畅"）

【ETF池选择】
  通过 ETF_POOL_MODE 参数切换：
  - 'wufu'  : 五福策略ETF池（100+只，覆盖大宗商品/海外/港股/指数/行业ETF）
  - 'sanma' : 三马ETF轮动池（17只，精选大类资产）
  - 'mini'  : 原版迷你池（4只，红利/创业板/纳指/黄金）

【交易逻辑】
  1. 每日盘前计算所有候选ETF的效率动量得分
  2. 选得分最高的 SELECT_NUM 只
  3. 卖出不在目标列表中的持仓
  4. 买入目标ETF，等权分配资金
  5. 可选：得分全部为负时切换到防御ETF或空仓

【使用方法】
  直接在 PTrade 量化平台导入此文件，设置回测参数即可运行
"""

import numpy as np
import pandas as pd
import math

# ============================================================
# 策略参数配置
# ============================================================

# ETF池模式选择: 'wufu' / 'sanma' / 'mini'
ETF_POOL_MODE = 'wufu'

# 动量和效率系数计算窗口（交易日）
N_DAYS = 20

# 选入ETF数目
SELECT_NUM = 1

# 是否启用空仓机制（当所有ETF得分为负时空仓）
ENABLE_EMPTY_POSITION = True

# 防御性ETF（得分全负时的避风港，仅在 ENABLE_EMPTY_POSITION=False 时生效）
DEFENSIVE_ETF = '511660.SS'  # 货币ETF

# 交易时间
SELL_TIME = '09:35'
BUY_TIME = '09:40'

# 佣金设置
COMMISSION_RATIO = 0.0001
MIN_COMMISSION = 0.001

# ============================================================
# ETF候选池定义
# ============================================================

# 五福策略ETF池（来自群主多策略实盘框架 v1.24 WuFuStrategy）
WUFU_ETF_POOL = [
    # 大宗商品ETF
    '161226.SZ',  # 国投白银LOF
    '159980.SZ',  # 有色ETF大成

    # 海外ETF
    '159509.SZ',  # 纳指科技ETF景顺
    '513290.SS',  # 纳指生物
    '159518.SZ',  # 标普油气ETF嘉实
    '159502.SZ',  # 标普生物科技ETF嘉实
    '159529.SZ',  # 标普消费ETF
    '513400.SS',  # 道琼斯
    '520830.SS',  # 沙特ETF
    '513080.SS',  # 法国ETF
    '520870.SS',  # 巴西ETF

    # 港股ETF
    '513090.SS',  # 香港证券
    '513180.SS',  # 恒指科技
    '513120.SS',  # HK创新药
    '513330.SS',  # 恒生互联
    '513750.SS',  # 港股非银
    '159892.SZ',  # 恒生医药ETF
    '159605.SZ',  # 中概互联ETF
    '513190.SS',  # H股金融
    '510900.SS',  # 恒生中国
    '513630.SS',  # 香港红利
    '513920.SS',  # 港股通央企红利
    '159323.SZ',  # 港股通汽车ETF
    '513970.SS',  # 恒生消费

    # 指数ETF
    '510500.SS',  # 中证500ETF
    '512100.SS',  # 中证1000ETF
    '563300.SS',  # 中证2000
    '510300.SS',  # 沪深300ETF
    '512050.SS',  # A500E
    '510760.SS',  # 上证ETF
    '159949.SZ',  # 创业板50ETF
    '159967.SZ',  # 创业板成长ETF
    '588080.SS',  # 科创板50
    '588220.SS',  # 科创100
    '511380.SS',  # 可转债ETF

    # 行业ETF
    '513310.SS',  # 中韩芯片
    '588200.SS',  # 科创芯片
    '159852.SZ',  # 软件ETF
    '512880.SS',  # 证券ETF
    '159206.SZ',  # 卫星ETF
    '512400.SS',  # 有色金属ETF
    '512980.SS',  # 传媒ETF
    '159516.SZ',  # 半导体设备ETF
    '515880.SS',  # 通信ETF
    '562500.SS',  # 机器人
    '159218.SZ',  # 卫星产业ETF
    '159869.SZ',  # 游戏ETF
    '159870.SZ',  # 化工ETF
    '159326.SZ',  # 电网设备ETF
    '560860.SS',  # 工业有色
    '159363.SZ',  # 创业板人工智能ETF华宝
    '588170.SS',  # 科创半导
    '159755.SZ',  # 电池ETF
    '512170.SS',  # 医疗ETF
    '512800.SS',  # 银行ETF
    '159819.SZ',  # 人工智能ETF易方达
    '512710.SS',  # 军工龙头
    '159638.SZ',  # 高端装备ETF嘉实
    '517520.SS',  # 黄金股
    '515980.SS',  # 人工智能
    '159995.SZ',  # 芯片ETF
    '159227.SZ',  # 航空航天ETF
    '512660.SS',  # 军工ETF
    '512690.SS',  # 酒ETF
    '516150.SS',  # 稀土基金
    '588790.SS',  # 科创智能
    '159992.SZ',  # 创新药ETF
    '512070.SS',  # 证券保险
    '562800.SS',  # 稀有金属
    '512010.SS',  # 医药ETF
    '515790.SS',  # 光伏ETF
    '159928.SZ',  # 消费ETF
    '159883.SZ',  # 医疗器械ETF
    '159998.SZ',  # 计算机ETF
    '515220.SS',  # 煤炭ETF
    '561980.SS',  # 芯片设备
    '515400.SS',  # 大数据
    '515120.SS',  # 创新药
    '159566.SZ',  # 储能电池ETF易方达
    '515050.SS',  # 5GETF
    '516510.SS',  # 云计算ETF
    '159256.SZ',  # 创业板软件ETF华夏
    '159766.SZ',  # 旅游ETF
    '512200.SS',  # 地产ETF
    '513350.SS',  # 油气ETF
    '159583.SZ',  # 通信设备ETF
    '159732.SZ',  # 消费电子ETF
    '516160.SS',  # 新能源
    '516520.SS',  # 智能驾驶
    '562590.SS',  # 半导材料
    '515030.SS',  # 新汽车
    '512670.SS',  # 国防ETF
    '561330.SS',  # 矿业ETF
    '516190.SS',  # 文娱ETF
    '159840.SZ',  # 锂电池ETF工银
    '159611.SZ',  # 电力ETF
    '159981.SZ',  # 能源化工ETF
    '159865.SZ',  # 养殖ETF
    '561360.SS',  # 石油ETF
    '159667.SZ',  # 工业母机ETF
    '515170.SS',  # 食品饮料ETF
    '513360.SS',  # 教育ETF
    '159825.SZ',  # 农业ETF
    '515210.SS',  # 钢铁ETF
]

# 三马ETF轮动池（来自群主多策略实盘框架 v1.24 EtfRotationStrategy）
SANMA_ETF_POOL = [
    '510180.SS',  # 上证180ETF
    '513030.SS',  # 德国30ETF
    '513100.SS',  # 纳指ETF
    '513520.SS',  # 日经225ETF
    '510410.SS',  # 资源ETF
    '518880.SS',  # 黄金ETF
    '501018.SS',  # 南方原油
    '159985.SZ',  # 豆粕ETF
    '511090.SS',  # 30年国债
    '159915.SZ',  # 创业板ETF
    '588120.SS',  # 科创100ETF
    '512480.SS',  # 半导体ETF
    '159851.SZ',  # 金科ETF
    '513020.SS',  # 香港科技ETF
    '159637.SZ',  # 新能源龙头ETF
    '513690.SS',  # 恒生股息ETF
    '510050.SS',  # 50ETF
]

# 原版迷你池（来自原始策略V1）
MINI_ETF_POOL = [
    '510880.SS',  # 红利ETF
    '159915.SZ',  # 创业板ETF
    '513100.SS',  # 纳指ETF
    '518880.SS',  # 黄金ETF
]


def _get_etf_pool():
    """根据配置返回对应的ETF池"""
    if ETF_POOL_MODE == 'wufu':
        return WUFU_ETF_POOL[:]
    elif ETF_POOL_MODE == 'sanma':
        return SANMA_ETF_POOL[:]
    elif ETF_POOL_MODE == 'mini':
        return MINI_ETF_POOL[:]
    else:
        log.error(f"未知的ETF池模式: {ETF_POOL_MODE}，使用默认mini池")
        return MINI_ETF_POOL[:]


# ============================================================
# 核心算法：效率动量得分计算
# ============================================================

def calculate_efficiency_momentum_score(pivot_series):
    """
    计算单个ETF的效率动量得分
    
    参数:
        pivot_series: numpy数组，长度至少为 N_DAYS+1 的 pivot 值序列
    
    返回:
        float: 效率动量得分 = momentum × efficiency_ratio
               如果数据不足或计算异常返回 NaN
    """
    if len(pivot_series) < 2:
        return np.nan
    
    # 动量 = 100 × ln(pivot_end / pivot_start)
    if pivot_series[0] <= 0 or pivot_series[-1] <= 0:
        return np.nan
    momentum = 100.0 * np.log(pivot_series[-1] / pivot_series[0])
    
    # 效率系数 = |起点到终点的直线距离| / 路径总长度
    log_pivots = np.log(pivot_series)
    direction = abs(log_pivots[-1] - log_pivots[0])
    volatility = np.sum(np.abs(np.diff(log_pivots)))
    
    if volatility <= 0:
        return np.nan
    
    efficiency_ratio = direction / volatility  # 范围 [0, 1]
    
    # 最终得分
    score = momentum * efficiency_ratio
    return score


def calculate_all_etf_scores(context, etf_pool):
    """
    计算候选池所有ETF的效率动量得分
    
    参数:
        context: PTrade上下文
        etf_pool: ETF代码列表
    
    返回:
        list: [(etf_code, score), ...] 按得分从高到低排序，已过滤NaN
    """
    # 获取历史数据（需要 N_DAYS+1 根K线来计算 N_DAYS 的动量）
    hist = get_history(N_DAYS + 1, '1d', ['open', 'high', 'low', 'close'], etf_pool, fq='pre')
    
    if hist is None or hist.empty:
        log.error("获取历史数据失败")
        return []
    
    scores = []
    
    for etf in etf_pool:
        try:
            df = hist[hist['code'] == etf]
            if df is None or len(df) < N_DAYS + 1:
                continue
            
            # 计算 Pivot = (Open + High + Low + Close) / 4
            pivot = (df['open'].values + df['high'].values + df['low'].values + df['close'].values) / 4.0
            
            score = calculate_efficiency_momentum_score(pivot)
            
            if not np.isnan(score):
                scores.append((etf, score))
                log.info(f'  [{etf}] 得分: {score:.4f}')
        except Exception as e:
            log.error(f'  [{etf}] 计算异常: {e}')
    
    # 按得分从高到低排序
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores


# ============================================================
# PTrade 标准接口
# ============================================================

def initialize(context):
    """
    策略初始化函数，只在策略启动时运行一次
    """
    # 回测设置
    if not is_trade():
        set_commission(commission_ratio=COMMISSION_RATIO, min_commission=MIN_COMMISSION, type='ETF')
        set_commission(commission_ratio=COMMISSION_RATIO, min_commission=MIN_COMMISSION, type='LOF')
        set_limit_mode(limit_mode='UNLIMITED')
    
    # 全局变量
    g.etf_pool = _get_etf_pool()
    g.target_etfs = []       # 当日目标ETF列表
    g.trade_count = 0        # 交易计数
    
    log.info("=" * 60)
    log.info("ETF效率动量轮动策略 - PTrade回测版")
    log.info(f"ETF池模式: {ETF_POOL_MODE} ({len(g.etf_pool)}只)")
    log.info(f"动量窗口: {N_DAYS}天, 选入数量: {SELECT_NUM}")
    log.info(f"空仓机制: {'开启' if ENABLE_EMPTY_POSITION else '关闭'}")
    log.info("=" * 60)


def before_trading_start(context, data):
    """
    盘前执行：计算ETF得分，确定今日目标持仓
    """
    today = context.current_dt.strftime('%Y-%m-%d')
    log.info(f"{'=' * 50}")
    log.info(f"📅 {today} 盘前分析")
    
    # 计算所有ETF的效率动量得分
    scored_etfs = calculate_all_etf_scores(context, g.etf_pool)
    
    if not scored_etfs:
        log.info("⚠️ 无有效ETF得分，今日空仓")
        g.target_etfs = []
        return
    
    # 打印排名前10
    log.info(f"\n📊 得分排名 TOP10:")
    for i, (etf, score) in enumerate(scored_etfs[:10]):
        marker = "🏆" if i < SELECT_NUM else "  "
        log.info(f"  {marker} #{i+1} {etf}: {score:.4f}")
    
    # 选择得分最高的 SELECT_NUM 只
    top_etfs = scored_etfs[:SELECT_NUM]
    
    # 空仓机制：如果最高得分为负，说明所有ETF都在下跌
    if ENABLE_EMPTY_POSITION and top_etfs[0][1] <= 0:
        log.info(f"⚠️ 最高得分 {top_etfs[0][1]:.4f} ≤ 0，所有ETF趋势向下，今日空仓")
        g.target_etfs = []
    else:
        g.target_etfs = [etf for etf, score in top_etfs]
        log.info(f"\n🎯 今日目标: {g.target_etfs}")
    
    # 如果不启用空仓且得分全负，使用防御ETF
    if not ENABLE_EMPTY_POSITION and not g.target_etfs:
        g.target_etfs = [DEFENSIVE_ETF]
        log.info(f"🛡️ 切换防御模式: {DEFENSIVE_ETF}")


def handle_data(context, data):
    """
    盘中执行：在指定时间执行卖出和买入
    """
    current_time = context.current_dt.strftime('%H:%M')
    
    # ---- 卖出阶段 ----
    if current_time == SELL_TIME:
        _execute_sell(context)
    
    # ---- 买入阶段 ----
    if current_time == BUY_TIME:
        _execute_buy(context)


def after_trading_end(context, data):
    """
    盘后执行：记录当日持仓和收益
    """
    g.trade_count += 1
    
    # 统计当前持仓
    positions = context.portfolio.positions
    hold_list = []
    for stock, pos in positions.items():
        if pos.amount > 0:
            hold_list.append(stock)
    
    total_value = context.portfolio.portfolio_value
    cash = context.portfolio.cash
    
    log.info(f"📊 盘后统计: 总资产={total_value:.2f}, 现金={cash:.2f}, "
             f"持仓={hold_list if hold_list else '空仓'}")


# ============================================================
# 交易执行函数
# ============================================================

def _execute_sell(context):
    """执行卖出操作"""
    log.info("========== 卖出操作开始 ==========")
    
    target_set = set(g.target_etfs)
    positions = context.portfolio.positions
    
    for security in list(positions.keys()):
        position = positions[security]
        if position.amount <= 0:
            continue
        
        if security not in target_set:
            log.info(f"📤 卖出: {security}, 持仓数量: {position.amount}")
            order_target_value(security, 0)
    
    log.info("========== 卖出操作完成 ==========")


def _execute_buy(context):
    """执行买入操作"""
    log.info("========== 买入操作开始 ==========")
    
    target_etfs = g.target_etfs
    if not target_etfs:
        log.info("今日无目标ETF，保持空仓")
        log.info("========== 买入操作完成 ==========")
        return
    
    # 计算可投资金额（留1%缓冲）
    total_value = context.portfolio.portfolio_value
    cash_buffer_ratio = 0.01
    investable_value = total_value * (1 - cash_buffer_ratio)
    
    # 等权分配
    target_value_per_etf = investable_value / len(target_etfs)
    
    log.info(f"总资产: {total_value:.2f}, 可投资: {investable_value:.2f}, "
             f"目标数量: {len(target_etfs)}, 单只目标: {target_value_per_etf:.2f}")
    
    for etf in target_etfs:
        # 检查当前持仓
        current_value = 0
        if etf in context.portfolio.positions:
            pos = context.portfolio.positions[etf]
            if pos.amount > 0:
                current_value = pos.amount * pos.last_sale_price
        
        # 如果已持有且金额接近目标（差异<5%），跳过
        if current_value > 0 and abs(current_value - target_value_per_etf) / target_value_per_etf < 0.05:
            log.info(f"✅ 继续持有: {etf}, 当前市值: {current_value:.2f}")
            continue
        
        log.info(f"📦 买入/调仓: {etf}, 目标金额: {target_value_per_etf:.2f}")
        order_target_value(etf, target_value_per_etf)
    
    log.info("========== 买入操作完成 ==========")
