from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import time
import math
import pickle

# ============================策略配置（个性化设置）==================================
strategy_config = {                  
    # 哈利布朗策略 - 一种基于资产配置的策略
    'hlbl_strategy': {                  
        'enabled': True,                # 策略启用标志
        'account_ratio': 0.5,           # 策略权重（占总资金的比例）
        'params': {
            'period'    : 22,           # 调仓周期（天）
            'trade_time': ["09:29", "09:33"],  # 交易时间 [卖出时间, 买入时间]
        }
    },
    # 桥水基金全天候策略 - 基于风险平价的资产配置策略
    'all_weather': {                        
        'enabled': True,                 # 策略启用标志
        'account_ratio': 0.5,            # 策略权重（占总资金的比例）
        'params': { 
            'period'    : 22,            # 调仓周期（天）
            'raise_rate': 0.3,           # 触发动态调仓的涨幅阈值
            'trade_time': ["09:30", "09:35"],  # 交易时间 [卖出时间, 买入时间]
        }
    },
    # 三马-ETF轮动策略 - 基于动量等多因子的ETF轮动策略
    'etf_rotation': {
        'enabled': False,                 # 策略启用标志
        'account_ratio': 0.2,            # 策略权重（占总资金的比例）
        'params': {
            'trade_time': ["10:35", "14:40"],  # 交易时间 [卖出时间, 买入时间]
            'm_days': 25,                 # 计算动量的天数
            'm_score': 5,                 # 最低排名分数要求
            'enable_stop_loss_by_cur_day': True,  # 是否启用日内止损
            'stoploss_limit_by_cur_day': -0.03,   # 日内止损阈值
            'holding_count': 1,           # 持仓数量（可设置为1, 2, 3等）
            'enable_buy_delay': True,   # 是否启用延迟买入（涨幅超过阈值时延迟到下午买入）
            'buy_delay_threshold': 0.05,  # 涨幅超过此阈值时延迟买入（0.09=9%）
            'buy_delay_time': "14:50",    # 延迟买入时间
            'etf_pool': [                 # ETF候选池
                "510180.SS",  # 上证180ETF
                "513030.SS",  # 德国30ETF
                "513100.SS",  # 纳指ETF
                "513520.SS",  # 日经225ETF
                "510410.SS",  # 资源ETF
                "518880.SS",  # 黄金ETF
                "501018.SS",  # 南方原油
                "159985.SZ",  # 豆粕ETF
                "511090.SS",  # 30年国债
                "159915.SZ",  # 创业板ETF
                "588120.SS",  # 科创100ETF
                "512480.SS",  # 半导体ETF
                "159851.SZ",  # 金科ETF
                "513020.SS",  # 香港科技ETF
                "159637.SZ",  # 新能源龙头ETF
                "513690.SS",  # 恒生股息ETF
                "510050.SS"   # 50ETF
            ]
        }
    },
    # 三马-小市值策略 - 基于小市值股票的投资策略
    'small_market_cap': {
        'enabled': False,                # 策略启用标志
        'account_ratio': 1.0,            # 策略权重（占总资金的比例）
        'params': {
            'xsz_version': "v3",         # 小市值策略版本（v1/v2/v3）
            'enable_dynamic_stock_num': True,  # 是否启用动态股票数量
            'xsz_stock_num': 5,            # 持仓股票数量
            'run_stoploss': True,          # 是否启用止损
            'stoploss_strategy': 3,        # 止损策略类型
            'stoploss_limit': 0.09,        # 止损阈值
            'stoploss_market': 0.05,       # 市场下跌止损阈值
            'DBL_control': True,           # 是否启用顶背离控制
            'check_dbl_days': 10,          # 顶背离检测天数
            'take_profit_ratio': 0.5,      # 止盈比例
            'trade_time': ["09:33", "09:40", "10:42", "10:00", "13:30", "14:00", "14:50"],  # 交易时间数组
            'first_run': True,            # 是否首次运行
            'empty_months': [1, 4],       # 空仓期月份配置
            'empty_fund_allocation': 'etf_rotation',  # 空仓时资金分配方式 ('etf_rotation':资金转到ETF轮动 或 'fixed_etf':资金转到下面的标的(xsz_buy_etf))
            'xsz_buy_etf': "511360.SS",    # 空仓ETF代码
            
            # ===== 调仓方式配置（二选一） =====
            # 方式1：固定星期调仓（每周固定几天调仓）
            'rebalance_weekdays': [1],    # 调仓星期列表（0=周一, 1=周二, 2=周三, 3=周四, 4=周五）
                                          # 示例: [1] 表示每周二调仓
                                          # 示例: [0, 3] 表示每周一和周四调仓
            
            # 方式2：间隔天数调仓（每隔n个交易日调仓）
            # 'rebalance_interval': 5,   # 每隔7个交易日调仓一次
                                          # 注意：使用此方式时，需要注释掉 rebalance_weekdays
                                          # 示例: 5 表示每5个交易日调仓一次（大约每周）
                                          # 示例: 10 表示每10个交易日调仓一次（大约每两周）
        }
    },
    # 五福闹新春策略 - 基于动量、R²、成交量等多因子的ETF轮动策略
    'wu_fu_strategy': {
        'enabled': False,                # 策略启用标志
        'account_ratio': 0.2,           # 策略权重（占总资金的比例）
        'params': {
            'trade_time': ["13:10", "13:15"],  # 交易时间 [卖出时间, 买入时间]
            'enable_strategy_isolation': True,  # 是否启用策略隔离（过滤与其他策略冲突的ETF）
            
            # ===== 五福策略参数 =====
            'holdings_num': 1,           # 同时持有ETF数量
            'defensive_etf': "159650.SZ",  # 防御性ETF代码
            'safe_haven_etf': '511660.SS',  # 避险ETF代码
            'min_money': 5000,          # 最小交易金额
            
            'lookback_days': 25,        # 动量计算回看天数
            'min_score_threshold': 0.5,  # 动量得分下限
            'max_score_threshold': 5,    # 动量得分上限
            
            'use_short_momentum_filter': False,   # 禁用短期动量过滤器
            'short_lookback_days': 10,           # 短期动量回看天数
            'short_momentum_threshold': 0.0,     # 短期动量阈值
            
            'enable_r2_filter': True,   # 启用R²稳定性过滤器
            'r2_threshold': 0.4,        # R²阈值
            
            'enable_annualized_return_filter': False,  # 禁用年化收益率过滤器
            'min_annualized_return': 1.0,  # 年化收益率阈值
            
            'enable_ma_filter': False,   # 禁用均线过滤器
            'ma_filter_days': 20,       # 均线周期
            
            'enable_volume_check': True,  # 启用成交量过滤器
            'volume_lookback': 5,        # 成交量回看天数
            'volume_threshold': 1.0,    # 成交量比值阈值
            
            'enable_loss_filter': True,  # 启用短期跌幅风控过滤器
            'loss': 0.97,              # 单日最大允许跌幅
            
            'use_rsi_filter': False,    # 禁用RSI超买过滤器
            'rsi_period': 6,           # RSI计算周期
            'rsi_lookback_days': 1,    # RSI回看天数
            'rsi_threshold': 98,        # RSI超买阈值
            
            'use_fixed_stop_loss': True,   # 启用固定比例止损
            'fixedStopLossThreshold': 0.95,  # 固定止损比例
            'use_pct_stop_loss': False,     # 禁用当日跌幅止损
            'pct_stop_loss_threshold': 0.95,  # 当日跌幅止损比例
            'use_atr_stop_loss': False,     # 禁用ATR动态止损
            'atr_period': 14,               # ATR计算周期
            'atr_multiplier': 2,           # ATR倍数
            'atr_trailing_stop': True,      # ATR使用追踪止损模式
            'atr_exclude_defensive': True,  # ATR止损排除防御性ETF
            
            'sell_cooldown_enabled': False,  # 禁用卖出冷却期机制
            'sell_cooldown_days': 3,        # 冷却期天数
        }
    },
    # 热点小市值策略 - 基于热点概念和小市值股票的投资策略
    'hot_spot_small_cap': {
        'enabled': False,               # 策略启用标志
        'account_ratio': 0.2,          # 策略权重（占总资金的比例）
        'params': {
            'stock_num': 10,            # 持仓股票数量
            'run_stoploss': True,       # 是否启用止损
            'stoploss_strategy': 3,     # 止损策略类型（1:个股止损 2:大盘止损 3:联合止损）
            'stoploss_limit': 0.94,     # 个股止损阈值（成本价 × 0.94）
            'stoploss_market': 0.97,    # 大盘止损参数
            'HV_control': False,        # 是否启用成交量异常检测
            'HV_duration': 120,         # 检查成交量时参考的历史天数
            'HV_ratio': 0.9,            # 当天成交量超过历史最高成交量的比例
            'pass_april': True,         # 是否在04月或01月期间执行空仓策略
            'trade_time': ["10:00", "10:30",  "10:35", "14:30", "14:35", "14:50"],  # 交易时间数组 [止损时间, 调仓卖出时间, 调仓买入时间, 检查时间, 补仓时间, 清仓时间]
            'rebalance_weekdays': [1],  # 调仓星期列表（0=周一, 1=周二, 2=周三, 3=周四, 4=周五）
            'empty_fund_allocation': 'etf_rotation'    # 空仓时资金分配方式 ('etf_rotation':资金转到ETF轮动 或 'None':空仓)
        }
    },
    # 基于XL指标的ETF轮动策略
    'xl_strategy': {
        'enabled': False,               # 策略启用标志
        'account_ratio': 0.2,            # 策略权重（占总资金的比例）
        'params': {
            'hold_count': 5,            # 持仓数量
            'commission_ratio': 0.0001,  # 佣金比例
            'min_commission': 0.001,     # 最小佣金
            'sell_time': '09:35',       # 卖出时间
            'buy_time': '09:40',        # 买入时间
            'history_days': 60,         # 历史数据天数
            'xl_param_n': 26,           # XL参数N
            'xl_param_zsbl': 2.6,       # XL参数ZSBL
            'xl_param_fxxs': 0.02,      # XL参数FXXS
            'xl_param_atrn': 26,        # XL参数ATRN
            'xl_filter_min': 0,         # XL值最小值过滤
            'xl_filter_max': None,      # XL值最大值过滤（None表示不限制）
        }
    },
    # 国债逆回购策略 - 现金管理策略
    'ipo_repo_strategy': {
        'enabled': False,                 # 策略启用标志
        'account_ratio': 1.0,            # 策略权重（占总资金的比例）
        'params': {
            'reserve_cash': 0.0           # 保留现金数额（元）
        }
    },
    # 自动撤单重提策略 - 优化交易执行的辅助策略
    'auto_cancel_retry': {
        'enabled': True,                 # 策略启用标志
        'account_ratio': 0.3,            # 策略权重（占总资金的比例）
        'params': {
            'unorder_time': 30            # 未成交撤单重提时间（秒）
        }
    }
}

# 非策略配置
other_config = {
    "real_trading_cash_ratio": 1.0,                         # 实盘资金使用比例
    "email_notification":{                                  # 邮件发送配置
        'enabled': False,                                    # 发送邮件开关
        'sender_email': '',                # 发送方的邮箱地址
        'receiver_emails': [''],           # 接收方的邮箱地址
        'smtp_password': '',                # 邮箱的smtp授权码，注意，不是邮箱密码，必填字段(str)
        'subject': "多策略回测实盘框架liyr"                  # 邮件主题 
    },
    "order_config":{                    # 下单配置
        "max_order_value": 200000,      # 单笔买入订单最大金额，默认20万元（买入金额高于这个金额进行拆单）
        "min_order_value": 2000,        # 单笔买入订单最小金额，默认2000元(低于这个金额不下单)
        "max_order_amount": 1000000     # 单笔卖出最大数量（超过这个数量进行拆单）
    }
}

# ============================策略基类==================================
class BaseStrategy:
    """
    所有量化策略的基类
    提供标准生命周期钩子函数
    """

    def __init__(self, name="基础策略", version="v1.0"):
        self.name = name
        self.version = version

    def before_trading_start(self, context, data=None):
        """
        每日开盘前执行（如9:15~9:25之间）
        可用于：数据预处理、调仓计划生成等
        """
        pass

    def handle_data(self, context, data=None):
        """
        盘中实时处理函数（每分钟或每日触发）
        """
        pass

    def after_trading_end(self, context, data=None):
        """
        每日收盘后执行（如15:05）
        可用于：统计、保存状态、发邮件等
        """
        pass
    
    # ============ 新增：带策略名的日志方法 ============
    def my_log(self, message, level="info", force_print=True):
        """打印带策略名称的信息日志
        Args:
            message: 日志消息
            level: 日志级别 ("info", "warn", "error")
            force_print: 是否强制打印，即使在实盘模式下也打印
        """
        # 回测时全打印，实盘时只有强制打印或错误级别才打印
        if not is_trade() or force_print or level in ["error", "warn"]:
            formatted = f"[{self.name}] {message}"
            if level == "warn":
                log.warning(formatted)
            elif level == "error":
                log.error(formatted)
            else:  # 默认 info
                log.info(formatted)
    
    def split_quantity_for_orders(self, current_amount, max_order_amount):
        """
        将数量拆分为多个部分，确保每部分（除最后一部分外）都是100的整数倍且不超过最大下单数量
        
        Args:
            current_amount: 待拆分数量
            max_order_amount: 最大下单数量
            
        Returns:
            list: 包含每笔订单数量的列表
        """
        current_amount = int(current_amount)
        abs_amount = abs(current_amount)
        if abs_amount == 0:
            return []
        
        sign = 1 if current_amount > 0 else -1
        
        # 确保单笔最大数量是100的倍数，最小为100
        safe_max_amount = (max_order_amount // 100) * 100
        if safe_max_amount == 0:
            safe_max_amount = 100
            
        order_amounts = []
        remaining = abs_amount
        
        # 贪婪拆分：尽可能使用 safe_max_amount，确保除最后一笔外都是100的整数倍且不超限
        while remaining > safe_max_amount:
            order_amounts.append(sign * safe_max_amount)
            remaining -= safe_max_amount
            
        # 最后一笔处理剩余数量
        if remaining > 0:
            # 最后一笔订单。注意：对于清仓，remaining 可能包含碎股；对于减仓，外层逻辑已确保其为100的倍数。
            order_amounts.append(sign * remaining)
                
        return order_amounts
    
    def order_target_value_with_split(self, context, security, target_value):
        """
        带资金拆分的order_target_value函数
        当需要交易的金额超过设定阈值时，自动拆分订单
        当需要交易的金额小于最小阈值时，不下单
        
        Args:
            security: 证券代码
            target_value: 目标持仓金额
        """
        max_order_value = other_config["order_config"]["max_order_value"]
        min_order_value = other_config["order_config"]["min_order_value"]
        max_order_amount = other_config["order_config"]["max_order_amount"]
        try:
            # 获取当前持仓金额
            current_value = 0
            current_amount = 0
            if security.split(".")[-1] not in ["SS","SZ"]:
                convert_map = {"XSHG":"SS","XSHE":"SZ"}
                security  = security.split(".")[0] + "." + convert_map[security.split(".")[1]]
            if security in context.portfolio.positions:
                pos = context.portfolio.positions[security]
                current_value = pos.last_sale_price * pos.amount
                current_amount = pos.amount
            
            # 处理卖出（清仓）
            if target_value == 0:
                if current_amount == 0:
                    return
                # 清仓逻辑：使用 max_order_amount 拆分股数
                order_amounts = self.split_quantity_for_orders(current_amount, max_order_amount)
                self.my_log(f"清仓拆分订单: {security}, 总持仓数量 {current_amount}, 拆分为 {len(order_amounts)} 笔, 每笔具体数量 {order_amounts}", force_print=True)
                for order_amount in order_amounts:
                    order(security, -1 * order_amount)
            else:
                # 计算需要交易的金额
                trade_value = target_value - current_value
                self.my_log(f"security:{security},target_value:{target_value},trade_value:{trade_value}")
                
                # 检查交易金额是否小于最小下单金额
                if abs(trade_value) < min_order_value:
                    self.my_log(f"交易金额太小: {security},当前持仓金额:{current_value} 交易金额 {trade_value:.2f}, 最小单笔资金限制 {min_order_value:.2f}, 跳过下单", force_print=True)
                    return
                
                # --- 核心修改：处理卖出（减仓）情况，确保遵守100股规则 ---
                if trade_value < 0:
                    # 计算需要卖出的股数，并向上取整到100的倍数（非清仓卖出必须是100的整数倍）
                    trade_amount = int(abs(trade_value) / pos.last_sale_price)
                    trade_amount = (trade_amount // 100) * 100
                                    
                    if trade_amount == 0:
                        self.my_log(f"卖出股数不足100股: {security}, 估算金额 {abs(trade_value):.2f}, 跳过下单", force_print=True)
                        return
                
                    # 统一使用拆单逻辑处理卖出，确保每笔订单均符合100股规则（除可能的清仓碎股外）
                    order_amounts = self.split_quantity_for_orders(trade_amount, max_order_amount)
                    self.my_log(f"卖出执行: {security}, 总卖出股数 {trade_amount}, 拆分为 {len(order_amounts)} 笔, 每笔具体数量 {order_amounts}", force_print=True)
                    for i, oa in enumerate(order_amounts):
                        order(security, -1 * oa)
                        if is_trade() and len(order_amounts) > 1: time.sleep(0.2)
                    return
                
                # --- 买入逻辑（加仓） ---
                # 获取当前可用现金
                available_cash = context.portfolio.cash
                
                # 如果交易金额超过可用现金，调整为可用现金
                if trade_value > available_cash:
                    self.my_log(f"⚠️ 资金不足: {security}, 所需交易金额 {trade_value:.2f}, 可用现金 {available_cash:.2f}, 调整为可用现金", 'warn')
                    trade_value = available_cash
                
                # 如果调整后交易金额仍然太小，跳过
                if trade_value < min_order_value:
                    self.my_log(f"交易金额太小: {security},当前持仓金额:{current_value} 交易金额 {trade_value:.2f}, 最小单笔资金限制 {min_order_value:.2f}, 跳过下单", force_print=True)
                    return
                
                # 如果交易金额绝对值小于最大订单金额，直接下单
                if trade_value <= max_order_value:
                    self.my_log(f"未达到拆单金额: {security},当前持仓金额:{current_value} 交易金额 {trade_value:.2f}, 最大单笔资金限制 {max_order_value:.2f}", force_print=True)
                    order_value(security, trade_value)
                    return
                
                # 否则拆分订单（买入）
                num_splits = int(abs(trade_value) / max_order_value) + (1 if abs(trade_value) % max_order_value != 0 else 0)
                trade_per_order = trade_value / num_splits
                
                self.my_log(f"拆分买单: {security}, 总交易金额 {trade_value:.2f}, 分为 {num_splits} 笔, 每笔约 {trade_per_order:.2f}", force_print=True)
                
                for i in range(num_splits):
                    if i == num_splits - 1:
                        current_trade_value = trade_value - (trade_per_order * (num_splits - 1))
                    else:
                        current_trade_value = trade_per_order
                    
                    # 每次下单前检查可用现金
                    current_available_cash = context.portfolio.cash
                    if current_trade_value > current_available_cash:
                        self.my_log(f"⚠️ 第{i+1}笔下单资金不足: 计划 {current_trade_value:.2f}, 可用 {current_available_cash:.2f}, 调整为可用现金", 'warn')
                        current_trade_value = current_available_cash
                    
                    if current_trade_value >= min_order_value:
                        order_value(security, current_trade_value)
                        if is_trade(): time.sleep(0.2)
                        self.my_log(f"执行第 {i+1}/{num_splits} 笔买单: 交易金额 {current_trade_value:.2f}", force_print=True)
                    else:
                        self.my_log(f"⚠️ 第{i+1}笔下单金额小于最小限制 {min_order_value:.2f}, 跳过", 'warn')
        except Exception as e:
            self.my_log(f"拆分订单执行失败 {security}: {e}", "error")
            # 如果拆分失败，尝试直接下单
            try:
                order_target_value(security, target_value)
            except Exception as e2:
                self.my_log(f"直接下单也失败 {security}: {e2}", "error")

# ============================哈利布朗策略类=============================
class HlBuLangStrategy(BaseStrategy):
    def __init__(self, account_ratio, trade_time, period):
        """
        :param account_ratio: 本策略使用的账户资金比例 (0.0 ~ 1.0)
        :param trade_time: 交易时间 [卖出时间, 买入时间]
        """
        super().__init__(name="哈利布朗轮动策略", version="v1.0")
        # 资金占比
        self.account_ratio = account_ratio
        
        # 本策略所用资金
        self.capital = 0
        
        # 策略执行时间
        self.trade_time = trade_time
        self.sell_time = trade_time[0]
        self.buy_time = trade_time[1]
        
        # 调仓周期：月度调仓
        self.period = period
        
        # 程序运行日记录
        self.run_count = 0
        
         # 策略池与资金分配
        self.fund_pools = {
            'fund_1': {'159957.SZ': 0.125, '159941.SZ': 0.125, '512890.SS': 0.125, '511260.SS': 0.125},
            'fund_2': {'512890.SS': 0.125, '511260.SS': 0.125},
            'fund_3': {'159934.SZ': 0.25, '510880.SS': 0.25},
            'fund_4': {'511260.SS': 0.25},
            'fund_5': {'511880.SS': 0.25}
        }
        
        # 当前策略标的池代码
        self.cur_strategy_stocks = list({stock for pool in self.fund_pools.values() for stock in pool.keys()})

        # 交易日标志位
        self.is_rebalance_day = False
        self.my_log(f"run_count:{self.run_count},period:{self.period}", force_print=True)

    def before_trading_start(self, context, data=None):
        """盘前：生成目标组合"""
        super().before_trading_start(context, data)
        #尝试启动pickle文件
        if is_trade():
            try: 
                with open(get_research_path() + 'HlBuLangStrategy.pkl','rb') as f:
                    self.run_count = pickle.load(f)
                    self.my_log(f"run_count本地加载成功，run_count:{self.run_count}", force_print=True)
            except FileNotFoundError:
                # 第一次运行，本地文件还未创建，使用默认值0
                self.my_log(f"首次运行，初始化run_count={self.run_count}，将在盘后保存", force_print=True)
            except Exception as e:
                self.my_log(f"run_count本地加载异常: {e}，使用默认值{self.run_count}", 'warn')
        
        # 更新当日交易状态
        self.is_rebalance_day = (self.run_count % self.period in [0, 1])
        self.my_log(f"run_count:{self.run_count},period:{self.period},is_rebalance_day:{self.is_rebalance_day}", force_print=True)
        
        # 如果是调仓日，计算目标持仓标的
        if self.is_rebalance_day:
            check_date = context.current_dt
            macd_300 = self.get_macd_M('000300.SS', check_date)
            macd_100 = self.get_macd_M('159941.SZ', check_date)
            macd_915 = self.get_macd_M('159952.SZ', check_date)
            macd_880 = self.get_macd_M('159934.SZ', check_date)
            zf = self.get_zf(context)
    
            # === 轮动逻辑 ===
            if macd_915 > 0:
                self.stock_fund_1 = '159957.SZ'
            else:
                if macd_100 > 0:
                    self.stock_fund_1 = '159941.SZ'
                else:
                    if zf > -6:
                        self.stock_fund_1 = '512890.SS'
                    else:
                        self.stock_fund_1 = '511260.SS'
    
            if zf > -6:
                self.stock_fund_2 = '512890.SS'
            else:
                self.stock_fund_2 = '511260.SS'
    
            if macd_880 > 0:
                self.stock_fund_3 = '159934.SZ'
            else:
                if macd_300 > 0 and zf > -6:
                    self.stock_fund_3 = '510880.SS'
                else:
                    self.stock_fund_3 = '159934.SZ'
            self.stock_fund_4 = "511260.SS"
            self.stock_fund_5 = "511880.SS"
            self.target_position = [self.stock_fund_1, self.stock_fund_2,
                self.stock_fund_3, self.stock_fund_4, self.stock_fund_5
            ]
            self.my_log(f"✅ 目标持仓: {[s.split('.')[0] for s in self.target_position]}", force_print=True)
        self.run_count += 1
        
    def handle_data(self, context, data=None):
        """盘中执行：分别在卖出时间和买入时间执行操作"""
        current_time = context.blotter.current_dt.strftime("%H:%M")
        
        if not self.is_rebalance_day:
            return
        
        # 卖出阶段
        if current_time == self.sell_time:
            self._execute_sell(context)
        
        # 买入阶段
        if current_time == self.buy_time:
            self._execute_buy(context)

    def after_trading_end(self, context, data=None):
        """
        每日收盘后执行（如15:05）
        可用于：统计、保存状态、发邮件等
        """
        if is_trade():
            try:
                with open(get_research_path() + 'HlBuLangStrategy.pkl', 'wb') as f:
                    pickle.dump(self.run_count, f)
                    self.my_log(f"run_count本地保存成功，run_count:{self.run_count}", force_print=True)
            except:
                self.my_log(f"run_count本地保存失败，run_count:{self.run_count}", force_print=True)

    def _execute_sell(self, context):
        """执行卖出操作"""
        # 获取当前持仓
        current_holdings = self._get_current_holdings(context)
        
        # 更新总资产
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # --- 卖出非目标持仓 ---
        self.my_log(f"🔄 执行卖出逻辑...", force_print=True)
        for stock in current_holdings:
            if stock not in self.target_position:
                self.my_log(f"❌ 卖出: {stock}", force_print=True)
                self.order_target_value_with_split(context, stock, 0)
    
    def _execute_buy(self, context):
        """执行买入操作"""
        # 更新总资产
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # --- 计算各标的应买入金额 ---
        self.my_log(f"🛒 执行买入逻辑...", force_print=True)
        buy_weights = self.calculate_buy_weights()
        total_allocated = 0
        for stock, weight in buy_weights.items():
            target_value = self.capital * weight
            self.my_log(f"✅ 买入: {stock}, 金额: {target_value:.2f}", force_print=True)
            self.order_target_value_with_split(context, stock, target_value)
            total_allocated += weight
        self.my_log(f"📊 总分配比例: {total_allocated:.3f} | 使用资金: {self.capital:.2f}", force_print=True)

    # ===== 工具函数保持不变 =====
    def get_macd_M(self, stock, check_date):
        h = get_history(100, 'mo', ['close'], security_list=stock, include=False, fq="pre")
        close_data = h['close'].values
        _, _, macd_data = get_MACD(close_data, 12, 26, 9)
        return macd_data[-1] if len(macd_data) > 0 else 0

    def get_zf(self, context):
        etf = '510880.SS'
        df_ALL = get_history(245, '1d', ['close'], security_list=etf, include=False, fq="pre")
        if len(df_ALL) < 2:
            return 0
        last_close = df_ALL['close'].values[-1]
        old_year_last_close = df_ALL['close'].values[0]
        return round((last_close - old_year_last_close) * 100 / old_year_last_close, 2)
        
    def calculate_buy_weights(self):
        """
        根据 selected_funds 和 fund_pools 计算最终买入权重
        返回: {stock: total_weight}
        """
        buy_weights = {}
    
        # 定义每一层的选择映射（必须在 before_trading_start 中赋值）
        selected_map = {
            'fund_1': self.stock_fund_1,
            'fund_2': self.stock_fund_2,
            'fund_3': self.stock_fund_3,
            'fund_4': self.stock_fund_4,
            'fund_5': self.stock_fund_5,
        }
    
        for layer_name, chosen_stock in selected_map.items():
            if not chosen_stock:
                continue
            # 获取该层配置
            pool = self.fund_pools.get(layer_name)
            if not pool:
                continue
            if chosen_stock in pool:
                weight = pool[chosen_stock]
                buy_weights[chosen_stock] = buy_weights.get(chosen_stock, 0) + weight
        return buy_weights

    def _get_current_holdings(self, context):
        """获取当前策略持仓"""
        try:
            holdings = []
            # 获取当前所有持仓
            positions = context.portfolio.positions
            # 筛选出当前持仓中属于本策略的股票
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in self.cur_strategy_stocks:
                    holdings.append(stock)
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'error')
            return []
if "hlbl_strategy" in strategy_config:
    strategy_config['hlbl_strategy']['class'] = HlBuLangStrategy

# ============================桥水基金全天候策略类=======================
class AllWeatherStrategy(BaseStrategy):
    """
    桥水基金全天候策略（Risk Parity 思想）
    支持：上市日期过滤、大类资产轮动、阈值触发再平衡
    """

    def __init__(self, account_ratio=1.0, period=22, raise_rate=0.3, trade_time=["09:33", "09:34"]):
        super().__init__(name="全天候策略", version="v1.0")
        
        # 资金配置
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.capital = 0  # 运行时更新

        # 时间控制
        self.trade_time = trade_time
        self.sell_time = trade_time[0]    # 卖出时间
        self.buy_time = trade_time[1]     # 买入时间
        self.period = period              # 固定周期（交易日数）
        self.raise_rate = max(0.0, raise_rate)  # 触发动态调仓的涨幅阈值

        # 运行计数器
        self.run_count = 0

        # 是否需要调仓标志
        self.should_rebalance = False

        # 大类资产池定义
        self.asset_pool = {
            'stock': {
                'rate': 0.3,
                # 'codes': ['510310.SS','513100.SS','513500.SS']
                'codes': ['510310.SS','159660.SZ','513500.SS']
            },
            'mid_bond': {
                'rate': 0.55,
                'codes': ['511010.SS']
            },
            # 'long_bond': {
                # 'rate': 0.4,
                # # 'codes': ['511260.SS']        # 和哈利布朗重复，采用替代标的
            # },
            'gold': {
                'rate': 0.075,
                'codes': ['518850.SS']
            },
            'goods': {
                'rate': 0.075,
                'codes': ['165513.SZ']
            }
        }

        # 构建股票到资产映射
        self.stock_to_asset = self._build_stock_map()

        # 所有可能涉及的交易标的
        self.cur_strategy_stocks =  [code for asset in self.asset_pool.values() for code in asset['codes']]

        # 当前和目标资产配置（用于 rebalance 判断）
        self.current_asset_alloc = {}
        self.target_asset_alloc = {}
        self.rebalanced_asset_values = {}
        self.rebalanced_stock_values = {}
        self.my_log(f"run_count:{self.run_count},period:{self.period}", force_print=True)

    def _build_stock_map(self):
        """构建 {code -> asset} 映射"""
        mapping = {}
        for asset_name, info in self.asset_pool.items():
            for code_dict_list in info['codes']:
                for code in code_dict_list:
                    mapping[code] = asset_name
        return mapping

    def before_trading_start(self, context, data=None):
        """盘前生成目标配置"""
        super().before_trading_start(context, data)
        
        # 更新资金
        if not is_trade(): 
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital =g.real_trading_cash_use * self.account_ratio
        
        
        #尝试启动pickle文件
        if is_trade():
            try: 
                with open(get_research_path() + 'AllWeatherStrategy.pkl','rb') as f:
                    self.run_count = pickle.load(f)
                    self.my_log(f"run_count本地加载成功，run_count:{self.run_count}", force_print=True)
            except FileNotFoundError:
                # 第一次运行，本地文件还未创建，使用默认值0
                self.my_log(f"首次运行，初始化run_count={self.run_count}，将在盘后保存", force_print=True)
            except Exception as e:
                self.my_log(f"run_count本地加载异常: {e}，使用默认值{self.run_count}", 'warn')

        # 判断是否应调仓
        is_periodic = (self.run_count % self.period in [0, 1])
        self.my_log(f"run_count:{self.run_count},period:{self.period},is_periodic:{is_periodic}", force_print=True)
        is_deviation = False

        if self.raise_rate > 0 and self.run_count > 0:
            max_raise = self.calc_asset_max_raise(context)
            is_deviation = max_raise > self.raise_rate

        self.should_rebalance = is_periodic or is_deviation

        if self.should_rebalance:
            self.target_asset_alloc = self.get_trade_target(context)
            self.my_log(f"✅ 触发调仓 | 原因: {'周期' if is_periodic else '偏离'}")
            self.my_log(f"🎯 目标配置: {self.format_alloc_summary(self.target_asset_alloc)}", force_print=True)

        self.run_count += 1

    def get_trade_target(self, context):
        """根据当前时间选择每类资产中最合适的ETF"""
        dt = context.current_dt
        target = {}

        for asset_name, config in self.asset_pool.items():
            selected_codes = config['codes']
            target[asset_name] = {
                'rate': config['rate'],
                'codes': selected_codes
            }
        return target

    def calc_asset_max_raise(self, context):
        """计算各大类资产最大增值比例"""
        current_values = {}

        # 统计当前各资产市值
        for sid, pos in context.portfolio.positions.items():
            asset = self.stock_to_asset.get(sid)
            if not asset:
                continue
            value = pos.value
            current_values[asset] = current_values.get(asset, 0) + value

        max_raise = 0.0
        for asset, prev_value in self.rebalanced_asset_values.items():
            curr_value = current_values.get(asset, 0)
            if prev_value > 0:
                ratio = curr_value / prev_value - 1
                if ratio > max_raise:
                    max_raise = ratio

        return max_raise

    def handle_data(self, context, data=None):
        """盘中执行：分别在卖出时间和买入时间执行操作"""
        current_time = context.blotter.current_dt.strftime("%H:%M")
        
        if not self.should_rebalance:
            return
        
        # 卖出阶段
        if current_time == self.sell_time:
            self._execute_sell(context)
        
        # 买入阶段
        if current_time == self.buy_time:
            self._execute_buy(context)

    def after_trading_end(self, context, data=None):
        """
        每日收盘后执行（如15:05）
        可用于：统计、保存状态、发邮件等
        """
        if is_trade():
            try:
                with open(get_research_path() + 'AllWeatherStrategy.pkl', 'wb') as f:
                    pickle.dump(self.run_count, f)
                    self.my_log(f"run_count本地保存成功，run_count:{self.run_count}", force_print=True)
            except:
                self.my_log(f"run_count本地保存失败，run_count:{self.run_count}", force_print=True)

    def _execute_sell(self, context):
        """执行卖出操作"""
        self.my_log("🔄 开始执行全天候策略卖出...", force_print=True)
        
        # 更新资金
        if not is_trade(): 
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio

        # 获取当前持仓分类
        current_holdings = self.stat_cur_asset(context)

        # 卖出不属于新组合的标的
        for asset, old_codes in current_holdings.items():
            new_codes = self.target_asset_alloc.get(asset, {}).get('codes', [])
            to_sell = set(old_codes) - set(new_codes)
            for code in to_sell:
                self.my_log(f"❌ 卖出: {code}", force_print=True)
                self.order_target_value_with_split(context, code, 0)
    
    def _execute_buy(self, context):
        """执行买入操作"""
        self.my_log("🛒 开始执行全天候策略买入...", force_print=True)
        
        # 更新资金
        if not is_trade(): 
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # 计算目标资产价值
        new_asset_values = {}
        for asset, alloc in self.target_asset_alloc.items():
            new_asset_values[asset] = self.capital * alloc['rate']

        # 买入新配置
        self.rebalanced_stock_values.clear()
        for asset, info in self.target_asset_alloc.items():
            codes = info['codes']
            num = len(codes)
            if num == 0:
                continue
            per_value = new_asset_values[asset] / num
            for code in codes:
                self.my_log(f"✅ 买入: {code}, 金额: {per_value:.2f}", force_print=True)
                self.order_target_value_with_split(context, code, per_value)
                self.rebalanced_stock_values[code] = per_value

        # 更新全局状态
        self.rebalanced_asset_values = new_asset_values
        self.current_asset_alloc = self.target_asset_alloc.copy()

        self.my_log(f"📈 再平衡完成 | 使用资金: {self.capital:.2f}", force_print=True)

    def stat_cur_asset(self, context):
        """统计当前各资产下的持仓代码"""
        result = {}
        for sid in context.portfolio.positions:
            if sid not in self.stock_to_asset:
                continue
            asset = self.stock_to_asset[sid]
            if asset not in result:
                result[asset] = []
            result[asset].append(sid)
        return result

    def format_alloc_summary(self, alloc):
        """格式化输出资产分配摘要"""
        return {k: f"{v['rate']:.1%}" for k, v in alloc.items()}

    def _get_current_holdings(self, context):
        """获取当前策略持仓"""
        try:
            holdings = []
            # 获取当前所有持仓
            positions = context.portfolio.positions
            
            # 筛选出当前持仓中属于本策略的股票
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in self.cur_strategy_stocks:
                    holdings.append(stock)
            
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'error')
            return []
if "all_weather" in strategy_config:
    strategy_config['all_weather']['class'] = AllWeatherStrategy

# ============================三马-ETF轮动策略类=============================
class EtfRotationStrategy(BaseStrategy):
    """
    ETF轮动策略（源自三马v10.2）
    - 基于动量、RSRS、成交量、RSI等多因子筛选
    - 支持日内止损、上市日期过滤
    """

    def __init__(self, account_ratio=1.0, trade_time=["10:35","10:37"],
                 m_days=25, m_score=5, enable_stop_loss_by_cur_day=True, stoploss_limit_by_cur_day=-0.03,
                 holding_count=1, etf_pool=None, enable_buy_delay=False, buy_delay_threshold=0.09, buy_delay_time="14:50"):
        super().__init__(name="三马ETF轮动策略", version="v1.0")
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.trade_time = trade_time
        self.sell_time = trade_time[0]
        self.buy_time = trade_time[1]
        self.capital = 0

        # 策略参数（可配置）
        self.m_days = m_days
        self.m_score = m_score
        self.enable_stop_loss_by_cur_day = enable_stop_loss_by_cur_day
        self.stoploss_limit_by_cur_day = stoploss_limit_by_cur_day
        self.holding_count = holding_count  # 持仓数量
        
        # 涨幅延迟买入参数
        self.enable_buy_delay = enable_buy_delay  # 是否启用延迟买入
        self.buy_delay_threshold = buy_delay_threshold  # 涨幅超过此阈值时延迟买入
        self.buy_delay_time = buy_delay_time  # 延迟买入时间
        self.delayed_buy_etfs = []  # 需要延迟买入的ETF列表
        self.min_money = 5000  # 最小交易金额

        # ETF池（从配置文件读取，如果未提供则使用默认值）
        if etf_pool is None:
            self.etf_pool = [
                "510180.SS", "513030.SS", "513100.SS", "513520.SS",
                "510410.SS", "518880.SS", "501018.SS", "159985.SZ", "511090.SS",
                "159915.SZ", "588120.SS", "512480.SS", "159851.SZ", '513020.SS',
                "159637.SZ", "513690.SS", "510050.SS"
            ]
        else:
            self.etf_pool = etf_pool
        self.cur_strategy_stocks = self.etf_pool[:]  # 所有可能交易的标的
        self.target_etfs = []  # 目标ETF列表
        self.current_etfs = []  # 当前持有的ETF列表

        # 日内止损监控
        self.holdings_for_stop_loss = []

    def before_trading_start(self, context, data=None):
        """盘前：过滤未上市ETF"""
        super().before_trading_start(context, data)
        listed_security = []
        # 获取证券基本信息（包含上市日期）
        stock_info = get_stock_info(self.etf_pool, field=['listed_date'])
        for security in self.etf_pool:
            try:
                # 获取上市日期（ptrade返回格式为"YYYY-MM-DD"字符串）
                listed_date_str = stock_info[security]['listed_date']
                if not listed_date_str:  # 若未获取到上市日期，视为未上市
                    self.my_log(f"❌ {security} 未获取到上市日期，排除",'warn')
                    continue
                
                # 转换为日期类型
                listed_date = datetime.strptime(listed_date_str, "%Y-%m-%d").date()
                current_date = context.current_dt.date()
                
                # 保留“当前时间已上市”的标的（上市日期 ≤ 当前回测日期）
                if listed_date <= current_date:
                    listed_security.append(security)
                else:
                    self.my_log(f"❌ {security} 未上市（上市日：{listed_date_str}），当前回测日：{current_date}，排除",'warn')
            except Exception as e:
                self.my_log(f"⚠️  过滤{security}时异常：{str(e)}，排除",'warn')
                continue
        self.etf_pool_filtered = listed_security
        self.my_log(f"✅ 有效ETF池数量: {len(self.etf_pool_filtered)}", force_print=True)

    def handle_data(self, context, data=None):
        """盘中：在指定时间执行卖出/买入/止损"""
        current_time = context.current_dt.strftime("%H:%M")

        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio

        # 日内止损检查
        if self.enable_stop_loss_by_cur_day and current_time in ["10:01", "10:31"]:
            self._check_intraday_stop_loss(context)

        # 卖出阶段：先选，再卖
        if current_time == self.sell_time:
            self.target_etfs = self._select_target_etf(context)  # ← 选股
            self._execute_sell(context)                         # ← 卖出

        # 买入阶段
        if current_time == self.buy_time:
            self._execute_buy(context)                          # ← 买入
        
        # 延迟买入阶段（仅在启用时执行）
        if self.enable_buy_delay and current_time == self.buy_delay_time and self.delayed_buy_etfs:
            self._execute_delayed_buy(context)

    # ==================== 拆分后的三大核心函数 ====================

    def _select_target_etf(self, context):
        """
        【选股】根据多因子筛选，返回目标ETF列表
        不执行任何交易，只做计算和日志
        """
        self.my_log(f"🔍 开始ETF选股... 需要选择 {self.holding_count} 个标的")
        rank_list = self._get_etf_rank(context, self.etf_pool_filtered)
        if not rank_list:
            self.my_log("❌ 无合格ETF通过筛选", "warn")
            return []

        targets = rank_list[:self.holding_count]
        self.my_log(f"🎯 选股结果: {targets}")
        return targets

    def _execute_sell(self, context):
        """
        【卖出】基于当前持仓和 self.target_etfs 决定是否清仓或调仓
        """
        current_holdings = self._get_current_holdings(context)
        self.current_etfs = current_holdings

        self.my_log(f"🔄 执行卖出逻辑...:{self.target_etfs}")
        if not self.target_etfs:
            # 无目标 → 全部清仓
            self.my_log("🗑️ 无目标ETF，清仓所有持仓", "warn")
            for etf in current_holdings:
                self.order_target_value_with_split(context, etf, 0)
            return

        # 有目标
        # 找出当前持仓中不在目标列表中的ETF并卖出
        etfs_to_sell = [etf for etf in current_holdings if etf not in self.target_etfs]
        print("etfs_to_sell:",etfs_to_sell)
        for etf in etfs_to_sell:
            self.my_log(f"🔄 调仓: 卖出 {etf}，不在目标列表中", force_print=True)
            self.order_target_value_with_split(context, etf, 0)
        
        if not current_holdings and self.target_etfs:
            self.my_log(f"🆕 当前无持仓，目标为 {self.target_etfs}", force_print=True)

    def _execute_buy(self, context):
        """
        【买入】如果 self.target_etfs 存在，则买入
        """
        self.my_log(f"🛒 执行买入逻辑...self.target_etfs:{self.target_etfs}")
        
        # 每天首次执行买入前清空延迟买入列表
        self.delayed_buy_etfs = []
        
        if self.target_etfs:
            # 获取小市值策略的trading_signal标志位
            small_market_cap_trading_signal = self._get_small_market_cap_trading_signal(context)
            
            # 计算实际买入金额
            actual_capital = self.capital
            
            # 如果小市值策略空仓且配置为将资金分配给ETF轮动
            if not small_market_cap_trading_signal:
                small_market_cap_strategy = self._get_strategy_by_name("小市值策略")
                if small_market_cap_strategy and hasattr(small_market_cap_strategy, 'account_ratio') and hasattr(small_market_cap_strategy, 'empty_fund_allocation'):
                    # 检查小市值策略的空仓资金分配配置
                    if small_market_cap_strategy.empty_fund_allocation == 'etf_rotation':
                        small_market_cap_ratio = small_market_cap_strategy.account_ratio
                        # 将小市值策略的资金加到ETF策略上
                        if not is_trade():
                            additional_capital = context.portfolio.portfolio_value * small_market_cap_ratio
                        else:
                            additional_capital = g.real_trading_cash_use * small_market_cap_ratio
                        actual_capital = self.capital + additional_capital
                        self.my_log(f"💡 小市值策略空仓，资金分配至ETF轮动: 原金额 {self.capital:.2f} + 追加 {additional_capital:.2f} = {actual_capital:.2f}", force_print=True)
                    else:
                        self.my_log(f"💡 小市值策略空仓，但资金将分配至固定ETF，不增加ETF轮动仓位: {self.capital:.2f}", force_print=True)
            else:
                # 小市值策略恢复交易，ETF使用原始资金
                self.my_log(f"💡ETF使用原始资金: {self.capital:.2f}", force_print=True)
            
            # 计算每个ETF的分配金额
            capital_per_etf = actual_capital / len(self.target_etfs)
            
            # 根据开关决定是否启用延迟买入
            if self.enable_buy_delay:
                # 启用延迟买入：检查涨幅，超过阈值则延迟
                self.my_log(f"🔧 延迟买入功能已启用，阈值: {self.buy_delay_threshold*100:.1f}%")
                immediate_buy_etfs = []
                # 直接获取昨日收盘价和当前价格
                his_day = get_history(1, frequency='1d', field=['close'], security_list=self.target_etfs, fq='pre', include=False)
                his_min = get_history(1, frequency='1m', field=['price'], security_list=self.target_etfs, fq='pre', include=False)
                current_data_all = {}
                for etf in self.target_etfs:
                    current_data = {}
                    stock_his = his_day[his_day['code'] == etf]
                    if not stock_his.empty:
                        current_data['pre_close'] = stock_his['close'].iloc[-1]
                    stock_his2 = his_min[his_min['code'] == etf]
                    if not stock_his2.empty:
                        current_data['last_price'] = stock_his2['price'].iloc[-1]
                    current_data_all[etf] = current_data
                
                for etf in self.target_etfs:
                    cur_data = current_data_all.get(etf, {})
                    if cur_data:
                        pre_close = cur_data.get('pre_close', 0)
                        last_price = cur_data.get('last_price', 0)
                        print(f"etf:{etf},pre_close:{pre_close}, last_price:{last_price}")
                        if pre_close > 0:
                            change_ratio = (last_price - pre_close) / pre_close
                            self.my_log(f"📊 {etf} 今日涨幅: {change_ratio*100:.2f}% (昨日收盘: {pre_close:.3f}, 当前价: {last_price:.3f})")
                            
                            # 如果涨幅超过阈值，加入延迟买入列表
                            if change_ratio >= self.buy_delay_threshold:
                                self.delayed_buy_etfs.append({
                                    'etf': etf,
                                    'target_value': capital_per_etf,
                                    'change_ratio': change_ratio
                                })
                                self.my_log(f"⏳ {etf} 涨幅 {change_ratio*100:.2f}% 超过阈值 {self.buy_delay_threshold*100:.2f}%，延迟到 {self.buy_delay_time} 买入", 'warn')
                                continue
                    
                    immediate_buy_etfs.append(etf)
            else:
                # 关闭延迟买入：直接买入所有目标ETF
                self.my_log(f"🔧 延迟买入功能已关闭，直接买入所有目标ETF")
                immediate_buy_etfs = self.target_etfs[:]
            
            # 执行买入
            for etf in immediate_buy_etfs:
                self.my_log(f"✅ 买入ETF: {etf}, 金额: {capital_per_etf:.2f}", force_print=True)
                self.order_target_value_with_split(context, etf, capital_per_etf)
            
            # 对于不在目标列表中的ETF持仓，清仓
            for etf in self.current_etfs:
                if etf not in self.target_etfs:
                    self.my_log(f"🗑️ 清仓非目标ETF: {etf}", force_print=True)
                    self.order_target_value_with_split(context, etf, 0)
        else:
            # 无目标ETF时，清仓所有当前ETF持仓
            for etf in self.current_etfs:
                self.my_log(f"🗑️ 清仓所有ETF: {etf}", force_print=True)
                self.order_target_value_with_split(context, etf, 0)
            self.my_log("⚠️ 无目标ETF，清仓所有持仓", "warn")

    def _execute_delayed_buy(self, context):
        """
        【延迟买入】执行涨幅超过阈值的ETF买入（14:50无论涨幅如何都买入）
        """
        self.my_log(f"⏰ 执行延迟买入... 延迟列表: {self.delayed_buy_etfs}")
        
        if not self.delayed_buy_etfs:
            return
        
        # 重新计算每个ETF的目标金额
        small_market_cap_trading_signal = self._get_small_market_cap_trading_signal(context)
        actual_capital = self.capital
        
        if not small_market_cap_trading_signal:
            small_market_cap_strategy = self._get_strategy_by_name("小市值策略")
            if small_market_cap_strategy and hasattr(small_market_cap_strategy, 'account_ratio') and hasattr(small_market_cap_strategy, 'empty_fund_allocation'):
                if small_market_cap_strategy.empty_fund_allocation == 'etf_rotation':
                    small_market_cap_ratio = small_market_cap_strategy.account_ratio
                    if not is_trade():
                        additional_capital = context.portfolio.portfolio_value * small_market_cap_ratio
                    else:
                        additional_capital = g.real_trading_cash_use * small_market_cap_ratio
                    actual_capital = self.capital + additional_capital
        
        # 加上之前已经买入的金额，计算总可用资金
        current_invested = 0
        for sid, pos in context.portfolio.positions.items():
            if pos.amount > 0 and sid in self.target_etfs:
                current_invested += pos.amount * pos.last_sale_price
        
        # 从总资金中扣除已投资金额，作为延迟买入的资金
        available_capital = actual_capital - current_invested
        delayed_count = len(self.delayed_buy_etfs)
        
        if delayed_count > 0 and available_capital > 0:
            capital_per_etf = available_capital / delayed_count
        else:
            capital_per_etf = 0
        
        # 直接执行买入，无论涨幅如何
        for item in self.delayed_buy_etfs:
            etf = item['etf']
            original_change = item['change_ratio']
            
            if capital_per_etf > self.min_money:
                self.my_log(f"✅ 延迟买入ETF: {etf}, 金额: {capital_per_etf:.2f}, 原涨幅: {original_change*100:.2f}%", force_print=True)
                self.order_target_value_with_split(context, etf, capital_per_etf)
            else:
                self.my_log(f"⚠️ {etf} 可用资金不足，跳过买入 (可用: {capital_per_etf:.2f}, 最小: {self.min_money:.2f})", 'warn')
        
        # 清空延迟买入列表
        self.delayed_buy_etfs = []

    # ==================== 辅助函数（保持不变）====================

    def _get_current_holdings(self, context):
        holdings = []
        for sid, pos in context.portfolio.positions.items():
            if pos.amount > 0 and sid in self.etf_pool:
                holdings.append(sid)
        return holdings

    def _get_small_market_cap_trading_signal(self, context):
        """获取小市值策略的trading_signal标志位"""
        try:
            small_market_cap_strategy = self._get_strategy_by_name("小市值策略")
            if small_market_cap_strategy and hasattr(small_market_cap_strategy, 'trading_signal'):
                return small_market_cap_strategy.trading_signal
            # 如果没找到小市值策略或其trading_signal属性，返回默认值True
            self.my_log(f"没找到小市值策略或其trading_signal属性，返回默认值True", 'warn')
            return True
        except Exception as e:
            self.my_log(f"获取小市值策略trading_signal失败: {e}", 'warn')
            return True

    def _get_strategy_by_name(self, strategy_name):
        """根据策略名称获取策略实例"""
        try:
            for strategy in trading_strategys:
                if hasattr(strategy, 'name') and strategy.name == strategy_name:
                    return strategy
            return None
        except Exception as e:
            self.my_log(f"获取策略{strategy_name}失败: {e}", 'warn')
            return None

    def _check_intraday_stop_loss(self, context):
        holdings = self._get_current_holdings(context)
        if not holdings:
            return
        current_data_all = self._get_current_data_ptrade_new(holdings)
        for etf in holdings:
            cur_data = current_data_all.get(etf, {})
            if not cur_data:
                continue
            ratio = (cur_data['last_price'] - cur_data['day_open']) / cur_data['day_open']
            if ratio <= self.stoploss_limit_by_cur_day:
                self.my_log(f"📉 {etf} 日内跌幅 {ratio*100:.2f}%，触发止损清仓", force_print=True)
                order_target_value(etf, 0)

    # ========== 原策略核心函数（保持不变）==========
    def _get_current_data_ptrade_new(self, stock_list):
        if isinstance(stock_list, str):
            stock_list = [stock_list]
        current_data_new = {}
        his = get_history(1, frequency='1d', field=['high_limit', 'low_limit', 'open'], security_list=stock_list, fq='pre', include=True)
        his2 = get_history(1, frequency='1m', field=['price'], security_list=stock_list, fq='pre', include=False)
        for stock in stock_list:
            current_data = {}
            stock_his = his[his['code'] == stock]
            if stock_his.empty:
                continue
            current_data['high_limit'] = stock_his['high_limit'].iloc[-1]
            current_data['low_limit'] = stock_his['low_limit'].iloc[-1]
            current_data['day_open'] = stock_his['open'].iloc[-1]

            stock_his2 = his2[his2['code'] == stock]
            if stock_his2.empty:
                continue
            current_data['last_price'] = stock_his2['price'].iloc[-1]
            current_data_new[stock] = current_data
        return current_data_new

    def _get_etf_rank(self, context, etf_pool):
        # 打印初始ETF池
        self.my_log("="*70)
        self.my_log(f"📋 初始ETF候选池 (共 {len(etf_pool)} 个)" + "*" * 60)
        for etf in etf_pool:
            self.my_log(f"  {etf}")
        self.my_log("="*70)
        
        rank_list = []
        current_data_all = self._get_current_data_ptrade_new(etf_pool)
        df_all = get_history(250, '1d', ['high', 'low', 'close', 'volume'], etf_pool, fq='pre')
        df_all = df_all.dropna() 
        # Step 1: 近3日跌幅过滤 + 日内止损过滤
        self.my_log("第1轮过滤: 近3日跌幅 + 日内止损" + "*" * 60)
        for etf in etf_pool[:]:
            df = df_all[df_all["code"] == etf].tail(self.m_days)
            current_data = current_data_all.get(etf)
            if not current_data or df.empty:
                self.my_log(f"❌ {etf} 数据缺失")
                continue
            prices = np.append(df["close"].values, current_data['last_price'])
            if len(prices) >= 4 and min(prices[-1]/prices[-2], prices[-2]/prices[-3], prices[-3]/prices[-4]) < 0.95:
                self.my_log(f"❌ {etf} 近3日跌幅过大")
                continue
            if self.enable_stop_loss_by_cur_day:
                ratio = (current_data['last_price'] - current_data['day_open']) / current_data['day_open']
                if ratio <= self.stoploss_limit_by_cur_day:
                    self.my_log(f"❌ {etf} 日内跌幅 {ratio*100:.2f}% 触发止损")
                    continue
            self.my_log(f"✔️ {etf} 通过第1轮过滤")
            rank_list.append(etf)
        
        self.my_log(f"\n📊 第1轮过滤后剩余: {len(rank_list)} 个 → {rank_list}\n")

        # Step 2: RSRS + 均线过滤
        rank_list = self._filter_rsrs(rank_list, df_all, current_data_all)
        
        # Step 3: 过滤成交量异常
        rank_list = self._filter_volume(context, rank_list, df_all)

        # Step 4: 动量打分
        rank_list = self._filter_moment_rank(rank_list, df_all, current_data_all, self.m_days, 0, self.m_score)

        self.my_log("="*70)
        return rank_list
    
    # RSRS 均线过滤
    def _filter_rsrs(self, stock_list,hist_data_all,current_data_all):
        self.my_log("第2轮过滤: RSRS+均线" + "*" * 60)
        # 计算斜率
        def _get_slope(security, days=18):
            try:
                hist_data = hist_data_all[hist_data_all["code"] == security].tail(days)
                if hist_data.empty or len(hist_data) < days:
                    return None
                slope = np.polyfit(hist_data['low'].values, hist_data['high'].values, 1)[0]
                return slope
            except Exception as e:
                self.my_log(f"计算{security} RSRS斜率失败: {e}",'error')
                return None
    
        # 计算阈值
        def _get_beta(security, lookback_days=250, window=20):
            try:
                hist_data = hist_data_all[hist_data_all["code"] == security].tail(lookback_days)
                if hist_data.empty or len(hist_data) < lookback_days:
                    return
    
                slope_list = []
                for i in range(len(hist_data) - window + 1):
                    window_data = hist_data.iloc[i:i + window]
                    low_values = window_data['low'].values
                    high_values = window_data['high'].values
    
                    if len(low_values) < window or len(high_values) < window:
                        continue
                    if np.any(np.isnan(low_values)) or np.any(np.isnan(high_values)):
                        continue
                    if np.any(np.isinf(low_values)) or np.any(np.isinf(high_values)):
                        continue
                    if np.std(low_values) == 0 or np.std(high_values) == 0:
                        continue
    
                    slope = np.polyfit(low_values, high_values, 1)[0]
                    slope_list.append(slope)
    
                if len(slope_list) < 2:
                    return None
    
                mean_slope = np.mean(slope_list)
                std_slope = np.std(slope_list)
                beta = mean_slope - 2 * std_slope
                return beta
            except Exception as e:
                self.my_log(f"计算{security} RSRS Beta失败: {e}",'error')
                return None
    
        # 计算强度
        def _check_with_strength(security):
            _slope = _get_slope(security)
            _beta = _get_beta(security)
            if _slope is None or _beta is None:
                return None, 0
            _strength = (_slope - _beta) / abs(_beta) if _beta != 0 else 0
            return _slope > _beta, _strength
    
        # 计算均值
        def _check_above_ma(security, days=20):
            try:
                hist = hist_data_all[hist_data_all["code"] == security].tail(days)
                if len(hist) < days:
                    return False
                current_price = current_data_all[security]['last_price']
                return current_price >= hist["close"].mean()
            except Exception as e:
                self.my_log(f"计算{security} {days}日均线失败: {e}",'error')
                return False
    
        res = []
        for stock in stock_list:
            stock_pass, stock_strength = _check_with_strength(stock)
            above_ma_5 = _check_above_ma(stock, 5)
            above_ma_10 = _check_above_ma(stock, 10)
            flag = "❌"
            reason = ""
            if stock_pass:
                if stock_strength > 0.15:
                    flag = "✔️"
                    reason = f"RSRS强度={stock_strength:.2f}>0.15"
                    res.append(stock)
                elif stock_strength > 0.03 and above_ma_5:
                    flag = "✔️"
                    reason = f"RSRS强度={stock_strength:.2f}>0.03 且站上MA5"
                    res.append(stock)
                elif above_ma_10:
                    flag = "✔️"
                    reason = f"站上MA10"
                    res.append(stock)
                else:
                    reason = f"RSRS强度={stock_strength:.2f} MA5={above_ma_5} MA10={above_ma_10}"
            else:
                reason = f"RSRS未通过 强度={stock_strength:.2f}"
            self.my_log(f"{flag} {stock} {reason}")
        
        self.my_log(f"\n📊 第2轮过滤后剩余: {len(res)} 个 → {res}\n")
        return res

    # 成交量过滤
    def _filter_volume(self, context, stock_list, hist_data_all, days=7, volume_threshold=2):
        """
        :param context:
        :param stock_list: 要检测的股票
        :param days: 检测周期天数
        :param volume_threshold: 阈值
        :return:
        """
        self.my_log("第3轮过滤: 成交量异常检测" + "*" * 60)
        # 获取开盘到现在每分钟得成交量
        mins = self._minutes_from_today_930(context)
        df_vol_all = get_history(count=mins, frequency='1m', field=['volume'], security_list=stock_list, fq='pre')
        def _get_volume_ratio(security):
            try:
                hist_data = hist_data_all[hist_data_all['code'] == security].tail(days)
                if hist_data.empty or len(hist_data) < days:
                    return
                avg_volume = hist_data['volume'].mean()
                df_vol = df_vol_all[df_vol_all['code'] == security]
                if df_vol is None or df_vol.empty:
                    return
                current_volume = df_vol['volume'].sum()
                _volume_ratio = current_volume / avg_volume
                # 检测到异常, 返回异常倍数
                if _volume_ratio > volume_threshold:
                    self.my_log(f"❌ {security} 成交量较近{days}日均值 x{_volume_ratio:.2f}")
                    return _volume_ratio
                self.my_log(f"✔️ {security} 成交量较近{days}日均值 x{_volume_ratio:.2f}")
            except Exception as e:
                self.my_log(f"⭕ 检查{security}成交量失败: {e}",'error')
                return
    
        res = []
        for stock in stock_list:
            ratio = _get_volume_ratio(stock)
            if not ratio:
                res.append(stock)
        
        self.my_log(f"\n📊 第3轮过滤后剩余: {len(res)} 个 → {res}\n")
        return res

    # 动量计算（效率动量版：score = momentum × efficiency_ratio）
    def _filter_moment_rank(self, stock_pool, hist_data_all, current_data_all, days, ll, hh):
        self.my_log("第4轮过滤: 效率动量得分计算" + "*" * 60)
        scores_data = pd.DataFrame(index=stock_pool, columns=["momentum", "efficiency_ratio", "score"])
        for code in stock_pool:
            try:
                hist_data = hist_data_all[hist_data_all['code'] == code].tail(days)
                if hist_data.empty or len(hist_data) < 2:
                    self.my_log(f"动量计算时跳过:{code}")
                    continue
                current_data = current_data_all[code]
                
                # 计算 Pivot = (Open + High + Low + Close) / 4
                o_vals = hist_data["open"].values if "open" in hist_data.columns else hist_data["close"].values
                h_vals = hist_data["high"].values
                l_vals = hist_data["low"].values
                c_vals = hist_data["close"].values
                pivot_hist = (o_vals + h_vals + l_vals + c_vals) / 4.0
                # 当日用 last_price 作为 pivot（盘中无完整OHLC）
                pivot = np.append(pivot_hist, current_data['last_price'])
                
                if pivot[0] <= 0 or pivot[-1] <= 0:
                    continue
                
                # 动量 = 100 × ln(pivot_end / pivot_start)
                momentum = 100.0 * np.log(pivot[-1] / pivot[0])
                scores_data.loc[code, "momentum"] = momentum
                
                # 效率系数 = |起点到终点的直线距离| / 路径总长度
                log_pivot = np.log(pivot)
                direction = abs(log_pivot[-1] - log_pivot[0])
                volatility = np.sum(np.abs(np.diff(log_pivot)))
                efficiency_ratio = direction / volatility if volatility > 0 else 0
                scores_data.loc[code, "efficiency_ratio"] = efficiency_ratio
                
                # 效率动量得分 = 动量 × 效率系数
                momentum_score = momentum * efficiency_ratio
                scores_data.loc[code, "score"] = momentum_score
    
                # 近3日跌幅过大则置零
                prices = np.append(c_vals, current_data['last_price'])
                if len(prices) >= 4 and min(prices[-1] / prices[-2], prices[-2] / prices[-3],
                       prices[-3] / prices[-4]) < 0.97:
                    scores_data.loc[code, "score"] = 0
            except Exception as e:
                self.my_log(f"计算{code}动量得分失败: {e}",'error')
                scores_data.loc[code, "score"] = 0
        # 打印被 ll 和 hh 过滤的标的
        for code in scores_data.index:
            score = scores_data.loc[code, 'score']
            if pd.isna(score):
                self.my_log(f"❌ {code} 被过滤: 动量得分数据缺失")
            elif score <= ll:
                self.my_log(f"❌ {code} 被过滤: 动量得分 {score:.4f} <= 下限阈值 {ll}")
            elif score >= hh:
                self.my_log(f"❌ {code} 被过滤: 动量得分 {score:.4f} >= 上限阈值 {hh}")
        valid_etfs = scores_data[(scores_data['score'] > ll) & (scores_data['score'] < hh)] \
            .sort_values("score", ascending=False)
        rank_list = valid_etfs.index.tolist()
        
        # 打印所有候选ETF的动量得分（从高到低）
        for code in valid_etfs.index:
            score = valid_etfs.loc[code, 'score']
            self.my_log(f"✔️ {code} 动量得分: {score:.4f}")
        
        self.my_log(f"\n📊 第4轮过滤后剩余: {len(rank_list)} 个 → {rank_list}\n")
        
        return rank_list
    
    # 计算RSI指标
    def _calculate_rsi(self, code, period=14):
        """计算RSI指标"""
        df = get_history(125, '1d', ['close', ],code, fq='pre')
        prices = df['close'].values
        deltas = np.diff(prices)
        seed = deltas[:period + 1]
        up = seed[seed >= 0].sum() / period
        down = -seed[seed < 0].sum() / period
        if down == 0:
            return 100
        rs = up / down
        rsi = 100. - 100. / (1. + rs)
        return rsi
    
    def _minutes_from_today_930(self, context):
        """返回从今天 9:30 到 context.current_dt 的分钟数（向下取整，早于9:30返回0）"""
        now = context.current_dt
        # 保留时区信息（若存在）
        tz = now.tzinfo
        open_dt = datetime(now.year, now.month, now.day, 9, 30, 0, tzinfo=tz)
        delta = now - open_dt
        minutes = int(delta.total_seconds() // 60)
        return max(0, minutes)
if "etf_rotation" in strategy_config:
    strategy_config['etf_rotation']['class'] = EtfRotationStrategy

# ============================国债逆回购策略类===========================
class IpoAndRepoStrategy(BaseStrategy):
    """
    国债逆回购策略（可转债打新已屏蔽）
    - 每日 15:00 执行国债逆回购
    - 可配置保留最低现金（如 10000 元）不参与逆回购
    """

    def __init__(self, reserve_cash=1000.0, account_ratio=1.0):
        """
        :param reserve_cash: 保留的最低现金（元），这部分不用于逆回购，默认 10000 元
        """
        super().__init__(name="国债逆回购策略", version="v1.1")
        self.reserve_cash = float(reserve_cash)
        self.repo_time = "14:59"
        self.cur_strategy_stocks = []

    def before_trading_start(self, context, data=None):
        """盘前：可留空"""
        pass

    def handle_data(self, context, data=None):
        """盘中：在指定时间执行逆回购"""
        if is_trade():
            current_time = context.current_dt.strftime("%H:%M")
            if current_time == self.repo_time:
                self._execute_repo(context)

    def after_trading_end(self, context, data=None):
        """盘后：可留空"""
        pass

    def _execute_repo(self, context):
        """执行国债逆回购，预留 reserve_cash 后使用剩余现金"""
        self.my_log("✅ 开始国债逆回购！", force_print=True)
        try:
            # 获取深市和沪市1天期逆回购最新价
            shen_snapshot = get_snapshot('131810.SZ')
            hu_snapshot = get_snapshot('204001.SS')
            
            shen_price = shen_snapshot['131810.SZ']['last_px']
            hu_price = hu_snapshot['204001.SS']['last_px']

            # 选择价格更高的市场
            if shen_price >= hu_price:
                choice = '131810.SZ'
            else:
                choice = '204001.SS'

            total_cash = context.portfolio.cash
            usable_cash = max(0.0, total_cash - self.reserve_cash)

            # 计算可下单数量（1张=100元，最小交易单位10张=1000元）
            # 例如：可用现金 9876 元 → 9876 // 1000 = 9 → 9 * 10 = 90 张
            amount = int(usable_cash / 1000) * 10

            self.my_log(f"深市价格: {shen_price}, 沪市价格: {hu_price} → 选择: {choice}", force_print=True)
            self.my_log(f"总现金: {total_cash:.2f}, 保留现金: {self.reserve_cash:.2f}, 可用现金: {usable_cash:.2f}", force_print=True)
            self.my_log(f"逆回购数量: {amount} 张", force_print=True)

            if amount > 0:
                order(choice, -amount)  # 卖出（融出资金）
                self.my_log(f"✅ 逆回购下单成功: {choice} x {amount}", force_print=True)
            else:
                self.my_log("⚠️ 可用现金不足1000元，跳过逆回购", 'warn')

        except Exception as e:
            self.my_log(f"❌ 国债逆回购失败: {e}", 'error')
if "ipo_repo_strategy" in strategy_config:
    strategy_config['ipo_repo_strategy']['class'] = IpoAndRepoStrategy

# ============================小国均线国债策略类===========================
class XLStrategy(BaseStrategy):
    """
    - 基于XL指标的ETF轮动策略
    - 支持涨跌停过滤和止损机制
    """

    def __init__(self, account_ratio=1.0, hold_count=5, commission_ratio=0.0001, min_commission=0.001,
                 sell_time='09:35', buy_time='09:40', history_days=60, xl_param_n=26, xl_param_zsbl=2.6,
                 xl_param_fxxs=0.02, xl_param_atrn=26, xl_filter_min=0, xl_filter_max=None):
        super().__init__(name="XL-ETF轮动策略", version="v1.0")
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.capital = 0
        
        # 策略参数
        self.hold_count = hold_count
        self.commission_ratio = commission_ratio
        self.min_commission = min_commission
        self.sell_time = sell_time
        self.buy_time = buy_time
        self.history_days = history_days
        self.xl_param_n = xl_param_n
        self.xl_param_zsbl = xl_param_zsbl
        self.xl_param_fxxs = xl_param_fxxs
        self.xl_param_atrn = xl_param_atrn
        self.xl_filter_min = xl_filter_min
        self.xl_filter_max = xl_filter_max
        
        # ETF池
        self.etf_dict = {
            "159201.SZ": "自由现金流ETF", "159206.SZ": "卫星ETF", "159262.SZ": "港股通科技ETF",
            "159316.SZ": "港股通创新药ETF", "159326.SZ": "电网设备ETF", "159363.SZ": "创业板人工智能ETF",
            "159399.SZ": "现金流ETF", "159506.SZ": "港股通医疗ETF富国", "159509.SZ": "纳指科技ETF",
            "159516.SZ": "半导体设备ETF", "159530.SZ": "机器人ETF易方达", "159545.SZ": "恒生红利低波ETF",
            "159566.SZ": "储能电池ETF易方达", "159570.SZ": "港股通创新药ETF", "159593.SZ": "中证A50指数ETF",
            "159605.SZ": "中概互联ETF", "159611.SZ": "电力ETF", "159636.SZ": "港股通科技30ETF",
            "159691.SZ": "港股红利ETF", "159732.SZ": "消费电子ETF", "159755.SZ": "电池ETF",
            "159766.SZ": "旅游ETF", "159781.SZ": "科创创业ETF易方达", "159792.SZ": "港股通互联网ETF",
            "159796.SZ": "电池50ETF", "159819.SZ": "人工智能ETF易方达", "159851.SZ": "金融科技ETF",
            "159852.SZ": "软件ETF", "159859.SZ": "生物医药ETF", "159865.SZ": "养殖ETF",
            "159869.SZ": "游戏ETF", "159870.SZ": "化工ETF", "159883.SZ": "医疗器械ETF",
            "159892.SZ": "恒生医药ETF", "159901.SZ": "深证100ETF易方达", "159915.SZ": "创业板ETF易方达",
            "159920.SZ": "恒生ETF", "159928.SZ": "消费ETF", "159941.SZ": "纳指ETF",
            "159949.SZ": "创业板50ETF", "159967.SZ": "创业板成长ETF", "159980.SZ": "有色ETF大成",
            "159985.SZ": "豆粕ETF", "159992.SZ": "创新药ETF", "159993.SZ": "证券ETF龙头",
            "159995.SZ": "芯片ETF", "159998.SZ": "计算机ETF", "510050.SS": "上证50ETF",
            "510180.SS": "上证180ETF", "510210.SS": "上证指数ETF", "510300.SS": "沪深300ETF",
            "510500.SS": "中证500ETF", "510720.SS": "红利国企ETF", "510880.SS": "红利ETF",
            "510900.SS": "恒生中国企业ETF易方达", "512000.SS": "券商ETF", "512010.SS": "医药ETF易方达",
            "512070.SS": "证券保险ETF易方达", "512100.SS": "中证1000ETF", "512170.SS": "医疗ETF",
            "512200.SS": "房地产ETF", "512290.SS": "生物医药ETF", "512400.SS": "有色金属ETF",
            "512480.SS": "半导体ETF", "512660.SS": "军工ETF", "512670.SS": "国防ETF",
            "512690.SS": "酒ETF", "512710.SS": "军工龙头ETF", "512760.SS": "芯片ETF",
            "512800.SS": "银行ETF", "512890.SS": "红利低波ETF", "512980.SS": "传媒ETF",
            "513020.SS": "港股科技ETF", "513050.SS": "中概互联网ETF易方达", "513060.SS": "恒生医疗ETF",
            "513090.SS": "香港证券ETF易方达", "513120.SS": "港股创新药ETF", "513160.SS": "港股科技30ETF",
            "513180.SS": "恒生科技指数ETF", "513190.SS": "港股通金融ETF", "513310.SS": "中韩半导体ETF",
            "513330.SS": "恒生互联网ETF", "513500.SS": "标普500ETF", "513550.SS": "港股通50ETF",
            "513630.SS": "港股红利指数ETF", "513690.SS": "港股红利ETF博时", "513750.SS": "港股通非银ETF",
            "513780.SS": "港股创新药50ETF", "513820.SS": "港股红利ETF基金", "513910.SS": "港股央企红利ETF",
            "513920.SS": "港股通央企红利ETF", "513970.SS": "恒生消费ETF", "513980.SS": "港股科技50ETF",
            "515000.SS": "科技ETF", "515030.SS": "新能源车ETF", "515050.SS": "5G通信ETF",
            "515170.SS": "食品饮料ETF", "515180.SS": "红利ETF易方达", "515210.SS": "钢铁ETF",
            "515220.SS": "煤炭ETF", "515230.SS": "软件ETF", "515300.SS": "300红利低波ETF",
            "515450.SS": "红利低波50ETF", "515650.SS": "消费50ETF", "515790.SS": "光伏ETF",
            "515800.SS": "800ETF", "515880.SS": "通信ETF", "516150.SS": "稀土ETF嘉实",
            "516650.SS": "有色金属ETF基金", "517520.SS": "黄金股ETF", "518880.SS": "黄金ETF",
            "520990.SS": "港股央企红利50ETF", "560860.SS": "工业有色ETF", "561980.SS": "半导体设备ETF",
            "562500.SS": "机器人ETF", "562800.SS": "稀有金属ETF", "563300.SS": "中证2000ETF",
            "563360.SS": "A500ETF华泰柏瑞", "588000.SS": "科创50ETF", "588170.SS": "科创半导体ETF",
            "588200.SS": "科创芯片ETF", "588220.SS": "科创100ETF基金", "588790.SS": "科创AIETF",
        }
        self.security = list(self.etf_dict.keys())
        
        # 与其他策略重复的ETF列表
        duplicate_etfs = {
            # 哈利布朗策略重复
            "159941.SZ", "512890.SS", "510880.SS",
            # 三马ETF轮动策略重复
            "510180.SS", "159985.SZ", "159915.SZ", "512480.SS", "159851.SZ", 
            "513020.SS", "513690.SS", "510050.SS", "518880.SS",
            # 桥水基金全天候策略重复
            "513500.SS"
        }
        
        # 过滤掉重复的ETF
        self.etf_dict = {k: v for k, v in self.etf_dict.items() if k not in duplicate_etfs}
        self.security = list(self.etf_dict.keys())
        
        self.etf_pool_filtered = []
        self.skip_buy_today = False
        
        # 初始化cur_strategy_stocks（用于策略冲突检测和非交易策略清仓检查）
        self.cur_strategy_stocks = self.security[:]
        
        # 佣金设置移至before_trading_start方法

    def before_trading_start(self, context, data=None):
        """盘前：过滤未上市ETF，打印当前持仓"""
        super().before_trading_start(context, data)
        
        # 打印当前策略持仓
        current_holdings = self._get_current_holdings(context)
        if current_holdings:
            self.my_log(f"📊 当前策略持仓 ({len(current_holdings)} 只):", force_print=True)
        else:
            self.my_log("📊 当前策略持仓: 空仓", force_print=True)
        
        # 调仓限制检查：持仓满额则当日禁买
        if len(current_holdings) >= self.hold_count:
            self.skip_buy_today = True
            self.my_log(f"⛔ 调仓限制：当前持仓({len(current_holdings)})已达目标数量({self.hold_count})，当日跳过买入", force_print=True)
        else:
            self.skip_buy_today = False
        
        # 过滤未上市ETF
        listed_security = []
        stock_info = get_stock_info(self.security, field=['listed_date'])
        for security in self.security:
            try:
                listed_date_str = stock_info[security]['listed_date']
                if not listed_date_str:
                    self.my_log(f"❌ {security} 未获取到上市日期，排除", 'warn')
                    continue
                
                listed_date = datetime.strptime(listed_date_str, "%Y-%m-%d").date()
                current_date = context.current_dt.date()
                
                if listed_date <= current_date:
                    listed_security.append(security)
                else:
                    self.my_log(f"❌ {security} 未上市（上市日：{listed_date_str}），当前回测日：{current_date}，排除", 'warn')
            except Exception as e:
                self.my_log(f"⚠️  过滤{security}时异常：{str(e)}，排除", 'warn')
                continue
        self.etf_pool_filtered = listed_security
        self.my_log(f"✅ 有效ETF池数量: {len(self.etf_pool_filtered)}", force_print=True)
        
        # 更新cur_strategy_stocks为过滤后的有效ETF池
        self.cur_strategy_stocks = self.etf_pool_filtered[:]

    def handle_data(self, context, data=None):
        """盘中：在指定时间执行卖出/买入"""
        current_time = context.current_dt.strftime("%H:%M")
        
        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # 分时段执行：先卖后买
        if current_time == self.sell_time:
            self.my_log(f"========== 开始执行卖出逻辑 ({current_time}) ==========", force_print=True)
            self.execute_sell(context)
        elif current_time == self.buy_time:
            if self.skip_buy_today:
                self.my_log(f"⛔ 买入时间({current_time})到达，但调仓限制生效，跳过买入", force_print=True)
                return
            self.my_log(f"========== 开始执行买入逻辑 ({current_time}) ==========", force_print=True)
            self.execute_buy(context)

    def _get_current_data_ptrade_new(self, stock_list):
        """获取股票最新数据，避免未来函数"""
        try:
            if isinstance(stock_list, str):
                stock_list = [stock_list]
            
            current_data_new = {}
            his = get_history(1, frequency='1d', field=['high_limit', 'low_limit', 'open', 'high', 'low'], 
                              security_list=stock_list, fq='pre', include=True)
            his2 = get_history(1, frequency='1m', field=['price'], 
                               security_list=stock_list, fq='pre', include=False)
            
            for stock in stock_list:
                current_data = {}
                stock_his_data = his[his['code'] == stock]
                if not stock_his_data.empty:
                    current_data['high_limit'] = stock_his_data['high_limit'].iloc[-1]
                    current_data['low_limit'] = stock_his_data['low_limit'].iloc[-1]
                    current_data['day_open'] = stock_his_data['open'].iloc[-1]
                    current_data['day_high'] = stock_his_data['high'].iloc[-1]
                    current_data['day_low'] = stock_his_data['low'].iloc[-1]
                else:
                    continue
                    
                stock_his2_data = his2[his2['code'] == stock]
                if not stock_his2_data.empty:
                    current_data['last_price'] = stock_his2_data['price'].iloc[-1]
                else:
                    current_data['last_price'] = current_data.get('day_open', 0)
                    
                current_data_new[stock] = current_data
            
            return current_data_new
        except Exception as e:
            log.error(f"获取股票最新数据失败: {e}")
            return {}

    def _get_current_holdings(self, context):
        """获取当前策略持仓"""
        try:
            holdings = []
            positions = context.portfolio.positions
            
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in self.security:
                    holdings.append(stock)
            
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'warn')
            return []

    def calculate_xl_batch(self, df, N=None, ZSBL=None, FXXS=None, ATRN=None, min_history_days=30):
        """批量计算所有标的的XL指标值和卖信号"""
        if N is None:
            N = self.xl_param_n
        if ZSBL is None:
            ZSBL = self.xl_param_zsbl
        if FXXS is None:
            FXXS = self.xl_param_fxxs
        if ATRN is None:
            ATRN = self.xl_param_atrn
        
        df = df.dropna()
        code_counts = df['code'].value_counts()
        valid_codes = code_counts[code_counts >= min_history_days].index.tolist()
        df = df[df['code'].isin(valid_codes)]
        
        if df.empty:
            return {}, {}
        
        def calc_indicators(group):
            group = group.sort_index()
            group['HHV_HIGH_N'] = group['high'].rolling(window=N).max()
            group['S'] = group['HHV_HIGH_N'].shift(1)
            group['LLV_LOW_N'] = group['low'].rolling(window=N).min()
            group['X'] = group['LLV_LOW_N'].shift(1)
            group['MTR1'] = group['high'] - group['low']
            group['MTR2'] = abs(group['close'].shift(1) - group['high'])
            group['MTR3'] = abs(group['close'].shift(1) - group['low'])
            group['MTR'] = group[['MTR1', 'MTR2', 'MTR3']].max(axis=1)
            group['ATR'] = group['MTR'].rolling(window=ATRN).mean().shift(1)
            group['ZS'] = group['ATR'] * ZSBL
            group['DS'] = group['S'] - group['ZS']
            group['XL'] = (group['close'] - group['close'].shift(ATRN)) / group['ATR']
            return group
        
        df = df.groupby('code', group_keys=False).apply(calc_indicators)
        
        xl_values = {}
        sell_signals = {}
        
        for code in valid_codes:
            code_df = df[df['code'] == code]
            if code_df.empty:
                continue
                
            last_row = code_df.iloc[-1]
            xl_value = last_row['XL'] if not pd.isna(last_row['XL']) else None
            ds_value = last_row['DS'] if not pd.isna(last_row['DS']) else None
            current_close = last_row['close']
            
            if xl_value is not None:
                if self.xl_filter_min is not None and xl_value < self.xl_filter_min:
                    xl_value = None
                if self.xl_filter_max is not None and xl_value > self.xl_filter_max:
                    xl_value = None
            
            xl_values[code] = xl_value
            sell_signals[code] = current_close < ds_value if ds_value is not None else False
        
        return xl_values, sell_signals

    def execute_sell(self, context):
        """执行卖出逻辑：计算目标持仓并卖出不在目标列表中的持仓"""
        df = get_history(self.history_days, '1d', field=['open', 'high', 'low', 'close'], 
                         security_list=self.etf_pool_filtered, fq='pre', include=False, is_dict=False)
        
        current_data_dict = self._get_current_data_ptrade_new(self.etf_pool_filtered)
        
        today_data_list = []
        for code in self.etf_pool_filtered:
            if code in current_data_dict:
                cd = current_data_dict[code]
                today_row = {
                    'code': code,
                    'open': cd.get('day_open', 0),
                    'high': cd.get('day_high', 0),
                    'low': cd.get('day_low', 0),
                    'close': cd.get('last_price', 0)
                }
                today_data_list.append(today_row)
        
        if today_data_list:
            today_df = pd.DataFrame(today_data_list)
            df = pd.concat([df, today_df], ignore_index=True)
        
        current_holdings = self._get_current_holdings(context)
        xl_values, sell_signals = self.calculate_xl_batch(df)

        sell_list = []
        if len(current_holdings) >= self.hold_count:
            for etf in current_holdings:
                if sell_signals.get(etf, False):
                    self.my_log(f"🚀 卖出 {etf}，触发卖信号", force_print=True)
                    self.order_target_value_with_split(context, etf, 0)
                    sell_list.append(etf)
        
        for etf in sell_list:
            if etf in current_holdings:
                current_holdings.remove(etf)
        
        sorted_xl = sorted(xl_values.items(), 
                          key=lambda x: x[1] if x[1] is not None and not pd.isna(x[1]) else float('-inf'), 
                          reverse=True)
        
        self.my_log("按XL排序的标的:", force_print=True)
        for i, (code, xl) in enumerate(sorted_xl[:10], 1):
            self.my_log(f"第{i}名: {code}, XL: {xl}, sell_signal: {sell_signals.get(code, False)}", force_print=True)
        
        target_list = current_holdings.copy()
        
        for etf_info in sorted_xl:
            if len(target_list) >= self.hold_count:
                break
            etf_code = etf_info[0]
            if etf_code not in target_list and not sell_signals.get(etf_code, False):
                target_list.append(etf_code)
        
        self.my_log(f"目标持仓列表: {target_list}", force_print=True)
        
        # 涨跌停过滤
        all_stocks = list(set(current_holdings + target_list))
        limit_status = check_limit(all_stocks) if all_stocks else {}
        
        buy_list = []
        for etf in target_list:
            if etf not in current_holdings:
                stock_status = limit_status.get(etf, 0)
                if stock_status in [-1, -2]:
                    self.my_log(f"⛔ 跌停不买：{etf} (状态码: {stock_status})", force_print=True)
                    continue
                buy_list.append(etf)
            else:
                buy_list.append(etf)
        
        filtered_target_list = buy_list
        self.my_log(f"跌停过滤后目标持仓列表: {filtered_target_list}", force_print=True)
        
        sell_forbidden_list = []
        for etf in current_holdings:
            if etf not in filtered_target_list:
                stock_status = limit_status.get(etf, 0)
                if stock_status in [1, 2]:
                    self.my_log(f"🔒 涨停不卖：{etf} (状态码: {stock_status})，保留持仓", force_print=True)
                    sell_forbidden_list.append(etf)
                    filtered_target_list.append(etf)
        
        for etf in current_holdings:
            if etf not in filtered_target_list:
                self.my_log(f"🗑️ 清仓非目标持仓 {etf}", force_print=True)
                self.order_target_value_with_split(context, etf, 0)
        
        self.my_log(f"✅ 卖出逻辑执行完毕，目标列表: {filtered_target_list}", force_print=True)

    def execute_buy(self, context):
        """执行买入逻辑：独立计算目标持仓并执行买入"""
        self.my_log("========== 开始独立买入逻辑计算 ==========", force_print=True)
        
        df = get_history(self.history_days, '1d', field=['open', 'high', 'low', 'close'], 
                         security_list=self.etf_pool_filtered, fq='pre', include=False, is_dict=False)
        
        current_data_dict = self._get_current_data_ptrade_new(self.etf_pool_filtered)
        
        today_data_list = []
        for code in self.etf_pool_filtered:
            if code in current_data_dict:
                cd = current_data_dict[code]
                today_row = {
                    'code': code,
                    'open': cd.get('day_open', 0),
                    'high': cd.get('day_high', 0),
                    'low': cd.get('day_low', 0),
                    'close': cd.get('last_price', 0)
                }
                today_data_list.append(today_row)
        
        if today_data_list:
            today_df = pd.DataFrame(today_data_list)
            df = pd.concat([df, today_df], ignore_index=True)
        
        current_holdings = self._get_current_holdings(context)
        xl_values, sell_signals = self.calculate_xl_batch(df)
        
        sorted_xl = sorted(xl_values.items(), 
                          key=lambda x: x[1] if x[1] is not None and not pd.isna(x[1]) else float('-inf'), 
                          reverse=True)
        
        target_list = current_holdings.copy()
        
        for etf_info in sorted_xl:
            if len(target_list) >= self.hold_count:
                break
            etf_code = etf_info[0]
            if etf_code not in target_list and not sell_signals.get(etf_code, False):
                target_list.append(etf_code)
        
        self.my_log(f"买入目标持仓列表: {target_list}", force_print=True)
        
        buy_list = []
        limit_status = check_limit(target_list) if target_list else {}
        
        for etf in target_list:
            if etf not in current_holdings:
                stock_status = limit_status.get(etf, 0)
                if stock_status in [-1, -2]:
                    self.my_log(f"⛔ 跌停不买：{etf} (状态码: {stock_status})", force_print=True)
                    continue
                buy_list.append(etf)
            else:
                buy_list.append(etf)
        
        final_target_list = buy_list
        self.my_log(f"跌停过滤后买入目标: {final_target_list}", force_print=True)
        
        if final_target_list:
            actual_capital = self.capital
            capital_per_etf = actual_capital / len(final_target_list)
            
            for etf in final_target_list:
                self.my_log(f"🚀 买入/调仓 {etf}，XL值: {xl_values.get(etf, 'N/A')}, 资金: {capital_per_etf:.2f}", force_print=True)
                self.order_target_value_with_split(context, etf, capital_per_etf)
        
        self.my_log("✅ 独立买入逻辑执行完毕", force_print=True)

if "xl_strategy" in strategy_config:
    strategy_config['xl_strategy']['class'] = XLStrategy

# ============================自动撤单重提类===========================
class AutoCancelRetryStrategy(BaseStrategy):
    """
    自动撤单重提策略
    - 监控未完全成交的订单（已报 '2'、部成 '7'）
    - 若超过设定时间仍未成交，撤单并重提剩余数量
    """

    def __init__(self, unorder_time=30, account_ratio=1.0):
        """
        :param unorder_time: 撤单重提等待时间（秒），默认 30 秒
        """
        super().__init__(name="自动撤单重提策略", version="v1.0")
        self.unorder_time = int(unorder_time)

    def before_trading_start(self, context, data=None):
        """盘前清空已处理订单记录（每天重置）"""
        pass

    def handle_data(self, context, data=None):
        """盘中实时检查订单状态并撤单重提"""
        if not is_trade():
            return  # 回测中跳过（回测通常无真实订单流）
        try:
            all_orders = get_all_orders()
            for _order in all_orders:
                # 涨跌停判断
                stock_flag = check_limit(_order['symbol'])[_order['symbol']]
                if stock_flag == 0:
                    # 遍历账户当日全部订单，对已报、部成状态订单进行撤单操作
                    if _order['status'] in ['2', '7']:
                        # 计算当前委托多久未成交
                        order_time = datetime.strptime(_order['entrust_time'], "%Y-%m-%d %H:%M:%S")
                        current_time = datetime.now()
                        time_difference = current_time - order_time
                        seconds_difference = time_difference.total_seconds()
                        # 对大于unorder_time s未成交的订单进行撤单重提
                        if abs(seconds_difference) > self.unorder_time:
                            # 开始撤单
                            cancel_order_ex(_order)
                            time.sleep(1)
                            # 计算未成交订单数量
                            unfinish_amount = _order['amount'] - _order['filled_amount']
                            self.my_log(f"下单数量:{_order['amount']},已成交数量:{_order['filled_amount']},未成交订单数量:{unfinish_amount}", force_print=True)
                            # 检查实际可用数量，避免超卖
                            if abs(unfinish_amount) > 0:
                                # 获取实际可用数量
                                position = get_position(_order['symbol'])
                                enable_amount = getattr(position, 'enable_amount', 0)
                                if enable_amount > 0:
                                    # 对于卖出单，确保不超过可用数量
                                    if unfinish_amount < 0:
                                        unfinish_amount = -min(enable_amount, abs(unfinish_amount))
                                        self.my_log(f"可用数量:{enable_amount},未成交订单数量:{unfinish_amount},实际下单数量:{unfinish_amount}", force_print=True)
                                order(_order['symbol'], unfinish_amount)
                                time.sleep(1)
                                self.my_log(f"重新下单完毕，下单标的:{_order['symbol']},下单数量:{unfinish_amount}", force_print=True)
        except Exception as e:
            self.my_log(f"自动撤单重提策略执行失败，失败原因: {e}", force_print=True)
    def after_trading_end(self, context, data=None):
        """盘后清理（可选）"""
        pass
if "auto_cancel_retry" in strategy_config:
    strategy_config['auto_cancel_retry']['class'] = AutoCancelRetryStrategy

# ============================小市值策略类=============================
class SmallMarketCapStrategy(BaseStrategy):
    """
    小市值策略（源自三马v10.2）
    - 基于小市值股票池选股
    - 支持多种选股版本（v1/v2/v3）
    - 支持动态调整持股数量
    - 支持止损止盈机制
    """

    def __init__(self, account_ratio=1.0, xsz_version="v3", enable_dynamic_stock_num=True,
                 xsz_stock_num=5, xsz_buy_etf="512800.SS", run_stoploss=True, stoploss_strategy=3,
                 stoploss_limit=0.09, stoploss_market=0.05, DBL_control=True, check_dbl_days=10, take_profit_ratio=0.5,
                 trade_time=["09:31", "09:40", "09:42", "10:00", "14:00", "14:50"],first_run=True, empty_months=[1, 4],
                 empty_fund_allocation='etf_rotation',
                 rebalance_weekdays=None, rebalance_interval=None):
        super().__init__(name="小市值策略", version="v1.0")
        
        # 资金配置
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.capital = 0  # 运行时更新
        
        # 策略参数
        self.xsz_version = xsz_version  # 选股版本: v1/v2/v3
        self.enable_dynamic_stock_num = enable_dynamic_stock_num  # 是否启用动态选股数量
        self.xsz_stock_num = xsz_stock_num  # 默认持股数量
        self.xsz_buy_etf = xsz_buy_etf  # 空仓时购买ETF
        
        # 止损参数
        self.run_stoploss = run_stoploss  # 是否进行止损
        self.stoploss_strategy = stoploss_strategy  # 1为止损线止损，2为市场趋势止损, 3为联合1、2策略
        self.stoploss_limit = stoploss_limit  # 止损线
        self.stoploss_market = stoploss_market  # 市场趋势止损参数
        
        # 顶背离控制
        self.DBL_control = DBL_control  # 小市值大盘顶背离记录（用于风险控制）
        self.dbl = []
        self.check_dbl_days = check_dbl_days  # 顶背离检测窗口期长度
        
        # 止盈参数
        self.take_profit_ratio = take_profit_ratio  # 止盈比例（0.5表示50%止盈）
        
        # 策略时间参数
        self.trade_time = trade_time                    # 所有交易时间数组
        self.prepare_time = trade_time[0]           # 盘前准备时间
        self.sell_time = trade_time[1]              # 卖出时间
        self.buy_time = trade_time[2]               # 买入时间
        self.stop_loss_time_1 = trade_time[3]         # 上午止损时间
        self.stop_loss_time_2 = trade_time[4]         # 上午止损时间
        self.limit_up_check_time = trade_time[5]    # 涨停检查时间
        self.close_account_time = trade_time[6]     # 清仓时间
        
        # 空仓期配置
        self.empty_months = empty_months  # 空仓期月份配置
        
        # 空仓资金分配配置
        self.empty_fund_allocation = empty_fund_allocation  # 空仓时资金分配方式 ('etf_rotation' 或 'fixed_etf')
        
        # 调仓方式配置（支持两种模式）
        self.rebalance_mode = None  # 'weekday' 或 'interval'
        self.rebalance_weekdays = rebalance_weekdays  # 调仓星期列表（0=周一, 1=周二, ..., 6=周日）
        self.rebalance_interval = rebalance_interval  # 调仓间隔天数（交易日）
        self.trading_days_since_rebalance = 0  # 距离上次调仓的交易日数
        
        # 判断使用哪种调仓模式
        if rebalance_weekdays is not None:
            self.rebalance_mode = 'weekday'
            self.my_log(f"调仓模式: 固定星期调仓, 调仓日: {rebalance_weekdays}", force_print=True)
        elif rebalance_interval is not None:
            self.rebalance_mode = 'interval'
            self.my_log(f"调仓模式: 间隔天数调仓, 间隔: {rebalance_interval}个交易日", force_print=True)
        else:
            # 默认使用周二调仓
            self.rebalance_mode = 'weekday'
            self.rebalance_weekdays = [1]
            self.my_log("调仓模式: 默认周二调仓", force_print=True)
        
        # 策略状态变量
        self.target_list = []  # 目标持仓股票
        self.yesterday_HL_list = []  # 昨日涨停股票
        self.trading_signal = True  # 交易信号
        self.first_run = first_run  # 是否为首次运行
        
        # 可能交易的股票池
        self.cur_strategy_stocks = []
        
    def before_trading_start(self, context, data=None):
        """盘前：变量预处理"""
        super().before_trading_start(context, data)
        
        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        #尝试启动pickle文件
        if is_trade():
            try: 
                with open(get_research_path() + 'SmallMarketCapStrategy.pkl','rb') as f:
                    saved_data = pickle.load(f)
                    self.trading_days_since_rebalance = saved_data.get('trading_days_since_rebalance', 0)
                    self.first_run = saved_data.get('first_run', self.first_run)
                    self.my_log(f"本地加载成功，trading_days_since_rebalance:{self.trading_days_since_rebalance}, first_run:{self.first_run}", force_print=True)
            except FileNotFoundError:
                # 第一次运行，本地文件还未创建，使用默认值
                self.my_log(f"首次运行，初始化trading_days_since_rebalance={self.trading_days_since_rebalance}, first_run={self.first_run}，将在盘后保存", force_print=True)
            except Exception as e:
                self.my_log(f"本地加载异常: {e}，使用默认值trading_days_since_rebalance={self.trading_days_since_rebalance}, first_run={self.first_run}", 'warn')
        
        # 间隔模式下，每个交易日开始时增加计数器
        if self.rebalance_mode == 'interval' and not self.first_run:
            self.trading_days_since_rebalance += 1
            self.my_log(f"交易日计数: 距离上次调仓{self.trading_days_since_rebalance}个交易日")
        
        # 小市值早盘变量预处理
        self._prepare_xsz(context)
        
    def handle_data(self, context, data=None):
        """盘中：在指定时间执行买卖操作"""
        current_time = context.current_dt.strftime("%H:%M")
        current_weekday = context.current_dt.weekday()  # 0=Monday, 1=Tuesday, ...
        current_date = context.current_dt.date()
        
        # 在不同时间点执行不同的策略操作
        if current_time == self.prepare_time:
            self._prepare_xsz(context)
            if self.DBL_control:
                self._check_dbl(context)
        elif current_time == self.sell_time:  # 每日执行
            # 判断是否需要调仓
            should_rebalance = self._should_rebalance_today(current_weekday, current_date)
            if should_rebalance:
                self._strategy_sell(context)
        elif current_time == self.buy_time:   # 每日执行
            # 判断是否需要调仓
            should_rebalance = self._should_rebalance_today(current_weekday, current_date)
            if should_rebalance:
                self._strategy_buy(context)
                # 调仓完成后重置计数器（间隔模式下）
                if self.rebalance_mode == 'interval':
                    self.trading_days_since_rebalance = 0
                    self.my_log("调仓完成，重置交易日计数器", force_print=True)
            # 首次运行完成后标记完成
            if self.first_run:
                self.first_run = False  # 标记首次运行已完成
        elif current_time in [self.stop_loss_time_1,self.stop_loss_time_2]:
            self._xsz_sell_stocks(context)
        elif current_time == self.limit_up_check_time:
            self._xsz_check_limit_up(context)
        elif current_time == self.close_account_time:
            self._close_account(context)

    def after_trading_end(self, context, data=None):
        """盘后：可留空"""
        # 获取指数成分股并剔除ST股票
        self.cur_strategy_stocks = self._filter_stocks_st_only(context)
        
        # 数据持久化
        if is_trade():
            try:
                with open(get_research_path() + 'SmallMarketCapStrategy.pkl', 'wb') as f:
                    # 使用字典格式保存，包含 trading_days_since_rebalance 和 first_run
                    saved_data = {
                        'trading_days_since_rebalance': self.trading_days_since_rebalance,
                        'first_run': self.first_run
                    }
                    pickle.dump(saved_data, f)
                    self.my_log(f"本地保存成功，trading_days_since_rebalance:{self.trading_days_since_rebalance}, first_run:{self.first_run}", force_print=True)
            except Exception as e:
                self.my_log(f"本地保存失败，trading_days_since_rebalance:{self.trading_days_since_rebalance}, first_run:{self.first_run}, 错误:{e}", force_print=True)

    # ==================== 小市值策略核心函数 ====================
    
    def _prepare_xsz(self, context):
        """小市值早盘变量预处理"""
        self.my_log(f"准备小市值策略变量trading_signal:{self.trading_signal}")
        self.trading_signal = False if context.current_dt.month in self.empty_months else True
        self.yesterday_HL_list = []
        
        # 获取当前持仓
        current_holdings = self._get_current_holdings(context)
        
        # 获取昨日涨停列表
        if current_holdings:
            try:
                df = get_history(count=1, frequency='1d', field=['close', 'high_limit', 'low_limit'],
                                 security_list=current_holdings, fq='pre')
                if not df.empty:
                    self.yesterday_HL_list = list(df[df['close'] == df['high_limit']].code)
            except Exception as e:
                self.my_log(f"获取昨日涨停列表失败: {e}", 'warn')
    
    def _should_rebalance_today(self, current_weekday, current_date):
        """
        判断今天是否需要调仓
        :param current_weekday: 当前星期 (0=周一, 1=周二, ...)
        :param current_date: 当前日期
        :return: True/False
        """
        # 首次运行时总是调仓
        if self.first_run:
            self.my_log("首次运行，执行调仓", force_print=True)
            return True
        
        # 根据调仓模式判断
        if self.rebalance_mode == 'weekday':
            # 固定星期调仓模式
            if current_weekday in self.rebalance_weekdays:
                self.my_log(f"到达调仓日（星期{current_weekday + 1}），执行调仓", force_print=True)
                return True
        elif self.rebalance_mode == 'interval':
            # 间隔天数调仓模式：使用交易日计数器
            if self.trading_days_since_rebalance >= self.rebalance_interval:
                self.my_log(f"距离上次调仓已经过了{self.trading_days_since_rebalance}个交易日（间隔配置:{self.rebalance_interval}），执行调仓", force_print=True)
                return True
            else:
                self.my_log(f"距离上次调仓{self.trading_days_since_rebalance}个交易日，未达间隔{self.rebalance_interval}个交易日，不调仓")
                return False
        
        return False
    
    def _strategy_sell(self, context):
        """小市值卖出逻辑"""
        self.my_log("开始执行小市值卖出逻辑", force_print=True)
        self.target_list = []
        
        # 近期有顶背离信号时暂停调仓（规避系统性风险）
        if self.DBL_control:
            # 首次运行检测最近10日顶背离
            if len(self.dbl) < 10:
                for i in range(9, -1, -1):
                    self._check_dbl(context, end_days=0 - i)
            
            if self.DBL_control and 1 in self.dbl[-self.check_dbl_days:]:
                self.my_log(f"近{self.check_dbl_days}日检测到大盘顶背离，暂停调仓以控制风险")
                return

        # 检测空仓期
        month = context.current_dt.month
        if month in self.empty_months:
            self.trading_signal = False
        
        if not self.trading_signal:
            return

        # 动态调整选股数量
        diff = None
        if self.enable_dynamic_stock_num:
            ma_para = 10  # 设置MA参数
            try:
                index_df = get_history(count=ma_para * 2, frequency='1d', field=['close', 'volume'],
                                       security_list='399101.SZ', fq='pre')
                if not index_df.empty:
                    index_df = index_df[index_df['volume'] > 0]
                    index_df['ma'] = index_df['close'].rolling(window=ma_para).mean()
                    if not index_df.empty:
                        last_row = index_df.iloc[-1]
                        diff = last_row['close'] - last_row['ma']
                        self.xsz_stock_num = 3 if diff >= 500 else \
                            3 if 200 <= diff < 500 else \
                                4 if -200 <= diff < 200 else \
                                    5 if -500 <= diff < -200 else \
                                        6
            except Exception as e:
                self.my_log(f"计算动态持股数量失败: {e}", 'warn')

        # 选择要启用的选股版本
        try:
            self.target_list = self._get_stock_list(context)[:self.xsz_stock_num]
            self.my_log(f'小市值 {self.xsz_version} 目标持股数: {self.xsz_stock_num} [diff:{str(diff)[:6]}] 目标持仓: {self.target_list}')
        except Exception as e:
            self.my_log(f"选股失败: {e}", 'error')
            return

        # 获取当前持仓
        current_holdings = self._get_current_holdings(context)
        
        # 卖出不在目标列表中的股票（除昨日涨停股）
        sell_list = [s for s in current_holdings if s not in self.target_list and s not in self.yesterday_HL_list]
        hold_list = [s for s in current_holdings if s in self.target_list or s in self.yesterday_HL_list]
        
        if sell_list:
            if hold_list:
                self.my_log(f"当前持有: {hold_list}")
            self.my_log(f"计划卖出: {sell_list}")
            
            for stock in sell_list:
                self.my_log(f"卖出股票: {stock}", force_print=True)
                self.order_target_value_with_split(context, stock, 0)
    
    def _strategy_buy(self, context):
        """小市值买入逻辑"""
        self.my_log("开始执行小市值买入逻辑", force_print=True)
        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        if not self.trading_signal:
            return

        # 计算目标持仓数，确保所有目标股票均分资金
        target_count = len(self.target_list)
        if target_count == 0:
            return
            
        # 计算每只股票的分配资金（目标总资金除以目标股票数量）
        target_cash_per_stock = self.capital / target_count
        
        # 买入/调整所有目标标的到目标资金量
        for stock in self.target_list:
            self.my_log(f"买入/调整股票: {stock}, 目标金额: {target_cash_per_stock:.2f}", force_print=True)
            self.order_target_value_with_split(context, stock, target_cash_per_stock)
    
    def _xsz_sell_stocks(self, context):
        """小市值止损止盈逻辑"""
        if not self.run_stoploss:
            return
            
        self.my_log("开始执行小市值止损止盈逻辑", force_print=True)
        
        current_holdings = self._get_current_holdings(context)
        if not current_holdings:
            return
            
        try:
            # 获取当前持仓信息
            positions = context.portfolio.positions
            
            if self.stoploss_strategy in [1, 3]:
                for stock in current_holdings:
                    if stock in positions:
                        pos = positions[stock]
                        price = pos.last_sale_price
                        avg_cost = pos.cost_basis
                        
                        # 个股盈利止盈
                        take_profit_price = avg_cost * (1 + self.take_profit_ratio)
                        if price >= take_profit_price:
                            self.my_log(f"收益{self.take_profit_ratio*100:.0f}%止盈,卖出 {stock}", force_print=True)
                            self.order_target_value_with_split(context, stock, 0)
                        # 个股止损
                        elif price < avg_cost * (1 - self.stoploss_limit):
                            self.my_log(f"收益止损,卖出 {stock}", force_print=True)
                            self.order_target_value_with_split(context, stock, 0)
            
            if self.stoploss_strategy in [2, 3]:
                try:
                    stock_df = get_history(count=1, frequency='1d', field=['close', 'open'],
                                           security_list=get_index_stocks('399101.SZ'), fq='pre')
                    if not stock_df.empty:
                        down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
                        # 市场大跌止损
                        if down_ratio >= self.stoploss_market:
                            self.my_log(f"大盘惨跌,平均降幅 {down_ratio:.2%}", force_print=True)
                            for stock in current_holdings:
                                self.order_target_value_with_split(context, stock, 0)
                except Exception as e:
                    self.my_log(f"市场趋势止损计算失败: {e}", 'warn')
        except Exception as e:
            self.my_log(f"止损止盈执行失败: {e}", 'error')
    
    def _xsz_check_limit_up(self, context):
        """检查昨日涨停股今日表现"""
        self.my_log("开始检查昨日涨停股表现", force_print=True)
        
        # 获取当前持仓
        holdings = self._get_current_holdings(context)
        if not holdings or not self.yesterday_HL_list:
            return
            
        try:
            # 获取最新价格数据
            current_data_all = self._get_current_data_ptrade_new(self.yesterday_HL_list)
            for stock in self.yesterday_HL_list:
                if stock in current_data_all:
                    current_data = current_data_all[stock]
                    if current_data['last_price'] < current_data['high_limit']:
                        self.my_log(f"{stock} 涨停打开，卖出", force_print=True)
                        self.order_target_value_with_split(context, stock, 0)
                    else:
                        self.my_log(f"{stock} 继续涨停，继续持有", force_print=True)
        except Exception as e:
            self.my_log(f"检查涨停股表现失败: {e}", 'warn')
    
    def _close_account(self, context):
        """清仓后次日资金可转"""
        if not self.trading_signal:
            current_holdings = self._get_current_holdings(context)
            
            # 空仓
            for stock in current_holdings:
                self.my_log(f"进入清仓期间 卖出 {stock}", force_print=True)
                self.order_target_value_with_split(context, stock, 0)
            
            # 如果配置为空仓时购买固定ETF
            if hasattr(self, 'empty_fund_allocation') and self.empty_fund_allocation == 'fixed_etf':   
                # 买入配置的固定ETF（如银华日利）
                if self.xsz_buy_etf and self.xsz_buy_etf not in current_holdings:
                    # 更新策略资金
                    if not is_trade():
                        fixed_etf_capital = context.portfolio.portfolio_value * self.account_ratio
                    else:
                        fixed_etf_capital = g.real_trading_cash_use * self.account_ratio
                    
                    self.my_log(f"空仓期购买固定ETF: {self.xsz_buy_etf}, 金额: {fixed_etf_capital:.2f}", force_print=True)
                    self.order_target_value_with_split(context, self.xsz_buy_etf, fixed_etf_capital)
    
    def _check_dbl(self, context, market_index='399101.SZ', end_days=0):
        """大盘顶背离检测"""
        try:
            def detect_divergence():
                """检测顶背离"""
                fast, slow, sign = 12, 26, 9  # MACD参数
                rows = (fast + slow + sign) * 5  # 确保足够数据量
                
                # 获取历史收盘价数据
                grid = get_history(count=rows + 10, frequency='1d', field='close', 
                                   security_list=market_index, fq='pre').dropna()
                if end_days < 0:
                    grid = grid.iloc[:end_days]

                if len(grid) < rows:
                    return False

                try:
                    # 计算MACD指标
                    grid['dif'], grid['dea'], grid['macd'] = self._mcad(grid.close, fast, slow, sign)

                    # 寻找死叉点
                    mask = (grid['macd'] < 0) & (grid['macd'].shift(1) >= 0)
                    if mask.sum() < 2:  # 需要至少2个死叉点对比
                        return False

                    # 取最近两个死叉点
                    key2, key1 = mask[mask].index[-2], mask[mask].index[-1]

                    # 顶背离核心条件
                    price_cond = grid.close[key2] < grid.close[key1]  # 价格创新高
                    dif_cond = grid.dif[key2] > grid.dif[key1] > 0  # DIF未创新高
                    macd_cond = grid.macd.iloc[-2] > 0 > grid.macd.iloc[-1]  # MACD由正转负

                    # 趋势验证：DIF近期处于下降趋势
                    if len(grid['dif']) > 20:
                        recent_avg = grid['dif'].iloc[-10:].mean()  # 近10日DIF均值
                        prev_avg = grid['dif'].iloc[-20:-10].mean()  # 前10日DIF均值
                        trend_cond = recent_avg < prev_avg
                    else:
                        trend_cond = False

                    return price_cond and dif_cond and macd_cond and trend_cond

                except Exception as e:
                    self.my_log(f"{market_index} 顶背离检测错误: {e}", 'warn')
                    return False
            self.my_log(f"开始大盘顶背离检测", force_print=True)
            if detect_divergence():
                self.dbl.append(1)
                self.my_log(f"检测到{market_index}顶背离信号", force_print=True)
                # 卖出当前非涨停股票
                current_holdings = self._get_current_holdings(context)
                if current_holdings:
                    current_data_all = self._get_current_data_ptrade_new(current_holdings)
                    for stock in current_holdings:
                        # 当前未涨停的股票清仓
                        if stock in current_data_all:
                            current_data = current_data_all[stock]
                            if current_data['last_price'] < current_data['high_limit'] * 0.99:
                                self.my_log(f"{stock} 因大盘顶背离清仓（非涨停股）", force_print=True)
                                self.order_target_value_with_split(context, stock, 0)
            else:
                self.dbl.append(0)
        except Exception as e:
            self.my_log(f"顶背离检测失败: {e}", 'error')

    # ==================== 辅助函数 ====================
    
    def _get_stock_list(self, context):
        """根据版本选择选股函数"""
        if self.xsz_version == "v1":
            return self._xsz_get_stock_list_v1(context)
        elif self.xsz_version == "v2":
            return self._xsz_get_stock_list_v2(context)
        elif self.xsz_version == "v3":
            return self._xsz_get_stock_list_v3(context)
        else:
            self.my_log(f"未知的选股版本: {self.xsz_version}，使用默认v3版本", 'warn')
            return self._xsz_get_stock_list_v3(context)
    
    def _xsz_get_stock_list_v1(self, context):
        """v1 选股模块 (双市值+行业分散)"""
        try:
            # 获取小市值股票池
            initial_list = self._filter_stocks(context, get_index_stocks('399101.SZ'))
            
            # 获取基本面数据
            df_val = get_fundamentals(
                security=initial_list,
                table='valuation',
                fields=['secu_abbr', 'float_value', 'total_value'],
                date=context.previous_date
            )
            
            if df_val is None or df_val.empty:
                return []
                
            # 数据处理
            df_val = df_val.reset_index()
            
            # 检测并规范代码列名
            code_col = None
            for c in ['secu_code', 'code', 'index', 'level_0', 'secu_abbr']:
                if c in df_val.columns:
                    code_col = c
                    break
            if code_col is None:
                code_col = df_val.columns[0]
            if code_col != 'secu_code':
                df_val = df_val.rename(columns={code_col: 'secu_code'})

            # 把 float_value / total_value 从字符串转为数值，并换算为 亿元
            for col in ['float_value', 'total_value']:
                if col in df_val.columns:
                    df_val[col] = pd.to_numeric(df_val[col], errors='coerce') / 1e8

            # 丢弃没有流通市值的数据
            if 'float_value' in df_val.columns:
                df_val = df_val.dropna(subset=['float_value'])
            else:
                df_val = df_val.dropna(subset=['total_value'])

            # 优先用 total_value 排序
            sort_col = 'total_value' if 'total_value' in df_val.columns else 'float_value'
            df_val = df_val.sort_values(sort_col, ascending=True)

            # 取前50只
            initial_list = df_val['secu_code'].astype(str).tolist()[:50]
            initial_list = initial_list[:30]
            
            # 行业分散选股（每个行业选一个股票）
            final_list = self._filter_industry_stock(initial_list)[:self.xsz_stock_num]
            return final_list
        except Exception as e:
            self.my_log(f"v1选股失败: {e}", 'error')
            return []
    
    def _xsz_get_stock_list_v2(self, context):
        """v2 选股模块 (国九+roa+roe)"""
        try:
            initial_list = self._filter_stocks(context, get_index_stocks('399101.SZ'))
            
            # 获取各表数据
            df_val = get_fundamentals(
                security=initial_list,
                table='valuation',
                fields=['total_value', 'turnover_rate'],
                date=context.previous_date
            )
            
            df_inc = get_fundamentals(
                security=initial_list,
                table='income_statement',
                fields=['np_parent_company_owners', 'net_profit','operating_revenue'],
                date=context.previous_date
            )
            
            df_ind = get_fundamentals(
                security=initial_list,
                table='profit_ability',
                fields=['roe', 'roa'],
                date=context.previous_date
            )

            # 合并表格
            if df_val is None or df_val.empty:
                df = pd.DataFrame()
            else:
                df = df_val.copy()
                if df_inc is not None and not df_inc.empty:
                    df = df.merge(df_inc, on='secu_code', how='left')
                if df_ind is not None and not df_ind.empty:
                    df = df.merge(df_ind, on='secu_code', how='left')

            # 过滤与排序
            if df.empty:
                df = pd.DataFrame(columns=['secu_code'])
            else:
                # 强制数值类型并去掉缺失
                for col in ['total_value', 'np_parent_company_owners', 'net_profit', 'operating_revenue', 'roe', 'roa']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                df['total_value'] = df['total_value'] / 100000000 # 转为亿元单位
                df = df.dropna(subset=['total_value', 'net_profit', 'operating_revenue', 'roe', 'roa'])

                # 应用条件
                df = df[
                    (df['total_value'] >= 5) & (df['total_value'] <= 50) &
                    (df['np_parent_company_owners'] > 0) &
                    (df['net_profit'] > 0) &
                    (df['operating_revenue'] > 1e8) &
                    (df['roe'] > 0.15) &
                    (df['roa'] > 0.10)
                ]

                # 按 total_value 升序并取前 50
                df = df.sort_values(by='total_value', ascending=True).head(50)

            if df.empty:
                return []
                
            final_list = list(df.code) if 'code' in df.columns else []
            
            # 价格过滤
            if final_list:
                last_prices = get_history(1, '1d', 'close', final_list, fq='pre')
                positions = context.portfolio.positions
                final_list = [stock for stock in final_list 
                             if stock in positions or 
                             (not last_prices.empty and 
                              last_prices.query(f'code in ["{stock}"]')['close'].iloc[-1] <= 20)][:self.xsz_stock_num]
            
            return final_list
        except Exception as e:
            self.my_log(f"v2选股失败: {e}", 'error')
            return []
    
    def _xsz_get_stock_list_v3(self, context):
        """v3 选股模块 (国九+红利+审计)"""
        try:
            initial_list = self._filter_stocks(context, get_index_stocks('399101.SZ'))
            
            # 拉取各表数据
            df_val = get_fundamentals(security=initial_list, table='valuation',
                                      fields=['total_value'], date=context.previous_date)
            df_inc = get_fundamentals(security=initial_list, table='income_statement',
                                      fields=['net_profit', 'operating_revenue'], date=context.previous_date)
            df_pa = get_fundamentals(security=initial_list, table='profit_ability',
                                     fields=['roe', 'roa'], date=context.previous_date)

            # 兼容返回格式并重置索引
            dfs = []
            for df in (df_val, df_inc, df_pa):
                if df is not None and not df.empty:
                    dfs.append(df.reset_index())

            if not dfs:
                self.my_log('xsz_v3: 无基本面数据返回')
                return [self.xsz_buy_etf]

            # 以 valuation为基础合并其它表
            df = dfs[0].copy()
            
            # 标准化代码列名
            code_col = None
            for c in ['secu_code', 'code', 'index', 'level_0', 'secu_abbr']:
                if c in df.columns:
                    code_col = c
                    break
            if code_col and code_col != 'secu_code':
                df = df.rename(columns={code_col: 'secu_code'})

            # 合并其它表
            def _ensure_code_and_merge(base_df, other_df):
                if other_df is None or other_df.empty:
                    return base_df
                other = other_df.reset_index()
                other_code = None
                for c in ['secu_code', 'code', 'index', 'level_0', 'secu_abbr']:
                    if c in other.columns:
                        other_code = c
                        break
                if other_code and other_code != 'secu_code':
                    other = other.rename(columns={other_code: 'secu_code'})
                if 'secu_code' not in other.columns:
                    other['secu_code'] = other.iloc[:, 0].astype(str)
                return base_df.merge(other, on='secu_code', how='left')

            if df_inc is not None:
                df = _ensure_code_and_merge(df, df_inc)
            if df_pa is not None:
                df = _ensure_code_and_merge(df, df_pa)

            # 强制数值并单位换算
            for col in ['total_value', 'net_profit', 'operating_revenue', 'roe', 'roa']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'total_value' in df.columns:
                df['total_value'] = df['total_value'] / 1e8  # 转为亿元

            # 过滤条件
            cond_cols = ['total_value', 'operating_revenue', 'roe', 'roa', 'net_profit']
            if not set(cond_cols).issubset(set(df.columns)):
                self.my_log('xsz_v3: 基本面数据字段缺失')
                return [self.xsz_buy_etf]

            df = df.dropna(subset=['total_value', 'operating_revenue', 'roe', 'roa', 'net_profit'])
            df = df[
                (df['total_value'] >= 10) &
                (df['total_value'] <= 100) &
                (df['operating_revenue'] > 1e8) &
                (df['roe'] > 0) &
                (df['roa'] > 0) &
                (df['net_profit'] > 2000000)
            ]

            if df.empty:
                self.my_log('xsz_v3: 无符合基本面条件的股票')
                return [self.xsz_buy_etf]

            # 按市值升序并取候选
            df = df.sort_values(by='total_value', ascending=True).head(self.xsz_stock_num * 5)

            # 取代码列
            if 'secu_code' in df.columns:
                final_list = df['secu_code'].astype(str).tolist()
            elif 'code' in df.columns:
                final_list = df['code'].astype(str).tolist()
            else:
                final_list = df.iloc[:, 0].astype(str).tolist()
                
            if not final_list:
                self.my_log('无适合股票，买入ETF')
                return [self.xsz_buy_etf]
                
            # 价格过滤
            if final_list:
                last_prices = get_history(1, frequency='1d', field='close', security_list=final_list)
                if not last_prices.empty:
                    positions = context.portfolio.positions
                    final_list = [s for s in final_list 
                                 if s in positions or 
                                 (not last_prices.empty and 
                                  last_prices.query(f'code in ["{s}"]')['close'].iloc[-1] <= 50)][:self.xsz_stock_num]
            
            return final_list
        except Exception as e:
            self.my_log(f"v3选股失败: {e}", 'error')
            return [self.xsz_buy_etf]
    
    def _filter_industry_stock(self, stock_list):
        """行业分散选股"""
        try:
            selected_stocks = []
            industry_list = []
            for stock_code in stock_list:
                try:
                    # 获取股票所属行业
                    blocks_info = get_stock_blocks(stock_code)
                    industry_name = blocks_info['HY'][0][1] if blocks_info['HY'] else '未知行业'
                    if industry_name not in industry_list:
                        industry_list.append(industry_name)
                        selected_stocks.append(stock_code)
                        # 选取了指定数量个不同行业的股票
                        if len(industry_list) == self.xsz_stock_num:
                            break
                except Exception as e:
                    self.my_log(f"获取股票{stock_code}行业信息失败: {e}", 'warn')
                    continue
            return selected_stocks
        except Exception as e:
            self.my_log(f"行业分散选股失败: {e}", 'error')
            return stock_list[:self.xsz_stock_num] if stock_list else []
    
    def _filter_stocks_st_only(self, context):
        """只过滤ST股票，不进行其他复杂过滤，提高效率"""
        try:
            stock_list = get_index_stocks('399101.SZ')
            # 获取ST状态
            is_ST_all = get_stock_status(stock_list, query_type='ST', query_date=context.previous_date)
            stock_infos = get_stock_info(stock_list, field=['stock_name'])
            
            filtered_stocks = []
            filtered_out_stocks = []  # 记录被过滤的ST股票
            
            for stock in stock_list:
                try:
                    is_ST = is_ST_all.get(stock, False)
                    stock_name = stock_infos[stock].get('stock_name', '') if stock_infos[stock] else ''
                    
                    # 基于股票名称过滤ST股票
                    if stock_name and ('ST' in stock_name or '*ST' in stock_name or 'st' in stock_name or '*st' in stock_name or 'S\'T' in stock_name):
                        filtered_out_stocks.append(f"{stock}({stock_name})-名称含ST")
                        continue
                    # 如果系统标记为ST也过滤
                    if is_ST:
                        filtered_out_stocks.append(f"{stock}({stock_name})-系统标记ST")
                        continue
                    
                    filtered_stocks.append(stock)
                except Exception as e:
                    # 如果出现异常，跳过该股票
                    self.my_log(f"ST过滤股票 {stock} 时发生异常：{str(e)}，跳过该股票", 'warn')
                    continue
            return filtered_stocks
        except Exception as e:
            self.my_log(f"ST股票过滤失败: {e}", 'error')
            # 如果出错，返回原列表
            return stock_list

    def _filter_stocks(self, context, stock_list):
        """基础过滤各种股票"""
        try:
            # 涨跌停和最近价格的判断
            last_prices = get_history(1, frequency='1m', field='close', security_list=stock_list, fq='pre')
            
            # 过滤标准
            filtered_stocks = []
            is_ST_all = get_stock_status(stock_list, query_type='ST', query_date=context.previous_date)
            is_paused_all = get_stock_status(stock_list, query_type='HALT', query_date=context.previous_date)
            is_de_listed_all = get_stock_status(stock_list, query_type='DELISTING', query_date=context.previous_date)
            cur_position = context.portfolio.positions
            stock_infos = get_stock_info(stock_list, field=['stock_name', 'listed_date'])
            
            for stock in stock_list:
                try:
                    is_ST = is_ST_all.get(stock, False)
                    is_paused = is_paused_all.get(stock, False)
                    is_de_listed = is_de_listed_all.get(stock, False)
                    stock_name = stock_infos[stock].get('stock_name', '') if stock_infos[stock] else ''

                    # 过滤条件
                    if stock_name and ('ST' in stock_name or 'st' in stock_name):
                        continue
                    if is_paused or is_ST or is_de_listed:
                        continue
                    if stock.startswith('30') or stock.startswith('68') or stock.startswith('8') or stock.startswith('4') or stock.startswith('9'):
                        continue
                        
                    if not last_prices.empty:
                        stock_last_prices = last_prices.query(f'code in ["{stock}"]')
                        if not stock_last_prices.empty:
                            last_price = stock_last_prices['close'].iloc[-1]
                            
                            if not (stock in cur_position or last_price < float('inf')):  # 简化检查
                                continue
                            if not (stock in cur_position or last_price > 0):  # 简化检查
                                continue
                    
                    start_date = stock_infos[stock]['listed_date']
                    if start_date:
                        datetime_obj = datetime.strptime(start_date, "%Y-%m-%d")
                        date_obj = datetime_obj.date()
                        if context.previous_date - date_obj < timedelta(days=375):
                            continue
                    
                    filtered_stocks.append(stock)
                except Exception as e:
                    self.my_log(f"过滤股票 {stock} 时发生异常：{str(e)}，跳过该股票", 'warn')
                    continue
            return filtered_stocks
        except Exception as e:
            self.my_log(f"股票过滤失败: {e}", 'error')
            return []
    
    def _get_current_holdings(self, context):
        """获取当前策略持仓"""
        try:
            holdings = []
            # 获取小市值股票池
            xsz_pool = self._filter_stocks_st_only(context)
            
            # 获取当前所有持仓
            positions = context.portfolio.positions
            
            # 筛选出当前持仓中属于小市值股票池的股票
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in xsz_pool:
                    holdings.append(stock)
            
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'error')
            return []
    
    def _get_current_strategy_value(self, context):
        """获取当前策略持仓市值"""
        try:
            current_value = 0
            holdings = self._get_current_holdings(context)
            positions = context.portfolio.positions
            
            for stock in holdings:
                if stock in positions:
                    pos = positions[stock]
                    current_value += pos.last_sale_price * pos.amount
            
            return current_value
        except Exception as e:
            self.my_log(f"计算策略持仓市值失败: {e}", 'error')
            return 0
    
    def _get_current_data_ptrade_new(self, stock_list):
        """获取股票最新数据"""
        try:
            if isinstance(stock_list, str):
                stock_list = [stock_list]
            
            current_data_new = {}
            his = get_history(1, frequency='1d', field=['high_limit', 'low_limit', 'open'], 
                              security_list=stock_list, fq='pre', include=True)
            his2 = get_history(1, frequency='1m', field=['price'], 
                               security_list=stock_list, fq='pre', include=False)
            
            for stock in stock_list:
                current_data = {}
                # 获取历史数据
                stock_his_data = his[his['code'] == stock]
                if not stock_his_data.empty:
                    current_data['high_limit'] = stock_his_data['high_limit'].iloc[-1]
                    current_data['low_limit'] = stock_his_data['low_limit'].iloc[-1]
                    current_data['day_open'] = stock_his_data['open'].iloc[-1]
                else:
                    continue
                    
                # 获取最新价
                stock_his2_data = his2[his2['code'] == stock]
                if not stock_his2_data.empty:
                    current_data['last_price'] = stock_his2_data['price'].iloc[-1]
                else:
                    continue
                    
                current_data_new[stock] = current_data
            
            return current_data_new
        except Exception as e:
            self.my_log(f"获取股票最新数据失败: {e}", 'error')
            return {}
    
    def _mcad(self, close, short=12, long=26, m=9):
        """计算MACD指标"""
        try:
            # 计算指数移动平均线
            def ema(series, n):
                return pd.Series.ewm(series, span=n, min_periods=n - 1, adjust=False).mean()
            
            dif = ema(close, short) - ema(close, long)
            dea = ema(dif, m)
            return dif, dea, (dif - dea) * 2
        except Exception as e:
            self.my_log(f"计算MACD指标失败: {e}", 'error')
            return pd.Series(), pd.Series(), pd.Series()

    def _get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            stock_name_data = get_stock_name([stock_code])
            return stock_name_data.get(stock_code, stock_code)
        except:
            return stock_code
if "small_market_cap" in strategy_config:
    strategy_config['small_market_cap']['class'] = SmallMarketCapStrategy

# ============================热点小市值策略类=============================
class HotSpotSmallCapStrategy(BaseStrategy):
    """
    热点小市值策略
    - 基于中小综指成分股的小市值股票池
    - 结合热点概念进行选股
    - 支持多种止损止盈机制
    - 支持每周调仓机制
    """

    def __init__(self, account_ratio=1.0, stock_num=10, run_stoploss=True, 
                 stoploss_strategy=3, stoploss_limit=0.94, stoploss_market=0.97,
                 HV_control=False, HV_duration=120, HV_ratio=0.9, pass_april=True,
                 trade_time=["10:00", "10:30",  "10:35", "14:30", "14:35", "14:50"],
                 rebalance_weekdays=[2],empty_fund_allocation='etf_rotation'):
        super().__init__(name="小市值策略", version="v1.0")
        # 小市值空仓资金分配(etf_rotation/None)
        self.empty_fund_allocation = empty_fund_allocation
        
        # 资金配置
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.capital = 0  # 运行时更新
        
        # 策略参数
        self.stock_num = stock_num  # 持股数量
        
        # 止损参数
        self.run_stoploss = run_stoploss
        self.stoploss_strategy = stoploss_strategy  # 1:个股止损 2:大盘止损 3:联合止损
        self.stoploss_limit = stoploss_limit  # 个股止损阈值
        self.stoploss_market = stoploss_market  # 大盘止损参数
        
        # 成交量控制
        self.HV_control = HV_control
        self.HV_duration = HV_duration
        self.HV_ratio = HV_ratio
        
        # 空仓期配置
        self.pass_april = pass_april
        
        # 策略时间参数
        self.trade_time = trade_time
        self.sell_time = trade_time[0]          # 止损时间
        self.rebalance_sell_time = trade_time[1]     # 调仓卖出时间
        self.rebalance_buy_time = trade_time[2]     # 调仓卖出时间
        self.check_time = trade_time[3]         # 检查时间
        self.remain_time = trade_time[4]         # 补仓时间
        self.close_time = trade_time[5]         # 清仓时间
        
        # 调仓配置
        self.rebalance_weekdays = rebalance_weekdays  # 调仓星期列表
        
        # 策略状态变量
        self.trading_signal = True  # 交易信号
        self.hold_list = []  # 当前持仓股票代码列表
        self.yesterday_HL_list = []  # 昨日涨停的股票列表
        self.target_list = []  # 本次调仓候选股票列表
        self.not_buy_again = []  # 当天已买入的股票列表
        self.reason_to_sell = ''  # 卖出原因
        self.zt = {}  # 涨停监控字典
        
        # 可能交易的股票池
        self.cur_strategy_stocks = []
        
        # 持久化变量
        self.count = 1  # 记录交易日
        self.trade_count = 0  # 记录总交易日
        self.up_tradeday = ''  # 上一个交易日
        
    def before_trading_start(self, context, data=None):
        """盘前：变量预处理和准备股票池"""
        super().before_trading_start(context, data)
        
        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # 尝试启动pickle文件
        if is_trade():
            try:
                with open(get_research_path() + 'HotSpotSmallCapStrategy_count.pkl', 'rb') as f:
                    self.count = pickle.load(f)
                    self.my_log(f"count本地加载成功，count:{self.count}", force_print=True)
            except FileNotFoundError:
                self.my_log(f"首次运行，初始化count={self.count}，将在盘后保存", force_print=True)
            except Exception as e:
                self.my_log(f"count本地加载异常: {e}，使用默认值{self.count}", 'warn')
                
            try:
                with open(get_research_path() + 'HotSpotSmallCapStrategy_trade_count.pkl', 'rb') as f:
                    self.trade_count = pickle.load(f)
                    self.my_log(f"trade_count本地加载成功，trade_count:{self.trade_count}", force_print=True)
            except FileNotFoundError:
                self.my_log(f"首次运行，初始化trade_count={self.trade_count}，将在盘后保存", force_print=True)
            except Exception as e:
                self.my_log(f"trade_count本地加载异常: {e}，使用默认值{self.trade_count}", 'warn')
        
        # 更新策略标的池，确保盘中交易时能正确识别持仓
        self.cur_strategy_stocks = self._filter_stocks_st_only(context)
        
        # 计算交易日计数
        self._record_count(context)
        
        # 盘前准备股票池（原09:05执行的逻辑）
        self._prepare_stock_list(context)
        
    def handle_data(self, context, data=None):
        """盘中：在指定时间执行买卖操作"""
        current_time = context.current_dt.strftime("%H:%M")
        current_weekday = context.current_dt.weekday()  # 0=Monday, 1=Tuesday, ...
        
        # 在不同时间点执行不同的策略操作
        if current_time == self.sell_time:
            self._sell_stocks(context)
        elif current_time == self.rebalance_sell_time:
            # 判断是否需要调仓
            self.my_log(f"当前第{self.trade_count}个交易日，周{self.count}，current_weekday:{current_weekday},self._should_rebalance_today(current_weekday)：{self._should_rebalance_today(current_weekday)}")
            if self._should_rebalance_today(current_weekday):
                self._weekly_adjustment_sell(context)
        elif current_time == self.rebalance_buy_time:
            # 判断是否需要调仓
            self.my_log(f"当前第{self.trade_count}个交易日，周{self.count}，current_weekday:{current_weekday},self._should_rebalance_today(current_weekday)：{self._should_rebalance_today(current_weekday)}")
            if self._should_rebalance_today(current_weekday):
                self._weekly_adjustment_buy(context)
        elif current_time == self.check_time:
            self._trade_afternoon(context)
        elif current_time == self.remain_time:
            # 检查是否需要补仓
            self._check_remain_amount(context)
        elif current_time == self.close_time:
            self._close_account(context)
    
    def _filter_stocks_st_only(self, context):
        """只过滤ST股票和ETF，不进行其他复杂过滤，提高效率"""
        try:
            stock_list = get_index_stocks('399101.SZ')
            # 获取ST状态
            is_ST_all = get_stock_status(stock_list, query_type='ST', query_date=context.previous_date)
            stock_infos = get_stock_info(stock_list, field=['stock_name'])
            
            filtered_stocks = []
            filtered_out_stocks = []  # 记录被过滤的ST股票
            
            for stock in stock_list:
                try:
                    is_ST = is_ST_all.get(stock, False)
                    stock_name = stock_infos[stock].get('stock_name', '') if stock_infos[stock] else ''
                    
                    # 基于股票名称过滤ST股票
                    if stock_name and ('ST' in stock_name or '*ST' in stock_name or 'st' in stock_name or '*st' in stock_name or 'S\'T' in stock_name):
                        filtered_out_stocks.append(f"{stock}({stock_name})-名称含ST")
                        continue
                    # 如果系统标记为ST也过滤
                    if is_ST:
                        filtered_out_stocks.append(f"{stock}({stock_name})-系统标记ST")
                        continue
                    
                    # 过滤掉ETF（代码以51、56、159开头的）
                    code_prefix = stock.split('.')[0]
                    if code_prefix.startswith('51') or code_prefix.startswith('56') or code_prefix.startswith('159'):
                        continue
                    
                    filtered_stocks.append(stock)
                except Exception as e:
                    # 如果出现异常，跳过该股票
                    self.my_log(f"ST过滤股票 {stock} 时发生异常：{str(e)}，跳过该股票", 'warn')
                    continue
            return filtered_stocks
        except Exception as e:
            self.my_log(f"ST股票过滤失败: {e}", 'error')
            # 如果出错，返回空列表
            return []

    def after_trading_end(self, context, data=None):
        """盘后：保存状态"""
        # 获取指数成分股并剔除ST股票
        self.cur_strategy_stocks = self._filter_stocks_st_only(context)
        
        # 数据持久化
        if is_trade():
            try:
                with open(get_research_path() + 'HotSpotSmallCapStrategy_count.pkl', 'wb') as f:
                    pickle.dump(self.count, f)
                    self.my_log(f"count本地保存成功，count:{self.count}", force_print=True)
            except:
                self.my_log(f"count本地保存失败，count:{self.count}", force_print=True)
                
            try:
                with open(get_research_path() + 'HotSpotSmallCapStrategy_trade_count.pkl', 'wb') as f:
                    pickle.dump(self.trade_count, f)
                    self.my_log(f"trade_count本地保存成功，trade_count:{self.trade_count}", force_print=True)
            except:
                self.my_log(f"trade_count本地保存失败，trade_count:{self.trade_count}", force_print=True)
    
    # ==================== 热点小市值策略核心函数 ====================
    
    def _record_count(self, context):
        """记录交易日计数"""
        today = context.blotter.current_dt
        weekdays = today.weekday() + 1
        current_date = str(context.current_dt.date())
        
        days = 0
        if self.up_tradeday != '':
            date_time_pre = datetime.strptime(self.up_tradeday, "%Y-%m-%d")
            date_time = datetime.strptime(current_date, "%Y-%m-%d")
            days = (date_time - date_time_pre).days
        else:
            self.count = 1
        
        if days > 1 or weekdays == 1:
            self.count = 1
        else:
            if days != 0:
                self.count += 1
        
        self.up_tradeday = current_date
        self.trade_count += 1
    
    def _should_rebalance_today(self, current_weekday):
        """
        判断今天是否需要调仓
        :param current_weekday: 当前星期 (0=周一, 1=周二, ...)
        :return: True/False
        """
        # 首次运行时总是调仓
        if self.trade_count == 1:
            self.my_log("首次运行，执行调仓", force_print=True)
            return True
        
        # 根据调仓模式判断
        self.my_log(f"current_weekday:{current_weekday},self.rebalance_weekdays:{self.rebalance_weekdays}", force_print=True)
        if current_weekday in self.rebalance_weekdays:
            self.my_log(f"到达调仓日（星期{current_weekday + 1}），执行调仓", force_print=True)
            return True
        
        return False
    
    def _prepare_stock_list(self, context):
        """准备股票池"""
        # 从当前持仓中提取股票代码，更新持仓列表
        self.hold_list = self._get_current_holdings(context)
        self.my_log(f'盘前准备：当前持仓 hold_list: {self.hold_list}', force_print=True)
        
        if self.hold_list:
            # 获取持仓股票昨日数据
            try:
                p = get_history(1, frequency="1d", field=['low', 'open', 'close', 'high_limit', 'volume'], 
                               security_list=self.hold_list, fq='pre', include=False)
                up_limit_list = list(p[p['close'] == p['high_limit']]['code'])
                self.yesterday_HL_list = up_limit_list
                self.my_log(f'昨日涨停股票 yesterday_HL_list: {self.yesterday_HL_list}', force_print=True)
            except Exception as e:
                self.my_log(f"获取昨日涨停列表失败: {e}", 'warn')
        else:
            self.yesterday_HL_list = []
            self.my_log(f'没有持仓，昨日涨停列表为空', force_print=True)
        
        # 根据当前日期判断是否为空仓日
        self.trading_signal = not self._today_is_between(context)
        self.my_log(f'交易信号: {self.trading_signal}, 持仓数: {len(self.hold_list)}', force_print=True)
    
    def _get_stock_list(self, context):
        """选股模块"""
        MKT_index = '399101.SZ'  # 中小综指
        # 使用 context.previous_date 获取昨日日期
        cur_formatted_time = context.previous_date.strftime("%Y%m%d")
        
        try:
            initial_list = self._filter_stocks(context, get_index_stocks(MKT_index, cur_formatted_time))
            initial_list = self._filter_new_stock(context, initial_list)  # 过滤次新股
            initial_list = self._filter_st_stock(context, initial_list)  # 过滤ST股票
            initial_list = self._filter_paused_stock(context, initial_list)  # 过滤停牌股票
            initial_list = self._filter_limitup_stock(context, initial_list)  # 过滤涨跌停股票
            
            # 获取流通市值数据
            circulating_market_cap_df = self._get_float_value(context, initial_list)
            if not circulating_market_cap_df.empty:
                # 按总市值排序
                sort_df = circulating_market_cap_df.sort_values(by='total_value', ascending=True)
                initial_list = list(sort_df.index)
                
                final_list = initial_list[:50]  # 限制数据规模
                
                # 加入热点概念选股
                # 使用 context.previous_date 获取昨日日期
                current_date = str(context.previous_date)
                final_list = self._get_hl_industry(final_list, current_date, n=7)
                
                # 取前2倍目标持仓股票数作为候选池
                final_list = final_list[:2 * self.stock_num]
                self.my_log(f"初选候选股票: {final_list}")
                return final_list
        except Exception as e:
            self.my_log(f"选股失败: {e}", 'error')
        
        return []
    
    def _weekly_adjustment_sell(self, context):
        """每周调仓卖出"""
        if not self.trading_signal:
            return
        if self.trade_count == 1 or self.count == 2:
            self.not_buy_again = []  # 重置当天已买入记录
            self.target_list = self._get_stock_list(context)
            # 取目标持仓数以内的股票作为调仓目标
            target_list = self.target_list[:self.stock_num]
            self.my_log(f"每周调仓目标股票: {target_list}")
            
            # 遍历当前持仓，若股票不在目标列表且非昨日涨停，则执行卖出操作
            for stock in self.hold_list:
                if stock not in target_list and stock not in self.yesterday_HL_list:
                    self.my_log(f"卖出股票 {stock}")
                    self.order_target_value_with_split(context, stock, 0)
                else:
                    self.my_log(f"持有股票 {stock}")

    def _weekly_adjustment_buy(self, context):
        """每周调仓卖出"""
        if not self.trading_signal:
            return
        if self.trade_count == 1 or self.count == 2:
            # self.not_buy_again = []  # 重置当天已买入记录
            # 取目标持仓数以内的股票作为调仓目标
            target_list = self.target_list[:self.stock_num]

            # 对目标股票执行买入操作
            self._buy_security(context, target_list)
            
            # 更新当天已买入记录
            for stock in target_list:
                if stock not in self.not_buy_again:
                    self.not_buy_again.append(stock)
    
    def _sell_stocks(self, context):
        """止损与止盈操作"""
        if not self.run_stoploss:
            return
        
        current_positions = self._get_current_holdings(context)
        if not current_positions:
            return
        
        if self.stoploss_strategy == 1:
            # 个股止盈或止损判断
            for stock in current_positions:
                position = context.portfolio.positions.get(stock)
                if not position:
                    continue
                price = position.last_sale_price
                avg_cost = position.cost_basis
                if price >= avg_cost * 2:
                    self.order_target_value_with_split(context, stock, 0)
                    self.my_log(f"收益100%止盈,卖出{stock}")
                elif price < avg_cost * self.stoploss_limit:
                    self.order_target_value_with_split(context, stock, 0)
                    self.my_log(f"股票 {stock} 触及止损阈值，执行卖出。")
                    self.reason_to_sell = 'stoploss'
        elif self.stoploss_strategy == 2:
            # 大盘止损判断
            try:
                # 优化策略，剖除未来数据：使用当日开盘价 + 实时价格
                current_date = str(context.current_dt.date())
                current_date = current_date.replace('-', '')
                index_stocks = get_index_stocks('399101.SZ', current_date)
                        
                # 获取当日开盘价（include=True 获取当日数据）
                his_day = get_history(1, frequency='1d', field=['open'], security_list=index_stocks, fq='pre', include=True)
                # 获取实时价格（include=False 获取最近一分钟数据）
                his_min = get_history(1, frequency='1m', field=['price'], security_list=index_stocks, fq='pre', include=False)
                        
                # 计算每只股票的跌幅（当前价/开盘价）
                down_ratios = []
                for stock in index_stocks:
                    try:
                        stock_day = his_day[his_day['code'] == stock]
                        stock_min = his_min[his_min['code'] == stock]
                                
                        if not stock_day.empty and not stock_min.empty:
                            open_price = stock_day['open'].iloc[-1]
                            current_price = stock_min['price'].iloc[-1]
                                    
                            if open_price > 0:
                                down_ratios.append(current_price / open_price)
                    except Exception as e:
                        continue
                        
                if down_ratios:
                    down_ratio = sum(down_ratios) / len(down_ratios)
                    self.my_log(f"大盘实时跌幅: {down_ratio}, 止损阈值: {self.stoploss_market}")
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        self.my_log(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in current_positions:
                            self.order_target_value_with_split(context, stock, 0)
                else:
                    self.my_log("无法获取大盘实时数据，跳过大盘止损判断", 'warn')
            except Exception as e:
                self.my_log(f"大盘止损计算失败: {e}", 'warn')
        elif self.stoploss_strategy == 3:
            # 联合止损策略
            try:
                # 优化策略，剖除未来数据：使用当日开盘价 + 实时价格
                current_date = str(context.current_dt.date())
                current_date = current_date.replace('-', '')
                index_stocks = get_index_stocks('399101.SZ', current_date)
                
                # 获取当日开盘价（include=True 获取当日数据）
                his_day = get_history(1, frequency='1d', field=['open'], security_list=index_stocks, fq='pre', include=True)
                # 获取实时价格（include=False 获取最近一分钟数据）
                his_min = get_history(1, frequency='1m', field=['price'], security_list=index_stocks, fq='pre', include=False)
                
                # 计算每只股票的跌幅（当前价/开盘价）
                down_ratios = []
                for stock in index_stocks:
                    try:
                        stock_day = his_day[his_day['code'] == stock]
                        stock_min = his_min[his_min['code'] == stock]
                        
                        if not stock_day.empty and not stock_min.empty:
                            open_price = stock_day['open'].iloc[-1]
                            current_price = stock_min['price'].iloc[-1]
                            
                            if open_price > 0:
                                down_ratios.append(current_price / open_price)
                    except Exception as e:
                        continue
                
                if down_ratios:
                    down_ratio = sum(down_ratios) / len(down_ratios)
                    self.my_log(f"大盘实时跌幅: {down_ratio}, 止损阈值: {self.stoploss_market}")
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        self.my_log(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in current_positions:
                            self.order_target_value_with_split(context, stock, 0)
                    else:
                        # 大盘没有跌破阈值，检查个股止损
                        for stock in current_positions:
                            position = context.portfolio.positions.get(stock)
                            if not position:
                                continue
                            price = position.last_sale_price
                            avg_cost = position.cost_basis
                            if price < avg_cost * self.stoploss_limit:
                                self.order_target_value_with_split(context, stock, 0)
                                self.my_log(f"股票 {stock} 触及止损阈值，执行卖出。")
                                self.reason_to_sell = 'stoploss'
                else:
                    self.my_log("无法获取大盘实时数据，跳过大盘止损判断", 'warn')
            except Exception as e:
                self.my_log(f"联合止损计算失败: {e}", 'warn')
    
    def _trade_afternoon(self, context):
        """下午交易任务"""
        if not self.trading_signal:
            return
        self.my_log(f"下午交易任务")
        # 检查涨停破板
        self._check_limit_up(context)
        
        # 检查成交量异常
        if self.HV_control:
            self._check_high_volume(context)
    
    def _check_limit_up(self, context):
        """检查昨日涨停股今日表现"""
        log.info(f"self.yesterday_HL_list:{self.yesterday_HL_list}")
        if not self.yesterday_HL_list:
            return
        
        for stock in self.yesterday_HL_list:
            try:
                if check_limit(stock)[stock] != 1:
                    self.my_log(f"股票 {stock} 涨停破板，触发卖出操作。")
                    self.order_target_value_with_split(context, stock, 0)
                    self.reason_to_sell = 'limitup'
                else:
                    self.my_log(f"股票 {stock} 仍维持涨停状态。")
            except Exception as e:
                self.my_log(f"检查涨停状态失败 {stock}: {e}", 'warn')
    
    def _check_remain_amount(self, context):
        """检查账户资金与持仓数量"""
        if self.reason_to_sell == 'limitup':
            self.hold_list = self._get_current_holdings(context)
            if len(self.hold_list) < self.stock_num:
                target_list = self._filter_not_buy_again(self.target_list)
                target_list = target_list[:min(self.stock_num, len(target_list))]                
                position_list = self._get_current_holdings(context)
                position_count = len(position_list)
                buy_count = self.stock_num - position_count
                self._buy_security(context, target_list[:buy_count])
            self.reason_to_sell = ''
        else:
            self.my_log("未检测到涨停破板卖出事件，不进行补仓买入。")
    
    def _check_high_volume(self, context):
        """检查成交量异常"""
        hold_list = self._get_current_holdings(context)
        if not hold_list:
            return
        
        try:
            for stock in hold_list:
                if check_limit(stock)[stock] == 1:
                    continue
                
                his1d = get_history(self.HV_duration, '1d', 'volume', security_list=stock, fq='pre')
                
                today_str = context.blotter.current_dt.strftime('%Y%m%d') + '093000'
                today_str_time = datetime.strptime(today_str, '%Y%m%d%H%M%S')
                diff_minues = int((context.blotter.current_dt - today_str_time).total_seconds() // 60)
                his1m = get_history(diff_minues, '1m', 'volume', security_list=stock, fq='pre')
                
                cur_volume = sum(list(his1m['volume']))
                his_volume_list = list(his1d['volume'])
                if cur_volume > self.HV_ratio * max(his_volume_list):
                    self.my_log(f"检测到股票 {stock} 出现异常放量，执行卖出操作。")
                    self.order_target_value_with_split(context, stock, 0)
        except Exception as e:
            self.my_log(f"成交量异常检测失败: {e}", 'warn')
    
    def _close_account(self, context):
        """清仓操作"""
        if not self.trading_signal:
            if self.hold_list:
                for stock in self.hold_list:
                    self.order_target_value_with_split(context, stock, 0)
                    self.my_log(f"空仓日平仓，卖出股票 {stock}。")
    
    def _buy_security(self, context, target_list):
        """买入操作"""
        try:
            # 更新策略资金
            if not is_trade():
                self.capital = context.portfolio.portfolio_value * self.account_ratio
            else:
                self.capital = g.real_trading_cash_use * self.account_ratio
            
            value = self.capital / self.stock_num
            print(f"portfolio_value:{context.portfolio.portfolio_value},account_ratio:{self.account_ratio},self.capital:{self.capital},self.stock_num:{self.stock_num},value:{value}")
        except ZeroDivisionError as e:
            self.my_log(f"资金分摇时除零错误: {e}")
            return
        
        self.my_log(f"准备买入:{target_list}，买入目标数量：{len(target_list)}")
        for stock in target_list:
            if stock not in self.zt:
                self.my_log(f"{stock},分配资金 {value}")
                self.my_log(f"已买入股票 {stock}，分配资金 {value:.2f}")
                self.order_target_value_with_split(context, stock, value)
                self.not_buy_again.append(stock)
                    
    
    # ==================== 辅助函数 ====================
    
    def _today_is_between(self, context):
        """判断当前日期是否为资金再平衡（空仓）日"""
        today_str = context.blotter.current_dt.strftime('%m-%d')
        if self.pass_april:
            if ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-31'):
                return True
        return False
    
    def _get_hl_industry(self, initial_list, date, n=7):
        """获取热点概念股票"""
        try:
            gnlis = []
            lis_df = []
            for code in initial_list:
                try:
                    dic = {}
                    blocks = get_stock_blocks(code)
                    gnlis.extend(blocks['GN'])
                    
                    lis = [i[0] for i in blocks['GN']]
                    dic[code] = lis
                    df = pd.DataFrame(dic)
                    df['code'] = code
                    df.columns = ['gn', 'code']
                    lis_df.append(df)
                except:
                    continue
            
            if not lis_df:
                return initial_list
            
            data = pd.concat(lis_df)
            gnlis2 = [i[0] for i in gnlis]
            df = pd.DataFrame(gnlis2)
            df.columns = ['gn']
            df.index.name = 'code'
            redian = df['gn'].value_counts()[:n].index.tolist()
            redian1 = [i for i in redian if i not in ['融资融券', '转融券标的']]
            
            data1 = data[data.gn.isin(redian1)]
            return data1['code'].unique().tolist()
        except Exception as e:
            self.my_log(f"获取热点概念股票失败: {e}", 'warn')
            return initial_list
    
    def _get_float_value(self, context, stocks):
        """获取市值数据函数"""
        df = pd.DataFrame()
        count = 0
        while count <= 10:
            count += 1
            if df.empty:
                time.sleep(1)
                try:
                    # 使用 context.previous_date 获取昨日日期
                    current_date = context.previous_date.strftime("%Y%m%d")
                    df = get_fundamentals(stocks, 'valuation', fields=['total_value', 'float_value', 'total_shares'], date=current_date)
                    if not df.empty:
                        self.my_log(f"获取流通市值第: {count}次, 获取成功")
                except:
                    self.my_log(f"获取流通市值第: {count}次, 获取不成功，正在重新获取")
        return df
    
    def _filter_stocks(self, context, stock_list):
        """基本过滤函数"""
        try:
            today = context.blotter.current_dt
            # 使用 context.previous_date 获取昨日日期
            previous_date_str = context.previous_date.strftime("%Y%m%d")
            position_list = self._get_current_holdings(context)
            
            # 过滤标准（全部使用昨日数据）
            filtered_stocks = []
            halt_status = get_stock_status(stock_list, 'HALT', previous_date_str)
            delisting_status = get_stock_status(stock_list, 'DELISTING', previous_date_str)
            st_status = get_stock_status(stock_list, 'ST', previous_date_str)
            stock_infos = get_stock_info(stock_list, ['stock_name', 'listed_date', 'de_listed_date'])
            current_date = context.previous_date
            
            for stock in stock_list:
                if '退' in stock_infos[stock]['stock_name']:
                    continue
                if halt_status[stock]:  # 使用昨日停牌状态
                    continue
                if st_status[stock]:
                    continue
                if delisting_status[stock]:
                    continue
                if stock.startswith('30') or stock.startswith('68') or stock.startswith('8') or stock.startswith('4'):
                    continue
                if not (stock in position_list or check_limit(stock)[stock] == 0):
                    continue
                
                # 次新股过滤
                if 'listed_date' in stock_infos[stock]:
                    listed_date_str = stock_infos[stock]['listed_date']
                    listed_date = datetime.strptime(listed_date_str, '%Y-%m-%d')
                    # context.previous_date 已经是 date 类型
                    if (current_date - listed_date.date()).days < 375:
                        continue
                else:
                    continue
                
                filtered_stocks.append(stock)
            return filtered_stocks
        except Exception as e:
            self.my_log(f"股票过滤失败: {e}", 'error')
            return []
    
    def _filter_new_stock(self, context, stock_list):
        """过滤次新股"""
        try:
            stock_infos = get_stock_info(stock_list, ['stock_name', 'listed_date', 'de_listed_date'])
            # 使用 context.previous_date 获取昨日日期
            current_date = context.previous_date
            for stock in stock_list.copy():
                if 'listed_date' in stock_infos[stock]:
                    listed_date_str = stock_infos[stock]['listed_date']
                    listed_date = datetime.strptime(listed_date_str, '%Y-%m-%d')
                    # context.previous_date 已经是 date 类型
                    if (current_date - listed_date.date()).days < 375:
                        stock_list.remove(stock)
                else:
                    stock_list.remove(stock)
            return stock_list
        except Exception as e:
            self.my_log(f"过滤次新股失败: {e}", 'warn')
            return stock_list
    
    def _filter_st_stock(self, context, stock_list):
        """过滤ST股票"""
        try:
            # 使用 context.previous_date 获取昨日日期
            previous_date = context.previous_date.strftime("%Y%m%d")
            st_status = get_stock_status(stock_list, 'ST', previous_date)
            for stock in stock_list.copy():
                if st_status[stock]:
                    stock_list.remove(stock)
            return stock_list
        except Exception as e:
            self.my_log(f"过滤ST股票失败: {e}", 'warn')
            return stock_list
    
    def _filter_paused_stock(self, context, stock_list):
        """过滤停牌股票"""
        try:
            # 使用 context.previous_date 获取昨日日期
            previous_date = context.previous_date.strftime("%Y%m%d")
            halt_status = get_stock_status(stock_list, 'HALT', previous_date)
            for stock in stock_list.copy():
                if halt_status[stock]:
                    stock_list.remove(stock)
            return stock_list
        except Exception as e:
            self.my_log(f"过滤停牌股票失败: {e}", 'warn')
            return stock_list
    
    def _filter_limitup_stock(self, context, stock_list):
        """过滤涨跌停股票"""
        try:
            position_list = self._get_current_holdings(context)
            return [stock for stock in stock_list if stock in position_list or check_limit(stock)[stock] == 0]
        except Exception as e:
            self.my_log(f"过滤涨跌停股票失败: {e}", 'warn')
            return stock_list
    
    def _filter_not_buy_again(self, stock_list):
        """过滤当日已买入的股票"""
        return [stock for stock in stock_list if stock not in self.not_buy_again]
    
    def _get_current_holdings(self, context):
        """获取当前策略持仓（热点小市值策略：使用中小综指成分股池过滤）"""
        try:
            holdings = []
            # 获取中小综指股票池作为热点小市值策略的潜在池子
            hot_spot_pool = get_index_stocks('399101.SZ')
            
            # 获取当前所有持仓
            positions = context.portfolio.positions
            all_holdings = [stock for stock, pos in positions.items() if pos.amount > 0]
            
            # 筛选出当前持仓中属于中小综指股票池的股票
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in hot_spot_pool:
                    holdings.append(stock)
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'error')
            return []
if "hot_spot_small_cap" in strategy_config:
    strategy_config['hot_spot_small_cap']['class'] = HotSpotSmallCapStrategy

# ============================五福策略类=============================
class WuFuStrategy(BaseStrategy):
    """
    五福闹新春策略
    - 基于动量、R²、成交量等多因子的ETF轮动策略
    - 支持多种止损机制和冷却期
    """

    def __init__(self, account_ratio=0.25, trade_time=["13:10", "13:11"], enable_strategy_isolation=True,
                 holdings_num=1, defensive_etf="159650.SZ", safe_haven_etf='511660.SS', min_money=5000,
                 lookback_days=25, min_score_threshold=0.5, max_score_threshold=5,
                 use_short_momentum_filter=False, short_lookback_days=10, short_momentum_threshold=0.0,
                 enable_r2_filter=True, r2_threshold=0.4,
                 enable_annualized_return_filter=False, min_annualized_return=1.0,
                 enable_ma_filter=False, ma_filter_days=20,
                 enable_volume_check=True, volume_lookback=5, volume_threshold=1.0,
                 enable_loss_filter=True, loss=0.97,
                 use_rsi_filter=False, rsi_period=6, rsi_lookback_days=1, rsi_threshold=98,
                 use_fixed_stop_loss=True, fixedStopLossThreshold=0.95,
                 use_pct_stop_loss=False, pct_stop_loss_threshold=0.95,
                 use_atr_stop_loss=False, atr_period=14, atr_multiplier=2,
                 atr_trailing_stop=True, atr_exclude_defensive=True,
                 sell_cooldown_enabled=False, sell_cooldown_days=3):
        super().__init__(name="五福闹新春策略", version="v3.1")
        self.account_ratio = max(0.0, min(1.0, account_ratio))
        self.trade_time = trade_time
        self.sell_time = trade_time[0]
        self.buy_time = trade_time[1]
        self.capital = 0
        self.enable_strategy_isolation = enable_strategy_isolation  # 策略隔离开关

        # 固定ETF池
        self.fixed_etf_pool = [
            # 大宗商品ETF：
            # '518880.SS',  # (黄金ETF)
            '161226.SZ',  # (国投白银LOF)
            '159980.SZ',  # (有色ETF大成)
            # '501018.SS',  # (南方原油ETF)
            # '159985.SZ',  # (豆粕ETF)

            # 海外ETF：
            # '513100.SS',  # (纳指ETF)
            '159509.SZ',  # (纳指科技ETF景顺)
            '513290.SS',  # (纳指生物)
            # '513500.SS',  # (标普500)
            '159518.SZ',  # (标普油气ETF嘉实)
            '159502.SZ',  # (标普生物科技ETF嘉实)
            '159529.SZ',  # (标普消费ETF)
            '513400.SS',  # (道琼斯)
            '520830.SS',  # (沙特ETF)
            # '513520.SS',  # (日经ETF)
            # '513030.SS',  # (德国ETF)
            '513080.SS',  # (法国ETF)
            '520870.SS',  # (巴西ETF)
            # 港股ETF：
            '513090.SS',  # (香港证券)
            '513180.SS',  # (恒指科技)
            '513120.SS',  # (HK创新药)
            '513330.SS',  # (恒生互联)
            '513750.SS',  # (港股非银)
            '159892.SZ',  # (恒生医药ETF)
            '159605.SZ',  # (中概互联ETF)
            '513190.SS',  # (H股金融)
            '510900.SS',  # (恒生中国)
            '513630.SS',  # (香港红利)
            '513920.SS',  # (港股通央企红利)
            '159323.SZ',  # (港股通汽车ETF)
            '513970.SS',  # (恒生消费)

            # 指数ETF：
            '510500.SS',  # (中证500ETF)
            '512100.SS',  # (中证1000ETF)
            '563300.SS',  # (中证2000)
            '510300.SS',  # (沪深300ETF)
            '512050.SS',  # (A500E)
            '510760.SS',  # (上证ETF)
            # '159915.SZ',  # (创业板ETF易方达)
            '159949.SZ',  # (创业板50ETF)
            '159967.SZ',  # (创业板成长ETF)
            '588080.SS',  # (科创板50)
            '588220.SS',  # (科创100)
            '511380.SS',  # (可转债ETF)

            # 行业ETF：
            '513310.SS',  # (中韩芯片)
            '588200.SS',  # (科创芯片)
            '159852.SZ',  # (软件ETF)
            '512880.SS',  # (证券ETF)
            '159206.SZ',  # (卫星ETF)
            '512400.SS',  # (有色金属ETF)
            '512980.SS',  # (传媒ETF)
            '159516.SZ',  # (半导体设备ETF)
            # '512480.SS',  # (半导体)
            '515880.SS',  # (通信ETF)
            '562500.SS',  # (机器人)
            '159218.SZ',  # (卫星产业ETF)
            '159869.SZ',  # (游戏ETF)
            '159870.SZ',  # (化工ETF)
            '159326.SZ',  # (电网设备ETF)
            # '159851.SZ',  # (金融科技ETF)
            '560860.SS',  # (工业有色)
            '159363.SZ',  # (创业板人工智能ETF华宝)
            '588170.SS',  # (科创半导)
            '159755.SZ',  # (电池ETF)
            '512170.SS',  # (医疗ETF)
            '512800.SS',  # (银行ETF)
            '159819.SZ',  # (人工智能ETF易方达)
            '512710.SS',  # (军工龙头)
            '159638.SZ',  # (高端装备ETF嘉实)
            '517520.SS',  # (黄金股)
            '515980.SS',  # (人工智能)
            '159995.SZ',  # (芯片ETF)
            '159227.SZ',  # (航空航天ETF)
            '512660.SS',  # (军工ETF)
            '512690.SS',  # (酒ETF)
            '516150.SS',  # (稀土基金)
            # '512890.SS',  # (红利低波)
            '588790.SS',  # (科创智能)
            '159992.SZ',  # (创新药ETF)
            '512070.SS',  # (证券保险)
            '562800.SS',  # (稀有金属)
            '512010.SS',  # (医药ETF)
            '515790.SS',  # (光伏ETF)
            # '510880.SS',  # (红利ETF)
            '159928.SZ',  # (消费ETF)
            '159883.SZ',  # (医疗器械ETF)
            '159998.SZ',  # (计算机ETF)
            '515220.SS',  # (煤炭ETF)
            '561980.SS',  # (芯片设备)
            '515400.SS',  # (大数据)
            '515120.SS',  # (创新药)
            '159566.SZ',  # (储能电池ETF易方达)
            '515050.SS',  # (5GETF)
            '516510.SS',  # (云计算ETF)
            '159256.SZ',  # (创业板软件ETF华夏)
            '159766.SZ',  # (旅游ETF)
            '512200.SS',  # (地产ETF)
            '513350.SS',  # (油气ETF)
            '159583.SZ',  # (通信设备ETF)
            '159732.SZ',  # (消费电子ETF)
            '516160.SS',  # (新能源)
            '516520.SS',  # (智能驾驶)
            '562590.SS',  # (半导材料)
            '515030.SS',  # (新汽车)
            '512670.SS',  # (国防ETF)
            '561330.SS',  # (矿业ETF)
            '516190.SS',  # (文娱ETF)
            '159840.SZ',  # (锂电池ETF工银)
            '159611.SZ',  # (电力ETF)
            '159981.SZ',  # (能源化工ETF)
            '159865.SZ',  # (养殖ETF)
            '561360.SS',  # (石油ETF)
            '159667.SZ',  # (工业母机ETF)
            '515170.SS',  # (食品饮料ETF)
            '513360.SS',  # (教育ETF)
            '159825.SZ',  # (农业ETF)
            '515210.SS',  # (钢铁ETF)
        ]

        # 策略参数（使用传入的配置值）
        self.holdings_num = holdings_num
        self.defensive_etf = defensive_etf
        self.safe_haven_etf = safe_haven_etf
        self.min_money = min_money

        self.lookback_days = lookback_days
        self.min_score_threshold = min_score_threshold
        self.max_score_threshold = max_score_threshold

        self.use_short_momentum_filter = use_short_momentum_filter
        self.short_lookback_days = short_lookback_days
        self.short_momentum_threshold = short_momentum_threshold
        
        self.enable_r2_filter = enable_r2_filter
        self.r2_threshold = r2_threshold
        
        self.enable_annualized_return_filter = enable_annualized_return_filter
        self.min_annualized_return = min_annualized_return
        
        self.enable_ma_filter = enable_ma_filter
        self.ma_filter_days = ma_filter_days
        
        self.enable_volume_check = enable_volume_check
        self.volume_lookback = volume_lookback
        self.volume_threshold = volume_threshold
        
        self.enable_loss_filter = enable_loss_filter
        self.loss = loss
        
        self.use_rsi_filter = use_rsi_filter
        self.rsi_period = rsi_period
        self.rsi_lookback_days = rsi_lookback_days
        self.rsi_threshold = rsi_threshold

        self.use_fixed_stop_loss = use_fixed_stop_loss
        self.fixedStopLossThreshold = fixedStopLossThreshold
        self.use_pct_stop_loss = use_pct_stop_loss
        self.pct_stop_loss_threshold = pct_stop_loss_threshold
        self.use_atr_stop_loss = use_atr_stop_loss
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.atr_trailing_stop = atr_trailing_stop
        self.atr_exclude_defensive = atr_exclude_defensive

        self.sell_cooldown_enabled = sell_cooldown_enabled
        self.sell_cooldown_days = sell_cooldown_days
        self.cooldown_end_date = None  # 初始化冷却期结束日期为空

        # 状态变量
        self.run_count = 0  # 运行计数器
        self.positions = {}  # 初始化持仓数量字典
        self.position_highs = {}  # 初始化ATR追踪最高价字典
        self.position_stop_prices = {}  # 初始化ATR止损价字典
        self.target_etfs_list = []  # 初始化目标ETF列表
        self.dynamic_etf_pool = []  # 初始化动态ETF池为空列表

        # 所有可能交易的标的
        self.cur_strategy_stocks = self.fixed_etf_pool[:] + [self.defensive_etf, self.safe_haven_etf]

    def before_trading_start(self, context, data=None):
        """盘前执行：更新动态ETF池"""
        super().before_trading_start(context, data)
        
        # 实盘时加载持久化数据
        if is_trade():
            try:
                with open(get_research_path() + 'WuFuStrategy.pkl', 'rb') as f:
                    saved_data = pickle.load(f)
                    self.run_count = saved_data.get('run_count', 0)
                    self.cooldown_end_date = saved_data.get('cooldown_end_date', None)
                    self.positions = saved_data.get('positions', {})
                    self.position_highs = saved_data.get('position_highs', {})
                    self.position_stop_prices = saved_data.get('position_stop_prices', {})
                    self.target_etfs_list = saved_data.get('target_etfs_list', [])
                    self.my_log(f"持久化数据加载成功: run_count={self.run_count}")
            except FileNotFoundError:
                self.my_log("首次运行，初始化持久化变量")
            except Exception as e:
                self.my_log(f"加载持久化数据失败: {e}", "warn")
        
        # 更新动态ETF池
        self._update_sector_pool(context)
        
        # 策略隔离：合并固定池+动态池，过滤与其他策略冲突的标的
        if getattr(self, 'enable_strategy_isolation', True):
            all_etf_set = set(self.fixed_etf_pool)
            if hasattr(self, 'dynamic_etf_pool') and self.dynamic_etf_pool:
                all_etf_set.update(self.dynamic_etf_pool)
            filtered_etf_set, filter_info = self._filter_conflict_etfs(all_etf_set)
            filtered_etf_set.add(self.defensive_etf)
            filtered_etf_set.add(self.safe_haven_etf)
            self.cur_strategy_stocks = list(filtered_etf_set)

    def handle_data(self, context, data=None):
        """盘中执行：在指定时间执行卖出/买入/止损"""
        current_time = context.current_dt.strftime("%H:%M")

        # 更新策略资金
        if not is_trade():
            self.capital = context.portfolio.portfolio_value * self.account_ratio
        else:
            self.capital = g.real_trading_cash_use * self.account_ratio
        
        # 增加运行计数器
        self.run_count += 1

        # 分钟级止损检查
        self._minute_level_stop_loss(context)
        self._minute_level_pct_stop_loss(context)
        self._minute_level_atr_stop_loss(context)

        # 卖出阶段
        if current_time == self.sell_time:
            self._etf_sell_trade(context)

        # 买入阶段
        if current_time == self.buy_time:
            self._etf_buy_trade(context)

    def _update_sector_pool(self, context):
        """更新动态ETF池"""
        try:
            all_etfs = self._get_all_securities_ptrade()  # 获取全市场ETF代码列表
            exclude_keywords = ['300', '500', '1000', '50', '上证', '创业板', '科创', '恒生', 'H股', '货币', '纳指', '标普', '债']  # 定义需排除的关键词
    
            sector_etfs = []  # 初始化行业ETF列表
            for code in all_etfs:
                name = self._get_security_name(code)
                if not name:
                    continue
                is_sector_etf = True
                for k in exclude_keywords:
                    if k in name:
                        is_sector_etf = False
                        break
                if is_sector_etf:
                    sector_etfs.append(code)
    
            if not sector_etfs:
                self.my_log("【警告】未能获取到基础 ETF 池！", "warn")
                return
    
            end_date = context.previous_date
            h = get_history(1, '1d', ['money'], sector_etfs, fq='pre')
            
            if h is None or h.empty:
                self.my_log("【警告】获取 ETF 成交额数据失败，使用空池", "warn")
                self.dynamic_etf_pool = []
                return
            
            qualified_etfs = h.query("money > 50000000")
            if qualified_etfs is None or qualified_etfs.empty:
                self.my_log("【警告】没有符合条件的 ETF，使用空池", "warn")
                self.dynamic_etf_pool = []
                return
                
            sorted_codes = qualified_etfs.sort_values(by='money', ascending=False)['code'].tolist()
    
            final_dynamic_pool = []
            seen_industries = set()
    
            for code in sorted_codes:
                name = self._get_security_name(code)
                industry_key = name[:2]
    
                if industry_key not in seen_industries:
                    final_dynamic_pool.append(code)
                    seen_industries.add(industry_key)
    
                if len(final_dynamic_pool) >= 100:
                    break
    
            self.dynamic_etf_pool = final_dynamic_pool
            etf_name_code_list = [f"{self._get_security_name(c)}({c})" for c in self.dynamic_etf_pool]
            self.my_log(f"【动态更新完成】热点资金涌入行业池(前{len(self.dynamic_etf_pool)}只): {etf_name_code_list}")
        except Exception as e:
            self.my_log(f"【严重错误】计算成交额时发生异常: {e}", "error")


    def _calculate_all_metrics_for_etf(self, context, etf_list):
        """计算单个ETF的所有指标"""
        try:
            all_metrics = []
            lookback = max(
                self.lookback_days,
                self.short_lookback_days,
                self.rsi_period + self.rsi_lookback_days,
                self.ma_filter_days,
                self.volume_lookback
            ) + 20

            prices = self._get_history_ptrade(etf_list)

            for etf in etf_list:
                etf_name = self._get_security_name(etf)
                if len(prices.query("code==@etf")) < max(self.lookback_days, self.ma_filter_days):
                    continue

                current_data = self._get_current_data_ptrade(etf)
                if current_data is None:
                    continue
                current_price = current_data['last_price']

                price_series = np.append(prices.query("code==@etf")["close"].values, current_price)

                # === 效率动量计算 ===
                # 获取OHLC数据计算Pivot
                etf_hist = prices.query("code==@etf")
                if 'open' in etf_hist.columns and 'high' in etf_hist.columns and 'low' in etf_hist.columns:
                    o_vals = etf_hist['open'].values
                    h_vals = etf_hist['high'].values
                    l_vals = etf_hist['low'].values
                    c_vals = etf_hist['close'].values
                    pivot_hist = (o_vals + h_vals + l_vals + c_vals) / 4.0
                else:
                    pivot_hist = etf_hist['close'].values
                pivot_series = np.append(pivot_hist, current_price)
                recent_pivot = pivot_series[-(self.lookback_days + 1):]
                
                if recent_pivot[0] <= 0 or recent_pivot[-1] <= 0:
                    continue
                
                # 动量 = 100 × ln(pivot_end / pivot_start)
                raw_momentum = 100.0 * np.log(recent_pivot[-1] / recent_pivot[0])
                
                # 效率系数 = |起点到终点的直线距离| / 路径总长度
                log_pivot = np.log(recent_pivot)
                direction = abs(log_pivot[-1] - log_pivot[0])
                volatility = np.sum(np.abs(np.diff(log_pivot)))
                r_squared = direction / volatility if volatility > 0 else 0  # 复用r_squared变量名以兼容后续过滤
                
                # 效率动量得分 = 动量 × 效率系数
                momentum_score = raw_momentum * r_squared
                # 兼容后续过滤器：用效率系数近似年化收益率的角色
                annualized_returns = raw_momentum / 100.0  # 简化为对数收益率

                if len(price_series) >= self.short_lookback_days + 1:
                    short_return = price_series[-1] / price_series[-(self.short_lookback_days + 1)] - 1
                    short_annualized = (1 + short_return) ** (250 / self.short_lookback_days) - 1
                else:
                    short_annualized = -np.inf

                ma_price = np.mean(price_series[-self.ma_filter_days:])
                current_above_ma = current_price >= ma_price

                volume_ratio = self._get_volume_ratio(context, etf, show_detail_log=False)
                day_ratios = []
                passed_loss_filter = True
                if len(price_series) >= 4:
                    day1 = price_series[-1] / price_series[-2]
                    day2 = price_series[-2] / price_series[-3]
                    day3 = price_series[-3] / price_series[-4]
                    day_ratios = [day1, day2, day3]
                    if min(day_ratios) < self.loss:
                        passed_loss_filter = False

                current_rsi = None
                max_recent_rsi = None
                passed_rsi_filter = True
                if self.use_rsi_filter and len(price_series) >= self.rsi_period + self.rsi_lookback_days:
                    rsi_values = self._calculate_rsi(price_series, self.rsi_period)
                    if len(rsi_values) >= self.rsi_lookback_days:
                        recent_rsi = rsi_values[-self.rsi_lookback_days:]
                        max_recent_rsi = np.max(recent_rsi)
                        current_rsi = recent_rsi[-1]
                        if np.any(recent_rsi > self.rsi_threshold):
                            ma5 = np.mean(price_series[-5:]) if len(price_series) >= 5 else current_price
                            if current_price < ma5:
                                passed_rsi_filter = False
                metrics = {
                    'etf': etf,
                    'etf_name': etf_name,
                    'momentum_score': momentum_score,
                    'annualized_returns': annualized_returns,
                    'r_squared': r_squared,
                    'short_annualized': short_annualized,
                    'current_price': current_price,
                    'ma_price': ma_price,
                    'volume_ratio': volume_ratio,
                    'day_ratios': day_ratios,
                    'current_rsi': current_rsi,
                    'max_recent_rsi': max_recent_rsi,
                    'passed_momentum': self.min_score_threshold <= momentum_score <= self.max_score_threshold,
                    'passed_short_mom': short_annualized >= self.short_momentum_threshold,
                    'passed_r2': r_squared > self.r2_threshold,
                    'passed_annual_ret': annualized_returns >= self.min_annualized_return,
                    'passed_ma': current_above_ma,
                    'passed_volume': volume_ratio is not None and volume_ratio < self.volume_threshold,
                    'passed_loss': passed_loss_filter,
                    'passed_rsi': passed_rsi_filter,
                }
                if metrics['etf'] in {m['etf'] for m in all_metrics}:
                    self.my_log(f"发现重复ETF数据: {metrics['etf']}，跳过。", "warn")
                    continue
                all_metrics.append(metrics)
            return all_metrics
        except Exception as e:
            self.my_log(f"计算指标出错: {e}", "warn")
            return None

    def _apply_filters(self, metrics_list):
        """应用过滤器"""
        steps = [
            ('动量得分', lambda m: m['passed_momentum'], True),
            ('短期动量', lambda m: m['passed_short_mom'], self.use_short_momentum_filter),
            ('R²', lambda m: m['passed_r2'], self.enable_r2_filter),
            ('年化收益率', lambda m: m['passed_annual_ret'], self.enable_annualized_return_filter),
            ('均线', lambda m: m['passed_ma'], self.enable_ma_filter),
            ('成交量', lambda m: m['passed_volume'], self.enable_volume_check),
            ('短期风控', lambda m: m['passed_loss'], self.enable_loss_filter),
            ('RSI', lambda m: m['passed_rsi'], self.use_rsi_filter),
        ]

        filtered = metrics_list[:]  # 复制原始列表
        for name, condition, is_enabled in steps:  # 遍历每个过滤步骤
            if is_enabled:  # 若该过滤器启用
                filtered = [m for m in filtered if condition(m)]  # 应用过滤条件
        return filtered  # 返回过滤后列表

    def _filter_conflict_etfs(self, etf_set):
        """过滤与其他策略冲突的ETF
        
        Returns:
            tuple: (过滤后的ETF集合, 过滤信息字典)
        """
        # 如果未启用策略隔离，则直接返回原始集合
        if not getattr(self, 'enable_strategy_isolation', True):
            return etf_set, {}
        
        # 获取其他策略的标的池
        other_strategy_stocks = {}  # {策略名: {代码前缀集合}}
        for strategy in trading_strategys:
            if strategy is not self:
                stocks = getattr(strategy, 'cur_strategy_stocks', [])
                if stocks:
                    strategy_codes = set()
                    for stock in stocks:
                        if not stock:
                            continue
                        code_prefix = stock.split('.')[0]
                        if code_prefix:
                            strategy_codes.add(code_prefix)
                    if strategy_codes:
                        other_strategy_stocks[strategy.name] = strategy_codes
        
        # 过滤冲突标的
        original_count = len(etf_set)
        filtered_etf_set = set()
        filter_info = {}  # {被过滤的ETF: 对应的策略名列表}
        
        for code in etf_set:
            code_prefix = code.split('.')[0]
            
            # 检查是否与其他策略冲突
            conflicting_strategies = []
            for strategy_name, strategy_codes in other_strategy_stocks.items():
                if code_prefix in strategy_codes:
                    conflicting_strategies.append(strategy_name)
            
            if conflicting_strategies:
                # 记录冲突信息
                filter_info[code] = conflicting_strategies
            else:
                filtered_etf_set.add(code)
        
        # 打印过滤信息
        if filter_info:
            self.my_log(f"【策略隔离】共检测到 {len(filter_info)} 只ETF与其他策略冲突:", 'warn')
            for code, strategies in filter_info.items():
                code_name = self._get_security_name(code)
                strategies_str = ', '.join(strategies)
                self.my_log(f"   - {code} {code_name} → 冲突策略: {strategies_str}", 'warn')
        
        if len(filtered_etf_set) < original_count:
            removed_count = original_count - len(filtered_etf_set)
            self.my_log(f"【策略隔离】已过滤{removed_count}只与其他策略冲突的ETF，剩余{len(filtered_etf_set)}只", 'warn')
        
        return filtered_etf_set, filter_info
    
    def _get_final_ranked_etfs(self, context):
        """获取最终排序的ETF列表"""
        all_metrics = []
        
        # 合并固定池与动态池
        etf_set = set(self.fixed_etf_pool)
        if hasattr(self, 'dynamic_etf_pool') and self.dynamic_etf_pool:
            etf_set.update(self.dynamic_etf_pool)
        
        # 过滤与其他策略冲突的标的
        etf_set, filter_info = self._filter_conflict_etfs(etf_set)
        
        # 确保防御ETF和避险ETF在池中
        etf_set.add(self.defensive_etf)
        etf_set.add(self.safe_haven_etf)
        
        # 更新 cur_strategy_stocks（用于策略冲突检测）
        self.cur_strategy_stocks = list(etf_set)
        
        combined_names_and_codes = [f"{self._get_security_name(code)}({code})" for code in etf_set]
        self.my_log(f"【ETF池合并】固定池与动态池合并完成，合计{len(etf_set)}只ETF，前10只分别是: {combined_names_and_codes[:10]}")
        
        etf_set = self._filter_TSTT_stock(context, list(etf_set))
        all_metrics = self._calculate_all_metrics_for_etf(context, etf_set)

        if all_metrics is None:
            return []

        for item in all_metrics:
            score = item.get('momentum_score')
            if pd.isna(score) or np.isnan(score):
                item['momentum_score'] = float('-inf')

        all_metrics.sort(key=lambda x: x.get('momentum_score', float('-inf')), reverse=True)

        log_lines_step1 = ["", ">>> 第一步：所有ETF按动量得分从大到小排序 (前10名) <<<"]
        for i, m in enumerate(all_metrics):
            if i >= 10:
                break
            def fmt_status(value_str, passed):
                return f"{value_str} {'✅' if passed else '❌'}"

            original_score = m.get('momentum_score')
            if original_score == float('-inf'):
                mom_score_str = "nan"
                mom_passed = False
            else:
                mom_score_str = f"{original_score:.4f}" if not pd.isna(original_score) else "nan"
                mom_passed = m['passed_momentum']

            short_str = f"{m['short_annualized']:.4f}" if not pd.isna(m['short_annualized']) else "nan"
            short = fmt_status(f"短期动量: {short_str}", m['passed_short_mom'])
            r2_str = f"{m['r_squared']:.3f}" if not pd.isna(m['r_squared']) else "nan"
            r2 = fmt_status(f"R²: {r2_str}", m['passed_r2'])
            ann_str = f"{m['annualized_returns']:.2%}" if not pd.isna(m['annualized_returns']) else "nan%"
            ann = fmt_status(f"年化收益率: {ann_str}", m['passed_annual_ret'])
            ma_price_str = f"{m['ma_price']:.2f}" if not pd.isna(m['ma_price']) else "nan"
            ma = fmt_status(f"均线: 当前价{m['current_price']:.2f} vs 均线{ma_price_str}", m['passed_ma'])
            vol_val = f"{m['volume_ratio']:.2f}" if m['volume_ratio'] is not None else "N/A"
            vol = fmt_status(f"成交量比值: {vol_val}", m['passed_volume'])
            min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
            loss_val = f"{min_ratio:.4f}" if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
            loss = fmt_status(f"短期风控（近3日最低比值）: {loss_val}", m['passed_loss'])
            rsi_str = f"{m['current_rsi']:.1f}" if m['current_rsi'] is not None and not pd.isna(m['current_rsi']) else "nan"
            max_rsi_str = f"{m['max_recent_rsi']:.1f}" if m['max_recent_rsi'] is not None and not pd.isna(m['max_recent_rsi']) else "nan"
            rsi = fmt_status(f"RSI: 当前{rsi_str} (峰值{max_rsi_str})", m['passed_rsi'])

            line = (
                f"{m['etf']} {m['etf_name']}: "
                f"{fmt_status(f'动量得分: {mom_score_str}', mom_passed)} ，"
                f"{short} ，"
                f"{r2}，"
                f"{ann}，"
                f"{ma}，"
                f"{vol}，"
                f"{loss}，"
                f"{rsi}"
            )
            log_lines_step1.append(line)
        print("all_metrics:",len(all_metrics))
        final_list = self._apply_filters(all_metrics)
        print("final_list:",len(final_list))
        for item in final_list:
            score = item.get('momentum_score')
            if pd.isna(score) or np.isnan(score):
                item['momentum_score'] = float('-inf')
        final_list.sort(key=lambda x: x.get('momentum_score', float('-inf')), reverse=True)
        top_10_final = final_list[:10]

        log_lines_step2 = ["", ">>> 第二步：符合全部过滤条件的ETF按动量得分从大到小排序 (前10名) <<<"]

        if top_10_final:
            for m in top_10_final:
                def fmt_status(value_str, passed):
                    return f"{value_str} {'✅' if passed else '❌'}"

                original_score = m.get('momentum_score')
                if original_score == float('-inf'):
                    mom_score_str = "nan"
                    mom_passed = False
                else:
                    mom_score_str = f"{original_score:.4f}" if not pd.isna(original_score) else "nan"
                    mom_passed = m['passed_momentum']

                mom = fmt_status(f"动量得分: {mom_score_str}", mom_passed)
                short_str = f"{m['short_annualized']:.4f}" if not pd.isna(m['short_annualized']) else "nan"
                short = fmt_status(f"短期动量: {short_str}", m['passed_short_mom'])
                r2_str = f"{m['r_squared']:.3f}" if not pd.isna(m['r_squared']) else "nan"
                r2 = fmt_status(f"R²: {r2_str}", m['passed_r2'])
                ann_str = f"{m['annualized_returns']:.2%}" if not pd.isna(m['annualized_returns']) else "nan%"
                ann = fmt_status(f"年化收益率: {ann_str}", m['passed_annual_ret'])
                ma_price_str = f"{m['ma_price']:.2f}" if not pd.isna(m['ma_price']) else "nan"
                ma = fmt_status(f"均线: 当前价{m['current_price']:.2f} vs 均线{ma_price_str}", m['passed_ma'])
                vol_val = f"{m['volume_ratio']:.2f}" if m['volume_ratio'] is not None else "N/A"
                vol = fmt_status(f"成交量比值: {vol_val}", m['passed_volume'])
                min_ratio = min(m['day_ratios']) if m['day_ratios'] else 'N/A'
                loss_val = f"{min_ratio:.4f}" if isinstance(min_ratio, float) and not pd.isna(min_ratio) else str(min_ratio)
                loss = fmt_status(f"短期风控（近3日最低比值）: {loss_val}", m['passed_loss'])
                rsi_str = f"{m['current_rsi']:.1f}" if m['current_rsi'] is not None and not pd.isna(m['current_rsi']) else "nan"
                max_rsi_str = f"{m['max_recent_rsi']:.1f}" if m['max_recent_rsi'] is not None and not pd.isna(m['max_recent_rsi']) else "nan"
                rsi = fmt_status(f"RSI: 当前{rsi_str} (峰值{max_rsi_str})", m['passed_rsi'])

                line = (
                    f"{m['etf']} {m['etf_name']}: "
                    f"{mom} ，"
                    f"{short} ，"
                    f"{r2}，"
                    f"{ann}，"
                    f"{ma}，"
                    f"{vol}，"
                    f"{loss}，"
                    f"{rsi}"
                )
                log_lines_step2.append(line)
        else:
            log_lines_step2.append("（无符合条件的ETF）")

        log_lines_step2.append("==================================================")

        full_log = "\n".join(log_lines_step1 + log_lines_step2)
        self.my_log(full_log)

        return final_list

    def _minute_level_stop_loss(self, context):
        """分钟级固定比例止损"""
        if not self.use_fixed_stop_loss:
            return
        if self._is_in_cooldown(context):
            return

        for security in list(context.portfolio.positions.keys()):
            security = self._get_switch_code(security)
            if security not in self.fixed_etf_pool:
                continue
            position = context.portfolio.positions[security]
            if position.amount <= 0:
                continue

            current_price = position.last_sale_price
            cost_price = position.cost_basis

            if current_price <= cost_price * self.fixedStopLossThreshold:
                security_name = self._get_security_name(security)
                loss_percent = (current_price / cost_price - 1) * 100
                self.my_log(
                    f"🚨 [分钟级] 固定百分比止损卖出: {security} {security_name}，当前价: {current_price:.3f}, 成本: {cost_price:.3f}, 预设阈值: {self.fixedStopLossThreshold}, 预估亏损: {loss_percent:.2f}%"
                )

                success = self._smart_order_target_value(security, 0, context)
                if success:
                    self.my_log(
                        f"✅ [分钟级] 已成功止损卖出: {security} {security_name}，实际亏损: {loss_percent:.2f}%"
                    )
                    self.position_highs.pop(security, None)
                    self.position_stop_prices.pop(security, None)
                    self._enter_safe_haven_and_set_cooldown(context, trigger_reason="分钟级固定止损")
                else:
                    self.my_log(f"❌ [分钟级] 止损卖出失败: {security} {security_name}")

    def _minute_level_pct_stop_loss(self, context):
        """分钟级当日跌幅止损"""
        if not self.use_pct_stop_loss:
            return
        if self._is_in_cooldown(context):
            return

        for security in list(context.portfolio.positions.keys()):
            security = self._get_switch_code(security)
            if security not in self.fixed_etf_pool:
                continue
            position = context.portfolio.positions[security]
            if position.amount <= 0:
                continue

            current_data = self._get_current_data_ptrade(security)
            if current_data is None:
                continue

            today_open = current_data['day_open']
            if not today_open or today_open <= 0:
                continue

            current_price = position.last_sale_price
            stop_price = today_open * self.pct_stop_loss_threshold

            if current_price <= stop_price:
                security_name = self._get_security_name(security)
                daily_loss = (current_price / today_open - 1) * 100
                self.my_log(
                    f"🚨 [分钟级] 当日跌幅止损卖出: {security} {security_name}，当前价: {current_price:.3f}, 开盘价: {today_open:.3f}, 触发价: {stop_price:.3f}, 当日预估跌幅: {daily_loss:.2f}%"
                )

                success = self._smart_order_target_value(security, 0, context)
                if success:
                    self.my_log(
                        f"✅ [分钟级] 已成功按当日跌幅止损卖出: {security} {security_name}，实际当日跌幅: {daily_loss:.2f}%"
                    )
                    self.position_highs.pop(security, None)
                    self.position_stop_prices.pop(security, None)
                    self._enter_safe_haven_and_set_cooldown(context, trigger_reason="分钟级当日跌幅止损")
                else:
                    self.my_log(f"❌ [分钟级] 当日跌幅止损卖出失败: {security} {security_name}")

    def _minute_level_atr_stop_loss(self, context):
        """分钟级ATR动态止损"""
        if not self.use_atr_stop_loss:
            return
        if self._is_in_cooldown(context):
            return

        for security in list(context.portfolio.positions.keys()):
            security = self._get_switch_code(security)
            if security not in self.fixed_etf_pool:
                continue
            position = context.portfolio.positions[security]
            if position.amount <= 0:
                continue
            if self.atr_exclude_defensive and security == self.defensive_etf:
                continue

            try:
                security_name = self._get_security_name(security)
                current_price = position.last_sale_price
                if current_price <= 0:
                    continue

                cost_price = position.cost_basis

                current_atr, _, success, _ = self._calculate_atr(security, self.atr_period)
                if not success or current_atr <= 0:
                    continue

                if security not in self.position_highs:
                    self.position_highs[security] = current_price
                else:
                    self.position_highs[security] = max(self.position_highs[security], current_price)

                if self.atr_trailing_stop:
                    atr_stop_price = self.position_highs[security] - self.atr_multiplier * current_atr
                else:
                    atr_stop_price = cost_price - self.atr_multiplier * current_atr

                self.position_stop_prices[security] = atr_stop_price

                if current_price <= atr_stop_price:
                    loss_percent = (current_price / cost_price - 1) * 100
                    atr_type = "跟踪" if self.atr_trailing_stop else "固定"
                    self.my_log(
                        f"🚨 [分钟级] ATR动态止损({atr_type})卖出: {security} {security_name}，当前价: {current_price:.3f}, 止损价: {atr_stop_price:.3f}, 亏损: {loss_percent:.2f}%"
                    )

                    success = self._smart_order_target_value(security, 0, context)
                    if success:
                        self.my_log(f"✅ [分钟级] ATR止损成功: {security} {security_name}")
                        self.position_highs.pop(security, None)
                        self.position_stop_prices.pop(security, None)
                        self._enter_safe_haven_and_set_cooldown(context, trigger_reason="分钟级ATR动态止损")
                    else:
                        self.my_log(f"❌ [分钟级] ATR止损失败: {security} {security_name}")
            except Exception as e:
                security_name = self._get_security_name(security)
                self.my_log(f"[分钟级] ATR止损检查异常 {security} {security_name}: {e}", "warn")

    def _etf_sell_trade(self, context):
        """ETF卖出/轮动"""
        self.my_log("========== 卖出操作开始 (轮动逻辑 - 严格模式) ==========")

        if self._is_in_cooldown(context):
            self.my_log("🔒 当前处于冷却期，跳过轮动逻辑中的卖出操作")
            self.my_log("========== 卖出操作完成 (轮动逻辑 - 严格模式) ==========")
            return

        ranked_etfs = self._get_final_ranked_etfs(context)
        target_etfs = []
        if ranked_etfs:
            for metrics in ranked_etfs[:self.holdings_num]:
                target_etfs.append(metrics['etf'])
                self.my_log(
                    f"确定最终目标: {metrics['etf']} {metrics['etf_name']}，得分: {metrics['momentum_score']:.4f}"
                )
        else:
            if self._check_defensive_etf_available(context):
                target_etfs = [self.defensive_etf]
                etf_name = self._get_security_name(self.defensive_etf)
                self.my_log(f"🛡️ 确定最终目标(防御模式): {self.defensive_etf} {etf_name}，得分: N/A")
            else:
                self.my_log("💤 无最终目标(空仓模式)")
                target_etfs = []

        self.target_etfs_list = target_etfs

        current_positions = context.portfolio.positions
        target_set = set(target_etfs)

         # 获取中小综指成分股
        try:
            zhongxiao_stocks = get_index_stocks('399101.SZ')
            # 转换为.SS/.SZ格式
            zhongxiao_stocks_ss_sz = set()
            for stock in zhongxiao_stocks:
                if '.XSHG' in stock:
                    zhongxiao_stocks_ss_sz.add(stock.replace('.XSHG', '.SS'))
                elif '.XSHE' in stock:
                    zhongxiao_stocks_ss_sz.add(stock.replace('.XSHE', '.SZ'))
                else:
                    zhongxiao_stocks_ss_sz.add(stock)
        except Exception as e:
            self.my_log(f"获取中小综指成分股失败: {e}", 'warn')
            zhongxiao_stocks_ss_sz = set()

        for security in current_positions:
            security = self._get_switch_code(security)
            position = current_positions[security]
            if position.amount > 0 and security not in target_set:
                # 检查是否属于中小综指成分股
                if security in zhongxiao_stocks_ss_sz:
                    self.my_log(f"⏭️ 跳过卖出 {security}（属于中小综指成分股）")
                    continue

                # 检查持仓是否属于其他策略
                security_prefix = security.split('.')[0]
                is_other_strategy_stock = False
                
                for strategy in trading_strategys:
                    if strategy is not self and hasattr(strategy, 'cur_strategy_stocks'):
                        other_stocks = getattr(strategy, 'cur_strategy_stocks', [])
                        for other_stock in other_stocks:
                            if other_stock.split('.')[0] == security_prefix:
                                is_other_strategy_stock = True
                                self.my_log(f"⏭️ 跳过卖出 {security}（属于策略: {strategy.name}）")
                                break
                        if is_other_strategy_stock:
                            break
                
                if is_other_strategy_stock:
                    continue
                
                security_name = self._get_security_name(security)
                # 转换为 XSHG/XSHE 格式与 fixed_etf_pool 比较
                security_for_check = security
                if ".SS" in security:
                    security_for_check = security.replace(".SS", ".XSHG")
                elif ".SZ" in security:
                    security_for_check = security.replace(".SZ", ".XSHE")
                if security_for_check not in self.fixed_etf_pool and security_for_check != self.defensive_etf:
                    self.my_log(f"🔍 发现持仓不在预设池中: {security} {security_name}")
                self.my_log(f"📤 准备卖出不在今日目标列表的持仓: {security} {security_name}")

                success = self._smart_order_target_value(security, 0, context)
                if success:
                    self.my_log(f"✅ 已成功卖出: {security} {security_name}")
                else:
                    self.my_log(f"❌ 卖出失败: {security} {security_name}")

                self.position_highs.pop(security, None)
                self.position_stop_prices.pop(security, None)

        self.my_log("========== 卖出操作完成 (轮动逻辑 - 严格模式) ==========")

    def _etf_buy_trade(self, context):
        """ETF买入"""
        self.my_log("========== 买入操作开始 ==========")

        self._exit_safe_haven_if_cooldown_ends(context)

        if self._is_in_cooldown(context):
            self.my_log("🔒 当前处于冷却期，跳过正常买入操作")
            self.my_log("========== 买入操作完成 ==========")
            return

        target_etfs = self.target_etfs_list
        if not target_etfs:
            self.my_log("根据昨日计算，今日无目标ETF，保持空仓")
            self.my_log("========== 买入操作完成 ==========")
            return

        # 兼容回测和实盘的资金计算
        if not is_trade():
            total_value = context.portfolio.portfolio_value * self.account_ratio
        else:
            total_value = g.real_trading_cash_use * self.account_ratio
        
        cash_buffer_ratio = 0.01
        investable_value = total_value * (1 - cash_buffer_ratio)

        target_value_per_etf = investable_value / len(target_etfs)

        self.my_log(
            f"账户总价值: {total_value:.2f}, 可投资金额: {investable_value:.2f}, 目标ETF数量: {len(target_etfs)}, 单只ETF目标金额: {target_value_per_etf:.2f}"
        )

        if target_value_per_etf < self.min_money:
            self.my_log(
                f"计算出的单只ETF目标金额 {target_value_per_etf:.2f} 小于最小交易额 {self.min_money:.2f}，无法买入任何目标ETF"
            )
            self.my_log("========== 买入操作完成 ==========")
            return

        for etf in target_etfs:
            current_value = 0
            if self._get_switch_code(etf) in context.portfolio.positions:
                position = context.portfolio.positions[self._get_switch_code(etf)]
                if position.amount > 0:
                    current_value = position.amount * position.last_sale_price

            value_diff = abs(target_value_per_etf - current_value)

            required_funds = max(0, value_diff)
            if context.portfolio.cash < required_funds:
                self.my_log(
                    f"可用现金不足，无法买入/调仓 {etf}。所需: {required_funds:.2f}, 可用: {context.portfolio.cash:.2f}"
                )
                continue

            success = self._smart_order_target_value(etf, target_value_per_etf, context)
            if success:
                etf_name = self._get_security_name(etf)
                if current_value == 0:
                    self.my_log(f"📦 买入新持仓: {etf} {etf_name}，目标金额: {target_value_per_etf:.2f}")
                elif current_value < target_value_per_etf:
                    self.my_log(f"📦 增持: {etf} {etf_name}，目标金额: {target_value_per_etf:.2f}")
                else:
                    self.my_log(f"📦 减持/调仓: {etf} {etf_name}，目标金额: {target_value_per_etf:.2f}")
            else:
                self.my_log(f"下单失败后，当前可用现金: {context.portfolio.cash:.2f}")

        self.my_log("========== 买入操作完成 ==========")

    def _smart_order_target_value(self, security, target_value, context):
        """智能下单函数"""
        current_data = self._get_current_data_ptrade(security)
        security_name = self._get_security_name(security)
        halt_status = get_stock_status(security, 'HALT')
        print(f"security====:{security}")
        print(f"convert:{self._get_switch_code(security)}")
        print(f"halt_status:{halt_status}")
        if halt_status[self._get_switch_code(security)]:
            self.my_log(f"{security} {security_name}: 今日停牌，跳过交易")
            return False
        if current_data['last_price'] >= current_data['high_limit']:
            self.my_log(f"{security} {security_name}: 当前涨停，跳过买入")
            return False
        if current_data['last_price'] <= current_data['low_limit']:
            self.my_log(f"{security} {security_name}: 当前跌停，跳过卖出")
            return False

        current_price = current_data['last_price']
        if current_price == 0:
            self.my_log(f"{security} {security_name}: 当前价格为0，跳过交易")
            return False

        if self._get_switch_code(security) in context.portfolio.positions:
            current_position = context.portfolio.positions[self._get_switch_code(security)]
            if current_position.amount > 0:
                closeable_amount = getattr(current_position, 'enable_amount', 0)
                if closeable_amount == 0:
                    self.my_log(f"{security} {security_name}: 当天买入不可卖出(T+1)")
                    return False

        self.order_target_value_with_split(context, security, target_value)
        
        if target_value == 0:
            self.my_log(f"📤 卖出 {security} {security_name}，目标金额: {target_value:.2f}")
        else:
            self.my_log(f"📥 买入/调整 {security} {security_name}，目标金额: {target_value:.2f}")
        
        return True

    def _is_in_cooldown(self, context):
        """检查是否处于冷却期"""
        if not self.sell_cooldown_enabled:
            return False
        if self.cooldown_end_date is None:
            return False
        return context.current_dt.date() <= self.cooldown_end_date

    def _enter_safe_haven_and_set_cooldown(self, context, trigger_reason):
        """进入避险并设置冷却期"""
        if not self.sell_cooldown_enabled:
            return

        for security in list(context.portfolio.positions.keys()):
            security = self._get_switch_code(security)
            if security in self.fixed_etf_pool or security == self.defensive_etf:
                position = context.portfolio.positions[security]
                if position.amount > 0:
                    security_name = self._get_security_name(security)
                    success = self._smart_order_target_value(security, 0, context)
                    if success:
                        self.my_log(f"✅ [冷却期] 卖出持仓: {security} {security_name}")
                    else:
                        self.my_log(f"❌ [冷却期] 卖出持仓失败: {security} {security_name}")
                    self.position_highs.pop(security, None)
                    self.position_stop_prices.pop(security, None)

        # 兼容回测和实盘的资金计算
        if not is_trade():
            total_value = context.portfolio.portfolio_value * self.account_ratio
        else:
            total_value = g.real_trading_cash_use * self.account_ratio
            
        if total_value > self.min_money:
            success = self._smart_order_target_value(self.safe_haven_etf, total_value * 0.99, context)
            if success:
                safe_name = self._get_security_name(self.safe_haven_etf)
                self.my_log(f"🛡️ [冷却期] 买入避险ETF: {self.safe_haven_etf} {safe_name}，金额: {total_value * 0.99:.2f}")
            else:
                self.my_log(f"❌ [冷却期] 买入避险ETF: {self.safe_haven_etf}")
        else:
            self.my_log(f"💡 [冷却期] 资金不足，无法买入避险ETF。总资产: {total_value:.2f}")

        self.cooldown_end_date = context.current_dt.date() + timedelta(days=self.sell_cooldown_days)
        self.my_log(f"🔒 触发冷却期，结束日期: {self.cooldown_end_date.strftime('%Y-%m-%d')}")
        self.my_log(f"🔒 [冷却期] 已进入冷却期，由 [{trigger_reason}] 触发。")

    def _exit_safe_haven_if_cooldown_ends(self, context):
        """检查并退出避险ETF（若冷却期结束）"""
        if not self.sell_cooldown_enabled or self.cooldown_end_date is None:
            return

        current_date = context.current_dt.date()
        if current_date > self.cooldown_end_date:
            self.my_log(f"🔓 冷却期结束，当前日期: {current_date.strftime('%Y-%m-%d')}")

            safe_haven_etf = self._get_switch_code(self.safe_haven_etf)
            if safe_haven_etf in context.portfolio.positions:
                position = context.portfolio.positions[safe_haven_etf]
                if position.amount > 0:
                    security_name = self._get_security_name(self.safe_haven_etf)
                    success = self._smart_order_target_value(self.safe_haven_etf, 0, context)
                    if success:
                        self.my_log(f"✅ [冷却期结束] 卖出避险ETF: {self.safe_haven_etf} {security_name}")
                    else:
                        self.my_log(f"❌ [冷却期结束] 卖出避险ETF失败: {self.safe_haven_etf} {security_name}")
                    self.position_highs.pop(self.safe_haven_etf, None)
                    self.position_stop_prices.pop(self.safe_haven_etf, None)

            self.cooldown_end_date = None
            self.my_log(f"🔄 策略恢复正常运行")
    
    def after_trading_end(self, context, data=None):
        """盘后执行：保存持久化数据"""
        super().after_trading_end(context, data)
        
        # 实盘时保存持久化数据
        if is_trade():
            try:
                saved_data = {
                    'run_count': self.run_count,
                    'cooldown_end_date': self.cooldown_end_date,
                    'positions': self.positions,
                    'position_highs': self.position_highs,
                    'position_stop_prices': self.position_stop_prices,
                    'target_etfs_list': self.target_etfs_list
                }
                with open(get_research_path() + 'WuFuStrategy.pkl', 'wb') as f:
                    pickle.dump(saved_data, f)
                self.my_log(f"持久化数据保存成功: run_count={self.run_count}")
            except Exception as e:
                self.my_log(f"保存持久化数据失败: {e}", "error")
    
    def after_trading_end(self, context, data=None):
        """盘后执行：保存持久化数据"""
        super().after_trading_end(context, data)
        
        # 实盘时保存持久化数据
        if is_trade():
            try:
                saved_data = {
                    'run_count': self.run_count,
                    'cooldown_end_date': self.cooldown_end_date,
                    'positions': self.positions,
                    'position_highs': self.position_highs,
                    'position_stop_prices': self.position_stop_prices,
                    'target_etfs_list': self.target_etfs_list
                }
                with open(get_research_path() + 'WuFuStrategy.pkl', 'wb') as f:
                    pickle.dump(saved_data, f)
                self.my_log(f"持久化数据保存成功: run_count={self.run_count}")
            except Exception as e:
                self.my_log(f"保存持久化数据失败: {e}", "error")

    def _check_defensive_etf_available(self, context):
        """检查防御性ETF是否可用"""
        try:
            defensive_etf = self.defensive_etf
            current_data = self._get_current_data_ptrade(defensive_etf)
            halt_status = get_stock_status(defensive_etf, 'HALT')
            if halt_status[self._get_switch_code(defensive_etf)]:
                defensive_etf_name = self._get_security_name(defensive_etf)
                self.my_log(f"防御性ETF {defensive_etf} {defensive_etf_name} 今日停牌")
                return False
            if current_data['last_price'] >= current_data['high_limit']:
                defensive_etf_name = self._get_security_name(defensive_etf)
                self.my_log(f"防御性ETF {defensive_etf} {defensive_etf_name} 当前涨停")
                return False
            if current_data['last_price'] <= current_data['low_limit']:
                defensive_etf_name = self._get_security_name(defensive_etf)
                self.my_log(f"防御性ETF {defensive_etf} {defensive_etf_name} 当前跌停")
                return False
            return True
        except Exception as e:
            return False

    def _get_volume_ratio(self, context, security, lookback_days=None, threshold=None, show_detail_log=True):
        """计算成交量比值"""
        if lookback_days is None:
            lookback_days = self.volume_lookback
        if threshold is None:
            threshold = self.volume_threshold

        # try:
        security_name = self._get_security_name(security)
        hist_data = self._get_history_ptrade(security).tail(lookback_days)
        if hist_data.empty or len(hist_data) < lookback_days:
            return None
        past_n_days_vol = hist_data['volume']
        if past_n_days_vol.isnull().any() or past_n_days_vol.eq(0).any():
            return None
        avg_volume = past_n_days_vol.mean()
        if avg_volume == 0:
            return None
        mins = self._minutes_from_today_930(context)
        df_vol = get_history(mins, frequency='1m', field=['volume'], security_list=security, fq='pre', include=True)
        if df_vol.empty:
            return None
        current_volume = df_vol['volume'].sum()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        return volume_ratio
        # except Exception as e:
            # print("None===6")
            # return None

    def _calculate_rsi(self, prices, period=6):
        """计算RSI指标"""
        if len(prices) < period + 1:
            return []

        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gains = np.zeros_like(prices)
        avg_losses = np.zeros_like(prices)

        avg_gains[period] = np.mean(gains[:period])
        avg_losses[period] = np.mean(losses[:period])

        rsi_values = np.zeros(len(prices))
        rsi_values[:period] = 50

        for i in range(period + 1, len(prices)):
            avg_gains[i] = (avg_gains[i - 1] * (period - 1) + gains[i - 1]) / period
            avg_losses[i] = (avg_losses[i - 1] * (period - 1) + losses[i - 1]) / period

            if avg_losses[i] == 0:
                rsi_values[i] = 100
            else:
                rs = avg_gains[i] / avg_losses[i]
                rsi_values[i] = 100 - (100 / (1 + rs))

        return rsi_values[period:]

    def _calculate_atr(self, security, period=14):
        """计算ATR指标"""
        try:
            needed_days = period + 20
            hist_data = self._get_history_ptrade(security).tail(needed_days)

            if len(hist_data) < period + 1:
                return 0, [], False, f"数据不足{period + 1}天"

            high_prices = hist_data['high'].values
            low_prices = hist_data['low'].values
            close_prices = hist_data['close'].values

            tr_values = np.zeros(len(high_prices))
            for i in range(1, len(high_prices)):
                tr1 = high_prices[i] - low_prices[i]
                tr2 = abs(high_prices[i] - close_prices[i - 1])
                tr3 = abs(low_prices[i] - close_prices[i - 1])
                tr_values[i] = max(tr1, tr2, tr3)

            atr_values = np.zeros(len(tr_values))
            for i in range(period, len(tr_values)):
                atr_values[i] = np.mean(tr_values[i - period + 1:i + 1])

            current_atr = atr_values[-1] if len(atr_values) > 0 else 0
            valid_atr = atr_values[period:] if len(atr_values) > period else atr_values
            return current_atr, valid_atr, True, "计算成功"
        except Exception as e:
            return 0, [], False, f"计算出错:{str(e)}"

    def _get_security_name(self, security):
        """安全获取证券名称函数"""
        try:
            name = get_stock_name(security)
            if name is None:
                return "未知名称"
            if not isinstance(name, dict):
                return "未知名称"
            result = name.get(security)
            if result and isinstance(result, str):
                return result
            return "未知名称"
        except Exception as e:
            log.warning(f"获取{security}名称失败: {e}")
            return "未知名称"

    def _get_current_data_ptrade(self, stock):
        """ptrade获取盘中行情"""
        current_data = {}
        if is_trade():
            ret = get_snapshot(stock)
            current_data['high_limit'] = ret[stock]['up_px']
            current_data['low_limit'] = ret[stock]['down_px']
            current_data['day_open'] = ret[stock]['open_px']
            current_data['last_price'] = ret[stock]['last_px']
        else:
            his = get_history(1, frequency='1d', field=['high_limit', 'low_limit', 'open'], security_list=stock, fq='pre', include=True)
            his2 = get_history(1, frequency='1m', field=['price'], security_list=stock, fq='pre', include=True)
            if his.empty:
                return None
            current_data['high_limit'] = his['high_limit'][-1]
            current_data['low_limit'] = his['low_limit'][-1]
            current_data['day_open'] = his['open'][-1]
            current_data['last_price'] = his2['price'][-1]
        return current_data

    def _get_history_ptrade(self, stock):
        """封装get_history函数获取日线数据，去除停牌日期数据，与聚宽数据保持对齐，可以传入列表"""
        day_history = get_history(50, '1d', ['open', 'high', 'low', 'close', 'volume'], stock, fq='pre')
        # 确保数据非空
        if day_history.empty:
            return pd.DataFrame()
        day_history = day_history.query("volume != 0")
        return day_history.dropna()

    def _get_all_securities_ptrade(self):
        """ptrade获取所有etf列表"""
        etf_list = []

        if is_trade():
            etf_list = get_etf_list()
        else:
            code_list = get_trend_data().keys()

            for code in code_list:
                if code.startswith(('159', '51', '52', '53', '55', '56', '58')):
                    etf_list.append(self._get_switch_code(code))
        return etf_list

    def _get_switch_code(self, code):
        """ptrade代码尾缀处理,全称改简称，查询停牌状态返回为简称"""
        if "XSHG" in code or "XSHE" in code:
            new_code = code.replace("XSHG", "SS").replace("XSHE", "SZ")
        # elif "SS" in code or "SZ" in code:
        #     new_code = code.replace("SS", "XSHG").replace("SZ", "XSHE")
        else:
            new_code = code
        return new_code

    def _filter_TSTT_stock(self, context, stock_list):
        """过滤停牌、ST、退市股票"""
        result_halt = get_stock_status(stock_list, query_type='HALT')
        stock_list = [stock for stock in stock_list if not result_halt.get(stock, False)]
        result_st = get_stock_status(stock_list, query_type='ST')
        stock_list = [stock for stock in stock_list if not result_st.get(stock, False)]
        result_delisting = get_stock_status(stock_list, query_type='DELISTING')
        stock_list = [stock for stock in stock_list if not result_delisting.get(stock, False)]
        return stock_list

    def _minutes_from_today_930(self, context):
        """返回从今天 9:30 到 context.current_dt 的分钟数（向下取整，早于9:30返回0）"""
        now = context.current_dt
        tz = now.tzinfo
        open_dt = datetime(now.year, now.month, now.day, 9, 30, 0, tzinfo=tz)
        delta = now - open_dt
        minutes = int(delta.total_seconds() // 60)
        return max(0, minutes)
    
    def _get_current_holdings(self, context):
        """获取当前策略持仓（五福策略）"""
        try:
            holdings = []
            positions = context.portfolio.positions
            
            all_etf_pool = set(self.fixed_etf_pool)
            if hasattr(self, 'dynamic_etf_pool') and self.dynamic_etf_pool:
                all_etf_pool.update(self.dynamic_etf_pool)
            all_etf_pool.add(self.defensive_etf)
            all_etf_pool.add(self.safe_haven_etf)
            
            cur_strategy_set = set(getattr(self, 'cur_strategy_stocks', []))
            all_etf_pool.update(cur_strategy_set)
            
            wufu_pool_with_suffix = set()
            for etf in all_etf_pool:
                wufu_pool_with_suffix.add(etf)
                if '.XSHG' in etf:
                    wufu_pool_with_suffix.add(etf.replace('.XSHG', '.SS'))
                elif '.XSHE' in etf:
                    wufu_pool_with_suffix.add(etf.replace('.XSHE', '.SZ'))
                elif '.SS' in etf:
                    wufu_pool_with_suffix.add(etf.replace('.SS', '.XSHG'))
                elif '.SZ' in etf:
                    wufu_pool_with_suffix.add(etf.replace('.SZ', '.XSHE'))
            
            other_strategy_codes = set()
            for strategy in trading_strategys:
                if strategy is not self and hasattr(strategy, 'cur_strategy_stocks'):
                    for stock in strategy.cur_strategy_stocks:
                        if stock:
                            other_strategy_codes.add(stock.split('.')[0])
            
            for stock, pos in positions.items():
                if pos.amount > 0:
                    stock_prefix = stock.split('.')[0]
                    if stock in wufu_pool_with_suffix:
                        holdings.append(stock)
            return holdings
        except Exception as e:
            self.my_log(f"获取当前持仓失败: {e}", 'error')
            return []

if "wu_fu_strategy" in strategy_config:
    strategy_config['wu_fu_strategy']['class'] = WuFuStrategy

# ============================检测策略冲突==============================
def check_strategy_conflict():
    # 获取n个策略的所有可能交易标的
    enabled_stock_list = []
    enabled_stock_time_sets = []
    enabled_stock_ratio = 0.0
    for strategy in trading_strategys:
        if hasattr(strategy, 'account_ratio'):
            if strategy.account_ratio > 0:
                enabled_stock_ratio += strategy.account_ratio
                enabled_stock_list.extend(getattr(strategy, 'cur_strategy_stocks', []))
                trade_time_attr = getattr(strategy, 'trade_time', None)
                if trade_time_attr is None:
                    continue  # 跳过无时间定义的策略
                # 判断类型并展开
                if isinstance(trade_time_attr, str):
                    enabled_stock_time_sets.append(trade_time_attr)
                elif isinstance(trade_time_attr, (list, tuple)):
                    enabled_stock_time_sets.extend(trade_time_attr)
                else:
                    # 可选：记录警告
                    log.warn(f"⚠️ 策略 {strategy.name} 的 trade_time 类型不支持: {type(trade_time_attr)}")
            
    # 计算标的池交集，统一处理代码后缀
    code_prefixes = []
    for code in enabled_stock_list:
        if code:
            # 提取代码前缀，忽略后缀
            prefix = code.split(".")[0]
            code_prefixes.append(prefix)
    
    # 检查是否有重复的前缀
    overlap_flag = len(enabled_stock_list) != len(set(enabled_stock_list))
    
    if overlap_flag:
        from collections import Counter
        counter = Counter(code_prefixes)
        duplicates = [item for item, count in counter.items() if count > 1]
        
        # 检查重复是否来自同一策略
        strategy_code_map = {}
        for strategy in trading_strategys:
            if hasattr(strategy, 'account_ratio') and strategy.account_ratio > 0:
                stocks = getattr(strategy, 'cur_strategy_stocks', [])
                for stock in stocks:
                    if stock:
                        prefix = stock.split(".")[0]
                        if prefix in duplicates:
                            if prefix not in strategy_code_map:
                                strategy_code_map[prefix] = []
                            strategy_code_map[prefix].append(strategy.name)
        
        # 过滤掉同一策略内部的重复
        actual_conflicts = []
        for prefix, strategies in strategy_code_map.items():
            # 如果多个不同策略都包含同一前缀，则认为是冲突
            if len(set(strategies)) > 1:
                actual_conflicts.append(prefix)
        
        if actual_conflicts:
            error_msg = (
                f"⛔ 策略标的池冲突！冲突的标的为:\n"
                f"{sorted(actual_conflicts)}\n"
                f"建议：调整某策略的基金池（如更换债券/黄金ETF）以避免重叠。"
            )
            log.info(error_msg)
            return False
        else:
            # 没有实际的策略间冲突，只是同一策略内部的后缀不同
            log.info("✅ 多策略标的池检查通过：无重叠，安全运行")
            return True
    elif enabled_stock_ratio > 1.0:
        error_msg = (
            f"⛔ 策略比例分配错误，当前所有策略总比例:"
            f"{enabled_stock_ratio}\n"
            f"建议：调整某策略的资金比例，使所有策略资金占比之和小于等于1。"
        )
        log.error(error_msg)
        return False
    else:
        log.info("✅ 多策略标的池检查通过：无重叠，安全运行")
        return True

# ============================邮件通知配置==============================
def send_qq_email(info=''):
    # 从other_config中获取邮件配置
    email_config = other_config['email_notification']
    sender_email = email_config.get('sender_email')
    receiver_emails = email_config.get('receiver_emails', [])
    smtp_password = email_config.get('smtp_password')
    subject_info = email_config.get('subject')
    enabled = email_config.get('enabled', False)
    print(f"sender_email{sender_email},enabled:{enabled}")
    if enabled:
        log.info(info)
        if sender_email and receiver_emails and smtp_password and is_trade():
            send_email(sender_email, receiver_emails, smtp_password, info=info, subject=subject_info)

# ============================创建策略实例==============================
trading_strategys = []
not_trading_strategys = []  # 非交易策略实例
def create_strategys():
    for name, config in strategy_config.items():
        strategy_cls = config['class']
        params = config['params'].copy()
        if 'account_ratio' in config:
            params['account_ratio'] = config['account_ratio']
        if config['enabled'] and config['account_ratio'] > 0:
            trading_strategys.append(strategy_cls(**params))
        else:
            not_trading_strategys.append(strategy_cls(**params))
    # ================= 标的池冲突检测 =================
    g.trade_flag = check_strategy_conflict()
create_strategys()

# 盘后打印函数
def print_strategy_summary(context):
    """制表展示每日收益，统计每个策略的持仓收益情况以及所有策略总体情况"""
    try:
        print(f"[定时] print_strategy_summary 开始 {context.current_dt}")
        
        # 获取总资产
        total_value = round(getattr(context.portfolio, "portfolio_value", getattr(context.portfolio, "total_value", 0)), 2)
        
        # 获取当前持仓
        positions = context.portfolio.positions
        if not positions:
            # 如果没有持仓，只显示总资产
            print(f"🚤🚤🚤🚤🚤 当前总资产: {total_value} 休息ing ")
            return "当前总资产: " + str(total_value) + " 无持仓"
        
        headers = [
            "策略名称", "股票代码", "股票名称", "持仓数量", "持仓价格",
            "当前价格", "盈亏数额", "盈亏比例", "股票市值", "仓位占比"
        ]
        
        rows = []
        total_market_value = 0
        
        # 获取所有持仓股票的最新价格
        position_stocks = [stock for stock, pos in positions.items() if pos.amount > 0]
        if not position_stocks:
            print(f"🚤🚤🚤🚤🚤 当前总资产: {total_value} 无持仓")
            return "当前总资产: " + str(total_value) + " 无持仓"
            
        try:
            # 获取股票名称
            stock_names = {}
            try:
                stock_name_data = get_stock_name(position_stocks)
                for stock in position_stocks:
                    stock_names[stock] = stock_name_data.get(stock, stock)
            except:
                for stock in position_stocks:
                    stock_names[stock] = stock
            
            # 获取最新价格数据
            current_data_all = {}
            try:
                his = get_history(1, frequency='1d', field=['high_limit', 'low_limit', 'open'], 
                                  security_list=position_stocks, fq='pre', include=True)
                #由于成本价cost basis是通过positions获取的不复权价格，所以这里用得到的最新价格也需要用不复权价格
                his2 = get_history(1, frequency='1m', field=['price'], 
                                   security_list=position_stocks, fq=None, include=False)   
                
                for stock in position_stocks:
                    current_data = {}
                    # 获取历史数据
                    stock_his_data = his[his['code'] == stock]
                    if not stock_his_data.empty:
                        current_data['high_limit'] = stock_his_data['high_limit'].iloc[-1]
                        current_data['low_limit'] = stock_his_data['low_limit'].iloc[-1]
                        current_data['day_open'] = stock_his_data['open'].iloc[-1]
                    
                    # 获取最新价
                    stock_his2_data = his2[his2['code'] == stock]
                    if not stock_his2_data.empty:
                        current_data['last_price'] = stock_his2_data['price'].iloc[-1]
                    
                    current_data_all[stock] = current_data
            except Exception as e:
                print(f"获取价格数据失败: {e}")
            
            # 按策略分组统计持仓
            strategy_positions = {}
            for stock, pos in positions.items():
                if pos.amount > 0:
                    # 查找该股票属于哪个策略
                    strategy_name = "未知策略"
                    for strategy in trading_strategys:
                        if hasattr(strategy, '_get_current_holdings'):
                            try:
                                strategy_holdings = strategy._get_current_holdings(context)
                                if stock in strategy_holdings:
                                    strategy_name = strategy.name
                                    break
                            except:
                                pass
                    
                    if strategy_name not in strategy_positions:
                        strategy_positions[strategy_name] = []
                    strategy_positions[strategy_name].append((stock, pos))
            
            # 统计各策略持仓详情
            strategy_names = list(strategy_positions.keys())
            for i, strategy_name in enumerate(strategy_names):
                stock_pos_list = strategy_positions[strategy_name]
                strategy_market_value = 0
                for stock, pos in stock_pos_list:
                    # 兼容不同环境的字段名
                    current_shares = int(getattr(pos, "total_amount", getattr(pos, "amount", 0)))
                    
                    # 获取当前价格
                    current_price = 0
                    if stock in current_data_all and 'last_price' in current_data_all[stock]:
                        current_price = round(current_data_all[stock]['last_price'], 3)
                    else:
                        current_price = round(getattr(pos, "last_sale_price", 0), 3)
                            
                    avg_cost = round(getattr(pos, "avg_cost", getattr(pos, "cost_basis", 0)), 3)
                            
                    profit_ratio = (current_price - avg_cost) / avg_cost if avg_cost not in (0, None) and avg_cost > 0 else 0
                    profit_ratio_percent = f"{profit_ratio * 100:.2f}% {'↑' if profit_ratio > 0 else '↓'}"
                    profit_amount = round((current_price - avg_cost) * current_shares, 2)
                    market_value = round(current_shares * current_price, 2)
                    strategy_market_value += market_value
                    total_market_value += market_value
                            
                    stock_name = stock_names.get(stock, stock)
                    pos_pct = ""
                    try:
                        base_total = getattr(context.portfolio, "total_value", getattr(context.portfolio, "portfolio_value", None))
                        if base_total and base_total > 0:
                            pos_pct = f"{market_value / base_total * 100:.2f}%"
                    except Exception:
                        pos_pct = ""
                                
                    rows.append([
                        strategy_name, stock, stock_name, f"{current_shares}", f"{avg_cost:.3f}",
                        f"{current_price:.3f}", f"{profit_amount:.2f}", profit_ratio_percent,
                        f"{market_value:.2f}", pos_pct
                    ])
                        
                # 添加策略汇总行
                if strategy_market_value > 0:
                    percentage = (strategy_market_value / total_value * 100) if total_value > 0 else 0
                    rows.append([f"{strategy_name}汇总", "", "", "", "", "", "", "", f"{strategy_market_value:.2f}", 
                                f"{percentage:.2f}%"])
                        
                # 在不同策略之间添加分隔行（除了最后一个策略）
                if i < len(strategy_names) - 1:
                    rows.append(["separator", "", "", "", "", "", "", "", "", ""])
        
        except Exception as e:
            print(f"统计持仓信息失败: {e}")
            return f"统计持仓信息失败: {e}"
        
        percentage_total = (total_market_value / total_value * 100) if total_value > 0 else 0
        rows.append(["总市值汇总", "", "", "", "", "", "", "", f"{total_market_value:.2f}", f"{percentage_total:.2f}%"])
        rows.append(["总资产汇总", "", "", "", "", "", "", "", f"{total_value:.2f}", ""])
        
        # 计算列宽
        col_widths = []
        for i, h in enumerate(headers):
            maxw = len(h)
            for r in rows:
                if i < len(r):
                    maxw = max(maxw, len(str(r[i])))
            col_widths.append(maxw+5)
        
        # 打印表头
        sep = " | "
        header_line = sep.join(h.center(col_widths[i]) for i, h in enumerate(headers))
        line_sep = "---------------------------------------------------------------------------------------------------------------------------------------------------------------"
        print(f"当前总资产: {total_value}")
        print(line_sep)
        print("策略名称              | 股票代码        | 股票名称        | 持仓数量    | 持仓价格     | 当前价格      | 盈亏数额      | 盈亏比例      | 股票市值        | 仓位占比")
        print(line_sep)
        
        # 打印每行内容
        for r in rows:
            # 检查是否为分隔行
            if len(r) >= 1 and str(r[0]) == "separator":
                print(line_sep.replace("-", "="))  # 分隔行
            else:
                line = sep.join(str(r[i]).ljust(col_widths[i]) for i in range(len(headers)))
                print(line)
        print(line_sep)
        
        # 构建返回的摘要信息
        summary_info = f"当前总资产: {total_value}\n"
        for i, strategy_name in enumerate(strategy_names):
            stock_pos_list = strategy_positions[strategy_name]
            strategy_market_value = sum(
                round(int(getattr(pos, "total_amount", getattr(pos, "amount", 0))) * (
                    current_data_all[stock]['last_price'] if stock in current_data_all and 'last_price' in current_data_all[stock] else 
                    getattr(pos, "last_sale_price", 0)
                ), 2) for stock, pos in stock_pos_list
            )
            percentage = (strategy_market_value / total_value * 100) if total_value > 0 else 0
            summary_info += f"{strategy_name}: 持仓市值 {strategy_market_value:.2f}元 ({percentage:.2f}%)\n"
            for stock, pos in stock_pos_list:
                current_shares = int(getattr(pos, "total_amount", getattr(pos, "amount", 0)))
                current_price = round(current_data_all[stock]['last_price'], 3) if stock in current_data_all and 'last_price' in current_data_all[stock] else round(getattr(pos, "last_sale_price", 0), 3)
                avg_cost = round(getattr(pos, "avg_cost", getattr(pos, "cost_basis", 0)), 3)
                market_value = round(current_shares * current_price, 2)
                stock_name = stock_names.get(stock, stock.split('.')[0])
                summary_info += f"{stock_name}({stock}): {current_shares}股, 成本价{avg_cost:.3f}, 现价{current_price:.3f}, 市值{market_value:.2f}元\n"
            summary_info += f"============================================================\n"
                
        # 将转义字符替换为实际的换行符和制表符，确保在邮件中正确显示
        return summary_info
        
    except Exception as e:
        print(f"打印策略收益统计失败: {e}")
        return f"打印策略收益统计失败: {e}"

def get_current_data_trade(context):
    # 获取当日成交信息
    try:
        trades_data = get_trades()
        executed_info = []
        for order_id, trade_list in trades_data.items():
            for trade in trade_list:
                # trade格式: [order_id, account_id, security, direction, amount, price, value, datetime]
                security = trade[2]  # 证券代码
                direction = trade[3]  # 买卖方向
                amount = trade[4]  # 数量
                price = trade[5]  # 价格
                date = trade[7]  # 日期字符串
                executed_info.append(f"{security} {direction} {amount}股 成交价格 {price}元 成交时间: {date}")
    except Exception as e:
        log.error(f"获取成交数据失败: {e}")
        executed_info = []  # 如果获取失败，设为空列表
    
    # 组织邮件内容
    executed_str = "\n".join(executed_info) if executed_info else "无成交"
    info = f"策略运行结束！\n当日成交:\n{executed_str}"
    return info

def initialize(context):
    # 回测设置
    if not is_trade():   
        # 设置佣金（国金的国债无佣金）
        set_commission(commission_ratio =0.0001, min_commission=0.001,type="ETF")
        set_commission(commission_ratio =0.0001, min_commission=0.001,type="LOF")
        # 设置成交数量限制模式
        set_limit_mode(limit_mode='UNLIMITED')

    # 初始化实盘资金
    g.real_trading_cash_use = context.portfolio.portfolio_value * other_config["real_trading_cash_ratio"]
    g.real_trading_cash_use = min(g.real_trading_cash_use, context.portfolio.portfolio_value)
    log.info(f"实盘使用资金:{g.real_trading_cash_use}")

def before_trading_start(context, data):
    """盘前执行函数"""
    # 先执行各策略的 before_trading_start（如策略隔离过滤）
    if g.trade_flag:
        for strategy in trading_strategys:
            if hasattr(strategy, 'account_ratio'):
                if strategy.account_ratio > 0:
                    strategy.before_trading_start(context, data)
            else:
                strategy.before_trading_start(context, data)
    
    # 策略冲突检查（各策略的 before_trading_start 执行后再检测）
    check_strategy_conflict()
    
    if is_trade():
        log.info(f"实盘使用资金:{g.real_trading_cash_use}")
    
    # 策略开始运行邮件通知
    send_qq_email('策略正常开始运行！！\n')

def handle_data(context, data):
    """盘中执行函数"""
    # 更新实盘资金
    g.real_trading_cash_use = context.portfolio.portfolio_value * other_config["real_trading_cash_ratio"]
    g.real_trading_cash_use = min(g.real_trading_cash_use, context.portfolio.portfolio_value)
    if g.trade_flag:
        # 遍历并执行所有交易策略
        for strategy in trading_strategys:
            if hasattr(strategy, 'account_ratio'):
                if strategy.account_ratio > 0:
                    strategy.handle_data(context, data)
            else:
                strategy.handle_data(context, data)
        
        # 遍历并检查所有非交易策略
        # sell_not_trading_strategys(context)
   
def after_trading_end(context, data):
    """盘后执行函数"""
    if g.trade_flag:
        # 遍历所有交易策略
        for strategy in trading_strategys:
            if hasattr(strategy, 'account_ratio'):
                if strategy.account_ratio > 0:
                    strategy.after_trading_end(context, data)
            else:
                strategy.after_trading_end(context, data)
        
        # 盘后邮件同步(发送当日成交信息)
        current_data_trade = get_current_data_trade(context)
        
        # 盘后邮件同步(发送各策略持仓信息)
        strategy_summary = print_strategy_summary(context)
        send_qq_email(info=f"当日成交信息:\n{current_data_trade}\n各策略持仓详情:\n{strategy_summary}\n")

def sell_not_trading_strategys(context):
    current_time = context.current_dt.strftime("%H:%M")
    if current_time == "09:31":
        try:
            log.info("🔄 开始执行非交易策略清仓检查...")
            # 获取所有不交易策略的潜在标的，准备卖出
            not_trading_strategys_stocks = []
            for strategy in not_trading_strategys:
                try:
                    not_trading_strategys_stocks.extend(strategy.cur_strategy_stocks)
                except Exception as e:
                    log.error(f"❌ 获取策略 {strategy.name} 股票池时出错: {e}")
                    continue
            
            # 去重处理
            not_trading_strategys_stocks = list(set(not_trading_strategys_stocks))
            
            # 获取当前持仓
            not_trading_holdings = []
            positions = context.portfolio.positions
            
            # 先获取所有交易策略的股票池
            trading_strategys_stocks = []
            for strategy in trading_strategys:
                try:
                    if hasattr(strategy, 'cur_strategy_stocks'):
                        trading_strategys_stocks.extend(strategy.cur_strategy_stocks)
                except Exception as e:
                    log.error(f"❌ 获取交易策略 {strategy.name} 股票池时出错: {e}")
                    continue
            trading_strategys_stocks = list(set(trading_strategys_stocks))
            
            # 筛选出当前持仓中属于非交易策略且不属于交易策略的股票
            for stock, pos in positions.items():
                if pos.amount > 0 and stock in not_trading_strategys_stocks and stock not in trading_strategys_stocks:
                    not_trading_holdings.append(stock)
            
            log.info(f"📊 非交易策略股票池共 {len(not_trading_strategys_stocks)} 只股票，当前持仓中有 {len(not_trading_holdings)} 只需要清仓")
            
            for stock in not_trading_holdings:
                log.info(f"❌ 卖出非交易策略标的: {stock}")
                order_target_value(stock, 0)
            
            if not_trading_holdings:
                log.info(f"✅ 非交易策略清仓完成，共清仓 {len(not_trading_holdings)} 只股票")
            else:
                log.info("✅ 无非交易策略持仓，无需清仓")
        except Exception as e:
            log.error(f"❌ 非交易策略清仓过程出错: {e}")
            import traceback
            log.error(f"详细错误信息: {traceback.format_exc()}")