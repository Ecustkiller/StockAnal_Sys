#======================================================================================
#1.会员功能开关，在31行修改0或者1. （0关闭/1开启））
#1.如果更改账号使用需要在34行，更改账号使用
#2.如果账号过了有效期，可以在36.37行修改日期
#3.如果需要修改仓位可以在52.53行修改仓位和百分比
#5.企业微信开启或者关闭如果需要修改，在188行修改0或者1. （0关闭/1开启））
#6.企业微信机器人修改，在189行，（可以换成自己的机器人KEY链接）
#鲸鱼喷水策略，经测试无未来函数
#如果想盈利高，能接受大回撤的可以在170到181行， 止盈止损开关配置。适量关闭一些。False=关闭  True=开启
#======================================================================================
import pandas as pd
import datetime
import numpy as np
import time
import logging
from datetime import timedelta
import requests
import json
import urllib.request
from urllib.error import URLError, HTTPError
"""
策略核心功能：基于技术指标和风险评估的股票交易策略=双指数下跌则清仓
"""
class TradingStrategy:
    """股票交易策略主类，整合策略初始化、市场分析、交易执行和风险管理全流程"""
    
    def __init__(self):
        """策略初始化方法，集中配置所有参数和状态变量"""
        
        # ================= 会员功能开关 =================
        self.enable_membership = 1                             # 0关闭/1开启（用户修改这里）
        self.enable_wechat_notify = 1  # 消息推送开关 0关闭/1开启（用户修改这里）
        self.wechat_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c2de2024-98b7-45cd-90b1-19afc727708c"  # 替换为你的Webhook密钥
        self.validity_end = datetime.datetime(2070, 10, 16).date()   # 有效期结束日     更换账号有效期结束日使用修改这里
        # ================= 账号和有效期设置 =================
        self.allowed_account_name = "59016421"                      # 唯一允许使用的账号    更换账号使用修改这里
        self.user_account_name = "59016421"  # 用户必须修改为实际账号名称
        # 设置准确的有效期范围（2025年7月20日至2070年12月31日）
        self.validity_start = datetime.datetime(2024, 7, 20).date()  # 有效期起始日     更换账号有效期起始日使用修改这里
        
        
        self.expired = False  # 策略过期标志
        self.not_yet_valid = False  # 策略尚未生效标志
        self.account_valid = False  # 账号有效性标志
        self.current_account = None  # 当前账号
        self.validity_days = 0  # 存储剩余天数
        
        # =================== 基础交易参数 ===================
        self.security = []          # 动态股票池，存储待交易股票代码
        self.index = "000001.SS"    # 基准指数，用于成分股与大盘风险参照
        self.lookback_days = 60     # 历史数据回溯天数，用于技术指标计算
        self.ma_periods = [5, 10, 20, 30, 60]  # 均线计算周期，支持多周期趋势判断
        
        #====================单股最高持仓金额==================
        self.max_holdings = 3      # 最大持仓股票数量，控制分散度                         更换持仓股票使用修改这里
        self.target_position_percentage = 0.33  # 单只股票目标持仓比例（可全局修改）      更换持仓股票百分比使用修改这里
        self.stock_name_cache = {}  # 股票名称缓存
       
        #====================================================
        self.max_risk_score = 0.005  # 最大可接受风险评分阈值，高于此值拒绝买入
        self.price_min = 10          # 筛选股票的最小股价（元）
        self.price_max = 50          # 筛选股票的最大股价（元）
        self.trade_cooldown = {}     # 交易冷却期，避免短时间内重复交易同一股票
        
        # ========================大盘风险控制参数/双指数管理=====================
        self.market_indices = [
            {'code': '000001.SS', 'name': '上证指数', 'threshold': -0.3},  # 上证指数下跌阈值
            {'code': '000300.SS', 'name': '沪深300', 'threshold': -0.3}  # 300指数下跌阈值
        ]
        self.combined_threshold = -0.6  # 双指数合值阈值（初始值0.5--0.6））
        self.market_bearish = False      # 大盘熊市状态标记
        self.morning_bearish = False    # 早盘大盘状态标记
        
        # =================== 定义时间点,字典时间点配置 ====================
        self.time_points = {
            "每日清空股票池": (9, 30),
            "成分股分析": (9, 33),
            "大盘风险检查": (9, 32),
            "待交易池价格获取": (9, 45),  # 新增
            "上午盘-止盈止损执行": (11, 25),
            "下午盘-止盈止损执行": (14, 45),
            "盘后数据确认": (15, 00)
        }
        
        # 价格获取时间点（时，分）
        self.data_times = [
            (9, 45),   # 新增：9:45获取待交易池价格
            (11, 25),  # 11:25 获取持仓股票价格
            (14, 45)   # 14:54 获取持仓股票价格         
        ]
        
        # 交易执行时间点（时，分）
        self.trade_times = [
            self.time_points["上午盘-止盈止损执行"],
            self.time_points["下午盘-止盈止损执行"]
        ]
        self.post_market_time = self.time_points["盘后数据确认"]  # 盘后时间变量
        
        #=======================成分股分析状态标记=====================
        self.analysis_completed = False        # 成分股分析完成标志
        self.analysis_type = None              # 记录分析类型（morning/afternoon）
        
        # ====================== 其他参数保持不变 =====================
        # 风险评估参数
        self.SHORT_TERM_GROWTH_THRESHOLD = 0.15  # 短期涨幅容忍阈值（15%），防止追高
        self.VOLUME_ABNORMAL_RATIO = 3.0         # 成交量异常敏感度（3倍均量）
        self.RSI_OVERBOUGHT_LEVEL = 90           # RSI超买阈值，高于此值视为超买
        self.RSI_OVERSOLD_LEVEL = 30             # RSI超卖阈值，低于此值视为超卖
        self.KDJ_J_OVERBOUGHT = 110               # KDJ J值超买阈值
        self.KDJ_J_OVERSOLD = 10                 # KDJ J值超卖阈值
        self.MIN_VOLUME_RATIO = 0.4              # 最小成交量比率（相对于20日均量）
        self.INCLUDE_CURRENT_DATA = False         # 是否包含当前交易日数据
        self.VOLUME_SPIKE_THRESHOLD = 2.0        # 成交量 spike 阈值（60日均量的倍数）
        
        # 市盈率控制参数
        self.max_pe_ttm = 200  # 最大允许的市盈(TTM)，超过此值的股票将被排除
        self.skip_high_pe_count = 0  # 因高PE被跳过的股票计数器
        
        # 交易成本参数
        self.SLIPPAGE = 0.001        # 滑点率，模拟实际交易中的价格偏差
        
        # 状态标记变量
        self.executed_data = {time: False for time in self.data_times}    # 价格获取状态标记
        self.executed_trade = {time: False for time in self.trade_times}  # 交易执行状态标记
        self.skip_pe_ttm_count = 0  # 市盈(TTM)亏损股票计数器
        
        # 交易冷却期参数
        self.BUY_COOLDOWN_DAYS = 5       # 买入后冷却期（天）
        self.SELL_COOLDOWN_DAYS = 3      # 卖出后冷却期（天）
        
        # 交易状态记录
        self.current_date = None      # 当前交易日，用于每日状态重置
        self.latest_prices = {}       # 最新价格缓存，减少重复获取
        self.optimized_stocks = []    # 优选股票池（带风险评分和形态优先级）
        self.position_profit = {}     # 持仓盈亏情况记录
        self.position_report = []     # 持仓报告，用于日志输出
        self.need_position_report = False   # 记录是否需要生成持仓报告的标记
        self.executed_orders = set()  # 已执行订单ID集合，避免重复下单
        self.today_traded = set()     # 当日已交易股票，避免重复交易
        self.total_profit = 0.0       # 累计总盈利
        self.trade_history = []       # 交易历史记录，用于策略分析
        self.price_index = {}         # 价格索引表，快速查询股票价格
        self.current_holdings = 0     # 当前实际持仓数量（排除科创板）
        self.last_holdings_update = None  # 记录上次更新时间，避免频繁计算
        # 价格重试次数配置
        self.price_retry_count = 3
        self.price_retry_interval = 0.01  # 重试间隔（分钟）
        # 止盈止损参数
        self.profit_take_threshold = 0.1  # 止盈阈值（10%），达到时卖出获利
        self.stop_loss_threshold = -0.025  # 止损阈值（-2.5%），达到时卖出止损
        
        # 月底清仓参数
        self.enable_monthly_clear = False  # 月底清仓开关
        self.last_clear_month = None      # 上次清仓的月份
        
        # 收益记录参数
        self.realized_profits = {}   # 存储已实现盈亏 {股票代码: 盈亏金额} 
        
        # 交易费用记录参数
        self.total_commission = 0       # 总佣金支出（元）
        self.total_stamp_duty = 0      # 总印花税支出（元）
        self.total_transfer_fee = 0       # 总过户费支出（元）-
        self.trade_fee_history = []       # 交易费用历史记录
        self.commission_rate = 0.0001     # 佣金费率（万分之一）
        self.stamp_duty_rate = 0.0005   # 印花税费率（万分之五）
        self.transfer_fee_rate = 0.00001  # 过户费费率（万0.1）
        self.commission_min = 5.0         # 佣金最低收费（5元）
        self.transfer_fee_min = 1.0       # 过户费最低收费（1元）
        
        # 形态分析开关
        self.enable_bullish_pattern = True  # 启用看涨形态分析
        self.enable_bearish_pattern = True  # 启用看跌形态分析
        
        # 止盈止损开关配置
        self.enable_fixed_take_profit = False    # 固定止盈开关 (5%)
        self.enable_fixed_stop_loss = True      # 固定止损开关 (-3%)
        self.enable_rsi_take_profit = True      # RSI指标止盈开关
        self.enable_kdj_take_profit = True     # KDJ指标止盈开关   False=关闭  True=开启
        self.enable_bearish_pattern_stop = True # 形态止损总开关
        self.enable_bear_engulfing_stop = True   # 看跌吞没形态止损
        self.enable_dark_cloud_stop = True       # 乌云盖顶形态止损
        self.enable_evening_star_stop = True     # 黄昏之星形态止损
        self.enable_three_crows_stop = True      # 三只乌鸦形态止损
        self.enable_shooting_star_stop = False    # 射击之星形态止损
        self.enable_hanging_man_stop = False      # 吊颈线形态止损
        self.enable_tweezers_top_stop = False     # 平顶形态止损
        
        self.initialization_phase = True
        self.holdings_sync_retry = 0
        self.max_retry = 3
        
        # 企业微信机器人配置
        
        self.strategy_name = "鲸鱼喷水策略"
        self.wechat_retry_times = 3  # 消息发送重试次数
        self.initialization_test_sent = False  # 新增初始化测试标志
        
        # 新增盘后持仓报告开关
        self.enable_position_report_push = True  # 是否启用盘后持仓报告推送
        self.position_report_time = (15, 00)  # 盘后持仓报告推送时间
        
    #===============================================================
    #                           初始函数    
    #===============================================================
    def initialize(self, context):
        """策略初始化函数，检查账号和有效期"""
        # 会员功能关闭时跳过所有检查
        if self.enable_membership == 0:
            self.account_valid = True
            # log.info("会员功能已关闭，跳过账号和有效期检查")
            
            # 关键修复：执行完整的初始化流程
            self.initialized = True
            # ================= 原有初始化代码 =================
            # 设置每日定时任务
            run_daily(context, self.analyze_index_stocks, time=self.format_time(*self.time_points["成分股分析"]))
            run_daily(context, self.check_market_risk, time=self.format_time(*self.time_points["大盘风险检查"]))
            run_daily(context, self.sync_holdings_daily, time='09:30')         
      
            # 设置月底清仓任务 - 修复run_monthly问题
            if self.enable_monthly_clear:
                # 改为每日检查月底
                run_daily(context, self.check_month_end, time='14:55')
        
            # 初始化后立即更新持仓数量
            self.current_holdings = self.calculate_valid_holdings(context)
        
            # 设置价格获取定时任务
            for time_tuple in self.data_times:
                run_daily(context, self.fetch_prices, time=self.format_time(*time_tuple))
            
            # 初始化股票池和记录初始资金
            set_universe(self.security)
            self.initial_capital = context.portfolio.starting_cash
            self.current_total_assets = context.portfolio.portfolio_value
            log.info(f"策略初始化完成，初始资金: {self.initial_capital:.2f}元")
            log.info(f"初始总资产: {self.current_total_assets:.2f}元")
            self.log_strategy_settings()
            
            # 尝试发送初始化消息（仅实盘发送）
            if not self.initialization_test_sent and self.enable_wechat_notify == 1:
                if is_trade():
                    test_message = (
                        f"✅ 策略初始化成功通知（会员功能已关闭）\n"
                        f"策略名称: {self.strategy_name}\n"
                        f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"初始资金: {self.initial_capital:.2f}元\n"
                        f"当前总资产: {self.current_total_assets:.2f}元"
                    )
                    if self.send_wechat_message(test_message):
                        log.info("企业微信测试消息发送成功")
                        self.initialization_test_sent = True
                    else:
                        log.error("企业微信测试消息发送失败，请检查配置")
                else:
                    self.initialization_test_sent = True
                    
            return
            
        # 原有会员检查代码
        try:
            # 获取当前账号名称（使用用户设置的值）
            current_account_name = self.user_account_name
        
            # 检查账号是否允许使用
            if current_account_name != self.allowed_account_name:
                self.account_valid = False
                log.error(f"账号名称 '{current_account_name}' 不允许运行此策略！策略仅限账号 '{self.allowed_account_name}' 使用。")
                return
                
            # 账号正确，检查有效期
            self.account_valid = True
            current_real_date = datetime.datetime.now().date()  # 真实当前日期
         
            # 检查是否在有效期内
            if current_real_date < self.validity_start:
                self.not_yet_valid = True
                log.error(f"策略尚未生效！当前日期 {current_real_date} 早于有效期起始日 {self.validity_start}")
                return
            
            if current_real_date > self.validity_end:
                self.expired = True
                log.error(f"策略已过期！当前日期 {current_real_date} 超过有效期结束日 {self.validity_end}")
                return
        
            # 账号有效且在有效期内，继续正常初始化
            # 计算剩余天数（基于真实日期）
            validity_days = (self.validity_end - current_real_date).days
            # log.info(f"策略初始化成功，账号名称 '{current_account_name}' 在有效期内（有效期: {self.validity_start} 至 {self.validity_end}，剩余 {validity_days} 天）")
            # log.info(f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # ================= 原有初始化代码 =================
            # 设置每日定时任务
            run_daily(context, self.analyze_index_stocks, time=self.format_time(*self.time_points["成分股分析"]))
            run_daily(context, self.check_market_risk, time=self.format_time(*self.time_points["大盘风险检查"]))
            run_daily(context, self.sync_holdings_daily, time='09:30')         
      
            # 设置月底清仓任务 - 修复run_monthly问题
            if self.enable_monthly_clear:
                # 改为每日检查月底
                run_daily(context, self.check_month_end, time='14:55')
        
            # 初始化后立即更新持仓数量
            self.current_holdings = self.calculate_valid_holdings(context)
        
            # 设置价格获取定时任务
            for time_tuple in self.data_times:
                run_daily(context, self.fetch_prices, time=self.format_time(*time_tuple))
            
            # 标记初始化阶段
            self.initialized = True
          
            # 初始化股票池和记录初始资金
            set_universe(self.security)
            self.initial_capital = context.portfolio.starting_cash
            self.current_total_assets = context.portfolio.portfolio_value
            log.info(f"策略初始化完成，初始资金: {self.initial_capital:.2f}元")
            log.info(f"初始总资产: {self.current_total_assets:.2f}元")
            self.log_strategy_settings()
        
            # 发送初始化测试消息（仅实盘发送）
            if not self.initialization_test_sent and self.enable_wechat_notify == 1:
                if is_trade():
                    test_message = (
                        f"✅ 策略初始化成功通知\n"
                        f"策略名称: {self.strategy_name}\n"
                        f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"初始资金: {self.initial_capital:.2f}元\n"
                        f"当前总资产: {self.current_total_assets:.2f}元"
                    )
                    if self.send_wechat_message(test_message):
                        log.info("企业微信测试消息发送成功")
                        self.initialization_test_sent = True
                    else:
                        log.error("企业微信测试消息发送失败，请检查配置")
                else:
                    self.initialization_test_sent = True
                
        except Exception as e:
            log.error(f"初始化过程中发生异常: {e}")
            # 记录详细上下文信息用于调试
            log.error(f"错误类型: {type(e)}")
            log.error(f"错误详细信息: {str(e)}")
            # 可以记录堆栈信息
            import traceback
            log.error(traceback.format_exc())
            self.account_valid = False
            
    # 新增月底检查方法
    def check_month_end(self, context):
        """每日检查是否月底"""
        if not self.enable_monthly_clear:
            log.info("月底清仓功能已关闭，跳过清仓操作")
            return
            
        current_date = context.current_dt.date()
        current_month = current_date.month
        month_name = context.current_dt.strftime("%Y年%m月")  # 获取月份名称

        # 如果是同一个月已清仓，跳过
        if self.last_clear_month == current_month:
            log.info(f"本月（{current_month}月）已执行过清仓操作，跳过")
            return
        
        # 检查是否为自然月最后一天（通过计算明天是否为下个月）
        tomorrow = current_date + timedelta(days=1)
        if tomorrow.month != current_date.month:
            log.info(f"检测到月底交易日: {current_date}，执行清仓操作")
            self.clear_positions_at_month_end(context)

                
    def get_stock_name(self, stock_code):
        """获取股票名称（带缓存）"""
        if stock_code in self.stock_name_cache:
            return self.stock_name_cache[stock_code]
    
        try:
            # PTrade使用get_stock_name内置函数获取股票名称
            name_dict = get_stock_name(stock_code)
            if isinstance(name_dict, dict) and stock_code in name_dict:
                name = name_dict[stock_code]
                self.stock_name_cache[stock_code] = name
                return name
            return None
        except Exception as e:
            log.error(f"获取股票名称失败: {e}")
            return None
            
    #发送消息到企业微信和开关控制     
    def send_wechat_message(self, message):
        """发送消息到企业微信"""
        # 回测模式不发送通知，只有实盘才发
        if not is_trade():
            return False
        # 检查开关状态
        if self.enable_wechat_notify == 0:  # 0表示关闭
            return False
            
        if not self.enable_wechat_notify:
            return
 
        # 添加调试信息
        log.info(f"准备发送企业微信消息，内容长度: {len(message)}")
        # 检查消息长度，企业微信限制为2048字节
        max_length = 2048
        if len(message.encode('utf-8')) > max_length:
            # 如果消息过长，截取前2048字节
            truncated_message = message.encode('utf-8')[:max_length].decode('utf-8', 'ignore')
            log.warning(f"消息过长({len(message.encode('utf-8'))}字节)，已截断至{len(truncated_message.encode('utf-8'))}字节")
            message = truncated_message
            
        headers = {"Content-Type": "application/json"}
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        data = json.dumps(payload).encode('utf-8')
    
        for i in range(self.wechat_retry_times):
            try:
                req = urllib.request.Request(
                    self.wechat_webhook,
                    data=data,
                    headers=headers,
                    method='POST'
                )
                with urllib.request.urlopen(req, timeout=5) as response:
                    result = json.loads(response.read().decode('utf-8'))
                    if result.get("errcode") == 0:
                        log.info("企业微信消息发送成功")
                        return True
                    else:
                        log.error(f"企业微信消息发送失败: {result.get('errmsg')}")
            except (URLError, HTTPError) as e:
                log.error(f"企业微信消息发送异常(尝试{i+1}/{self.wechat_retry_times}): {e.reason}")
                time.sleep(1)
            except Exception as e:
                log.error(f"企业微信消息发送异常(尝试{i+1}/{self.wechat_retry_times}): {str(e)}")
                time.sleep(1)
    
        log.error("企业微信消息发送失败，已达最大重试次数")
        return False    
        
    def format_time(self, hour, minute):
        """格式化时间为HH:MM字符串"""
        return f"{hour}:{minute:02d}"
        
    def sync_holdings_daily(self, context):
        """每日定时同步持仓数据，增强异常处理"""
        try:
            self.sync_holdings(context)
            log.info("每日持仓数据同步完成，当前有效持仓数量: {}".format(self.current_holdings))
        except Exception as e:
            log.error(f"每日持仓同步异常: {e}")
        
    def calculate_valid_holdings(self, context):
        """计算有效持仓数量（修复科创板排除逻辑）"""
        positions = context.portfolio.positions
        valid_count = 0
        for stock in positions:
            if not stock.startswith('688') and positions[stock].amount > 0:
                valid_count += 1
        return valid_count
        
    def update_holdings(self, context):
        """统一更新持仓数量的方法，添加同步前置检查"""
        if not hasattr(context, 'portfolio') or not context.portfolio.positions:
            return
        
        self.current_holdings = self.calculate_valid_holdings(context)
        log.debug(f"持仓数量更新为: {self.current_holdings}")
        
    #===============================================================
    #                         记录策略设置
    #===============================================================    
    def log_strategy_settings(self):
        """记录策略设置"""
        # 打印企业微信和会员状态
        log.info(f"企业微信通知状态: {'开启' if self.enable_wechat_notify == 1 else '关闭'}")
        # log.info(f"会员功能状态: {'开启' if self.enable_membership == 1 else '关闭'}")
        log.info(f"最大持仓数量: {self.max_holdings}只")
        log.info(f"单只股票目标仓位: 33%")
        log.info("止盈止损开关状态:")
        log.info(f"固定止盈开关: {'开启' if self.enable_fixed_take_profit else '关闭'}")
        log.info(f"固定止损开关: {'开启' if self.enable_fixed_stop_loss else '关闭'}")
        log.info(f"RSI止盈开关: {'开启' if self.enable_rsi_take_profit else '关闭'}")
        log.info(f"KDJ止盈开关: {'开启' if self.enable_kdj_take_profit else '关闭'}")
        log.info(f"形态止损总开关: {'开启' if self.enable_bearish_pattern_stop else '关闭'}")
        log.info(f"看跌吞没止损开关: {'开启' if self.enable_bear_engulfing_stop else '关闭'}")
        log.info(f"乌云盖顶止损开关: {'开启' if self.enable_dark_cloud_stop else '关闭'}")
        log.info(f"黄昏之星止损开关: {'开启' if self.enable_evening_star_stop else '关闭'}")
        log.info(f"三只乌鸦止损开关: {'开启' if self.enable_three_crows_stop else '关闭'}")
        log.info(f"射击之星止损开关: {'开启' if self.enable_shooting_star_stop else '关闭'}")
        log.info(f"吊颈线止损开关: {'开启' if self.enable_hanging_man_stop else '关闭'}")
        log.info(f"平顶止损开关: {'开启' if self.enable_tweezers_top_stop else '关闭'}")
        log.info(f"月底清仓开关: {'开启' if self.enable_monthly_clear else '关闭'}")
        
        # 打印费用设置
        log.info("交易费用设置:")
        log.info(f"佣金费率: {self.commission_rate*10000}%% (万分之一)")
        log.info(f"印花税费率: {self.stamp_duty_rate*1000}‰ (千分之一)")
        log.info(f"过户费费率: {self.transfer_fee_rate*100000}‰% (万0.1)")
        
    #月底清仓函数（添加通知支持）    
    def clear_positions_at_month_end(self, context):
        """月底清仓函数（添加通知支持）"""
        if not self.enable_monthly_clear:
            log.info("月底清仓功能已关闭，跳过清仓操作")
            return
            
        current_date = context.current_dt.date()
        current_month = current_date.month
        month_name = context.current_dt.strftime("%Y年%m月")  # 获取月份名称
    
        # 如果是同一个月已清仓，跳过
        if self.last_clear_month == current_month:
            log.info(f"本月（{current_month}月）已执行过清仓操作，跳过")
            return
        
        # 检查是否为自然月最后一天（通过计算明天是否为下个月）
        tomorrow = current_date + timedelta(days=1)
        if tomorrow.month == current_month:
            log.info(f"今天({current_date})不是本月最后一天，无需清仓")
            return
        
        log.info("==== 月底清仓卖出开始 ====")
        log.info(f"检测到月底交易日: {current_date}，执行清仓操作")
        
        # 发送清仓开始通知（统一使用🚨🚨🚨🚨图标）
        positions = context.portfolio.positions
        if self.enable_wechat_notify:
            start_msg = (
                f"🚨🚨🚨🚨 {month_name}月底清仓卖出开始\n"
                f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"当前持仓: {len(positions)}只股票"
            )
            self.send_wechat_message(start_msg)
    
        if not positions:
            log.info("当前无持仓，无需清仓")
            self.last_clear_month = current_month
            return
        
        # 记录清仓前总资产
        total_assets_before = self.calculate_total_assets(context)
        log.info(f"清仓前总资产: {total_assets_before:.2f}元")
    
        # 主动获取1分钟历史价格
        gear_price_cache = {}
        for stock in positions.keys():
            normalized_stock = self.normalize_stock_code(stock)
            try:
                hist_data = get_history(
                    count=1,
                    frequency='1m',
                    field=['open', 'close', 'high', 'low', 'volume','price'],
                    security_list=[normalized_stock],
                    include=True
                )
                if not hist_data.empty:
                    latest_price = hist_data['price'].iloc[-1]
                    gear_price_cache[normalized_stock] = latest_price
            except Exception as e:
                log.error(f"获取{normalized_stock}历史价格失败: {e}")
    
        # 初始化统计变量
        sold_count = 0
        failed_count = 0
        total_profit = 0.0
        
        # 遍历所有持仓并卖出
        for stock in list(positions.keys()):
            position = positions[stock]
            if position.amount <= 0:
                continue
            
            normalized_stock = self.normalize_stock_code(stock)
            current_price = gear_price_cache.get(normalized_stock)
        
            if current_price is None:
                log.error(f"无法获取{normalized_stock}的最新价格，使用持仓均价")
                current_price = position.avg_price
            
            # 计算保护限价
            limit_price = None
            if current_price is not None:
                limit_price = round(current_price * 0.995, 2)  # 使用更保守的0.995倍
                log.info(f"【月底清仓】{normalized_stock}保护限价: {current_price:.2f} * 0.995 = {limit_price}")
        
            # 计算盈亏
            profit = (current_price - position.avg_price) * position.amount
        
            # 执行卖出订单-清仓
            try:
                # 移除market_type参数（平台兼容性）
                order_result = order(stock, -position.amount, limit_price)
            
                if order_result:
                    # 获取股票名称
                    stock_name = self.get_stock_name(normalized_stock) or "未知名称"
                    
                    log.info(
                        f"月底清仓卖出: {normalized_stock}, "
                        f"数量: {position.amount}股, "
                        f"价格: {current_price:.2f}, "
                        f"保护限价: {limit_price if limit_price else '无'}, "
                        f"盈亏: {profit:.2f}元"
                    )
                    self.today_traded.add(normalized_stock)
                    
                    # 发送清仓成功通知（统一使用📉📉图标）
                    if self.enable_wechat_notify:
                        success_msg = (
                            f"📉 卖出成功\n"
                            f"股票: {normalized_stock} ({stock_name})\n"
                            f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"数量: {position.amount}股\n"
                            f"价格: {current_price:.2f}元\n"
                            f"盈亏: {profit:.2f}元\n"
                            f"原因: 月底清仓"
                        )
                        
                        # 发送并确保通知送达
                        if not self.send_wechat_message(success_msg):
                            log.warning(f"清仓卖出通知发送失败: {normalized_stock}")
                            time.sleep(0.5)
                            self.send_wechat_message(success_msg)
                    
                    sold_count += 1
                    total_profit += profit
                else:
                    log.error(f"月底清仓卖出失败: {normalized_stock}")
                    
                    # 发送失败通知（统一使用❌❌图标）
                    if self.enable_wechat_notify:
                        stock_name = self.get_stock_name(normalized_stock) or "未知名称"
                        fail_msg = (
                            f"❌❌ 月底清仓卖出失败\n"
                            f"股票: {normalized_stock} ({stock_name})\n"
                            f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"数量: {position.amount}股"
                        )
                        self.send_wechat_message(fail_msg)
                    
                    failed_count += 1
            except Exception as e:
                log.error(f"清仓订单执行异常: {e}")
                failed_count += 1
                
                # 发送异常通知
                if self.enable_wechat_notify:
                    stock_name = self.get_stock_name(normalized_stock) or "未知名称"
                    error_msg = (
                        f"🚨🚨 月底清仓卖出异常\n"
                        f"股票: {normalized_stock} ({stock_name})\n"
                        f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"错误: {str(e)[:100]}"
                    )
                    self.send_wechat_message(error_msg)
        
        # 更新清仓月份标记
        self.last_clear_month = current_month
    
        # 记录清仓后总资产
        total_assets_after = self.calculate_total_assets(context)
        log.info(f"清仓后总资产: {total_assets_after:.2f}元")
        
        # 发送清仓完成通知（统一使用✅图标）
        if self.enable_wechat_notify:
            end_msg = (
                f"✅ {month_name}月底清仓完成\n"
                f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"成功清仓股票: {sold_count}只\n"
                f"清仓失败股票: {failed_count}只\n"
                f"总盈亏: {total_profit:.2f}元\n"
                f"现金余额: {context.portfolio.cash:.2f}元"
            )
            self.send_wechat_message(end_msg)
            
        log.info("==== 月底清仓完成 ====")
    
    def fetch_current_positions_prices(self, context):
        """获取持仓股票的当前价格"""
        positions_prices = {}
        positions = context.portfolio.positions
        
        if not positions:
            return positions_prices
            
        try:
            position_stocks = [self.normalize_stock_code(stock) for stock in positions.keys()]
            hist_data = get_history(
                count=1,
                frequency='1m',
                field=['open', 'close', 'high', 'low', 'volume','price'],
                security_list=position_stocks,
                include=True
            )
            
            for stock in position_stocks:
                if stock in hist_data['code'].values:
                    price = hist_data[hist_data['code'] == stock]['price'].iloc[-1]
                    positions_prices[stock] = price
                else:
                    positions_prices[stock] = None
                    
            return positions_prices
        except Exception as e:
            log.error(f"获取持仓股票价格失败: {e}")
            return positions_prices
    
    def fetch_trade_pool_current_prices(self, context):
        """获取待交易股票池的当前价格"""
        trade_pool_prices = {}
        if not self.security:
            return trade_pool_prices
            
        try:
            trade_stocks = [self.normalize_stock_code(stock) for stock in self.security]
            hist_data = get_history(
                count=1,
                frequency='1m',
                field=['open', 'close', 'high', 'low', 'volume','price'],
                security_list=trade_stocks,
                include=True
            )
            
            for stock in trade_stocks:
                if stock in hist_data['code'].values:
                    price = hist_data[hist_data['code'] == stock]['price'].iloc[-1]
                    trade_pool_prices[stock] = price
                else:
                    trade_pool_prices[stock] = None
                    
            return trade_pool_prices
        except Exception as e:
            log.error(f"获取待交易股票池价格失败: {e}")
            return trade_pool_prices
       
    def calculate_trade_fees(self, trade_value, is_sell=False):
        """
        计算交易费用（包含最低收费限制）
        :param trade_value: 交易金额（元）
        :param is_sell: 是否为卖出交易
        :return: 包含各项费用的字典
        """
        if trade_value <= 0:
            return {
                'commission': 0,
                'stamp_duty': 0,
                'transfer_fee': 0,
                'total_fee': 0
            }
            
        # 计算基础费用
        commission_base = trade_value * self.commission_rate
        stamp_duty = trade_value * self.stamp_duty_rate if is_sell else 0
        transfer_fee_base = trade_value * self.transfer_fee_rate
        
        # 应用最低收费限制
        commission = max(commission_base, self.commission_min)
        transfer_fee = max(transfer_fee_base, self.transfer_fee_min)
        
        total_fee = commission + stamp_duty + transfer_fee
        return {
            'commission': commission,
            'stamp_duty': stamp_duty,
            'transfer_fee': transfer_fee,
            'total_fee': total_fee
        }
    
    def record_trade_fees(self, fees, stock_code, is_sell=False):
        """
        记录交易费用并更新总费用
        :param fees: 交易费用字典
        :param stock_code: 股票代码
        :param is_sell: 是否为卖出交易
        """
        # 更新总费用
        self.total_commission += fees['commission']
        self.total_stamp_duty += fees['stamp_duty']
        self.total_transfer_fee += fees['transfer_fee']
        
        # 记录费用历史
        trade_type = "卖出" if is_sell else "买入"
        self.trade_fee_history.append({
            'date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'stock': stock_code,
            'type': trade_type,
            'commission': fees['commission'],
            'stamp_duty': fees['stamp_duty'],
            'transfer_fee': fees['transfer_fee'],
            'total_fee': fees['total_fee']
        })
        
        # 打印费用日志（显示最低收费应用）
        commission_note = f"(最低收费{self.commission_min}元)" if fees['commission'] == self.commission_min else ""
        transfer_note = f"(最低收费{self.transfer_fee_min}元)" if fees['transfer_fee'] == self.transfer_fee_min else ""
        log.info(f"{trade_type} {stock_code} 交易费用 - "
                 f"佣金: {fees['commission']:.2f}元 {commission_note}, "
                 f"印花税: {fees['stamp_duty']:.2f}元, "
                 f"过户费: {fees['transfer_fee']:.2f}元 {transfer_note}, "
                 f"总费用: {fees['total_fee']:.2f}元")
    
    def get_total_trade_fees(self):
        """返回总交易费用"""
        total = self.total_commission + self.total_stamp_duty + self.total_transfer_fee
        return {
            'total_commission': self.total_commission,
            'total_stamp_duty': self.total_stamp_duty,
            'total_transfer_fee': self.total_transfer_fee,
            'total_fee': total
        }
    
    def generate_fee_report(self):
        """生成交易费用报告"""
        fees = self.get_total_trade_fees()
        log.info("==== 交易费用统计报告 ====")
        log.info(f"总佣金支出: {fees['total_commission']:.2f}元")
        log.info(f"总印花税支出: {fees['total_stamp_duty']:.2f}元")
        log.info(f"总过户费支出: {fees['total_transfer_fee']:.2f}元")
        log.info(f"交易费用总支出: {fees['total_fee']:.2f}元")
        
    def calculate_total_assets(self, context):
        """计算当前总资产（现金+持仓市值）"""
        cash = context.portfolio.cash
        positions_value = self.calculate_positions_value(context)
        return cash + positions_value
    
    def calculate_positions_value(self, context):
        """计算当前持仓市值，修复价格获取方式"""
        positions_value = 0.0
        for stock, position in context.portfolio.positions.items():
            if position.amount > 0:  # 只计算多头持仓
                normalized_stock = self.normalize_stock_code(stock)
                
                # 从self.latest_prices获取价格，添加错误处理
                current_price = self.latest_prices.get(normalized_stock)
                if current_price is None:
                    log.warning(f"获取{normalized_stock}价格失败，使用持仓均价")
                    current_price = position.avg_price  # 使用持仓均价作为 fallback
                    
                if current_price > 0:
                    positions_value += position.amount * current_price
        return positions_value         
        
    def normalize_stock_code(self, stock_code):
        """统一股票代码格式为.SS后缀（上海交易所）"""
        code_part = stock_code.split('.')[0]
        return f"{code_part}.SS"
    
    def fetch_prices(self, context):
        """统一价格获取函数，根据时间点决定获取待交易池或持仓股票价格"""
        current_time = context.current_dt.time()
        if (current_time.hour, current_time.minute) == (9, 45):
            self.fetch_trade_pool_prices(context)
        else:
            self.fetch_position_prices(context)
    
    def fetch_trade_pool_prices(self, context):
      """获取待交易股票池的价格"""
      if not self.security:
          log.info("待交易股票池为空，跳过价格获取")
          return
          
      try:
          # 标准化股票代码列表
          stock_list = [self.normalize_stock_code(stock) for stock in self.security]
          
          # 批量获取股票池中的最新价格
          price_data = get_history(
              count=1, frequency='1m', field=['open', 'close', 'high', 'low', 'volume','price'], 
              security_list=stock_list, include=True
          )
          
          self.price_index = {}
          valid_count = 0
          
          # 正确解析价格数据
          if 'code' in price_data.columns:  # 检查是否包含code列
              # 方法1: 使用pivot_table重塑数据
              try:
                  pivoted_data = price_data.pivot_table(
                      index=price_data.index.get_level_values(0),  # 使用时间索引
                      columns='code', 
                      values='price'
                  )
                  for stock in stock_list:
                      if stock in pivoted_data.columns:
                          latest_price = pivoted_data[stock].iloc[-1]
                          self.price_index[stock] = latest_price
                          valid_count += 1
                      else:
                          log.warning(f"股票 {stock} 不在返回数据中")
                          self.price_index[stock] = None
              except Exception as e:
                  log.error(f"重塑数据失败: {e}，尝试备选方法")
                  
          # 更新最新价格缓存
          self.latest_prices = self.price_index.copy()
          
      except Exception as e:
          log.error(f"批量获取股票池价格数据失败: {e}")    
    
    def fetch_position_prices(self, context):
        """获取持仓股票价格并生成持仓报告"""
        positions = context.portfolio.positions
        if not positions:
            log.info("当前无持仓，跳过价格获取")
            return
            
        try:
            position_stocks = [self.normalize_stock_code(stock) for stock in positions.keys()]
            his1 = get_history(1, '1m', 'price', security_list=position_stocks)
            self.price_index = {}
            for stock in position_stocks:
                try:
                    stock_price = his1.query(f'code in ["{stock}"]')['price'].iloc[-1]
                    self.price_index[stock] = stock_price
                except Exception as e:
                    log.error(f"获取持仓股票 {stock} 数据失败: {e}")
                    self.price_index[stock] = None
            self.latest_prices = self.price_index.copy()
        except Exception as e:
            log.error(f"获取持仓股票K线数据失败: {e}")
    
    #生成详细的持仓报告，包含盈亏情况统计
    def generate_position_report(self, context):
        """生成详细的持仓报告，包含盈亏情况统计"""
        log.info("输出持仓报告:")
        try:
            positions = context.portfolio.positions
            position_report = []
            cash = context.portfolio.cash              
            positions_value = 0   
            total_assets = context.portfolio.portfolio_value
                
            # 计算每只持仓的市值和盈亏
            for stock, position in positions.items():
                if position.amount > 0:
                    normalized_stock = self.normalize_stock_code(stock)
                    current_market_price = self.latest_prices.get(normalized_stock, position.avg_price)
                    purchase_price = position.avg_price
                    profit_ratio = (current_market_price - purchase_price) / purchase_price if purchase_price != 0 else 0
                        
                    market_value = position.amount * current_market_price
                    positions_value += market_value
                    
                    # 计算持仓比例
                    position_ratio = (market_value / total_assets) * 100 if total_assets > 0 else 0
                    
                    # 获取股票名称（正确的方式）
                    try:
                        # 使用平台提供的get_stock_name函数获取股票名称
                        name_dict = get_stock_name(stock)
                        if isinstance(name_dict, dict) and stock in name_dict:
                            stock_name = name_dict[stock]
                        else:
                            stock_name = normalized_stock
                        
                        # 对名称进行格式化处理
                        if len(stock_name) == 3:  # 三个字名称
                            stock_name = stock_name + " "  # 添加一个空格保持对齐
                        elif len(stock_name) > 4:  # 超过四个字符
                            stock_name = stock_name[:4] + ".."
                    except Exception as e:
                        log.error(f"获取{stock}名称失败: {e}")
                        stock_name = normalized_stock
                    
                    position_report.append({
                        'stock': normalized_stock,
                        'name': stock_name,
                        'amount': position.amount,
                        'cost_price': purchase_price,
                        'current_price': current_market_price,
                        'market_value': market_value,
                        'profit_ratio': profit_ratio,
                        'position_ratio': position_ratio
                    })
                
            self.current_total_assets = total_assets
                
            log.info(f"当前总资产: {total_assets:.2f}元，现金余额: {cash:.2f}元，持仓市值: {positions_value:.2f}元")
            if position_report:
                # 更新表头
                log.info("#" * 101)
                log.info(f"{'序号':>4}  {'股票代码':<8}{'名称':<5}  {'持仓数量':>8}  {'成本价':>6}  {'现价':>8}  {'持仓市值':>8}    {'盈亏比':>6}     {'持仓比%':>6}")
                log.info("=" * 101)
                
                # 按市值降序排列
                sorted_report = sorted(position_report, key=lambda x: x['market_value'], reverse=True)
                
                for i, item in enumerate(sorted_report, 1):
                    profit_ratio = item['profit_ratio']
                    profit_str = f"+{profit_ratio*100:.2f}%" if profit_ratio >= 0 else f"{profit_ratio*100:.2f}%"
                    
                    position_ratio_str = f"{item['position_ratio']:.2f}%"
                    
                    # 对齐名称列
                    name_display = item['name']
                    
                    log.info(
                        f"{i:<4}  {item['stock']:<8}  {name_display:<5}  "  # 使用调整后的名称
                        f"{item['amount']:>10}  "
                        f"{item['cost_price']:>10.2f}  {item['current_price']:>10.2f}  "
                        f"{item['market_value']:>12.2f}  {profit_str:>10}     {position_ratio_str:>8}"
                    )
                log.info("#" * 101)
                       
        except Exception as e:
            log.error(f"生成持仓报告出错: {e}")
            
    def get_platform_positions(self):
        """
        获取交易平台的当前持仓数据
        :return: 持仓字典 {stock_code: position_object}
        """
        try:
            # 平台提供get_positions()函数获取持仓
            return get_positions()
        except Exception as e:
            log.error(f"获取平台持仓数据失败: {e}")
            return {}            
            
    def sync_holdings(self, context):
        """同步持仓数据，完善初始化阶段检测与重试机制"""
        if self.initialization_phase:
            # 检查是否为交易日且已开盘（优化周末判断逻辑）
            is_trading_day = context.current_dt.weekday() < 5  # 0-4为交易日
            is_after_open = context.current_dt.time() >= datetime.time(9, 30)
        
            if is_trading_day and is_after_open:
                self.initialization_phase = False
            else:
                self.holdings_sync_retry += 1
                if self.holdings_sync_retry <= self.max_retry:
                    log.info(f"初始化阶段延迟持仓同步，重试 {self.holdings_sync_retry}/{self.max_retry}")
                    return
                else:
                    log.warning("持仓同步重试超限，强制更新持仓")
    
        try:
            # 从平台获取最新持仓数据（模拟接口，实际需根据平台调整）
            platform_positions = self.get_platform_positions()
        
            # 对比本地与平台持仓，更新差异
            local_positions = set(context.portfolio.positions.keys())
            platform_positions = set(platform_positions.keys())
        
            # 处理新增持仓
            new_positions = platform_positions - local_positions
        
            # 处理移除持仓
            removed_positions = local_positions - platform_positions
        
            # 强制更新持仓数量
            self.current_holdings = self.calculate_valid_holdings(context)
        
        except Exception as e:
            log.error(f"持仓数据同步失败: {e}")
            # 失败时使用本地计算结果作为 fallback
            self.current_holdings = self.calculate_valid_holdings(context)
            
    def post_market_confirmation(self, context):
        """盘后数据确认，包含持仓报告推送"""
        # 只生成一次持仓报告
        self.generate_position_report(context)
        
        # 发送企业微信持仓报告（内部包含持仓信息，但不会生成额外报告）
        if self.enable_position_report_push:
            self.send_position_report_wechat(context)
        
        # 记录策略绩效
        self.record_strategy_performance(context)
        
        # 生成费用报告
        self.generate_fee_report()
        
    def send_position_report_wechat(self, context):
        """发送持仓报告到企业微信"""
        positions = context.portfolio.positions
        if not positions:
            message = f"📊📊 鲸鱼喷水量化策略盘后持仓报告\n\n当前无持仓"
            self.send_wechat_message(message)
            return
        
        
        # 获取当前总资产和现金
        total_assets = context.portfolio.portfolio_value
        cash = context.portfolio.cash
        
        # 创建报告头部
        message = "📊 鲸鱼喷水量化盘后持仓报告\n\n"
        message += f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"当前总资产: {total_assets:.2f}元\n"
        message += f"现金余额: {cash:.2f}元\n\n"
        message += "持仓明细:\n"
        
        # 添加持仓明细
        for i, (stock, position) in enumerate(positions.items(), 1):
            normalized_stock = self.normalize_stock_code(stock)
            stock_name = self.get_stock_name(normalized_stock) or "未知名称"
            
            # 获取当前价格
            current_price = self.latest_prices.get(normalized_stock, position.avg_price)
            
            # 计算持仓市值和盈亏
            market_value = position.amount * current_price
            profit = (current_price - position.avg_price) * position.amount
            profit_ratio = (current_price - position.avg_price) / position.avg_price if position.avg_price else 0
            
            # 格式化盈亏显示
            profit_sign = "+" if profit >= 0 else ""
            profit_ratio_sign = "+" if profit_ratio >= 0 else ""
            
            # 添加到消息
            message += f"{i}. {normalized_stock} ({stock_name})\n"
            message += f"  持仓数量: {position.amount}股\n"
            message += f"  成本价格: {position.avg_price:.2f}元\n"
            message += f"  当前价格: {current_price:.2f}元\n"
            message += f"  持仓市值: {market_value:.2f}元\n"
            message += f"  持仓盈亏: {profit_sign}{profit:.2f}元 ({profit_ratio_sign}{profit_ratio:.2%})\n\n"
        
        # 添加总收益信息
        if self.initial_capital > 0:
            total_profit = total_assets - self.initial_capital
            total_profit_ratio = total_profit / self.initial_capital
            profit_sign = "+" if total_profit >= 0 else ""
            profit_ratio_sign = "+" if total_profit_ratio >= 0 else ""
            
            message += f"策略总收益: {profit_sign}{total_profit:.2f}元 ({profit_ratio_sign}{total_profit_ratio:.2%})"
        
        # 发送企业微信消息
        self.send_wechat_message(message)    
    
    def record_strategy_performance(self, context):
        today = context.current_dt.date()
        total_assets = context.portfolio.portfolio_value
        cash = context.portfolio.cash
        positions_value = self.calculate_positions_value(context)
        current_holdings = self.calculate_valid_holdings(context)
     
        # 计算累计收益（考虑初始资金）
        cumulative_return = (total_assets - self.initial_capital) / self.initial_capital * 100 if self.initial_capital != 0 else 0
    
        # 计算当日收益（考虑持仓盈亏）
        daily_profit = total_assets - (self.initial_capital + self.total_commission + self.total_stamp_duty + self.total_transfer_fee)
    
        # 记录绩效
        self.trade_history.append({
            'date': today,
            'total_assets': total_assets,
            'daily_profit': daily_profit,
            'cumulative_return': cumulative_return,
            'current_holdings': current_holdings
        })
    
        # 输出日志（动态时间）
        current_time = context.current_dt.strftime("%Y-%m-%d %H:%M:%S")
        log.info("==== 策略绩效收益报告 ====")
        log.info(f"当前总资产: {total_assets:.2f}元")
        log.info(f"累计总收益: {daily_profit:.2f}元")
        log.info(f"累计收益率: {cumulative_return:.2f}%")
        log.info(f"当前持仓数量: {current_holdings}只")
        
    #===============================================================    
    #=========================核心盘中处理 ==========================
    #===============================================================            
    def handle_data(self, context, data=None):
        """
        主数据处理函数
        """
        # 添加账号和有效期检查（放在最前面）
        if not self.account_valid or self.expired or self.not_yet_valid:
            # 只记录一次日志，避免频繁输出
            if not hasattr(self, 'invalid_logged'):
                # 根据具体原因生成错误信息
                if not self.account_valid:
                    reason = "账号无效"
                elif self.expired:
                    reason = "策略已过期"
                elif self.not_yet_valid:
                    reason = "策略尚未生效"
                else:
                    reason = "未知原因"
                    
                log.error(f"策略无法执行：{reason}（账号: {self.current_account}）")
                self.invalid_logged = True
            return
    
        # ================= 原有数据处理代码 =================
        current_dt = context.current_dt
        current_date = current_dt.date()
        current_time = current_dt.time()
        current_trade_tuple = (current_time.hour, current_time.minute)
        formatted_time = current_dt.strftime("%Y-%m-%d %H:%M:%S")
    
        # 新交易日重置状态标记
        if self.current_date != current_date:
            self.executed_data = {time: False for time in self.data_times}
            self.executed_trade = {
                self.time_points["上午盘-止盈止损执行"]: False,
                self.time_points["下午盘-止盈止损执行"]: False,
                self.time_points["盘后数据确认"]: False
            }
            self.current_date = current_date
            self.latest_prices = {}
            self.price_index = {}
            self.position_profit = {}
            self.position_report = []
            self.today_traded = set()
            self.market_bearish = False
            self.current_holdings = self.calculate_valid_holdings(context)
        
            # 重置无效状态日志标志
            if hasattr(self, 'invalid_logged'):
                del self.invalid_logged
    
        # 同步持仓（仅首次处理时）
        if self.current_date != context.current_dt.date():
            self.sync_holdings(context)
    
        # 盘后数据确认
        if current_trade_tuple == self.time_points["盘后数据确认"]:
            if not self.executed_trade.get(current_trade_tuple, False):
                log.info(f"==== 盘后数据确认开始 ====")
                self.post_market_confirmation(context)  # 包含持仓报告推送
                self.executed_trade[current_trade_tuple] = True
                log.info(f"==== 盘后数据确认完成 ====")
            return  # 盘后处理后直接返回，不执行后续交易逻辑
    
        # 交易时间处理
        if current_trade_tuple in self.trade_times:
            if not self.executed_trade.get(current_trade_tuple, False):
                # 执行交易逻辑
                session_label = "上午盘" if current_trade_tuple == self.time_points["上午盘-止盈止损执行"] else "下午盘"
                self.execute_trade(context, current_trade_tuple, data)
                self.executed_trade[current_trade_tuple] = True
       
    def execute_buy_strategy(self, context, data=None, from_analysis=False):
        """执行买入策略"""
        if not self.account_valid or self.expired:
            return
        # 大盘风险检查（熊市状态禁止买入）
        if self.market_bearish:
            log.warning(f"大盘双指数检测处于熊市状态，禁止新买入操作！！！")
            return
                    
        if not self.security:
            log.info("当前待交易股票池为空，无法执行买入操作")
            return
                    
        log.info("大盘未禁，开始执行买入操作")
        
        # 使用更新后的持仓计数
        current_holdings = self.calculate_valid_holdings(context)
        log.info(f"当前持仓数量: {current_holdings}, 最大持仓限制: {self.max_holdings}")
         
        if self.security:
            # 获取所有待交易股票（40只）
            selected_stocks = self.security.copy()
        
            # 按优化排序（如果优化信息存在）
            if self.optimized_stocks:
                stock_to_info = {info['stock']: info for info in self.optimized_stocks}
                selected_stocks.sort(key=lambda stock: (
                    stock_to_info.get(stock, {}).get('priority', 4),
                    stock_to_info.get(stock, {}).get('risk_score', 1.0)
                ))
        else:
            selected_stocks = []
    
        # 确定需要买入的新股票（不在持仓中的优选股）
        positions = context.portfolio.positions
        stocks_to_buy = [stock for stock in selected_stocks if stock not in positions]
        
        # 仅执行持仓不足时的买入操作
        if current_holdings < self.max_holdings:
            available_slots = self.max_holdings - current_holdings
            log.info(f"剩余可买入仓位: {available_slots}只")
            
            bought_count = 0  # 已买入股票数量
            for stock in stocks_to_buy:
                # 实时检查持仓状态
                current_holdings = self.calculate_valid_holdings(context)
                # 达到持仓限制时停止检查
                if current_holdings >= self.max_holdings:
                    log.info("持仓已达上限，终止买入流程")
                    break
                    
                log.info(f"检查待买入股票: {stock}")
                if self.execute_single_buy(context, stock):
                    bought_count += 1
                    log.info(f"成功买入第{bought_count}只股票，剩余仓位: {available_slots - bought_count}")
            
            if bought_count > 0:
                log.info(f"本次买入操作共买入{bought_count}只股票")
            else:
                log.info("待买入股票均未通过二次风控检查")
        
        else:
            log.info("持仓已满，不执行买入操作")
    
    def execute_single_buy(self, context, stock):
        """执行单只股票的买入操作，包含完整风控检查流程，返回是否买入成功"""
        # 大盘风险二次检查
        if self.market_bearish:
            log.warning(f"大盘处于下跌状态，禁止买入{stock}")
            return False
        # 统一股票代码格式并检查科创板限制
        original_stock = self.normalize_stock_code(stock)
        if original_stock.startswith('688'):
            log.warning(f"禁止买入科创板股票: {original_stock}")
            return False
        # 检查当日是否已交易该股票
        if original_stock in self.today_traded:
            log.info(f"今日已交易过{original_stock}，跳过买入")
            return False
        # 获取最新价格，增加重试机制
        current_price = self.latest_prices.get(original_stock)
        retry_count = 0
        while current_price is None or current_price <= 0 and retry_count < self.price_retry_count:
            self.fetch_trade_pool_prices(context)
            current_price = self.latest_prices.get(original_stock)
            retry_count += 1
            
        if current_price is None or current_price <= 0:
            log.error(f"多次获取{original_stock}最新价格失败，放弃买入")
            return False
        # 买入前二次风控检查
        if not self.second_risk_check(original_stock, current_price, context):
            log.warning(f"二次风控检查不通过，拒绝买入{original_stock}")
            return False
        # 检查当前持仓数量是否已达上限
        current_holdings = len(context.portfolio.positions)
        if current_holdings >= self.max_holdings:
            log.info(f"已达最大持仓限制({self.max_holdings}只)，无法买入{original_stock}")
            return False
        # 计算当前总资产（现金+持仓市值）    
        total_assets = context.portfolio.portfolio_value
        
        # 计算单只股票目标持仓金额（使用全局比例）
        target_value = total_assets * self.target_position_percentage
        
        # 检查可用现金是否足够
        available_cash = context.portfolio.cash
        if available_cash < target_value:
            log.info(f"可用现金不足: {available_cash:.2f} < {target_value:.2f}，无法买入{stock}")
            return False
        # 执行买入订单（使用order_value，不传入limit_price）
        try:
            order_result = order_value(stock, target_value)
            
            
            if order_result:
                # 计算交易成本
                trade_value = min(target_value, available_cash)  # 实际交易金额
                fees = self.calculate_trade_fees(trade_value, is_sell=False)
                self.record_trade_fees(fees, stock, is_sell=False)
        
                log.info(f"买入成功，股票：{stock}，金额：{trade_value:.2f}元")
                self.trade_cooldown[stock] = self.BUY_COOLDOWN_DAYS
                self.current_holdings = self.calculate_valid_holdings(context)
                self.sync_holdings(context)
            
                # 获取股票名称
                stock_name = self.get_stock_name(stock) or "未知名称"
                # 发送企业微信通知 - 确保正确的缩进
                buy_message = (
                    f"📈 买入通知\n"
                    f"股票: {stock} ({stock_name})\n"
                    f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"数量: {int(trade_value / current_price)}股\n"
                    f"价格: {current_price:.2f}元\n"
                    f"金额: {trade_value:.2f}元"
                )
                self.send_wechat_message(buy_message)
            
                return True
            return False
        except Exception as e:
            log.error(f"买入订单执行异常: {e}")
            return False       
            
    def second_risk_check(self, stock_code, current_price, context):
        """买入前二次风险检查，增强风控能力"""
        try:
            normalized_code = self.normalize_stock_code(stock_code)
            
            # 获取20日历史数据用于技术分析
            hist_data = get_history(
                count=20,
                frequency='1d',
                field=['open', 'close', 'high', 'low', 'volume'],
                security_list=[normalized_code],
                fq='pre',
                include=False  # 排除当日数据
            )
            
            if len(hist_data) < 10:
                return True  # 数据不足时默认通过检查
            
            # 计算短期均线（5、10、20日）
            ma_values = self.calculate_moving_averages(hist_data, [5, 10, 20])
            ma5, ma10, ma20 = ma_values
            
            # 检查短期均线死叉（5日均线下穿10日均线）
            if ma5 is not None and ma10 is not None:
                ma5_prev = hist_data['close'].rolling(window=5).mean().iloc[-10]
                ma10_prev = hist_data['close'].rolling(window=10).mean().iloc[-10]
                if ma5_prev > ma10_prev and ma5 < ma10:
                    log.warning(f"{normalized_code}出现5日与10日均线死叉，拒绝买入")
                    return False
            
            # 检查短期下跌趋势（5日<10日<20日）
            if ma5 is not None and ma10 is not None and ma20 is not None:
                if ma5 < ma10 and ma10 < ma20:
                    log.warning(f"{normalized_code}处于短期下跌趋势，拒绝买入")
                    return False
            
            # 检查最近5日放量下跌
            avg_volume_20 = hist_data['volume'].rolling(window=20).mean().iloc[-1]
            for i in range(1, 6):
                k = hist_data.iloc[-i]
                if k['close'] < k['open'] and k['volume'] > avg_volume_20 * 1.5:
                    log.warning(f"{normalized_code}近期放量下跌，拒绝买入")
                    return False
            
            # 检查看跌形态（仅在启用开关时执行）
            if self.enable_bearish_pattern:
                bearish_patterns = self.analyze_bearish_patterns(hist_data)
                if bearish_patterns:
                    log.warning(f"{normalized_code}存在看跌形态: {', '.join(bearish_patterns)}，拒绝买入")
                    return False
            
            # 检查KDJ指标超买
            kdj_j = self.calculate_kdj_j(hist_data)
            if kdj_j is not None and kdj_j > self.KDJ_J_OVERBOUGHT:
                log.warning(f"{normalized_code} KDJ J值超买({kdj_j:.2f} > {self.KDJ_J_OVERBOUGHT})，拒绝买入")
                return False
            
            # 检查RSI指标超买
            rsi = self.calculate_rsi(hist_data)
            if rsi is not None and rsi > self.RSI_OVERBOUGHT_LEVEL:
                log.warning(f"{normalized_code} RSI超买({rsi:.2f} > {self.RSI_OVERBOUGHT_LEVEL})，拒绝买入")
                return False
            
            return True
        except Exception as e:
            log.error(f"二次风险检查出错: {e}")
            return True  # 出错时默认通过检查
    
    def execute_trade(self, context, trade_time, data=None):
        """执行交易操作（止盈止损），使用最近获取的持仓价格"""
        if not self.account_valid or self.expired:
            return
        positions = context.portfolio.positions
        if not positions:
            return
        sold_count = 0  # 卖出数量计数器
        # 确定当前操作时段（上午盘/下午盘）
        session_label = "上午盘" if trade_time == self.time_points["上午盘-止盈止损执行"] else "下午盘"
        log.info(f"===== {session_label}止盈止损流程开始 ====")
        
        # 【性能优化】批量获取所有持仓股票的历史数据
        hist_data_cache = {}
        position_stocks = [self.normalize_stock_code(s) for s in positions.keys() if not self.normalize_stock_code(s).startswith('688')]
        
        if position_stocks:
            try:
                log.info(f"批量获取{len(position_stocks)}只持仓股票的历史数据...")
                # 批量获取历史数据
                hist_data_batch = get_history(
                    count=20,
                    frequency='1d',
                    field=['open', 'close', 'high', 'low', 'volume'],
                    security_list=position_stocks,
                    fq='pre',
                    include=False
                )
                
                # 将批量数据按股票代码分组存储
                if hist_data_batch is not None and not hist_data_batch.empty:
                    for stock in position_stocks:
                        stock_data = hist_data_batch[hist_data_batch['code'] == stock]
                        if len(stock_data) >= 14:  # 确保有足够数据用于技术分析
                            hist_data_cache[stock] = stock_data
                    log.info(f"批量获取历史数据完成，有效股票数: {len(hist_data_cache)}")
                else:
                    log.warning("批量获取历史数据返回空，将在需要时逐个获取")
            except Exception as e:
                log.error(f"批量获取历史数据失败: {e}，将在需要时逐个获取")
        
        # 遍历当前持仓
        for stock in list(context.portfolio.positions.keys()):
            position = context.portfolio.positions[stock]
            original_stock = self.normalize_stock_code(stock)
        
            # 跳过科创板股票
            if original_stock.startswith('688'):
                continue
            
            # T+1规则：跳过当天买入的股票（A股当天买入不能卖出）
            if original_stock in self.today_traded:
                log.info(f"跳过当天买入的股票: {original_stock}（T+1限制）")
                continue
            
            # 获取最新价格，增加重试机制
            current_price = self.latest_prices.get(original_stock)
            retry_count = 0
            while current_price is None or current_price <= 0 and retry_count < self.price_retry_count:
                self.fetch_position_prices(context)
                current_price = self.latest_prices.get(original_stock)
                retry_count += 1
                
            if current_price is None or current_price <= 0:
                log.error(f"多次获取{original_stock}最新价格失败，放弃交易")
                continue
        
            if current_price is None or current_price <= 0:
                log.error(f"获取{original_stock}最新价格失败或无效价格，无法进行交易判断")
                continue
                
            # 获取持仓信息并计算盈亏
            position = positions.get(stock)
            if position and position.amount > 0:
                purchase_price = position.avg_price
                current_profit = (current_price - purchase_price) / purchase_price
                
                # 记录盈亏情况
                self.position_profit[original_stock] = {
                    'market_price': current_price,
                    'cost_price': purchase_price,
                    'profit_ratio': current_profit
                }
                
                # 获取历史数据用于技术分析（优先使用缓存）
                hist_data = None
                if original_stock in hist_data_cache:
                    hist_data = hist_data_cache[original_stock]
                else:
                    try:
                        hist_data = get_history(
                            count=20,
                            frequency='1d',
                            field=['open', 'close', 'high', 'low', 'volume'],
                            security_list=[original_stock],
                            fq='pre',
                            include=False
                        )
                    except Exception as e:
                        log.error(f"获取{original_stock}历史数据失败: {e}")
                
                # 止盈止损条件判断
                shares_to_sell = 0
                
                # 1. 基于固定阈值的止盈止损（添加开关判断）
                if self.enable_fixed_take_profit and current_profit >= self.profit_take_threshold:
                    shares_to_sell = position.amount
                    log.info(f"触发固定止盈，股票：{original_stock}，盈亏：{current_profit:.2%}")
                    
                if self.enable_fixed_stop_loss and current_profit <= self.stop_loss_threshold:
                    shares_to_sell = position.amount
                    log.info(f"触发固定止损，股票：{original_stock}，盈亏：{current_profit:.2%}")
                
                # 2. 基于技术指标的止盈（添加开关判断）
                if hist_data is not None and len(hist_data) >= 14:
                    # RSI止盈
                    if self.enable_rsi_take_profit:
                        rsi = self.calculate_rsi(hist_data)
                        if rsi is not None and rsi > 90:  # RSI高位止盈，可能超买
                            shares_to_sell = position.amount
                            log.info(f"触发RSI止盈，股票：{original_stock}，RSI：{rsi:.2f}>90，盈亏：{current_profit:.2%}")
                    
                    # KDJ止盈
                    if self.enable_kdj_take_profit:
                        kdj_j = self.calculate_kdj_j(hist_data)
                        if kdj_j is not None and kdj_j > 90:  # KDJ J值高位，可能超买
                            shares_to_sell = position.amount
                            log.info(f"触发KDJ止盈，股票：{original_stock}，KDJ J：{kdj_j:.2f}>90，盈亏：{current_profit:.2%}")
                
                # 3. 基于K线形态的止损（添加总开关和分项开关）
                if (self.enable_bearish_pattern and 
                    self.enable_bearish_pattern_stop and 
                    hist_data is not None and 
                    len(hist_data) >= 3):
                        
                    if trade_time == self.time_points["上午盘-止盈止损执行"]:
                        # 上午盘使用前日数据
                        hist_data = hist_data.iloc[:-1]  # 移除最后一行（当日数据）
                    bearish_patterns = self.analyze_bearish_patterns(hist_data)
                    
                    # 检查各独立形态开关
                    triggered_patterns = []
                    for pattern in bearish_patterns:
                        if pattern == "看跌吞没" and self.enable_bear_engulfing_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "乌云盖顶" and self.enable_dark_cloud_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "黄昏之星" and self.enable_evening_star_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "三只乌鸦" and self.enable_three_crows_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "射击之星" and self.enable_shooting_star_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "吊颈线" and self.enable_hanging_man_stop:
                            triggered_patterns.append(pattern)
                        elif pattern == "平顶" and self.enable_tweezers_top_stop:
                            triggered_patterns.append(pattern)
                    
                    if triggered_patterns:
                        shares_to_sell = position.amount
                        log.info(f"触发形态止损，股票：{original_stock}，看跌形态：{', '.join(triggered_patterns)}，盈亏：{current_profit:.2%}")
                
                # 执行卖出操作
                if shares_to_sell > 0:
                    # 执行卖出订单
                    order_result = order(stock, -shares_to_sell)
                    if order_result:
                        # 计算交易明细和实际盈利
                        trade_value = shares_to_sell * current_price
                        # 计算交易费用
                        fees = self.calculate_trade_fees(trade_value, is_sell=True)
                        # 记录交易费用
                        self.record_trade_fees(fees, original_stock, is_sell=True)
                    
                        commission = fees['commission']
                        slippage = trade_value * self.SLIPPAGE
                        stamp_duty = fees['stamp_duty']
                        transfer_fee = fees['transfer_fee']
                        realized_profit = trade_value - (shares_to_sell * purchase_price) - commission - slippage - stamp_duty - transfer_fee
            
                        # 更新现金余额
                        context.portfolio.cash += (trade_value - commission - slippage)
            
                        # 更新总资产
                        self.current_total_assets = context.portfolio.portfolio_value
            
                        # 记录卖出日志
                        log.info(f"卖出成功，股票：{original_stock}，数量：{shares_to_sell}股，执行单价：{current_price:.2f}，成本价：{purchase_price:.2f}，盈亏额：{realized_profit:.2f}元")
                        log.info(f"当前总资产：{self.current_total_assets:.2f}元")
            
                        # 更新交易冷却期
                        self.trade_cooldown[stock] = self.SELL_COOLDOWN_DAYS
                        self.current_holdings = self.calculate_valid_holdings(context)
                        
                        # 记录已实现盈亏
                        self.realized_profits[stock] = realized_profit
                        sold_count += 1
                    
                        # 标记该股票当日已交易
                        self.today_traded.add(original_stock)
                        
                        # 获取股票名称
                        stock_name = self.get_stock_name(original_stock) or "未知名称"
                        # 发送企业微信通知
                        sell_message = (
                            f"📉 卖出通知\n"
                            f"股票: {original_stock} ({stock_name})\n"
                            f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"数量: {shares_to_sell}股\n"
                            f"价格: {current_price:.2f}元\n"
                            f"盈亏: {realized_profit:.2f}元\n"
                            f"原因: {', '.join(triggered_patterns) if triggered_patterns else '止盈止损'}"
                        )
                        self.send_wechat_message(sell_message) 
        # 在止盈止损操作后添加资产状况日志
        #self.generate_position_report(context)   #在止盈止损操作中不再生成持仓报告
        log.info(f"===== {session_label}止盈止损流程结束，共交易{sold_count}只股票 ====")
        # 新增：止盈止损后重置成分股分析状态
        self.analysis_completed = False  # 关键修改
        return sold_count > 0  # 返回是否有卖出操作
    
    #===============================================================    
    # ======================熊市清仓通知系统  =======================
    #===============================================================    
    def check_market_risk(self, context):
        """检查大盘风险（指数触发逻辑），添加完整的熊市清仓通知系统"""
        if not self.account_valid or self.expired:
            return
        try:
            index_data = {}
        
            # 获取各指数数据
            for index_info in self.market_indices:
                code = index_info['code']
            
                # 获取日线和分钟线数据
                daily_data = get_history(
                    count=2,
                    frequency='1d',
                    field=['close'],
                    security_list=code,
                    fq='pre',
                    include=False
                )
                minute_data = get_history(
                    count=1,
                    frequency='1m',
                    field=['close'],
                    security_list=code,
                    include=self.INCLUDE_CURRENT_DATA
                )
            
                if len(daily_data) < 2:
                    index_data[code] = {'change': 0.0, 'valid': False}
                    continue
                
                # 计算涨跌幅
                if not minute_data.empty:
                    latest_close = minute_data['close'].iloc[-1]
                    prev_close = daily_data['close'].iloc[-1]
                    change = (latest_close - prev_close) / prev_close * 100
                else:
                    change = 0.0
                
                index_data[code] = {'change': change, 'valid': True}
        
            # 记录各指数状态
            log_msgs = []
            all_valid = all(index_info['valid'] for index_info in index_data.values())
        
            for index_info in self.market_indices:
                code = index_info['code']
                data = index_data.get(code, {})
                status = "下跌超预警阈值" if data.get('change', 0) <= index_info['threshold'] else "正常"
                log_msgs.append(f"{index_info['name']}: 涨跌幅{data.get('change', 0):.2f}%，{status}")
        
            log.info(", ".join(log_msgs))
        
            # 判断是否触发清仓（核心逻辑：双指数合值触发）
            if all_valid:
                # 计算双指数涨跌幅合值（直接相加）
                index_changes = [data['change'] for data in index_data.values()]
                combined_change = sum(index_changes)
        
                # 判断合值是否超过阈值（注意：负数合值需小于阈值）
                is_bearish = combined_change <= self.combined_threshold
            
                if is_bearish:
                    log.warning(f"[熊市触发] 双指数合值({combined_change:.2f}%)超过阈值({self.combined_threshold}%)，开始清仓！")
                    self.market_bearish = True
                    self.morning_bearish = True
                    self.security = []  # 清空股票池
                
                    # ================= 添加熊市清仓通知系统 =================
                    # 1. 发送熊市开始通知
                    if self.enable_wechat_notify:
                        start_msg = (f"🚨🚨🚨 熊市清仓卖出开始\n"
                                     f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                     f"双指数合值: {combined_change:.2f}%\n"
                                     f"触发阈值: {self.combined_threshold}%\n"
                                     f"当前持仓: {len(context.portfolio.positions)}只股票")
                        self.send_wechat_message(start_msg)
                
                    # 主动获取持仓股票价格
                    positions = context.portfolio.positions
                    gear_price_cache = {}
                    for stock in positions.keys():
                        normalized_stock = self.normalize_stock_code(stock)
                        try:
                            hist_data = get_history(
                                count=1, frequency='1m', field=['price'],
                                security_list=[normalized_stock], include=True
                            )
                            if not hist_data.empty:
                                latest_price = hist_data['price'].iloc[-1]
                                gear_price_cache[normalized_stock] = latest_price
                        except Exception as e:
                            log.error(f"获取{normalized_stock}价格失败: {e}")
                
                    # 记录清仓结果
                    sold_count = 0
                    failed_count = 0
                    total_profit = 0.0
                
                    for stock in list(positions.keys()):
                        position = positions[stock]
                        normalized_stock = self.normalize_stock_code(stock)
                        current_price = gear_price_cache.get(normalized_stock, position.avg_price)
                        limit_price = round(current_price * 0.995, 2) if current_price else None
                        profit = (current_price - position.avg_price) * position.amount
                    
                        try:
                            # 执行卖出订单
                            order_result = order(stock, -position.amount, limit_price)
                        
                            # 获取股票名称
                            stock_name = self.get_stock_name(normalized_stock) or "未知股票"
                        
                            if order_result:
                                log.info(
                                    f"【熊市清仓卖出】股票：{stock}，"
                                    f"数量：{position.amount}股，"
                                    f"执行单价：{current_price:.2f}，"
                                    f"保护限价：{limit_price if limit_price else '无'}, "
                                    f"盈亏额：{profit:.2f}元"
                                )
                                sold_count += 1
                                total_profit += profit
                            
                                # 2. 发送单只股票成功通知
                                if self.enable_wechat_notify:
                                    success_msg = (f"📉 卖出成功\n"
                                                   f"股票: {normalized_stock} ({stock_name})\n"
                                                   f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                                   f"数量: {position.amount}股\n"
                                                   f"价格: {current_price:.2f}元\n"
                                                   f"盈亏: {profit:.2f}元\n"
                                                   f"原因: 熊市清仓")
                                    self.send_wechat_message(success_msg)
                            else:
                                log.error(f"熊市清仓卖出失败: {normalized_stock}")
                                failed_count += 1
                            
                                # 3. 发送单只股票失败通知
                                if self.enable_wechat_notify:
                                    fail_msg = (f"❌ 熊市清仓卖出失败\n"
                                                f"股票: {normalized_stock} ({stock_name})\n"
                                                f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                                f"数量: {position.amount}股\n"
                                                f"原因: 委托未成功")
                                    self.send_wechat_message(fail_msg)
                        
                            self.today_traded.add(normalized_stock)
                        except Exception as e:
                            log.error(f"熊市清仓卖出异常: {e}")
                            failed_count += 1
                        
                            # 4. 发送异常通知
                            if self.enable_wechat_notify:
                                error_msg = (f"🚨 熊市清仓卖出异常\n"
                                             f"股票: {normalized_stock}\n"
                                             f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                             f"错误: {str(e)[:100]}")
                                self.send_wechat_message(error_msg)
                
                    # 5. 发送熊市清仓总结通知
                    if self.enable_wechat_notify:
                        summary_msg = (f"✅ 熊市清仓卖出完成\n"
                                       f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                       f"成功清仓股票: {sold_count}只\n"
                                       f"清仓失败股票: {failed_count}只\n"
                                       f"总盈亏: {total_profit:.2f}元\n"
                                       f"现金余额: {context.portfolio.cash:.2f}元")
                        self.send_wechat_message(summary_msg)
                    # ================= 通知系统结束 =================
    
        except Exception as e:
            log.error(f"指数风险检查出错: {e}")
        
            # 6. 添加错误通知
            if self.enable_wechat_notify:
                error_msg = (f"🚨 大盘风险检查失败\n"
                             f"时间: {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                             f"错误: {str(e)[:100]}")
                self.send_wechat_message(error_msg)
                
    #===============================================================
    #========================生成优选股池 ===========================   
    #===============================================================
    def analyze_index_stocks(self, context):
        """分析指数成分股，生成优选股池（09:40执行），分析完成后触发买入"""
        if not self.account_valid or self.expired:
            return
        log.info("开始分析指数成分股")
        try:
            # 重置市盈(TTM)亏损股票计数器
            self.skip_pe_ttm_count = 0
            
            # 获取指数成分股列表
            stock_list = get_index_stocks(self.index)
            total_stocks = len(stock_list)
            log.info(f"获取成分股数量: {total_stocks}")
            
            # 过滤掉科创板和冷却期股票
            filtered_stocks = [s for s in stock_list if not s.startswith('688') and s not in self.trade_cooldown]
            log.info(f"过滤后股票数量: {len(filtered_stocks)}")
            
            # 【性能优化1】批量获取所有股票的历史数据
            log.info(f"开始批量获取{len(filtered_stocks)}只股票的历史数据...")
            all_hist_data = {}
            try:
                # 批量获取历史数据（一次性获取所有股票）
                hist_data_batch = get_history(
                    count=self.lookback_days,
                    frequency='1d',
                    field=['open', 'close', 'high', 'low', 'volume'],
                    security_list=filtered_stocks,
                    fq='pre',
                    include=self.INCLUDE_CURRENT_DATA
                )
                
                # 将批量数据按股票代码分组存储
                if hist_data_batch is not None and not hist_data_batch.empty:
                    for stock in filtered_stocks:
                        stock_data = hist_data_batch[hist_data_batch['code'] == stock]
                        if len(stock_data) >= 20:  # 确保有足够数据
                            all_hist_data[stock] = stock_data
                    log.info(f"批量获取历史数据完成，有效股票数: {len(all_hist_data)}")
                else:
                    log.warning("批量获取历史数据返回空，将回退到逐个获取")
            except Exception as e:
                log.error(f"批量获取历史数据失败: {e}，将回退到逐个获取")
            
            # 如果批量获取失败，回退到逐个获取
            if not all_hist_data:
                log.info("开始逐个获取历史数据...")
                for stock in filtered_stocks:
                    try:
                        hist_data = get_history(
                            count=self.lookback_days,
                            frequency='1d',
                            field=['open', 'close', 'high', 'low', 'volume'],
                            security_list=[stock],
                            fq='pre',
                            include=self.INCLUDE_CURRENT_DATA
                        )
                        if len(hist_data) >= 20:
                            all_hist_data[stock] = hist_data
                    except Exception as e:
                        log.error(f"获取{stock}历史数据失败: {e}")
            
            # 【性能优化2】批量获取所有股票的PE数据
            pe_data_cache = {}
            stocks_with_data = list(all_hist_data.keys())
            if stocks_with_data:
                try:
                    log.info(f"批量获取{len(stocks_with_data)}只股票的市盈率数据...")
                    normalized_stocks = [self.normalize_stock_code(s) for s in stocks_with_data]
                    pe_df = get_fundamentals(
                        security=normalized_stocks,
                        table='valuation',
                        fields=['pe_ttm'],
                        date=context.previous_date
                    )
                    
                    # 处理返回数据（兼容不同格式）
                    if pe_df is not None and not pe_df.empty:
                        pe_df = pe_df.reset_index()
                        
                        # 查找股票代码列名
                        code_col = None
                        for col_name in ['secu_code', 'code', 'index', 'level_0']:
                            if col_name in pe_df.columns:
                                code_col = col_name
                                break
                        
                        if code_col and 'pe_ttm' in pe_df.columns:
                            for _, row in pe_df.iterrows():
                                pe_data_cache[row[code_col]] = row['pe_ttm']
                            log.info(f"批量获取市盈率数据完成，有效数据{len(pe_data_cache)}条")
                        else:
                            log.warning(f"PE数据格式异常，列名: {pe_df.columns.tolist()}，将回退到逐个获取")
                    else:
                        log.warning("批量获取PE数据返回空，将回退到逐个获取")
                except Exception as e:
                    log.error(f"批量获取市盈率数据失败: {e}，将回退到逐个获取")
            
            candidate_stocks = []  # 候选股票池
            
            # 遍历成分股进行筛选分析
            for i, stock in enumerate(stocks_with_data):
                hist_data = all_hist_data[stock]
                
                # 进度显示
                if (i + 1) % 100 == 0 or (i + 1) == len(stocks_with_data):
                    log.info(f"分析进度: {i+1}/{len(stocks_with_data)}，跳过市盈(TTM)亏损股票{self.skip_pe_ttm_count}只")
                
                # 基础筛选：价格范围检查
                current_price = hist_data['close'].iloc[-1]
                if not (self.price_min <= current_price <= self.price_max):
                    continue
                    
                # 获取市盈(TTM)数据并进行高PE筛选
                normalized_stock = self.normalize_stock_code(stock)
                
                # 优先从缓存获取PE数据
                if normalized_stock in pe_data_cache:
                    pe_ttm = pe_data_cache[normalized_stock]
                else:
                    # 缓存中没有，逐个获取
                    try:
                        pe_data = get_fundamentals(
                            security=normalized_stock,
                            table='valuation',
                            fields=['pe_ttm'],
                            date=context.previous_date
                        )
                        
                        if not pe_data.empty and 'pe_ttm' in pe_data.columns:
                            pe_ttm = pe_data['pe_ttm'].values[0]
                        else:
                            log.warning(f"无法获取{normalized_stock}的市盈(TTM)数据，跳过")
                            continue
                    except Exception as e:
                        log.error(f"获取{stock}市盈(TTM)数据失败: {e}")
                        continue
                
                # 筛选掉市盈(TTM)亏损、为0或超过阈值的股票
                if pe_ttm <= 0:
                    self.skip_pe_ttm_count += 1
                    continue
                elif pe_ttm > self.max_pe_ttm:
                    continue
                    
                # 计算多周期均线
                ma_values = self.calculate_moving_averages(hist_data, self.ma_periods)
                
                # 筛选：强势K线形态检查
                if not self.is_strong_candlestick(hist_data.iloc[-1], hist_data.iloc[-2]):
                    continue
                    
                # 筛选：短期涨幅不超过阈值
                short_growth = self.short_term_growth_rate(hist_data)
                if short_growth > self.SHORT_TERM_GROWTH_THRESHOLD:
                    continue
                    
                # 筛选：无异常成交量
                if self.is_volume_abnormal(hist_data, self.VOLUME_ABNORMAL_RATIO):
                    continue
                    
                # 筛选：均线多头排列
                ma_status = self.analyze_ma_status(ma_values[0], ma_values[1], ma_values[2], ma_values[3])
                if not ma_status.startswith("多头排列"):
                    continue
                    
                # KDJ指标筛选（低位金叉或上行趋势）
                kdj_j = self.calculate_kdj_j(hist_data)
                if kdj_j is not None and (kdj_j < self.KDJ_J_OVERSOLD or kdj_j < 50):
                    continue
                    
                # RSI指标筛选（不处于超买区域）
                rsi = self.calculate_rsi(hist_data)
                if rsi is not None and rsi > self.RSI_OVERBOUGHT_LEVEL:
                    continue
                    
                # 筛选：无看跌形态（在优选股池阶段排除）
                if self.enable_bearish_pattern:
                    bearish_patterns = self.analyze_bearish_patterns(hist_data)
                    if bearish_patterns:
                        continue
                        
                # 添加到候选池
                candidate_stocks.append({
                            'stock': stock,
                            'price': current_price,
                            'ma_values': ma_values,
                            'hist_data': hist_data,
                            'kdj_j': kdj_j,
                            'rsi': rsi,
                            'pe_ttm': pe_ttm  # 记录市盈TTM数据
                        })
            
            log.info(f"候选股数量: {len(candidate_stocks)}")

            
            # 风险评估，筛选低风险股票
            self.optimized_stocks = []
            for stock_info in candidate_stocks:
                _, risk_score = self.calculate_risk_metrics(
                    stock_info['hist_data'], 
                    stock_info['ma_values'], 
                    stock_info['stock']
                )
                if risk_score < self.max_risk_score:
                    stock_info['risk_score'] = risk_score
                    self.optimized_stocks.append(stock_info)
            
            # 定义看涨形态优先级
            HIGH_PRIORITY_PATTERNS = ["出水芙蓉", "双底形态", "看涨吞没", "早晨之星", "红三兵", "启明之星", "低位十字星","看涨十字星"]
            MEDIUM_PRIORITY_PATTERNS = ["旭日东升", "好友反攻", "锤头线", "倒锤头线"]
            LOW_PRIORITY_PATTERNS = ["上升三法", "平底", "下探回升", "孕线"]
    
            # 对优选股进行看涨形态分析并设置优先级
            for stock_info in self.optimized_stocks:
                bullish_patterns = []
                if self.enable_bullish_pattern:
                    bullish_patterns = self.analyze_bullish_patterns(stock_info['hist_data'])
                stock_info['bullish_patterns'] = bullish_patterns
        
                # 设置形态优先级（1=最高，4=最低）
                stock_info['priority'] = 4  # 默认最低
                if any(pattern in HIGH_PRIORITY_PATTERNS for pattern in bullish_patterns):
                    stock_info['priority'] = 1
                elif any(pattern in MEDIUM_PRIORITY_PATTERNS for pattern in bullish_patterns):
                    stock_info['priority'] = 2
                elif any(pattern in LOW_PRIORITY_PATTERNS for pattern in bullish_patterns):
                    stock_info['priority'] = 3
    
            # 显示优选股池前40股，包含市盈TTM信息
            if self.optimized_stocks:
                # 按优先级和风险评分排序
                self.optimized_stocks.sort(key=lambda x: (x['priority'], x['risk_score']))
        
                # 按优先级分组显示
                priority_groups = {
                    1: "强反转形态（高优先级）",
                    2: "次强反转/突破形态（中优先级）",
                    3: "持续形态与支撑确认（中低优先级）",
                    4: "其他形态"
                }
        
                log.info("优选股池（前40股）:")
                current_priority = None
        
                for i, stock_info in enumerate(self.optimized_stocks[:40]):
                    patterns = ", ".join(stock_info['bullish_patterns']) if stock_info['bullish_patterns'] else "无"
                    kdj_info = f"KDJ_J: {stock_info['kdj_j']:.2f}" if stock_info['kdj_j'] is not None else ""
                    rsi_info = f"RSI: {stock_info['rsi']:.2f}" if stock_info['rsi'] is not None else ""
                    pe_info = f"PE(TTM): {stock_info['pe_ttm']:.2f}"
                    tech_info = ", ".join(filter(None, [kdj_info, rsi_info, pe_info]))
            
                    # 显示分组标题
                    if stock_info['priority'] != current_priority:
                        current_priority = stock_info['priority']
                        log.info(f"=== {priority_groups[current_priority]} ===")
            
                    log.info(f"第{i+1}名: {stock_info['stock']}, 风险评分: {stock_info['risk_score']:.4f}, 看涨形态: {patterns}, {tech_info}")
            
            # 更新待交易股票池
            if self.optimized_stocks:
                self.optimized_stocks.sort(key=lambda x: (x['priority'], x['risk_score']))
                selected = [stock_info['stock'] for stock_info in self.optimized_stocks[:40]]  # 取前40只
                self.security = selected
                set_universe(self.security)
                log.info(f"待交易股票池: {selected[:40]}")  # 显示前40只
            else:
                self.security = []
                set_universe([])
                log.info("优选股池为空，清空股票池")
            
            # 成分股分析完成后直接触发买入策略
            self.analysis_completed = True
            
            self.execute_buy_strategy(context, from_analysis=True)
           
        except Exception as e:
            log.error(f"成分股分析出错: {e}")

    def is_strong_candlestick(self, today, yesterday):
        """检查强势K线形态"""
        # 当天是阳线
        if today['close'] < today['open']:
            return False
        
        # 涨幅大于1.5%
        increase = (today['close'] - today['open']) / today['open']
        return increase >= 0.015
        
    def calculate_macd(self, hist_data, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        if len(hist_data) < slow + signal:
            return None, None, None
            
        close = hist_data['close']
        
        exp1 = close.ewm(span=fast, adjust=False).mean()
        exp2 = close.ewm(span=slow, adjust=False).mean()
        
        macd_line = exp1 - exp2
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return macd_line.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]
    
    def calculate_risk_metrics(self, hist_data, ma_values, stock_code):
        """综合计算股票风险指标，包含所有技术风险因子"""
        # 使用局部变量缓存计算结果
        close_prices = hist_data['close']
        latest_close = close_prices.iloc[-1]
        latest_open = hist_data['open'].iloc[-1]
        latest_volume = hist_data['volume'].iloc[-1]
        
        # 解包均线值
        ma5, ma10, ma20, ma30, ma60 = ma_values
        
        risk_details = {}  # 风险明细
        total_risk_score = 0.0  # 总风险评分
        # 计算股票风险评分，综合多指标评估
        if len(hist_data) < 30:
            return float('inf')
            
        close_prices = hist_data['close']
        price_volatility = close_prices.std() / close_prices.mean()
        
        # 计算价格趋势强度
        if ma_values[0] is not None and ma_values[1] is not None and ma_values[2] is not None:
            ma5 = ma_values[0]
            ma10 = ma_values[1]
            ma20 = ma_values[2]
            
            # 均线多头排列加分，空头排列减分
            if ma5 > ma10 and ma10 > ma20:
                trend_strength = 1
            elif ma5 < ma10 and ma10 < ma20:
                trend_strength = -1
            else:
                trend_strength = 0
        else:
            trend_strength = 0
            
        # RSI评分 (50附近风险最低)
        if rsi is not None:
            rsi_score = abs(rsi - 50) / 50
        else:
            rsi_score = 0.5
            
        # KDJ J值评分 (50附近风险最低)
        if kdj_j is not None:
            kdj_score = abs(kdj_j - 50) / 50
        else:
            kdj_score = 0.5
            
        # MACD评分 (柱状值越大越好)
        if hist is not None:
            macd_score = max(0, -hist)  # 负数柱状值表示下跌趋势
        else:
            macd_score = 0
            
        # 成交量评分 (异常高的成交量可能意味着风险)
        volume = hist_data['volume']
        volume_ma20 = volume.rolling(window=20).mean().iloc[-1]
        latest_volume = volume.iloc[-1]
        
        if volume_ma20 > 0:
            volume_ratio = latest_volume / volume_ma20
            volume_score = min(1, max(0, volume_ratio - 1))  # 超过1倍的部分视为风险
        else:
            volume_score = 0
            
        # 综合评分 (权重可根据实际情况调整)
        risk_score = (
            0.3 * price_volatility +
            0.2 * (1 - trend_strength) +  # 趋势强度取反，因为我们要的是风险评分
            0.2 * rsi_score +
            0.1 * kdj_score +
            0.1 * macd_score +
            0.1 * volume_score
        )
        
        return risk_details, total_risk_score
    
    def analyze_bullish_patterns(self, hist_data):
        """分析所有看涨K线形态"""
        if len(hist_data) < 2:
            return []
            
        patterns = []
        
        # 1. 出水芙蓉形态
        if self.check_waterlily_pattern(hist_data):
            patterns.append("出水芙蓉")
            
        # 2. 双底形态
        if self.check_double_bottom(hist_data):
            patterns.append("双底形态")
            
        # 3. 看涨吞没形态
        if self.check_bullish_engulfing(hist_data):
            patterns.append("看涨吞没")
            
        # 4. 早晨之星形态
        if self.check_morning_star(hist_data):
            patterns.append("早晨之星")
            
        # 5. 红三兵形态
        if self.check_three_white_soldiers(hist_data):
            patterns.append("红三兵")
            
        # 6. 启明之星形态
        if self.check_doji_star(hist_data):
            patterns.append("启明之星")
            
        # 7. 低位十字星形态
        if self.check_low_doji(hist_data):
            patterns.append("低位十字星")
            
        # 8. 旭日东升形态
        if self.check_sunrise_pattern(hist_data):
            patterns.append("旭日东升")
            
        # 9. 好友反攻形态
        if self.check_friend_retreat(hist_data):
            patterns.append("好友反攻")
            
        # 10. 锤头线形态
        if self.check_hammer(hist_data):
            patterns.append("锤头线")
            
        # 11. 倒锤头线形态
        if self.check_inverted_hammer(hist_data):
            patterns.append("倒锤头线")
            
        # 12. 上升三法形态
        if self.check_ascending_three_methods(hist_data):
            patterns.append("上升三法")
            
        # 13. 平底形态
        if self.check_flat_bottom(hist_data):
            patterns.append("平底")
            
        # 14. 下探回升形态
        if self.check_dip_recovery(hist_data):
            patterns.append("下探回升")
            
        # 15. 孕线形态
        if self.check_harami_pattern(hist_data):
            patterns.append("孕线")
            
        # 16. 看涨十字星形态
        if self.check_bullish_doji(hist_data):
            patterns.append("看涨十字星")            
            
        return patterns
    
    def check_bullish_doji(self, hist_data):
        """检查看涨十字星形态"""
        if len(hist_data) < 2:
            return False
        
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
    
        # 第一天是阴线
        is_day1_bearish = day1['close'] < day1['open']
    
        # 第二天是十字星
        body2 = abs(day2['close'] - day2['open'])
        is_doji = body2 / (day2['high'] - day2['low']) < 0.1
    
        # 十字星收盘价高于前一天收盘价
        is_higher_close = day2['close'] > day1['close']
    
        return (is_day1_bearish and is_doji and is_higher_close)
    
    def analyze_bearish_patterns(self, hist_data):
        """分析看跌K线形态"""
        if len(hist_data) < 3:
            return []
            
        patterns = []
        
        # 黄昏之星
        if self.check_evening_star(hist_data):
            patterns.append("黄昏之星")
            
        # 看跌吞没
        if self.check_bearish_engulfing(hist_data):
            patterns.append("看跌吞没")
            
        # 乌云盖顶
        if self.check_dark_cloud_cover(hist_data):
            patterns.append("乌云盖顶")
            
        # 射击之星
        if self.check_shooting_star(hist_data):
            patterns.append("射击之星")
            
        # 吊颈线
        if self.check_hanging_man(hist_data):
            patterns.append("吊颈线")
            
        # 三只乌鸦
        if self.check_three_crows(hist_data):
            patterns.append("三只乌鸦")
            
        # 平顶
        if self.check_tweezers_top(hist_data):
            patterns.append("平顶")
            
        return patterns
        
    #===============================================================    
    # ==================各种K线形态检查方法===========================
    #===============================================================
    def calculate_moving_averages(self, hist_data, periods):
        """计算多周期移动平均线，返回各周期均线值"""
        ma_values = []
        for period in periods:
            if len(hist_data) < period:
                ma_values.append(None)
            else:
                ma = hist_data['close'].rolling(window=period).mean().iloc[-1]
                ma_values.append(ma)
        return ma_values
    
    def calculate_rsi(self, hist_data, period=14):
        """计算RSI指标"""
        if len(hist_data) < period + 1:
            return None
            
        close_prices = hist_data['close']
        deltas = close_prices.diff()
        
        # 分别计算上涨和下跌的变化
        gain = deltas.where(deltas > 0, 0)
        loss = -deltas.where(deltas < 0, 0)
        
        # 计算平均收益和平均损失
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # 计算RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1]
    
    def calculate_kdj_j(self, hist_data, period=9):
        """计算KDJ指标中的J值"""
        if len(hist_data) < period:
            return None
            
        low_values = hist_data['low'].rolling(window=period).min()
        high_values = hist_data['high'].rolling(window=period).max()
        close_prices = hist_data['close']
        
        # 计算RSV
        rsv = (close_prices - low_values) / (high_values - low_values) * 100
        
        # 计算K、D、J值
        k = rsv.ewm(com=2).mean()  # 相当于SMA(3)
        d = k.ewm(com=2).mean()    # 相当于SMA(3)
        j = 3 * k - 2 * d
        
        return j.iloc[-1]
    
    def is_strong_candlestick(self, current_k, prev_k):
        """判断是否为强势K线形态（阳线且具备上涨动能）"""
        # 阳线且收盘高于前一日收盘
        is_yang = current_k['close'] > current_k['open']
        is_higher_close = current_k['close'] > prev_k['close']
        
        # 上下影线分析（下影线长于上影线视为强势）
        upper_shadow = current_k['high'] - current_k['close']
        lower_shadow = current_k['open'] - current_k['low'] if is_yang else current_k['close'] - current_k['low']
        is_strong_shadow = lower_shadow > upper_shadow * 1.5
        
        # 涨幅分析（1%-3%为理想涨幅）
        increase_rate = (current_k['close'] - current_k['open']) / current_k['open'] * 100
        is_proper_increase = 1 <= increase_rate <= 3
        
        return is_yang and is_higher_close and (is_strong_shadow or is_proper_increase)
    
    def short_term_growth_rate(self, hist_data):
        """计算5日短期涨幅，用于判断是否追高"""
        if len(hist_data) < 5:
            return 0
        close_5d_ago = hist_data['close'].iloc[-5]
        latest_close = hist_data['close'].iloc[-1]
        return (latest_close - close_5d_ago) / close_5d_ago
    
    def is_volume_abnormal(self, hist_data, threshold):
        """判断成交量是否异常放大（超过历史均量一定倍数）"""
        if len(hist_data) < 20:
            return False
        recent_volumes = hist_data['volume'].iloc[-20:-1]
        avg_volume = recent_volumes.mean()
        latest_volume = hist_data['volume'].iloc[-1]
        return latest_volume > avg_volume * threshold
    
    def analyze_ma_status(self, ma5, ma10, ma20, ma30):
        """分析均线排列状态，判断多空趋势"""
        if None in (ma5, ma10, ma20, ma30):
            return "均线数据不足"
        
        # 多头排列判断（短周期在上，长周期在下）
        if ma5 > ma10 > ma20 > ma30:
            return "多头排列良好"
        elif ma5 > ma10 > ma20:
            return "多头排列一般"
        # 空头排列判断（短周期在下，长周期在上）
        elif ma5 < ma10 < ma20 < ma30:
            return "空头排列"
        else:
            return "均线纠结"
            
    def check_waterlily_pattern(self, hist_data):
        """检查出水芙蓉形态（一根大阳线穿过多根均线）"""
        if len(hist_data) < 20:
            return False
            
        last = hist_data.iloc[-1]
        ma_values = self.calculate_moving_averages(hist_data, [5, 10, 20])
        
        if None in ma_values:
            return False
            
        ma5, ma10, ma20 = ma_values
        
        # 检查是否为阳线，且收盘价高于开盘价的3%以上
        is_bullish = last['close'] > last['open'] and (last['close'] - last['open']) / last['open'] > 0.03
        
        # 检查收盘价是否同时上穿5、10、20日均线
        cross_ma = last['close'] > ma5 and last['close'] > ma10 and last['close'] > ma20 and \
                  last['open'] <= ma5 and last['open'] <= ma10 and last['open'] <= ma20
        
        # 出水芙蓉形态确认
        return is_bullish and cross_ma
    
    def check_double_bottom(self, hist_data):
        """检查双底形态"""
        if len(hist_data) < 20:
            return False
            
        # 获取近20个交易日数据
        recent_data = hist_data.iloc[-20:]
        
        # 寻找可能的低点（局部最小值）
        potential_bottoms = []
        for i in range(2, len(recent_data) - 2):
            # 定义局部最小值条件：当前日最低价低于前后各两天的最低价
            if (recent_data.iloc[i]['low'] < recent_data.iloc[i-1]['low'] and
                recent_data.iloc[i]['low'] < recent_data.iloc[i-2]['low'] and
                recent_data.iloc[i]['low'] < recent_data.iloc[i+1]['low'] and
                recent_data.iloc[i]['low'] < recent_data.iloc[i+2]['low']):
                potential_bottoms.append(i)
        
        # 至少需要两个低点才能形成双底
        if len(potential_bottoms) < 2:
            return False
            
        # 获取两个最近的低点
        bottom1_idx, bottom2_idx = potential_bottoms[-2], potential_bottoms[-1]
        
        # 检查两个低点之间的距离是否合适（3-10个交易日）
        distance = bottom2_idx - bottom1_idx
        if distance < 3 or distance > 10:
            return False
            
        # 获取两个低点的价格
        bottom1_price = recent_data.iloc[bottom1_idx]['low']
        bottom2_price = recent_data.iloc[bottom2_idx]['low']
        
        # 检查两个低点价格是否相近（相差不超过3%）
        price_diff = abs(bottom1_price - bottom2_price) / bottom1_price
        if price_diff > 0.03:
            return False
            
        # 获取颈线位置（两个低点之间的最高点）
        peak_price = recent_data.iloc[bottom1_idx:bottom2_idx]['high'].max()
        
        # 检查第二个低点后是否有突破颈线的动作
        post_bottom2_data = recent_data.iloc[bottom2_idx+1:]
        if len(post_bottom2_data) == 0:
            return False
            
        # 检查是否有收盘价突破颈线
        has_breakout = any(price['close'] > peak_price for _, price in post_bottom2_data.iterrows())
        
        return has_breakout
    
    def check_morning_star(self, hist_data):
        """检查早晨之星形态"""
        if len(hist_data) < 3:
            return False
            
        day1 = hist_data.iloc[-3]
        day2 = hist_data.iloc[-2]
        day3 = hist_data.iloc[-1]
        
        # 第一天是长阴线
        is_day1_bearish = day1['close'] < day1['open']
        body1 = abs(day1['close'] - day1['open'])
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.7
        
        # 第二天是十字星或小实体
        body2 = abs(day2['close'] - day2['open'])
        is_small_body2 = body2 / (day2['high'] - day2['low']) < 0.3
        
        # 第三天是长阳线
        is_day3_bearish = day3['close'] > day3['open']
        body3 = abs(day3['close'] - day3['open'])
        is_long_body3 = body3 / (day3['high'] - day3['low']) > 0.7
        
        # 第二天价格缺口
        gap_down = day2['high'] < day1['close']
        gap_up = day3['open'] > day2['low']
        
        # 第三天收盘价收复失地
        recovery = day3['close'] > (day1['close'] + day1['open']) / 2
        
        return (is_day1_bearish and is_long_body1 and 
                is_small_body2 and 
                is_day3_bearish and is_long_body3 and 
                gap_down and gap_up and recovery)
    
    def check_bullish_engulfing(self, hist_data):
        """检查看涨吞没形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天是阴线
        is_day1_bearish = day1['close'] < day1['open']
        
        # 第二天是阳线
        is_day2_bearish = day2['close'] > day2['open']
        
        # 第二天实体完全吞没第一天实体
        day2_body = day2['close'] - day2['open']
        day1_body = day1['open'] - day1['close']
        
        return (is_day1_bearish and is_day2_bearish and 
                day2['close'] > day1['open'] and 
                day2['open'] < day1['close'])
    
    def check_hammer(self, hist_data):
        """检查锤头线形态"""
        if len(hist_data) < 1:
            return False
            
        day = hist_data.iloc[-1]
        
        # 实体较小
        body = abs(day['close'] - day['open'])
        is_small_body = body / (day['high'] - day['low']) < 0.3
        
        # 下影线较长
        lower_shadow = min(day['open'], day['close']) - day['low']
        is_long_lower_shadow = lower_shadow / (day['high'] - day['low']) > 0.6
        
        # 上影线较短
        upper_shadow = day['high'] - max(day['open'], day['close'])
        is_short_upper_shadow = upper_shadow / (day['high'] - day['low']) < 0.1
        
        # 最好处于下降趋势中（简化判断）
        prev_close = hist_data['close'].iloc[-2] if len(hist_data) >= 2 else day['close']
        is_down_trend = day['close'] < prev_close
        
        return (is_small_body and is_long_lower_shadow and is_short_upper_shadow and is_down_trend)
    
    def check_inverted_hammer(self, hist_data):
        """检查倒锤头线形态"""
        if len(hist_data) < 1:
            return False
            
        day = hist_data.iloc[-1]
        
        # 实体较小
        body = abs(day['close'] - day['open'])
        is_small_body = body / (day['high'] - day['low']) < 0.3
        
        # 上影线较长
        upper_shadow = day['high'] - max(day['open'], day['close'])
        is_long_upper_shadow = upper_shadow / (day['high'] - day['low']) > 0.6
        
        # 下影线较短
        lower_shadow = min(day['open'], day['close']) - day['low']
        is_short_lower_shadow = lower_shadow / (day['high'] - day['low']) < 0.1
        
        # 最好处于下降趋势中（简化判断）
        prev_close = hist_data['close'].iloc[-2] if len(hist_data) >= 2 else day['close']
        is_down_trend = day['close'] < prev_close
        
        return (is_small_body and is_long_upper_shadow and is_short_lower_shadow and is_down_trend)
   
    def check_doji_star(self, hist_data):
        """检查启明星形态"""
        if len(hist_data) < 3:
            return False
            
        day1 = hist_data.iloc[-3]
        day2 = hist_data.iloc[-2]
        day3 = hist_data.iloc[-1]
        
        # 第一天是长阴线
        is_day1_bearish = day1['close'] < day1['open']
        body1 = abs(day1['close'] - day1['open'])
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.7
        
        # 第二天是十字星
        body2 = abs(day2['close'] - day2['open'])
        is_doji = body2 / (day2['high'] - day2['low']) < 0.1
        
        # 第三天是长阳线
        is_day3_bearish = day3['close'] > day3['open']
        body3 = abs(day3['close'] - day3['open'])
        is_long_body3 = body3 / (day3['high'] - day3['low']) > 0.7
        
        # 第二天价格缺口
        gap_down = day2['high'] < day1['close']
        gap_up = day3['open'] > day2['low']
        
        # 第三天收盘价收复失地
        recovery = day3['close'] > (day1['close'] + day1['open']) / 2
        
        return (is_day1_bearish and is_long_body1 and 
                is_doji and 
                is_day3_bearish and is_long_body3 and 
                gap_down and gap_up and recovery)
    
    #===============================================================
    # ======================形态检测方法=============================
    #===============================================================
    def check_three_white_soldiers(self, hist_data):
        """检测红三兵形态"""
        if len(hist_data) < 3:
            return False
            
        day1 = hist_data.iloc[-3]
        day2 = hist_data.iloc[-2]
        day3 = hist_data.iloc[-1]
        
        # 连续三根阳线
        is_day1_bullish = day1['close'] > day1['open']
        is_day2_bullish = day2['close'] > day2['open']
        is_day3_bullish = day3['close'] > day3['open']
        
        # 实体逐步增长
        body1 = day1['close'] - day1['open']
        body2 = day2['close'] - day2['open']
        body3 = day3['close'] - day3['open']
        is_body_increasing = body3 > body2 > body1 > 0
        
        # 收盘价依次升高
        is_close_increasing = day3['close'] > day2['close'] > day1['close']
        
        # 开盘价在前一天实体范围内
        is_open_within_body = (day1['open'] < day2['open'] < day1['close'] and 
                              day2['open'] < day3['open'] < day2['close'])
        
        # 成交量温和放大
        volume1 = day1['volume']
        volume2 = day2['volume']
        volume3 = day3['volume']
        is_volume_increasing = volume3 >= volume2 >= volume1
        
        return (is_day1_bullish and is_day2_bullish and is_day3_bullish and 
                is_body_increasing and is_close_increasing and 
                is_open_within_body and is_volume_increasing)
    def check_low_doji(self, hist_data):
        """检测低位十字星形态"""
        if len(hist_data) < 2:
            return False
            
        day = hist_data.iloc[-1]
        prev_day = hist_data.iloc[-2]
        
        # 十字星特征
        body = abs(day['close'] - day['open'])
        is_doji = body / (day['high'] - day['low']) < 0.2
        
        # 处于下降趋势的低位
        is_down_trend = day['close'] < prev_day['close'] and prev_day['close'] < hist_data['close'].iloc[-5]
        
        # 下影线较长
        lower_shadow = min(day['open'], day['close']) - day['low']
        is_long_shadow = lower_shadow / (day['high'] - day['low']) > 0.5
        
        return is_doji and is_down_trend and is_long_shadow
    def check_sunrise_pattern(self, hist_data):
        """检测旭日东升形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天阴线，第二天阳线
        is_day1_bearish = day1['close'] < day1['open']
        is_day2_bullish = day2['close'] > day2['open']
        
        # 阳线实体吞没阴线实体的70%以上
        day1_body = day1['open'] - day1['close']
        day2_body = day2['close'] - day2['open']
        is_engulfing = day2_body > day1_body * 0.7
        
        # 第二天收盘价高于第一天开盘价
        is_close_above_open = day2['close'] > day1['open']
        
        return is_day1_bearish and is_day2_bullish and is_engulfing and is_close_above_open
    def check_friend_retreat(self, hist_data):
        """检测好友反攻形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天阴线，第二天阳线
        is_day1_bearish = day1['close'] < day1['open']
        is_day2_bullish = day2['close'] > day2['open']
        
        # 第二天开盘价低于第一天收盘价（跳空低开）
        is_gap_down = day2['open'] < day1['close']
        
        # 第二天收盘价接近第一天开盘价（差值小于1%）
        price_diff = abs(day2['close'] - day1['open']) / day1['open']
        is_close_proximity = price_diff < 0.02
        
        return is_day1_bearish and is_day2_bullish and is_gap_down and is_close_proximity
    def check_ascending_three_methods(self, hist_data):
        """检测上升三法形态"""
        if len(hist_data) < 5:
            return False
            
        day1 = hist_data.iloc[-5]
        day2 = hist_data.iloc[-4]
        day3 = hist_data.iloc[-3]
        day4 = hist_data.iloc[-2]
        day5 = hist_data.iloc[-1]
        
        # 第一天长阳线
        is_day1_bullish = day1['close'] > day1['open']
        body1 = day1['close'] - day1['open']
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.6
        
        # 中间三天小阴线或小阳线，实体在第一天实体范围内
        is_consolidation = (day2['open'] > day1['close'] and day2['close'] < day1['open'] and
                           day3['open'] > day1['close'] and day3['close'] < day1['open'] and
                           day4['open'] > day1['close'] and day4['close'] < day1['open'])
        
        # 第五天长阳线，收盘价高于第一天收盘价
        is_day5_bullish = day5['close'] > day5['open']
        is_close_higher = day5['close'] > day1['close']
        
        return is_day1_bullish and is_long_body1 and is_consolidation and is_day5_bullish and is_close_higher
    def check_flat_bottom(self, hist_data):
        """检测平底形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 两天最低价几乎相同（差值小于0.5%）
        low_diff = abs(day1['low'] - day2['low']) / day1['low']
        is_same_low = low_diff < 0.01
        
        # 第二天是阳线或十字星
        is_day2_positive = day2['close'] >= day2['open']
        
        # 第二天收盘价高于第一天收盘价
        is_close_higher = day2['close'] > day1['close']
        
        return is_same_low and is_day2_positive and is_close_higher
    def check_dip_recovery(self, hist_data):
        """检测下探回升形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天长下影线，收盘价接近开盘价
        lower_shadow1 = min(day1['open'], day1['close']) - day1['low']
        body1 = abs(day1['close'] - day1['open'])
        is_long_shadow1 = lower_shadow1 / (day1['high'] - day1['low']) > 0.6
        is_small_body1 = body1 / (day1['high'] - day1['low']) < 0.3
        
        # 第二天阳线，收盘价高于第一天收盘价
        is_day2_bullish = day2['close'] > day2['open']
        is_close_higher = day2['close'] > day1['close']
        
        return is_long_shadow1 and is_small_body1 and is_day2_bullish and is_close_higher
    def check_harami_pattern(self, hist_data):
        """检测孕线形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天长实体，第二天短实体
        body1 = abs(day1['close'] - day1['open'])
        body2 = abs(day2['close'] - day2['open'])
        is_big_body1 = body1 / (day1['high'] - day1['low']) > 0.5
        is_small_body2 = body2 / (day2['high'] - day2['low']) < 0.3
        
        # 第二天实体完全在第一天实体范围内
        is_within_body = (day2['open'] > min(day1['open'], day1['close']) and
                         day2['close'] < max(day1['open'], day1['close']))
        
        # 第一天阴线，第二天阳线（看涨孕线）
        is_day1_bearish = day1['close'] < day1['open']
        is_day2_bullish = day2['close'] > day2['open']
        
        return is_big_body1 and is_small_body2 and is_within_body and is_day1_bearish
    
    def check_evening_star(self, hist_data):
        """检查黄昏之星形态"""
        if len(hist_data) < 3:
            return False
            
        day1 = hist_data.iloc[-3]
        day2 = hist_data.iloc[-2]
        day3 = hist_data.iloc[-1]
        
        # 第一天是长阳线
        is_day1_bullish = day1['close'] > day1['open']
        body1 = abs(day1['close'] - day1['open'])
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.7
        
        # 第二天是十字星或小实体
        body2 = abs(day2['close'] - day2['open'])
        is_small_body2 = body2 / (day2['high'] - day2['low']) < 0.3
        
        # 第三天是长阴线
        is_day3_bearish = day3['close'] < day3['open']
        body3 = abs(day3['close'] - day3['open'])
        is_long_body3 = body3 / (day3['high'] - day3['low']) > 0.7
        
        # 第二天价格缺口
        gap_up = day2['low'] > day1['close']
        gap_down = day3['high'] < day2['open']
        
        # 第三天收盘价跌破第一天的中点
        breakdown = day3['close'] < (day1['close'] + day1['open']) / 2
        
        return (is_day1_bullish and is_long_body1 and 
                is_small_body2 and 
                is_day3_bearish and is_long_body3 and 
                gap_up and gap_down and breakdown)
    
    def check_bearish_engulfing(self, hist_data):
        """检查看跌吞没形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天是阳线
        is_day1_bullish = day1['close'] > day1['open']
        
        # 第二天是阴线
        is_day2_bearish = day2['close'] < day2['open']
        
        # 第二天实体完全吞没第一天实体
        day2_body = day2['open'] - day2['close']
        day1_body = day1['close'] - day1['open']
        
        return (is_day1_bullish and is_day2_bearish and 
                day2['open'] > day1['close'] and 
                day2['close'] < day1['open'])
    
    def check_dark_cloud_cover(self, hist_data):
        """检查乌云盖顶形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 第一天是长阳线
        is_day1_bullish = day1['close'] > day1['open']
        body1 = abs(day1['close'] - day1['open'])
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.7
        
        # 第二天是长阴线
        is_day2_bearish = day2['close'] < day2['open']
        body2 = abs(day2['close'] - day2['open'])
        is_long_body2 = body2 / (day2['high'] - day2['low']) > 0.7
        
        # 第二天开盘价高于第一天最高价
        gap_up = day2['open'] > day1['high']
        
        # 第二天收盘价跌破第一天实体的中点
        breakdown = day2['close'] < (day1['close'] + day1['open']) / 2
        
        return (is_day1_bullish and is_long_body1 and 
                is_day2_bearish and is_long_body2 and 
                gap_up and breakdown)
    
    def check_shooting_star(self, hist_data):
        """检查射击之星形态"""
        if len(hist_data) < 1:
            return False
            
        day = hist_data.iloc[-1]
        
        # 实体较小，位于下方
        body = abs(day['close'] - day['open'])
        is_small_body = body / (day['high'] - day['low']) < 0.3
        is_body_lower = min(day['open'], day['close']) < (day['high'] + day['low']) / 2
        
        # 上影线较长
        upper_shadow = day['high'] - max(day['open'], day['close'])
        is_long_upper_shadow = upper_shadow / (day['high'] - day['low']) > 0.6
        
        # 下影线较短
        lower_shadow = min(day['open'], day['close']) - day['low']
        is_short_lower_shadow = lower_shadow / (day['high'] - day['low']) < 0.1
        
        # 最好处于上升趋势中（简化判断）
        prev_close = hist_data['close'].iloc[-2] if len(hist_data) >= 2 else day['close']
        is_up_trend = day['close'] > prev_close
        
        return (is_small_body and is_body_lower and is_long_upper_shadow and is_short_lower_shadow and is_up_trend)
    def check_hanging_man(self, hist_data):
        """检查吊颈线形态"""
        if len(hist_data) < 1:
            return False
            
        day = hist_data.iloc[-1]
        
        # 实体较小，位于上方
        body = abs(day['close'] - day['open'])
        is_small_body = body / (day['high'] - day['low']) < 0.3
        is_body_upper = max(day['open'], day['close']) > (day['high'] + day['low']) / 2
        
        # 下影线较长
        lower_shadow = min(day['open'], day['close']) - day['low']
        is_long_lower_shadow = lower_shadow / (day['high'] - day['low']) > 0.6
        
        # 上影线较短
        upper_shadow = day['high'] - max(day['open'], day['close'])
        is_short_upper_shadow = upper_shadow / (day['high'] - day['low']) < 0.1
        
        # 最好处于上升趋势中（简化判断）
        prev_close = hist_data['close'].iloc[-2] if len(hist_data) >= 2 else day['close']
        is_up_trend = day['close'] > prev_close
        
        return (is_small_body and is_body_upper and is_long_lower_shadow and is_short_upper_shadow and is_up_trend)
    
    def check_three_crows(self, hist_data):
        """检查三只乌鸦形态"""
        if len(hist_data) < 3:
            return False
            
        day1 = hist_data.iloc[-3]
        day2 = hist_data.iloc[-2]
        day3 = hist_data.iloc[-1]
        
        # 三天都是阴线
        is_day1_bearish = day1['close'] < day1['open']
        is_day2_bearish = day2['close'] < day2['open']
        is_day3_bearish = day3['close'] < day3['open']
        
        # 实体都比较长
        body1 = abs(day1['close'] - day1['open'])
        body2 = abs(day2['close'] - day2['open'])
        body3 = abs(day3['close'] - day3['open'])
        
        is_long_body1 = body1 / (day1['high'] - day1['low']) > 0.7
        is_long_body2 = body2 / (day2['high'] - day2['low']) > 0.7
        is_long_body3 = body3 / (day3['high'] - day3['low']) > 0.7
        
        # 每天开盘价在前一天实体范围内
        day1_range = (min(day1['open'], day1['close']), max(day1['open'], day1['close']))
        day2_range = (min(day2['open'], day2['close']), max(day2['open'], day2['close']))
        
        is_open_in_range1 = day1_range[0] < day2['open'] < day1_range[1]
        is_open_in_range2 = day2_range[0] < day3['open'] < day2_range[1]
        
        # 每天收盘价低于前一天收盘价
        is_lower_close1 = day2['close'] < day1['close']
        is_lower_close2 = day3['close'] < day2['close']
        
        return (is_day1_bearish and is_day2_bearish and is_day3_bearish and 
                is_long_body1 and is_long_body2 and is_long_body3 and 
                is_open_in_range1 and is_open_in_range2 and 
                is_lower_close1 and is_lower_close2)
    
    def check_tweezers_top(self, hist_data):
        """检查平顶形态"""
        if len(hist_data) < 2:
            return False
            
        day1 = hist_data.iloc[-2]
        day2 = hist_data.iloc[-1]
        
        # 两天最高价几乎相同
        high_diff = abs(day1['high'] - day2['high'])
        is_same_high = high_diff / day1['high'] < 0.003
        
        # 第一天是阳线
        is_day1_bullish = day1['close'] > day1['open']
        
        # 第二天是阴线或十字星
        is_day2_bearish = day2['close'] <= day2['open']
        
        # 第二天收盘价低于第一天收盘价
        is_lower_close = day2['close'] < day1['close']
        
        return (is_same_high and is_day1_bullish and is_day2_bearish and is_lower_close)
    
    def calculate_risk_metrics(self, hist_data, ma_values, stock_code):
        """综合计算股票风险指标，包含所有技术风险因子"""
        # 使用局部变量缓存计算结果
        close_prices = hist_data['close']
        latest_close = close_prices.iloc[-1]
        latest_open = hist_data['open'].iloc[-1]
        latest_volume = hist_data['volume'].iloc[-1]
        
        # 解包均线值
        ma5, ma10, ma20, ma30, ma60 = ma_values
        
        risk_details = {}  # 风险明细
        total_risk_score = 0.0  # 总风险评分
        
        # 1. MACD指标背离检测
        try:
            ema12 = close_prices.ewm(span=12, adjust=False).mean()
            ema26 = close_prices.ewm(span=26, adjust=False).mean()
            dif = ema12 - ema26
            dea = dif.ewm(span=9, adjust=False).mean()
            macd = (dif - dea) * 2
            
            # 判断MACD柱状图收缩（顶背离信号）
            if len(macd) >= 3 and macd.iloc[-1] < macd.iloc[-2] < macd.iloc[-3] and macd.iloc[-1] > 0:
                risk_details["macd_divergence"] = {"value": 1, "weight": 0.12}
                total_risk_score += 0.12
        except Exception as e:
            log.error(f"MACD计算错误: {e}")
        
        # 2. 大阴线跌破20日均线
        if ma20 is not None and latest_close < ma20 and latest_close < latest_open and (latest_open - latest_close) / latest_open > 0.02:
            risk_details["big_red_line_break_ma20"] = {"value": 1, "weight": 0.10}
            total_risk_score += 0.10
        
        # 3. 量价背离（高权重风险因子）
        if len(hist_data) >= 5:
            price_trend = (latest_close - hist_data['close'].iloc[-5]) / hist_data['close'].iloc[-5]
            volume_trend = (latest_volume - hist_data['volume'].iloc[-5]) / hist_data['volume'].iloc[-5]
            if price_trend > 0 and volume_trend < 0:
                risk_details["price_volume_divergence"] = {"value": 1, "weight": 0.20}
                total_risk_score += 0.20
        
        # 4. 均线死叉
        if ma5 is not None and ma10 is not None and ma5 < ma10:
            ma5_prev = hist_data['close'].rolling(window=5).mean().iloc[-10]
            ma10_prev = hist_data['close'].rolling(window=10).mean().iloc[-10]
            if ma5_prev > ma10_prev:
                risk_details["ma_death_cross"] = {"value": 1, "weight": 0.15}
                total_risk_score += 0.15
        
        # 5. 弱势形态（20日均线向下）
        if ma20 is not None and len(hist_data) >= 21:
            current_ma20 = ma20
            prev_ma20 = hist_data['close'].rolling(window=20).mean().iloc[-2]
            if latest_close < current_ma20 and current_ma20 < prev_ma20:
                risk_details["weak_technical_pattern"] = {"value": 1, "weight": 0.20}
                total_risk_score += 0.20
        
        # 6. 下跌趋势中放量
        if ma20 is not None and len(hist_data) >= 21 and ma60 is not None:
            current_ma20 = ma20
            prev_ma20 = hist_data['close'].rolling(window=20).mean().iloc[-2]
            ma_volume_60 = hist_data['volume'].rolling(window=60).mean().iloc[-1]
            
            is_weak_pattern = latest_close < current_ma20 and current_ma20 < prev_ma20
            is_volume_spike = latest_volume > ma_volume_60 * self.VOLUME_SPIKE_THRESHOLD
            
            if is_weak_pattern and is_volume_spike:
                risk_details["volume_spike_in_downtrend"] = {"value": 1, "weight": 0.10}
                total_risk_score += 0.10
        
        # 7. 连续下跌
        if len(hist_data) >= 3:
            is_three_red = all(hist_data['close'].iloc[-i] < hist_data['open'].iloc[-i] for i in range(1, 4))
            close_3d_ago = hist_data['close'].iloc[-3]
            cumulative_decline = (latest_close - close_3d_ago) / close_3d_ago
            
            if is_three_red and cumulative_decline < -0.05:
                risk_details["continuous_decline"] = {"value": 1, "weight": 0.12}
                total_risk_score += 0.12
        
        # 8. RSI超买风险
        if len(hist_data) >= 14:
            rsi = self.calculate_rsi(hist_data)
            if rsi is not None and rsi > self.RSI_OVERBOUGHT_LEVEL:
                risk_details["rsi_overbought"] = {"value": 1, "weight": 0.15}
                total_risk_score += 0.15
        
        # 9. KDJ超买风险
        if len(hist_data) >= 9:
            kdj_j = self.calculate_kdj_j(hist_data)
            if kdj_j is not None and kdj_j > self.KDJ_J_OVERBOUGHT:
                risk_details["kdj_overbought"] = {"value": 1, "weight": 0.12}
                total_risk_score += 0.12
        
        # 10. 布林带突破风险
        if len(hist_data) >= 20:
            # 计算布林带
            middle_band = hist_data['close'].rolling(window=20).mean()
            std_dev = hist_data['close'].rolling(window=20).std()
            upper_band = middle_band + 2 * std_dev
            lower_band = middle_band - 2 * std_dev
            
            if latest_close > upper_band.iloc[-1]:
                risk_details["bollinger_breakout"] = {"value": 1, "weight": 0.10}
                total_risk_score += 0.10
        
        # 确保风险分数在合理范围内
        total_risk_score = max(0, min(total_risk_score, 1.0))  # 限制风险评分在0-1之间
        
        return risk_details, total_risk_score
        
# 策略实例化
strategy = TradingStrategy()
# 策略初始化
def initialize(context):
    strategy.initialize(context)
# 数据处理
def handle_data(context, data):
    strategy.handle_data(context, data)