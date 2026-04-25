# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 股票市场数据分析系统
开发者：熊猫大侠
版本：v2.1.0
许可证：MIT License
"""
# scenario_predictor.py
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import openai
import logging
from logging.handlers import RotatingFileHandler
from enhanced_data_collector import get_collector
"""

"""

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ScenarioPredictor:
    def __init__(self, analyzer, openai_api_key=None, openai_model=None):
        self.analyzer = analyzer
        self.data_collector = get_collector()
        self.openai_api_key = os.getenv('OPENAI_API_KEY', os.getenv('OPENAI_API_KEY'))
        self.openai_api_url = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1')
        self.openai_model = os.getenv('OPENAI_API_MODEL', 'gemini-2.0-pro-exp-02-05')
        # logging.info(f"scenario_predictor初始化完成：「{self.openai_api_key} {self.openai_api_url} {self.openai_model}」")

    def generate_scenarios(self, stock_code, market_type='A', days=60):
        """生成乐观、中性、悲观三种市场情景预测"""
        try:
            # 获取股票数据和技术指标
            df = self.analyzer.get_stock_data(stock_code, market_type)
            df = self.analyzer.calculate_indicators(df)

            # 获取股票信息
            stock_info = self.analyzer.get_stock_info(stock_code)

            # 计算基础数据
            current_price = df.iloc[-1]['close']
            avg_volatility = df['Volatility'].mean()

            # 根据历史波动率计算情景
            scenarios = self._calculate_scenarios(df, days)

            # 收集增强版多维度数据
            enhanced_data = {}
            try:
                enhanced_data = self.data_collector.collect_comprehensive_data(stock_code, market_type)
            except Exception as e:
                logging.warning(f"增强数据收集失败，使用基础数据: {e}")

            # 使用AI生成各情景的分析（注入增强数据）
            if self.openai_api_key:
                ai_analysis = self._generate_ai_analysis(stock_code, stock_info, df, scenarios, enhanced_data)
                scenarios.update(ai_analysis)

            # logging.info(f"返回前的情景预测：{scenarios}")
            return scenarios
        except Exception as e:
            # logging.info(f"生成情景预测出错: {str(e)}")
            return {}

    def _calculate_scenarios(self, df, days, n_simulations=5000):
        """
        基于蒙特卡洛模拟(GBM几何布朗运动)计算价格预测
        
        GBM模型: dS = μ·S·dt + σ·S·dW
        离散化: S(t+1) = S(t) * exp((μ - σ²/2)·dt + σ·√dt·Z)
        
        参数:
            df: 历史价格DataFrame
            days: 预测天数
            n_simulations: 模拟路径数量（默认5000条）
        
        输出:
            概率锥（5%/25%/50%/75%/95%分位数）+ VaR + 三情景
        """
        current_price = df.iloc[-1]['close']
        
        # ========== 1. 估计GBM参数 ==========
        # 计算对数收益率
        log_returns = np.log(df['close'] / df['close'].shift(1)).dropna()
        
        # 日均收益率（漂移项μ）
        mu_daily = log_returns.mean()
        # 日波动率（扩散项σ）
        sigma_daily = log_returns.std()
        
        # 年化参数（用于展示）
        mu_annual = mu_daily * 252
        sigma_annual = sigma_daily * np.sqrt(252)
        
        # 时间步长
        dt = 1.0  # 1个交易日
        
        # ========== 2. 蒙特卡洛模拟 ==========
        np.random.seed(None)  # 每次运行不同结果
        
        # 生成随机数矩阵 (n_simulations x days)
        Z = np.random.standard_normal((n_simulations, days))
        
        # GBM离散化公式: S(t+1) = S(t) * exp((μ - σ²/2)·dt + σ·√dt·Z)
        drift = (mu_daily - 0.5 * sigma_daily ** 2) * dt
        diffusion = sigma_daily * np.sqrt(dt) * Z
        
        # 计算每日收益率
        daily_returns = np.exp(drift + diffusion)
        
        # 构建价格路径矩阵 (n_simulations x (days+1))
        price_paths = np.zeros((n_simulations, days + 1))
        price_paths[:, 0] = current_price
        
        for t in range(days):
            price_paths[:, t + 1] = price_paths[:, t] * daily_returns[:, t]
        
        # ========== 3. 计算概率锥（分位数） ==========
        percentiles = [5, 10, 25, 50, 75, 90, 95]
        probability_cone = {}
        for p in percentiles:
            probability_cone[f'p{p}'] = np.percentile(price_paths, p, axis=0).tolist()
        
        # ========== 4. 计算VaR和CVaR ==========
        final_prices = price_paths[:, -1]
        final_returns = (final_prices - current_price) / current_price * 100  # 百分比收益
        
        # VaR: 在给定置信水平下的最大预期损失
        var_95 = np.percentile(final_returns, 5)   # 95%置信度VaR
        var_99 = np.percentile(final_returns, 1)   # 99%置信度VaR
        
        # CVaR (条件VaR / 期望损失): 超过VaR时的平均损失
        cvar_95 = final_returns[final_returns <= var_95].mean()
        cvar_99 = final_returns[final_returns <= var_99].mean()
        
        # VaR金额
        var_95_amount = current_price * var_95 / 100
        var_99_amount = current_price * var_99 / 100
        
        # ========== 5. 模拟统计信息 ==========
        expected_price = np.mean(final_prices)
        median_price = np.median(final_prices)
        std_price = np.std(final_prices)
        
        # 上涨/下跌概率
        prob_up = np.mean(final_prices > current_price) * 100
        prob_down = 100 - prob_up
        
        # 涨幅超过10%/20%的概率
        prob_up_10 = np.mean(final_prices > current_price * 1.10) * 100
        prob_up_20 = np.mean(final_prices > current_price * 1.20) * 100
        # 跌幅超过10%/20%的概率
        prob_down_10 = np.mean(final_prices < current_price * 0.90) * 100
        prob_down_20 = np.mean(final_prices < current_price * 0.80) * 100
        
        # 最大涨幅和最大跌幅
        max_return = (np.max(final_prices) / current_price - 1) * 100
        min_return = (np.min(final_prices) / current_price - 1) * 100
        
        # ========== 6. 提取三情景路径（用于兼容旧前端） ==========
        # 乐观 = 90%分位数路径，中性 = 50%分位数路径，悲观 = 10%分位数路径
        opt_path = probability_cone['p90']
        neu_path = probability_cone['p50']
        pes_path = probability_cone['p10']
        
        optimistic_target = opt_path[-1]
        neutral_target = neu_path[-1]
        pessimistic_target = pes_path[-1]
        
        # 生成日期序列
        start_date = datetime.now()
        dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days + 1)]
        
        # ========== 7. 抽样展示路径（取10条代表性路径） ==========
        sample_indices = np.random.choice(n_simulations, min(10, n_simulations), replace=False)
        sample_paths = {}
        for idx, si in enumerate(sample_indices):
            sample_paths[f'path_{idx}'] = price_paths[si].tolist()
        
        # ========== 8. 组织结果 ==========
        return {
            'current_price': current_price,
            'simulation_type': 'monte_carlo_gbm',
            'n_simulations': n_simulations,
            'prediction_days': days,
            'dates': dates,
            
            # GBM参数
            'gbm_params': {
                'mu_daily': float(mu_daily),
                'sigma_daily': float(sigma_daily),
                'mu_annual': float(mu_annual),
                'sigma_annual': float(sigma_annual),
            },
            
            # 概率锥数据
            'probability_cone': {k: [round(v, 4) for v in vals] for k, vals in probability_cone.items()},
            
            # VaR风险指标
            'var': {
                'var_95': round(float(var_95), 2),
                'var_99': round(float(var_99), 2),
                'cvar_95': round(float(cvar_95), 2),
                'cvar_99': round(float(cvar_99), 2),
                'var_95_amount': round(float(var_95_amount), 2),
                'var_99_amount': round(float(var_99_amount), 2),
            },
            
            # 模拟统计
            'statistics': {
                'expected_price': round(float(expected_price), 2),
                'median_price': round(float(median_price), 2),
                'std_price': round(float(std_price), 2),
                'prob_up': round(float(prob_up), 1),
                'prob_down': round(float(prob_down), 1),
                'prob_up_10': round(float(prob_up_10), 1),
                'prob_up_20': round(float(prob_up_20), 1),
                'prob_down_10': round(float(prob_down_10), 1),
                'prob_down_20': round(float(prob_down_20), 1),
                'max_return': round(float(max_return), 2),
                'min_return': round(float(min_return), 2),
            },
            
            # 抽样路径（用于前端展示）
            'sample_paths': sample_paths,
            
            # 三情景（兼容旧前端）
            'optimistic': {
                'target_price': round(float(optimistic_target), 2),
                'change_percent': round(float((optimistic_target / current_price - 1) * 100), 2),
                'path': dict(zip(dates, [round(float(p), 4) for p in opt_path]))
            },
            'neutral': {
                'target_price': round(float(neutral_target), 2),
                'change_percent': round(float((neutral_target / current_price - 1) * 100), 2),
                'path': dict(zip(dates, [round(float(p), 4) for p in neu_path]))
            },
            'pessimistic': {
                'target_price': round(float(pessimistic_target), 2),
                'change_percent': round(float((pessimistic_target / current_price - 1) * 100), 2),
                'path': dict(zip(dates, [round(float(p), 4) for p in pes_path]))
            }
        }

    def _generate_ai_analysis(self, stock_code, stock_info, df, scenarios, enhanced_data=None):
        """使用AI生成各情景的分析说明，包含风险和机会因素（增强版：整合多维度数据）"""
        try:
            openai.api_key = self.openai_api_key
            openai.api_base = self.openai_api_url
    
            # 提取关键数据
            current_price = df.iloc[-1]['close']
            ma5 = df.iloc[-1]['MA5']
            ma20 = df.iloc[-1]['MA20']
            ma60 = df.iloc[-1]['MA60']
            rsi = df.iloc[-1]['RSI']
            macd = df.iloc[-1]['MACD']
            signal = df.iloc[-1]['Signal']
    
            # 构建增强版数据上下文
            enhanced_context = ""
            if enhanced_data:
                # 基本面数据
                fund = enhanced_data.get('fundamental', {})
                if fund:
                    enhanced_context += f"\n    3. 基本面数据:\n"
                    if fund.get('pe_ttm') is not None:
                        enhanced_context += f"    - PE(TTM): {fund['pe_ttm']:.2f} ({fund.get('valuation_signal', '')})\n"
                    if fund.get('roe') is not None:
                        enhanced_context += f"    - ROE: {fund['roe']:.2f}% ({fund.get('profitability_signal', '')})\n"
                    if fund.get('debt_ratio') is not None:
                        enhanced_context += f"    - 资产负债率: {fund['debt_ratio']:.2f}% ({fund.get('financial_health', '')})\n"
                    if fund.get('revenue_growth_3y') is not None:
                        enhanced_context += f"    - 营收3年CAGR: {fund['revenue_growth_3y']:.2f}% ({fund.get('growth_signal', '')})\n"
                    if fund.get('total_mv') is not None:
                        enhanced_context += f"    - 总市值: {fund['total_mv']:.2f}亿 ({fund.get('market_cap_level', '')})\n"
    
                # 资金流数据
                capital = enhanced_data.get('capital_flow', {})
                if capital:
                    enhanced_context += f"\n    4. 资金流向:\n"
                    if capital.get('capital_signal'):
                        enhanced_context += f"    - {capital['capital_signal']}\n"
                    if capital.get('north_signal'):
                        enhanced_context += f"    - 北向资金: {capital['north_signal']}\n"
    
                # 市场情绪数据
                sentiment = enhanced_data.get('market_sentiment', {})
                if sentiment:
                    enhanced_context += f"\n    5. 市场情绪:\n"
                    if sentiment.get('sentiment_signal'):
                        enhanced_context += f"    - {sentiment['sentiment_signal']}\n"
                    if sentiment.get('emotion_phase'):
                        enhanced_context += f"    - BJCJ情绪阶段: 【{sentiment['emotion_phase']}】 建议仓位: {sentiment.get('suggested_position', '')}\n"
                    if sentiment.get('fbl') is not None:
                        enhanced_context += f"    - 封板率: {sentiment['fbl']:.0f}%, 赚钱效应: {sentiment.get('earn_rate', 0):.0f}%\n"
    
                # 宏观数据
                macro = enhanced_data.get('macro', {})
                if macro:
                    enhanced_context += f"\n    6. 宏观环境:\n"
                    if macro.get('amount_signal'):
                        enhanced_context += f"    - {macro['amount_signal']}\n"
                    if macro.get('spread_signal'):
                        enhanced_context += f"    - {macro['spread_signal']}\n"
                    if macro.get('hs300_change_20d') is not None:
                        enhanced_context += f"    - 沪深300近20日: {macro['hs300_change_20d']:.2f}%\n"
    
            # 构建提示词，增加对风险和机会因素的要求
            prompt = f"""分析股票{stock_code}（{stock_info.get('股票名称', '未知')}）的三种市场情景:
    
    1. 当前技术面数据:
    - 当前价格: {current_price}
    - 均线: MA5={ma5}, MA20={ma20}, MA60={ma60}
    - RSI: {rsi}
    - MACD: {macd}, Signal: {signal}
    
    2. 预测目标价:
    - 乐观情景: {scenarios['optimistic']['target_price']:.2f} ({scenarios['optimistic']['change_percent']:.2f}%)
    - 中性情景: {scenarios['neutral']['target_price']:.2f} ({scenarios['neutral']['change_percent']:.2f}%)
    - 悲观情景: {scenarios['pessimistic']['target_price']:.2f} ({scenarios['pessimistic']['change_percent']:.2f}%)
    {enhanced_context}
    请结合以上所有维度的数据（技术面、基本面、资金流、市场情绪、宏观环境），提供以下内容，格式为JSON:
    {{
    "optimistic_analysis": "乐观情景分析(100字以内)...",
    "neutral_analysis": "中性情景分析(100字以内)...",
    "pessimistic_analysis": "悲观情景分析(100字以内)...",
    "risk_factors": ["主要风险因素1", "主要风险因素2", "主要风险因素3", "主要风险因素4", "主要风险因素5"],
    "opportunity_factors": ["主要机会因素1", "主要机会因素2", "主要机会因素3", "主要机会因素4", "主要机会因素5"]
    }}
    
    风险和机会因素应该具体说明，每条5-15个字，简明扼要。
    """
    
            # 调用AI API
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": "你是专业的股票分析师，擅长技术分析和情景预测。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7
            )
    
            # 解析AI回复
            import json
            try:
                analysis = json.loads(response.choices[0].message.content)
                # 确保返回的JSON包含所需的所有字段
                if "risk_factors" not in analysis:
                    analysis["risk_factors"] = self._get_default_risk_factors()
                if "opportunity_factors" not in analysis:
                    analysis["opportunity_factors"] = self._get_default_opportunity_factors()
                return analysis
            except:
                # 如果解析失败，尝试从文本中提取JSON
                import re
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response.choices[0].message.content)
                if json_match:
                    json_str = json_match.group(1)
                    try:
                        analysis = json.loads(json_str)
                        # 确保包含所需的所有字段
                        if "risk_factors" not in analysis:
                            analysis["risk_factors"] = self._get_default_risk_factors()
                        if "opportunity_factors" not in analysis:
                            analysis["opportunity_factors"] = self._get_default_opportunity_factors()
                        return analysis
                    except:
                        # JSON解析失败时返回默认值
                        return self._get_default_analysis()
                else:
                    # 无法提取JSON时返回默认值
                    return self._get_default_analysis()
        except Exception as e:
            print(f"生成AI分析出错: {str(e)}")
            return self._get_default_analysis()
    
    def _get_default_risk_factors(self):
        """返回默认的风险因素"""
        return [
            "宏观经济下行压力增大",
            "行业政策收紧可能性",
            "原材料价格上涨",
            "市场竞争加剧",
            "技术迭代风险"
        ]
    
    def _get_default_opportunity_factors(self):
        """返回默认的机会因素"""
        return [
            "行业景气度持续向好",
            "公司新产品上市",
            "成本控制措施见效",
            "产能扩张计划",
            "国际市场开拓机会"
        ]
    
    def _get_default_analysis(self):
        """返回默认的分析结果（包含风险和机会因素）"""
        return {
            "optimistic_analysis": "乐观情景分析暂无",
            "neutral_analysis": "中性情景分析暂无",
            "pessimistic_analysis": "悲观情景分析暂无",
            "risk_factors": self._get_default_risk_factors(),
            "opportunity_factors": self._get_default_opportunity_factors()
        }
    
    
    
    
    
    



























































