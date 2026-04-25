# -*- coding: utf-8 -*-
"""
多模型协作股票分析系统（基础版，不含辩论机制）
功能: 多个AI模型并行分析股票，综合给出投资建议
优化: 整合多维度数据源（技术面+基本面+资金流+市场情绪+宏观环境+新闻），大幅提升分析深度
"""

import json
import logging
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from stock_analyzer import StockAnalyzer
from enhanced_data_collector import get_collector


class MultiModelAnalyzer:
    """多模型协作分析器（基础版）- 增强数据版"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_analyzer = StockAnalyzer()
        self.data_collector = get_collector()
        
        # 可用模型列表（仅保留本地已安装的模型）
        self.available_models = ["qwen2.5:14b", "deepseek-coder:6.7b"]
        
        # Ollama API配置
        self.ollama_url = "http://localhost:11434/v1/chat/completions"
    
    def get_stock_comprehensive_data(self, stock_code: str, market_type: str = 'A') -> Dict:
        """获取股票全面数据用于AI分析（增强版）"""
        try:
            # 使用增强版数据收集器获取多维度数据
            comprehensive_data = self.data_collector.collect_comprehensive_data(stock_code, market_type)
            
            # 生成AI可读的格式化文本
            formatted_text = self.data_collector.format_data_for_ai(comprehensive_data)
            
            comprehensive_data['formatted_text'] = formatted_text
            return comprehensive_data
            
        except Exception as e:
            self.logger.error(f"获取股票全面数据失败: {e}")
            # 降级到基础数据
            return self._get_basic_data_fallback(stock_code, market_type)
    
    def _get_basic_data_fallback(self, stock_code: str, market_type: str = 'A') -> Dict:
        """降级方案：获取基础技术面数据"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            
            df = self.base_analyzer.get_stock_data(stock_code, market_type, start_date, end_date)
            if df.empty:
                return {}
            
            df = self.base_analyzer.calculate_indicators(df)
            latest = df.iloc[-1]
            
            basic_text = f"""
📊 股票: {stock_code}
═══ 技术面数据（基础版）═══
  当前价格: {float(latest['close']):.2f}
  涨跌幅: {float(latest.get('change_pct', 0)):.2f}%
  RSI: {float(latest.get('rsi', latest.get('RSI', 50))):.2f}
  MACD: {float(latest.get('macd', latest.get('MACD', 0))):.4f}
  MA5: {float(latest.get('ma_5', latest.get('MA5', 0))):.2f}
  MA20: {float(latest.get('ma_20', latest.get('MA20', 0))):.2f}
  量比: {float(latest.get('volume_ratio', latest.get('Volume_Ratio', 1))):.2f}
  注意：基本面、资金流、市场情绪等数据暂不可用，请仅基于技术面分析。
"""
            return {
                'stock_code': stock_code,
                'technical': {'current_price': float(latest['close'])},
                'formatted_text': basic_text
            }
        except Exception as e:
            self.logger.error(f"降级数据获取也失败: {e}")
            return {}
    
    def call_model_analysis(self, model_name: str, stock_data: Dict, timeout: int = 120) -> Dict:
        """调用单个模型进行分析（增强版prompt）"""
        try:
            formatted_text = stock_data.get('formatted_text', '数据不可用')
            stock_code = stock_data.get('stock_code', 'N/A')
            
            prompt = f"""
你是一位拥有20年经验的顶级股票分析师，精通技术分析、基本面分析、资金流分析和宏观经济分析。
请基于以下全面的多维度数据，对股票进行深度分析并给出投资建议。

{formatted_text}

═══════════════════════════════════════
请从以下维度进行深度分析：

1. 【技术面综合判断】
   - 趋势方向和强度（均线排列、MACD、RSI等综合判断）
   - 关键支撑位和压力位
   - 短期技术信号（金叉/死叉、超买超卖、量价配合等）

2. 【基本面价值评估】
   - 估值水平是否合理（PE/PB与行业对比）
   - 盈利能力和成长性
   - 财务健康状况

3. 【资金面分析】
   - 主力资金动向（是否有主力建仓/出货迹象）
   - 北向资金态度
   - 量能变化含义

4. 【市场环境评估】
   - 当前市场情绪是否有利
   - 所属行业是否处于热点
   - 宏观环境对该股的影响

5. 【综合投资建议】
   - 明确的操作建议（买入/持有/卖出）
   - 1-10分综合评分
   - 风险等级（高/中/低）
   - 建议仓位比例
   - 止损位和目标位

请以JSON格式回复：
{{
    "model_name": "{model_name}",
    "technical_analysis": "技术面综合分析（包含趋势、指标、量价关系）",
    "fundamental_analysis": "基本面分析（估值、盈利、成长）",
    "capital_flow_analysis": "资金面分析（主力动向、北向资金）",
    "market_environment": "市场环境评估（情绪、行业、宏观）",
    "recommendation": "买入/持有/卖出",
    "score": 分数(1-10),
    "risk_level": "高/中/低",
    "suggested_position": "建议仓位百分比",
    "stop_loss": "建议止损位",
    "target_price": "目标价位",
    "key_reasons": ["核心理由1", "核心理由2", "核心理由3", "核心理由4", "核心理由5"],
    "risk_warnings": ["风险提示1", "风险提示2", "风险提示3"],
    "confidence": 置信度(0.0-1.0),
    "time_horizon": "建议持有周期（短线/中线/长线）"
}}
"""

            payload = {
                "model": model_name,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": 2000
            }
            
            start_time = time.time()
            response = requests.post(self.ollama_url, json=payload, timeout=timeout)
            analysis_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                try:
                    start_idx = content.find('{')
                    end_idx = content.rfind('}') + 1
                    if start_idx != -1 and end_idx != 0:
                        json_str = content[start_idx:end_idx]
                        analysis_result = json.loads(json_str)
                        analysis_result['analysis_time'] = round(analysis_time, 2)
                        analysis_result['model_name'] = model_name
                        analysis_result['raw_content'] = content
                        return analysis_result
                except json.JSONDecodeError:
                    pass
                
                return {
                    'model_name': model_name,
                    'technical_analysis': content,
                    'recommendation': '持有',
                    'score': 5,
                    'risk_level': '中',
                    'key_reasons': ['分析内容解析失败，请查看原始内容'],
                    'confidence': 0.5,
                    'analysis_time': round(analysis_time, 2),
                    'raw_content': content
                }
            else:
                return {
                    'model_name': model_name,
                    'error': f'API调用失败: {response.status_code}',
                    'analysis_time': round(analysis_time, 2)
                }
                
        except Exception as e:
            return {
                'model_name': model_name,
                'error': str(e),
                'analysis_time': 0
            }
    
    def multi_model_analysis(self, stock_code: str, market_type: str = 'A',
                             models: Optional[List[str]] = None, max_workers: int = 4) -> Dict:
        """
        多模型协作分析主函数（增强版）
        """
        start_time = time.time()
        
        if models is None:
            models = self.available_models
        else:
            models = [m for m in models if m in self.available_models]
        
        if not models:
            return {'error': '没有可用的模型'}
        
        self.logger.info(f"开始增强版多模型分析: {stock_code}, 使用模型: {models}")
        
        # 获取全面的股票数据（增强版）
        stock_data = self.get_stock_comprehensive_data(stock_code, market_type)
        if not stock_data:
            return {'error': '无法获取股票数据'}
        
        # 并行调用多个模型
        model_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_model = {
                executor.submit(self.call_model_analysis, model, stock_data): model
                for model in models
            }
            
            for future in as_completed(future_to_model, timeout=300):
                model_name = future_to_model[future]
                try:
                    result = future.result()
                    model_results.append(result)
                    self.logger.info(f"模型分析完成: {model_name}")
                except Exception as e:
                    self.logger.error(f"模型分析失败: {model_name}: {e}")
                    model_results.append({
                        'model_name': model_name,
                        'error': str(e)
                    })
        
        # 综合分析结果
        valid_results = [r for r in model_results if 'error' not in r]
        
        if not valid_results:
            return {
                'error': '所有模型分析均失败',
                'model_results': model_results
            }
        
        # 计算综合评分和建议
        recommendations = [r.get('recommendation', '持有') for r in valid_results]
        scores = [r.get('score', 5) for r in valid_results if isinstance(r.get('score'), (int, float))]
        
        rec_counts = {}
        for rec in recommendations:
            rec_counts[rec] = rec_counts.get(rec, 0) + 1
        
        main_recommendation = max(rec_counts, key=rec_counts.get)
        avg_score = sum(scores) / len(scores) if scores else 5
        consensus_ratio = rec_counts[main_recommendation] / len(valid_results)
        
        # 收集所有理由和风险提示
        all_reasons = []
        all_risks = []
        for result in valid_results:
            reasons = result.get('key_reasons', [])
            if isinstance(reasons, list):
                all_reasons.extend(reasons)
            risks = result.get('risk_warnings', [])
            if isinstance(risks, list):
                all_risks.extend(risks)
        unique_reasons = list(dict.fromkeys(all_reasons))[:7]
        unique_risks = list(dict.fromkeys(all_risks))[:5]
        
        total_time = round(time.time() - start_time, 2)
        
        # 数据维度统计
        data_dimensions = []
        if stock_data.get('technical'):
            data_dimensions.append('技术面')
        if stock_data.get('fundamental'):
            data_dimensions.append('基本面')
        if stock_data.get('capital_flow'):
            data_dimensions.append('资金流')
        if stock_data.get('market_sentiment'):
            data_dimensions.append('市场情绪')
        if stock_data.get('macro'):
            data_dimensions.append('宏观环境')
        if stock_data.get('news'):
            data_dimensions.append('新闻舆情')
        
        return {
            'stock_data': stock_data.get('technical', {}),
            'model_results': model_results,
            'summary': {
                'final_recommendation': main_recommendation,
                'average_score': round(avg_score, 1),
                'consensus_ratio': round(consensus_ratio, 2),
                'total_models': len(models),
                'successful_models': len(valid_results),
                'recommendation_distribution': rec_counts,
                'key_reasons': unique_reasons,
                'risk_warnings': unique_risks,
                'total_execution_time': total_time,
                'data_dimensions': data_dimensions,
                'data_dimensions_count': len(data_dimensions),
            }
        }


if __name__ == "__main__":
    analyzer = MultiModelAnalyzer()
    result = analyzer.multi_model_analysis("000001")
    print(json.dumps(result, ensure_ascii=False, indent=2))
