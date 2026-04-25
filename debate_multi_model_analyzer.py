# -*- coding: utf-8 -*-
"""
带辩论机制的多模型协作股票分析系统
创建时间: 2025-09-28
功能: 多个AI模型分析股票，当有分歧时进行辩论，最终给出确定性答案
优化: 整合多维度数据源（技术面+基本面+资金流+市场情绪+宏观环境+新闻），大幅提升分析深度
"""

import json
import logging
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from stock_analyzer import StockAnalyzer
from enhanced_data_collector import get_collector


class DebateMultiModelAnalyzer:
    """带辩论机制的多模型协作分析器 - 增强数据版"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_analyzer = StockAnalyzer()
        self.data_collector = get_collector()
        
        # 不同公司的模型列表（仅保留本地已安装的模型）
        self.available_models = {
            "alibaba": ["qwen2.5:14b"],
            "deepseek": ["deepseek-coder:6.7b"]
        }
        
        # 所有模型的平铺列表
        self.all_models = []
        for company_models in self.available_models.values():
            self.all_models.extend(company_models)
        
        # Ollama API配置
        self.ollama_url = "http://localhost:11434/v1/chat/completions"
        
        # 辩论配置
        self.debate_rounds = 2  # 辩论轮次
        self.disagreement_threshold = 0.6  # 分歧阈值，低于此值触发辩论
        
    def get_stock_comprehensive_data(self, stock_code: str, market_type: str = 'A') -> Dict:
        """获取股票全面数据用于AI分析（增强版）"""
        try:
            comprehensive_data = self.data_collector.collect_comprehensive_data(stock_code, market_type)
            formatted_text = self.data_collector.format_data_for_ai(comprehensive_data)
            comprehensive_data['formatted_text'] = formatted_text
            return comprehensive_data
        except Exception as e:
            self.logger.error(f"获取股票全面数据失败: {e}")
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
    
    def call_model_analysis(self, model_name: str, stock_data: Dict, context: str = "", timeout: int = 120) -> Dict:
        """调用单个模型进行分析（增强版prompt）"""
        try:
            formatted_text = stock_data.get('formatted_text', '数据不可用')
            stock_code = stock_data.get('stock_code', 'N/A')
            
            prompt = f"""
你是一位拥有20年经验的顶级股票分析师，精通技术分析、基本面分析、资金流分析和宏观经济分析。
请基于以下全面的多维度数据，对股票进行深度分析并给出投资建议。

{formatted_text}

{context}

═══════════════════════════════════════
请从以下维度进行深度分析：

1. 【技术面综合判断】趋势方向、关键支撑压力位、短期技术信号
2. 【基本面价值评估】估值水平、盈利能力、成长性、财务健康
3. 【资金面分析】主力资金动向、北向资金态度、量能变化
4. 【市场环境评估】市场情绪、行业热度、宏观环境影响
5. 【综合投资建议】操作建议、评分、风险等级、止损目标位

请以JSON格式回复：
{{
    "model_name": "{model_name}",
    "technical_analysis": "技术面综合分析",
    "fundamental_analysis": "基本面分析",
    "capital_flow_analysis": "资金面分析",
    "market_environment": "市场环境评估",
    "recommendation": "买入/持有/卖出",
    "score": 分数(1-10),
    "risk_level": "高/中/低",
    "key_reasons": ["核心理由1", "核心理由2", "核心理由3", "核心理由4", "核心理由5"],
    "risk_warnings": ["风险提示1", "风险提示2", "风险提示3"],
    "confidence": 置信度(0.0-1.0)
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
                
                # 尝试解析JSON
                try:
                    # 提取JSON部分
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
                
                # 如果JSON解析失败，返回基本结构
                return {
                    'model_name': model_name,
                    'technical_analysis': content,
                    'recommendation': '持有',
                    'score': 5,
                    'risk_level': '中',
                    'key_reasons': ['分析内容解析失败'],
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
    
    def analyze_disagreement(self, model_results: List[Dict]) -> Dict:
        """分析模型间的分歧程度"""
        valid_results = [r for r in model_results if 'error' not in r]
        
        if len(valid_results) < 2:
            return {
                'has_disagreement': False,
                'disagreement_level': 0.0,
                'conflicting_views': []
            }
        
        # 统计投资建议
        recommendations = [r.get('recommendation', '持有') for r in valid_results]
        rec_counts = {}
        for rec in recommendations:
            rec_counts[rec] = rec_counts.get(rec, 0) + 1
        
        # 计算分歧程度
        total_models = len(valid_results)
        max_consensus = max(rec_counts.values())
        consensus_ratio = max_consensus / total_models
        disagreement_level = 1 - consensus_ratio
        
        # 识别冲突观点
        conflicting_views = []
        if disagreement_level >= (1 - self.disagreement_threshold):
            for rec, count in rec_counts.items():
                if count < max_consensus:
                    supporting_models = [r['model_name'] for r in valid_results if r.get('recommendation') == rec]
                    conflicting_views.append({
                        'recommendation': rec,
                        'supporting_models': supporting_models,
                        'count': count
                    })
        
        return {
            'has_disagreement': disagreement_level >= (1 - self.disagreement_threshold),
            'disagreement_level': round(disagreement_level, 2),
            'consensus_ratio': round(consensus_ratio, 2),
            'main_recommendation': max(rec_counts, key=rec_counts.get),
            'conflicting_views': conflicting_views,
            'recommendation_distribution': rec_counts
        }
    
    def conduct_debate(self, stock_data: Dict, initial_results: List[Dict], disagreement_info: Dict) -> Dict:
        """进行模型间辩论（增强版：包含多维度数据上下文）"""
        self.logger.info(f"检测到分歧，开始辩论机制，分歧程度: {disagreement_info['disagreement_level']}")
        
        valid_results = [r for r in initial_results if 'error' not in r]
        
        # 准备辩论上下文 - 增强版包含各维度分析
        debate_context = "以下是其他分析师的观点，请考虑这些观点并坚持或调整你的分析：\n\n"
        
        for i, result in enumerate(valid_results, 1):
            debate_context += f"分析师{i} ({result['model_name']}):\n"
            debate_context += f"- 建议: {result.get('recommendation', '未知')}\n"
            debate_context += f"- 评分: {result.get('score', 0)}/10\n"
            if result.get('technical_analysis'):
                debate_context += f"- 技术面: {str(result['technical_analysis'])[:150]}\n"
            if result.get('fundamental_analysis'):
                debate_context += f"- 基本面: {str(result['fundamental_analysis'])[:150]}\n"
            if result.get('capital_flow_analysis'):
                debate_context += f"- 资金面: {str(result['capital_flow_analysis'])[:150]}\n"
            debate_context += f"- 核心理由: {', '.join(result.get('key_reasons', []))}\n"
            debate_context += f"- 风险提示: {', '.join(result.get('risk_warnings', []))}\n\n"
        
        debate_context += "请在充分考虑上述观点后，重新分析并给出你的最终判断。如果你改变了观点，请说明原因。"
        
        # 进行辩论轮次
        debate_results = []
        
        for round_num in range(1, self.debate_rounds + 1):
            self.logger.info(f"进行第{round_num}轮辩论")
            round_results = []
            
            # 让每个模型重新分析
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_model = {}
                for result in valid_results:
                    model_name = result['model_name']
                    future = executor.submit(
                        self.call_model_analysis, 
                        model_name, 
                        stock_data, 
                        debate_context,
                        120  # 增加超时时间
                    )
                    future_to_model[future] = model_name
                
                for future in as_completed(future_to_model, timeout=300):
                    model_name = future_to_model[future]
                    try:
                        result = future.result()
                        result['debate_round'] = round_num
                        round_results.append(result)
                        self.logger.info(f"第{round_num}轮辩论: {model_name} 完成")
                    except Exception as e:
                        self.logger.error(f"第{round_num}轮辩论: {model_name} 失败: {e}")
                        round_results.append({
                            'model_name': model_name,
                            'error': str(e),
                            'debate_round': round_num
                        })
            
            debate_results.append({
                'round': round_num,
                'results': round_results
            })
            
            # 更新辩论上下文为最新结果
            if round_num < self.debate_rounds:
                debate_context = f"经过第{round_num}轮讨论，以下是更新后的观点：\n\n"
                for result in round_results:
                    if 'error' not in result:
                        debate_context += f"{result['model_name']}:\n"
                        debate_context += f"- 建议: {result.get('recommendation', '未知')}\n"
                        debate_context += f"- 评分: {result.get('score', 0)}/10\n"
                        debate_context += f"- 理由: {', '.join(result.get('key_reasons', []))}\n\n"
                
                debate_context += f"请继续第{round_num + 1}轮讨论，进一步完善你的分析。"
        
        return {
            'debate_conducted': True,
            'total_rounds': self.debate_rounds,
            'debate_results': debate_results
        }
    
    def generate_final_decision(self, stock_data: Dict, initial_results: List[Dict], 
                              debate_info: Optional[Dict] = None) -> Dict:
        """生成最终确定性决策"""
        
        # 使用辩论后的结果或初始结果
        if debate_info and debate_info.get('debate_conducted'):
            final_round = debate_info['debate_results'][-1]['results']
            analysis_results = [r for r in final_round if 'error' not in r]
        else:
            analysis_results = [r for r in initial_results if 'error' not in r]
        
        if not analysis_results:
            return {
                'final_decision': '无法做出决策',
                'confidence_level': 0.0,
                'reasoning': '所有模型分析失败'
            }
        
        # 重新计算共识
        recommendations = [r.get('recommendation', '持有') for r in analysis_results]
        scores = [r.get('score', 5) for r in analysis_results if isinstance(r.get('score'), (int, float))]
        confidences = [r.get('confidence', 0.5) for r in analysis_results if isinstance(r.get('confidence'), (int, float))]
        
        rec_counts = {}
        for rec in recommendations:
            rec_counts[rec] = rec_counts.get(rec, 0) + 1
        
        total_models = len(analysis_results)
        main_recommendation = max(rec_counts, key=rec_counts.get)
        consensus_ratio = rec_counts[main_recommendation] / total_models
        avg_score = sum(scores) / len(scores) if scores else 5
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.5
        
        # 生成确定性决策
        if consensus_ratio >= 0.8:
            decision_confidence = "高"
            decision_text = f"强烈建议【{main_recommendation}】"
        elif consensus_ratio >= 0.6:
            decision_confidence = "中"
            decision_text = f"建议【{main_recommendation}】"
        else:
            decision_confidence = "低"
            if main_recommendation in ["买入", "卖出"]:
                decision_text = "建议【持有】（由于模型间存在分歧，采取保守策略）"
                main_recommendation = "持有"
            else:
                decision_text = f"谨慎建议【{main_recommendation}】"
        
        # 收集所有理由和风险提示
        all_reasons = []
        all_risks = []
        for result in analysis_results:
            reasons = result.get('key_reasons', [])
            if isinstance(reasons, list):
                all_reasons.extend(reasons)
            risks = result.get('risk_warnings', [])
            if isinstance(risks, list):
                all_risks.extend(risks)
        
        unique_reasons = list(dict.fromkeys(all_reasons))[:7]
        unique_risks = list(dict.fromkeys(all_risks))[:5]
        
        return {
            'final_decision': main_recommendation,
            'decision_text': decision_text,
            'confidence_level': decision_confidence,
            'consensus_ratio': round(consensus_ratio, 2),
            'average_score': round(avg_score, 1),
            'average_confidence': round(avg_confidence, 2),
            'supporting_models': rec_counts[main_recommendation],
            'total_models': total_models,
            'key_supporting_reasons': unique_reasons,
            'risk_warnings': unique_risks,
            'recommendation_distribution': rec_counts
        }
    
    def debate_analysis(self, stock_code: str, market_type: str = 'A', 
                       models: Optional[List[str]] = None, max_workers: int = 4) -> Dict:
        """
        带辩论机制的多模型分析主函数（增强版）
        """
        start_time = time.time()
        
        if models is None:
            models = self.all_models
        else:
            models = [m for m in models if m in self.all_models]
        
        if not models:
            return {'error': '没有可用的模型'}
        
        self.logger.info(f"开始增强版辩论分析: {stock_code}, 使用模型: {models}")
        
        # 获取全面的股票数据（增强版）
        stock_data = self.get_stock_comprehensive_data(stock_code, market_type)
        if not stock_data:
            return {'error': '无法获取股票数据'}
        
        # 第一轮：初始分析
        self.logger.info("进行初始多模型分析")
        initial_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_model = {
                executor.submit(self.call_model_analysis, model, stock_data): model 
                for model in models
            }
            
            for future in as_completed(future_to_model, timeout=300):
                model_name = future_to_model[future]
                try:
                    result = future.result()
                    initial_results.append(result)
                    self.logger.info(f"初始分析: {model_name} 完成")
                except Exception as e:
                    self.logger.error(f"初始分析: {model_name} 失败: {e}")
                    initial_results.append({
                        'model_name': model_name,
                        'error': str(e)
                    })
        
        # 分析分歧程度
        disagreement_info = self.analyze_disagreement(initial_results)
        
        # 如果存在分歧，进行辩论
        debate_info = None
        if disagreement_info['has_disagreement']:
            self.logger.info(f"检测到分歧（程度: {disagreement_info['disagreement_level']}），启动辩论机制")
            debate_info = self.conduct_debate(stock_data, initial_results, disagreement_info)
        else:
            self.logger.info("模型间达成共识，无需辩论")
        
        # 生成最终决策
        final_decision = self.generate_final_decision(stock_data, initial_results, debate_info)
        
        # 数据维度统计
        data_dimensions = []
        for dim in ['technical', 'fundamental', 'capital_flow', 'market_sentiment', 'macro', 'news']:
            if stock_data.get(dim):
                data_dimensions.append(dim)
        
        # 组装完整结果
        complete_result = {
            'stock_data': stock_data.get('technical', {}),
            'analysis_metadata': {
                'total_models': len(models),
                'successful_initial_analysis': len([r for r in initial_results if 'error' not in r]),
                'disagreement_detected': disagreement_info['has_disagreement'],
                'debate_conducted': debate_info is not None,
                'total_execution_time': round(time.time() - start_time, 2),
                'data_dimensions': data_dimensions,
                'data_dimensions_count': len(data_dimensions),
            },
            'initial_analysis': {
                'results': initial_results,
                'disagreement_info': disagreement_info
            },
            'debate_process': debate_info,
            'final_decision': final_decision,
            'model_companies': {
                company: [m for m in models if m in company_models]
                for company, company_models in self.available_models.items()
            }
        }
        
        self.logger.info(f"增强版辩论分析完成: {stock_code}, 总耗时: {complete_result['analysis_metadata']['total_execution_time']}秒")
        
        return complete_result


# 测试函数
if __name__ == "__main__":
    analyzer = DebateMultiModelAnalyzer()
    result = analyzer.debate_analysis("000001")
    print(json.dumps(result, ensure_ascii=False, indent=2))
