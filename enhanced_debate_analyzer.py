# -*- coding: utf-8 -*-
"""
增强版辩论分析系统 - 基于FinGenius思想优化
创建时间: 2025-09-28
功能: 严格两阶段分离、结构化辩论、动态投票机制
"""

import json
import logging
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from dataclasses import dataclass, field
from stock_analyzer import StockAnalyzer
from enhanced_data_collector import get_collector


class AnalysisPhase(Enum):
    """分析阶段枚举"""
    RESEARCH = "research"      # 研究阶段：深度分析
    DEBATE = "debate"         # 辩论阶段：观点交锋
    CONCLUSION = "conclusion" # 结论阶段：最终决策


class DebateRole(Enum):
    """辩论角色定义"""
    FAST_RESPONDER = "快速响应者"    # qwen2.5:14b - 快速反应，先发制人
    DEEP_THINKER = "深度思考者"      # deepseek-coder:6.7b - 代码与技术深度分析  
    NEUTRAL_JUDGE = "中立裁判"       # qwen2.5:14b - 综合能力，中立观点
    TECH_EXPERT = "技术专家"         # deepseek-coder:6.7b - 技术导向
    COMPREHENSIVE_ANALYST = "综合分析师" # qwen2.5:14b - 综合判断
    FINAL_SUMMARIZER = "总结者"      # qwen2.5:14b - 最后总结


@dataclass
class DebateParticipant:
    """辩论参与者"""
    model_name: str
    role: DebateRole
    company: str
    speaking_order: int
    research_result: Optional[Dict] = None
    debate_speeches: List[Dict] = field(default_factory=list)
    vote_history: List[Dict] = field(default_factory=list)
    final_vote: Optional[str] = None
    confidence_level: float = 0.0


@dataclass
class DebateRound:
    """辩论轮次"""
    round_number: int
    speeches: List[Dict] = field(default_factory=list)
    votes: Dict[str, str] = field(default_factory=dict)
    vote_changes: List[Dict] = field(default_factory=list)
    consensus_level: float = 0.0


class EnhancedDebateAnalyzer:
    """增强版辩论分析器 - 基于FinGenius思想"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_analyzer = StockAnalyzer()
        self.data_collector = get_collector()
        
        # 当前分析阶段
        self.current_phase = AnalysisPhase.RESEARCH
        
        # 辩论参与者配置 - 基于FinGenius的发言顺序设计
        self.participants = [
            DebateParticipant("qwen2.5:14b", DebateRole.FAST_RESPONDER, "Alibaba", 1),
            DebateParticipant("deepseek-coder:6.7b", DebateRole.DEEP_THINKER, "DeepSeek", 2),
            DebateParticipant("qwen2.5:14b", DebateRole.NEUTRAL_JUDGE, "Alibaba", 3),
            DebateParticipant("deepseek-coder:6.7b", DebateRole.TECH_EXPERT, "DeepSeek", 4),
            DebateParticipant("qwen2.5:14b", DebateRole.COMPREHENSIVE_ANALYST, "Alibaba", 5),
            DebateParticipant("qwen2.5:14b", DebateRole.FINAL_SUMMARIZER, "Alibaba", 6),
        ]
        
        # 辩论配置
        self.debate_rounds = 2
        self.disagreement_threshold = 0.6
        self.ollama_url = "http://localhost:11434/v1/chat/completions"
        
        # 辩论状态
        self.debate_history: List[DebateRound] = []
        self.research_results: Dict[str, Any] = {}
        self.final_consensus: Optional[Dict] = None
        
        # 专业化提示词模板
        self.debate_prompts = self._initialize_debate_prompts()
    
    def _initialize_debate_prompts(self) -> Dict[str, str]:
        """初始化专业化辩论提示词"""
        return {
            DebateRole.FAST_RESPONDER.value: """
🚀 你是【快速响应者】- 以敏锐的市场嗅觉著称
你的特长：快速捕捉市场信号，果断表态，引领讨论方向

基于研究数据：{research_summary}

你的任务：
1. 🎯 立即表明立场：看涨(bullish)或看跌(bearish)
2. ⚡ 快速给出3个核心理由（基于研究数据）
3. 🔥 设定讨论基调，为后续专家提供思路

发言要求：
- 简洁有力，直击要害
- 基于数据，不做推测
- 为后续辩论奠定基础

{context_info}
""",
            
            DebateRole.DEEP_THINKER.value: """
🧠 你是【深度思考者】- 以全面分析见长
你的特长：深入挖掘数据背后的逻辑，提供多维度视角

前面专家观点：{previous_opinions}
研究数据：{research_summary}

你的任务：
1. 🔍 深度分析前面专家的观点（支持/质疑）
2. 📊 从多个维度重新审视数据
3. 🎯 给出你的深度判断和理由

发言要求：
- 逻辑严密，论证充分
- 回应前面观点，展现思考深度
- 可以挑战但要有理有据

{context_info}
""",
            
            DebateRole.NEUTRAL_JUDGE.value: """
⚖️ 你是【中立裁判】- 以客观公正著称
你的特长：平衡各方观点，寻找客观真相

当前辩论态势：{debate_summary}
研究数据：{research_summary}

你的任务：
1. ⚖️ 客观评价前面专家的观点
2. 🎯 指出分析中的合理点和薄弱点
3. 📊 基于数据给出你的中立判断

发言要求：
- 保持客观中立立场
- 平衡看待各方观点
- 基于事实和数据说话

{context_info}
""",
            
            DebateRole.TECH_EXPERT.value: """
🔧 你是【技术专家】- 以技术分析专长著称
你的特长：从技术角度解读数据，关注技术指标和模式

技术数据重点：{technical_data}
专家观点汇总：{all_opinions}

你的任务：
1. 📈 从技术角度分析股票走势
2. 🔍 验证或质疑其他专家的技术判断
3. ⚙️ 基于技术指标给出专业意见

发言要求：
- 突出技术分析视角
- 用数据和指标说话
- 补充其他专家遗漏的技术要点

{context_info}
""",
            
            DebateRole.COMPREHENSIVE_ANALYST.value: """
🎯 你是【综合分析师】- 以全局把控能力著称
你的特长：整合多方信息，做出综合判断

完整辩论记录：{full_debate_history}
所有研究数据：{complete_research_data}

你的任务：
1. 🔄 整合所有专家观点和数据
2. ⚖️ 权衡各种因素的重要性
3. 🎯 给出你的综合判断和最终立场

发言要求：
- 体现全局思维和综合能力
- 整合前面所有有价值的观点
- 为最终决策提供重要参考

{context_info}
""",
            
            DebateRole.FINAL_SUMMARIZER.value: """
📋 你是【总结者】- 负责最终总结和决策
你的特长：总结归纳，得出最终结论

完整辩论过程：{complete_debate}
投票分布情况：{vote_distribution}

你的任务：
1. 📊 总结主要观点和分歧点
2. 🎯 分析投票分布和变化趋势
3. ⚖️ 给出最终总结性判断

发言要求：
- 全面总结辩论过程
- 指出关键分歧和共识
- 为最终决策画下句号

{context_info}
"""
        }
    
    def get_stock_basic_data(self, stock_code: str, market_type: str = 'A') -> Dict:
        """获取股票全面数据用于分析（增强版）"""
        try:
            # 使用增强版数据收集器获取多维度数据
            comprehensive_data = self.data_collector.collect_comprehensive_data(stock_code, market_type)
            formatted_text = self.data_collector.format_data_for_ai(comprehensive_data)
            
            # 保持兼容性，同时提供格式化文本
            tech = comprehensive_data.get('technical', {})
            result = {
                'stock_code': stock_code,
                'current_price': tech.get('current_price', 0),
                'change_pct': tech.get('change_pct', 0),
                'volume': tech.get('volume', 0),
                'rsi': tech.get('rsi', 50),
                'macd': tech.get('macd', 0),
                'ma5': tech.get('ma5', 0),
                'ma20': tech.get('ma20', 0),
                'bollinger_upper': tech.get('bollinger_upper', 0),
                'bollinger_lower': tech.get('bollinger_lower', 0),
                'atr': tech.get('atr', 0),
                'volume_ratio': tech.get('volume_ratio', 1),
                # 增强数据
                'formatted_text': formatted_text,
                'comprehensive_data': comprehensive_data,
            }
            return result
            
        except Exception as e:
            self.logger.error(f"获取股票全面数据失败: {e}")
            return {}
    
    async def research_phase(self, stock_code: str, market_type: str = 'A') -> Dict[str, Any]:
        """第一阶段：研究阶段 - 所有模型并行深度分析"""
        self.logger.info("🔬 开始研究阶段 - 多模型并行深度分析")
        self.current_phase = AnalysisPhase.RESEARCH
        
        # 获取基础数据
        stock_data = self.get_stock_basic_data(stock_code, market_type)
        if not stock_data:
            return {'error': '无法获取股票数据'}
        
        # 并行调用所有模型进行深度分析
        research_results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_participant = {
                executor.submit(self._conduct_research_analysis, participant, stock_data): participant
                for participant in self.participants
            }
            
            for future in as_completed(future_to_participant, timeout=300):
                participant = future_to_participant[future]
                try:
                    result = future.result()
                    if result and 'error' not in result:
                        participant.research_result = result
                        research_results[participant.model_name] = result
                        self.logger.info(f"✅ {participant.model_name} 研究完成")
                    else:
                        error_msg = result.get('error', '未知错误') if result else '返回结果为空'
                        self.logger.warning(f"⚠️ {participant.model_name} 研究返回错误: {error_msg}")
                        participant.research_result = {'error': error_msg}
                        research_results[participant.model_name] = {'error': error_msg}
                except Exception as e:
                    self.logger.error(f"❌ {participant.model_name} 研究失败: {e}")
                    participant.research_result = {'error': str(e)}
                    research_results[participant.model_name] = {'error': str(e)}
        
        self.research_results = {
            'stock_data': stock_data,
            'individual_results': research_results,
            'research_summary': self._generate_research_summary(research_results)
        }
        
        self.logger.info(f"🔬 研究阶段完成，{len([r for r in research_results.values() if 'error' not in r])}/{len(self.participants)} 个模型成功")
        return self.research_results
    
    def _conduct_research_analysis(self, participant: DebateParticipant, stock_data: Dict) -> Dict:
        """为单个参与者进行研究分析"""
        # 使用增强版多维度数据
        formatted_text = stock_data.get('formatted_text', '')
        if not formatted_text:
            formatted_text = f"股票代码: {stock_data.get('stock_code', 'N/A')}, 当前价格: {stock_data.get('current_price', 0):.2f}"
        
        research_prompt = f"""
你是一位拥有20年经验的顶级股票分析师，精通技术分析、基本面分析、资金流分析和宏观经济分析。
请基于以下全面的多维度数据，对股票进行深度研究分析：

{formatted_text}

请从以下维度进行深度分析：
1. 技术面分析（趋势方向、支撑压力位、关键技术指标信号）
2. 基本面分析（估值水平、盈利能力、成长性、财务健康）
3. 资金面分析（主力资金动向、北向资金、量能变化）
4. 市场环境分析（市场情绪、行业热度、宏观环境）
5. 风险因素识别（技术风险、基本面风险、市场风险）
6. 投资机会评估（短中长期机会）

请以JSON格式回复：
{{
    "model_name": "{participant.model_name}",
    "technical_analysis": "详细技术面分析",
    "fundamental_analysis": "基本面分析",
    "capital_flow_analysis": "资金面分析",
    "market_sentiment": "市场情绪和环境分析", 
    "risk_assessment": "风险评估",
    "opportunity_analysis": "机会分析",
    "preliminary_view": "初步观点(bullish/bearish/neutral)",
    "confidence": 置信度(0.0-1.0),
    "key_factors": ["关键因素1", "关键因素2", "关键因素3", "关键因素4", "关键因素5"]
}}
"""
        
        return self._call_model_sync(participant.model_name, research_prompt, timeout=90)
    
    async def debate_phase(self) -> Dict[str, Any]:
        """第二阶段：辩论阶段 - 结构化多轮辩论"""
        self.logger.info("🗣️ 开始辩论阶段 - 结构化多轮辩论")
        self.current_phase = AnalysisPhase.DEBATE
        
        # 检查研究结果
        if not self.research_results:
            return {'error': '缺少研究阶段数据'}
        
        # 初始分歧检测
        initial_disagreement = self._analyze_initial_disagreement()
        
        if not initial_disagreement['has_disagreement']:
            self.logger.info("🤝 初步分析显示观点一致，进行简化辩论")
            self.debate_rounds = 1
        
        # 进行多轮结构化辩论
        for round_num in range(1, self.debate_rounds + 1):
            self.logger.info(f"🎤 第{round_num}轮辩论开始")
            debate_round = await self._conduct_debate_round(round_num)
            self.debate_history.append(debate_round)
        
        return {
            'initial_disagreement': initial_disagreement,
            'debate_rounds': len(self.debate_history),
            'debate_history': [self._serialize_debate_round(r) for r in self.debate_history],
            'final_vote_distribution': self._get_final_vote_distribution()
        }
    
    async def _conduct_debate_round(self, round_num: int) -> DebateRound:
        """进行单轮辩论"""
        debate_round = DebateRound(round_number=round_num)
        
        # 按发言顺序让每个参与者发言
        for participant in sorted(self.participants, key=lambda p: p.speaking_order):
            if participant.research_result and 'error' not in participant.research_result:
                self.logger.info(f"🎤 {participant.model_name} ({participant.role.value}) 发言中...")
                
                # 生成个性化辩论提示
                debate_prompt = self._generate_debate_prompt(participant, round_num)
                
                # 获取发言
                speech_result = self._call_model_sync(participant.model_name, debate_prompt, timeout=60)
                
                if 'error' not in speech_result:
                    # 记录发言
                    speech = {
                        'speaker': participant.model_name,
                        'role': participant.role.value,
                        'round': round_num,
                        'content': speech_result.get('speech', ''),
                        'stance': speech_result.get('stance', 'neutral'),
                        'key_points': speech_result.get('key_points', []),
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    debate_round.speeches.append(speech)
                    participant.debate_speeches.append(speech)
                    
                    # 记录投票
                    vote = speech_result.get('vote', 'neutral')
                    if vote in ['bullish', 'bearish', 'neutral']:
                        debate_round.votes[participant.model_name] = vote
                        
                        # 检查投票变化
                        if participant.final_vote and participant.final_vote != vote:
                            change = {
                                'model': participant.model_name,
                                'from': participant.final_vote,
                                'to': vote,
                                'round': round_num,
                                'reason': 'debate_influence'
                            }
                            debate_round.vote_changes.append(change)
                        
                        participant.final_vote = vote
                        participant.vote_history.append({
                            'round': round_num,
                            'vote': vote,
                            'timestamp': datetime.now().isoformat()
                        })
                    
                    self.logger.info(f"✅ {participant.model_name} 发言完成: {vote}")
                else:
                    self.logger.error(f"❌ {participant.model_name} 发言失败")
        
        # 计算本轮共识程度
        debate_round.consensus_level = self._calculate_consensus_level(debate_round.votes)
        
        return debate_round
    
    def _generate_debate_prompt(self, participant: DebateParticipant, round_num: int) -> str:
        """为参与者生成个性化辩论提示"""
        base_template = self.debate_prompts.get(participant.role.value, "")
        
        # 准备上下文信息
        context_data = {
            'research_summary': self._format_research_summary(participant.research_result),
            'previous_opinions': self._get_previous_opinions(participant.speaking_order),
            'debate_summary': self._get_debate_summary(),
            'technical_data': self._extract_technical_data(),
            'all_opinions': self._get_all_current_opinions(),
            'full_debate_history': self._format_full_debate_history(),
            'complete_research_data': self._format_complete_research_data(),
            'complete_debate': self._format_complete_debate(),
            'vote_distribution': self._get_current_vote_distribution(),
            'context_info': f"""
当前轮次: 第{round_num}轮
你的角色: {participant.role.value}
发言顺序: 第{participant.speaking_order}位

请以JSON格式回复：
{{
    "speech": "你的发言内容",
    "stance": "你的立场(bullish/bearish/neutral)",
    "key_points": ["要点1", "要点2", "要点3"],
    "vote": "你的投票(bullish/bearish/neutral)",
    "confidence": 置信度(0.0-1.0)
}}
"""
        }
        
        # 格式化模板
        try:
            return base_template.format(**context_data)
        except KeyError as e:
            self.logger.warning(f"模板格式化缺少参数: {e}")
            return base_template
    
    async def conclusion_phase(self) -> Dict[str, Any]:
        """第三阶段：结论阶段 - 生成最终决策"""
        self.logger.info("⚖️ 开始结论阶段 - 生成最终决策")
        self.current_phase = AnalysisPhase.CONCLUSION
        
        # 分析最终投票分布
        final_votes = self._get_final_vote_distribution()
        
        # 计算共识程度 - 修复逻辑
        valid_votes = {k: v for k, v in final_votes.items() if v and v != 'neutral'}
        total_participants = len([v for v in final_votes.values() if v])  # 实际参与的模型数
        
        if not valid_votes:
            # 如果没有明确的看涨/看跌投票，检查是否都是中性
            neutral_votes = len([v for v in final_votes.values() if v == 'neutral'])
            if neutral_votes > 0:
                consensus_ratio = neutral_votes / total_participants if total_participants > 0 else 0.0
                main_decision = 'neutral'
            else:
                consensus_ratio = 0.0
                main_decision = 'neutral'
        else:
            vote_counts = {}
            for vote in valid_votes.values():
                vote_counts[vote] = vote_counts.get(vote, 0) + 1
            
            main_decision = max(vote_counts, key=vote_counts.get)
            consensus_ratio = vote_counts.get(main_decision, 0) / total_participants if total_participants > 0 else 0.0
        
        # 将英文投票转换为中文建议
        decision_map = {
            'bullish': '看涨',
            'bearish': '看跌', 
            'neutral': '中性'
        }
        chinese_decision = decision_map.get(main_decision, '未知')
        
        # 生成最终结论 - 优化逻辑
        if main_decision == 'neutral':
            if consensus_ratio >= 0.8:
                decision_confidence = "高"
                decision_text = f"一致认为【{chinese_decision}】"
            elif consensus_ratio >= 0.6:
                decision_confidence = "中"
                decision_text = f"倾向于【{chinese_decision}】"
            else:
                decision_confidence = "低"
                decision_text = f"观点分歧较大，暂时【{chinese_decision}】"
        else:
            if consensus_ratio >= 0.8:
                decision_confidence = "高"
                decision_text = f"强烈建议【{chinese_decision}】"
            elif consensus_ratio >= 0.6:
                decision_confidence = "中"
                decision_text = f"建议【{chinese_decision}】"
            else:
                decision_confidence = "低"
                decision_text = f"谨慎建议【{chinese_decision}】（存在分歧）"
        
        # 分析投票变化
        vote_changes = []
        for round_data in self.debate_history:
            vote_changes.extend(round_data.vote_changes)
        
        self.final_consensus = {
            'final_decision': main_decision,
            'decision_text': decision_text,
            'confidence_level': decision_confidence,
            'consensus_ratio': round(consensus_ratio, 2),
            'vote_distribution': final_votes,
            'vote_changes': vote_changes,
            'total_participants': len(self.participants),
            'successful_participants': total_participants,  # 使用修正后的参与者数量
            'debate_effectiveness': len(vote_changes) > 0,  # 是否有观点变化
            'failed_participants': len(self.participants) - total_participants,  # 失败的参与者数量
        }
        
        return self.final_consensus
    
    def _call_model_sync(self, model_name: str, prompt: str, timeout: int = 60) -> Dict:
        """调用单个模型"""
        try:
            payload = {
                "model": model_name,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": 1000
            }
            
            start_time = time.time()
            response = requests.post(self.ollama_url, json=payload, timeout=timeout)
            analysis_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # 尝试解析JSON
                try:
                    start_idx = content.find('{')
                    end_idx = content.rfind('}') + 1
                    if start_idx != -1 and end_idx != 0:
                        json_str = content[start_idx:end_idx]
                        parsed_result = json.loads(json_str)
                        parsed_result['analysis_time'] = round(analysis_time, 2)
                        parsed_result['model_name'] = model_name
                        return parsed_result
                except json.JSONDecodeError:
                    pass
                
                return {
                    'model_name': model_name,
                    'content': content,
                    'analysis_time': round(analysis_time, 2),
                    'raw_response': True
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
    
    # 辅助方法
    def _analyze_initial_disagreement(self) -> Dict:
        """分析初始分歧程度"""
        preliminary_views = []
        for participant in self.participants:
            if participant.research_result and 'error' not in participant.research_result:
                view = participant.research_result.get('preliminary_view', 'neutral')
                preliminary_views.append(view)
        
        if not preliminary_views:
            return {'has_disagreement': False, 'disagreement_level': 0.0}
        
        view_counts = {}
        for view in preliminary_views:
            view_counts[view] = view_counts.get(view, 0) + 1
        
        total = len(preliminary_views)
        max_consensus = max(view_counts.values())
        consensus_ratio = max_consensus / total
        disagreement_level = 1 - consensus_ratio
        
        return {
            'has_disagreement': disagreement_level >= (1 - self.disagreement_threshold),
            'disagreement_level': round(disagreement_level, 2),
            'consensus_ratio': round(consensus_ratio, 2),
            'view_distribution': view_counts
        }
    
    def _get_final_vote_distribution(self) -> Dict[str, str]:
        """获取最终投票分布"""
        return {p.model_name: p.final_vote or 'neutral' for p in self.participants}
    
    def _calculate_consensus_level(self, votes: Dict[str, str]) -> float:
        """计算共识程度"""
        if not votes:
            return 0.0
        
        vote_counts = {}
        for vote in votes.values():
            vote_counts[vote] = vote_counts.get(vote, 0) + 1
        
        max_count = max(vote_counts.values())
        return max_count / len(votes)
    
    # 格式化方法（简化实现）
    def _format_research_summary(self, research_result: Dict) -> str:
        if not research_result or 'error' in research_result:
            return "研究数据不可用"
        tech_analysis = research_result.get('technical_analysis', 'N/A')
        if isinstance(tech_analysis, str) and len(tech_analysis) > 100:
            return f"技术分析: {tech_analysis[:100]}..."
        return f"技术分析: {tech_analysis}"
    
    def _get_previous_opinions(self, current_order: int) -> str:
        opinions = []
        for participant in self.participants:
            if participant.speaking_order < current_order and participant.debate_speeches:
                latest_speech = participant.debate_speeches[-1]
                content = latest_speech.get('content', '')
                if isinstance(content, str) and len(content) > 100:
                    content = content[:100] + "..."
                opinions.append(f"{participant.role.value}: {content}")
        return "\n".join(opinions) if opinions else "暂无前面专家观点"
    
    def _get_debate_summary(self) -> str:
        if not self.debate_history:
            return "辩论尚未开始"
        return f"已进行{len(self.debate_history)}轮辩论"
    
    def _extract_technical_data(self) -> str:
        if not self.research_results.get('stock_data'):
            return "技术数据不可用"
        data = self.research_results['stock_data']
        return f"RSI: {data.get('rsi', 0):.2f}, MACD: {data.get('macd', 0):.4f}"
    
    def _get_all_current_opinions(self) -> str:
        opinions = []
        for participant in self.participants:
            if participant.debate_speeches:
                latest = participant.debate_speeches[-1]
                opinions.append(f"{participant.model_name}: {latest.get('stance', 'neutral')}")
        return "; ".join(opinions) if opinions else "暂无观点"
    
    def _format_full_debate_history(self) -> str:
        return f"共{len(self.debate_history)}轮辩论，{sum(len(r.speeches) for r in self.debate_history)}次发言"
    
    def _format_complete_research_data(self) -> str:
        return f"研究阶段数据：{len(self.research_results.get('individual_results', {}))}个模型完成分析"
    
    def _format_complete_debate(self) -> str:
        return self._format_full_debate_history()
    
    def _get_current_vote_distribution(self) -> str:
        votes = self._get_final_vote_distribution()
        vote_counts = {}
        for vote in votes.values():
            vote_counts[vote] = vote_counts.get(vote, 0) + 1
        return str(vote_counts)
    
    def _generate_research_summary(self, research_results: Dict) -> str:
        successful = len([r for r in research_results.values() if 'error' not in r])
        return f"{successful}/{len(research_results)} 个模型成功完成研究"
    
    def _serialize_debate_round(self, round_data: DebateRound) -> Dict:
        return {
            'round_number': round_data.round_number,
            'speeches_count': len(round_data.speeches),
            'votes': round_data.votes,
            'vote_changes': round_data.vote_changes,
            'consensus_level': round_data.consensus_level
        }
    
    async def enhanced_analysis(self, stock_code: str, market_type: str = 'A') -> Dict[str, Any]:
        """完整的增强版分析流程"""
        start_time = time.time()
        
        try:
            # 第一阶段：研究
            research_result = await self.research_phase(stock_code, market_type)
            if 'error' in research_result:
                return research_result
            
            # 第二阶段：辩论
            debate_result = await self.debate_phase()
            if 'error' in debate_result:
                return debate_result
            
            # 第三阶段：结论
            conclusion_result = await self.conclusion_phase()
            
            # 组装完整结果
            total_time = time.time() - start_time
            
            return {
                'stock_code': stock_code,
                'analysis_metadata': {
                    'total_execution_time': round(total_time, 2),
                    'phases_completed': 3,
                    'participants': len(self.participants),
                    'debate_rounds': len(self.debate_history)
                },
                'research_phase': research_result,
                'debate_phase': debate_result,
                'conclusion_phase': conclusion_result,
                'enhanced_features': {
                    'two_phase_separation': True,
                    'structured_debate': True,
                    'dynamic_voting': True,
                    'role_specialization': True
                }
            }
            
        except Exception as e:
            self.logger.error(f"增强版分析失败: {e}")
            return {
                'error': str(e),
                'execution_time': time.time() - start_time
            }


# 测试函数
if __name__ == "__main__":
    import asyncio
    
    async def test_enhanced_analyzer():
        analyzer = EnhancedDebateAnalyzer()
        result = await analyzer.enhanced_analysis("000001")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(test_enhanced_analyzer())
