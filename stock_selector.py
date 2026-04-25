#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票选股推送系统
功能：
1. 更新最新股票数据
2. 运行选股策略
3. 推送选股结果到企业微信
"""

import os
import sys
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
from typing import List, Dict, Any
import glob

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_selector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockSelector:
    def __init__(self, stock_data_dir="/Users/ecustkiller/stock_data"):
        """
        初始化选股系统
        Args:
            stock_data_dir: 股票数据目录路径
        """
        self.stock_data_dir = stock_data_dir
        self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=1c64aba7-30f9-4bc1-9d7e-5981e23fa3ef"
        
        # 检查数据目录是否存在
        if not os.path.exists(self.stock_data_dir):
            logger.error(f"股票数据目录不存在: {self.stock_data_dir}")
            sys.exit(1)
            
        logger.info(f"股票选股系统初始化完成，数据目录: {self.stock_data_dir}")

    def send_wecom_message(self, message: str) -> bool:
        """
        发送消息到企业微信
        Args:
            message: 要发送的消息内容
        Returns:
            bool: 发送是否成功
        """
        data = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        try:
            logger.info("正在发送消息到企业微信...")
            response = requests.post(
                self.webhook_url,
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            logger.info(f"企业微信推送结果: HTTP {response.status_code}")
            logger.info(f"响应内容: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info("✅ 消息发送成功")
                    return True
                else:
                    logger.error(f"❌ 企业微信API错误: {result}")
                    return False
            else:
                logger.error(f"❌ HTTP请求失败: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 发送消息异常: {e}")
            return False

    def get_stock_list(self) -> List[Dict[str, str]]:
        """
        获取所有股票列表
        Returns:
            List[Dict]: 股票列表，包含股票代码和名称
        """
        stock_files = glob.glob(os.path.join(self.stock_data_dir, "*.csv"))
        stocks = []
        
        for file_path in stock_files:
            filename = os.path.basename(file_path)
            # 解析文件名格式: 股票代码_股票名称.csv
            if '_' in filename:
                parts = filename.replace('.csv', '').split('_', 1)
                if len(parts) == 2:
                    stock_code, stock_name = parts
                    # 只处理A股股票（6位数字代码）
                    if stock_code.isdigit() and len(stock_code) == 6:
                        stocks.append({
                            'code': stock_code,
                            'name': stock_name,
                            'file_path': file_path
                        })
        
        logger.info(f"找到 {len(stocks)} 只A股股票")
        return stocks

    def analyze_stock(self, stock_info: Dict[str, str]) -> Dict[str, Any]:
        """
        分析单只股票
        Args:
            stock_info: 股票信息字典
        Returns:
            Dict: 分析结果
        """
        try:
            # 读取股票数据
            df = pd.read_csv(stock_info['file_path'])
            
            if df.empty or len(df) < 30:
                return {'code': stock_info['code'], 'name': stock_info['name'], 'score': 0, 'reason': '数据不足'}
            
            # 确保数据按日期排序
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # 获取最新数据
            latest = df.iloc[-1]
            prev_20 = df.iloc[-21:-1] if len(df) >= 21 else df.iloc[:-1]
            
            # 转换数据类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 计算技术指标
            close_prices = df['close'].dropna()
            volumes = df['volume'].dropna()
            
            if len(close_prices) < 20 or len(volumes) < 20:
                return {'code': stock_info['code'], 'name': stock_info['name'], 'score': 0, 'reason': '有效数据不足'}
            
            # 计算移动平均线
            ma5 = close_prices.rolling(window=5).mean().iloc[-1]
            ma10 = close_prices.rolling(window=10).mean().iloc[-1]
            ma20 = close_prices.rolling(window=20).mean().iloc[-1]
            
            # 计算成交量平均
            vol_ma5 = volumes.rolling(window=5).mean().iloc[-1]
            vol_ma20 = volumes.rolling(window=20).mean().iloc[-1]
            
            current_price = close_prices.iloc[-1]
            current_volume = volumes.iloc[-1]
            
            # 选股评分逻辑
            score = 0
            reasons = []
            
            # 1. 价格突破20日均线
            if current_price > ma20:
                score += 20
                reasons.append("突破20日均线")
            
            # 2. 短期均线多头排列
            if ma5 > ma10 > ma20:
                score += 15
                reasons.append("均线多头排列")
            
            # 3. 成交量放大
            if current_volume > vol_ma20 * 1.5:
                score += 20
                reasons.append("成交量放大")
            
            # 4. 近期涨幅适中（避免追高）
            if len(close_prices) >= 5:
                recent_change = (current_price - close_prices.iloc[-6]) / close_prices.iloc[-6] * 100
                if 2 <= recent_change <= 8:  # 2%-8%的涨幅
                    score += 15
                    reasons.append(f"近期涨幅适中({recent_change:.1f}%)")
            
            # 5. 价格相对位置
            high_20 = close_prices.rolling(window=20).max().iloc[-1]
            low_20 = close_prices.rolling(window=20).min().iloc[-1]
            position = (current_price - low_20) / (high_20 - low_20) * 100
            
            if 30 <= position <= 80:  # 不在极端位置
                score += 10
                reasons.append(f"价格位置合理({position:.1f}%)")
            
            return {
                'code': stock_info['code'],
                'name': stock_info['name'],
                'score': score,
                'reason': '; '.join(reasons) if reasons else '无明显优势',
                'current_price': current_price,
                'ma20': ma20,
                'volume_ratio': current_volume / vol_ma20 if vol_ma20 > 0 else 0,
                'position': position
            }
            
        except Exception as e:
            logger.error(f"分析股票 {stock_info['code']} 时出错: {e}")
            return {'code': stock_info['code'], 'name': stock_info['name'], 'score': 0, 'reason': f'分析错误: {str(e)}'}

    def select_stocks(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """
        选择推荐股票
        Args:
            top_n: 返回前N只股票
        Returns:
            List[Dict]: 推荐股票列表
        """
        logger.info("开始股票选股分析...")
        
        stocks = self.get_stock_list()
        if not stocks:
            logger.error("没有找到股票数据文件")
            return []
        
        # 分析所有股票
        results = []
        for i, stock in enumerate(stocks[:100]):  # 限制分析前100只股票，避免太慢
            if i % 20 == 0:
                logger.info(f"分析进度: {i+1}/{min(100, len(stocks))}")
            
            result = self.analyze_stock(stock)
            if result['score'] > 30:  # 只保留评分较高的股票
                results.append(result)
        
        # 按评分排序
        results.sort(key=lambda x: x['score'], reverse=True)
        
        logger.info(f"选股分析完成，找到 {len(results)} 只符合条件的股票")
        return results[:top_n]

    def format_recommendation_message(self, selected_stocks: List[Dict[str, Any]]) -> str:
        """
        格式化推荐消息
        Args:
            selected_stocks: 选中的股票列表
        Returns:
            str: 格式化的消息
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if not selected_stocks:
            return f"""📊 每日选股推荐 - {current_time}

❌ 今日暂无符合条件的推荐股票

📈 市场提醒:
• 当前市场可能处于调整期
• 建议保持观望，等待更好机会
• 严格控制风险，理性投资

⚠️ 风险提示: 投资有风险，入市需谨慎"""

        message_lines = [
            f"📊 每日选股推荐 - {current_time}",
            "",
            "🚀 今日推荐股票:"
        ]
        
        for i, stock in enumerate(selected_stocks[:5], 1):  # 只显示前5只
            message_lines.append(
                f"• {stock['code']} {stock['name']} "
                f"(评分:{stock['score']}) - {stock['reason']}"
            )
        
        message_lines.extend([
            "",
            "📈 选股标准:",
            "• 突破20日均线 • 均线多头排列",
            "• 成交量放大 • 涨幅适中",
            "• 价格位置合理",
            "",
            "⚠️ 风险提示: 投资有风险，入市需谨慎",
            "💡 建议: 请结合个人风险承受能力谨慎投资"
        ])
        
        return "\n".join(message_lines)

    def run_selection_and_push(self):
        """
        执行选股并推送
        """
        logger.info("=" * 50)
        logger.info("开始执行每日选股推送任务")
        logger.info("=" * 50)
        
        try:
            # 1. 选股
            selected_stocks = self.select_stocks(top_n=10)
            
            # 2. 格式化消息
            message = self.format_recommendation_message(selected_stocks)
            
            # 3. 推送到企业微信
            success = self.send_wecom_message(message)
            
            if success:
                logger.info("✅ 选股推送任务完成")
            else:
                logger.error("❌ 选股推送任务失败")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 选股推送任务异常: {e}")
            return False

    def test_wecom_connection(self):
        """
        测试企业微信连接
        """
        test_message = f"📊 股票选股系统测试\n\n✅ 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n🚀 系统运行正常！"
        return self.send_wecom_message(test_message)

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python3 stock_selector.py select_only    # 执行选股推送")
        print("  python3 stock_selector.py test          # 测试企业微信推送")
        sys.exit(1)
    
    command = sys.argv[1]
    
    # 根据运行环境调整数据目录
    if os.path.exists("/Users/ecustkiller/stock_data"):
        data_dir = "/Users/ecustkiller/stock_data"  # 本地环境
    elif os.path.exists("/root/StockAnal_Sys/stock_data"):
        data_dir = "/root/StockAnal_Sys/stock_data"  # 服务器环境
    else:
        data_dir = "./stock_data"  # 默认当前目录
    
    selector = StockSelector(stock_data_dir=data_dir)
    
    if command == "select_only":
        # 执行选股推送
        success = selector.run_selection_and_push()
        sys.exit(0 if success else 1)
        
    elif command == "test":
        # 测试企业微信推送
        success = selector.test_wecom_connection()
        if success:
            print("✅ 企业微信推送测试成功")
        else:
            print("❌ 企业微信推送测试失败")
        sys.exit(0 if success else 1)
        
    else:
        print(f"未知命令: {command}")
        sys.exit(1)

if __name__ == '__main__':
    main()