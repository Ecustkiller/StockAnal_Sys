#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试企业微信推送
"""

import requests
import json
from datetime import datetime

def test_wecom_webhook():
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=1c64aba7-30f9-4bc1-9d7e-5981e23fa3ef"
    
    # 测试消息
    message = f"📊 股票选股系统测试\n\n✅ 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n🚀 系统运行正常！"
    
    # 企业微信机器人消息格式
    data = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    
    try:
        print("正在发送测试消息到企业微信...")
        print(f"消息内容:\n{message}")
        print(f"发送数据: {json.dumps(data, ensure_ascii=False)}")
        
        response = requests.post(
            webhook_url,
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        print(f"\nHTTP状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                print("\n✅ 测试成功！企业微信推送正常工作")
                return True
            else:
                print(f"\n❌ 企业微信API错误: {result}")
                return False
        else:
            print(f"\n❌ HTTP请求失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        return False

if __name__ == '__main__':
    success = test_wecom_webhook()
    exit(0 if success else 1)