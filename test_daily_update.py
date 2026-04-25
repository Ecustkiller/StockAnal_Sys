#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试每日股票数据更新脚本
用于验证更新功能是否正常工作
"""

import sys
import os
import subprocess
from datetime import datetime

def test_update():
    """测试数据更新功能"""
    print("=" * 50)
    print("🧪 测试每日股票数据更新功能")
    print("=" * 50)
    
    # 检查脚本是否存在
    script_path = "/root/StockAnal_Sys/update_daily_stock_data.py"
    if not os.path.exists(script_path):
        print(f"❌ 更新脚本不存在: {script_path}")
        return False
    
    # 检查数据目录是否存在
    data_dir = "/root/StockAnal_Sys/stock_data"
    if not os.path.exists(data_dir):
        print(f"❌ 数据目录不存在: {data_dir}")
        return False
    
    # 统计现有文件数量
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    print(f"📊 当前股票数据文件数量: {len(csv_files)}")
    
    if len(csv_files) == 0:
        print("⚠️ 警告: 没有找到股票数据文件，请先运行全量下载")
        return False
    
    print(f"⏰ 开始测试更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 30)
    
    try:
        # 执行更新脚本
        result = subprocess.run([
            'python3', script_path
        ], capture_output=True, text=True, cwd="/root/StockAnal_Sys")
        
        print("📝 脚本输出:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ 错误输出:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("✅ 测试完成，更新脚本运行正常")
            return True
        else:
            print(f"❌ 测试失败，退出码: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        return False

def test_cron_setup():
    """测试定时任务设置"""
    print("\n" + "=" * 50)
    print("🕒 检查定时任务设置")
    print("=" * 50)
    
    try:
        # 检查crontab
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        
        if result.returncode == 0:
            cron_content = result.stdout
            if 'update_daily_stock_data.py' in cron_content:
                print("✅ 定时任务已正确设置")
                print("\n📅 相关定时任务:")
                for line in cron_content.split('\n'):
                    if 'update_daily_stock_data.py' in line:
                        print(f"   {line}")
                return True
            else:
                print("⚠️ 未找到股票数据更新的定时任务")
                return False
        else:
            print("❌ 无法读取crontab配置")
            return False
            
    except Exception as e:
        print(f"❌ 检查定时任务时发生错误: {e}")
        return False

def main():
    """主函数"""
    print("🚀 股票数据更新系统测试")
    
    # 测试更新功能
    update_ok = test_update()
    
    # 测试定时任务
    cron_ok = test_cron_setup()
    
    print("\n" + "=" * 50)
    print("📋 测试结果汇总")
    print("=" * 50)
    print(f"数据更新功能: {'✅ 正常' if update_ok else '❌ 异常'}")
    print(f"定时任务设置: {'✅ 正常' if cron_ok else '❌ 异常'}")
    
    if update_ok and cron_ok:
        print("\n🎉 所有测试通过！系统已准备就绪")
        print("\n💡 提示:")
        print("   - 系统将在每个工作日下午4点和晚上8点自动更新数据")
        print("   - 更新结果会推送到企业微信")
        print("   - 可以通过日志文件查看详细信息")
    else:
        print("\n⚠️ 部分测试失败，请检查配置")
    
    return update_ok and cron_ok

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

