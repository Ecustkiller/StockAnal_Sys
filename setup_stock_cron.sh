#!/bin/bash

# 股票选股推送系统定时任务设置脚本

echo "==========================================" 
echo "🚀 设置股票选股推送定时任务"
echo "=========================================="

# 获取当前crontab内容
CURRENT_CRON=$(crontab -l 2>/dev/null)

# 创建临时文件
TEMP_CRON="/tmp/new_crontab"

# 保留现有的crontab内容
echo "$CURRENT_CRON" > $TEMP_CRON

# 添加股票选股推送任务
echo "" >> $TEMP_CRON
echo "# 股票选股推送系统 - 自动生成" >> $TEMP_CRON
echo "# 每个工作日早上9:30执行选股推送" >> $TEMP_CRON
echo "30 9 * * 1-5 cd /root/StockAnal_Sys && source venv/bin/activate && python3 stock_selector.py select_only >> stock_selector.log 2>&1" >> $TEMP_CRON
echo "" >> $TEMP_CRON

# 安装新的crontab
crontab $TEMP_CRON

# 清理临时文件
rm $TEMP_CRON

echo "✅ 定时任务设置完成！"
echo ""
echo "📋 当前定时任务列表:"
crontab -l
echo ""
echo "📅 选股推送时间: 每个工作日早上9:30"
echo "📝 日志文件: /root/StockAnal_Sys/stock_selector.log"
echo ""
echo "🔧 管理命令:"
echo "  查看日志: tail -f /root/StockAnal_Sys/stock_selector.log"
echo "  手动测试: cd /root/StockAnal_Sys && python3 stock_selector.py test"
echo "  手动选股: cd /root/StockAnal_Sys && python3 stock_selector.py select_only"
echo "=========================================="