#!/bin/bash

# 每日股票数据更新定时任务设置脚本

echo "=========================================="
echo "🚀 设置每日股票数据更新定时任务"
echo "=========================================="

# 项目路径
PROJECT_DIR="/root/StockAnal_Sys"
SCRIPT_PATH="$PROJECT_DIR/update_daily_stock_data.py"
LOG_DIR="$PROJECT_DIR/logs"

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# 确保脚本有执行权限
chmod +x "$SCRIPT_PATH"

# 备份现有的crontab
echo "📋 备份现有crontab..."
crontab -l > /tmp/current_cron_backup_$(date +%Y%m%d_%H%M%S) 2>/dev/null || echo "# 新的crontab" > /tmp/current_cron_backup_$(date +%Y%m%d_%H%M%S)

# 创建新的cron任务配置
CRON_FILE="/tmp/stock_update_cron"

# 获取现有的crontab内容（如果存在）
crontab -l > "$CRON_FILE" 2>/dev/null || echo "# 股票数据更新定时任务" > "$CRON_FILE"

# 检查是否已经存在股票数据更新任务
if grep -q "update_daily_stock_data.py" "$CRON_FILE"; then
    echo "⚠️ 检测到已存在的股票数据更新任务，将替换..."
    # 移除旧的股票数据更新任务
    grep -v "update_daily_stock_data.py" "$CRON_FILE" > "${CRON_FILE}.tmp"
    mv "${CRON_FILE}.tmp" "$CRON_FILE"
fi

# 添加新的股票数据更新任务
cat >> "$CRON_FILE" << EOF

# 股票数据每日更新任务 - 每个工作日收盘后执行
# 每个工作日下午4点执行数据更新（避开交易时间）
0 16 * * 1-5 cd $PROJECT_DIR && source venv/bin/activate && python3 update_daily_stock_data.py >> $LOG_DIR/daily_update_cron.log 2>&1

# 每个工作日晚上8点再执行一次（确保数据完整性）
0 20 * * 1-5 cd $PROJECT_DIR && source venv/bin/activate && python3 update_daily_stock_data.py >> $LOG_DIR/daily_update_cron.log 2>&1

EOF

# 加载新的crontab配置
crontab "$CRON_FILE"

if [ $? -eq 0 ]; then
    echo "✅ 定时任务设置成功！"
    echo ""
    echo "📅 定时任务详情："
    echo "   - 每个工作日下午4点：数据更新"
    echo "   - 每个工作日晚上8点：数据更新（备份）"
    echo ""
    echo "📁 日志文件："
    echo "   - 脚本日志: $LOG_DIR/daily_update.log"
    echo "   - Cron日志: $LOG_DIR/daily_update_cron.log"
    echo ""
    echo "🔍 查看当前定时任务："
    echo "   crontab -l"
    echo ""
    echo "📊 手动测试更新："
    echo "   cd $PROJECT_DIR && source venv/bin/activate && python3 update_daily_stock_data.py"
    echo ""
    echo "📱 企业微信通知已配置，更新完成后会自动推送结果"
else
    echo "❌ 定时任务设置失败！"
    exit 1
fi

# 清理临时文件
rm -f "$CRON_FILE"

echo "=========================================="

