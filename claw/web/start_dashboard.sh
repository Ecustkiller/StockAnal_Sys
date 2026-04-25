#!/bin/bash
# A股量化日报 Dashboard 启动脚本
# 访问: http://localhost:5099

cd "$(dirname "$0")/../.."

echo "=================================="
echo "  📊 A股量化日报 Dashboard"
echo "=================================="

# 检查 python3
if ! command -v python3 &> /dev/null; then
    echo "❌ 未找到 python3"
    exit 1
fi

# 安装依赖（和 app.py 共用）
echo "📦 检查依赖..."
pip3 install -q -r claw/web/requirements.txt 2>/dev/null

# 启动
echo "🚀 启动 Dashboard..."
echo "📱 访问 http://localhost:5099"
echo ""
python3 -m claw.web.dashboard_app
