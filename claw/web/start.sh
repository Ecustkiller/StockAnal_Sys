#!/bin/bash
# A股综合评分系统 - 启动脚本

cd "$(dirname "$0")"

echo "=================================="
echo "  A股综合评分系统 Web版"
echo "=================================="

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo "❌ 未找到python3，请先安装Python 3.8+"
    exit 1
fi

# 安装依赖
echo "📦 检查依赖..."
pip3 install -q -r requirements.txt 2>/dev/null

# 启动服务
echo "🚀 启动服务..."
echo "📱 访问 http://localhost:5088 开始使用"
echo ""
python3 app.py
