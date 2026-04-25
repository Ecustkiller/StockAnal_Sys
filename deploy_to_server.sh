#!/bin/bash
# 股票分析系统云服务器自动部署脚本
# 作者：AI Assistant
# 日期：2025-09-29

set -e  # 遇到错误立即退出

# 配置变量
SERVER_IP="9.134.135.23"
SERVER_DOMAIN="yuanping-any1.devcloud.woa.com"
SERVER_USER="root"  # 默认用户，如果不对请修改
PROJECT_NAME="StockAnal_Sys"
REMOTE_DIR="/root/${PROJECT_NAME}"
LOCAL_PACKAGE="../${PROJECT_NAME}.tar.gz"

echo "=========================================="
echo "🚀 股票分析系统云服务器自动部署脚本"
echo "=========================================="
echo "📍 目标服务器: ${SERVER_IP} (${SERVER_DOMAIN})"
echo "👤 用户: ${SERVER_USER}"
echo "📦 项目包: ${LOCAL_PACKAGE}"
echo "📂 远程目录: ${REMOTE_DIR}"
echo "=========================================="

# 检查本地部署包是否存在
if [ ! -f "$LOCAL_PACKAGE" ]; then
    echo "❌ 错误：部署包 $LOCAL_PACKAGE 不存在！"
    exit 1
fi

echo "✅ 部署包检查通过"

# 1. 上传项目包到服务器
echo ""
echo "📤 步骤1: 上传项目包到服务器..."
scp -o StrictHostKeyChecking=no "$LOCAL_PACKAGE" "${SERVER_USER}@${SERVER_IP}:~/"
echo "✅ 项目包上传完成"

# 2. 连接服务器并执行部署
echo ""
echo "🔧 步骤2: 连接服务器执行部署..."
ssh -o StrictHostKeyChecking=no "${SERVER_USER}@${SERVER_IP}" << 'EOF'
set -e

echo "🔍 检查系统信息..."
echo "系统版本: $(cat /etc/os-release | grep PRETTY_NAME)"
echo "Python版本: $(python3 --version 2>/dev/null || echo '未安装')"
echo "当前用户: $(whoami)"
echo "当前目录: $(pwd)"

# 更新系统包
echo ""
echo "📦 更新系统包..."
apt update -y
apt install -y python3 python3-pip python3-venv curl wget unzip

# 解压项目
echo ""
echo "📂 解压项目..."
if [ -d "StockAnal_Sys" ]; then
    echo "🗑️  删除旧版本..."
    rm -rf StockAnal_Sys
fi

tar -xzf StockAnal_Sys.tar.gz
cd StockAnal_Sys

echo "✅ 项目解压完成"
echo "📁 项目内容:"
ls -la

# 创建虚拟环境
echo ""
echo "🐍 创建Python虚拟环境..."
python3 -m venv venv
source venv/bin/activate

# 安装Python依赖
echo ""
echo "📚 安装Python依赖..."
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Python环境配置完成"

# 安装Ollama
echo ""
echo "🤖 安装Ollama..."
if ! command -v ollama &> /dev/null; then
    curl -fsSL https://ollama.ai/install.sh | sh
    echo "✅ Ollama安装完成"
else
    echo "✅ Ollama已安装"
fi

# 启动Ollama服务
echo ""
echo "🚀 启动Ollama服务..."
systemctl enable ollama || echo "⚠️  无法启用ollama服务，尝试手动启动"
systemctl start ollama || echo "⚠️  无法启动ollama服务，稍后手动启动"

# 等待Ollama服务启动
echo "⏳ 等待Ollama服务启动..."
sleep 10

# 下载AI模型
echo ""
echo "🧠 下载AI模型..."
echo "正在下载 qwen2:7b..."
ollama pull qwen2:7b &

echo "正在下载 gemma2:9b..."
ollama pull gemma2:9b &

echo "正在下载 mistral:7b..."
ollama pull mistral:7b &

echo "正在下载 llama3.2:3b..."
ollama pull llama3.2:3b &

echo "正在下载 deepseek-coder:6.7b..."
ollama pull deepseek-coder:6.7b &

# 等待所有模型下载完成
echo "⏳ 等待所有模型下载完成..."
wait

echo "✅ AI模型下载完成"

# 配置防火墙
echo ""
echo "🔥 配置防火墙..."
ufw allow 8890/tcp || echo "⚠️  防火墙配置失败，请手动开放8890端口"

# 创建启动脚本
echo ""
echo "📝 创建启动脚本..."
cat > start_server.sh << 'SCRIPT_EOF'
#!/bin/bash
cd /root/StockAnal_Sys
source venv/bin/activate

# 启动Ollama服务（如果未运行）
if ! pgrep -x "ollama" > /dev/null; then
    echo "启动Ollama服务..."
    ollama serve &
    sleep 5
fi

# 启动股票分析服务
echo "启动股票分析服务..."
nohup python3 web_server.py > app.log 2>&1 &

echo "🎉 服务启动完成！"
echo "📊 访问地址: http://$(curl -s ifconfig.me):8890"
echo "📊 本地访问: http://localhost:8890"
echo "📋 日志文件: $(pwd)/app.log"
SCRIPT_EOF

chmod +x start_server.sh

# 创建停止脚本
cat > stop_server.sh << 'SCRIPT_EOF'
#!/bin/bash
echo "停止股票分析服务..."
pkill -f "python3 web_server.py"
echo "✅ 服务已停止"
SCRIPT_EOF

chmod +x stop_server.sh

# 创建重启脚本
cat > restart_server.sh << 'SCRIPT_EOF'
#!/bin/bash
echo "重启股票分析服务..."
./stop_server.sh
sleep 2
./start_server.sh
SCRIPT_EOF

chmod +x restart_server.sh

echo ""
echo "🎉 部署完成！"
echo "=========================================="
echo "📊 访问地址:"
echo "  - http://$(curl -s ifconfig.me):8890"
echo "  - http://localhost:8890"
echo ""
echo "🛠️  管理命令:"
echo "  启动服务: ./start_server.sh"
echo "  停止服务: ./stop_server.sh"
echo "  重启服务: ./restart_server.sh"
echo ""
echo "📋 日志文件: $(pwd)/app.log"
echo "=========================================="

# 自动启动服务
echo ""
echo "🚀 自动启动服务..."
./start_server.sh

sleep 5

echo ""
echo "🔍 检查服务状态..."
if pgrep -f "python3 web_server.py" > /dev/null; then
    echo "✅ 股票分析服务运行正常"
else
    echo "❌ 股票分析服务启动失败，请检查日志"
    tail -20 app.log
fi

if pgrep -x "ollama" > /dev/null; then
    echo "✅ Ollama服务运行正常"
else
    echo "❌ Ollama服务未运行，请手动启动"
fi

echo ""
echo "🎊 部署和启动完成！"
EOF

echo "✅ 服务器部署完成"

# 3. 验证部署
echo ""
echo "🔍 步骤3: 验证部署..."
sleep 10

echo "测试服务器连通性..."
if curl -s --max-time 10 "http://${SERVER_IP}:8890" > /dev/null; then
    echo "✅ 服务部署成功！可以访问 http://${SERVER_IP}:8890"
else
    echo "⚠️  服务可能还在启动中，请稍后访问 http://${SERVER_IP}:8890"
fi

echo ""
echo "🎉 全流程部署完成！"
echo "=========================================="
echo "📊 访问地址:"
echo "  - http://${SERVER_IP}:8890"
echo "  - http://${SERVER_DOMAIN}:8890"
echo ""
echo "🔧 SSH登录服务器:"
echo "  ssh ${SERVER_USER}@${SERVER_IP}"
echo ""
echo "📁 项目目录: ${REMOTE_DIR}"
echo "📋 管理脚本:"
echo "  - 启动: ./start_server.sh"
echo "  - 停止: ./stop_server.sh" 
echo "  - 重启: ./restart_server.sh"
echo "=========================================="
