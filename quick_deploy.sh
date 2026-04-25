#!/bin/bash
# 快速部署脚本 - 需要你提供正确的用户名
# 使用方法: ./quick_deploy.sh YOUR_USERNAME

if [ -z "$1" ]; then
    echo "❌ 请提供用户名！"
    echo "使用方法: ./quick_deploy.sh YOUR_USERNAME"
    echo "例如: ./quick_deploy.sh ubuntu"
    exit 1
fi

SERVER_USER="$1"
SERVER_IP="9.134.135.23"
LOCAL_PACKAGE="../StockAnal_Sys.tar.gz"

echo "🚀 开始快速部署..."
echo "👤 用户: $SERVER_USER"
echo "🖥️  服务器: $SERVER_IP"

# 测试连接
echo "🔍 测试SSH连接..."
if ! ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SERVER_USER@$SERVER_IP" "echo '连接成功'" 2>/dev/null; then
    echo "❌ SSH连接失败！请检查："
    echo "  1. 用户名是否正确"
    echo "  2. SSH密钥是否配置"
    echo "  3. 服务器是否允许该用户登录"
    exit 1
fi

echo "✅ SSH连接成功"

# 上传项目包
echo "📤 上传项目包..."
scp -o StrictHostKeyChecking=no "$LOCAL_PACKAGE" "$SERVER_USER@$SERVER_IP:~/"

# 执行部署
echo "🔧 执行远程部署..."
ssh -o StrictHostKeyChecking=no "$SERVER_USER@$SERVER_IP" << 'EOF'
set -e

echo "📦 更新系统包..."
sudo apt update -y
sudo apt install -y python3 python3-pip python3-venv curl wget unzip

echo "📂 解压项目..."
if [ -d "StockAnal_Sys" ]; then
    rm -rf StockAnal_Sys
fi
tar -xzf StockAnal_Sys.tar.gz
cd StockAnal_Sys

echo "🐍 配置Python环境..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "🤖 安装Ollama..."
if ! command -v ollama &> /dev/null; then
    curl -fsSL https://ollama.ai/install.sh | sh
fi

echo "🚀 启动Ollama服务..."
sudo systemctl enable ollama 2>/dev/null || true
sudo systemctl start ollama 2>/dev/null || ollama serve &

sleep 10

echo "🧠 下载AI模型..."
ollama pull qwen2:7b &
ollama pull gemma2:9b &
ollama pull mistral:7b &
wait

echo "🔥 配置防火墙..."
sudo ufw allow 8890/tcp 2>/dev/null || true

echo "📝 创建管理脚本..."
cat > start_server.sh << 'SCRIPT_EOF'
#!/bin/bash
cd ~/StockAnal_Sys
source venv/bin/activate
if ! pgrep -x "ollama" > /dev/null; then
    ollama serve &
    sleep 5
fi
nohup python3 web_server.py > app.log 2>&1 &
echo "🎉 服务启动完成！访问: http://$(curl -s ifconfig.me):8890"
SCRIPT_EOF

chmod +x start_server.sh

cat > stop_server.sh << 'SCRIPT_EOF'
#!/bin/bash
pkill -f "python3 web_server.py"
echo "✅ 服务已停止"
SCRIPT_EOF

chmod +x stop_server.sh

echo "🚀 启动服务..."
./start_server.sh

echo "🎊 部署完成！"
EOF

echo "✅ 部署完成！"
echo "🌐 访问地址: http://$SERVER_IP:8890"
