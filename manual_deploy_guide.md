# 🚀 股票分析系统云服务器手动部署指南

## 📋 前提条件

1. 确保你能SSH登录到云服务器：`9.134.135.23`
2. 确认你的用户名和登录方式（密码/密钥）

## 🔧 部署步骤

### 步骤1: 上传项目包

```bash
# 从本地上传项目包到服务器（请替换YOUR_USERNAME为你的实际用户名）
scp ../StockAnal_Sys.tar.gz YOUR_USERNAME@9.134.135.23:~/
```

### 步骤2: 登录服务器

```bash
# SSH登录服务器
ssh YOUR_USERNAME@9.134.135.23
```

### 步骤3: 在服务器上执行以下命令

```bash
# 更新系统
sudo apt update -y
sudo apt install -y python3 python3-pip python3-venv curl wget unzip

# 解压项目
tar -xzf StockAnal_Sys.tar.gz
cd StockAnal_Sys

# 创建Python虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install --upgrade pip
pip install -r requirements.txt

# 安装Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# 启动Ollama服务
sudo systemctl enable ollama
sudo systemctl start ollama

# 等待服务启动
sleep 10

# 下载AI模型（后台并行下载）
ollama pull qwen2:7b &
ollama pull gemma2:9b &
ollama pull mistral:7b &
ollama pull llama3.2:3b &
ollama pull deepseek-coder:6.7b &

# 等待所有模型下载完成
wait

# 开放防火墙端口
sudo ufw allow 8890/tcp

# 创建启动脚本
cat > start_server.sh << 'EOF'
#!/bin/bash
cd ~/StockAnal_Sys
source venv/bin/activate

# 确保Ollama服务运行
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
EOF

chmod +x start_server.sh

# 创建停止脚本
cat > stop_server.sh << 'EOF'
#!/bin/bash
pkill -f "python3 web_server.py"
echo "✅ 服务已停止"
EOF

chmod +x stop_server.sh

# 启动服务
./start_server.sh
```

### 步骤4: 验证部署

```bash
# 检查服务状态
ps aux | grep "python3 web_server.py"
ps aux | grep "ollama"

# 查看日志
tail -f app.log
```

## 🌐 访问服务

部署完成后，你可以通过以下地址访问：
- `http://9.134.135.23:8890`
- `http://yuanping-any1.devcloud.woa.com:8890`

## 🛠️ 管理命令

- 启动服务：`./start_server.sh`
- 停止服务：`./stop_server.sh`
- 查看日志：`tail -f app.log`
- 重启服务：`./stop_server.sh && ./start_server.sh`

## ❗ 常见问题

1. **权限问题**：如果遇到权限错误，在命令前加`sudo`
2. **端口被占用**：使用`sudo lsof -i :8890`检查端口占用
3. **模型下载慢**：模型下载可能需要较长时间，请耐心等待
4. **防火墙问题**：确保云服务商的安全组也开放了8890端口

## 🔍 故障排除

```bash
# 查看系统资源
free -h
df -h
top

# 查看网络状态
netstat -tlnp | grep 8890
netstat -tlnp | grep 11434

# 重启Ollama服务
sudo systemctl restart ollama

# 查看Ollama日志
sudo journalctl -u ollama -f
```
