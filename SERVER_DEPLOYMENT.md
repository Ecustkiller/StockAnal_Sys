# 股票选股推送系统 - 服务器部署指南

## 📁 需要上传的文件

请将以下文件上传到服务器的 `/root/StockAnal_Sys/` 目录：

1. **stock_selector.py** - 主要的选股推送脚本
2. **test_wecom.py** - 企业微信推送测试脚本  
3. **setup_stock_cron.sh** - 定时任务设置脚本

## 🚀 部署步骤

### 1. 上传文件到服务器

```bash
# 在服务器上执行
cd /root/StockAnal_Sys

# 确保虚拟环境已激活
source venv/bin/activate

# 给脚本执行权限
chmod +x stock_selector.py
chmod +x test_wecom.py
chmod +x setup_stock_cron.sh
```

### 2. 安装所需依赖

```bash
# 安装数据分析相关的包
pip install pandas numpy requests

# 如果需要更新股票数据，还需要安装baostock
pip install baostock

# 验证安装
python3 -c "import pandas, numpy, requests; print('依赖包安装成功!')"
```

### 3. 测试企业微信推送

```bash
# 测试企业微信推送功能
python3 test_wecom.py

# 或者使用主脚本测试
python3 stock_selector.py test
```

### 4. 手动测试选股功能

```bash
# 手动执行一次选股推送
python3 stock_selector.py select_only
```

### 5. 设置定时任务

```bash
# 自动设置定时任务
./setup_stock_cron.sh

# 或者手动设置
crontab -e
# 添加以下行：
# 30 9 * * 1-5 cd /root/StockAnal_Sys && source venv/bin/activate && python3 stock_selector.py select_only >> stock_selector.log 2>&1
```

## 📋 系统功能

### ✅ 主要功能
- **智能选股**: 基于技术指标的多维度选股策略
- **企业微信推送**: 每日早上9:30自动推送选股结果
- **日志记录**: 完整的运行日志便于排查问题
- **错误处理**: 完善的异常处理和错误提示

### ✅ 选股策略
- 突破20日均线 (20分)
- 短期均线多头排列 (15分)
- 成交量放大 (20分)
- 近期涨幅适中 (15分)
- 价格位置合理 (10分)

### ✅ 安全特性
- 自动检测数据目录
- 完善的错误处理
- 日志记录所有操作
- 支持不同运行环境

## 📅 定时任务说明

- **执行时间**: 每个工作日早上9:30
- **日志文件**: `/root/StockAnal_Sys/stock_selector.log`
- **推送内容**: 前5只推荐股票及选股理由

## 🔧 管理命令

```bash
# 查看运行日志
tail -f /root/StockAnal_Sys/stock_selector.log

# 查看定时任务
crontab -l

# 手动测试推送
cd /root/StockAnal_Sys && python3 stock_selector.py test

# 手动执行选股
cd /root/StockAnal_Sys && python3 stock_selector.py select_only

# 检查系统状态
ps aux | grep python3
```

## ⚠️ 注意事项

1. **数据目录**: 系统会自动检测股票数据目录位置
2. **网络连接**: 确保服务器能访问企业微信API
3. **权限设置**: 确保脚本有执行权限
4. **日志管理**: 定期清理日志文件避免占用过多空间
5. **错误处理**: 如果推送失败会在日志中记录详细错误信息

## 🆘 故障排查

### 企业微信推送失败
1. 检查webhook URL是否正确
2. 检查网络连接是否正常
3. 查看详细错误日志

### 选股分析失败
1. 检查股票数据文件是否存在
2. 检查数据格式是否正确
3. 查看pandas/numpy是否安装正确

### 定时任务不执行
1. 检查crontab是否设置正确
2. 检查脚本路径是否正确
3. 检查虚拟环境是否激活

## 📞 技术支持

如果遇到问题，请查看日志文件获取详细错误信息，或联系技术支持。
