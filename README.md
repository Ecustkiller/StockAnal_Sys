# 🚀 智能股票分析系统

![版本](https://img.shields.io/badge/版本-2.1.0-blue.svg)
![Python](https://img.shields.io/badge/Python-3.9+-green.svg)
![Flask](https://img.shields.io/badge/Flask-2.0+-red.svg)
![AKShare](https://img.shields.io/badge/AKShare-1.16+-orange.svg)
![AI](https://img.shields.io/badge/AI_API-Gemini-blueviolet.svg)

> 一个基于Python和Flask的专业级智能股票分析系统，整合多维度股票分析能力和人工智能辅助决策功能。

![系统首页截图](./images/1.png)

## 📝 项目概述

智能股票分析系统是一个**企业级**的Web应用，通过AKShare获取实时股票数据，结合技术分析、基本面分析和资金面分析，为投资者提供全方位的投资决策支持。系统集成了Gemini AI，能够提供智能化的投资建议和市场分析。

## ✨ 核心功能

### 🔍 多维度股票分析
- **技术面分析**：趋势识别、支撑压力位、多种技术指标（RSI、MACD、KDJ、布林带、ATR等）
- **基本面分析**：估值分析、财务健康、成长前景评估
- **资金面分析**：主力资金流向、北向资金、机构持仓跟踪
- **智能评分**：100分制综合评分系统，采用时空共振交易框架

### 📊 市场分析工具
- **市场扫描**：智能筛选高评分股票，发现投资机会
- **投资组合分析**：评估组合表现，提供优化建议
- **风险监控**：多维度风险预警系统
- **指数行业分析**：支持沪深300、中证500等指数和主要行业成分股分析

### 🤖 AI增强功能
- **智能问答**：基于Gemini AI的股票问答系统，支持联网搜索
- **情景预测**：AI生成的多种走势预测（乐观、中性、悲观）
- **AI分析报告**：自动生成专业投资分析报告
- **舆情分析**：AI解读新闻对股价的影响

### 📰 实时数据更新
- **财经要闻**：时间线形式展示最新财经新闻，自动高亮关键信息
- **舆情热点监控**：自动识别和展示市场舆情热点
- **全球市场状态**：实时显示全球主要证券市场开闭市状态
- **自动刷新机制**：系统定时自动刷新，确保数据实时性

### 🎨 可视化界面
- **交互式图表**：ApexCharts驱动的K线图、技术指标图表
- **响应式设计**：适配桌面和移动设备的专业界面
- **财经门户风格**：三栏式布局，专业的用户体验

## 🏗️ 系统架构

```
智能股票分析系统/
│
├── 🌐 前端展示层
│   ├── templates/          # HTML模板
│   ├── static/            # 静态资源
│   └── Bootstrap 5 + ApexCharts
│
├── 🔧 业务逻辑层
│   ├── web_server.py       # Flask路由控制
│   ├── stock_analyzer.py   # 核心分析引擎
│   ├── fundamental_analyzer.py  # 基本面分析
│   ├── capital_flow_analyzer.py # 资金流向分析
│   ├── industry_analyzer.py     # 行业分析
│   ├── scenario_predictor.py    # 情景预测
│   ├── stock_qa.py             # AI问答
│   └── risk_monitor.py         # 风险监控
│
├── 🗄️ 数据服务层
│   ├── AKShare API        # 股票数据
│   ├── 财联社 API         # 新闻数据
│   ├── Gemini AI API     # AI分析
│   ├── SERP/Tavily API   # 新闻搜索
│   └── 缓存系统
│
└── 🔐 支撑服务层
    ├── database.py        # 数据库操作
    ├── news_fetcher.py    # 新闻获取
    └── auth_middleware.py # 认证中间件
```

## 💻 技术栈

### 后端技术
- **框架**: Flask 3.1.0
- **数据分析**: Pandas, NumPy, SciPy
- **数据源**: AKShare 1.16+
- **AI集成**: Gemini API, OpenAI SDK
- **缓存**: Flask-Caching, Redis (可选)
- **数据库**: SQLAlchemy (可选)

### 前端技术
- **UI框架**: Bootstrap 5
- **图表库**: ApexCharts
- **工具库**: jQuery, HTML2PDF
- **字体图标**: Font Awesome

### 数据源
- **股票数据**: AKShare → 东方财富
- **新闻数据**: 财联社API
- **AI服务**: Gemini API
- **新闻搜索**: SERP API, Tavily API

## 📦 安装部署

### 环境要求
- Python 3.9+
- pip包管理器
- 网络连接（用于获取股票数据和AI服务）

### 快速安装

1. **克隆代码库**
```bash
git clone https://github.com/LargeCupPanda/StockAnal_Sys.git
cd StockAnal_Sys
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **配置环境变量**

创建 `.env` 文件并配置API密钥：
```env
# Gemini AI API 配置
OPENAI_API_KEY=your_gemini_api_key
OPENAI_API_URL=https://generativelanguage.googleapis.com/v1beta
OPENAI_API_MODEL=gemini-1.5-flash
NEWS_MODEL=gemini-1.5-flash
FUNCTION_CALL_MODEL=gemini-1.5-pro

# 可选：新闻搜索API
SERP_API_KEY=your_serp_api_key
TAVILY_API_KEY=your_tavily_api_key

# 可选：Redis缓存
USE_REDIS_CACHE=False
REDIS_URL=redis://localhost:6379

# 可选：数据库
USE_DATABASE=False
```

4. **启动系统**
```bash
python web_server.py
```

访问 `http://localhost:8889` 即可使用系统。

### Docker部署

1. **使用Docker Compose**
```bash
docker-compose up -d
```

2. **环境配置**
```yaml
# docker-compose.yml 已包含完整配置
# 需要挂载 .env 文件到容器
```

## 🎯 使用指南

### 主要功能页面

| 功能 | 地址 | 说明 |
|------|------|------|
| 🏠 首页 | `/` | 财经门户风格首页，实时新闻 |
| 📊 智能仪表盘 | `/dashboard` | 股票分析主界面 |
| 🤖 智能问答 | `/qa` | AI驱动的股票问答 |
| 🔍 市场扫描 | `/market_scan` | 批量股票筛选 |
| 💰 资金流向 | `/capital_flow` | 资金流向分析 |
| 📈 基本面分析 | `/fundamental` | 财务指标分析 |
| 🎯 情景预测 | `/scenario_predict` | AI预测分析 |
| ⚠️ 风险监控 | `/risk_monitor` | 风险评估 |
| 🏭 行业分析 | `/industry_analysis` | 行业对比分析 |
| 💼 投资组合 | `/portfolio` | 组合管理 |

### 常用操作

1. **分析股票**：在智能仪表盘输入股票代码，点击"分析"
2. **查看股票详情**：点击股票代码进入详情页面
3. **市场扫描**：选择指数或行业，设置最低评分进行筛选
4. **AI问答**：选择股票后提问，获取AI专业分析
5. **查看实时新闻**：在首页浏览最新财经新闻和热点

## 📊 核心指标

### 技术分析指标
- **趋势指标**: MA5、MA20、MA60
- **动量指标**: RSI、MACD、ROC
- **波动指标**: 布林带、ATR、波动率
- **成交量指标**: 量比、成交量移动平均
- **支撑压力**: 自动计算关键价位

### 评分系统
采用**时空共振交易系统**框架：
- 趋势因子: 30%
- 技术指标: 25%
- 成交量因子: 20%
- 波动率因子: 15%
- 动量因子: 10%

## 🔧 配置选项

### 技术指标参数
可在 `stock_analyzer.py` 中调整：
```python
self.params = {
    'ma_periods': {'short': 5, 'medium': 20, 'long': 60},
    'rsi_period': 14,
    'bollinger_period': 20,
    'bollinger_std': 2,
    'volume_ma_period': 20,
    'atr_period': 14
}
```

### 缓存策略
- **股票数据缓存**: 减少API调用
- **分析结果缓存**: 避免重复计算
- **新闻数据缓存**: 按日期存储
- **自动清理**: 收盘时间自动清理缓存

## 📚 API 文档

系统提供完整的REST API，访问 `/api/docs` 查看Swagger文档。

### 主要API端点

| API | 方法 | 说明 |
|-----|------|------|
| `/api/enhanced_analysis` | POST | 增强股票分析 |
| `/api/start_market_scan` | POST | 启动市场扫描 |
| `/api/qa` | POST | 智能问答 |
| `/api/scenario_predict` | POST | 情景预测 |
| `/api/latest_news` | GET | 最新新闻 |
| `/api/capital_flow` | POST | 资金流向 |
| `/api/risk_analysis` | POST | 风险分析 |

## 🚀 性能优化

### 系统性能
- **数据获取**: 0.1秒内完成股票数据获取
- **技术指标计算**: 0.01秒内完成所有指标计算
- **AI分析**: 10秒内完成增强分析
- **批量扫描**: 支持100+股票并发分析

### 缓存机制
- 多层缓存策略
- 智能缓存失效
- 收盘时间自动清理

## ⚠️ 重要声明

**本系统为学习研究版本，旨在探索人工智能在股票分析领域的应用。**

- ❌ **不构成投资建议**：AI生成的分析内容仅供参考
- ⚠️ **投资有风险**：入市需谨慎
- 🎓 **教育用途**：适用于学习量化分析和AI应用

## 🤝 贡献指南

欢迎提交Issue和Pull Request！

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 🙏 致谢

感谢以下开源项目的支持：
- [AKShare](https://github.com/akfamily/akshare) - 金融数据接口
- [Flask](https://flask.palletsprojects.com/) - Web框架
- [ApexCharts](https://apexcharts.com/) - 图表库
- [Bootstrap](https://getbootstrap.com/) - UI框架

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

<div align="center">
  ⭐ 如果这个项目对您有帮助，请给它一个Star！
</div>