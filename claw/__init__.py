"""
Claw — A股量化选股与交易系统（工程化版）
========================================

工程化目录结构：

    claw/
    ├── core/              # 核心配置、通用工具、日志
    ├── data_pipeline/     # 数据下载与同步
    ├── scoring/           # 评分系统（九维150分制）
    ├── screeners/         # 筛选器（多周期、主线、WR等）
    ├── strategies/        # 可回测策略
    ├── backtest/          # 回测框架
    ├── analysis/          # BCI、情绪、对比等分析工具
    ├── optimizations/     # v4 漏洞修复模块（一日游/反向修正等）
    ├── scrapers/          # 爬虫（淘股吧/OCR）
    ├── web/               # Flask Web 评分系统
    └── utils/             # 工具函数

使用方式：
    from claw import settings, get_tushare_client
    from claw.strategies.strategy_03_optimized import select_optimized_elite

版本：v4.0
"""
from claw.core.config import settings, TOKEN, SNAPSHOT_DIR, KLINE_60M_DIR

__version__ = "4.0.0"
__all__ = ["settings", "TOKEN", "SNAPSHOT_DIR", "KLINE_60M_DIR"]
