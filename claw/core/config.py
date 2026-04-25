"""
Claw 全局配置中心
==================
唯一的配置入口。所有模块（新 claw.* + 旧根目录脚本）都应从此处导入 TOKEN 和路径。

优先级（从高到低）：
    1. 环境变量（建议生产使用 export + .env）
    2. 项目根 .env 文件
    3. 此文件内的默认值（仅开发用）

使用方式：
    from claw.core.config import settings
    TOKEN = settings.TUSHARE_TOKEN
    SNAPSHOT_DIR = settings.SNAPSHOT_DIR

向后兼容：为保持旧脚本可运行，本文件末尾导出了 TOKEN/SNAPSHOT_DIR 等模块级常量。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


# ============================================================
# 加载 .env（可选）
# ============================================================
def _load_dotenv() -> None:
    """尽量加载项目根的 .env 文件，不依赖 python-dotenv。"""
    # 优先从 StockAnal_Sys 根目录加载（claw 作为子包时 parents[2] 即项目根）
    root = Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if not env_path.exists():
        # 兜底：尝试 parents[3]（如果目录层级不同）
        env_path = root.parent / ".env"
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'").strip('"')
            # 环境变量优先，不覆盖
            os.environ.setdefault(k, v)
    except Exception:
        pass


_load_dotenv()


# ============================================================
# 配置对象
# ============================================================
@dataclass
class Settings:
    """全局配置对象（单例）"""

    # === 项目路径 ===
    PROJECT_ROOT: Path = field(default_factory=lambda: Path(__file__).resolve().parents[2])

    # === API Tokens ===
    TUSHARE_TOKEN: str = field(default_factory=lambda: os.environ.get(
        "TUSHARE_TOKEN",
        # 默认值：保持与旧代码一致以向后兼容（上线前应改为空串）
        "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59",
    ))

    # === 数据目录 ===
    SNAPSHOT_DIR: str = field(default_factory=lambda: os.environ.get(
        "SNAPSHOT_DIR", os.path.expanduser("~/stock_data/daily_snapshot")
    ))
    STOCK_CSV_DIR: str = field(default_factory=lambda: os.environ.get(
        "STOCK_CSV_DIR", os.path.expanduser("~/stock_data")
    ))
    VR_DIR: str = field(default_factory=lambda: os.environ.get(
        "VR_DIR", os.path.expanduser("~/stock_data/volume_ratio")
    ))

    # === 60 分钟 K 线多年目录 ===
    KLINE_60M_DIRS: List[str] = field(default_factory=lambda: [
        os.path.expanduser(f"~/Downloads/{y}/60min") for y in range(2026, 2019, -1)
    ])

    # === Web 服务 ===
    WEB_HOST: str = field(default_factory=lambda: os.environ.get("WEB_HOST", "127.0.0.1"))
    WEB_PORT: int = field(default_factory=lambda: int(os.environ.get("WEB_PORT", "5088")))

    # === 项目内输出目录（相对 PROJECT_ROOT）===
    REPORTS_DIR_NAME: str = "reports"
    DATA_DIR_NAME: str = "data"
    BACKTEST_RESULTS_DIR: str = field(default_factory=lambda: os.environ.get(
        "BACKTEST_RESULTS_DIR",
        str(Path(__file__).resolve().parents[2] / "backtest_results"),
    ))

    # === 运行时参数 ===
    DEFAULT_TOP_N: int = 10
    DEFAULT_HOLD_DAYS: int = 1

    # ------ 派生属性 ------
    @property
    def KLINE_60M_DIR(self) -> str:
        """最近年份的 60 分钟 K 线目录（兼容旧代码）"""
        return self.KLINE_60M_DIRS[0]

    @property
    def reports_dir(self) -> Path:
        p = self.PROJECT_ROOT / self.REPORTS_DIR_NAME
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def data_dir(self) -> Path:
        p = self.PROJECT_ROOT / self.DATA_DIR_NAME
        p.mkdir(parents=True, exist_ok=True)
        return p

    def validate(self) -> List[str]:
        """返回配置问题列表，供启动自检"""
        issues = []
        if not self.TUSHARE_TOKEN or len(self.TUSHARE_TOKEN) < 30:
            issues.append("⚠️ TUSHARE_TOKEN 未设置或长度异常")
        if not Path(self.SNAPSHOT_DIR).exists():
            issues.append(f"⚠️ 本地快照目录不存在: {self.SNAPSHOT_DIR}")
        return issues


# ============================================================
# 单例 + 向后兼容的模块级常量
# ============================================================
settings = Settings()

# 向后兼容：允许旧代码 `from claw.core.config import TOKEN`
TOKEN: str = settings.TUSHARE_TOKEN
TUSHARE_TOKEN: str = settings.TUSHARE_TOKEN
SNAPSHOT_DIR: str = settings.SNAPSHOT_DIR
STOCK_CSV_DIR: str = settings.STOCK_CSV_DIR
KLINE_60M_DIR: str = settings.KLINE_60M_DIR
KLINE_60M_DIRS: List[str] = settings.KLINE_60M_DIRS
VR_DIR: str = settings.VR_DIR
PROJECT_ROOT: Path = settings.PROJECT_ROOT


def reload() -> None:
    """重新加载配置（测试用）"""
    global settings, TOKEN, TUSHARE_TOKEN, SNAPSHOT_DIR, STOCK_CSV_DIR
    global KLINE_60M_DIR, KLINE_60M_DIRS, VR_DIR, PROJECT_ROOT
    _load_dotenv()
    settings = Settings()
    TOKEN = settings.TUSHARE_TOKEN
    TUSHARE_TOKEN = settings.TUSHARE_TOKEN
    SNAPSHOT_DIR = settings.SNAPSHOT_DIR
    STOCK_CSV_DIR = settings.STOCK_CSV_DIR
    KLINE_60M_DIR = settings.KLINE_60M_DIR
    KLINE_60M_DIRS = settings.KLINE_60M_DIRS
    VR_DIR = settings.VR_DIR
    PROJECT_ROOT = settings.PROJECT_ROOT


if __name__ == "__main__":
    print("=" * 60)
    print("  Claw 配置自检")
    print("=" * 60)
    print(f"PROJECT_ROOT  : {settings.PROJECT_ROOT}")
    print(f"TUSHARE_TOKEN : {settings.TUSHARE_TOKEN[:8]}...{settings.TUSHARE_TOKEN[-4:]}")
    print(f"SNAPSHOT_DIR  : {settings.SNAPSHOT_DIR}")
    print(f"KLINE_60M_DIR : {settings.KLINE_60M_DIR}")
    print(f"VR_DIR        : {settings.VR_DIR}")
    print(f"WEB_PORT      : {settings.WEB_PORT}")
    print(f"reports_dir   : {settings.reports_dir}")
    print(f"data_dir      : {settings.data_dir}")
    print("-" * 60)
    issues = settings.validate()
    if issues:
        print("⚠️ 发现以下问题：")
        for i in issues:
            print(f"  {i}")
    else:
        print("✅ 配置检查通过")
    print("=" * 60)
