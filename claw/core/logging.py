"""
轻量日志模块
=============
统一日志格式，支持同时输出到控制台 + 文件。

使用：
    from claw.core.logging import get_logger
    log = get_logger(__name__)
    log.info("开始选股")
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from claw.core.config import settings

_configured = False


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """配置根 logger（只需调用一次）"""
    global _configured
    if _configured:
        return

    root = logging.getLogger("claw")
    root.setLevel(level)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """返回命名 logger（自动做一次全局配置）"""
    if not _configured:
        setup_logging()
    # claw.xxx -> 加到 claw 命名空间下
    if not name.startswith("claw"):
        name = f"claw.{name}"
    return logging.getLogger(name)
