"""
Tushare Pro API 统一客户端
===========================
替代原来散落在 20+ 个脚本里的 ts() 函数。

使用：
    from claw.core.tushare_client import ts, TushareClient

    # 兼容旧接口
    df = ts("daily", {"trade_date": "20260420"}, fields="ts_code,close")

    # 面向对象（推荐）
    client = TushareClient()
    df = client.call("daily", trade_date="20260420")
    df = client.daily("20260420", fields=["ts_code", "close"])
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Union

import pandas as pd
import requests

from claw.core.config import settings


class TushareClient:
    """Tushare Pro API 客户端（带简单的重试和限频控制）"""

    API_URL = "http://api.tushare.pro"

    def __init__(self, token: Optional[str] = None, timeout: int = 30,
                 max_retries: int = 2, rate_limit_sleep: float = 0.0):
        self.token = token or settings.TUSHARE_TOKEN
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_sleep = rate_limit_sleep

    def call(self, api_name: str,
             params: Optional[Dict] = None,
             fields: Optional[Union[str, List[str]]] = None,
             **kwargs) -> pd.DataFrame:
        """
        调用任意 Tushare Pro 接口。

        参数:
            api_name: 接口名（如 'daily', 'stock_basic'）
            params: 请求参数 dict；也可直接用 kwargs 传
            fields: 返回字段，str 或 list
            **kwargs: 会合并到 params

        返回:
            pandas.DataFrame（无数据时返回空 DataFrame）
        """
        p = dict(params or {})
        p.update(kwargs)

        payload = {"api_name": api_name, "token": self.token, "params": p}
        if fields is not None:
            payload["fields"] = ",".join(fields) if isinstance(fields, list) else fields

        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.post(self.API_URL, json=payload, timeout=self.timeout)
                j = resp.json()
                if j.get("code") != 0:
                    # Tushare 业务错误：不重试
                    return pd.DataFrame()
                data = j.get("data") or {}
                items = data.get("items", [])
                cols = data.get("fields", [])
                if self.rate_limit_sleep > 0:
                    time.sleep(self.rate_limit_sleep)
                return pd.DataFrame(items, columns=cols)
            except (requests.exceptions.RequestException, ValueError) as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(1.0 + attempt)
                continue

        # 所有重试都失败：静默返回空 DataFrame（与旧行为保持一致）
        if last_err is not None:
            pass  # 调用方可通过空结果判断
        return pd.DataFrame()

    # === 便捷接口封装 ===
    def daily(self, trade_date: str, fields: Optional[List[str]] = None) -> pd.DataFrame:
        return self.call("daily", trade_date=trade_date, fields=fields)

    def stock_basic(self, fields: Optional[List[str]] = None) -> pd.DataFrame:
        return self.call("stock_basic", list_status="L",
                         fields=fields or ["ts_code", "name", "industry", "market"])

    def trade_cal(self, start_date: str, end_date: str,
                   exchange: str = "SSE") -> pd.DataFrame:
        return self.call("trade_cal",
                         exchange=exchange, start_date=start_date, end_date=end_date,
                         is_open="1", fields="cal_date")

    def daily_basic(self, trade_date: str, fields: Optional[List[str]] = None) -> pd.DataFrame:
        return self.call("daily_basic", trade_date=trade_date, fields=fields)

    def moneyflow(self, trade_date: str, fields: Optional[List[str]] = None) -> pd.DataFrame:
        return self.call("moneyflow", trade_date=trade_date, fields=fields)


# ============================================================
# 全局单例（向后兼容）
# ============================================================
_default_client: Optional[TushareClient] = None


def get_client() -> TushareClient:
    """返回全局默认客户端（懒加载）"""
    global _default_client
    if _default_client is None:
        _default_client = TushareClient()
    return _default_client


def ts(api_name: str,
       params: Optional[Dict] = None,
       fields: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
    """
    向后兼容的函数式调用（等价于所有旧脚本里的 ts 函数）。

    旧用法：
        df = ts("daily", {"trade_date": "20260420"}, "ts_code,close")
    """
    return get_client().call(api_name, params=params, fields=fields)
