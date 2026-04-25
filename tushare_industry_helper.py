# -*- coding: utf-8 -*-
"""
Tushare 行业成分股获取工具
===========================
统一替代 AKShare 的 ak.stock_board_industry_cons_em() 接口，
使用 Tushare 的 stock_basic + daily_basic 获取行业成分股数据。

解决问题：
  - AKShare 东方财富接口频繁被反爬拦截导致 RemoteDisconnected 错误
  - Tushare 接口更稳定，且项目已有 token 和客户端

使用方式：
    from tushare_industry_helper import get_industry_stocks_ts, get_industry_stock_codes_ts

    # 获取行业成分股详细数据（含行情）
    stocks = get_industry_stocks_ts("化学制药")

    # 仅获取股票代码列表
    codes = get_industry_stock_codes_ts("半导体")
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ============================================================
# Tushare Token 和 API 配置
# ============================================================
TUSHARE_TOKEN = os.environ.get(
    "TUSHARE_TOKEN",
    "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
)
TUSHARE_API_URL = "http://api.tushare.pro"

# ============================================================
# 东方财富行业名 → Tushare 行业名 映射表
# 前端使用的是东方财富行业分类，需要映射到 Tushare 的申万行业分类
# ============================================================
EM_TO_TS_INDUSTRY_MAP = {
    # 医药医疗
    "化学制药": ["化学制药", "西药"],
    "中药": ["中成药", "中药"],
    "医疗器械": ["医疗器械"],
    "医疗服务": ["医疗服务", "医院"],
    "生物制品": ["生物制品", "疫苗"],
    "医药商业": ["医药商业", "医药流通"],
    # 电子科技
    "消费电子": ["元器件", "电子元件", "消费电子"],
    "半导体": ["半导体", "芯片"],
    "光学光电子": ["光学光电", "LED"],
    "电子元件": ["元器件", "电子元件"],
    "电子化学品": ["电子化学品"],
    # 通信
    "通信设备": ["通信设备"],
    "通信服务": ["通信服务"],
    # IT
    "计算机设备": ["计算机设备", "IT设备"],
    "软件开发": ["软件服务", "软件开发"],
    "互联网服务": ["互联网", "网络服务"],
    # 电力设备新能源
    "电力设备": ["电气设备", "电力设备"],
    "电网设备": ["电网设备", "输变电"],
    "储能设备": ["储能", "电池"],
    "光伏设备": ["光伏", "太阳能"],
    "风电设备": ["风电", "风能"],
    # 汽车
    "汽车零部件": ["汽车配件", "汽车零部件"],
    "汽车整车": ["汽车整车", "乘用车"],
    "汽车服务": ["汽车服务"],
    # 机械设备
    "专用设备": ["专用机械", "专用设备"],
    "通用设备": ["通用机械", "通用设备"],
    "工程机械": ["工程机械"],
    "仪器仪表": ["仪器仪表"],
    # 军工
    "航天航空": ["航天装备", "航空"],
    "国防军工": ["军工", "国防"],
    "船舶制造": ["船舶", "船舶制造"],
    # 金属矿产
    "工业金属": ["工业金属", "铝", "铜"],
    "贵金属": ["贵金属", "黄金"],
    "小金属": ["小金属"],
    "钢铁": ["钢铁", "特钢"],
    # 能源
    "煤炭开采": ["煤炭开采"],
    "石油开采": ["石油开采"],
    # 化工
    "化学原料": ["化工原料"],
    "化学制品": ["化工", "化学制品"],
    "农化制品": ["农药", "化肥"],
    "塑料制品": ["塑料"],
    # 消费
    "食品饮料": ["食品", "饮料", "白酒", "乳品"],
    "食品加工": ["食品", "食品加工"],
    "酿酒行业": ["白酒", "啤酒", "饮料"],
    "纺织服装": ["纺织", "服装"],
    "纺织服饰": ["纺织", "服装"],
    "家电行业": ["家电", "白色家电"],
    "家用电器": ["家电", "白色家电"],
    "家用轻工": ["家具", "轻工"],
    "美容护理": ["美容", "日化"],
    "珠宝首饰": ["珠宝", "首饰"],
    "商业百货": ["商业", "百货", "零售"],
    # 房地产
    "房地产开发": ["房地产", "地产"],
    "房地产服务": ["物业", "房地产服务"],
    # 金融
    "银行": ["银行"],
    "证券": ["证券"],
    "保险": ["保险"],
    "多元金融": ["多元金融", "信托", "租赁"],
    # 文化传媒
    "游戏": ["游戏"],
    "影视院线": ["影视", "传媒"],
    "文化传媒": ["传媒", "影视", "出版"],
    "广告营销": ["广告", "营销"],
    "教育": ["教育"],
    # 旅游
    "旅游酒店": ["旅游", "景区", "酒店", "餐饮"],
    # 造纸包装
    "造纸印刷": ["造纸", "印刷"],
    "包装材料": ["包装", "印刷"],
    # 建筑建材
    "装修装饰": ["装修", "装饰"],
    "装修建材": ["建材", "装修"],
    "工程建设": ["建筑", "工程"],
    "水泥建材": ["水泥", "建材"],
    "玻璃玻纤": ["玻璃", "玻纤"],
    "专业工程": ["专业工程"],
    "工程咨询服务": ["工程咨询"],
    # 公用事业
    "环保行业": ["环保", "环境治理"],
    "水务": ["水务"],
    "电力行业": ["电力", "火电", "水电"],
    "燃气": ["燃气"],
    # 交通运输
    "交运设备": ["交通运输", "物流"],
    "物流行业": ["物流", "快递"],
    "航运港口": ["航运", "港口"],
    "铁路公路": ["铁路", "公路"],
    "航空机场": ["航空", "机场"],
    # 农业
    "农牧饲渔": ["农业", "畜牧", "渔业"],
    "饲料": ["饲料"],
    "种植业": ["种植", "粮食"],
    # 其他
    "贸易行业": ["贸易", "外贸"],
    "专业服务": ["专业服务", "咨询"],
    "非金属材料": ["非金属", "陶瓷"],
    "照明设备": ["照明"],
}


# ============================================================
# 数据缓存
# ============================================================
_cache: Dict[str, Tuple[datetime, any]] = {}
_CACHE_TTL = 3600  # 缓存1小时


def _get_cache(key: str):
    """获取缓存数据"""
    if key in _cache:
        cache_time, data = _cache[key]
        if (datetime.now() - cache_time).total_seconds() < _CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data):
    """设置缓存"""
    _cache[key] = (datetime.now(), data)


# ============================================================
# Tushare API 调用（带重试）
# ============================================================
def _ts_call(api_name: str, params: dict = None, fields: str = None,
             max_retries: int = 3) -> pd.DataFrame:
    """
    调用 Tushare Pro API，带重试机制。

    参数:
        api_name: 接口名
        params: 请求参数
        fields: 返回字段
        max_retries: 最大重试次数

    返回:
        pd.DataFrame
    """
    payload = {
        "api_name": api_name,
        "token": TUSHARE_TOKEN,
        "params": params or {},
    }
    if fields:
        payload["fields"] = fields

    for attempt in range(max_retries):
        try:
            resp = requests.post(TUSHARE_API_URL, json=payload, timeout=30)
            j = resp.json()
            if j.get("code") != 0:
                logger.warning(f"Tushare {api_name} 业务错误: {j.get('msg', '')}")
                return pd.DataFrame()
            data = j.get("data") or {}
            items = data.get("items", [])
            cols = data.get("fields", [])
            return pd.DataFrame(items, columns=cols)
        except Exception as e:
            logger.warning(f"Tushare {api_name} 第{attempt+1}次请求失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(1.0 + attempt)
    return pd.DataFrame()


# ============================================================
# 获取全市场股票基础信息（带缓存）
# ============================================================
def _get_all_stocks() -> pd.DataFrame:
    """获取全市场上市股票基础信息，带缓存"""
    cache_key = "all_stocks_basic"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    logger.info("从 Tushare 获取全市场股票基础信息...")
    df = _ts_call(
        "stock_basic",
        params={"list_status": "L"},
        fields="ts_code,symbol,name,industry,market,list_date"
    )

    if not df.empty:
        # 过滤掉ST和退市股
        df = df[~df["name"].str.contains("ST|退", na=False)]
        # 只保留主板、中小板、创业板、科创板
        df = df[df["ts_code"].str.match(r"^(00|30|60|68)")]
        _set_cache(cache_key, df)
        logger.info(f"获取到 {len(df)} 只上市股票")

    return df


def _get_latest_trade_date() -> str:
    """获取最近的交易日"""
    cache_key = "latest_trade_date"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")

    cal = _ts_call(
        "trade_cal",
        params={"exchange": "SSE", "start_date": start, "end_date": today, "is_open": "1"},
        fields="cal_date"
    )

    if not cal.empty:
        latest = sorted(cal["cal_date"].tolist())[-1]
        _set_cache(cache_key, latest)
        return latest

    # 兜底：返回今天
    return today


# ============================================================
# 东方财富行业名 → Tushare 行业名 匹配
# ============================================================
def _match_ts_industries(em_industry: str) -> List[str]:
    """
    将东方财富行业名映射为 Tushare 行业名列表。
    支持精确匹配和模糊匹配。

    参数:
        em_industry: 东方财富行业名（如 "化学制药"、"半导体"）

    返回:
        Tushare 行业名列表
    """
    # 1. 精确匹配映射表
    if em_industry in EM_TO_TS_INDUSTRY_MAP:
        return EM_TO_TS_INDUSTRY_MAP[em_industry]

    # 2. 可能前端传入的就是 Tushare 行业名，直接返回
    all_stocks = _get_all_stocks()
    if not all_stocks.empty:
        ts_industries = all_stocks["industry"].dropna().unique().tolist()
        if em_industry in ts_industries:
            return [em_industry]

    # 3. 模糊匹配：前2字匹配
    if len(em_industry) >= 2:
        matched = []
        for em_key, ts_list in EM_TO_TS_INDUSTRY_MAP.items():
            if em_industry[:2] in em_key or em_key[:2] in em_industry:
                matched.extend(ts_list)
        if matched:
            return list(set(matched))

    # 4. 最后兜底：直接用原名搜索
    return [em_industry]


# ============================================================
# 核心接口：获取行业成分股
# ============================================================
def get_industry_stock_codes_ts(industry: str) -> List[str]:
    """
    获取行业成分股代码列表（6位纯数字代码）。

    参数:
        industry: 行业名称（支持东方财富或Tushare行业名）

    返回:
        股票代码列表，如 ["000001", "600036", ...]
    """
    cache_key = f"industry_codes_{industry}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    all_stocks = _get_all_stocks()
    if all_stocks.empty:
        logger.error("无法获取股票基础信息")
        return []

    # 映射行业名
    ts_industries = _match_ts_industries(industry)
    logger.info(f"行业映射: '{industry}' → {ts_industries}")

    # 筛选同行业股票
    mask = all_stocks["industry"].isin(ts_industries)
    industry_stocks = all_stocks[mask]

    if industry_stocks.empty:
        # 尝试模糊匹配
        logger.warning(f"精确匹配无结果，尝试模糊匹配: {industry}")
        if len(industry) >= 2:
            mask = all_stocks["industry"].str.contains(industry[:2], na=False)
            industry_stocks = all_stocks[mask]

    if industry_stocks.empty:
        logger.warning(f"行业 '{industry}' 未找到任何成分股")
        return []

    # 提取6位代码（去掉 .SH/.SZ 后缀）
    codes = industry_stocks["symbol"].tolist()
    logger.info(f"行业 '{industry}' 找到 {len(codes)} 只成分股")

    _set_cache(cache_key, codes)
    return codes


def get_industry_stocks_ts(industry: str, with_market_data: bool = True) -> List[Dict]:
    """
    获取行业成分股详细数据（含行情信息）。

    参数:
        industry: 行业名称（支持东方财富或Tushare行业名）
        with_market_data: 是否附带最新行情数据

    返回:
        股票信息字典列表，格式兼容原 AKShare 接口的返回格式
    """
    cache_key = f"industry_stocks_detail_{industry}_{with_market_data}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    all_stocks = _get_all_stocks()
    if all_stocks.empty:
        logger.error("无法获取股票基础信息")
        return []

    # 映射行业名
    ts_industries = _match_ts_industries(industry)
    logger.info(f"行业映射: '{industry}' → {ts_industries}")

    # 筛选同行业股票
    mask = all_stocks["industry"].isin(ts_industries)
    industry_stocks = all_stocks[mask].copy()

    if industry_stocks.empty:
        # 尝试模糊匹配
        if len(industry) >= 2:
            mask = all_stocks["industry"].str.contains(industry[:2], na=False)
            industry_stocks = all_stocks[mask].copy()

    if industry_stocks.empty:
        logger.warning(f"行业 '{industry}' 未找到任何成分股")
        return []

    result = []

    # 获取最新行情数据
    market_data = {}
    if with_market_data:
        market_data = _get_batch_market_data(industry_stocks["ts_code"].tolist())

    for _, row in industry_stocks.iterrows():
        ts_code = row["ts_code"]
        symbol = row["symbol"]
        mkt = market_data.get(ts_code, {})

        item = {
            "code": symbol,
            "name": str(row.get("name", "")),
            "price": mkt.get("close", 0.0),
            "change": mkt.get("pct_chg", 0.0),
            "change_amount": mkt.get("change", 0.0),
            "volume": mkt.get("vol", 0.0),
            "turnover": mkt.get("amount", 0.0),
            "amplitude": 0.0,  # Tushare daily 无振幅字段，可后续计算
            "turnover_rate": mkt.get("turnover_rate", 0.0),
            "pe": mkt.get("pe", 0.0),
            "pb": mkt.get("pb", 0.0),
            "total_mv": mkt.get("total_mv", 0.0),
        }
        result.append(item)

    # 按市值降序排列
    result.sort(key=lambda x: x.get("total_mv", 0), reverse=True)

    logger.info(f"行业 '{industry}' 返回 {len(result)} 只成分股数据")
    _set_cache(cache_key, result)
    return result


def get_industry_stocks_df(industry: str) -> pd.DataFrame:
    """
    获取行业成分股 DataFrame（兼容原 AKShare 返回格式）。

    参数:
        industry: 行业名称

    返回:
        pd.DataFrame，列名与东方财富接口兼容：代码、名称、最新价、涨跌幅等
    """
    stocks = get_industry_stocks_ts(industry)
    if not stocks:
        return pd.DataFrame()

    df = pd.DataFrame(stocks)
    # 重命名列以兼容原 AKShare 格式
    df = df.rename(columns={
        "code": "代码",
        "name": "名称",
        "price": "最新价",
        "change": "涨跌幅",
        "change_amount": "涨跌额",
        "volume": "成交量",
        "turnover": "成交额",
        "amplitude": "振幅",
        "turnover_rate": "换手率",
        "pe": "市盈率-动态",
        "pb": "市净率",
        "total_mv": "总市值",
    })
    return df


# ============================================================
# 批量获取行情数据
# ============================================================
def _get_batch_market_data(ts_codes: List[str]) -> Dict[str, Dict]:
    """
    批量获取股票最新行情数据。
    使用 daily_basic + daily 接口。

    参数:
        ts_codes: ts_code 列表

    返回:
        {ts_code: {close, pct_chg, vol, amount, pe, pb, total_mv, turnover_rate, change}}
    """
    trade_date = _get_latest_trade_date()
    cache_key = f"batch_market_{trade_date}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    result = {}

    # 获取 daily_basic（PE、PB、市值、换手率）
    logger.info(f"获取 {trade_date} 的 daily_basic 数据...")
    basic_df = _ts_call(
        "daily_basic",
        params={"trade_date": trade_date},
        fields="ts_code,close,turnover_rate,pe,pb,total_mv,circ_mv"
    )
    time.sleep(0.3)

    # 获取 daily（涨跌幅、成交量、成交额）
    logger.info(f"获取 {trade_date} 的 daily 数据...")
    daily_df = _ts_call(
        "daily",
        params={"trade_date": trade_date},
        fields="ts_code,close,pct_chg,change,vol,amount"
    )

    # 合并数据
    if not basic_df.empty:
        for _, row in basic_df.iterrows():
            ts_code = row["ts_code"]
            result[ts_code] = {
                "close": _safe_float(row.get("close")),
                "turnover_rate": _safe_float(row.get("turnover_rate")),
                "pe": _safe_float(row.get("pe")),
                "pb": _safe_float(row.get("pb")),
                "total_mv": round(_safe_float(row.get("total_mv", 0)) / 10000, 2),  # 万元→亿元
                "circ_mv": round(_safe_float(row.get("circ_mv", 0)) / 10000, 2),
            }

    if not daily_df.empty:
        for _, row in daily_df.iterrows():
            ts_code = row["ts_code"]
            if ts_code not in result:
                result[ts_code] = {}
            result[ts_code].update({
                "close": _safe_float(row.get("close")),
                "pct_chg": _safe_float(row.get("pct_chg")),
                "change": _safe_float(row.get("change")),
                "vol": _safe_float(row.get("vol")),
                "amount": _safe_float(row.get("amount")),
            })

    _set_cache(cache_key, result)
    logger.info(f"获取到 {len(result)} 只股票的行情数据")
    return result


def _safe_float(val, default=0.0) -> float:
    """安全转换为浮点数"""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ============================================================
# 便捷接口：获取行业列表
# ============================================================
def get_all_industries() -> List[str]:
    """获取 Tushare 中所有行业名称列表"""
    all_stocks = _get_all_stocks()
    if all_stocks.empty:
        return []
    return sorted(all_stocks["industry"].dropna().unique().tolist())


def get_em_industry_list() -> List[str]:
    """获取东方财富行业名称列表（即映射表中的 key）"""
    return sorted(EM_TO_TS_INDUSTRY_MAP.keys())
