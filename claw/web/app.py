#!/usr/bin/env python3
"""
A股综合评分系统 - Web版（本地数据优先架构）
数据源优先级：
  1. 本地数据（~/stock_data/daily_snapshot/*.parquet + ~/stock_data/*.csv）— 最快，零延迟
  2. Ashare（新浪+腾讯）— 实时K线数据，免费无限制
  3. Tushare — 基本面、资金流向、交易日历
  4. AKShare — 涨停池、情绪数据
"""
import sys
import requests
import time
import json
import os
import glob
import traceback
from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ===== 引入Ashare =====
sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
try:
    from Ashare import get_price as ashare_get_price
    ASHARE_AVAILABLE = True
    print("✅ Ashare数据源已加载")
except ImportError:
    ASHARE_AVAILABLE = False
    print("⚠️ Ashare不可用，将使用Tushare作为K线数据源")

# ===== 引入AKShare =====
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    print("✅ AKShare数据源已加载")
except ImportError:
    AKSHARE_AVAILABLE = False
    print("⚠️ AKShare不可用，涨停池数据将使用Tushare替代")

app = Flask(__name__, static_folder='static')
CORS(app)

# ===== Tushare配置 =====
TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

# ===== 本地数据路径 =====
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
STOCK_CSV_DIR = os.path.expanduser("~/stock_data")
LOCAL_DATA_AVAILABLE = os.path.isdir(SNAPSHOT_DIR)

if LOCAL_DATA_AVAILABLE:
    # 扫描本地快照文件，获取可用日期列表
    _snapshot_files = sorted(glob.glob(os.path.join(SNAPSHOT_DIR, "2*.parquet")))
    LOCAL_TRADE_DATES = [os.path.basename(f).replace('.parquet', '') for f in _snapshot_files]
    print(f"✅ 本地数据已加载: {len(LOCAL_TRADE_DATES)}个交易日快照, 最新: {LOCAL_TRADE_DATES[-1] if LOCAL_TRADE_DATES else 'N/A'}")
    
    # 构建个股CSV索引: ts_code -> filepath
    _csv_files = glob.glob(os.path.join(STOCK_CSV_DIR, "*.csv"))
    LOCAL_CSV_INDEX = {}
    for f in _csv_files:
        basename = os.path.basename(f)
        # 格式: 000001_平安银行.csv -> 提取code部分
        code6 = basename.split('_')[0] if '_' in basename else basename.replace('.csv', '')
        if len(code6) == 6 and code6.isdigit():
            # 转为ts_code格式
            if code6.startswith(('60', '68')):
                ts_code = code6 + '.SH'
            else:
                ts_code = code6 + '.SZ'
            LOCAL_CSV_INDEX[ts_code] = f
    print(f"  📁 个股CSV索引: {len(LOCAL_CSV_INDEX)}只")
    
    # 加载stock_basic
    _basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
    if os.path.exists(_basic_file):
        LOCAL_STOCK_BASIC = pd.read_parquet(_basic_file)
        print(f"  📋 stock_basic: {len(LOCAL_STOCK_BASIC)}只")
    else:
        LOCAL_STOCK_BASIC = None
else:
    LOCAL_TRADE_DATES = []
    LOCAL_CSV_INDEX = {}
    LOCAL_STOCK_BASIC = None
    print("⚠️ 本地数据目录不存在，将使用远程API")


# ===== BCI板块完整性数据加载 =====
import re as _re

BCI_DIR = os.path.expanduser("~/WorkBuddy/Claw")

def load_bci_data(date_str=None):
    """从BCI板块完整性分析v3 md文件中解析概念→BCI映射
    
    Returns:
        dict: {概念名: BCI分数}，如 {"固态电池": 85, "华为概念": 83, ...}
    """
    # 查找最新的BCI v3文件
    bci_files = sorted(glob.glob(os.path.join(BCI_DIR, "BCI板块完整性分析v3_*.md")))
    if not bci_files:
        bci_files = sorted(glob.glob(os.path.join(BCI_DIR, "BCI板块完整性分析v2_*.md")))
    if not bci_files:
        bci_files = sorted(glob.glob(os.path.join(BCI_DIR, "BCI板块完整性分析_*.md")))
    if not bci_files:
        return {}
    
    # 如果指定日期，找对应文件；否则用最新的
    target_file = bci_files[-1]
    if date_str:
        for f in bci_files:
            if date_str in os.path.basename(f):
                target_file = f
                break
    
    try:
        with open(target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 解析概念板块BCI排名表
        # 格式: | 1 | **固态电池** | **85** ⭐5 | 7家 | ...
        pattern = r'\|\s*\d+\s*\|\s*\*?\*?([^|*]+?)\*?\*?\s*\|\s*\*?\*?(\d+)\*?\*?\s*⭐?\d?\s*\|'
        matches = _re.findall(pattern, content)
        
        bci_map = {}
        for name, bci in matches:
            bci_map[name.strip()] = int(bci)
        
        if bci_map:
            print(f"✅ BCI数据已加载: {len(bci_map)}个概念, 来源: {os.path.basename(target_file)}")
        return bci_map
    except Exception as e:
        print(f"⚠️ BCI数据加载失败: {e}")
        return {}

# 行业→相关概念关键词映射（用于将行业映射到BCI概念）
# 行业→直接BCI映射（基于v3.3的行业级别BCI计算方式）
# v3.3的行业BCI = 该行业涨停票所属概念中的平均BCI，而非最高BCI
INDUSTRY_CONCEPT_MAP = {
    # 科技类
    "元器件": ["半导体概念", "华为概念", "电子", "通信技术", "5G概念", "国产芯片"],
    "半导体": ["半导体概念", "国产芯片", "华为概念", "电子"],
    "通信设备": ["通信技术", "华为概念", "5G概念", "光通信模块"],
    "计算机应用": ["人工智能", "华为概念", "数据中心"],
    "计算机设备": ["人工智能", "华为概念", "数据中心"],
    "电子元件": ["电子", "半导体概念", "华为概念", "国产芯片"],
    "光学光电": ["光通信模块", "CPO概念", "华为概念", "电子"],
    "软件服务": ["人工智能", "华为概念", "数据中心"],
    "互联网": ["人工智能", "数据中心"],
    # 新能源类
    "电气设备": ["电力设备", "储能概念", "光伏概念", "新能源"],
    "电池": ["锂电池概念", "固态电池", "电池技术", "储能概念", "钠离子电池"],
    "电源设备": ["储能概念", "电力设备", "新能源"],
    "光伏设备": ["光伏概念", "新能源", "电力设备"],
    "风电设备": ["电力设备", "新能源"],
    # 材料类
    "塑料": ["新材料", "基础化工"],
    "化工原料": ["基础化工", "新材料"],
    "化学制品": ["基础化工", "新材料"],
    "小金属": ["小金属概念", "新材料"],
    "钢铁": ["新材料", "基础化工"],
    "有色金属": ["小金属概念", "新材料"],
    # 制造类
    "专用机械": ["专精特新", "光伏概念", "电力设备"],
    "通用机械": ["专精特新", "电力设备"],
    "汽车配件": ["新能源车", "新能源"],
    "汽车整车": ["新能源车", "新能源"],
    # 默认
}

def get_industry_bci(industry, bci_map):
    """获取行业对应的BCI分数（用平均值而非最高值，对齐v3.3）
    
    v3.3的行业BCI = 该行业涨停票所属概念中的平均BCI
    例如：元器件行业映射到[半导体(78), 华为(83), 电子(67), 通信(72), 5G(67), 国产芯片(73)]
    平均 = (78+83+67+72+67+73)/6 = 73.3 ≈ 73，接近v3.3的元器件BCI=72
    
    Returns:
        (BCI分数, 对应概念名) 或 (0, None)
    """
    if not bci_map or not industry:
        return 0, None
    
    # 1. 精确匹配行业→概念映射
    concepts = INDUSTRY_CONCEPT_MAP.get(industry, [])
    bci_values = []
    best_concept = None
    best_bci = 0
    
    for concept in concepts:
        bci = bci_map.get(concept, 0)
        if bci > 0:
            bci_values.append(bci)
            if bci > best_bci:
                best_bci = bci
                best_concept = concept
    
    # 用平均值而非最高值，对齐v3.3的行业级别BCI计算方式
    if bci_values:
        avg_bci = int(np.mean(bci_values) + 0.5)
        return avg_bci, best_concept
    
    # 2. 模糊匹配（行业名包含在概念名中，或反过来）
    for concept, bci in bci_map.items():
        if industry in concept or concept in industry:
            if bci > best_bci:
                best_bci = bci
                best_concept = concept
    
    return best_bci, best_concept

# 启动时加载BCI数据
BCI_DATA = load_bci_data()


# ===== 本地快照缓存（避免重复读parquet） =====
_snapshot_cache = {}

def load_snapshot(date_str):
    """加载某天的全市场快照（带内存缓存）"""
    if date_str in _snapshot_cache:
        return _snapshot_cache[date_str]
    fpath = os.path.join(SNAPSHOT_DIR, f"{date_str}.parquet")
    if os.path.exists(fpath):
        df = pd.read_parquet(fpath)
        # 数值列转换
        num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg',
                    'vol', 'amount', 'pe_ttm', 'pb', 'total_mv', 'circ_mv',
                    'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'net_mf_amount']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        _snapshot_cache[date_str] = df
        return df
    return None


def ts_api(api_name, params=None, fields=None):
    """调用Tushare API"""
    if params is None:
        params = {}
    d = {"api_name": api_name, "token": TOKEN, "params": params}
    if fields:
        d["fields"] = fields
    for retry in range(3):
        try:
            r = requests.post("http://api.tushare.pro", json=d, timeout=30)
            j = r.json()
            if j.get("code") != 0:
                print(f"  ⚠ Tushare {api_name} 错误: {j.get('msg', '')}")
                return pd.DataFrame()
            return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
        except Exception as e:
            print(f"  ⚠ Tushare {api_name} 重试{retry+1}: {e}")
            time.sleep(1)
    return pd.DataFrame()


# ===== 数据缓存层 =====
class DataCache:
    """数据缓存，避免重复调用API"""
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._ttl = 300  # 5分钟缓存

    def get(self, key):
        if key in self._cache:
            if time.time() - self._cache_time.get(key, 0) < self._ttl:
                return self._cache[key]
        return None

    def set(self, key, value):
        self._cache[key] = value
        self._cache_time[key] = time.time()

    def clear(self):
        self._cache.clear()
        self._cache_time.clear()

cache = DataCache()


# ===== 工具函数：ts_code ↔ Ashare代码 互转 =====
def ts_code_to_ashare(ts_code):
    """600776.SH → sh600776"""
    code6 = ts_code[:6]
    return ('sh' if ts_code.endswith('.SH') else 'sz') + code6


def ashare_to_ts_code(ashare_code):
    """sh600776 → 600776.SH"""
    prefix = ashare_code[:2]
    code6 = ashare_code[2:]
    return code6 + ('.SH' if prefix == 'sh' else '.SZ')


# ===== 数据获取层（本地优先 → 远程兜底） =====

def get_trade_dates():
    """获取交易日历（本地优先 → Tushare兜底）"""
    # 优先使用本地快照文件名作为交易日历
    if LOCAL_TRADE_DATES:
        return LOCAL_TRADE_DATES
    
    cached = cache.get("trade_dates")
    if cached is not None:
        return cached
    now = datetime.now()
    start_y = (now - timedelta(days=730)).strftime("%Y%m%d")
    end_y = (now + timedelta(days=365)).strftime("%Y%m%d")
    cal = ts_api("trade_cal", {"exchange": "SSE", "start_date": start_y, "end_date": end_y}, "cal_date,is_open")
    if cal.empty:
        return []
    cal = cal[cal["is_open"] == 1].sort_values("cal_date").reset_index(drop=True)
    dates = cal["cal_date"].tolist()
    cache.set("trade_dates", dates)
    return dates


def get_latest_trade_date():
    """获取最新交易日（非交易日自动回溯到最近的交易日）"""
    dates = get_trade_dates()
    today = datetime.now().strftime("%Y%m%d")
    past = [d for d in dates if d <= today]
    if past:
        latest = past[-1]
        print(f"  📅 今天: {today}, 最新交易日: {latest}")
        return latest
    return None


def get_stock_list():
    """获取股票列表（本地优先 → Tushare兜底）"""
    cached = cache.get("stock_list")
    if cached is not None:
        return cached
    
    stk = None
    source = "未知"
    
    # 优先从本地stock_basic.parquet加载
    if LOCAL_STOCK_BASIC is not None:
        stk = LOCAL_STOCK_BASIC.copy()
        # 确保有必要的列
        if 'ts_code' in stk.columns and 'name' in stk.columns:
            if 'industry' not in stk.columns:
                stk['industry'] = '未知'
            source = "本地"
    
    # 兜底Tushare
    if stk is None or stk.empty:
        stk = ts_api("stock_basic", {"list_status": "L"}, "ts_code,name,industry")
        source = "Tushare"
    
    if stk is None or stk.empty:
        return {"ind_map": {}, "name_map": {}, "code_by_name": {}}
    
    stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
    stk = stk[~stk["name"].str.contains("ST|退", na=False)]
    result = {
        "ind_map": dict(zip(stk["ts_code"], stk["industry"])),
        "name_map": dict(zip(stk["ts_code"], stk["name"])),
        "code_by_name": dict(zip(stk["name"], stk["ts_code"])),
    }
    print(f"  📋 股票列表来源: {source} ({len(result['name_map'])}只)")
    cache.set("stock_list", result)
    return result


def resolve_stock_code(query):
    """解析用户输入为ts_code格式"""
    query = query.strip()
    stock_list = get_stock_list()
    name_map = stock_list["name_map"]
    code_by_name = stock_list["code_by_name"]

    if query in code_by_name:
        return code_by_name[query]

    for name, code in code_by_name.items():
        if query in name:
            return code

    if query.isdigit() and len(query) == 6:
        for suffix in [".SH", ".SZ"]:
            ts_code = query + suffix
            if ts_code in name_map:
                return ts_code

    if query.upper() in name_map:
        return query.upper()

    return None


# ===== K线数据获取（本地CSV优先 → Ashare → Tushare） =====

def get_kline_local(ts_code, count=30, trade_dates=None, t0_idx=None):
    """从本地CSV读取K线数据，如果CSV不够新则用parquet快照补充"""
    if ts_code not in LOCAL_CSV_INDEX:
        return None, "本地无此股票CSV"
    try:
        fpath = LOCAL_CSV_INDEX[ts_code]
        df = pd.read_csv(fpath)
        if df.empty:
            return None, "本地CSV为空"
        
        # 本地CSV列名: date,code,open,high,low,close,preclose,volume,amount,...,pctChg,...
        # 统一格式
        if 'date' in df.columns:
            df['trade_date'] = df['date'].astype(str).str.replace('-', '').str[:8]
        elif 'trade_date' not in df.columns:
            return None, "本地CSV无日期列"
        
        # 列名映射
        col_map = {}
        if 'volume' in df.columns and 'vol' not in df.columns:
            col_map['volume'] = 'vol'
        if 'pctChg' in df.columns and 'pct_chg' not in df.columns:
            col_map['pctChg'] = 'pct_chg'
        if col_map:
            df = df.rename(columns=col_map)
        
        # 确保必要列存在
        required = ['trade_date', 'open', 'high', 'low', 'close']
        for r in required:
            if r not in df.columns:
                return None, f"本地CSV缺少列{r}"
        
        # 转float
        for r in ['open', 'high', 'low', 'close', 'vol', 'pct_chg']:
            if r in df.columns:
                df[r] = pd.to_numeric(df[r], errors='coerce')
        
        # 如果没有pct_chg，手动计算
        if 'pct_chg' not in df.columns:
            df['pct_chg'] = df['close'].pct_change() * 100
        
        # 如果没有vol列，用0填充
        if 'vol' not in df.columns:
            df['vol'] = 0
        
        df = df.sort_values('trade_date').reset_index(drop=True)
        df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'pct_chg']].copy()
        
        # 过滤掉trade_date为nan或空的无效行
        df = df[df['trade_date'].notna() & (df['trade_date'] != '') & (df['trade_date'] != 'nan')].reset_index(drop=True)
        
        if df.empty:
            return None, "本地CSV过滤后为空"
        
        # 检查CSV是否覆盖到T0，如果不够新则用parquet快照补充
        csv_latest = df['trade_date'].iloc[-1]
        if trade_dates and t0_idx is not None:
            T0 = trade_dates[t0_idx]
            if csv_latest < T0:
                # CSV缺少最新数据，从parquet快照补充
                missing_dates = [d for d in trade_dates if csv_latest < d <= T0]
                supplement_rows = []
                for d in missing_dates:
                    snap = load_snapshot(d)
                    if snap is not None:
                        row = snap[snap['ts_code'] == ts_code]
                        if not row.empty:
                            r = row.iloc[0]
                            # 快照vol单位是手，CSV vol单位是股，需要转换
                            snap_vol = float(r.get('vol', 0))
                            # 检测CSV的vol量级：如果CSV最后5行平均vol > 快照vol的10倍，说明CSV是股单位
                            csv_avg_vol = df['vol'].tail(5).mean() if len(df) >= 5 else df['vol'].mean()
                            if csv_avg_vol > 0 and snap_vol > 0 and csv_avg_vol / snap_vol > 10:
                                snap_vol = snap_vol * 100  # 手→股
                            supplement_rows.append({
                                'trade_date': d,
                                'open': float(r.get('open', np.nan)),
                                'high': float(r.get('high', np.nan)),
                                'low': float(r.get('low', np.nan)),
                                'close': float(r.get('close', np.nan)),
                                'vol': snap_vol,
                                'pct_chg': float(r.get('pct_chg', np.nan)),
                            })
                if supplement_rows:
                    sup_df = pd.DataFrame(supplement_rows)
                    df = pd.concat([df, sup_df], ignore_index=True)
                    df = df.sort_values('trade_date').reset_index(drop=True)
                    print(f"  📊 本地CSV补充了 {len(supplement_rows)} 天快照数据 (CSV到{csv_latest}, 补到{T0})")
        
        # 取最后count根
        df = df.tail(count).reset_index(drop=True)
        
        print(f"  📊 本地CSV返回 {len(df)} 根K线, 日期: {df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
        return df, "本地CSV+快照"
    except Exception as e:
        return None, f"本地CSV异常: {e}"


# ===== 60分钟K线数据加载 =====
MIN60_DIRS = [
    os.path.expanduser("~/Downloads/2026/60min"),
    os.path.expanduser("~/Downloads/2025/60min"),
    os.path.expanduser("~/Downloads/2024/60min"),
]

def _ts_code_to_60min_prefix(ts_code):
    """将ts_code(如688582.SH)转为60min文件前缀(如sh688582)"""
    code, market = ts_code.split('.')
    if market == 'SH':
        return f"sh{code}"
    elif market == 'SZ':
        return f"sz{code}"
    elif market == 'BJ':
        return f"bj{code}"
    return None

def get_kline_60min_local(ts_code, count=80):
    """从本地60分钟K线CSV读取数据，自动合并多年数据
    
    Args:
        ts_code: 股票代码，如 '002108.SZ'
        count: 需要的60分钟K线根数（默认80根≈20个交易日）
    
    Returns:
        (DataFrame, source_str) 或 (None, error_str)
        DataFrame列: datetime, open, high, low, close, vol
    """
    prefix = _ts_code_to_60min_prefix(ts_code)
    if not prefix:
        return None, "无法解析ts_code"
    
    fname = f"{prefix}.csv"
    all_dfs = []
    
    for d in MIN60_DIRS:
        fpath = os.path.join(d, fname)
        if os.path.exists(fpath):
            try:
                df = pd.read_csv(fpath, encoding='utf-8')
                if df.empty:
                    continue
                # 统一列名：日期,时间,开盘,最高,最低,收盘,成交量,成交额
                col_map = {}
                cols = df.columns.tolist()
                if '日期' in cols:
                    col_map = {'日期': 'date', '时间': 'time', '开盘': 'open',
                               '最高': 'high', '最低': 'low', '收盘': 'close',
                               '成交量': 'vol', '成交额': 'amount'}
                    df = df.rename(columns=col_map)
                
                # 构建datetime列
                df['date'] = df['date'].astype(str).str[:10]
                df['time'] = df['time'].astype(str).str.strip()
                df['datetime'] = df['date'] + ' ' + df['time']
                
                for c in ['open', 'high', 'low', 'close', 'vol']:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                
                df = df[['datetime', 'date', 'time', 'open', 'high', 'low', 'close', 'vol']].copy()
                df = df.dropna(subset=['close'])
                all_dfs.append(df)
            except Exception as e:
                print(f"  ⚠️ 60min加载异常 {fpath}: {e}")
                continue
    
    if not all_dfs:
        return None, "本地无60分钟K线数据"
    
    result = pd.concat(all_dfs, ignore_index=True)
    result = result.sort_values('datetime').drop_duplicates(subset=['datetime']).reset_index(drop=True)
    
    # 取最近count根
    if len(result) > count:
        result = result.iloc[-count:].reset_index(drop=True)
    
    return result, "本地60min"


def get_kline_snapshot(ts_code, trade_dates, t0_idx, lookback=30):
    """从本地parquet快照拼接K线数据"""
    if not LOCAL_DATA_AVAILABLE:
        return None, "本地快照不可用"
    try:
        start_idx = max(0, t0_idx - lookback)
        dates_needed = trade_dates[start_idx:t0_idx + 1]
        
        rows = []
        for d in dates_needed:
            snap = load_snapshot(d)
            if snap is not None:
                row = snap[snap['ts_code'] == ts_code]
                if not row.empty:
                    r = row.iloc[0]
                    rows.append({
                        'trade_date': d,
                        'open': r.get('open', np.nan),
                        'high': r.get('high', np.nan),
                        'low': r.get('low', np.nan),
                        'close': r.get('close', np.nan),
                        'vol': r.get('vol', 0),
                        'pct_chg': r.get('pct_chg', np.nan),
                    })
        
        if len(rows) < 5:
            return None, f"本地快照数据不足({len(rows)}天)"
        
        df = pd.DataFrame(rows)
        print(f"  📊 本地快照拼接 {len(df)} 根K线, 日期: {df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
        return df, "本地快照"
    except Exception as e:
        return None, f"本地快照异常: {e}"


def get_kline_ashare(ts_code, count=30):
    """用Ashare获取日线K线数据"""
    if not ASHARE_AVAILABLE:
        return None, "Ashare不可用"
    try:
        ashare_code = ts_code_to_ashare(ts_code)
        df = ashare_get_price(ashare_code, frequency='1d', count=count)
        if df is None or len(df) == 0:
            return None, "Ashare返回空数据"
        df = df.reset_index()
        date_col = df.columns[0]
        df['trade_date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y%m%d')
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if cl == 'volume': col_map[c] = 'vol'
        df = df.rename(columns=col_map)
        required = ['open', 'high', 'low', 'close', 'vol']
        for r in required:
            if r not in df.columns:
                return None, f"Ashare缺少列{r}"
        for r in required:
            df[r] = df[r].astype(float)
        df['pct_chg'] = df['close'].pct_change() * 100
        df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'pct_chg']].copy()
        df = df.dropna(subset=['trade_date'])
        df = df[df['trade_date'] != ''].reset_index(drop=True)
        print(f"  📊 Ashare返回 {len(df)} 根K线, 日期: {df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
        return df, "Ashare"
    except Exception as e:
        return None, f"Ashare异常: {e}"


def get_kline_tushare(ts_code, start_date, end_date):
    """用Tushare获取日线K线数据"""
    df = ts_api("daily", {"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
                "ts_code,trade_date,open,high,low,close,pct_chg,vol")
    time.sleep(0.3)
    if df.empty:
        return None, "Tushare返回空数据"
    df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'pct_chg']].copy()
    df.sort_values('trade_date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df, "Tushare"


def get_kline(ts_code, trade_dates, t0_idx, lookback=30):
    """
    获取K线数据，本地CSV优先 → 本地快照 → Ashare → Tushare
    返回: (DataFrame, 数据源名称)
    """
    # 1. 优先本地CSV（最完整，有历史数据），传入trade_dates和t0_idx以便补充缺失天数
    df, src = get_kline_local(ts_code, count=lookback, trade_dates=trade_dates, t0_idx=t0_idx)
    if df is not None and len(df) >= 10:
        return df, src

    # 2. 本地快照拼接
    df, src = get_kline_snapshot(ts_code, trade_dates, t0_idx, lookback)
    if df is not None and len(df) >= 10:
        return df, src

    # 3. Ashare
    df, src = get_kline_ashare(ts_code, count=lookback)
    if df is not None and len(df) >= 10:
        print(f"  📊 K线数据来源: Ashare ({len(df)}根)")
        return df, src

    # 4. 兜底Tushare
    start_date = trade_dates[max(0, t0_idx - lookback)]
    end_date = trade_dates[t0_idx]
    df, src = get_kline_tushare(ts_code, start_date, end_date)
    if df is not None and len(df) >= 5:
        print(f"  📊 K线数据来源: Tushare ({len(df)}根)")
        return df, src

    return pd.DataFrame(), "无数据"


# ===== 实时行情获取（Ashare优先） =====

def get_realtime_price_ashare(ts_code):
    """用Ashare获取最新价格（日线最后一根）"""
    if not ASHARE_AVAILABLE:
        return None
    try:
        ashare_code = ts_code_to_ashare(ts_code)
        df = ashare_get_price(ashare_code, frequency='1d', count=1)
        if df is not None and len(df) > 0:
            return float(df['close'].iloc[-1])
    except:
        pass
    return None


# ===== 涨停池数据（本地快照优先 → AKShare → Tushare） =====

def get_zt_data(date_str, trade_dates=None):
    """获取涨停池数据，本地快照优先"""
    cache_key = f"zt_data_{date_str}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    result = {'zt_cnt': 0, 'ind_zt_dict': {}, 'source': '无数据'}

    # 1. 优先从本地快照统计
    snap = load_snapshot(date_str)
    if snap is not None and 'pct_chg' in snap.columns:
        zt_df = snap[snap['pct_chg'] >= 9.5].copy()
        zb_approx = snap[(snap['pct_chg'] >= 8.0) & (snap['pct_chg'] < 9.5)]  # 近似炸板
        
        result['zt_cnt'] = len(zt_df)
        result['zb_cnt'] = len(zb_approx)
        result['fbl'] = round(len(zt_df) / (len(zt_df) + len(zb_approx)) * 100, 1) if (len(zt_df) + len(zb_approx)) > 0 else 0
        
        if 'industry' in zt_df.columns:
            result['ind_zt_dict'] = zt_df['industry'].value_counts().to_dict()
        
        if 'ts_code' in zt_df.columns:
            # 转为6位代码格式
            result['zt_codes'] = set(zt_df['ts_code'].str[:6].tolist())
        
        result['source'] = f'本地快照({date_str})'
        print(f"  🔥 涨停池来源: 本地快照 ({date_str}, 涨停{result['zt_cnt']})")
        cache.set(cache_key, result)
        return result

    # 2. 回溯查找
    if trade_dates:
        dates_to_try = []
        try:
            idx = trade_dates.index(date_str)
            for i in range(1, 4):
                if idx - i >= 0:
                    dates_to_try.append(trade_dates[idx - i])
        except ValueError:
            pass
        
        for d in dates_to_try:
            snap = load_snapshot(d)
            if snap is not None and 'pct_chg' in snap.columns:
                zt_df = snap[snap['pct_chg'] >= 9.5].copy()
                result['zt_cnt'] = len(zt_df)
                if 'industry' in zt_df.columns:
                    result['ind_zt_dict'] = zt_df['industry'].value_counts().to_dict()
                if 'ts_code' in zt_df.columns:
                    result['zt_codes'] = set(zt_df['ts_code'].str[:6].tolist())
                result['source'] = f'本地快照(回溯{d})'
                print(f"  🔥 涨停池来源: 本地快照 (回溯{d}, 涨停{result['zt_cnt']})")
                cache.set(cache_key, result)
                return result

    # 3. AKShare兜底
    if AKSHARE_AVAILABLE:
        try:
            zt_df = ak.stock_zt_pool_em(date=date_str)
            time.sleep(0.3)
            zb_df = ak.stock_zt_pool_zbgc_em(date=date_str)
            time.sleep(0.3)
            if len(zt_df) > 0:
                result['zt_cnt'] = len(zt_df)
                result['zb_cnt'] = len(zb_df)
                result['fbl'] = round(len(zt_df) / (len(zt_df) + len(zb_df)) * 100, 1) if (len(zt_df) + len(zb_df)) > 0 else 0
                if '所属行业' in zt_df.columns:
                    result['ind_zt_dict'] = zt_df['所属行业'].value_counts().to_dict()
                if '代码' in zt_df.columns:
                    result['zt_codes'] = set(zt_df['代码'].tolist())
                result['source'] = 'AKShare'
                cache.set(cache_key, result)
                return result
        except Exception as e:
            print(f"  ⚠ AKShare涨停池获取失败: {e}")

    print(f"  🔥 涨停池: 无数据")
    cache.set(cache_key, result)
    return result

# ===== 基本面数据（本地快照优先 → Tushare兜底） =====

def get_basic_data(ts_code, trade_date, trade_dates=None):
    """获取基本面数据，本地快照优先"""
    result = {'pe': None, 'mv': 0, 'tr': 0, 'vr': 0, 'source': '无数据'}
    
    # 构建要尝试的日期列表
    dates_to_try = [trade_date]
    if trade_dates:
        try:
            idx = trade_dates.index(trade_date)
            for i in range(1, 6):
                if idx - i >= 0:
                    dates_to_try.append(trade_dates[idx - i])
        except ValueError:
            pass
    
    # 1. 优先从本地快照读取
    for d in dates_to_try:
        snap = load_snapshot(d)
        if snap is not None:
            row = snap[snap['ts_code'] == ts_code]
            if not row.empty:
                r = row.iloc[0]
                pe_val = r.get('pe_ttm', r.get('pe', None))
                mv_val = r.get('total_mv', 0)
                tr_val = r.get('turnover_rate_f', r.get('turnover_rate', 0))
                vr_val = r.get('volume_ratio', 0)
                
                result['pe'] = float(pe_val) if pd.notna(pe_val) else None
                result['mv'] = float(mv_val) / 10000 if pd.notna(mv_val) and mv_val else 0
                result['tr'] = float(tr_val) if pd.notna(tr_val) and tr_val else 0
                result['vr'] = float(vr_val) if pd.notna(vr_val) and vr_val else 0
                result['source'] = f'本地快照({d})' if d != trade_date else '本地快照'
                if d != trade_date:
                    print(f"  📋 基本面数据回溯到 {d} (本地)")
                return result
    
    # 2. Tushare兜底
    for d in dates_to_try:
        bas = ts_api("daily_basic", {"ts_code": ts_code, "trade_date": d},
                     "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio")
        time.sleep(0.2)
        if not bas.empty:
            result['pe'] = bas["pe_ttm"].iloc[0]
            result['mv'] = float(bas["total_mv"].iloc[0]) / 10000 if bas["total_mv"].iloc[0] else 0
            result['tr'] = float(bas["turnover_rate_f"].iloc[0]) if bas["turnover_rate_f"].iloc[0] else 0
            result['vr'] = float(bas["volume_ratio"].iloc[0]) if bas["volume_ratio"].iloc[0] else 0
            result['source'] = f'Tushare({d})' if d != trade_date else 'Tushare'
            return result
    
    print(f"  ⚠ 基本面数据获取失败")
    return result


# ===== 资金流向（本地快照优先 → Tushare兜底） =====

def get_moneyflow(ts_code, trade_date, trade_dates=None):
    """获取资金流向数据，本地快照优先"""
    # 构建要尝试的日期列表
    dates_to_try = [trade_date]
    if trade_dates:
        try:
            idx = trade_dates.index(trade_date)
            for i in range(1, 6):
                if idx - i >= 0:
                    dates_to_try.append(trade_dates[idx - i])
        except ValueError:
            pass
    
    # 1. 优先从本地快照读取（net_mf_amount字段）
    for d in dates_to_try:
        snap = load_snapshot(d)
        if snap is not None and 'net_mf_amount' in snap.columns:
            row = snap[snap['ts_code'] == ts_code]
            if not row.empty:
                net_mf = row.iloc[0].get('net_mf_amount', 0)
                if pd.notna(net_mf):
                    nb_yi = float(net_mf) / 10000  # 万元 → 亿元
                    src = f'本地快照({d})' if d != trade_date else '本地快照'
                    if d != trade_date:
                        print(f"  💰 资金流向回溯到 {d} (本地)")
                    return {'net_inflow_yi': nb_yi, 'source': src}
    
    # 2. Tushare兜底
    for d in dates_to_try:
        mf = ts_api("moneyflow", {"ts_code": ts_code, "trade_date": d},
                    "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
        time.sleep(0.2)
        if not mf.empty:
            nb = (float(mf["buy_elg_amount"].iloc[0] or 0) + float(mf["buy_lg_amount"].iloc[0] or 0)
                  - float(mf["sell_elg_amount"].iloc[0] or 0) - float(mf["sell_lg_amount"].iloc[0] or 0))
            nb_yi = nb / 10000
            src = f'Tushare({d})' if d != trade_date else 'Tushare'
            return {'net_inflow_yi': nb_yi, 'source': src}
    
    return {'net_inflow_yi': 0, 'source': '无数据'}


# ===== 主线热点计算（本地快照优先 → Tushare兜底） =====

def calc_mainline_scores(T0, trade_dates):
    """计算主线热点得分，本地快照优先"""
    cache_key = f"mainline_{T0}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    stock_list = get_stock_list()
    ind_map = stock_list["ind_map"]

    t0_idx = trade_dates.index(T0)
    dates7_start = max(0, t0_idx - 6)
    dates7 = trade_dates[dates7_start:t0_idx + 1]

    daily_data = {}
    data_source = "未知"
    
    # 1. 优先从本地快照加载
    local_ok = True
    for d in dates7:
        snap = load_snapshot(d)
        if snap is not None:
            # 快照已包含ts_code, pct_chg, amount, open, high, low, close, vol等
            daily_data[d] = snap
        else:
            local_ok = False
            break
    
    if local_ok and len(daily_data) == len(dates7):
        data_source = "本地快照"
        print(f"  🏠 主线热点: 从本地快照加载 {len(dates7)} 天数据")
    else:
        # 2. Tushare兜底
        daily_data = {}
        data_source = "Tushare"
        print(f"  🌐 主线热点: 从Tushare加载 {len(dates7)} 天数据")
        for d in dates7:
            df = ts_api("daily", {"trade_date": d}, "ts_code,pct_chg,amount,open,high,low,close,vol")
            time.sleep(0.3)
            daily_data[d] = df

    ind_perf = {}
    for d in dates7:
        df = daily_data.get(d, pd.DataFrame())
        if df is None or df.empty:
            continue
        df = df.copy()
        df["ind"] = df["ts_code"].map(ind_map)
        # 确保pct_chg是数值
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors='coerce')
        grp = df.groupby("ind").agg(
            avg=("pct_chg", "mean"),
            lim=("pct_chg", lambda x: (x >= 9.5).sum())
        ).reset_index()
        grp["rk"] = grp["avg"].rank(ascending=False)
        for _, row in grp.iterrows():
            if row["ind"] not in ind_perf:
                ind_perf[row["ind"]] = []
            ind_perf[row["ind"]].append({
                "avg": row["avg"], "rk": int(row["rk"]), "lim": int(row["lim"])
            })

    mainline_scores = {}
    for ind, perfs in ind_perf.items():
        avg_rk = np.mean([p["rk"] for p in perfs])
        total_lim = sum(p["lim"] for p in perfs)
        top20_days = sum(1 for p in perfs if p["rk"] <= 20)

        score = 0
        if avg_rk <= 10: score += 8
        elif avg_rk <= 20: score += 5
        elif avg_rk <= 30: score += 3
        elif avg_rk <= 50: score += 1

        if top20_days >= 5: score += 6
        elif top20_days >= 3: score += 5
        elif top20_days >= 2: score += 4
        elif top20_days >= 1: score += 2

        if total_lim >= 30: score += 6
        elif total_lim >= 15: score += 5
        elif total_lim >= 10: score += 4
        elif total_lim >= 5: score += 3
        elif total_lim >= 2: score += 1

        mainline_scores[ind] = min(score, 20)

    # 板块涨停数
    ind_zt_map = {}
    d0_df = daily_data.get(T0, pd.DataFrame())
    if d0_df is not None and not d0_df.empty:
        d0_c = d0_df.copy()
        d0_c["ind"] = d0_c["ts_code"].map(ind_map)
        d0_c["pct_chg"] = pd.to_numeric(d0_c["pct_chg"], errors='coerce')
        for ind_name, grp in d0_c.groupby("ind"):
            ind_zt_map[ind_name] = int((grp["pct_chg"] >= 9.5).sum())

    result = {
        "mainline_scores": mainline_scores,
        "ind_zt_map": ind_zt_map,
        "daily_T0": d0_df,
        "data_source": data_source,
    }
    cache.set(cache_key, result)
    return result


# ===== 9 Skill 评分函数（对齐 sector_deep_pick_v2.py v3.3） =====

# 默认权重
DEFAULT_WEIGHTS = {
    "TXCG": 15,     # S1
    "元子元": 10,    # S2
    "山茶花": 15,    # S3
    "Mistery": 10,  # S4
    "TDS": 10,      # S5
    "百胜WR": 15,   # S6
    "事件驱动": 10,  # S7
    "多周期": 5,     # S8
    "基本面": 10,    # S9
}

WEIGHTS_FILE = os.path.expanduser("~/WorkBuddy/Claw/track/skill_weights.json")

def load_skill_weights():
    """加载动态权重"""
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE, 'r') as f:
                data = json.load(f)
                w = data.get('weights', DEFAULT_WEIGHTS)
                for k, v in DEFAULT_WEIGHTS.items():
                    if k not in w:
                        w[k] = v
                return w
        except:
            pass
    return DEFAULT_WEIGHTS.copy()


def _score_s1_txcg(c, sector_zt, emotion_stage, max_score, bci_score=0):
    """S1-TXCG(天时+地利+人和) — 9分制量化"""
    tags = []
    chg5 = c.get('chg5', 0); chg1 = c.get('pct_chg', 0)
    is_zt = c.get('is_zt', False); ma_tag = c.get('ma_tag', '弱')
    closes = c.get('closes', [])

    # 天时(0-3)
    tianshi = 0
    if emotion_stage == '起爆': tianshi = 3
    elif emotion_stage == '一致': tianshi = 3
    elif emotion_stage == '修复': tianshi = 2
    elif emotion_stage == '分歧': tianshi = 2
    elif emotion_stage == '启动': tianshi = 2
    elif emotion_stage == '冰点': tianshi = 1
    elif emotion_stage == '退潮': tianshi = 0
    elif emotion_stage == '主升': tianshi = 3
    if sector_zt >= 10 and tianshi < 3: tianshi = min(3, tianshi + 1)

    # 地利(0-3)
    dili = 0
    if ma_tag == '多头': dili += 1
    if chg5 < -10: dili += 1; tags.append(f"深超跌{chg5:+.0f}%")
    elif chg5 < -5: dili += 1; tags.append(f"超跌{chg5:+.0f}%")
    elif -5 <= chg5 <= 5: dili += 1; tags.append(f"安全{chg5:+.0f}%")
    if len(closes) >= 5:
        big_drop = 0
        for i in range(-5, 0):
            idx = i + len(closes)
            if idx >= 1 and closes[idx] < closes[idx-1] * 0.95:
                big_drop += 1
        if big_drop == 0: dili += 1
        elif big_drop >= 2: tags.append("⚠前面搅浑")
    else:
        dili += 1
    dili = min(3, dili)

    # 人和(0-3)
    renhe = 0
    if bci_score >= 70:
        renhe += 2; tags.append(f"BCI={bci_score}板块极完整")
    elif bci_score >= 50:
        renhe += 2; tags.append(f"BCI={bci_score}板块较完整")
    elif bci_score >= 30:
        renhe += 1; tags.append(f"BCI={bci_score}板块一般")
    elif sector_zt >= 10:
        renhe += 2; tags.append(f"板块{sector_zt}家涨停")
    elif sector_zt >= 5:
        renhe += 1; tags.append(f"板块{sector_zt}家涨停")
    elif sector_zt >= 3:
        renhe += 1
    if is_zt: renhe += 1; tags.append("涨停=股性好")
    elif chg1 >= 7: renhe += 1
    renhe = min(3, renhe)

    raw_9 = tianshi + dili + renhe
    s = int(raw_9 / 9 * max_score + 0.5)
    if raw_9 >= 8: tags.append(f"天地人{raw_9}/9🔥重仓")
    elif raw_9 >= 7: tags.append(f"天地人{raw_9}/9✅可参与")
    else: tags.append(f"天地人{raw_9}/9")
    return min(max_score, s), tags


def _score_s2_yuanziyuan(c, max_score):
    """S2-元子元(情绪周期+个股情绪状态)"""
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0)
    is_zt = c.get('is_zt', False)
    vol_ratio = c.get('vol_ratio', 1)

    # 个股情绪阶段判定
    stage = '未知'
    if is_zt and chg5 < -5:
        stage = '冰点启动'; s += 5; tags.append("🔥冰点涨停=最佳买点")
    elif is_zt and chg5 < 5:
        stage = '发酵确认'; s += 4; tags.append("发酵确认")
    elif chg1 >= 5 and chg5 < 0:
        stage = '冰点启动'; s += 4; tags.append("超跌大涨")
    elif is_zt and 5 <= chg5 < 15:
        stage = '主升加速'; s += 3; tags.append("主升涨停")
    elif chg1 >= 5 and 0 <= chg5 < 10:
        stage = '发酵确认'; s += 3; tags.append("大阳安全")
    elif is_zt and chg5 >= 15:
        if vol_ratio >= 3:
            stage = '高潮见顶'; s += 0; tags.append("⚠爆量高位涨停=见顶")
        else:
            stage = '主升加速'; s += 2; tags.append("⚠高位涨停")
    elif chg1 > 0 and chg5 < 10:
        stage = '发酵确认'; s += 2
    elif chg1 > 0 and chg5 >= 10:
        stage = '主升加速'; s += 1
    elif chg1 < -3 and chg5 > 15:
        stage = '退潮补跌'; s += 0; tags.append("⚠退潮补跌")
    elif chg1 <= 0:
        if chg5 < -10:
            stage = '冰点启动'; s += 2; tags.append("深跌待启动")
        else:
            stage = '分歧换手'; s += 1

    # 量价关系加分
    if chg1 > 3 and vol_ratio < 1.2:
        s += 2; tags.append("缩量上涨")
    elif chg1 < 2 and vol_ratio > 2.5:
        s -= 1; tags.append("⚠放量滞涨")

    # 连板接力节点加分
    if is_zt and 0 <= chg5 <= 10:
        s += 1

    tags.append(f"情绪:{stage}")
    return min(max_score, max(0, s)), tags


def _score_s3_camellia(c, sector_zt, max_score, bci_score=0):
    """S3-山茶花(龙头三维评分15分制)"""
    tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0); is_zt = c.get('is_zt', False)

    # 主动性(1-5)
    active = 1
    if is_zt and chg1 >= 19:
        active = 5; tags.append("主动5:20cm涨停")
    elif is_zt:
        active = 4; tags.append("主动4:涨停")
    elif chg1 >= 7:
        active = 3; tags.append("主动3:大涨")
    elif chg1 >= 5:
        active = 2

    # 带动性(1-5) BCI加权
    drive = 1
    if bci_score >= 70:
        drive = 5; tags.append(f"带动5:BCI={bci_score}板块极完整")
    elif bci_score >= 55:
        drive = 4; tags.append(f"带动4:BCI={bci_score}")
    elif bci_score >= 40:
        drive = 3; tags.append(f"带动3:BCI={bci_score}")
    elif sector_zt >= 10:
        drive = 5; tags.append(f"带动5:板块{sector_zt}家涨停")
    elif sector_zt >= 7:
        drive = 4
    elif sector_zt >= 5:
        drive = 3
    elif sector_zt >= 3:
        drive = 2

    # 抗跌性(1-5)
    resist = 1
    if is_zt and chg5 < -5:
        resist = 5; tags.append("抗跌5:超跌涨停穿越")
    elif is_zt and chg5 < 5:
        resist = 4; tags.append("抗跌4:低位涨停")
    elif chg1 > 0 and chg5 < -3:
        resist = 4; tags.append("抗跌4:逆势上涨")
    elif chg1 > 0 and chg5 < 0:
        resist = 3
    elif chg1 > 0:
        resist = 2

    dragon_score = active + drive + resist
    if dragon_score >= 12:
        tags.append(f"🐉龙头确认{dragon_score}/15")
    elif dragon_score >= 9:
        tags.append(f"准龙头{dragon_score}/15")
    else:
        tags.append(f"非龙头{dragon_score}/15")

    s = int(dragon_score / 15 * max_score + 0.5)
    return min(max_score, s), tags


def _score_s4_mistery(c, max_score):
    """S4-Mistery(M1趋势+M2买点+M3卖点+M4量价+M5形态+M6仓位管理)"""
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); vr = c.get('vol_ratio', 0)
    bbw = c.get('bbw', 0); ma_tag = c.get('ma_tag', '弱')
    closes = c.get('closes', []); highs = c.get('highs', [])
    ma5 = c.get('ma5', 0); ma20 = c.get('ma20', 0)

    # M1趋势(0-3)
    if ma_tag == '多头': s += 3
    elif ma_tag == '短多': s += 2
    elif chg1 > 0: s += 1

    # M2买点(0-3)
    m2 = 0
    if len(closes) >= 7 and ma5 > 0 and ma20 > 0:
        old_closes = closes[-7:-2]
        if len(old_closes) >= 5:
            old_ma5 = np.mean(old_closes[-5:])
            old_ma20 = np.mean(closes[-min(20, len(closes)):-5]) if len(closes) > 5 else old_ma5
            if old_ma5 <= old_ma20 and ma5 > ma20:
                m2 += 2; tags.append("520金叉")
    if len(closes) >= 5 and ma5 > 0:
        below_ma5 = any(closes[i] < np.mean(closes[max(0,i-4):i+1]) for i in range(-5, -1) if i+len(closes) >= 0)
        if below_ma5 and closes[-1] > ma5:
            m2 += 1; tags.append("破五反五")
    if bbw < 0.12 and chg1 > 3:
        m2 += 2; tags.append(f"BBW={bbw:.3f}极低起爆")
    elif bbw < 0.15 and chg1 > 3:
        m2 += 1; tags.append(f"BBW={bbw:.3f}低")
    s += min(3, m2)

    # M3卖点(-2~0)
    if chg1 < 2 and vr > 2.5:
        s -= 1; tags.append("⚠M3放量滞涨")
    if len(highs) >= 4:
        recent_high = max(highs[-4:-1])
        if highs[-1] < recent_high and chg1 < 1:
            s -= 1; tags.append("⚠M3滞涨不创新高")

    # M4量价(0-2)
    if chg1 > 3 and vr >= 2:
        s += 2; tags.append("量价齐升")
    elif chg1 > 0 and vr >= 1.5:
        s += 1

    # M5形态(0-2)
    if len(closes) >= 3 and len(highs) >= 3:
        if chg1 > 5 and vr > 1.5:
            prev_range = abs(closes[-2] - closes[-3]) / closes[-3] * 100 if closes[-3] > 0 else 0
            if prev_range < 2:
                s += 2; tags.append("M5空中加油")
        if len(highs) >= 2:
            prev_upper = (highs[-2] - closes[-2]) / closes[-2] * 100 if closes[-2] > 0 else 0
            if prev_upper > 3 and closes[-1] > highs[-2] * 0.98:
                s += 1; tags.append("M5仙人指路收复")

    # M6仓位管理(0-5)
    m6 = 0
    ma5_val = c.get('ma5', 0); ma10_val = c.get('ma10', 0); ma20_val = c.get('ma20', 0)
    if len(closes) >= 1 and ma5_val > 0 and ma10_val > 0 and ma20_val > 0:
        if closes[-1] > ma5_val > ma10_val > ma20_val and chg1 > 0:
            m6 += 2
        elif closes[-1] > ma5_val > ma10_val:
            m6 += 1
    if ma20_val > 0 and len(closes) >= 1 and abs(closes[-1] - ma20_val) / ma20_val < 0.08:
        m6 += 1
    if len(closes) >= 20:
        lows_list = c.get('lows', [])
        if lows_list and len(lows_list) >= 20:
            support_20 = min(lows_list[-20:])
            if closes[-1] > support_20 * 1.05:
                m6 += 1
    if len(closes) >= 7:
        chg_7d = (closes[-1] - closes[-7]) / closes[-7] * 100 if closes[-7] > 0 else 0
        if chg_7d > 5: m6 += 1
        elif chg_7d < -3: m6 -= 1
    s += min(5, max(0, m6))

    return min(max_score, max(0, s)), tags


def _score_s5_tds(c, max_score):
    """S5-TDS(波峰波谷趋势+T1推进+T2吞没+T3突破+T4三K反转+T5反转+T6双向突破)"""
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    highs = c.get('highs', []); lows = c.get('lows', []); closes = c.get('closes', [])

    # 波峰波谷趋势(0-3) 窗口±5
    if len(highs) >= 15:
        peaks = []; troughs = []
        for i in range(5, len(highs)-5):
            if highs[i] >= max(highs[max(0,i-5):i]) and highs[i] >= max(highs[i+1:min(len(highs),i+6)]):
                peaks.append(highs[i])
            if lows[i] <= min(lows[max(0,i-5):i]) and lows[i] <= min(lows[i+1:min(len(lows),i+6)]):
                troughs.append(lows[i])
        if len(peaks) >= 2 and len(troughs) >= 2:
            if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]:
                s += 3; tags.append("上升趋势(峰谷抬高)")
            elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]:
                s += 2; tags.append("趋势转折")
        elif len(peaks) >= 2 and peaks[-1] > peaks[-2]:
            s += 2
    elif len(highs) >= 10:
        rh = max(highs[-5:]); ph = max(highs[-10:-5])
        rl = min(lows[-5:]); pl = min(lows[-10:-5])
        if rh > ph and rl > pl: s += 3; tags.append("上升趋势")
        elif rh > ph: s += 2
        elif rl > pl: s += 1

    # T1推进(0-2)
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            s += 2; tags.append("T1推进")
        elif highs[-1] > highs[-2]:
            s += 1

    # T2吞没(0-1)
    if len(closes) >= 3:
        if closes[-2] < closes[-3] and closes[-1] > closes[-2] and closes[-1] > highs[-2]:
            s += 1; tags.append("T2吞没")

    # T3突破(0-2)
    if is_zt:
        s += 2; tags.append("T3涨停突破")
    elif chg1 >= 7:
        s += 1; tags.append("T3大阳突破")
    if len(closes) >= 20 and len(highs) >= 20:
        prev_high = max(highs[-20:-1])
        if closes[-1] >= prev_high:
            s += 1; tags.append("突破20日前高")

    # T5反转(0-2)
    if len(closes) >= 5 and len(lows) >= 5:
        chg5 = c.get('chg5', 0)
        if chg5 < -10 and chg1 > 3:
            s += 2; tags.append("T5底部反转")
        elif chg5 < -5 and chg1 > 0:
            if len(closes) >= 2:
                body = abs(closes[-1] - closes[-2])
                lower_shadow = closes[-1] - lows[-1] if closes[-1] > lows[-1] else 0
                if lower_shadow > body * 2 and lower_shadow > 0:
                    s += 1; tags.append("T5锤子线")

    # T4三K反转(0-2)
    if len(closes) >= 4 and len(highs) >= 4:
        opens = c.get('opens', [])
        if len(opens) >= 4:
            k1_body = abs(closes[-3] - opens[-3])
            k2_body = abs(closes[-2] - opens[-2])
            k3_body = abs(closes[-1] - opens[-1])
            chg5_val = c.get('chg5', 0)
            if chg5_val < -3:
                if k2_body <= max(k1_body, k3_body):
                    if closes[-1] > opens[-1] and closes[-1] > highs[-2]:
                        s += 2; tags.append("T4看涨三K反转")
            elif chg5_val > 10:
                if k2_body <= max(k1_body, k3_body):
                    if closes[-1] < opens[-1] and closes[-1] < lows[-2]:
                        s -= 1; tags.append("⚠T4看跌三K反转")

    # T6双向突破(0-2)
    if len(highs) >= 15:
        _peaks = []; _troughs = []
        for i in range(5, len(highs)-5):
            if highs[i] >= max(highs[max(0,i-5):i]) and highs[i] >= max(highs[i+1:min(len(highs),i+6)]):
                _peaks.append(highs[i])
            if lows[i] <= min(lows[max(0,i-5):i]) and lows[i] <= min(lows[i+1:min(len(lows),i+6)]):
                _troughs.append(lows[i])
        if len(_peaks) >= 3 and len(_troughs) >= 3:
            p_trend1 = 1 if _peaks[-2] > _peaks[-3] else -1
            p_trend2 = 1 if _peaks[-1] > _peaks[-2] else -1
            t_trend1 = 1 if _troughs[-2] > _troughs[-3] else -1
            t_trend2 = 1 if _troughs[-1] > _troughs[-2] else -1
            if p_trend1 != p_trend2 or t_trend1 != t_trend2:
                if closes[-1] > _peaks[-1]:
                    s += 2; tags.append("T6看涨双向突破")
                elif chg1 > 5:
                    s += 1; tags.append("T6突破信号")

    return min(max_score, max(0, s)), tags


def _score_s6_wr(c, max_score):
    """S6-百胜WR(WR-1首板放量7条件 + WR-2右侧趋势起爆5条件 + WR-3底倍量柱)"""
    tags = []
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    vr = c.get('vol_ratio', 0); bbw = c.get('bbw', 0)
    ma_tag = c.get('ma_tag', '弱'); net = c.get('net_inflow', 0)
    turnover = c.get('turnover', 0)
    circ_mv = c.get('circ_mv', 0)
    mv_yi = circ_mv / 10000 if circ_mv else 0
    closes = c.get('closes', []); highs = c.get('highs', [])

    # WR-1 首板放量涨停模型(0-7)
    wr1 = 0
    wr1_detail = []
    if is_zt:
        wr1 += 1; wr1_detail.append("涨停✅")
        if vr >= 3: wr1 += 1; wr1_detail.append(f"量比{vr:.1f}✅")
        else: wr1_detail.append(f"量比{vr:.1f}❌")
        if turnover and turnover >= 8: wr1 += 1; wr1_detail.append(f"换手{turnover:.0f}%✅")
        else: wr1_detail.append(f"换手{turnover:.0f}%❌" if turnover else "换手?")
        if '多' in ma_tag: wr1 += 1; wr1_detail.append("均线多头✅")
        else: wr1_detail.append(f"均线{ma_tag}❌")
        if 30 <= mv_yi <= 150: wr1 += 1; wr1_detail.append(f"市值{mv_yi:.0f}亿✅")
        else: wr1_detail.append(f"市值{mv_yi:.0f}亿❌")
        if net > 0: wr1 += 1; wr1_detail.append(f"净入{net:+.1f}亿✅")
        else: wr1_detail.append(f"净入{net:+.1f}亿❌")
        zt_time = c.get('zt_time', None)
        if zt_time:
            if zt_time <= "10:30":
                wr1 += 1; wr1_detail.append(f"封板{zt_time}✅")
            else:
                wr1_detail.append(f"封板{zt_time}偏晚❌")
        else:
            wr1_detail.append("封板时间?")
        tags.append(f"WR1={wr1}/7({'|'.join(wr1_detail)})")
        if wr1 >= 6: tags.append("🔥WR1高分")
        elif wr1 >= 5: tags.append("WR1较强")

    # WR-2 右侧趋势起爆模型(0-5)
    wr2 = 0
    wr2_detail = []
    if bbw < 0.12: wr2 += 1; wr2_detail.append(f"BBW={bbw:.3f}极低✅")
    elif bbw < 0.15: wr2 += 1; wr2_detail.append(f"BBW={bbw:.3f}低✅")
    else: wr2_detail.append(f"BBW={bbw:.3f}❌")
    if vr >= 2.5: wr2 += 1; wr2_detail.append(f"倍量{vr:.1f}x✅")
    else: wr2_detail.append(f"量{vr:.1f}x❌")
    if is_zt: wr2 += 1; wr2_detail.append("涨停突破✅")
    elif chg1 >= 7: wr2 += 1; wr2_detail.append(f"大阳{chg1:+.1f}%✅")
    else: wr2_detail.append(f"涨{chg1:+.1f}%❌")
    if '多' in ma_tag: wr2 += 1; wr2_detail.append("均线多头✅")
    else: wr2_detail.append(f"均线{ma_tag}❌")
    if len(closes) >= 20 and len(highs) >= 20:
        prev_high = max(highs[-20:-1])
        if closes[-1] >= prev_high:
            wr2 += 1; wr2_detail.append("突破前高✅")
        else:
            wr2_detail.append("未破前高❌")
    tags.append(f"WR2={wr2}/5({'|'.join(wr2_detail)})")
    if wr2 >= 4: tags.append("🔥WR2起爆")

    # WR-3 底倍量柱短线模型(0-4)
    wr3 = 0
    wr3_detail = []
    kline_60m = c.get('kline_60m', None)
    if kline_60m and len(kline_60m.get('vols', [])) >= 12:
        vols_60 = kline_60m['vols']
        closes_60 = kline_60m['closes']
        highs_60 = kline_60m['highs']
        lows_60 = kline_60m['lows']
        n60 = len(vols_60)
        first_dbl_idx = None
        for i in range(max(1, n60-20), n60):
            if vols_60[i] >= vols_60[i-1] * 2 and closes_60[i] > closes_60[i-1]:
                recent_range = closes_60[max(0, i-20):i+1]
                mid_price = (max(recent_range) + min(recent_range)) / 2
                if closes_60[i] <= mid_price * 1.05:
                    first_dbl_idx = i
                    break
        if first_dbl_idx is not None:
            wr3 += 1; wr3_detail.append("底倍量柱✅")
            first_low = lows_60[first_dbl_idx]
            first_high = highs_60[first_dbl_idx]
            for j in range(first_dbl_idx + 1, n60):
                if vols_60[j] >= vols_60[j-1] * 2:
                    if closes_60[j] > first_high:
                        wr3 += 1; wr3_detail.append("二次倍量确认✅")
                    if lows_60[j] >= first_low:
                        wr3 += 1; wr3_detail.append("支撑不破✅")
                    else:
                        wr3_detail.append("⚠破支撑")
                    break
            if closes_60[-1] >= first_low:
                wr3 += 1; wr3_detail.append(f"支撑{first_low:.2f}上方✅")
            else:
                wr3_detail.append(f"⚠已破支撑{first_low:.2f}")
            tags.append(f"WR3={wr3}/4({'|'.join(wr3_detail)})")
            if wr3 >= 3: tags.append("🔥WR3底倍量确认")
        else:
            tags.append("WR3=0/4(无信号)")
    else:
        tags.append("WR3=N/A(无60m数据)")

    # 取三个模型的高分映射到max_score
    best_raw = max(wr1, wr2, wr3)
    if best_raw == wr1: best_max = 7
    elif best_raw == wr2: best_max = 5
    else: best_max = 4
    s = int(best_raw / best_max * max_score + 0.5) if best_max > 0 else 0
    return min(max_score, s), tags


def _score_s7_event(c, sector_zt, max_score, bci_score=0):
    """S7-事件驱动(板块效应+低位埋伏+资金流向+股性弹性)"""
    s = 0; tags = []
    chg5 = c.get('chg5', 0)
    net = c.get('net_inflow', 0); is_zt = c.get('is_zt', False)
    chg1 = c.get('pct_chg', 0)

    # 板块效应(0-3) BCI加权
    if bci_score >= 60:
        s += 3; tags.append(f"BCI={bci_score}事件催化强")
    elif bci_score >= 40:
        s += 2; tags.append(f"BCI={bci_score}事件催化中")
    elif sector_zt >= 10: s += 3; tags.append(f"事件催化强({sector_zt}家涨停)")
    elif sector_zt >= 7: s += 2
    elif sector_zt >= 5: s += 2
    elif sector_zt >= 3: s += 1

    # 低位埋伏(0-3)
    if chg5 < -5: s += 3; tags.append(f"低位埋伏{chg5:+.0f}%")
    elif chg5 < 0: s += 2; tags.append(f"低位{chg5:+.0f}%")
    elif chg5 < 5: s += 2
    elif chg5 < 10: s += 1
    elif chg5 >= 15: s -= 1; tags.append(f"⚠已涨{chg5:+.0f}%非低位")

    # 资金流向(0-2)
    if net > 1: s += 2; tags.append(f"大资金{net:+.1f}亿")
    elif net > 0.3: s += 1
    elif net < -1: s -= 1; tags.append(f"❌净出{net:.1f}亿")

    # 股性弹性(0-2)
    if is_zt: s += 2; tags.append("涨停=弹性好")
    elif chg1 >= 7: s += 1; tags.append("大涨=有弹性")

    return min(max_score, max(0, s)), tags


def _score_s8_multi_period(c, max_score):
    """S8-多周期共振(大周期+中周期+小周期)"""
    tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0)
    ma_tag = c.get('ma_tag', '弱')
    closes = c.get('closes', [])
    ma20 = c.get('ma20', 0)

    # 大周期(±3)
    big = 0
    if ma_tag == '多头': big = 3
    elif ma_tag == '短多': big = 1
    elif len(closes) >= 1 and ma20 > 0:
        if closes[-1] > ma20: big = 1
        elif closes[-1] < ma20 * 0.95: big = -2
        else: big = -1

    # 中周期(±2)
    mid = 0
    if chg5 > 5: mid = 2
    elif chg5 > 2: mid = 1
    elif chg5 > -2: mid = 0
    elif chg5 > -5: mid = -1
    else: mid = -2

    # 小周期(±1)
    small = 0
    if chg1 > 3: small = 1
    elif chg1 > 0: small = 0
    elif chg1 > -2: small = 0
    else: small = -1

    raw_score = big + mid + small
    if raw_score >= 5: tags.append(f"三周期共振{raw_score:+d}🔥")
    elif raw_score >= 3: tags.append(f"多周期偏多{raw_score:+d}")
    elif raw_score <= -3: tags.append(f"多周期偏空{raw_score:+d}⚠")

    s = int((raw_score + 6) / 12 * max_score + 0.5)
    return min(max_score, max(0, s)), tags


def _score_s9_fundamental(c, max_score):
    """S9-基本面(PE+市值+利润增速+行业地位+ROE)"""
    s = 0; tags = []
    pe = c.get('pe', None)
    circ_mv = c.get('circ_mv', None)
    profit_yoy = c.get('profit_yoy', None)
    roe = c.get('roe', None)
    industry_rank = c.get('industry_mv_rank', 99)

    # PE估值
    if pe is not None and pe > 0:
        if pe < 15: s += 3; tags.append(f"PE={pe:.0f}低估")
        elif pe < 25: s += 2; tags.append(f"PE={pe:.0f}")
        elif pe < 40: s += 1
        elif pe > 200: s -= 1; tags.append(f"PE={pe:.0f}⚠")
    elif pe is not None and pe < 0:
        s -= 1; tags.append("PE<0亏损")

    # 流通市值甜区(30-150亿)
    if circ_mv is not None:
        mv_yi = circ_mv / 10000
        if 30 <= mv_yi <= 150: s += 2; tags.append(f"市值{mv_yi:.0f}亿✅")
        elif 15 <= mv_yi < 30: s += 1; tags.append(f"市值{mv_yi:.0f}亿小")
        elif 150 < mv_yi <= 500: s += 1; tags.append(f"市值{mv_yi:.0f}亿")
        elif mv_yi > 500: s += 0; tags.append(f"市值{mv_yi:.0f}亿大")
        else: s += 0; tags.append(f"市值{mv_yi:.0f}亿微")

    # 净利润增速
    if profit_yoy is not None:
        if profit_yoy > 50: s += 2; tags.append(f"利润+{profit_yoy:.0f}%🔥")
        elif profit_yoy > 20: s += 2; tags.append(f"利润+{profit_yoy:.0f}%")
        elif profit_yoy > 0: s += 1
        elif profit_yoy < -20: s -= 1; tags.append(f"利润{profit_yoy:.0f}%⚠")

    # 行业地位
    if industry_rank <= 3: s += 2; tags.append(f"行业TOP{industry_rank}")
    elif industry_rank <= 10: s += 1; tags.append(f"行业TOP{industry_rank}")

    # ROE
    if roe is not None:
        if roe > 15: s += 1; tags.append(f"ROE={roe:.0f}%优")
        elif roe < 0: s -= 1

    return min(max_score, max(0, s)), tags


def _calc_txcg_bonus(c, sector_zt):
    """TXCG六大模型量化加分(0-5)"""
    txcg_bonus = 0; tags = []
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    chg5 = c.get('chg5', 0)
    closes = c.get('closes', []); highs = c.get('highs', [])
    opens = c.get('opens', []); lows = c.get('lows', [])
    ma5 = c.get('ma5', 0)

    # 模型1：连板竞争
    if is_zt and sector_zt >= 3:
        txcg_bonus += 1
    # 模型2：分歧期策略
    if chg5 < -5 and chg1 > 3:
        txcg_bonus += 1
    # 模型3：反包修复
    if len(closes) >= 3 and len(opens) >= 3:
        prev_chg = (closes[-2] - closes[-3]) / closes[-3] * 100 if closes[-3] > 0 else 0
        if prev_chg < -3 and chg1 > 2:
            txcg_bonus += 1; tags.append("TXCG反包")
    # 模型4：承接战法
    if ma5 > 0 and len(closes) >= 1 and abs(closes[-1] - ma5) / ma5 < 0.02 and chg1 > 0:
        txcg_bonus += 1
    # 模型5：大长腿
    if len(closes) >= 2 and len(opens) >= 2 and len(lows) >= 2:
        body_prev = abs(closes[-2] - opens[-2])
        lower_shadow_prev = min(closes[-2], opens[-2]) - lows[-2]
        if lower_shadow_prev > body_prev * 2 and lower_shadow_prev > 0 and chg1 > 0:
            txcg_bonus += 1; tags.append("TXCG大长腿")
    # 模型6：唯一性
    if is_zt and sector_zt == 1:
        txcg_bonus += 1; tags.append("TXCG唯一涨停")

    return min(txcg_bonus, 5), tags


# ===== 核心评分函数 =====

def score_single_stock(ts_code):
    """对单只股票进行9 Skill综合评分（对齐sector_deep_pick_v2.py v3.3）"""
    stock_list = get_stock_list()
    name_map = stock_list["name_map"]
    ind_map = stock_list["ind_map"]

    name = name_map.get(ts_code, "未知")
    ind = ind_map.get(ts_code, "未知")

    weights = load_skill_weights()

    trade_dates = get_trade_dates()
    T0 = get_latest_trade_date()
    if not T0:
        return {"error": "无法获取最新交易日"}

    t0_idx = trade_dates.index(T0)

    # 记录数据源使用情况
    data_sources = {}

    # ===== 获取主线数据（用于板块涨停数等） =====
    ml_data = calc_mainline_scores(T0, trade_dates)
    ind_zt_map = ml_data["ind_zt_map"]
    data_sources["主线热度"] = ml_data.get("data_source", "Tushare")

    # ===== 获取K线数据（Ashare优先） =====
    kdf, kline_src = get_kline(ts_code, trade_dates, t0_idx, lookback=30)
    data_sources["K线数据"] = kline_src

    # ===== 从K线提取技术指标 =====
    c_data = {}  # 构建与sector_deep_pick_v2.py兼容的数据字典
    c0 = 0
    if kdf is not None and not kdf.empty and len(kdf) >= 5:
        kdf_sorted = kdf.sort_values('trade_date').reset_index(drop=True)
        n = len(kdf_sorted)
        cc = kdf_sorted['close'].astype(float).values.tolist()
        hh = kdf_sorted['high'].astype(float).values.tolist()
        ll = kdf_sorted['low'].astype(float).values.tolist()
        vv = kdf_sorted['vol'].astype(float).values.tolist()
        oo = kdf_sorted['open'].astype(float).values.tolist() if 'open' in kdf_sorted.columns else cc[:]
        c0 = cc[-1]

        # 涨跌幅
        pct_last = float(kdf_sorted['pct_chg'].iloc[-1]) if 'pct_chg' in kdf_sorted.columns and pd.notna(kdf_sorted['pct_chg'].iloc[-1]) else 0
        chg5 = (cc[-1] / cc[-6] - 1) * 100 if len(cc) >= 6 and cc[-6] > 0 else 0
        chg10 = (cc[-1] / cc[-11] - 1) * 100 if len(cc) >= 11 and cc[-11] > 0 else 0

        # 均线
        ma5 = float(np.mean(cc[-5:])) if len(cc) >= 5 else cc[-1]
        ma10 = float(np.mean(cc[-10:])) if len(cc) >= 10 else float(np.mean(cc))
        ma20 = float(np.mean(cc[-20:])) if len(cc) >= 20 else float(np.mean(cc))

        if ma5 > ma10 > ma20: ma_tag = '多头'
        elif ma5 > ma10: ma_tag = '短多'
        else: ma_tag = '弱'

        # 量比
        avg_v5 = float(np.mean(vv[-6:-1])) if len(vv) >= 6 else float(np.mean(vv))
        vol_ratio = vv[-1] / avg_v5 if avg_v5 > 0 else 0

        # BBW
        std20 = float(np.std(cc[-20:])) if len(cc) >= 20 else float(np.std(cc))
        bbw = (4 * std20) / ma20 if ma20 > 0 else 0

        is_zt = pct_last >= 9.5

        c_data = {
            'code': ts_code, 'name': name, 'industry': ind,
            'close': c0, 'pct_chg': pct_last,
            'closes': cc, 'highs': hh, 'lows': ll, 'opens': oo,
            'chg5': chg5, 'chg10': chg10,
            'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma_tag': ma_tag,
            'vol_ratio': vol_ratio, 'bbw': bbw,
            'is_zt': is_zt,
        }
    else:
        return {"error": "无法获取K线数据，该股票可能停牌或不存在"}

    # ===== 基本面数据 =====
    basic = get_basic_data(ts_code, T0, trade_dates)
    pe = basic['pe']
    mv = basic['mv']  # 亿
    tr = basic['tr']
    vr_basic = basic['vr']
    data_sources["基本面"] = basic['source']

    # 更新c_data中的基本面字段
    c_data['pe'] = pe if pe and pe > 0 else None
    c_data['circ_mv'] = mv * 10000 if mv else 0  # 亿→万
    c_data['total_mv'] = mv * 10000 if mv else 0
    c_data['turnover'] = tr if tr else 0
    if vr_basic and vr_basic > 0.1:
        c_data['vol_ratio'] = vr_basic  # 用基本面的量比覆盖（更准确）

    # ===== 资金流向 =====
    mf_data = get_moneyflow(ts_code, T0, trade_dates)
    nb_yi = mf_data['net_inflow_yi']
    data_sources["资金流向"] = mf_data['source']
    c_data['net_inflow'] = nb_yi

    # ===== 涨停池 =====
    zt_data = get_zt_data(T0, trade_dates)
    data_sources["涨停池"] = zt_data.get('source', 'N/A')

    # ===== 60分钟K线（WR-3） =====
    kdf_60, src_60 = get_kline_60min_local(ts_code, count=80)
    if kdf_60 is not None and not kdf_60.empty and len(kdf_60) >= 12:
        kdf_60 = kdf_60.sort_values('datetime').reset_index(drop=True)
        c_data['kline_60m'] = {
            'closes': kdf_60['close'].astype(float).tolist(),
            'highs': kdf_60['high'].astype(float).tolist(),
            'lows': kdf_60['low'].astype(float).tolist(),
            'vols': kdf_60['vol'].astype(float).tolist(),
        }
        data_sources["60分钟K线"] = src_60

    # ===== BCI板块完整性 =====
    ind_bci, ind_bci_concept = get_industry_bci(ind, BCI_DATA)
    bci_score = ind_bci

    # ===== 板块涨停数 =====
    sector_zt = ind_zt_map.get(ind, 0)

    # ===== 情绪阶段（自动推断） =====
    zt_cnt = zt_data.get('zt_cnt', 0)
    fbl = zt_data.get('fbl', 0)
    if zt_cnt >= 80 and fbl >= 75: emotion_stage = '起爆'
    elif zt_cnt >= 60 and fbl >= 60: emotion_stage = '一致'
    elif zt_cnt >= 40 and fbl >= 50: emotion_stage = '修复'
    elif zt_cnt >= 25 and fbl >= 40: emotion_stage = '分歧'
    elif zt_cnt >= 15: emotion_stage = '启动'
    elif zt_cnt < 15 and fbl < 40: emotion_stage = '退潮'
    else: emotion_stage = '分歧'

    # ===== 行业市值排名 =====
    # 从T0快照中计算
    snap_t0 = load_snapshot(T0)
    industry_rank = 99
    if snap_t0 is not None:
        ind_stocks = snap_t0[snap_t0.get('industry', pd.Series()) == ind] if 'industry' in snap_t0.columns else pd.DataFrame()
        if not ind_stocks.empty and 'total_mv' in ind_stocks.columns:
            ind_stocks_sorted = ind_stocks.sort_values('total_mv', ascending=False).reset_index(drop=True)
            rank_match = ind_stocks_sorted[ind_stocks_sorted['ts_code'] == ts_code]
            if not rank_match.empty:
                industry_rank = rank_match.index[0] + 1
    c_data['industry_mv_rank'] = industry_rank

    # ===== 补查财务数据（ROE、利润增速） =====
    year = int(T0[:4])
    for period in [f"{year}0331", f"{year-1}1231", f"{year-1}0930"]:
        try:
            fi = ts_api("fina_indicator", {"ts_code": ts_code, "period": period},
                        "ts_code,roe,netprofit_yoy,q_profit_yoy")
            time.sleep(0.2)
            if not fi.empty:
                roe_val = fi['roe'].iloc[0]
                profit_val = fi['netprofit_yoy'].iloc[0]
                if pd.isna(profit_val):
                    profit_val = fi['q_profit_yoy'].iloc[0] if 'q_profit_yoy' in fi.columns else None
                if pd.notna(roe_val): c_data['roe'] = float(roe_val)
                if pd.notna(profit_val): c_data['profit_yoy'] = float(profit_val)
                data_sources["财务数据"] = f"Tushare({period})"
                break
        except:
            pass

    # ===== 9 Skill 评分 =====
    s1, t1 = _score_s1_txcg(c_data, sector_zt, emotion_stage, weights['TXCG'], bci_score)
    s2, t2 = _score_s2_yuanziyuan(c_data, weights['元子元'])
    s3, t3 = _score_s3_camellia(c_data, sector_zt, weights['山茶花'], bci_score)
    s4, t4 = _score_s4_mistery(c_data, weights['Mistery'])
    s5, t5 = _score_s5_tds(c_data, weights['TDS'])
    s6, t6 = _score_s6_wr(c_data, weights['百胜WR'])
    s7, t7 = _score_s7_event(c_data, sector_zt, weights['事件驱动'], bci_score)
    s8, t8 = _score_s8_multi_period(c_data, weights['多周期'])
    s9, t9 = _score_s9_fundamental(c_data, weights['基本面'])

    # TXCG六大模型加分
    txcg_bonus, t_bonus = _calc_txcg_bonus(c_data, sector_zt)

    # 汇总所有标签
    all_tags = []
    for t in [t1, t2, t3, t4, t5, t6, t7, t8, t9, t_bonus]:
        all_tags.extend(t)

    total = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + txcg_bonus
    max_total = sum(weights.values()) + 5  # 100 + 5 = 105

    # ===== 评级 =====
    pct = total / max_total * 100
    if pct >= 75:
        level = "⭐ 强推"
        level_color = "#ff4444"
    elif pct >= 62:
        level = "✅ 推荐"
        level_color = "#44bb44"
    elif pct >= 50:
        level = "👀 关注"
        level_color = "#ffaa00"
    else:
        level = "⏸ 观望"
        level_color = "#999999"

    # ===== 涨跌幅数据 =====
    chg5 = c_data.get('chg5', 0)
    chg10 = c_data.get('chg10', 0)
    r5 = chg5
    r10 = chg10
    r20 = 0
    if len(c_data.get('closes', [])) >= 20:
        r20 = (c_data['closes'][-1] / c_data['closes'][-20] - 1) * 100

    # ===== 构建关键标签（简略展示） =====
    key_tags = [t for t in all_tags if any(k in t for k in ['🔥', '超跌', 'BBW', '量', '净', '突破', '趋势', 'WR', '涨停', '安全', '大阳', '多头', 'PE', '市值', '利润', '行业TOP', 'ROE', '龙头', '冰点', '反转', '反包', 'BCI', '情绪'])]

    return {
        "code": ts_code,
        "name": name,
        "industry": ind,
        "close": c0,
        "date": T0,
        "total_score": total,
        "max_total": max_total,
        "level": level,
        "level_color": level_color,
        "emotion_stage": emotion_stage,
        "bci_score": bci_score,
        "bci_concept": ind_bci_concept,
        "sector_zt": sector_zt,
        "dimensions": [
            {"name": "S1-TXCG", "score": s1, "max": weights['TXCG'], "brief": ', '.join(t1[:3]) if t1 else "—"},
            {"name": "S2-元子元", "score": s2, "max": weights['元子元'], "brief": ', '.join(t2[:3]) if t2 else "—"},
            {"name": "S3-山茶花", "score": s3, "max": weights['山茶花'], "brief": ', '.join(t3[:3]) if t3 else "—"},
            {"name": "S4-Mistery", "score": s4, "max": weights['Mistery'], "brief": ', '.join(t4[:3]) if t4 else "—"},
            {"name": "S5-TDS", "score": s5, "max": weights['TDS'], "brief": ', '.join(t5[:3]) if t5 else "—"},
            {"name": "S6-百胜WR", "score": s6, "max": weights['百胜WR'], "brief": ', '.join(t6[:2]) if t6 else "—"},
            {"name": "S7-事件驱动", "score": s7, "max": weights['事件驱动'], "brief": ', '.join(t7[:3]) if t7 else "—"},
            {"name": "S8-多周期", "score": s8, "max": weights['多周期'], "brief": ', '.join(t8[:2]) if t8 else "—"},
            {"name": "S9-基本面", "score": s9, "max": weights['基本面'], "brief": ', '.join(t9[:3]) if t9 else "—"},
            {"name": "TXCG加分", "score": txcg_bonus, "max": 5, "brief": ', '.join(t_bonus[:3]) if t_bonus else "无额外加分"},
        ],
        "key_tags": key_tags[:12],
        "market_data": {
            "r5": round(r5, 2),
            "r10": round(r10, 2),
            "r20": round(r20, 2),
            "pe": round(pe, 1) if pe and pe > 0 else None,
            "mv": round(mv, 1),
            "turnover": round(tr, 2) if tr else 0,
            "net_inflow": round(nb_yi, 2),
            "volume_ratio": round(c_data.get('vol_ratio', 0), 2),
            "is_zt": c_data.get('is_zt', False),
            "bbw": round(c_data.get('bbw', 0), 3),
            "ma_tag": c_data.get('ma_tag', '弱'),
        },
        "data_sources": data_sources,
    }


# ===== API路由 =====

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/score", methods=["POST"])
def api_score():
    """评分接口"""
    data = request.get_json()
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "请输入股票代码或名称"})

    ts_code = resolve_stock_code(query)
    if not ts_code:
        return jsonify({"error": f"未找到股票: {query}"})

    try:
        result = score_single_stock(ts_code)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"评分失败: {str(e)}"})


@app.route("/api/search", methods=["GET"])
def api_search():
    """股票搜索建议"""
    q = request.args.get("q", "").strip()
    if len(q) < 1:
        return jsonify([])

    stock_list = get_stock_list()
    name_map = stock_list["name_map"]
    suggestions = []

    for code, name in name_map.items():
        if q in name or q in code[:6]:
            suggestions.append({"code": code[:6], "name": name, "ts_code": code})
            if len(suggestions) >= 10:
                break

    return jsonify(suggestions)


@app.route("/api/status", methods=["GET"])
def api_status():
    """数据源状态"""
    return jsonify({
        "local_data": LOCAL_DATA_AVAILABLE,
        "local_snapshots": len(LOCAL_TRADE_DATES),
        "local_csvs": len(LOCAL_CSV_INDEX),
        "latest_snapshot": LOCAL_TRADE_DATES[-1] if LOCAL_TRADE_DATES else None,
        "ashare": ASHARE_AVAILABLE,
        "akshare": AKSHARE_AVAILABLE,
        "tushare": True,
        "cache_size": len(cache._cache),
    })


@app.route("/api/cache/clear", methods=["POST"])
def api_clear_cache():
    """清除缓存"""
    cache.clear()
    _snapshot_cache.clear()
    return jsonify({"message": "缓存已清除"})


if __name__ == "__main__":
    print("=" * 60)
    print("A股综合评分系统 Web版 — 9 Skill v3.3（对齐sector_deep_pick_v2）")
    print(f"  评分体系: 9维度 + TXCG加分 = 满分105")
    print(f"  本地数据: {'✅ ' + str(len(LOCAL_TRADE_DATES)) + '天快照 + ' + str(len(LOCAL_CSV_INDEX)) + '只CSV' if LOCAL_DATA_AVAILABLE else '❌ 不可用'}")
    print(f"  Ashare:   {'✅ 可用' if ASHARE_AVAILABLE else '❌ 不可用'}")
    print(f"  AKShare:  {'✅ 可用' if AKSHARE_AVAILABLE else '❌ 不可用'}")
    print(f"  Tushare:  ✅ 可用")
    print("访问 http://localhost:5088 开始使用")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5088, debug=False)
