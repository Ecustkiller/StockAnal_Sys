# -*- coding: utf-8 -*-
"""
Claw A股量化评分系统 — Flask Blueprint 桥接模块
================================================
将 claw.web.app 中的核心评分功能封装为 Flask Blueprint，
集成到 StockAnal_Sys 的主 web_server.py 中。

路由前缀: /claw
"""
import sys
import os
import time
import traceback
import threading
from datetime import datetime, timedelta

import requests
import pandas as pd

from flask import Blueprint, request, jsonify, render_template

# ===== 延迟导入 Claw 模块（避免启动时报错） =====
_claw_app = None
_claw_import_error = None


def _ensure_claw_loaded():
    """延迟加载 Claw 的 web/app.py 模块"""
    global _claw_app, _claw_import_error
    if _claw_app is not None:
        return True
    if _claw_import_error is not None:
        return False
    try:
        # 导入 claw.web.app 中的核心函数
        from claw.web.app import (
            score_single_stock,
            resolve_stock_code,
            get_stock_list,
            get_trade_dates,
            get_latest_trade_date,
            load_bci_data,
            BCI_DATA,
            LOCAL_DATA_AVAILABLE,
            LOCAL_TRADE_DATES,
            LOCAL_CSV_INDEX,
            ASHARE_AVAILABLE,
            AKSHARE_AVAILABLE,
            cache as claw_cache,
        )
        _claw_app = {
            'score_single_stock': score_single_stock,
            'resolve_stock_code': resolve_stock_code,
            'get_stock_list': get_stock_list,
            'get_trade_dates': get_trade_dates,
            'get_latest_trade_date': get_latest_trade_date,
            'load_bci_data': load_bci_data,
            'BCI_DATA': BCI_DATA,
            'LOCAL_DATA_AVAILABLE': LOCAL_DATA_AVAILABLE,
            'LOCAL_TRADE_DATES': LOCAL_TRADE_DATES,
            'LOCAL_CSV_INDEX': LOCAL_CSV_INDEX,
            'ASHARE_AVAILABLE': ASHARE_AVAILABLE,
            'AKSHARE_AVAILABLE': AKSHARE_AVAILABLE,
            'claw_cache': claw_cache,
        }
        print("✅ Claw 评分系统模块加载成功")
        return True
    except Exception as e:
        _claw_import_error = str(e)
        print(f"⚠️ Claw 评分系统模块加载失败: {e}")
        traceback.print_exc()
        return False


# ===== 创建 Blueprint =====
claw_bp = Blueprint('claw', __name__, url_prefix='/claw')


@claw_bp.route('/')
def claw_index():
    """Claw 评分系统首页"""
    return render_template('claw_score.html')


@claw_bp.route('/api/score', methods=['POST'])
def claw_api_score():
    """Claw 九维评分接口"""
    if not _ensure_claw_loaded():
        return jsonify({"error": f"Claw 模块加载失败: {_claw_import_error}"}), 500

    data = request.get_json()
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "请输入股票代码或名称"})

    ts_code = _claw_app['resolve_stock_code'](query)
    if not ts_code:
        return jsonify({"error": f"未找到股票: {query}"})

    try:
        result = _claw_app['score_single_stock'](ts_code)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"评分失败: {str(e)}"})


@claw_bp.route('/api/search', methods=['GET'])
def claw_api_search():
    """Claw 股票搜索建议"""
    if not _ensure_claw_loaded():
        return jsonify([])

    q = request.args.get("q", "").strip()
    if len(q) < 1:
        return jsonify([])

    stock_list = _claw_app['get_stock_list']()
    name_map = stock_list["name_map"]
    suggestions = []

    for code, name in name_map.items():
        if q in name or q in code[:6]:
            suggestions.append({"code": code[:6], "name": name, "ts_code": code})
            if len(suggestions) >= 10:
                break

    return jsonify(suggestions)


@claw_bp.route('/api/status', methods=['GET'])
def claw_api_status():
    """Claw 数据源状态"""
    if not _ensure_claw_loaded():
        return jsonify({
            "error": f"Claw 模块未加载: {_claw_import_error}",
            "loaded": False,
        })

    return jsonify({
        "loaded": True,
        "local_data": _claw_app['LOCAL_DATA_AVAILABLE'],
        "local_snapshots": len(_claw_app['LOCAL_TRADE_DATES']),
        "local_csvs": len(_claw_app['LOCAL_CSV_INDEX']),
        "latest_snapshot": _claw_app['LOCAL_TRADE_DATES'][-1] if _claw_app['LOCAL_TRADE_DATES'] else None,
        "ashare": _claw_app['ASHARE_AVAILABLE'],
        "akshare": _claw_app['AKSHARE_AVAILABLE'],
        "tushare": True,
        "cache_size": len(_claw_app['claw_cache']._cache),
    })


@claw_bp.route('/api/cache/clear', methods=['POST'])
def claw_api_clear_cache():
    """清除 Claw 缓存"""
    if not _ensure_claw_loaded():
        return jsonify({"error": "Claw 模块未加载"}), 500

    _claw_app['claw_cache'].clear()
    return jsonify({"message": "Claw 缓存已清除"})


# ===== 数据同步功能 =====

SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59")

_sync_lock = threading.Lock()
_last_sync_time = None
_last_sync_result = None


def _ts_api(api_name, **kwargs):
    """轻量级 Tushare API 调用"""
    d = {"api_name": api_name, "token": TUSHARE_TOKEN, "params": kwargs, "fields": ""}
    for retry in range(3):
        try:
            r = requests.post("http://api.tushare.pro", json=d, timeout=30)
            j = r.json()
            if j.get("data") and j["data"].get("items"):
                return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
            return pd.DataFrame()
        except Exception as e:
            if retry < 2:
                time.sleep(2)
    return pd.DataFrame()


def _sync_snapshot(trade_date):
    """同步一天的全市场快照"""
    out_file = os.path.join(SNAPSHOT_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        return True, "已存在"

    # 1. 日线数据
    df_daily = _ts_api("daily", trade_date=trade_date)
    if df_daily.empty:
        return False, "无数据"
    time.sleep(0.3)

    # 2. daily_basic（PE/PB/市值/换手）
    df_basic = _ts_api("daily_basic", trade_date=trade_date,
                        fields="ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate,turnover_rate_f")
    if not df_basic.empty:
        dup_cols = [c for c in df_basic.columns if c in df_daily.columns and c != "ts_code"]
        df_basic = df_basic.drop(columns=dup_cols, errors="ignore")
        df_daily = df_daily.merge(df_basic, on="ts_code", how="left")
    time.sleep(0.3)

    # 3. 资金流向
    df_mf = _ts_api("moneyflow", trade_date=trade_date, fields="ts_code,net_mf_amount")
    if not df_mf.empty:
        df_daily = df_daily.merge(df_mf, on="ts_code", how="left")
    time.sleep(0.3)

    # 4. 股票基本信息
    basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
    df_info = pd.DataFrame()
    if os.path.exists(basic_file):
        age = time.time() - os.path.getmtime(basic_file)
        if age < 7 * 86400:
            df_info = pd.read_parquet(basic_file)

    if df_info.empty:
        df_info = _ts_api("stock_basic", exchange="", list_status="L",
                           fields="ts_code,name,industry,market,list_date")
        if not df_info.empty:
            df_info.to_parquet(basic_file, index=False)

    if not df_info.empty:
        df_daily = df_daily.merge(df_info[["ts_code", "name", "industry"]], on="ts_code", how="left")

    # 数值类型转换
    num_cols = ["open", "high", "low", "close", "pre_close", "change", "pct_chg",
                "vol", "amount", "pe_ttm", "pb", "total_mv", "circ_mv",
                "turnover_rate", "turnover_rate_f", "net_mf_amount"]
    for col in num_cols:
        if col in df_daily.columns:
            df_daily[col] = pd.to_numeric(df_daily[col], errors="coerce")

    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    df_daily.to_parquet(out_file, index=False)
    return True, f"{len(df_daily)}只"


def _get_missing_dates():
    """获取本地缺失的交易日"""
    # 获取本地已有日期
    existing = set()
    if os.path.exists(SNAPSHOT_DIR):
        existing = set(f.replace(".parquet", "") for f in os.listdir(SNAPSHOT_DIR)
                       if f.endswith(".parquet") and f[0].isdigit())

    # 获取最近30天的交易日历
    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    cal = _ts_api("trade_cal", exchange="SSE", is_open="1", start_date=start, end_date=today)
    if cal.empty:
        return []

    trade_dates = sorted(cal["cal_date"].tolist())
    # 只返回不超过今天且本地不存在的日期
    missing = [d for d in trade_dates if d <= today and d not in existing]
    return missing


def do_data_sync():
    """执行数据同步（线程安全）"""
    global _last_sync_time, _last_sync_result

    if not _sync_lock.acquire(blocking=False):
        return {"status": "busy", "message": "同步正在进行中"}

    try:
        missing = _get_missing_dates()
        if not missing:
            result = {"status": "ok", "message": "数据已是最新", "synced": 0}
            _last_sync_time = datetime.now().isoformat()
            _last_sync_result = result
            return result

        synced = 0
        errors = []
        for d in missing:
            ok, info = _sync_snapshot(d)
            if ok and info != "已存在":
                synced += 1
                print(f"  📊 同步 {d}: {info}")
            elif not ok:
                errors.append(f"{d}: {info}")
            time.sleep(0.5)

        result = {
            "status": "ok",
            "message": f"同步完成: 新增{synced}天",
            "synced": synced,
            "errors": errors if errors else None,
        }
        _last_sync_time = datetime.now().isoformat()
        _last_sync_result = result
        return result
    finally:
        _sync_lock.release()


def _reload_claw_data():
    """同步后重新加载 Claw 的本地数据索引"""
    global _claw_app
    try:
        # 重新导入以刷新 LOCAL_TRADE_DATES 等全局变量
        import importlib
        import claw.web.app as claw_web_app
        importlib.reload(claw_web_app)
        _claw_app = None  # 强制下次调用时重新加载
        _claw_import_error = None
        print("✅ Claw 数据索引已重新加载")
    except Exception as e:
        print(f"⚠️ 重新加载 Claw 数据索引失败: {e}")


@claw_bp.route('/api/sync', methods=['POST'])
def claw_api_sync():
    """手动触发数据同步"""
    result = do_data_sync()
    if result.get("synced", 0) > 0:
        _reload_claw_data()
    return jsonify(result)


@claw_bp.route('/api/sync/status', methods=['GET'])
def claw_api_sync_status():
    """查询数据同步状态"""
    # 检查本地数据缺口
    missing = _get_missing_dates()

    return jsonify({
        "last_sync_time": _last_sync_time,
        "last_sync_result": _last_sync_result,
        "missing_dates": missing,
        "missing_count": len(missing),
        "is_syncing": _sync_lock.locked(),
        "latest_local": sorted(
            f.replace(".parquet", "") for f in os.listdir(SNAPSHOT_DIR)
            if f.endswith(".parquet") and f[0].isdigit()
        )[-1] if os.path.exists(SNAPSHOT_DIR) else None,
    })


# ===== 启动时自动同步 =====
def auto_sync_on_startup():
    """后台线程：启动时自动检查并同步缺失数据"""
    time.sleep(5)  # 等待服务完全启动
    try:
        missing = _get_missing_dates()
        if missing:
            print(f"🔄 检测到 {len(missing)} 天数据缺口: {missing}")
            print(f"   正在自动同步...")
            result = do_data_sync()
            print(f"   ✅ 自动同步完成: {result.get('message', '')}")
            if result.get("synced", 0) > 0:
                _reload_claw_data()
        else:
            print("✅ 本地数据已是最新，无需同步")
    except Exception as e:
        print(f"⚠️ 自动同步失败: {e}")


def start_auto_sync():
    """启动自动同步后台线程"""
    t = threading.Thread(target=auto_sync_on_startup, daemon=True)
    t.start()
    print("🔄 Claw 数据自动同步线程已启动")
