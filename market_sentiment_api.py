# market_sentiment_api.py
# -*- coding: utf-8 -*-
"""
市场情绪API模块 - 整合Claw的情绪分析+大盘择时+热点板块识别
为首页提供一站式市场情绪数据
"""

import logging
import time
import json
import os
import threading
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

logger = logging.getLogger('market_sentiment_api')

# ==================== 数据缓存 ====================
_sentiment_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 600  # 缓存10分钟


def _get_cache(key):
    """获取缓存"""
    with _cache_lock:
        if key in _sentiment_cache:
            data, ts = _sentiment_cache[key]
            if time.time() - ts < CACHE_TTL:
                return data
    return None


def _set_cache(key, data):
    """设置缓存"""
    with _cache_lock:
        _sentiment_cache[key] = (data, time.time())


# ==================== 市场情绪数据获取 ====================
def get_market_sentiment_summary():
    """
    获取市场情绪摘要数据，用于首页展示
    
    返回:
        dict: {
            'date': '2026-04-25',
            'sentiment': {
                'zt_cnt': 涨停家数,
                'dt_cnt': 跌停家数,
                'zb_cnt': 炸板家数,
                'fbl': 封板率,
                'earn_rate': 赚钱效应,
                'total_amount': 成交额(亿),
                'up_cnt': 上涨家数,
                'down_cnt': 下跌家数,
                'total_stocks': 总股票数,
                'phase': 情绪阶段,
                'position_advice': 仓位建议,
                'max_pct': 最大仓位百分比,
            },
            'board_stats': {
                'board_dist': {连板数: 家数},
                'max_board': 最高连板,
                'max_board_stocks': 最高连板股票,
            },
            'hot_industries': [
                {'name': 行业名, 'zt_count': 涨停数},
                ...
            ],
            'timing': {
                'score': 择时总分,
                'position': 建议仓位比例,
                'signal': 信号描述,
                'signals_detail': {各信号详情},
            },
            'status': 'ok' / 'error',
            'updated_at': 更新时间,
        }
    """
    cache_key = "sentiment_summary"
    cached = _get_cache(cache_key)
    if cached:
        return cached
    
    result = {
        'date': '',
        'sentiment': {},
        'board_stats': {},
        'hot_industries': [],
        'timing': {},
        'status': 'ok',
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    
    try:
        # 获取最近交易日
        from claw.analysis.market_sentiment import get_trade_dates, get_market_sentiment
        
        trade_dates = get_trade_dates("20260101")
        if not trade_dates:
            result['status'] = 'error'
            result['error'] = '无法获取交易日历'
            return result
        
        latest_date = trade_dates[-1]
        result['date'] = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"
        
        # 获取市场情绪数据
        logger.info(f"获取 {latest_date} 市场情绪数据...")
        s = get_market_sentiment(latest_date)
        
        # 情绪核心指标
        result['sentiment'] = {
            'zt_cnt': s.get('zt_cnt', 0),
            'dt_cnt': s.get('dt_cnt', 0),
            'zb_cnt': s.get('zb_cnt', 0),
            'fbl': s.get('fbl', 0),
            'earn_rate': s.get('earn_rate', 0),
            'total_amount': s.get('total_amount', 0),
            'up_cnt': s.get('up_cnt', 0),
            'down_cnt': s.get('down_cnt', 0),
            'flat_cnt': s.get('flat_cnt', 0),
            'total_stocks': s.get('total_stocks', 0),
            'phase': s.get('bjcj3_phase', '未知'),
            'position_advice': s.get('bjcj3_pos', '未知'),
            'max_pct': s.get('bjcj3_max_pct', 0),
            'st_zt_cnt': s.get('st_zt_cnt', 0),
            'st_dt_cnt': s.get('st_dt_cnt', 0),
        }
        
        # 连板统计
        result['board_stats'] = {
            'board_dist': {str(k): v for k, v in s.get('board_dist', {}).items()},
            'max_board': s.get('max_board', 0),
            'max_board_stocks': [
                {'code': item[0], 'name': item[1], 'board': item[2]}
                for item in s.get('max_board_stocks', [])
            ] if s.get('max_board_stocks') else [],
        }
        
        # 热门行业（涨停行业TOP10）
        result['hot_industries'] = [
            {'name': name, 'zt_count': count}
            for name, count in s.get('ind_zt_top10', [])
        ]
        
    except Exception as e:
        logger.error(f"获取市场情绪数据失败: {e}")
        result['sentiment'] = {
            'phase': '数据获取失败',
            'position_advice': '-',
            'max_pct': 0,
        }
    
    # 获取大盘择时信号
    try:
        timing_data = _get_timing_signal()
        result['timing'] = timing_data
    except Exception as e:
        logger.error(f"获取择时信号失败: {e}")
        result['timing'] = {
            'score': 0,
            'position': 0.5,
            'signal': '数据获取失败',
            'signals_detail': {},
        }
    
    _set_cache(cache_key, result)
    return result


def _get_timing_signal():
    """
    获取最新的大盘择时信号
    使用Claw的MarketTimer
    """
    try:
        from claw.timing.market_timer import MarketTimer
        
        mt = MarketTimer(mode="balanced")
        
        # 只生成最近60个交易日的数据（节省时间）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        
        df = mt.generate(start_date, end_date)
        
        if df.empty:
            return {
                'score': 0,
                'position': 0.5,
                'signal': '数据不足',
                'signals_detail': {},
            }
        
        # 取最新一行
        latest = df.iloc[-1]
        score = int(latest.get('score', 10))
        position = float(latest.get('position', 0.5))
        
        # 信号描述
        if score >= 16:
            signal = '强烈看多'
            signal_color = 'success'
        elif score >= 12:
            signal = '偏多'
            signal_color = 'primary'
        elif score >= 8:
            signal = '中性'
            signal_color = 'warning'
        elif score >= 4:
            signal = '偏空'
            signal_color = 'danger'
        else:
            signal = '强烈看空'
            signal_color = 'danger'
        
        # 各信号详情
        signal_names = {
            's01': 'MA60趋势', 's02': 'MA20/60交叉', 's03': '20日动量',
            's04': 'MACD柱', 's05': 'ADX强度', 's06': '涨跌停差',
            's07': '市场宽度', 's08': '成交额分位', 's09': '波动率',
            's10': '连板情绪',
        }
        
        signals_detail = {}
        for key, name in signal_names.items():
            val = int(latest.get(key, 1))
            signals_detail[key] = {
                'name': name,
                'value': val,
                'label': ['看空', '中性', '看多'][val],
            }
        
        return {
            'score': score,
            'max_score': 20,
            'position': round(position * 100),
            'signal': signal,
            'signal_color': signal_color,
            'signals_detail': signals_detail,
            'trade_date': str(latest.get('trade_date', '')),
        }
        
    except Exception as e:
        logger.error(f"MarketTimer 执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'score': 0,
            'position': 50,
            'signal': '计算失败',
            'signal_color': 'secondary',
            'signals_detail': {},
        }


def get_hot_sectors():
    """
    获取热点板块数据
    基于Claw的mainline_screener思路，使用Tushare数据
    
    返回:
        dict: {
            'hot_industries': [{'name': 行业, 'avg_chg': 平均涨幅, 'up_pct': 上涨比例, 'limit_up': 涨停数}],
            'mainline': [持续强势的主线行业],
            'date': 日期,
        }
    """
    cache_key = "hot_sectors"
    cached = _get_cache(cache_key)
    if cached:
        return cached
    
    result = {
        'hot_industries': [],
        'mainline': [],
        'date': '',
        'status': 'ok',
    }
    
    try:
        from claw.analysis.market_sentiment import _ts_api, get_trade_dates
        
        # 获取最近3个交易日
        trade_dates = get_trade_dates("20260101")
        if len(trade_dates) < 3:
            result['status'] = 'error'
            return result
        
        recent_dates = trade_dates[-3:]
        result['date'] = f"{recent_dates[-1][:4]}-{recent_dates[-1][4:6]}-{recent_dates[-1][6:]}"
        
        # 获取行业映射
        stk = _ts_api("stock_basic", {"list_status": "L"}, "ts_code,name,industry")
        time.sleep(0.5)
        if stk.empty:
            result['status'] = 'error'
            return result
        
        stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
        stk = stk[~stk["name"].str.contains("ST|退", na=False)]
        ind_map = dict(zip(stk["ts_code"], stk["industry"]))
        
        # 获取最近3天数据
        daily_data = {}
        for d in recent_dates:
            df = _ts_api("daily", {"trade_date": d}, "ts_code,pct_chg,amount")
            time.sleep(0.8)
            if not df.empty:
                daily_data[d] = df
        
        if not daily_data:
            result['status'] = 'error'
            return result
        
        # 计算行业热度
        ind_ranks = {}
        for date_str, df in daily_data.items():
            df["industry"] = df["ts_code"].map(ind_map)
            df = df[df["industry"].notna()].copy()
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            
            ind_avg = df.groupby("industry").agg(
                avg_chg=("pct_chg", "mean"),
                count=("ts_code", "count"),
                up_pct=("pct_chg", lambda x: (x > 0).sum() / len(x) * 100 if len(x) > 0 else 0),
                limit_up=("pct_chg", lambda x: (x >= 9.5).sum()),
            ).reset_index()
            
            ind_avg["rank"] = ind_avg["avg_chg"].rank(ascending=False)
            
            for _, row in ind_avg.iterrows():
                ind = row["industry"]
                if ind not in ind_ranks:
                    ind_ranks[ind] = []
                ind_ranks[ind].append({
                    "date": date_str,
                    "rank": int(row["rank"]),
                    "avg_chg": round(float(row["avg_chg"]), 2),
                    "up_pct": round(float(row["up_pct"]), 1),
                    "limit_up": int(row["limit_up"]),
                })
        
        # 计算综合得分
        ind_scores = []
        for ind, records in ind_ranks.items():
            avg_rank = np.mean([r["rank"] for r in records])
            avg_chg = np.mean([r["avg_chg"] for r in records])
            consistency = sum(1 for r in records if r["rank"] <= 20)
            total_limit = sum(r["limit_up"] for r in records)
            
            # 最新一天的数据
            latest = records[-1] if records else {}
            
            score = consistency * 3 + avg_chg * 2 - avg_rank * 0.1 + total_limit * 0.5
            
            ind_scores.append({
                'name': ind,
                'score': round(score, 1),
                'avg_chg': round(avg_chg, 2),
                'avg_rank': round(avg_rank, 1),
                'consistency': consistency,
                'latest_chg': latest.get('avg_chg', 0),
                'latest_up_pct': latest.get('up_pct', 0),
                'latest_limit_up': latest.get('limit_up', 0),
            })
        
        # 排序
        ind_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # 热门行业TOP15
        result['hot_industries'] = ind_scores[:15]
        
        # 主线行业（连续3天TOP20且平均涨幅>0.5%）
        result['mainline'] = [
            item['name'] for item in ind_scores[:15]
            if item['consistency'] >= 2 and item['avg_chg'] > 0.5
        ]
        
    except Exception as e:
        logger.error(f"获取热点板块失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        result['status'] = 'error'
        result['error'] = str(e)
    
    _set_cache(cache_key, result)
    return result
