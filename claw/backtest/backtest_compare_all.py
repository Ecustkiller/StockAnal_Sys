#!/usr/bin/env python3
"""
多策略回测对比 — 3套评分体系横向PK
====================================
将3套评分体系放在同一框架下，用相同数据、相同买卖规则回测，公平对比。

策略A: v1.0 简化版 (100分制) — backtest.py的评分逻辑
  D1多周期(15) + D2主线(20) + D3三Skill(35) + D4安全边际(15) + D5基本面(15)
  无BCI、无WR、无60分钟K线

策略B: v2.1 完整版 (150分制) — backtest_v2.py的评分逻辑
  D1多周期(15) + D2主线+BCI(25) + D3三Skill+TXCG(47) + D4安全边际(15) + D5基本面(15) + D9百胜WR(15)
  含BCI板块完整性、60分钟K线WR-3、真实量比

策略C: 9 Skill v3.3 (105分制) — sector_deep_pick_v2.py的评分逻辑
  S1-TXCG(15) + S2-元子元(10) + S3-山茶花(15) + S4-Mistery(10) + S5-TDS(10)
  + S6-百胜WR(15) + S7-事件驱动(10) + S8-多周期(5) + S9-基本面(10) + TXCG加分(5)

买卖规则（统一）：
  买入: T+1开盘价
  卖出: T+N收盘价 (N=1,2,3,5)
  每天取TOP20

用法:
  python3 backtest_compare_all.py                    # 默认回测
  python3 backtest_compare_all.py --start 20260320   # 指定起始日
  python3 backtest_compare_all.py --top 10           # 每天TOP10
  python3 backtest_compare_all.py --report           # 生成HTML对比报告
"""
import os, sys, time, argparse, json, webbrowser
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ===== 配置 =====
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
KLINE_60M_DIRS = [
    os.path.expanduser("~/Downloads/2026/60min"),
    os.path.expanduser("~/Downloads/2025/60min"),
]
VR_DIR = os.path.expanduser("~/stock_data/volume_ratio")
RESULTS_DIR = os.path.expanduser("~/WorkBuddy/Claw/backtest_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# ===== 数据加载（公共） =====
def get_available_dates():
    files = [f for f in os.listdir(SNAPSHOT_DIR)
             if f.endswith('.parquet') and f != 'stock_basic.parquet']
    return sorted([f.replace('.parquet', '') for f in files])

_snap_cache = {}
def load_snapshot(date_str):
    if date_str in _snap_cache:
        return _snap_cache[date_str]
    path = os.path.join(SNAPSHOT_DIR, f"{date_str}.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'pct_chg',
                'vol', 'amount', 'pe_ttm', 'total_mv', 'circ_mv',
                'turnover_rate_f', 'net_mf_amount']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    _snap_cache[date_str] = df
    return df

def load_volume_ratio(date_str):
    path = os.path.join(VR_DIR, f"{date_str}.parquet")
    if not os.path.exists(path):
        return {}
    df = pd.read_parquet(path)
    vr_dict = {}
    for _, row in df.iterrows():
        if pd.notna(row.get('volume_ratio')):
            vr_dict[row['ts_code']] = float(row['volume_ratio'])
    return vr_dict

_60m_cache = {}
def load_60m_kline(ts_code, target_date=None):
    if ts_code in _60m_cache:
        df_60 = _60m_cache[ts_code]
    else:
        code6 = ts_code[:6]
        prefix = 'sh' if ts_code.endswith('.SH') else ('sz' if ts_code.endswith('.SZ') else 'bj')
        fname = f"{prefix}{code6}.csv"
        dfs = []
        for kdir in KLINE_60M_DIRS:
            candidate = os.path.join(kdir, fname)
            if os.path.exists(candidate):
                try:
                    _df = pd.read_csv(candidate, encoding='utf-8',
                                      names=['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                                      header=0)
                    _df['date'] = _df['date'].astype(str).str.replace('-', '')
                    dfs.append(_df)
                except:
                    pass
        if not dfs:
            _60m_cache[ts_code] = None
            return None
        df_60 = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time']).reset_index(drop=True)
        _60m_cache[ts_code] = df_60
    if df_60 is None:
        return None
    if target_date:
        df_filtered = df_60[df_60['date'] <= target_date].tail(30)
    else:
        df_filtered = df_60.tail(30)
    if len(df_filtered) < 12:
        return None
    return {
        'closes': df_filtered['close'].astype(float).tolist(),
        'highs': df_filtered['high'].astype(float).tolist(),
        'lows': df_filtered['low'].astype(float).tolist(),
        'vols': df_filtered['volume'].astype(float).tolist(),
    }

def build_history(dates, target_idx, offsets=None):
    if offsets is None:
        offsets = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20]
    history = {}
    for offset in offsets:
        idx = target_idx - offset
        if 0 <= idx < len(dates):
            d = dates[idx]
            if d not in history:
                df = load_snapshot(d)
                if df is not None:
                    history[d] = df
    return history

def filter_base(df):
    """基础过滤：非ST、主板+创业板+科创板"""
    df = df[df['ts_code'].str.match(r'^(00|30|60|68)', na=False)].copy()
    if 'name' in df.columns:
        df = df[~df['name'].str.contains('ST|退', na=False)]
    return df

def build_kline_data(history, dates, target_idx, n_days=11):
    """构建K线数据"""
    kline_data = {}
    for offset in range(min(n_days, target_idx + 1)):
        idx = target_idx - offset
        if idx < 0:
            break
        d = dates[idx]
        if d in history:
            for _, row in history[d].iterrows():
                code = row['ts_code']
                if code not in kline_data:
                    kline_data[code] = []
                kline_data[code].append({
                    'trade_date': d,
                    'open': row.get('open', 0),
                    'high': row.get('high', 0),
                    'low': row.get('low', 0),
                    'close': row.get('close', 0),
                    'pct_chg': row.get('pct_chg', 0),
                    'vol': row.get('vol', 0),
                    'amount': row.get('amount', 0),
                })
    for code in kline_data:
        kline_data[code].sort(key=lambda x: x['trade_date'])
    return kline_data

def build_close_prices(history):
    cp = {}
    for d, df in history.items():
        for _, row in df.iterrows():
            code = row['ts_code']
            if code not in cp:
                cp[code] = {}
            cp[code][d] = row['close']
    return cp

def calc_industry_stats(df_t0, history, dates, target_idx, ind_map):
    """计算行业涨停统计、主线热度、BCI等公共数据"""
    # 行业涨停统计
    ind_zt_map = defaultdict(int)
    ind_zt_stocks = defaultdict(list)
    zt_codes_set = set()
    for _, row in df_t0.iterrows():
        if row.get('pct_chg', 0) >= 9.5:
            ind = ind_map.get(row['ts_code'], '?')
            ind_zt_map[ind] += 1
            ind_zt_stocks[ind].append(row.to_dict())
            zt_codes_set.add(row['ts_code'])
    total_zt = len(zt_codes_set)

    # 近3天行业涨停统计
    ind_perf = defaultdict(list)
    ind_zt_daily = defaultdict(lambda: [0, 0, 0])
    d1_zt_codes = set()
    d2_zt_codes = set()

    for offset in [0, 1, 2]:
        idx = target_idx - offset
        if idx < 0 or idx >= len(dates):
            continue
        d = dates[idx]
        if d not in history:
            continue
        df_d = filter_base(history[d])
        if 'industry' in df_d.columns and 'pct_chg' in df_d.columns:
            zt_d = df_d[df_d['pct_chg'] >= 9.5]['ts_code'].tolist()
            if offset == 1:
                d1_zt_codes = set(zt_d)
            elif offset == 2:
                d2_zt_codes = set(zt_d)
            for ind, grp in df_d.groupby('industry'):
                avg_chg = grp['pct_chg'].mean()
                lim_cnt = int((grp['pct_chg'] >= 9.5).sum())
                all_ind_avg = df_d.groupby('industry')['pct_chg'].mean().sort_values(ascending=False)
                rk = list(all_ind_avg.index).index(ind) + 1 if ind in all_ind_avg.index else 99
                ind_perf[ind].append({'avg': avg_chg, 'lim': lim_cnt, 'rk': rk})
                ind_zt_daily[ind][offset] = lim_cnt

    # 主线热度分
    mainline_scores = {}
    for ind, perfs in ind_perf.items():
        avg_rk = np.mean([p['rk'] for p in perfs])
        total_lim = sum(p['lim'] for p in perfs)
        top20_days = sum(1 for p in perfs if p['rk'] <= 20)
        score = 0
        if avg_rk <= 10: score += 8
        elif avg_rk <= 20: score += 5
        elif avg_rk <= 30: score += 3
        elif avg_rk <= 50: score += 1
        if top20_days >= 3: score += 6
        elif top20_days >= 2: score += 4
        elif top20_days >= 1: score += 2
        if total_lim >= 20: score += 6
        elif total_lim >= 10: score += 4
        elif total_lim >= 5: score += 3
        elif total_lim >= 2: score += 1
        # 板块持续性
        day_lims = ind_zt_daily.get(ind, [0, 0, 0])
        latest_lim = day_lims[0]
        prev_lim = day_lims[1] if len(day_lims) > 1 else 0
        if latest_lim >= prev_lim * 1.5 and latest_lim >= 5:
            score += 3
        elif latest_lim >= prev_lim and latest_lim >= 3:
            score += 2
        elif prev_lim >= 5 and latest_lim < prev_lim * 0.5:
            score -= 3
        elif latest_lim < prev_lim:
            score -= 1
        if prev_lim == 0 and latest_lim >= 10:
            score -= 2
        if all(c > 0 for c in day_lims[:3] if c is not None):
            score += 2
        mainline_scores[ind] = max(min(score, 25), 0)

    # BCI板块完整性
    ind_zb_map = {}
    if 'industry' in df_t0.columns:
        for ind, grp_rows in df_t0.groupby('industry'):
            zb_approx = len(grp_rows[(grp_rows['pct_chg'] >= 5) & (grp_rows['pct_chg'] < 9.5)])
            ind_zb_map[ind] = max(0, zb_approx // 3)

    ind_bci_map = {}
    bas_d = {}
    vr_data = load_volume_ratio(dates[target_idx])
    for _, row in df_t0.iterrows():
        bas_d[row['ts_code']] = {
            'pe_ttm': row.get('pe_ttm'),
            'total_mv': row.get('total_mv'),
            'turnover_rate_f': row.get('turnover_rate_f'),
            'net_mf_amount': row.get('net_mf_amount'),
            'volume_ratio': vr_data.get(row['ts_code'], 0),
        }

    for ind_name, zt_count in ind_zt_map.items():
        if zt_count == 0:
            ind_bci_map[ind_name] = 0
            continue
        zt_list = ind_zt_stocks.get(ind_name, [])
        ind_bci_map[ind_name] = calc_bci(ind_name, zt_list, ind_zt_daily, ind_zb_map,
                                          d1_zt_codes, d2_zt_codes, bas_d)

    # 全市场成交额
    total_market_amount = df_t0['amount'].sum() / 100000 if 'amount' in df_t0.columns else 0
    if total_market_amount > 20000: dynamic_mv_cap = 500
    elif total_market_amount > 15000: dynamic_mv_cap = 300
    else: dynamic_mv_cap = 150

    return {
        'ind_zt_map': ind_zt_map, 'ind_zt_stocks': ind_zt_stocks,
        'total_zt': total_zt, 'mainline_scores': mainline_scores,
        'ind_bci_map': ind_bci_map, 'ind_zt_daily': ind_zt_daily,
        'd1_zt_codes': d1_zt_codes, 'd2_zt_codes': d2_zt_codes,
        'bas_d': bas_d, 'dynamic_mv_cap': dynamic_mv_cap,
    }


def calc_bci(ind_name, zt_list, ind_zt_daily, ind_zb_map, d1_zt_codes, d2_zt_codes, bas_d):
    n = len(zt_list)
    if n == 0:
        return 0
    bci = 0
    if n >= 8: bci += 20
    elif n >= 5: bci += 17
    elif n >= 3: bci += 13
    elif n >= 2: bci += 8
    else: bci += 3
    连板数 = sum(1 for s in zt_list if s.get('ts_code', '') in d1_zt_codes)
    首板数 = n - 连板数
    层级数 = (1 if 首板数 > 0 else 0) + (1 if 连板数 > 0 else 0)
    最高板估计 = 2 if 连板数 > 0 else 1
    三连板 = sum(1 for s in zt_list if s.get('ts_code', '') in d1_zt_codes and s.get('ts_code', '') in d2_zt_codes)
    if 三连板 > 0:
        层级数 = min(层级数 + 1, 3)
        最高板估计 = 3
    s2_bci = min(层级数 * 5, 12) + min(最高板估计 * 2, 8)
    bci += min(s2_bci, 20)
    max_amount = max((s.get('amount', 0) for s in zt_list), default=0)
    if max_amount > 500000: bci += 15
    elif max_amount > 200000: bci += 12
    elif max_amount > 100000: bci += 9
    elif max_amount > 50000: bci += 6
    else: bci += 3
    zb_count = ind_zb_map.get(ind_name, 0)
    total_try = n + zb_count
    封板率 = n / total_try if total_try > 0 else 1
    if zb_count == 0: bci += 10
    elif 封板率 > 0.8: bci += 8
    elif 封板率 > 0.6: bci += 5
    elif 封板率 > 0.4: bci += 3
    else: bci += 1
    day_counts = ind_zt_daily.get(ind_name, [0, 0, 0])
    持续天数 = sum(1 for c in day_counts if c > 0)
    有效天 = [(i, c) for i, c in enumerate(day_counts) if c > 0]
    if len(有效天) >= 2:
        斜率 = (有效天[-1][1] - 有效天[0][1]) / max(有效天[-1][0] - 有效天[0][0], 1)
    else:
        斜率 = 0
    s5_bci = 0
    if 持续天数 >= 3: s5_bci = 6
    elif 持续天数 == 2: s5_bci = 3
    else: s5_bci = 1
    if 斜率 > 1: s5_bci += 4
    elif 斜率 > 0: s5_bci += 3
    elif 斜率 == 0: s5_bci += 1
    bci += min(s5_bci, 10)
    换手板数 = 0
    for s in zt_list:
        code_s = s.get('ts_code', '')
        tr_s = bas_d.get(code_s, {}).get('turnover_rate_f', 0)
        if tr_s and tr_s > 8:
            换手板数 += 1
    换手比 = 换手板数 / n if n > 0 else 0
    bci += min(int(换手比 * 10), 10)
    pct_list = [s.get('pct_chg', 0) for s in zt_list]
    if len(pct_list) >= 2:
        pct_std = np.std(pct_list)
        if pct_std < 1: bci += 15
        elif pct_std < 2: bci += 10
        elif pct_std < 3: bci += 7
        else: bci += 3
    else:
        bci += 5
    return min(bci, 100)


# =====================================================================
# 策略A: v1.0 简化版 (100分制)
# =====================================================================
def score_v1_simple(dates, target_idx, common_data):
    """v1.0简化版评分：D1(15)+D2(20)+D3(35)+D4(15)+D5(15) = 100分"""
    T = dates[target_idx]
    history = common_data['history']
    df_t0 = common_data['df_t0']
    cp = common_data['cp']
    kline_data = common_data['kline_data']
    ind_map = common_data['ind_map']
    stats = common_data['stats']
    mainline_scores_v1 = {}  # v1用简化版主线分(满分20)
    for ind, s in stats['mainline_scores'].items():
        mainline_scores_v1[ind] = min(s, 20)

    T0 = T
    T5 = dates[target_idx - 5] if target_idx >= 5 else None
    T10 = dates[target_idx - 10] if target_idx >= 10 else None
    T20 = dates[target_idx - 20] if target_idx >= 20 else None

    results = []
    for _, row in df_t0.iterrows():
        code = row['ts_code']
        nm = row.get('name', '?')
        ind = ind_map.get(code, '?')
        pe = float(row.get('pe_ttm')) if pd.notna(row.get('pe_ttm')) else None
        mv = float(row.get('total_mv', 0)) / 10000 if row.get('total_mv') else 0
        tr = float(row.get('turnover_rate_f', 0)) if row.get('turnover_rate_f') else 0
        nb = float(row.get('net_mf_amount', 0)) if row.get('net_mf_amount') else 0
        nb_yi = nb / 10000
        pct_last = float(row.get('pct_chg', 0))
        is_zt = pct_last >= 9.5

        p = cp.get(code, {})
        c0 = p.get(T0)
        c5 = p.get(T5) if T5 else None
        c10 = p.get(T10) if T10 else None
        c20 = p.get(T20) if T20 else None
        if not c0 or not c5 or not c10 or not c20:
            continue
        if c5 == 0 or c10 == 0 or c20 == 0:
            continue
        r5 = (c0 - c5) / c5 * 100
        r10 = (c0 - c10) / c10 * 100
        r20 = (c0 - c20) / c20 * 100

        # D1: 多周期共振 (15分)
        big = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small = 1 if r5 > 3 else (-1 if r5 < -2 else 0)
        period_raw = big * 3 + mid * 2 + small * 1
        d1 = int((period_raw + 6) / 12 * 15 + 0.5)
        d1 = max(0, min(15, d1))

        # D2: 主线热点 (20分)
        d2 = mainline_scores_v1.get(ind, 0)

        # D3: 三Skill (35分)
        klines = kline_data.get(code, [])
        d3 = 0; mistery = 0; tds = 0; yuanzi = 0; is_ma_bull = False
        if len(klines) >= 5:
            kdf = pd.DataFrame(klines)
            cc = kdf['close'].values.astype(float)
            oo = kdf['open'].values.astype(float)
            hh = kdf['high'].values.astype(float)
            ll = kdf['low'].values.astype(float)
            vv = kdf['vol'].values.astype(float)
            n = len(cc)
            ma5 = pd.Series(cc).rolling(5).mean()
            ma10 = pd.Series(cc).rolling(10).mean() if n >= 10 else pd.Series([np.nan]*n)
            if n >= 10 and not pd.isna(ma5.iloc[-1]) and not pd.isna(ma10.iloc[-1]):
                if ma5.iloc[-1] > ma10.iloc[-1] and cc[-1] > ma5.iloc[-1]:
                    is_ma_bull = True
            # Mistery (15分)
            if is_ma_bull: mistery += 4
            elif n >= 5 and cc[-1] > ma5.iloc[-1]: mistery += 2
            if n >= 10:
                bb_mid = pd.Series(cc).rolling(min(20, n)).mean()
                bb_std = pd.Series(cc).rolling(min(20, n)).std()
                if not pd.isna(bb_mid.iloc[-1]) and not pd.isna(bb_std.iloc[-1]) and bb_mid.iloc[-1] > 0:
                    bbw = bb_std.iloc[-1] * 2 / bb_mid.iloc[-1]
                    if bbw < 0.10: mistery += 4
                    elif bbw < 0.15: mistery += 3
                    elif bbw < 0.20: mistery += 2
            if n >= 2:
                if cc[-1] > cc[-2] and vv[-1] > vv[-2]: mistery += 3
                elif cc[-1] > cc[-2]: mistery += 1
            if n >= 1:
                body = cc[-1] - oo[-1]
                upper = hh[-1] - max(cc[-1], oo[-1])
                if body > 0 and body > upper * 2: mistery += 2
                elif body > 0: mistery += 1
            mistery = min(mistery, 15)
            # TDS (10分)
            peaks, troughs = [], []
            for i in range(2, n - 2):
                if hh[i] > max(hh[max(0, i-3):i]) and hh[i] > max(hh[i+1:min(n, i+3)]):
                    peaks.append(hh[i])
                if ll[i] < min(ll[max(0, i-3):i]) and ll[i] < min(ll[i+1:min(n, i+3)]):
                    troughs.append(ll[i])
            if len(peaks) >= 2 and len(troughs) >= 2:
                if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]: tds += 4
                elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]: tds += 2
            if n >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]: tds += 2
            if peaks and cc[-1] > peaks[-1]: tds += 2
            if n >= 2 and cc[-1] > oo[-1] and cc[-1] > hh[-2] and cc[-2] < oo[-2]: tds += 2
            tds = min(tds, 10)
            # 元子元 (10分)
            if r5 > 10: yuanzi += 3
            elif r5 > 5: yuanzi += 4
            elif r5 > 0: yuanzi += 3
            if pct_last > 0: yuanzi += 3
            elif pct_last > -1: yuanzi += 2
            if mainline_scores_v1.get(ind, 0) >= 10: yuanzi += 3
            elif mainline_scores_v1.get(ind, 0) >= 5: yuanzi += 2
            yuanzi = min(yuanzi, 10)
            d3 = mistery + tds + yuanzi

        # D4: 安全边际 (15分)
        d4 = 0
        if abs(r5) <= 5: d4 += 5
        elif abs(r5) <= 10: d4 += 3
        elif abs(r5) <= 15: d4 += 1
        if abs(r10) <= 10: d4 += 5
        elif abs(r10) <= 15: d4 += 3
        elif abs(r10) <= 20: d4 += 1
        if tr and tr <= 5: d4 += 5
        elif tr and tr <= 10: d4 += 3
        elif tr and tr <= 15: d4 += 1
        d4 = min(d4, 15)

        # D5: 基本面 (15分)
        d5 = 0
        if pe and pe > 0:
            if pe <= 15: d5 += 6
            elif pe <= 25: d5 += 5
            elif pe <= 40: d5 += 4
            elif pe <= 60: d5 += 3
            elif pe <= 100: d5 += 1
        if 100 <= mv <= 500: d5 += 3
        elif 500 < mv <= 2000: d5 += 2
        elif mv > 2000: d5 += 1
        elif 50 <= mv < 100: d5 += 2
        if nb_yi > 1: d5 += 6
        elif nb_yi > 0.3: d5 += 4
        elif nb_yi > 0: d5 += 2
        d5 = min(d5, 15)

        # 风险扣分
        risk = 0
        if r5 > 20: risk += 5 if not is_ma_bull else 3
        elif r5 > 15: risk += 3
        if r10 > 30: risk += 5
        elif r10 > 20: risk += 3
        if nb_yi < -1: risk += 3
        elif nb_yi < -0.5: risk += 1
        if mv > 3000: risk += 3
        if stats['ind_zt_map'].get(ind, 0) < 2 and mainline_scores_v1.get(ind, 0) < 5:
            risk += 3
        risk = min(risk, 30)

        # 保护因子
        protect = 0
        if is_ma_bull: protect += 3
        if mistery >= 12: protect += 2
        if is_zt: protect += 3
        if is_zt and stats['ind_zt_map'].get(ind, 0) >= 3: protect += 2
        if nb_yi > 2: protect += 2
        protect = min(protect, 15)

        raw_total = d1 + d2 + d3 + d4 + d5
        net_risk = max(risk - protect, 0)
        total = raw_total - net_risk

        results.append({
            'code': code, 'name': nm, 'industry': ind, 'date': T,
            'close': c0, 'pct_chg': pct_last, 'is_zt': is_zt,
            'total': total, 'r5': r5, 'r10': r10, 'r20': r20,
            'pe': pe, 'mv': mv, 'tr': tr, 'nb_yi': nb_yi,
        })

    results.sort(key=lambda x: x['total'], reverse=True)
    return results


# =====================================================================
# 策略B: v2.1 完整版 (150分制) — 含BCI+WR+60分钟K线
# =====================================================================
def score_v2_full(dates, target_idx, common_data):
    """v2.1完整版评分：D1(15)+D2(25)+D3(47)+D4(15)+D5(15)+D9(15) = 132基础分 + 风险/保护"""
    T = dates[target_idx]
    history = common_data['history']
    df_t0 = common_data['df_t0']
    cp = common_data['cp']
    kline_data = common_data['kline_data']
    ind_map = common_data['ind_map']
    stats = common_data['stats']
    mainline_scores = stats['mainline_scores']
    ind_bci_map = stats['ind_bci_map']
    bas_d = stats['bas_d']
    ind_zt_map = stats['ind_zt_map']
    dynamic_mv_cap = stats['dynamic_mv_cap']

    T0 = T
    T5 = dates[target_idx - 5] if target_idx >= 5 else None
    T10 = dates[target_idx - 10] if target_idx >= 10 else None
    T20 = dates[target_idx - 20] if target_idx >= 20 else None

    results = []
    for _, row in df_t0.iterrows():
        code = row['ts_code']
        nm = row.get('name', '?')
        ind = ind_map.get(code, '?')
        pe = float(row.get('pe_ttm')) if pd.notna(row.get('pe_ttm')) else None
        mv = float(row.get('total_mv', 0)) / 10000 if row.get('total_mv') else 0
        tr = float(row.get('turnover_rate_f', 0)) if row.get('turnover_rate_f') else 0
        nb = float(row.get('net_mf_amount', 0)) if row.get('net_mf_amount') else 0
        nb_yi = nb / 10000
        pct_last = float(row.get('pct_chg', 0))
        is_zt = pct_last >= 9.5

        p = cp.get(code, {})
        c0 = p.get(T0)
        c5 = p.get(T5) if T5 else None
        c10 = p.get(T10) if T10 else None
        c20 = p.get(T20) if T20 else None
        if not c0 or not c5 or not c10 or not c20: continue
        if c5 == 0 or c10 == 0 or c20 == 0: continue
        r5 = (c0 - c5) / c5 * 100
        r10 = (c0 - c10) / c10 * 100
        r20 = (c0 - c20) / c20 * 100

        # 粗筛
        big_raw = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid_raw = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small_raw = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
        ps_raw = big_raw * 3 + mid_raw * 2 + small_raw * 1
        if ps_raw < 4: continue
        if mv < 30: continue
        if r5 > 40 or r10 > 50: continue

        # D1: 多周期共振 (15分)
        big = 0
        if r20 > 10: big = 3
        elif r20 > 5: big = 2
        elif r20 > 0: big = 1
        elif r20 > -5: big = 0
        elif r20 > -10: big = -1
        else: big = -3
        mid = 0
        if r10 > 5: mid = 2
        elif r10 > 2: mid = 1
        elif r10 > -2: mid = 0
        elif r10 > -5: mid = -1
        else: mid = -2
        small = 0
        if r5 > 3: small = 1
        elif r5 > 0: small = 0
        elif r5 > -2: small = 0
        else: small = -1
        period_raw = big + mid + small
        d1 = int((period_raw + 6) / 12 * 15 + 0.5)
        d1 = max(0, min(15, d1))

        # D2: 主线热点 (25分，含BCI)
        d2_base = mainline_scores.get(ind, 0)
        ind_bci = ind_bci_map.get(ind, 0)
        if ind_bci >= 70: d2_base += 3
        elif ind_bci >= 50: d2_base += 2
        elif ind_bci >= 30: d2_base += 1
        d2 = min(d2_base, 25)

        # D3: 三Skill (47分)
        klines = kline_data.get(code, [])
        d3 = 0; mistery = 0; tds = 0; yuanzi = 0; txcg_model = 0
        is_ma_bull = False; consecutive_yang = 0
        vol5 = 0; vol10 = 0; bbw_val = 0
        ma5_val = 0; ma10_val = 0; ma20_val = 0
        peaks = []; troughs = []
        vol_ratio_val = 1

        if len(klines) >= 10:
            kdf = pd.DataFrame(klines).sort_values('trade_date').drop_duplicates(subset=['trade_date']).reset_index(drop=True)
            n = len(kdf)
            if n >= 10:
                cc = kdf['close'].astype(float).values
                oo = kdf['open'].astype(float).values
                hh = kdf['high'].astype(float).values
                ll = kdf['low'].astype(float).values
                vv = kdf['vol'].astype(float).values
                ma5_val = pd.Series(cc).rolling(5).mean().iloc[-1]
                ma10_val = pd.Series(cc).rolling(10).mean().iloc[-1]
                ma20_val = pd.Series(cc).rolling(min(20, n)).mean().iloc[-1]
                ema12 = pd.Series(cc).ewm(span=12, adjust=False).mean()
                ema26 = pd.Series(cc).ewm(span=min(26, n), adjust=False).mean()
                dif = (ema12 - ema26).iloc[-1]
                dea = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]
                is_ma_bull = cc[-1] > ma5_val > ma10_val > ma20_val
                for k in range(n - 1, -1, -1):
                    if cc[k] > oo[k]: consecutive_yang += 1
                    else: break
                vol5 = np.mean(vv[-5:])
                vol10 = np.mean(vv[-min(10, n):])
                vr_real = bas_d.get(code, {}).get('volume_ratio', 0)
                vol_ratio_val = vr_real if vr_real > 0 else (vol5 / vol10 if vol10 > 0 else 1)
                std20 = np.std(cc[-min(20, n):])
                bbw_val = (4 * std20) / ma20_val if ma20_val > 0 else 0

                # Mistery (20分)
                if is_ma_bull: mistery += 3
                elif cc[-1] > ma5_val > ma10_val: mistery += 2
                elif cc[-1] > ma20_val: mistery += 1
                ma5s = pd.Series(cc).rolling(5).mean()
                ma20s = pd.Series(cc).rolling(min(20, n)).mean()
                m2 = 0
                if n >= 7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5] <= ma20s.iloc[-5] and ma5_val > ma20_val:
                    m2 += 2
                if n >= 5:
                    below = any(cc[i] < ma5s.iloc[i] for i in range(max(0, n-5), n-1) if not np.isnan(ma5s.iloc[i]))
                    if below and cc[-1] > ma5_val: m2 += 1
                if bbw_val < 0.12 and pct_last > 3: m2 += 2
                elif bbw_val < 0.15 and pct_last > 3: m2 += 1
                mistery += min(3, m2)
                if pct_last < 2 and vol5 / vol10 > 2.5: mistery -= 1
                if n >= 4 and hh[-1] < max(hh[-4:-1]) and pct_last < 1: mistery -= 1
                if vol_ratio_val > 1.3 and pct_last > 0: mistery += 2
                elif vol_ratio_val > 1: mistery += 1
                if dif > dea: mistery += 1
                if n >= 3:
                    prev_range = abs(cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                    if pct_last > 5 and vol_ratio_val > 1.5 and prev_range < 2: mistery += 2
                    if n >= 2:
                        prev_upper = (hh[-2] - cc[-2]) / cc[-2] * 100 if cc[-2] > 0 else 0
                        if prev_upper > 3 and cc[-1] > hh[-2] * 0.98: mistery += 1
                m6 = 0
                if cc[-1] > ma5_val > ma10_val > ma20_val and pct_last > 0: m6 += 2
                elif cc[-1] > ma5_val > ma10_val: m6 += 1
                if ma20_val > 0 and abs(cc[-1] - ma20_val) / ma20_val < 0.08: m6 += 1
                if n >= 20 and cc[-1] > min(ll[-20:]) * 1.05: m6 += 1
                if n >= 7:
                    chg_7d = (cc[-1] - cc[-7]) / cc[-7] * 100 if cc[-7] > 0 else 0
                    if chg_7d > 5: m6 += 1
                    elif chg_7d < -3: m6 -= 1
                mistery += min(5, max(0, m6))
                mistery = max(min(mistery, 20), 0)

                # TDS (12分)
                win = min(5, n // 3)
                for i in range(win, n - win):
                    if hh[i] >= max(hh[max(0, i-win):i]) and hh[i] >= max(hh[i+1:min(n, i+win+1)]):
                        peaks.append(hh[i])
                    if ll[i] <= min(ll[max(0, i-win):i]) and ll[i] <= min(ll[i+1:min(n, i+win+1)]):
                        troughs.append(ll[i])
                if len(peaks) >= 2 and len(troughs) >= 2:
                    if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]: tds += 3
                    elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]: tds += 2
                elif len(peaks) >= 2 and peaks[-1] > peaks[-2]: tds += 2
                if n >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]: tds += 2
                elif n >= 2 and hh[-1] > hh[-2]: tds += 1
                if n >= 3 and cc[-2] < oo[-2] and cc[-1] > oo[-1] and cc[-1] > hh[-2]: tds += 1
                if peaks and cc[-1] > peaks[-1]: tds += 2
                elif pct_last >= 9.5: tds += 1
                if n >= 4 and r5 < -3:
                    k1b = abs(cc[-3] - oo[-3]); k2b = abs(cc[-2] - oo[-2]); k3b = abs(cc[-1] - oo[-1])
                    if k2b <= max(k1b, k3b) and cc[-1] > oo[-1] and cc[-1] > hh[-2]: tds += 2
                if r5 < -10 and pct_last > 3: tds += 2
                elif r5 < -5 and pct_last > 0:
                    body = abs(cc[-1] - oo[-1])
                    lower_shadow = min(cc[-1], oo[-1]) - ll[-1]
                    if lower_shadow > body * 2 and lower_shadow > 0: tds += 1
                if len(peaks) >= 3 and len(troughs) >= 3:
                    p_t1 = 1 if peaks[-2] > peaks[-3] else -1
                    p_t2 = 1 if peaks[-1] > peaks[-2] else -1
                    t_t1 = 1 if troughs[-2] > troughs[-3] else -1
                    t_t2 = 1 if troughs[-1] > troughs[-2] else -1
                    if p_t1 != p_t2 or t_t1 != t_t2:
                        if cc[-1] > peaks[-1]: tds += 2
                        elif pct_last > 5: tds += 1
                tds = max(min(tds, 12), 0)

                # 元子元 (10分)
                if is_zt and r5 < -5: yuanzi += 5
                elif is_zt and r5 < 5: yuanzi += 4
                elif pct_last >= 5 and r5 < 0: yuanzi += 4
                elif is_zt and 5 <= r5 < 15: yuanzi += 3
                elif pct_last >= 5 and 0 <= r5 < 10: yuanzi += 3
                elif is_zt and r5 >= 15:
                    yuanzi += 0 if vol_ratio_val >= 3 else 2
                elif pct_last > 0 and r5 < 10: yuanzi += 2
                elif pct_last < -3 and r5 > 15: yuanzi += 0
                elif pct_last <= 0:
                    yuanzi += 2 if r5 < -10 else 1
                if pct_last > 3 and vol_ratio_val < 1.2: yuanzi += 2
                elif pct_last < 2 and vol_ratio_val > 2.5: yuanzi -= 1
                if mainline_scores.get(ind, 0) >= 10: yuanzi += 2
                elif mainline_scores.get(ind, 0) >= 5: yuanzi += 1
                yuanzi = max(min(yuanzi, 10), 0)

                # TXCG六大模型 (0-5)
                ind_zt_count_local = ind_zt_map.get(ind, 0)
                if is_zt and ind_zt_count_local >= 3: txcg_model += 1
                if r5 < -5 and pct_last > 3: txcg_model += 1
                if n >= 3:
                    prev_chg = (cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                    if prev_chg < -3 and pct_last > 2: txcg_model += 1
                if ma5_val > 0 and abs(cc[-1] - ma5_val) / ma5_val < 0.02 and pct_last > 0: txcg_model += 1
                if n >= 2:
                    body_prev = abs(cc[-2] - oo[-2])
                    ls_prev = min(cc[-2], oo[-2]) - ll[-2]
                    if ls_prev > body_prev * 2 and ls_prev > 0 and pct_last > 0: txcg_model += 1
                if is_zt and ind_zt_count_local == 1: txcg_model += 1
                txcg_model = min(txcg_model, 5)
                d3 = mistery + tds + yuanzi + txcg_model

        # D4: 安全边际 (15分)
        d4 = 0
        if abs(r5) <= 5: d4 += 5
        elif abs(r5) <= 10: d4 += 3
        elif abs(r5) <= 15: d4 += 1
        if abs(r10) <= 10: d4 += 5
        elif abs(r10) <= 15: d4 += 3
        elif abs(r10) <= 20: d4 += 1
        if tr and tr <= 5: d4 += 5
        elif tr and tr <= 10: d4 += 3
        elif tr and tr <= 15: d4 += 1
        d4 = min(d4, 15)

        # D5: 基本面 (15分)
        d5 = 0
        if pe and pe > 0:
            if pe <= 15: d5 += 6
            elif pe <= 25: d5 += 5
            elif pe <= 40: d5 += 4
            elif pe <= 60: d5 += 3
            elif pe <= 100: d5 += 1
        if 100 <= mv <= 500: d5 += 3
        elif 500 < mv <= 2000: d5 += 2
        elif mv > 2000: d5 += 1
        elif 50 <= mv < 100: d5 += 2
        if nb_yi > 1: d5 += 6
        elif nb_yi > 0.3: d5 += 4
        elif nb_yi > 0: d5 += 2
        d5 = min(d5, 15)

        # D9: 百胜WR (15分)
        d9 = 0; wr1 = 0; wr2 = 0; wr3 = 0
        vr_wr = bas_d.get(code, {}).get('volume_ratio', 0)
        if is_zt:
            wr1 += 1
            if vr_wr and vr_wr >= 3: wr1 += 1
            if tr and tr >= 8: wr1 += 1
            if is_ma_bull: wr1 += 1
            if 30 <= mv <= 150: wr1 += 1
            if nb_yi > 0: wr1 += 1
            if len(klines) >= 1:
                last_open = float(klines[-1].get('open', 0))
                if last_open > 0 and (c0 - last_open) / last_open * 100 < 2: wr1 += 1
        if len(klines) >= 10 and n >= 10:
            if bbw_val < 0.15: wr2 += 1
            if vr_wr and vr_wr >= 2.5: wr2 += 1
            elif vol10 > 0 and vol5 / vol10 > 2.5: wr2 += 1
            if pct_last >= 7: wr2 += 1
            if is_ma_bull: wr2 += 1
            if peaks and cc[-1] > peaks[-1]: wr2 += 1
        # WR3(60分钟)延迟到第二阶段计算
        best_wr = max(wr1, wr2)
        if best_wr == wr1: best_wr_max = 7
        elif best_wr == wr2: best_wr_max = 5
        else: best_wr_max = 4
        d9 = int(best_wr / best_wr_max * 15 + 0.5) if best_wr_max > 0 else 0
        d9 = min(d9, 15)
        # 保存wr1/wr2供第二阶段使用
        _wr1_save = wr1; _wr2_save = wr2

        # 风险扣分 (0~-30)
        risk = 0
        ind_zt_count = ind_zt_map.get(ind, 0)
        if r5 > 20: risk += 3 if is_ma_bull else 5
        elif r5 > 15: risk += 2
        if r10 > 25: risk += 3 if is_ma_bull else 5
        elif r10 > 20: risk += 2
        if r20 > 50: risk += 8
        elif r20 > 35: risk += 4
        ind_bci_risk = ind_bci_map.get(ind, 0)
        if ind_zt_count < 3:
            if ind_bci_risk >= 50: risk += 1
            elif mainline_scores.get(ind, 0) >= 8: risk += 2
            elif ind_zt_count == 0:
                risk += 3 if ind_bci_risk >= 30 else 5
            else: risk += 3
        if nb_yi < -2: risk += 1 if is_zt else 3
        elif nb_yi < -0.5: risk += 1
        if mv > dynamic_mv_cap:
            risk += 5 if mv > 1000 else 3
        if tr and tr > 50: risk += 3
        elif tr and tr > 30: risk += 1
        risk = min(risk, 30)

        # 保护因子 (0~+15)
        protect = 0
        if is_ma_bull: protect += 3
        if consecutive_yang >= 5: protect += 3
        elif consecutive_yang >= 3: protect += 2
        if mistery >= 12: protect += 2
        if is_zt: protect += 3
        if is_zt and ind_zt_count >= 3: protect += 2
        if nb_yi > 2: protect += 2
        if ind_bci_map.get(ind, 0) >= 70: protect += 2
        elif ind_bci_map.get(ind, 0) >= 50: protect += 1
        protect = min(protect, 15)

        raw_total = d1 + d2 + d3 + d4 + d5 + d9
        net_risk = max(risk - protect, 0)
        total = raw_total - net_risk

        results.append({
            'code': code, 'name': nm, 'industry': ind, 'date': T,
            'close': c0, 'pct_chg': pct_last, 'is_zt': is_zt,
            'total': total, 'r5': r5, 'r10': r10, 'r20': r20,
            'pe': pe, 'mv': mv, 'tr': tr, 'nb_yi': nb_yi,
            'bci': ind_bci_map.get(ind, 0),
            '_wr1': _wr1_save, '_wr2': _wr2_save,
        })

    results.sort(key=lambda x: x['total'], reverse=True)
    # 第二阶段：只对TOP50补算60分钟WR3
    for item in results[:50]:
        code = item['code']
        wr3 = 0
        kline_60m = load_60m_kline(code, T)
        if kline_60m and len(kline_60m.get('vols', [])) >= 12:
            vols_60 = kline_60m['vols']
            closes_60 = kline_60m['closes']
            highs_60 = kline_60m['highs']
            lows_60 = kline_60m['lows']
            n60 = len(vols_60)
            first_dbl_idx = None
            for i_60 in range(max(1, n60 - 20), n60):
                if vols_60[i_60] >= vols_60[i_60-1] * 2 and closes_60[i_60] > closes_60[i_60-1]:
                    recent_range = closes_60[max(0, i_60-20):i_60+1]
                    mid_price = (max(recent_range) + min(recent_range)) / 2
                    if closes_60[i_60] <= mid_price * 1.05:
                        first_dbl_idx = i_60
                        break
            if first_dbl_idx is not None:
                wr3 += 1
                first_low = lows_60[first_dbl_idx]
                first_high = highs_60[first_dbl_idx]
                for j_60 in range(first_dbl_idx + 1, n60):
                    if vols_60[j_60] >= vols_60[j_60-1] * 2:
                        if closes_60[j_60] > first_high: wr3 += 1
                        if lows_60[j_60] >= first_low: wr3 += 1
                        break
                if closes_60[-1] >= first_low: wr3 += 1
        if wr3 > max(item['_wr1'], item['_wr2']):
            new_d9 = int(wr3 / 4 * 15 + 0.5)
            old_d9 = int(max(item['_wr1'], item['_wr2']) / (7 if item['_wr1'] >= item['_wr2'] else 5) * 15 + 0.5)
            item['total'] += min(new_d9, 15) - min(old_d9, 15)
    results.sort(key=lambda x: x['total'], reverse=True)
    return results


# =====================================================================
# 策略C: 9 Skill v3.3 (105分制)
# =====================================================================
def score_9skill(dates, target_idx, common_data):
    """9 Skill v3.3评分：S1-S9 + TXCG加分 = 105分"""
    T = dates[target_idx]
    history = common_data['history']
    df_t0 = common_data['df_t0']
    cp = common_data['cp']
    kline_data = common_data['kline_data']
    ind_map = common_data['ind_map']
    stats = common_data['stats']
    mainline_scores = stats['mainline_scores']
    ind_bci_map = stats['ind_bci_map']
    bas_d = stats['bas_d']
    ind_zt_map = stats['ind_zt_map']

    T0 = T
    T5 = dates[target_idx - 5] if target_idx >= 5 else None
    T10 = dates[target_idx - 10] if target_idx >= 10 else None
    T20 = dates[target_idx - 20] if target_idx >= 20 else None

    # 推断情绪阶段
    total_zt = stats['total_zt']
    if total_zt >= 80: emotion_stage = '起爆'
    elif total_zt >= 60: emotion_stage = '一致'
    elif total_zt >= 40: emotion_stage = '修复'
    elif total_zt >= 25: emotion_stage = '分歧'
    elif total_zt >= 15: emotion_stage = '启动'
    else: emotion_stage = '退潮'

    results = []
    for _, row in df_t0.iterrows():
        code = row['ts_code']
        nm = row.get('name', '?')
        ind = ind_map.get(code, '?')
        pe = float(row.get('pe_ttm')) if pd.notna(row.get('pe_ttm')) else None
        mv = float(row.get('total_mv', 0)) / 10000 if row.get('total_mv') else 0
        circ_mv = float(row.get('circ_mv', 0)) if row.get('circ_mv') else 0
        tr = float(row.get('turnover_rate_f', 0)) if row.get('turnover_rate_f') else 0
        nb = float(row.get('net_mf_amount', 0)) if row.get('net_mf_amount') else 0
        nb_yi = nb / 10000
        pct_last = float(row.get('pct_chg', 0))
        is_zt = pct_last >= 9.5

        p = cp.get(code, {})
        c0 = p.get(T0)
        c5 = p.get(T5) if T5 else None
        c10 = p.get(T10) if T10 else None
        c20 = p.get(T20) if T20 else None
        if not c0 or not c5 or not c10 or not c20: continue
        if c5 == 0 or c10 == 0 or c20 == 0: continue
        r5 = (c0 - c5) / c5 * 100
        r10 = (c0 - c10) / c10 * 100
        r20 = (c0 - c20) / c20 * 100

        # 粗筛（宽松一点，让更多票进来）
        if mv < 20: continue
        if r5 > 50 or r10 > 60: continue

        klines = kline_data.get(code, [])
        if len(klines) < 5: continue

        kdf = pd.DataFrame(klines).sort_values('trade_date').drop_duplicates(subset=['trade_date']).reset_index(drop=True)
        n = len(kdf)
        if n < 5: continue

        cc = kdf['close'].astype(float).values
        oo = kdf['open'].astype(float).values
        hh = kdf['high'].astype(float).values
        ll = kdf['low'].astype(float).values
        vv = kdf['vol'].astype(float).values

        ma5 = float(np.mean(cc[-5:])) if n >= 5 else cc[-1]
        ma10 = float(np.mean(cc[-10:])) if n >= 10 else float(np.mean(cc))
        ma20 = float(np.mean(cc[-20:])) if n >= 20 else float(np.mean(cc))
        if ma5 > ma10 > ma20: ma_tag = '多头'
        elif ma5 > ma10: ma_tag = '短多'
        else: ma_tag = '弱'
        is_ma_bull = ma_tag == '多头'

        avg_v5 = float(np.mean(vv[-6:-1])) if n >= 6 else float(np.mean(vv))
        vol_ratio = vv[-1] / avg_v5 if avg_v5 > 0 else 0
        vr_real = bas_d.get(code, {}).get('volume_ratio', 0)
        if vr_real > 0: vol_ratio = vr_real

        std20 = float(np.std(cc[-min(20, n):])) if n >= 5 else 0
        bbw = (4 * std20) / ma20 if ma20 > 0 else 0

        sector_zt = ind_zt_map.get(ind, 0)
        bci_score = ind_bci_map.get(ind, 0)

        # ===== S1-TXCG (15分) =====
        s1 = 0
        # 天时(0-3)
        tianshi = 0
        if emotion_stage in ('起爆', '一致'): tianshi = 3
        elif emotion_stage in ('修复', '分歧', '启动'): tianshi = 2
        elif emotion_stage == '冰点': tianshi = 1
        # 地利(0-3)
        dili = 0
        if is_ma_bull: dili += 1
        if tr and 5 <= tr <= 20: dili += 1
        if r5 < 0 and pct_last > 0: dili += 1  # 做空动能衰竭
        # 人和(0-3)
        renhe = 0
        if bci_score >= 60: renhe += 2
        elif bci_score >= 30: renhe += 1
        elif sector_zt >= 3: renhe += 1
        if is_zt: renhe += 1
        renhe = min(renhe, 3)
        s1_raw = tianshi + dili + renhe  # 0-9
        s1 = int(s1_raw / 9 * 15 + 0.5)
        s1 = max(0, min(15, s1))

        # ===== S2-元子元 (10分) =====
        s2 = 0
        if is_zt and r5 < -5: s2 += 5
        elif is_zt and r5 < 5: s2 += 4
        elif pct_last >= 5 and r5 < 0: s2 += 4
        elif is_zt and 5 <= r5 < 15: s2 += 3
        elif pct_last >= 5 and 0 <= r5 < 10: s2 += 3
        elif is_zt and r5 >= 15:
            s2 += 0 if vol_ratio >= 3 else 2
        elif pct_last > 0 and r5 < 10: s2 += 2
        elif pct_last <= 0:
            s2 += 2 if r5 < -10 else 1
        if pct_last > 3 and vol_ratio < 1.2: s2 += 2
        elif pct_last < 2 and vol_ratio > 2.5: s2 -= 1
        if mainline_scores.get(ind, 0) >= 10: s2 += 2
        elif mainline_scores.get(ind, 0) >= 5: s2 += 1
        s2 = max(0, min(10, s2))

        # ===== S3-山茶花 (15分) =====
        s3 = 0
        # 主动性(0-5)
        zhudong = 0
        if is_zt: zhudong += 2
        if pct_last > 5: zhudong += 1
        if vol_ratio > 1.5: zhudong += 1
        if n >= 2 and hh[-1] > hh[-2]: zhudong += 1
        zhudong = min(zhudong, 5)
        # 带动性(0-5)
        daidong = 0
        if sector_zt >= 5: daidong += 2
        elif sector_zt >= 3: daidong += 1
        if bci_score >= 60: daidong += 2
        elif bci_score >= 30: daidong += 1
        if mainline_scores.get(ind, 0) >= 10: daidong += 1
        daidong = min(daidong, 5)
        # 抗跌性(0-5)
        kangdie = 0
        if is_ma_bull: kangdie += 2
        if r5 > 0 and r10 > 0: kangdie += 1
        if n >= 3 and min(ll[-3:]) > ma10: kangdie += 1
        if tr and tr < 15: kangdie += 1
        kangdie = min(kangdie, 5)
        s3 = zhudong + daidong + kangdie
        s3 = min(s3, 15)

        # ===== S4-Mistery (10分) =====
        s4 = 0
        if is_ma_bull: s4 += 2
        elif cc[-1] > ma5 > ma10: s4 += 1
        if bbw < 0.12 and pct_last > 3: s4 += 2
        elif bbw < 0.15 and pct_last > 3: s4 += 1
        if vol_ratio > 1.3 and pct_last > 0: s4 += 2
        elif vol_ratio > 1: s4 += 1
        if n >= 3:
            prev_range = abs(cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
            if pct_last > 5 and vol_ratio > 1.5 and prev_range < 2: s4 += 2
        if pct_last < 2 and vol_ratio > 2.5: s4 -= 1
        s4 = max(0, min(10, s4))

        # ===== S5-TDS (10分) =====
        s5 = 0
        peaks = []; troughs = []
        win = min(5, n // 3) if n >= 6 else 2
        for i in range(win, n - win):
            if hh[i] >= max(hh[max(0, i-win):i]) and hh[i] >= max(hh[i+1:min(n, i+win+1)]):
                peaks.append(hh[i])
            if ll[i] <= min(ll[max(0, i-win):i]) and ll[i] <= min(ll[i+1:min(n, i+win+1)]):
                troughs.append(ll[i])
        if len(peaks) >= 2 and len(troughs) >= 2:
            if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]: s5 += 3
            elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]: s5 += 2
        if n >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]: s5 += 2
        elif n >= 2 and hh[-1] > hh[-2]: s5 += 1
        if peaks and cc[-1] > peaks[-1]: s5 += 2
        if r5 < -10 and pct_last > 3: s5 += 2
        s5 = max(0, min(10, s5))

        # ===== S6-百胜WR (15分) =====
        s6 = 0; wr1 = 0; wr2 = 0; wr3 = 0
        if is_zt:
            wr1 += 1
            if vol_ratio >= 3: wr1 += 1
            if tr and tr >= 8: wr1 += 1
            if is_ma_bull: wr1 += 1
            if 30 <= mv <= 150: wr1 += 1
            if nb_yi > 0: wr1 += 1
            if len(klines) >= 1:
                last_open = float(klines[-1].get('open', 0))
                if last_open > 0 and (c0 - last_open) / last_open * 100 < 2: wr1 += 1
        if n >= 10:
            if bbw < 0.15: wr2 += 1
            if vol_ratio >= 2.5: wr2 += 1
            if pct_last >= 7: wr2 += 1
            if is_ma_bull: wr2 += 1
            if peaks and cc[-1] > peaks[-1]: wr2 += 1
        # WR3(60分钟)延迟到第二阶段计算
        best_wr = max(wr1, wr2)
        if best_wr == wr1: best_wr_max = 7
        elif best_wr == wr2: best_wr_max = 5
        else: best_wr_max = 4
        s6 = int(best_wr / best_wr_max * 15 + 0.5) if best_wr_max > 0 else 0
        s6 = min(s6, 15)
        _wr1_save = wr1; _wr2_save = wr2

        # ===== S7-事件驱动 (10分) =====
        s7 = 0
        if sector_zt >= 5: s7 += 3
        elif sector_zt >= 3: s7 += 2
        elif sector_zt >= 1: s7 += 1
        if bci_score >= 60: s7 += 2
        elif bci_score >= 30: s7 += 1
        if r5 < -5 and pct_last > 0: s7 += 2  # 低位埋伏
        elif r5 < 0 and pct_last > 0: s7 += 1
        if nb_yi > 0.5: s7 += 2
        elif nb_yi > 0: s7 += 1
        s7 = max(0, min(10, s7))

        # ===== S8-多周期 (5分) =====
        s8 = 0
        big_s = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid_s = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small_s = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
        period_raw = big_s * 3 + mid_s * 2 + small_s * 1  # -6~+6
        s8 = int((period_raw + 6) / 12 * 5 + 0.5)
        s8 = max(0, min(5, s8))

        # ===== S9-基本面 (10分) =====
        s9 = 0
        if pe and pe > 0:
            if pe <= 20: s9 += 4
            elif pe <= 40: s9 += 3
            elif pe <= 60: s9 += 2
            elif pe <= 100: s9 += 1
        if 80 <= mv <= 400: s9 += 3
        elif 400 < mv <= 1500: s9 += 2
        elif 40 <= mv < 80: s9 += 1
        if nb_yi > 0.5: s9 += 3
        elif nb_yi > 0: s9 += 2
        s9 = min(s9, 10)

        # ===== TXCG加分 (0-5) =====
        txcg_bonus = 0
        if is_zt and sector_zt >= 3: txcg_bonus += 1
        if r5 < -5 and pct_last > 3: txcg_bonus += 1
        if n >= 3:
            prev_chg = (cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
            if prev_chg < -3 and pct_last > 2: txcg_bonus += 1
        if ma5 > 0 and abs(cc[-1] - ma5) / ma5 < 0.02 and pct_last > 0: txcg_bonus += 1
        if n >= 2:
            body_prev = abs(cc[-2] - oo[-2])
            ls_prev = min(cc[-2], oo[-2]) - ll[-2]
            if ls_prev > body_prev * 2 and ls_prev > 0 and pct_last > 0: txcg_bonus += 1
        txcg_bonus = min(txcg_bonus, 5)

        total = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + txcg_bonus

        results.append({
            'code': code, 'name': nm, 'industry': ind, 'date': T,
            'close': c0, 'pct_chg': pct_last, 'is_zt': is_zt,
            'total': total, 'r5': r5, 'r10': r10, 'r20': r20,
            'pe': pe, 'mv': mv, 'tr': tr, 'nb_yi': nb_yi,
            'bci': bci_score,
            '_wr1': _wr1_save, '_wr2': _wr2_save,
        })

    results.sort(key=lambda x: x['total'], reverse=True)
    # 第二阶段：只对TOP50补算60分钟WR3
    for item in results[:50]:
        code = item['code']
        wr3 = 0
        kline_60m = load_60m_kline(code, T)
        if kline_60m and len(kline_60m.get('vols', [])) >= 12:
            vols_60 = kline_60m['vols']
            closes_60 = kline_60m['closes']
            highs_60 = kline_60m['highs']
            lows_60 = kline_60m['lows']
            n60 = len(vols_60)
            first_dbl_idx = None
            for i_60 in range(max(1, n60 - 20), n60):
                if vols_60[i_60] >= vols_60[i_60-1] * 2 and closes_60[i_60] > closes_60[i_60-1]:
                    recent_range = closes_60[max(0, i_60-20):i_60+1]
                    mid_price = (max(recent_range) + min(recent_range)) / 2
                    if closes_60[i_60] <= mid_price * 1.05:
                        first_dbl_idx = i_60
                        break
            if first_dbl_idx is not None:
                wr3 += 1
                first_low = lows_60[first_dbl_idx]
                first_high = highs_60[first_dbl_idx]
                for j_60 in range(first_dbl_idx + 1, n60):
                    if vols_60[j_60] >= vols_60[j_60-1] * 2:
                        if closes_60[j_60] > first_high: wr3 += 1
                        if lows_60[j_60] >= first_low: wr3 += 1
                        break
                if closes_60[-1] >= first_low: wr3 += 1
        if wr3 > max(item['_wr1'], item['_wr2']):
            new_s6 = int(wr3 / 4 * 15 + 0.5)
            old_s6 = int(max(item['_wr1'], item['_wr2']) / (7 if item['_wr1'] >= item['_wr2'] else 5) * 15 + 0.5)
            item['total'] += min(new_s6, 15) - min(old_s6, 15)
    results.sort(key=lambda x: x['total'], reverse=True)
    return results


# =====================================================================
# 统一回测引擎
# =====================================================================
def calc_future_returns(dates, target_idx, picks, hold_days=[1, 2, 3, 5]):
    results = []
    for pick in picks:
        code = pick['code']
        entry = {}
        if target_idx + 1 < len(dates):
            d_t1 = dates[target_idx + 1]
            df_t1 = load_snapshot(d_t1)
            if df_t1 is not None:
                row_t1 = df_t1[df_t1['ts_code'] == code]
                if not row_t1.empty:
                    entry['buy_date'] = d_t1
                    entry['buy_price'] = float(row_t1.iloc[0]['open'])
        if 'buy_price' not in entry or entry['buy_price'] <= 0:
            entry['buy_price'] = None
            for hd in hold_days:
                entry[f'ret_{hd}d'] = None
            results.append({**pick, **entry})
            continue
        buy_price = entry['buy_price']
        for hd in hold_days:
            sell_idx = target_idx + 1 + hd
            if sell_idx < len(dates):
                d_sell = dates[sell_idx]
                df_sell = load_snapshot(d_sell)
                if df_sell is not None:
                    row_sell = df_sell[df_sell['ts_code'] == code]
                    if not row_sell.empty:
                        sell_price = float(row_sell.iloc[0]['close'])
                        entry[f'ret_{hd}d'] = (sell_price - buy_price) / buy_price * 100
                    else:
                        entry[f'ret_{hd}d'] = None
                else:
                    entry[f'ret_{hd}d'] = None
            else:
                entry[f'ret_{hd}d'] = None
        results.append({**pick, **entry})
    return results


def run_compare_backtest(start_date=None, top_n=20, hold_days=[1, 2, 3, 5]):
    """运行3套评分体系的对比回测"""
    dates = get_available_dates()
    print(f"可用快照: {len(dates)}天 ({dates[0]} ~ {dates[-1]})")

    min_history = 20
    start_idx = min_history
    if start_date:
        if start_date in dates:
            start_idx = max(dates.index(start_date), min_history)
        else:
            for i, d in enumerate(dates):
                if d >= start_date:
                    start_idx = max(i, min_history)
                    break

    max_hold = max(hold_days)
    end_idx = len(dates) - max_hold - 1
    if start_idx > end_idx:
        end_idx = len(dates) - 2
        hold_days = [1]

    backtest_dates = dates[start_idx:end_idx + 1]
    print(f"\n回测区间: {backtest_dates[0]} ~ {backtest_dates[-1]} ({len(backtest_dates)}天)")
    print(f"每天取TOP{top_n}, 持有期: {hold_days}天")
    print(f"买入: T+1开盘价 | 卖出: T+N收盘价")
    print("=" * 120)

    strategies = {
        'A_v1_simple': {'name': 'v1.0简化版(100分)', 'func': score_v1_simple, 'picks': [], 'daily': []},
        'B_v2_full': {'name': 'v2.1完整版(150分)', 'func': score_v2_full, 'picks': [], 'daily': []},
        'C_9skill': {'name': '9Skill v3.3(105分)', 'func': score_9skill, 'picks': [], 'daily': []},
    }

    for target_date in backtest_dates:
        target_idx = dates.index(target_date)
        print(f"\n📅 {target_date}", end="", flush=True)
        t0 = time.time()

        # 构建公共数据（只加载一次）
        history = build_history(dates, target_idx)
        if target_date not in history:
            print(" 无数据，跳过")
            continue

        df_t0 = filter_base(history[target_date])
        cp = build_close_prices(history)
        kline_data = build_kline_data(history, dates, target_idx)
        ind_map = dict(zip(df_t0['ts_code'], df_t0['industry'])) if 'industry' in df_t0.columns else {}
        stats = calc_industry_stats(df_t0, history, dates, target_idx, ind_map)

        common_data = {
            'history': history, 'df_t0': df_t0, 'cp': cp,
            'kline_data': kline_data, 'ind_map': ind_map, 'stats': stats,
        }

        # 对每个策略评分
        for key, strat in strategies.items():
            scored = strat['func'](dates, target_idx, common_data)
            if not scored:
                continue
            top_picks = scored[:top_n]
            picks_with_returns = calc_future_returns(dates, target_idx, top_picks, hold_days)
            valid_picks = [p for p in picks_with_returns if p.get('buy_price') is not None]
            strat['picks'].extend(picks_with_returns)

            if valid_picks:
                rets_1d = [p['ret_1d'] for p in valid_picks if p.get('ret_1d') is not None]
                if rets_1d:
                    avg_ret = np.mean(rets_1d)
                    win_rate = sum(1 for r in rets_1d if r > 0) / len(rets_1d) * 100
                    strat['daily'].append({
                        'date': target_date, 'avg_ret_1d': avg_ret,
                        'win_rate_1d': win_rate, 'n_scored': len(scored),
                        'n_valid': len(valid_picks),
                    })

        elapsed = time.time() - t0
        # 打印当日各策略T+1均收益
        parts = []
        for key, strat in strategies.items():
            if strat['daily'] and strat['daily'][-1]['date'] == target_date:
                ds = strat['daily'][-1]
                parts.append(f"{key[:1]}:{ds['avg_ret_1d']:+.2f}%")
        print(f" | {' '.join(parts)} ({elapsed:.1f}s)", flush=True)

        # 每天处理完后清理不需要的快照缓存（只保留最近25天）
        keep_dates = set(dates[max(0, target_idx - 25):target_idx + max(hold_days) + 2])
        for d in list(_snap_cache.keys()):
            if d not in keep_dates:
                del _snap_cache[d]
        # 限制60分钟缓存大小
        if len(_60m_cache) > 500:
            _60m_cache.clear()

    # 清理缓存
    _snap_cache.clear()
    _60m_cache.clear()

    return strategies, hold_days


def analyze_compare(strategies, hold_days):
    """分析对比结果"""
    print("\n" + "=" * 120)
    print("📊 多策略回测对比结果")
    print("=" * 120)

    # 1. 总体对比表
    print("\n### 1. 总体收益对比（T+1持有期）")
    print(f"{'策略':<25} {'交易日':>6} {'样本数':>6} {'均收益':>8} {'中位数':>8} {'胜率':>8} {'盈亏比':>8} {'累计收益':>10} {'年化':>8} {'最大回撤':>8} {'Sharpe':>8}")
    print("-" * 130)

    summary = {}
    for key, strat in strategies.items():
        picks = strat['picks']
        daily = strat['daily']
        rets_1d = [p['ret_1d'] for p in picks if p.get('ret_1d') is not None]
        if not rets_1d:
            continue

        wins = [r for r in rets_1d if r > 0]
        losses = [r for r in rets_1d if r <= 0]
        avg_ret = np.mean(rets_1d)
        win_rate = len(wins) / len(rets_1d) * 100
        avg_win = np.mean(wins) if wins else 0
        avg_loss = abs(np.mean(losses)) if losses else 0.01
        pr = avg_win / avg_loss if avg_loss > 0 else 99

        # 累计净值
        cum = 1.0; peak = 1.0; max_dd = 0
        daily_rets = []
        for ds in daily:
            r = ds['avg_ret_1d']
            cum *= (1 + r / 100)
            daily_rets.append(r)
            if cum > peak: peak = cum
            dd = (peak - cum) / peak * 100
            if dd > max_dd: max_dd = dd

        cum_ret = (cum - 1) * 100
        n_days = len(daily)
        annual = ((cum ** (250 / n_days)) - 1) * 100 if n_days > 0 else 0
        sharpe = np.mean(daily_rets) / np.std(daily_rets) * np.sqrt(250) if np.std(daily_rets) > 0 else 0

        tag = '🏆' if cum_ret == max(
            (sum(1 + d['avg_ret_1d']/100 for d in s['daily']) if s['daily'] else 0)
            for s in strategies.values()
        ) else '  '

        print(f"  {strat['name']:<23} {n_days:>5} {len(rets_1d):>5}  {avg_ret:>+7.3f}%  {np.median(rets_1d):>+7.3f}%  "
              f"{win_rate:>6.1f}%  {pr:>7.2f}  {cum_ret:>+9.2f}%  {annual:>+7.1f}%  -{max_dd:>6.2f}%  {sharpe:>7.2f}")

        summary[key] = {
            'name': strat['name'], 'n_days': n_days, 'n_trades': len(rets_1d),
            'avg_ret': avg_ret, 'win_rate': win_rate, 'profit_ratio': pr,
            'cum_return': cum_ret, 'annual_return': annual, 'max_dd': max_dd,
            'sharpe': sharpe, 'daily_rets': daily_rets,
        }

    # 2. 各持有期对比
    print("\n### 2. 各持有期对比")
    for hd in hold_days:
        key_name = f'ret_{hd}d'
        print(f"\n  --- T+{hd}日 ---")
        print(f"  {'策略':<25} {'样本':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8}")
        print(f"  {'-'*60}")
        for skey, strat in strategies.items():
            rets = [p[key_name] for p in strat['picks'] if p.get(key_name) is not None]
            if not rets: continue
            w = [r for r in rets if r > 0]
            l = [r for r in rets if r <= 0]
            wr = len(w) / len(rets) * 100
            avg_w = np.mean(w) if w else 0
            avg_l = abs(np.mean(l)) if l else 0.01
            pr = avg_w / avg_l if avg_l > 0 else 99
            print(f"  {strat['name']:<25} {len(rets):>5}  {np.mean(rets):>+7.3f}%  {wr:>6.1f}%  {pr:>7.2f}")

    # 3. 选股重叠度分析
    print("\n### 3. 选股重叠度分析（每日TOP20中重叠的股票比例）")
    all_dates = set()
    for strat in strategies.values():
        for ds in strat['daily']:
            all_dates.add(ds['date'])

    overlap_stats = defaultdict(list)
    for date in sorted(all_dates):
        picks_by_strat = {}
        for skey, strat in strategies.items():
            day_picks = [p['code'] for p in strat['picks'] if p['date'] == date]
            picks_by_strat[skey] = set(day_picks[:20])

        keys = list(picks_by_strat.keys())
        for i in range(len(keys)):
            for j in range(i+1, len(keys)):
                s1, s2 = picks_by_strat[keys[i]], picks_by_strat[keys[j]]
                if s1 and s2:
                    overlap = len(s1 & s2) / max(len(s1 | s2), 1) * 100
                    overlap_stats[f"{keys[i]} vs {keys[j]}"].append(overlap)

    for pair, overlaps in overlap_stats.items():
        print(f"  {pair}: 平均重叠度 {np.mean(overlaps):.1f}% (最高{max(overlaps):.0f}%, 最低{min(overlaps):.0f}%)")

    # 4. 月度对比
    print("\n### 4. 月度收益对比")
    monthly = {}
    for skey, strat in strategies.items():
        monthly[skey] = defaultdict(list)
        for ds in strat['daily']:
            ym = ds['date'][:6]
            monthly[skey][ym].append(ds['avg_ret_1d'])

    all_months = sorted(set(m for mm in monthly.values() for m in mm.keys()))
    header = f"  {'月份':<8}"
    for skey, strat in strategies.items():
        header += f" {strat['name'][:12]:>14}"
    header += f" {'最优':>10}"
    print(header)
    print(f"  {'-'*70}")

    for ym in all_months:
        line = f"  {ym:<8}"
        best_val = -999
        best_name = ''
        for skey, strat in strategies.items():
            rets = monthly[skey].get(ym, [])
            if rets:
                cum = 1.0
                for r in rets:
                    cum *= (1 + r / 100)
                m_ret = (cum - 1) * 100
                line += f" {m_ret:>+13.2f}%"
                if m_ret > best_val:
                    best_val = m_ret
                    best_name = skey[:1]
            else:
                line += f" {'N/A':>14}"
        line += f" {best_name:>10}"
        print(line)

    # 5. 最终结论
    print("\n" + "=" * 120)
    print("🏆 最终结论")
    print("=" * 120)

    if summary:
        best_cum = max(summary.values(), key=lambda x: x['cum_return'])
        best_sharpe = max(summary.values(), key=lambda x: x['sharpe'])
        best_wr = max(summary.values(), key=lambda x: x['win_rate'])

        print(f"\n  📈 累计收益最高: {best_cum['name']} → {best_cum['cum_return']:+.2f}% (年化{best_cum['annual_return']:+.1f}%)")
        print(f"  📊 Sharpe最高:   {best_sharpe['name']} → {best_sharpe['sharpe']:.2f}")
        print(f"  🎯 胜率最高:     {best_wr['name']} → {best_wr['win_rate']:.1f}%")

        # 综合评分
        print(f"\n  📋 综合评分（累计收益40% + Sharpe30% + 胜率20% + 回撤10%）:")
        for skey, s in summary.items():
            # 归一化
            cum_norm = (s['cum_return'] - min(x['cum_return'] for x in summary.values())) / max(max(x['cum_return'] for x in summary.values()) - min(x['cum_return'] for x in summary.values()), 0.01) * 100
            sharpe_norm = (s['sharpe'] - min(x['sharpe'] for x in summary.values())) / max(max(x['sharpe'] for x in summary.values()) - min(x['sharpe'] for x in summary.values()), 0.01) * 100
            wr_norm = (s['win_rate'] - min(x['win_rate'] for x in summary.values())) / max(max(x['win_rate'] for x in summary.values()) - min(x['win_rate'] for x in summary.values()), 0.01) * 100
            dd_norm = (max(x['max_dd'] for x in summary.values()) - s['max_dd']) / max(max(x['max_dd'] for x in summary.values()) - min(x['max_dd'] for x in summary.values()), 0.01) * 100
            composite = cum_norm * 0.4 + sharpe_norm * 0.3 + wr_norm * 0.2 + dd_norm * 0.1
            print(f"     {s['name']:<25} 综合分: {composite:.1f}/100")

    return summary


def generate_compare_html(strategies, hold_days, summary):
    """生成HTML对比报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(RESULTS_DIR, f"compare_report_{timestamp}.html")

    # 净值曲线数据
    nav_data = {}
    for skey, strat in strategies.items():
        cum = 1.0
        dates_list = []
        nav_list = []
        for ds in strat['daily']:
            cum *= (1 + ds['avg_ret_1d'] / 100)
            dates_list.append(ds['date'])
            nav_list.append(round(cum, 4))
        nav_data[skey] = {'dates': dates_list, 'navs': nav_list, 'name': strat['name']}

    # 所有日期
    all_dates = sorted(set(d for nd in nav_data.values() for d in nd['dates']))

    colors = {'A_v1_simple': '#63b3ed', 'B_v2_full': '#48bb78', 'C_9skill': '#ecc94b'}
    series_js = []
    for skey, nd in nav_data.items():
        series_js.append(f"""{{
            name: '{nd["name"]}',
            type: 'line', smooth: true,
            data: {json.dumps(nd['navs'])},
            lineStyle: {{ color: '{colors.get(skey, "#fff")}', width: 2 }},
        }}""")

    # 月度数据
    monthly_data = {}
    for skey, strat in strategies.items():
        monthly_data[skey] = defaultdict(list)
        for ds in strat['daily']:
            ym = ds['date'][:6]
            monthly_data[skey][ym].append(ds['avg_ret_1d'])

    all_months = sorted(set(m for mm in monthly_data.values() for m in mm.keys()))
    bar_series = []
    for skey, strat in strategies.items():
        data = []
        for ym in all_months:
            rets = monthly_data[skey].get(ym, [])
            if rets:
                cum = 1.0
                for r in rets:
                    cum *= (1 + r / 100)
                data.append(round((cum - 1) * 100, 2))
            else:
                data.append(0)
        bar_series.append(f"""{{
            name: '{strat["name"]}',
            type: 'bar',
            data: {json.dumps(data)},
            itemStyle: {{ color: '{colors.get(skey, "#fff")}' }},
        }}""")

    # 核心指标卡片
    cards_html = ""
    for skey, s in summary.items():
        color = colors.get(skey, '#fff')
        cards_html += f"""
        <div class="card" style="border-left: 4px solid {color};">
          <div class="label">{s['name']}</div>
          <div class="value {'green' if s['cum_return'] > 0 else 'red'}">{s['cum_return']:+.2f}%</div>
          <div style="font-size:12px;color:#718096;margin-top:8px;">
            年化{s['annual_return']:+.1f}% | 胜率{s['win_rate']:.1f}% | 盈亏比{s['profit_ratio']:.2f} | Sharpe{s['sharpe']:.2f} | 回撤-{s['max_dd']:.1f}%
          </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>多策略回测对比报告</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0f1923; color: #e0e0e0; }}
.header {{ background: linear-gradient(135deg, #1a2332, #2d3748); padding: 30px 40px; border-bottom: 2px solid #e53e3e; }}
.header h1 {{ font-size: 28px; color: #fff; }}
.header .subtitle {{ color: #a0aec0; font-size: 14px; margin-top: 8px; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
.dashboard {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin: 20px 0; }}
.card {{ background: #1a2332; border-radius: 12px; padding: 20px; border: 1px solid #2d3748; }}
.card .label {{ font-size: 14px; color: #a0aec0; font-weight: 600; }}
.card .value {{ font-size: 28px; font-weight: 700; margin-top: 8px; }}
.card .value.green {{ color: #48bb78; }}
.card .value.red {{ color: #fc8181; }}
.chart-container {{ background: #1a2332; border-radius: 12px; padding: 20px; margin: 20px 0; border: 1px solid #2d3748; }}
.chart-title {{ font-size: 18px; font-weight: 600; margin-bottom: 16px; color: #fff; }}
.chart {{ width: 100%; height: 450px; }}
.footer {{ text-align: center; padding: 30px; color: #4a5568; font-size: 12px; }}
</style>
</head>
<body>
<div class="header">
  <h1>📊 多策略回测对比报告</h1>
  <div class="subtitle">3套评分体系横向PK | 回测区间: {all_dates[0] if all_dates else 'N/A'} ~ {all_dates[-1] if all_dates else 'N/A'} | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
</div>
<div class="container">
  <div class="dashboard">{cards_html}</div>
  <div class="chart-container">
    <div class="chart-title">📈 累计净值曲线对比</div>
    <div id="navChart" class="chart"></div>
  </div>
  <div class="chart-container">
    <div class="chart-title">📊 月度收益对比</div>
    <div id="monthlyChart" class="chart"></div>
  </div>
</div>
<div class="footer">多策略回测对比报告 | v1.0简化版 vs v2.1完整版 vs 9Skill v3.3 | 仅供研究参考</div>
<script>
var navChart = echarts.init(document.getElementById('navChart'));
navChart.setOption({{
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: {json.dumps([nd['name'] for nd in nav_data.values()])}, textStyle: {{ color: '#a0aec0' }}, top: 0 }},
  grid: {{ left: 60, right: 30, top: 40, bottom: 40 }},
  xAxis: {{ type: 'category', data: {json.dumps(all_dates)}, axisLabel: {{ color: '#718096', rotate: 45 }} }},
  yAxis: {{ type: 'value', axisLabel: {{ color: '#718096' }}, splitLine: {{ lineStyle: {{ color: '#2d3748' }} }} }},
  series: [{','.join(series_js)}]
}});
var monthlyChart = echarts.init(document.getElementById('monthlyChart'));
monthlyChart.setOption({{
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: {json.dumps([s['name'] for s in strategies.values()])}, textStyle: {{ color: '#a0aec0' }}, top: 0 }},
  grid: {{ left: 60, right: 30, top: 40, bottom: 40 }},
  xAxis: {{ type: 'category', data: {json.dumps(all_months)}, axisLabel: {{ color: '#718096' }} }},
  yAxis: {{ type: 'value', axisLabel: {{ color: '#718096', formatter: '{{value}}%' }}, splitLine: {{ lineStyle: {{ color: '#2d3748' }} }} }},
  series: [{','.join(bar_series)}]
}});
window.addEventListener('resize', function() {{ navChart.resize(); monthlyChart.resize(); }});
</script>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n📊 HTML对比报告: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description='多策略回测对比 — 3套评分体系横向PK')
    parser.add_argument('--start', type=str, default=None, help='回测起始日期(YYYYMMDD)')
    parser.add_argument('--top', type=int, default=20, help='每天取TOP N只(默认20)')
    parser.add_argument('--hold', type=str, default='1,2,3,5', help='持有天数')
    parser.add_argument('--report', action='store_true', help='生成HTML对比报告')
    parser.add_argument('--save', action='store_true', help='保存CSV')
    args = parser.parse_args()

    hold_days = [int(x) for x in args.hold.split(',')]

    print("=" * 120)
    print("📈 多策略回测对比 — 3套评分体系横向PK")
    print("=" * 120)
    print(f"策略A: v1.0简化版(100分) — 无BCI/无WR/无60分钟K线")
    print(f"策略B: v2.1完整版(150分) — 含BCI+WR+60分钟K线+真实量比")
    print(f"策略C: 9Skill v3.3(105分) — 9维度+TXCG加分")
    print(f"参数: TOP{args.top} | 持有期{hold_days}天 | 起始{args.start or '自动'}")

    strategies, hold_days = run_compare_backtest(
        start_date=args.start, top_n=args.top, hold_days=hold_days,
    )

    summary = analyze_compare(strategies, hold_days)

    if args.report and summary:
        report_path = generate_compare_html(strategies, hold_days, summary)
        if report_path:
            webbrowser.open(f'file://{os.path.abspath(report_path)}')

    if args.save:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for skey, strat in strategies.items():
            if strat['picks']:
                df = pd.DataFrame(strat['picks'])
                path = os.path.join(RESULTS_DIR, f"compare_{skey}_{timestamp}.csv")
                df.to_csv(path, index=False, encoding='utf-8-sig')
                print(f"💾 {strat['name']}: {path}")


if __name__ == '__main__':
    main()
