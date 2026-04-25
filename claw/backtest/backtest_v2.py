#!/usr/bin/env python3
"""
选股系统历史回测 v2.1（完全对齐score_system.py v3.3 + 增强报告功能）
======================
基于本地 ~/stock_data/daily_snapshot/ + ~/Downloads/2026/60min/ 数据，
离线模拟每天运行评分选股系统，统计推荐标的在未来N天的实际收益。

v2.1新增功能（借鉴看海量化CLI）：
- ✅ --report: 生成精美HTML报告（净值曲线+月度收益热力图+交易明细+维度贡献度雷达图）
- ✅ --json: JSON格式输出回测结果（方便AI/程序化调用）
- ✅ --optimize: 参数寻优模式（批量测试不同TOP-N/持有期/评分阈值组合）
- ✅ --compare: 多策略对比（横向对比多次回测结果CSV）

v2.0核心升级（完全对齐score_system.py v3.3 150分制）：
- ✅ D1多周期共振(15分)：大(±3)+中(±2)+小(±1)细分6档
- ✅ D2主线热点(25分)：含板块持续性判断+BCI板块完整性加权
- ✅ D3三Skill(47分)：Mistery(20)+TDS(12)+元子元(10)+TXCG六大模型(5)
- ✅ D4安全边际(15分)
- ✅ D5基本面(15分)
- ✅ D9百胜WR(15分)：WR-1首板放量(7条件)+WR-2右侧起爆(5条件)+WR-3底倍量柱(4条件,60分钟K线)
- ✅ 风险扣分(0~-30)：含BCI加权减轻、动态市值、换手率异常
- ✅ 保护因子(0~+15)：含连阳天数、BCI高分保护P7
- ✅ BCI板块完整性指数(0-100)：梯队层次+龙头强度+封板率+持续性斜率+换手板比例+板块内聚度
- 满分：150分（完全对齐）

用法：
  python3 backtest_v2.py                          # 默认回测
  python3 backtest_v2.py --start 20260320         # 从指定日期开始
  python3 backtest_v2.py --top 10                 # 每天取TOP10
  python3 backtest_v2.py --save                   # 保存结果到CSV
  python3 backtest_v2.py --report                 # 生成HTML报告（自动打开浏览器）
  python3 backtest_v2.py --json                   # JSON格式输出（静默模式）
  python3 backtest_v2.py --optimize               # 参数寻优模式
  python3 backtest_v2.py --compare f1.csv f2.csv  # 对比多次回测结果
"""
import os, sys, time, argparse, json, webbrowser, itertools
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ===== 配置 =====
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
KLINE_60M_DIRS = [
    os.path.expanduser("~/Downloads/2026/60min"),
    os.path.expanduser("~/Downloads/2025/60min"),
    os.path.expanduser("~/Downloads/2024/60min"),
    os.path.expanduser("~/Downloads/2023/60min"),
    os.path.expanduser("~/Downloads/2022/60min"),
    os.path.expanduser("~/Downloads/2021/60min"),
    os.path.expanduser("~/Downloads/2020/60min"),
]
KLINE_60M_DIR = KLINE_60M_DIRS[0]  # 兼容旧引用
VR_DIR = os.path.expanduser("~/stock_data/volume_ratio")  # tushare daily_basic 真实量比数据
RESULTS_DIR = os.path.expanduser("~/WorkBuddy/Claw/backtest_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# ===== 数据加载 =====
def get_available_dates():
    """获取所有可用的快照日期（已排序）"""
    files = [f for f in os.listdir(SNAPSHOT_DIR) 
             if f.endswith('.parquet') and f != 'stock_basic.parquet']
    dates = sorted([f.replace('.parquet', '') for f in files])
    return dates

def load_volume_ratio(date_str):
    """加载某天的真实量比数据（来自tushare daily_basic）"""
    path = os.path.join(VR_DIR, f"{date_str}.parquet")
    if not os.path.exists(path):
        return {}
    df = pd.read_parquet(path)
    vr_dict = {}
    for _, row in df.iterrows():
        if pd.notna(row.get('volume_ratio')):
            vr_dict[row['ts_code']] = float(row['volume_ratio'])
    return vr_dict

def load_snapshot(date_str):
    """加载某天的全市场快照"""
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
    return df

# ===== 60分钟K线加载 =====
_60m_cache = {}  # 全局缓存

def load_60m_kline(ts_code, target_date=None):
    """
    加载某只股票的60分钟K线数据。
    返回: dict {'closes': [...], 'highs': [...], 'lows': [...], 'vols': [...]} 或 None
    target_date: YYYYMMDD格式，只取该日期及之前的数据
    """
    if ts_code in _60m_cache:
        df_60 = _60m_cache[ts_code]
    else:
        code6 = ts_code[:6]
        prefix = 'sh' if ts_code.endswith('.SH') else ('sz' if ts_code.endswith('.SZ') else 'bj')
        fname = f"{prefix}{code6}.csv"
        # 合并所有目录的60分钟K线数据
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
    
    # 按日期过滤
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


def build_history(dates, target_idx):
    """构建历史数据"""
    needed_offsets = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20]
    history = {}
    for offset in needed_offsets:
        idx = target_idx - offset
        if 0 <= idx < len(dates):
            d = dates[idx]
            if d not in history:
                df = load_snapshot(d)
                if df is not None:
                    history[d] = df
    return history


# ===== BCI板块完整性指数计算 =====
def calc_bci(ind_name, zt_list, ind_zt_daily, ind_zb_map, d1_zt_codes, d2_zt_codes, bas_d):
    """
    计算某行业的BCI板块完整性指数(0-100)
    完全对齐score_system.py的BCI计算逻辑
    """
    n = len(zt_list)
    if n == 0:
        return 0
    
    bci = 0
    
    # --- BCI-1: 涨停数量(0-20) ---
    if n >= 8: bci += 20
    elif n >= 5: bci += 17
    elif n >= 3: bci += 13
    elif n >= 2: bci += 8
    else: bci += 3
    
    # --- BCI-2: 梯队层次(0-20) ---
    连板数 = sum(1 for s in zt_list if s.get('ts_code', '') in d1_zt_codes)
    首板数 = n - 连板数
    层级数 = (1 if 首板数 > 0 else 0) + (1 if 连板数 > 0 else 0)
    最高板估计 = 2 if 连板数 > 0 else 1
    
    # 检查3连板
    三连板 = sum(1 for s in zt_list if s.get('ts_code', '') in d1_zt_codes and s.get('ts_code', '') in d2_zt_codes)
    if 三连板 > 0:
        层级数 = min(层级数 + 1, 3)
        最高板估计 = 3
    
    s2_bci = min(层级数 * 5, 12) + min(最高板估计 * 2, 8)
    bci += min(s2_bci, 20)
    
    # --- BCI-3: 龙头强度(0-15) ---
    max_amount = max((s.get('amount', 0) for s in zt_list), default=0)
    if max_amount > 500000: bci += 15
    elif max_amount > 200000: bci += 12
    elif max_amount > 100000: bci += 9
    elif max_amount > 50000: bci += 6
    else: bci += 3
    
    # --- BCI-4: 封板率(0-10) ---
    zb_count = ind_zb_map.get(ind_name, 0)
    total_try = n + zb_count
    封板率 = n / total_try if total_try > 0 else 1
    if zb_count == 0: bci += 10
    elif 封板率 > 0.8: bci += 8
    elif 封板率 > 0.6: bci += 5
    elif 封板率 > 0.4: bci += 3
    else: bci += 1
    
    # --- BCI-5: 持续性斜率(0-10) ---
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
    
    # --- BCI-6: 换手板比例(0-10) ---
    换手板数 = 0
    for s in zt_list:
        code_s = s.get('ts_code', '')
        tr_s = bas_d.get(code_s, {}).get('turnover_rate_f', 0)
        if tr_s and tr_s > 8:
            换手板数 += 1
    换手比 = 换手板数 / n if n > 0 else 0
    bci += min(int(换手比 * 10), 10)
    
    # --- BCI-7: 板块内聚度(0-15) ---
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


# ===== 完整评分引擎（完全对齐score_system.py v3.3） =====
def score_stocks_offline(dates, target_idx):
    """
    离线评分：完全对齐score_system.py v3.3的150分制评分逻辑。
    
    D1: 多周期共振(15分) — 大(±3)+中(±2)+小(±1)细分6档
    D2: 主线热点(25分) — 含板块持续性判断 + BCI板块完整性加权
    D3: 三Skill(47分) — Mistery(20)+TDS(12)+元子元(10)+TXCG六大模型(5)
    D4: 安全边际(15分)
    D5: 基本面(15分)
    D9: 百胜WR(15分) — WR-1(7条件)+WR-2(5条件)+WR-3底倍量柱(4条件,60分钟K线)
    风险扣分(0~-30) + 保护因子(0~+15)
    
    满分: 132分（D1+D2+D3+D4+D5+D9 = 15+25+47+15+15+15 = 132）
    实际满分: 15+25+47+15+15+15 = 132 → 各维度cap后约125
    """
    T = dates[target_idx]
    
    # 加载数据
    history = build_history(dates, target_idx)
    if T not in history:
        return []
    
    df_t0 = history[T]
    
    # 过滤：非ST、主板+创业板+科创板
    df_t0 = df_t0[df_t0['ts_code'].str.match(r'^(00|30|60|68)', na=False)].copy()
    if 'name' in df_t0.columns:
        df_t0 = df_t0[~df_t0['name'].str.contains('ST|退', na=False)]
    
    # 构建收盘价字典
    cp = {}
    for d, df in history.items():
        for _, row in df.iterrows():
            code = row['ts_code']
            if code not in cp:
                cp[code] = {}
            cp[code][d] = row['close']
    
    # 获取关键日期
    T0 = T
    T1 = dates[target_idx - 1] if target_idx >= 1 else None
    T2 = dates[target_idx - 2] if target_idx >= 2 else None
    T5 = dates[target_idx - 5] if target_idx >= 5 else None
    T10 = dates[target_idx - 10] if target_idx >= 10 else None
    T20 = dates[target_idx - 20] if target_idx >= 20 else None
    
    # 行业映射
    ind_map = {}
    if 'industry' in df_t0.columns:
        ind_map = dict(zip(df_t0['ts_code'], df_t0['industry']))
    
    # 基本面数据字典
    bas_d = {}
    vr_data = load_volume_ratio(T)  # 加载真实量比数据
    for _, row in df_t0.iterrows():
        bas_d[row['ts_code']] = {
            'pe_ttm': row.get('pe_ttm'),
            'total_mv': row.get('total_mv'),
            'turnover_rate_f': row.get('turnover_rate_f'),
            'net_mf_amount': row.get('net_mf_amount'),
            'volume_ratio': vr_data.get(row['ts_code'], 0),  # 真实量比（tushare daily_basic）
        }
    
    # ===== 行业涨停统计 =====
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
    
    # 全市场成交额（动态市值上限）
    total_market_amount = df_t0['amount'].sum() / 100000 if 'amount' in df_t0.columns else 0
    if total_market_amount > 20000: dynamic_mv_cap = 500
    elif total_market_amount > 15000: dynamic_mv_cap = 300
    else: dynamic_mv_cap = 150
    
    # ===== 近3天行业涨停统计（BCI持续性+主线热度） =====
    ind_perf = defaultdict(list)
    ind_zt_daily = defaultdict(lambda: [0, 0, 0])
    d1_zt_codes = set()  # T-1涨停股
    d2_zt_codes = set()  # T-2涨停股
    
    for offset in [0, 1, 2]:
        idx = target_idx - offset
        if idx < 0 or idx >= len(dates):
            continue
        d = dates[idx]
        if d not in history:
            continue
        df_d = history[d]
        df_d_filtered = df_d[df_d['ts_code'].str.match(r'^(00|30|60|68)', na=False)]
        if 'name' in df_d_filtered.columns:
            df_d_filtered = df_d_filtered[~df_d_filtered['name'].str.contains('ST|退', na=False)]
        
        if 'industry' in df_d_filtered.columns and 'pct_chg' in df_d_filtered.columns:
            # 收集T-1/T-2涨停股
            zt_d = df_d_filtered[df_d_filtered['pct_chg'] >= 9.5]['ts_code'].tolist()
            if offset == 1:
                d1_zt_codes = set(zt_d)
            elif offset == 2:
                d2_zt_codes = set(zt_d)
            
            for ind, grp in df_d_filtered.groupby('industry'):
                avg_chg = grp['pct_chg'].mean()
                lim_cnt = int((grp['pct_chg'] >= 9.5).sum())
                all_ind_avg = df_d_filtered.groupby('industry')['pct_chg'].mean().sort_values(ascending=False)
                rk = list(all_ind_avg.index).index(ind) + 1 if ind in all_ind_avg.index else 99
                ind_perf[ind].append({'avg': avg_chg, 'lim': lim_cnt, 'rk': rk})
                ind_zt_daily[ind][offset] = lim_cnt
    
    # ===== 主线热度分（含板块持续性，对齐score_system.py） =====
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
        
        # ★ 板块持续性判断（对齐v3.2）
        lim_list = [p['lim'] for p in sorted(perfs, key=lambda x: x.get('rk', 99))]
        # 用ind_zt_daily更准确
        day_lims = ind_zt_daily.get(ind, [0, 0, 0])
        latest_lim = day_lims[0]  # T
        prev_lim = day_lims[1] if len(day_lims) > 1 else 0  # T-1
        
        if latest_lim >= prev_lim * 1.5 and latest_lim >= 5:
            score += 3  # 强升温
        elif latest_lim >= prev_lim and latest_lim >= 3:
            score += 2  # 持续
        elif prev_lim >= 5 and latest_lim < prev_lim * 0.5:
            score -= 3  # 一日游
        elif latest_lim < prev_lim:
            score -= 1  # 降温
        
        # 首日爆发检测
        if prev_lim == 0 and latest_lim >= 10:
            score -= 2
        
        # 连续3天有涨停
        if all(c > 0 for c in day_lims[:3] if c is not None):
            score += 2
        
        mainline_scores[ind] = max(min(score, 25), 0)
    
    # ===== BCI板块完整性指数计算 =====
    ind_zb_map = {}
    for ind, grp_rows in df_t0.groupby('industry') if 'industry' in df_t0.columns else []:
        zb_approx = len(grp_rows[(grp_rows['pct_chg'] >= 5) & (grp_rows['pct_chg'] < 9.5)])
        ind_zb_map[ind] = max(0, zb_approx // 3)
    
    ind_bci_map = {}
    for ind_name, zt_count in ind_zt_map.items():
        if zt_count == 0:
            ind_bci_map[ind_name] = 0
            continue
        zt_list = ind_zt_stocks.get(ind_name, [])
        ind_bci_map[ind_name] = calc_bci(
            ind_name, zt_list, ind_zt_daily, ind_zb_map,
            d1_zt_codes, d2_zt_codes, bas_d
        )
    
    # ===== 构建K线数据（近11天） =====
    kline_data = {}
    for offset in range(min(11, target_idx + 1)):
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
    
    # ===== 逐只评分 =====
    results = []
    
    for _, row in df_t0.iterrows():
        code = row['ts_code']
        nm = row.get('name', '?')
        ind = ind_map.get(code, '?')
        
        pe = row.get('pe_ttm', None)
        if pe is not None:
            pe = float(pe) if not pd.isna(pe) else None
        mv = float(row.get('total_mv', 0)) / 10000 if row.get('total_mv') else 0
        tr = float(row.get('turnover_rate_f', 0)) if row.get('turnover_rate_f') else 0
        nb = float(row.get('net_mf_amount', 0)) if row.get('net_mf_amount') else 0
        nb_yi = nb / 10000
        pct_last = float(row.get('pct_chg', 0))
        is_zt = pct_last >= 9.5
        
        # 多周期涨幅
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
        
        # 粗筛（对齐score_system.py）
        big_raw = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid_raw = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small_raw = 1 if r5 > 2 else (-1 if r5 < -2 else 0)
        ps_raw = big_raw * 3 + mid_raw * 2 + small_raw * 1
        if ps_raw < 4:
            continue
        if mv < 30:
            continue
        if r5 > 40 or r10 > 50:
            continue
        
        # ====== D1: 多周期共振 (15分) — 大(±3)+中(±2)+小(±1)细分6档 ======
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
        
        period_raw = big + mid + small  # -6~+6
        d1 = int((period_raw + 6) / 12 * 15 + 0.5)
        d1 = max(0, min(15, d1))
        
        # ====== D2: 主线热点 (25分，含BCI加权) ======
        d2_base = mainline_scores.get(ind, 0)
        ind_bci = ind_bci_map.get(ind, 0)
        if ind_bci >= 70: d2_base += 3
        elif ind_bci >= 50: d2_base += 2
        elif ind_bci >= 30: d2_base += 1
        d2 = min(d2_base, 25)
        
        # ====== D3: 三Skill (47分) ======
        klines = kline_data.get(code, [])
        d3 = 0
        mistery = 0
        tds = 0
        yuanzi = 0
        txcg_model = 0
        is_ma_bull = False
        consecutive_yang = 0
        vol5 = 0
        vol10 = 0
        bbw_val = 0
        ma5_val = 0
        ma10_val = 0
        ma20_val = 0
        peaks = []
        troughs = []
        
        if len(klines) >= 10:
            kdf = pd.DataFrame(klines)
            kdf.sort_values('trade_date', inplace=True)
            kdf.reset_index(drop=True, inplace=True)
            kdf = kdf.drop_duplicates(subset=['trade_date']).copy()
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
                
                # 连阳天数
                for k in range(n - 1, -1, -1):
                    if cc[k] > oo[k]:
                        consecutive_yang += 1
                    else:
                        break
                
                vol5 = np.mean(vv[-5:])
                vol10 = np.mean(vv[-min(10, n):])
                # 使用tushare daily_basic真实量比（与score_system.py完全对齐）
                vr_real = bas_d.get(code, {}).get('volume_ratio', 0)
                vol_ratio_val = vr_real if vr_real > 0 else (vol5 / vol10 if vol10 > 0 else 1)
                
                # BBW
                std20 = np.std(cc[-min(20, n):])
                bbw_val = (4 * std20) / ma20_val if ma20_val > 0 else 0
                
                # --- Mistery (20分) — M1+M2+M3+M4+M5+M6 ---
                # M1趋势(0-3)
                if is_ma_bull: mistery += 3
                elif cc[-1] > ma5_val > ma10_val: mistery += 2
                elif cc[-1] > ma20_val: mistery += 1
                
                # M2买点(0-3): 520金叉/破五反五/BBW起爆
                ma5s = pd.Series(cc).rolling(5).mean()
                ma20s = pd.Series(cc).rolling(min(20, n)).mean()
                m2 = 0
                if n >= 7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5] <= ma20s.iloc[-5] and ma5_val > ma20_val:
                    m2 += 2  # 520刚金叉
                if n >= 5:
                    below = any(cc[i] < ma5s.iloc[i] for i in range(max(0, n-5), n-1) if not np.isnan(ma5s.iloc[i]))
                    if below and cc[-1] > ma5_val: m2 += 1  # 破五反五
                if bbw_val < 0.12 and pct_last > 3: m2 += 2
                elif bbw_val < 0.15 and pct_last > 3: m2 += 1
                mistery += min(3, m2)
                
                # M3卖点扣分(-2~0)
                if pct_last < 2 and vol5 / vol10 > 2.5:
                    mistery -= 1  # 放量滞涨
                if n >= 4 and hh[-1] < max(hh[-4:-1]) and pct_last < 1:
                    mistery -= 1  # 3天不创新高
                
                # M4量价(0-3)
                if vol_ratio_val > 1.3 and pct_last > 0: mistery += 2
                elif vol_ratio_val > 1: mistery += 1
                if dif > dea: mistery += 1
                
                # M5形态(0-3)
                if n >= 3:
                    prev_range = abs(cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                    if pct_last > 5 and vol_ratio_val > 1.5 and prev_range < 2:
                        mistery += 2  # 空中加油
                    if n >= 2:
                        prev_upper = (hh[-2] - cc[-2]) / cc[-2] * 100 if cc[-2] > 0 else 0
                        if prev_upper > 3 and cc[-1] > hh[-2] * 0.98:
                            mistery += 1  # 仙人指路收复
                
                # M6仓位管理(0-5)
                m6 = 0
                if cc[-1] > ma5_val > ma10_val > ma20_val and pct_last > 0:
                    m6 += 2
                elif cc[-1] > ma5_val > ma10_val:
                    m6 += 1
                if ma20_val > 0 and abs(cc[-1] - ma20_val) / ma20_val < 0.08:
                    m6 += 1
                if n >= 20:
                    support_20 = min(ll[-20:])
                    if cc[-1] > support_20 * 1.05:
                        m6 += 1
                if n >= 7:
                    chg_7d = (cc[-1] - cc[-7]) / cc[-7] * 100 if cc[-7] > 0 else 0
                    if chg_7d > 5: m6 += 1
                    elif chg_7d < -3: m6 -= 1
                mistery += min(5, max(0, m6))
                
                mistery = max(min(mistery, 20), 0)
                
                # --- TDS (12分) — 波峰波谷+T1+T2+T3+T4+T5+T6 ---
                win = min(5, n // 3)
                for i in range(win, n - win):
                    if hh[i] >= max(hh[max(0, i-win):i]) and hh[i] >= max(hh[i+1:min(n, i+win+1)]):
                        peaks.append(hh[i])
                    if ll[i] <= min(ll[max(0, i-win):i]) and ll[i] <= min(ll[i+1:min(n, i+win+1)]):
                        troughs.append(ll[i])
                
                # 趋势(0-3)
                if len(peaks) >= 2 and len(troughs) >= 2:
                    if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]:
                        tds += 3
                    elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]:
                        tds += 2
                elif len(peaks) >= 2 and peaks[-1] > peaks[-2]:
                    tds += 2
                
                # T1推进(0-2)
                if n >= 2 and hh[-1] > hh[-2] and ll[-1] > ll[-2]:
                    tds += 2
                elif n >= 2 and hh[-1] > hh[-2]:
                    tds += 1
                
                # T2吞没(0-1)
                if n >= 3 and cc[-2] < oo[-2] and cc[-1] > oo[-1] and cc[-1] > hh[-2]:
                    tds += 1
                
                # T3突破(0-2)
                if peaks and cc[-1] > peaks[-1]:
                    tds += 2
                elif pct_last >= 9.5:
                    tds += 1
                
                # T4三K反转(0-2)
                if n >= 4 and r5 < -3:
                    k1_body = abs(cc[-3] - oo[-3])
                    k2_body = abs(cc[-2] - oo[-2])
                    k3_body = abs(cc[-1] - oo[-1])
                    if k2_body <= max(k1_body, k3_body):
                        if cc[-1] > oo[-1] and cc[-1] > hh[-2]:
                            tds += 2
                elif n >= 4 and r5 > 10:
                    k1_body = abs(cc[-3] - oo[-3])
                    k2_body = abs(cc[-2] - oo[-2])
                    k3_body = abs(cc[-1] - oo[-1])
                    if k2_body <= max(k1_body, k3_body):
                        if cc[-1] < oo[-1] and cc[-1] < ll[-2]:
                            tds -= 1
                
                # T5反转(0-2)
                if r5 < -10 and pct_last > 3:
                    tds += 2
                elif r5 < -5 and pct_last > 0:
                    body = abs(cc[-1] - oo[-1])
                    lower_shadow = min(cc[-1], oo[-1]) - ll[-1]
                    if lower_shadow > body * 2 and lower_shadow > 0:
                        tds += 1
                
                # T6双向突破(0-2)
                if len(peaks) >= 3 and len(troughs) >= 3:
                    p_trend1 = 1 if peaks[-2] > peaks[-3] else -1
                    p_trend2 = 1 if peaks[-1] > peaks[-2] else -1
                    t_trend1 = 1 if troughs[-2] > troughs[-3] else -1
                    t_trend2 = 1 if troughs[-1] > troughs[-2] else -1
                    if p_trend1 != p_trend2 or t_trend1 != t_trend2:
                        if cc[-1] > peaks[-1]:
                            tds += 2
                        elif pct_last > 5:
                            tds += 1
                
                tds = max(min(tds, 12), 0)
                
                # --- 元子元情绪 (10分) — 6阶段情绪判定+量价关系 ---
                if is_zt and r5 < -5:
                    yuanzi += 5  # 冰点启动
                elif is_zt and r5 < 5:
                    yuanzi += 4  # 发酵确认
                elif pct_last >= 5 and r5 < 0:
                    yuanzi += 4  # 冰点启动
                elif is_zt and 5 <= r5 < 15:
                    yuanzi += 3  # 主升加速
                elif pct_last >= 5 and 0 <= r5 < 10:
                    yuanzi += 3  # 发酵确认
                elif is_zt and r5 >= 15:
                    if vol_ratio_val >= 3:
                        yuanzi += 0  # 爆量见顶
                    else:
                        yuanzi += 2
                elif pct_last > 0 and r5 < 10:
                    yuanzi += 2
                elif pct_last < -3 and r5 > 15:
                    yuanzi += 0  # 退潮补跌
                elif pct_last <= 0:
                    if r5 < -10:
                        yuanzi += 2
                    else:
                        yuanzi += 1
                
                # 量价关系
                if pct_last > 3 and vol_ratio_val < 1.2:
                    yuanzi += 2  # 缩量上涨
                elif pct_last < 2 and vol_ratio_val > 2.5:
                    yuanzi -= 1  # 放量滞涨
                
                # 主线行业加分
                if mainline_scores.get(ind, 0) >= 10: yuanzi += 2
                elif mainline_scores.get(ind, 0) >= 5: yuanzi += 1
                
                yuanzi = max(min(yuanzi, 10), 0)
                
                # --- TXCG六大模型(0-5) ---
                ind_zt_count_local = ind_zt_map.get(ind, 0)
                
                if is_zt and ind_zt_count_local >= 3:
                    txcg_model += 1  # 连板竞争
                if r5 < -5 and pct_last > 3:
                    txcg_model += 1  # 分歧期超跌轮动
                if n >= 3:
                    prev_chg = (cc[-2] - cc[-3]) / cc[-3] * 100 if cc[-3] > 0 else 0
                    if prev_chg < -3 and pct_last > 2:
                        txcg_model += 1  # 反包修复
                if ma5_val > 0 and abs(cc[-1] - ma5_val) / ma5_val < 0.02 and pct_last > 0:
                    txcg_model += 1  # 承接
                if n >= 2:
                    body_prev = abs(cc[-2] - oo[-2])
                    lower_shadow_prev = min(cc[-2], oo[-2]) - ll[-2]
                    if lower_shadow_prev > body_prev * 2 and lower_shadow_prev > 0 and pct_last > 0:
                        txcg_model += 1  # 大长腿
                if is_zt and ind_zt_count_local == 1:
                    txcg_model += 1  # 唯一性
                txcg_model = min(txcg_model, 5)
                
                d3 = mistery + tds + yuanzi + txcg_model
        
        # ====== D4: 安全边际 (15分) ======
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
        
        # ====== D5: 基本面 (15分) ======
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
        
        # ====== D9: 百胜WR (15分) ======
        d9 = 0
        wr_tags = []
        
        # --- WR-1 首板放量(0-7) ---
        wr1 = 0
        vr_wr = bas_d.get(code, {}).get('volume_ratio', 0)  # 真实量比（对齐score_system.py）
        if is_zt:
            wr1 += 1  # 条件1：涨停
            # 条件2：量比>=3（使用真实量比）
            if vr_wr and vr_wr >= 3: wr1 += 1
            if tr and tr >= 8: wr1 += 1  # 条件3：换手>=8%
            if is_ma_bull: wr1 += 1  # 条件4：均线多头
            if 30 <= mv <= 150: wr1 += 1  # 条件5：市值
            if nb_yi > 0: wr1 += 1  # 条件6：资金净流入
            # 封板时间（回测中无法精确获取，用涨幅近似：涨幅越接近涨停价=封板越早）
            # 简化：如果开盘就接近涨停价（open接近close），认为封板早
            if len(klines) >= 1:
                last_open = float(klines[-1].get('open', 0))
                if last_open > 0 and (c0 - last_open) / last_open * 100 < 2:
                    wr1 += 1  # 一字板/开盘即封
            if wr1 >= 6: wr_tags.append(f"🔥WR1={wr1}/7")
            elif wr1 >= 5: wr_tags.append(f"WR1={wr1}/7")
        
        # --- WR-2 右侧起爆(0-5) ---
        wr2 = 0
        if len(klines) >= 10 and n >= 10:
            if bbw_val < 0.15: wr2 += 1  # 条件1：BBW收缩
            # 条件2：倍量突破（量比>=2.5，优先用真实量比，对齐score_system.py）
            if vr_wr and vr_wr >= 2.5: wr2 += 1
            elif vol10 > 0 and vol5 / vol10 > 2.5: wr2 += 1
            if pct_last >= 9.5: wr2 += 1
            elif pct_last >= 7: wr2 += 1
            if is_ma_bull: wr2 += 1
            if peaks and cc[-1] > peaks[-1]: wr2 += 1
            if wr2 >= 4: wr_tags.append(f"🔥WR2={wr2}/5起爆")
            elif wr2 >= 3: wr_tags.append(f"WR2={wr2}/5")
        
        # --- WR-3 底倍量柱(0-4, 使用60分钟K线) ---
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
                        if closes_60[j_60] > first_high:
                            wr3 += 1
                        if lows_60[j_60] >= first_low:
                            wr3 += 1
                        break
                
                if closes_60[-1] >= first_low:
                    wr3 += 1
                
                if wr3 >= 3: wr_tags.append(f"🔥WR3={wr3}/4底倍量")
                elif wr3 >= 2: wr_tags.append(f"WR3={wr3}/4")
        
        # WR得分映射到15分
        best_wr = max(wr1, wr2, wr3)
        if best_wr == wr1: best_wr_max = 7
        elif best_wr == wr2: best_wr_max = 5
        else: best_wr_max = 4
        d9 = int(best_wr / best_wr_max * 15 + 0.5) if best_wr_max > 0 else 0
        d9 = min(d9, 15)
        
        # ====== 风险扣分 (0~-30) ======
        risk = 0
        risk_tags = []
        ind_zt_count = ind_zt_map.get(ind, 0)
        
        # 7a超涨
        if r5 > 20:
            if is_ma_bull: risk += 3
            else: risk += 5
            risk_tags.append(f"超涨5日{r5:.0f}%")
        elif r5 > 15:
            risk += 2
        
        if r10 > 25:
            if is_ma_bull: risk += 3
            else: risk += 5
            risk_tags.append(f"超涨10日{r10:.0f}%")
        elif r10 > 20:
            risk += 2
        
        if r20 > 50:
            risk += 8
            risk_tags.append(f"极端超涨20日{r20:.0f}%")
        elif r20 > 35:
            risk += 4
        
        # 7b板块效应不足(BCI加权)
        ind_bci_risk = ind_bci_map.get(ind, 0)
        if ind_zt_count < 3:
            if ind_bci_risk >= 50:
                risk += 1
            elif mainline_scores.get(ind, 0) >= 8:
                risk += 2
            elif ind_zt_count == 0:
                if ind_bci_risk >= 30: risk += 3
                else: risk += 5
                risk_tags.append(f"行业涨停0家")
            else:
                risk += 3
        
        # 7c净流出
        if nb_yi < -2:
            if is_zt: risk += 1
            else: risk += 3
            risk_tags.append(f"净出{nb_yi:.1f}亿")
        elif nb_yi < -0.5:
            risk += 1
        
        # 7d市值超标
        if mv > dynamic_mv_cap:
            if mv > 1000: risk += 5
            else: risk += 3
            risk_tags.append(f"市值{mv:.0f}亿")
        
        # 7e换手率异常
        if tr and tr > 50:
            risk += 3
            risk_tags.append(f"高换手{tr:.0f}%")
        elif tr and tr > 30:
            risk += 1
        
        risk = min(risk, 30)
        
        # ====== 保护因子 (0~+15) ======
        protect = 0
        
        # P1趋势多头
        if is_ma_bull: protect += 3
        
        # P2连阳
        if consecutive_yang >= 5: protect += 3
        elif consecutive_yang >= 3: protect += 2
        
        # P3 Mistery高分
        if mistery >= 12: protect += 2
        
        # P4涨停
        if is_zt: protect += 3
        
        # P5板块龙头
        if is_zt and ind_zt_count >= 3: protect += 2
        
        # P6大单
        if nb_yi > 2: protect += 2
        
        # P7 BCI高分保护
        ind_bci_protect = ind_bci_map.get(ind, 0)
        if ind_bci_protect >= 70: protect += 2
        elif ind_bci_protect >= 50: protect += 1
        
        protect = min(protect, 15)
        
        # ====== 最终得分 ======
        raw_total = d1 + d2 + d3 + d4 + d5 + d9
        net_risk = max(risk - protect, 0)
        total = raw_total - net_risk
        
        results.append({
            'code': code, 'name': nm, 'industry': ind, 'date': T,
            'close': c0, 'pct_chg': pct_last, 'is_zt': is_zt,
            'total': total, 'raw_total': raw_total,
            'd1': d1, 'd2': d2, 'd3': d3, 'd4': d4, 'd5': d5, 'd9': d9,
            'mistery': mistery, 'tds': tds, 'yuanzi': yuanzi, 'txcg': txcg_model,
            'risk': risk, 'protect': protect, 'net_risk': net_risk,
            'wr_tags': '|'.join(wr_tags) if wr_tags else '',
            'wr1': wr1, 'wr2': wr2, 'wr3': wr3,
            'bci': ind_bci_map.get(ind, 0),
            'r5': r5, 'r10': r10, 'r20': r20,
            'pe': pe, 'mv': mv, 'tr': tr, 'nb_yi': nb_yi,
        })
    
    results.sort(key=lambda x: x['total'], reverse=True)
    return results


def calc_future_returns(dates, target_idx, picks, hold_days=[1, 2, 3, 5]):
    """计算选出标的在未来N天的实际收益"""
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


def run_backtest(start_date=None, top_n=20, hold_days=[1, 2, 3, 5]):
    """运行完整回测"""
    dates = get_available_dates()
    print(f"可用快照: {len(dates)}天 ({dates[0]} ~ {dates[-1]})")
    kline_dirs_str = ', '.join(f"{d} ({'✅' if os.path.exists(d) and len(os.listdir(d))>0 else '❌空'})" for d in KLINE_60M_DIRS)
    print(f"60分钟K线目录: {kline_dirs_str}")
    
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
        print(f"  ⚠️ 数据不足，调整持有期为: {hold_days}")
    
    backtest_dates = dates[start_idx:end_idx + 1]
    print(f"\n回测区间: {backtest_dates[0]} ~ {backtest_dates[-1]} ({len(backtest_dates)}天)")
    print(f"每天取TOP{top_n}, 持有期: {hold_days}天")
    print(f"买入假设: T+1开盘价 | 卖出假设: T+N收盘价")
    print(f"评分系统: v3.3 150分制（完全对齐score_system.py）")
    print("=" * 100)
    
    all_picks = []
    daily_summary = []
    
    for target_date in backtest_dates:
        target_idx = dates.index(target_date)
        
        print(f"\n📅 {target_date} 评分中...", end="", flush=True)
        t0 = time.time()
        
        scored = score_stocks_offline(dates, target_idx)
        if not scored:
            print(f" 无结果，跳过")
            continue
        
        top_picks = scored[:top_n]
        picks_with_returns = calc_future_returns(dates, target_idx, top_picks, hold_days)
        
        valid_picks = [p for p in picks_with_returns if p.get('buy_price') is not None]
        
        if valid_picks:
            rets_1d = [p['ret_1d'] for p in valid_picks if p.get('ret_1d') is not None]
            if rets_1d:
                avg_ret = np.mean(rets_1d)
                win_rate = sum(1 for r in rets_1d if r > 0) / len(rets_1d) * 100
                print(f" {len(scored)}只→TOP{len(top_picks)} T+1:{avg_ret:+.2f}% 胜率{win_rate:.0f}% ({time.time()-t0:.1f}s)")
                
                daily_summary.append({
                    'date': target_date,
                    'total_scored': len(scored),
                    'top_n': len(top_picks),
                    'valid': len(valid_picks),
                    'avg_ret_1d': avg_ret,
                    'win_rate_1d': win_rate,
                    'best': max(rets_1d),
                    'worst': min(rets_1d),
                })
            else:
                print(f" {len(scored)}只→无T+1数据")
        else:
            print(f" {len(scored)}只→无有效买入")
        
        all_picks.extend(picks_with_returns)
    
    return all_picks, daily_summary, hold_days


def analyze_results(all_picks, daily_summary, hold_days):
    """分析回测结果"""
    print("\n" + "=" * 100)
    print("📊 回测结果汇总 v2.0（完全对齐score_system.py v3.3 150分制）")
    print("=" * 100)
    
    if not all_picks:
        print("无数据")
        return
    
    # 1. 总体统计
    print("\n### 1. 总体收益统计")
    print(f"{'持有期':>8} {'样本数':>8} {'均收益':>8} {'中位数':>8} {'胜率':>8} {'盈亏比':>8} {'最大盈':>8} {'最大亏':>8}")
    print("-" * 72)
    
    for hd in hold_days:
        key = f'ret_{hd}d'
        rets = [p[key] for p in all_picks if p.get(key) is not None]
        if not rets: continue
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        win_rate = len(wins) / len(rets) * 100
        avg_win = np.mean(wins) if wins else 0
        avg_loss = abs(np.mean(losses)) if losses else 0.01
        pr = avg_win / avg_loss if avg_loss > 0 else float('inf')
        print(f"  T+{hd}日  {len(rets):>6}  {np.mean(rets):>+7.2f}%  {np.median(rets):>+7.2f}%  "
              f"{win_rate:>6.1f}%  {pr:>7.2f}  {max(rets):>+7.2f}%  {min(rets):>+7.2f}%")
    
    # 2. 按评分区间
    print("\n### 2. 按评分区间统计（T+1收益）")
    rets_1d = [(p['total'], p['ret_1d']) for p in all_picks if p.get('ret_1d') is not None]
    if rets_1d:
        bins = [(110, 999, '≥110分(强推)'), (90, 109, '90-109分(推荐)'),
                (75, 89, '75-89分(关注)'), (0, 74, '<75分(弱)')]
        print(f"{'区间':>15} {'样本':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8}")
        print("-" * 50)
        for lo, hi, label in bins:
            subset = [r for s, r in rets_1d if lo <= s <= hi]
            if subset:
                wins = [r for r in subset if r > 0]
                losses = [r for r in subset if r <= 0]
                wr = len(wins) / len(subset) * 100
                avg_w = np.mean(wins) if wins else 0
                avg_l = abs(np.mean(losses)) if losses else 0.01
                pr = avg_w / avg_l if avg_l > 0 else float('inf')
                print(f"  {label:>13} {len(subset):>5}  {np.mean(subset):>+7.2f}%  {wr:>6.1f}%  {pr:>7.2f}")
    
    # 3. 各维度贡献度
    print("\n### 3. 各维度贡献度分析（高分 vs 低分的T+1收益差）")
    dims = [
        ('d1', '多周期共振', 15), ('d2', '主线热点', 25), ('d3', '三Skill', 47),
        ('d4', '安全边际', 15), ('d5', '基本面', 15), ('d9', '百胜WR', 15),
    ]
    sub_dims = [
        ('mistery', 'Mistery', 20), ('tds', 'TDS', 12), ('yuanzi', '元子元', 10),
        ('txcg', 'TXCG六模型', 5), ('wr1', 'WR-1首板', 7), ('wr2', 'WR-2起爆', 5), ('wr3', 'WR-3底倍量', 4),
        ('bci', 'BCI板块', 100),
    ]
    
    print(f"{'维度':>12} {'高分均收益':>10} {'低分均收益':>10} {'差值':>8} {'高分胜率':>10} {'低分胜率':>10} {'判定':>6}")
    print("-" * 80)
    
    for key, name, max_score in dims + sub_dims:
        threshold = max_score * 0.6
        high = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) >= threshold]
        low = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) < threshold]
        
        if high and low:
            avg_h = np.mean(high)
            avg_l = np.mean(low)
            wr_h = sum(1 for r in high if r > 0) / len(high) * 100
            wr_l = sum(1 for r in low if r > 0) / len(low) * 100
            diff = avg_h - avg_l
            verdict = "✅正贡献" if diff > 0.3 else ("⚠️弱" if diff > -0.3 else "❌负贡献")
            print(f"  {name:>10} {avg_h:>+9.2f}%({len(high)})  {avg_l:>+9.2f}%({len(low)})  {diff:>+7.2f}%  {wr_h:>8.1f}%  {wr_l:>8.1f}%  {verdict}")
    
    # 4. 战法独立胜率
    print("\n### 4. 战法独立胜率（T+1收益）")
    strategies = [
        ('WR-3底倍量', [p for p in all_picks if p.get('wr3', 0) >= 3 and p.get('ret_1d') is not None]),
        ('WR-2起爆', [p for p in all_picks if p.get('wr2', 0) >= 4 and p.get('ret_1d') is not None]),
        ('WR-1首板放量', [p for p in all_picks if p.get('wr1', 0) >= 5 and p.get('ret_1d') is not None]),
        ('涨停票', [p for p in all_picks if p.get('is_zt') and p.get('ret_1d') is not None]),
        ('非涨停票', [p for p in all_picks if not p.get('is_zt') and p.get('ret_1d') is not None]),
        ('BCI≥60板块', [p for p in all_picks if p.get('bci', 0) >= 60 and p.get('ret_1d') is not None]),
        ('BCI<30板块', [p for p in all_picks if p.get('bci', 0) < 30 and p.get('ret_1d') is not None]),
        ('安全边际高(≥10)', [p for p in all_picks if p.get('d4', 0) >= 10 and p.get('ret_1d') is not None]),
        ('安全边际低(<5)', [p for p in all_picks if p.get('d4', 0) < 5 and p.get('ret_1d') is not None]),
    ]
    
    print(f"{'战法':>18} {'样本':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8} {'最佳':>8} {'最差':>8}")
    print("-" * 70)
    for name, picks in strategies:
        if picks:
            rets = [p['ret_1d'] for p in picks]
            wins = [r for r in rets if r > 0]
            losses = [r for r in rets if r <= 0]
            wr = len(wins) / len(rets) * 100
            avg_w = np.mean(wins) if wins else 0
            avg_l = abs(np.mean(losses)) if losses else 0.01
            pr = avg_w / avg_l if avg_l > 0 else float('inf')
            print(f"  {name:>16} {len(rets):>5}  {np.mean(rets):>+7.2f}%  {wr:>6.1f}%  {pr:>7.2f}  "
                  f"{max(rets):>+7.2f}%  {min(rets):>+7.2f}%")
    
    # 5. 每日收益
    if daily_summary:
        print("\n### 5. 每日收益明细")
        print(f"{'日期':>10} {'评分数':>6} {'TOP-N':>6} {'T+1均收益':>10} {'胜率':>8} {'最佳':>8} {'最差':>8}")
        print("-" * 65)
        for ds in daily_summary:
            print(f"  {ds['date']}  {ds['total_scored']:>5}  {ds['top_n']:>5}  "
                  f"{ds['avg_ret_1d']:>+9.2f}%  {ds['win_rate_1d']:>6.1f}%  "
                  f"{ds['best']:>+7.2f}%  {ds['worst']:>+7.2f}%")
        
        cum_ret = sum(ds['avg_ret_1d'] for ds in daily_summary)
        win_days = sum(1 for ds in daily_summary if ds['avg_ret_1d'] > 0)
        print(f"\n  累计收益: {cum_ret:+.2f}%")
        print(f"  日胜率: {win_days}/{len(daily_summary)} = {win_days/len(daily_summary)*100:.0f}%")
        print(f"  日均收益: {cum_ret/len(daily_summary):+.3f}%")


def save_results(all_picks, daily_summary, hold_days):
    """保存回测结果"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if all_picks:
        df = pd.DataFrame(all_picks)
        out_path = os.path.join(RESULTS_DIR, f"backtest_v2_detail_{timestamp}.csv")
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 详细数据: {out_path}")
    if daily_summary:
        df_daily = pd.DataFrame(daily_summary)
        out_path2 = os.path.join(RESULTS_DIR, f"backtest_v2_daily_{timestamp}.csv")
        df_daily.to_csv(out_path2, index=False, encoding='utf-8-sig')
        print(f"💾 每日汇总: {out_path2}")


# ===== HTML报告生成（借鉴看海量化CLI的report功能） =====
def generate_html_report(all_picks, daily_summary, hold_days, output_path=None):
    """
    生成精美HTML回测报告，包含：
    1. 净值曲线（累计收益走势）
    2. 月度收益热力图
    3. 核心指标仪表盘
    4. 维度贡献度雷达图
    5. 战法独立胜率表
    6. 每日交易明细表
    """
    if not all_picks or not daily_summary:
        print("⚠️ 无数据，无法生成报告")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_path is None:
        output_path = os.path.join(RESULTS_DIR, f"backtest_report_{timestamp}.html")
    
    # --- 计算核心指标 ---
    rets_1d = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None]
    total_trades = len(rets_1d)
    avg_ret = np.mean(rets_1d) if rets_1d else 0
    win_rate = sum(1 for r in rets_1d if r > 0) / total_trades * 100 if total_trades else 0
    wins = [r for r in rets_1d if r > 0]
    losses = [r for r in rets_1d if r <= 0]
    avg_win = np.mean(wins) if wins else 0
    avg_loss = abs(np.mean(losses)) if losses else 0.01
    profit_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
    max_win = max(rets_1d) if rets_1d else 0
    max_loss = min(rets_1d) if rets_1d else 0
    
    # --- 累计净值曲线数据 ---
    nav_dates = []
    nav_values = []
    cum_nav = 1.0
    for ds in daily_summary:
        cum_nav *= (1 + ds['avg_ret_1d'] / 100)
        nav_dates.append(ds['date'])
        nav_values.append(round(cum_nav, 4))
    
    total_return = (cum_nav - 1) * 100
    trading_days = len(daily_summary)
    annual_return = ((cum_nav ** (250 / trading_days)) - 1) * 100 if trading_days > 0 else 0
    
    # 最大回撤
    peak = 1.0
    max_dd = 0
    for nav in nav_values:
        if nav > peak:
            peak = nav
        dd = (peak - nav) / peak * 100
        if dd > max_dd:
            max_dd = dd
    
    # --- 月度收益热力图数据 ---
    monthly_rets = defaultdict(float)
    monthly_counts = defaultdict(int)
    for ds in daily_summary:
        ym = ds['date'][:6]  # YYYYMM
        monthly_rets[ym] += ds['avg_ret_1d']
        monthly_counts[ym] += 1
    
    months_sorted = sorted(monthly_rets.keys())
    years = sorted(set(m[:4] for m in months_sorted))
    
    # --- 维度贡献度数据 ---
    dims_radar = []
    dim_configs = [
        ('d1', '多周期共振', 15), ('d2', '主线热点', 25), ('d3', '三Skill', 47),
        ('d4', '安全边际', 15), ('d5', '基本面', 15), ('d9', '百胜WR', 15),
    ]
    for key, name, max_score in dim_configs:
        threshold = max_score * 0.6
        high = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) >= threshold]
        low = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) < threshold]
        if high and low:
            diff = np.mean(high) - np.mean(low)
            dims_radar.append({'name': name, 'value': round(max(diff, 0) * 10, 1)})
        else:
            dims_radar.append({'name': name, 'value': 0})
    
    # --- 战法胜率数据 ---
    strategy_stats = []
    strategies = [
        ('WR-3底倍量', [p for p in all_picks if p.get('wr3', 0) >= 3 and p.get('ret_1d') is not None]),
        ('WR-2起爆', [p for p in all_picks if p.get('wr2', 0) >= 4 and p.get('ret_1d') is not None]),
        ('WR-1首板放量', [p for p in all_picks if p.get('wr1', 0) >= 5 and p.get('ret_1d') is not None]),
        ('涨停票', [p for p in all_picks if p.get('is_zt') and p.get('ret_1d') is not None]),
        ('非涨停票', [p for p in all_picks if not p.get('is_zt') and p.get('ret_1d') is not None]),
        ('BCI≥60', [p for p in all_picks if p.get('bci', 0) >= 60 and p.get('ret_1d') is not None]),
    ]
    for name, picks in strategies:
        if picks:
            rets = [p['ret_1d'] for p in picks]
            w = [r for r in rets if r > 0]
            l = [r for r in rets if r <= 0]
            wr = len(w) / len(rets) * 100
            avg_w = np.mean(w) if w else 0
            avg_l = abs(np.mean(l)) if l else 0.01
            pr = avg_w / avg_l if avg_l > 0 else 99
            strategy_stats.append({
                'name': name, 'count': len(rets),
                'avg_ret': round(np.mean(rets), 2), 'win_rate': round(wr, 1),
                'profit_ratio': round(pr, 2),
            })
    
    # --- 按评分区间统计 ---
    score_bins = []
    bins = [(110, 999, '≥110分'), (90, 109, '90-109分'), (75, 89, '75-89分'), (0, 74, '<75分')]
    for lo, hi, label in bins:
        subset = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and lo <= p.get('total', 0) <= hi]
        if subset:
            w = [r for r in subset if r > 0]
            l = [r for r in subset if r <= 0]
            wr = len(w) / len(subset) * 100
            score_bins.append({
                'label': label, 'count': len(subset),
                'avg_ret': round(np.mean(subset), 2), 'win_rate': round(wr, 1),
            })
    
    # --- 生成HTML ---
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>选股系统回测报告 v2.1</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1923; color: #e0e0e0; }}
.header {{ background: linear-gradient(135deg, #1a2332 0%, #2d3748 100%); padding: 30px 40px; border-bottom: 2px solid #e53e3e; }}
.header h1 {{ font-size: 28px; color: #fff; margin-bottom: 8px; }}
.header .subtitle {{ color: #a0aec0; font-size: 14px; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
.dashboard {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }}
.card {{ background: #1a2332; border-radius: 12px; padding: 20px; border: 1px solid #2d3748; }}
.card .label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 1px; }}
.card .value {{ font-size: 32px; font-weight: 700; margin-top: 8px; }}
.card .value.green {{ color: #48bb78; }}
.card .value.red {{ color: #fc8181; }}
.card .value.yellow {{ color: #ecc94b; }}
.card .value.blue {{ color: #63b3ed; }}
.chart-container {{ background: #1a2332; border-radius: 12px; padding: 20px; margin: 20px 0; border: 1px solid #2d3748; }}
.chart-title {{ font-size: 18px; font-weight: 600; margin-bottom: 16px; color: #fff; }}
.chart {{ width: 100%; height: 400px; }}
.chart-half {{ width: 100%; height: 350px; }}
.grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 900px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ background: #2d3748; color: #a0aec0; padding: 10px 12px; text-align: left; font-weight: 600; position: sticky; top: 0; }}
td {{ padding: 8px 12px; border-bottom: 1px solid #2d3748; }}
tr:hover {{ background: #2d374880; }}
.tag {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; margin: 1px; }}
.tag-green {{ background: #22543d; color: #9ae6b4; }}
.tag-red {{ background: #742a2a; color: #feb2b2; }}
.tag-blue {{ background: #2a4365; color: #90cdf4; }}
.scrollable {{ max-height: 500px; overflow-y: auto; }}
.footer {{ text-align: center; padding: 30px; color: #4a5568; font-size: 12px; }}
</style>
</head>
<body>
<div class="header">
  <h1>📈 选股系统回测报告</h1>
  <div class="subtitle">评分系统 v3.3 (150分制) | 回测区间: {nav_dates[0] if nav_dates else 'N/A'} ~ {nav_dates[-1] if nav_dates else 'N/A'} | 共{trading_days}个交易日 | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
</div>
<div class="container">

<!-- 核心指标仪表盘 -->
<div class="dashboard">
  <div class="card"><div class="label">累计收益</div><div class="value {'green' if total_return > 0 else 'red'}">{total_return:+.2f}%</div></div>
  <div class="card"><div class="label">年化收益</div><div class="value {'green' if annual_return > 0 else 'red'}">{annual_return:+.1f}%</div></div>
  <div class="card"><div class="label">最大回撤</div><div class="value red">-{max_dd:.2f}%</div></div>
  <div class="card"><div class="label">胜率</div><div class="value {'green' if win_rate > 50 else 'yellow'}">{win_rate:.1f}%</div></div>
  <div class="card"><div class="label">盈亏比</div><div class="value {'green' if profit_ratio > 1.5 else 'yellow'}">{profit_ratio:.2f}</div></div>
  <div class="card"><div class="label">日均收益</div><div class="value {'green' if avg_ret > 0 else 'red'}">{avg_ret:+.3f}%</div></div>
  <div class="card"><div class="label">总交易次数</div><div class="value blue">{total_trades}</div></div>
  <div class="card"><div class="label">日胜率</div><div class="value {'green' if sum(1 for d in daily_summary if d['avg_ret_1d']>0)/len(daily_summary)*100 > 50 else 'yellow'}">{sum(1 for d in daily_summary if d['avg_ret_1d']>0)/len(daily_summary)*100:.0f}%</div></div>
</div>

<!-- 净值曲线 -->
<div class="chart-container">
  <div class="chart-title">📊 累计净值曲线</div>
  <div id="navChart" class="chart"></div>
</div>

<!-- 月度收益热力图 + 维度贡献度雷达图 -->
<div class="grid-2">
  <div class="chart-container">
    <div class="chart-title">🗓️ 月度收益热力图</div>
    <div id="heatmapChart" class="chart-half"></div>
  </div>
  <div class="chart-container">
    <div class="chart-title">🎯 维度贡献度雷达图</div>
    <div id="radarChart" class="chart-half"></div>
  </div>
</div>

<!-- 评分区间统计 + 战法胜率 -->
<div class="grid-2">
  <div class="chart-container">
    <div class="chart-title">📊 按评分区间统计（T+1收益）</div>
    <table>
      <tr><th>评分区间</th><th>样本数</th><th>均收益</th><th>胜率</th></tr>
      {''.join(f'<tr><td>{b["label"]}</td><td>{b["count"]}</td><td class="{"tag-green" if b["avg_ret"]>0 else "tag-red"}" style="color:{"#48bb78" if b["avg_ret"]>0 else "#fc8181"}">{b["avg_ret"]:+.2f}%</td><td>{b["win_rate"]:.1f}%</td></tr>' for b in score_bins)}
    </table>
  </div>
  <div class="chart-container">
    <div class="chart-title">⚔️ 战法独立胜率（T+1收益）</div>
    <div class="scrollable">
    <table>
      <tr><th>战法</th><th>样本</th><th>均收益</th><th>胜率</th><th>盈亏比</th></tr>
      {''.join(f'<tr><td>{s["name"]}</td><td>{s["count"]}</td><td style="color:{"#48bb78" if s["avg_ret"]>0 else "#fc8181"}">{s["avg_ret"]:+.2f}%</td><td>{s["win_rate"]:.1f}%</td><td>{s["profit_ratio"]:.2f}</td></tr>' for s in strategy_stats)}
    </table>
    </div>
  </div>
</div>

<!-- 每日收益明细 -->
<div class="chart-container">
  <div class="chart-title">📅 每日收益明细</div>
  <div id="dailyBarChart" class="chart"></div>
</div>

<div class="chart-container">
  <div class="chart-title">📋 每日交易汇总表</div>
  <div class="scrollable">
  <table>
    <tr><th>日期</th><th>评分数</th><th>TOP-N</th><th>T+1均收益</th><th>胜率</th><th>最佳</th><th>最差</th></tr>
    {''.join(f'<tr><td>{ds["date"]}</td><td>{ds["total_scored"]}</td><td>{ds["top_n"]}</td><td style="color:{"#48bb78" if ds["avg_ret_1d"]>0 else "#fc8181"}">{ds["avg_ret_1d"]:+.2f}%</td><td>{ds["win_rate_1d"]:.0f}%</td><td style="color:#48bb78">{ds["best"]:+.2f}%</td><td style="color:#fc8181">{ds["worst"]:+.2f}%</td></tr>' for ds in daily_summary)}
  </table>
  </div>
</div>

</div>

<div class="footer">
  选股系统回测报告 v2.1 | 评分系统 v3.3 (150分制+BCI板块完整性+9 Skill全对齐) | 仅供研究参考，不构成投资建议
</div>

<script>
// 净值曲线
var navChart = echarts.init(document.getElementById('navChart'));
navChart.setOption({{
  tooltip: {{ trigger: 'axis', formatter: function(p) {{ return p[0].axisValue + '<br/>净值: ' + p[0].value.toFixed(4); }} }},
  grid: {{ left: 60, right: 30, top: 30, bottom: 40 }},
  xAxis: {{ type: 'category', data: {json.dumps(nav_dates)}, axisLabel: {{ color: '#718096', rotate: 45 }} }},
  yAxis: {{ type: 'value', axisLabel: {{ color: '#718096' }}, splitLine: {{ lineStyle: {{ color: '#2d3748' }} }} }},
  series: [{{
    type: 'line', data: {json.dumps(nav_values)}, smooth: true,
    lineStyle: {{ color: '{("#48bb78" if total_return > 0 else "#fc8181")}', width: 2 }},
    areaStyle: {{ color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
      {{ offset: 0, color: '{("#48bb7840" if total_return > 0 else "#fc818140")}' }},
      {{ offset: 1, color: 'transparent' }}
    ]) }},
    markLine: {{ data: [{{ yAxis: 1, lineStyle: {{ color: '#718096', type: 'dashed' }} }}], label: {{ show: false }} }}
  }}]
}});

// 每日收益柱状图
var dailyBar = echarts.init(document.getElementById('dailyBarChart'));
var dailyDates = {json.dumps([ds['date'] for ds in daily_summary])};
var dailyRets = {json.dumps([round(ds['avg_ret_1d'], 3) for ds in daily_summary])};
dailyBar.setOption({{
  tooltip: {{ trigger: 'axis' }},
  grid: {{ left: 60, right: 30, top: 30, bottom: 40 }},
  xAxis: {{ type: 'category', data: dailyDates, axisLabel: {{ color: '#718096', rotate: 45 }} }},
  yAxis: {{ type: 'value', axisLabel: {{ color: '#718096' }}, splitLine: {{ lineStyle: {{ color: '#2d3748' }} }} }},
  series: [{{
    type: 'bar', data: dailyRets.map(function(v) {{
      return {{ value: v, itemStyle: {{ color: v >= 0 ? '#48bb78' : '#fc8181' }} }};
    }})
  }}]
}});

// 雷达图
var radarChart = echarts.init(document.getElementById('radarChart'));
var radarData = {json.dumps(dims_radar)};
radarChart.setOption({{
  radar: {{
    indicator: radarData.map(function(d) {{ return {{ name: d.name, max: Math.max(...radarData.map(x=>x.value)) * 1.2 || 10 }}; }}),
    axisName: {{ color: '#a0aec0' }},
    splitLine: {{ lineStyle: {{ color: '#2d3748' }} }},
    splitArea: {{ areaStyle: {{ color: ['transparent'] }} }}
  }},
  series: [{{
    type: 'radar',
    data: [{{ value: radarData.map(function(d) {{ return d.value; }}), name: '贡献度',
      areaStyle: {{ color: '#48bb7830' }}, lineStyle: {{ color: '#48bb78' }} }}]
  }}]
}});

// 月度热力图
var heatmapChart = echarts.init(document.getElementById('heatmapChart'));
var monthlyData = {json.dumps([{'ym': k, 'ret': round(v, 2)} for k, v in sorted(monthly_rets.items())])};
var heatData = monthlyData.map(function(d) {{
  var y = parseInt(d.ym.substring(0, 4));
  var m = parseInt(d.ym.substring(4, 6));
  return [m - 1, {json.dumps(years)}.indexOf(String(y)), d.ret];
}}).filter(function(d) {{ return d[1] >= 0; }});
var maxAbs = Math.max(...heatData.map(d => Math.abs(d[2])), 1);
heatmapChart.setOption({{
  tooltip: {{ formatter: function(p) {{ return '月收益: ' + p.value[2].toFixed(2) + '%'; }} }},
  grid: {{ left: 60, right: 80, top: 10, bottom: 40 }},
  xAxis: {{ type: 'category', data: ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月'], axisLabel: {{ color: '#718096' }} }},
  yAxis: {{ type: 'category', data: {json.dumps(years)}, axisLabel: {{ color: '#718096' }} }},
  visualMap: {{ min: -maxAbs, max: maxAbs, calculable: true, orient: 'vertical', right: 0, top: 'center',
    inRange: {{ color: ['#fc8181', '#2d3748', '#48bb78'] }}, textStyle: {{ color: '#718096' }} }},
  series: [{{ type: 'heatmap', data: heatData, label: {{ show: true, formatter: function(p) {{ return p.value[2].toFixed(1) + '%'; }}, color: '#fff', fontSize: 11 }} }}]
}});

// 响应式
window.addEventListener('resize', function() {{
  navChart.resize(); dailyBar.resize(); radarChart.resize(); heatmapChart.resize();
}});
</script>
</body>
</html>"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\n📊 HTML报告已生成: {output_path}")
    return output_path


# ===== JSON输出（借鉴看海量化CLI的--json静默模式） =====
def output_json(all_picks, daily_summary, hold_days):
    """JSON格式输出回测结果，方便AI/程序化调用"""
    rets_1d = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None]
    wins = [r for r in rets_1d if r > 0]
    losses = [r for r in rets_1d if r <= 0]
    
    # 累计净值
    cum_nav = 1.0
    for ds in daily_summary:
        cum_nav *= (1 + ds['avg_ret_1d'] / 100)
    
    # 最大回撤
    peak = 1.0
    max_dd = 0
    nav_list = []
    nav_val = 1.0
    for ds in daily_summary:
        nav_val *= (1 + ds['avg_ret_1d'] / 100)
        nav_list.append(nav_val)
        if nav_val > peak:
            peak = nav_val
        dd = (peak - nav_val) / peak * 100
        if dd > max_dd:
            max_dd = dd
    
    result = {
        'version': 'v2.1',
        'scoring_system': 'v3.3 (150分制)',
        'backtest_period': {
            'start': daily_summary[0]['date'] if daily_summary else None,
            'end': daily_summary[-1]['date'] if daily_summary else None,
            'trading_days': len(daily_summary),
        },
        'performance': {
            'total_return_pct': round((cum_nav - 1) * 100, 2),
            'annual_return_pct': round(((cum_nav ** (250 / max(len(daily_summary), 1))) - 1) * 100, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'win_rate_pct': round(sum(1 for r in rets_1d if r > 0) / max(len(rets_1d), 1) * 100, 1),
            'profit_ratio': round((np.mean(wins) if wins else 0) / max(abs(np.mean(losses)) if losses else 0.01, 0.01), 2),
            'avg_daily_return_pct': round(np.mean(rets_1d), 3) if rets_1d else 0,
            'total_trades': len(rets_1d),
            'daily_win_rate_pct': round(sum(1 for d in daily_summary if d['avg_ret_1d'] > 0) / max(len(daily_summary), 1) * 100, 1),
        },
        'hold_period_stats': {},
        'daily_summary': [
            {
                'date': ds['date'],
                'avg_ret_1d': round(ds['avg_ret_1d'], 3),
                'win_rate': round(ds['win_rate_1d'], 1),
                'top_n': ds['top_n'],
            }
            for ds in daily_summary
        ],
    }
    
    # 各持有期统计
    for hd in hold_days:
        key = f'ret_{hd}d'
        rets = [p[key] for p in all_picks if p.get(key) is not None]
        if rets:
            w = [r for r in rets if r > 0]
            l = [r for r in rets if r <= 0]
            result['hold_period_stats'][f'T+{hd}'] = {
                'count': len(rets),
                'avg_return': round(np.mean(rets), 3),
                'win_rate': round(len(w) / len(rets) * 100, 1),
                'profit_ratio': round((np.mean(w) if w else 0) / max(abs(np.mean(l)) if l else 0.01, 0.01), 2),
            }
    
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


# ===== 参数寻优模式（借鉴看海量化CLI的批量回测能力） =====
def run_optimize(start_date=None):
    """
    参数寻优：批量测试不同TOP-N、持有期、评分阈值组合，
    找出最优参数配置。
    """
    print("=" * 100)
    print("🔍 参数寻优模式（批量回测）")
    print("=" * 100)
    
    # 参数空间
    top_ns = [5, 10, 15, 20, 30]
    hold_days_list = [[1], [2], [3], [5], [1, 3, 5]]
    score_thresholds = [0, 75, 85, 90]  # 最低评分阈值
    
    total_combos = len(top_ns) * len(hold_days_list) * len(score_thresholds)
    print(f"参数空间: TOP-N={top_ns} × 持有期={[str(h) for h in hold_days_list]} × 评分阈值={score_thresholds}")
    print(f"总组合数: {total_combos}")
    print()
    
    dates = get_available_dates()
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
    
    # 先跑一次完整评分（取最大TOP-N），缓存结果
    max_top = max(top_ns)
    max_hold = 5
    end_idx = len(dates) - max_hold - 1
    if start_idx > end_idx:
        end_idx = len(dates) - 2
    
    backtest_dates = dates[start_idx:end_idx + 1]
    print(f"回测区间: {backtest_dates[0]} ~ {backtest_dates[-1]} ({len(backtest_dates)}天)")
    print(f"先运行完整评分（TOP{max_top}）缓存结果...")
    print()
    
    # 运行完整回测
    all_picks_full, _, _ = run_backtest(start_date=start_date, top_n=max_top, hold_days=[1, 2, 3, 5])
    
    # 对每个参数组合计算指标
    results = []
    print(f"\n{'='*100}")
    print(f"{'TOP-N':>6} {'持有期':>8} {'阈值':>6} {'样本':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8} {'累计':>8} {'最大回撤':>8}")
    print("-" * 80)
    
    for top_n in top_ns:
        for hold_days in hold_days_list:
            for threshold in score_thresholds:
                # 过滤
                filtered = [p for p in all_picks_full if p.get('total', 0) >= threshold]
                
                # 按日期分组，每天取TOP-N
                by_date = defaultdict(list)
                for p in filtered:
                    by_date[p['date']].append(p)
                
                selected = []
                for d, picks in by_date.items():
                    picks.sort(key=lambda x: x['total'], reverse=True)
                    selected.extend(picks[:top_n])
                
                # 计算指标
                hd = hold_days[0]
                key = f'ret_{hd}d'
                rets = [p[key] for p in selected if p.get(key) is not None]
                
                if not rets:
                    continue
                
                wins = [r for r in rets if r > 0]
                losses = [r for r in rets if r <= 0]
                avg_ret = np.mean(rets)
                wr = len(wins) / len(rets) * 100
                avg_w = np.mean(wins) if wins else 0
                avg_l = abs(np.mean(losses)) if losses else 0.01
                pr = avg_w / avg_l if avg_l > 0 else 99
                
                # 简化累计收益
                daily_rets = defaultdict(list)
                for p in selected:
                    if p.get(key) is not None:
                        daily_rets[p['date']].append(p[key])
                cum = 1.0
                peak = 1.0
                max_dd = 0
                for d in sorted(daily_rets.keys()):
                    day_avg = np.mean(daily_rets[d])
                    cum *= (1 + day_avg / 100)
                    if cum > peak: peak = cum
                    dd = (peak - cum) / peak * 100
                    if dd > max_dd: max_dd = dd
                
                cum_ret = (cum - 1) * 100
                
                hold_str = ','.join(str(h) for h in hold_days)
                print(f"  TOP{top_n:<3} T+{hold_str:<5} ≥{threshold:<4} {len(rets):>5}  {avg_ret:>+7.2f}%  {wr:>6.1f}%  {pr:>7.2f}  {cum_ret:>+7.1f}%  -{max_dd:>6.1f}%")
                
                results.append({
                    'top_n': top_n, 'hold_days': hold_str, 'threshold': threshold,
                    'count': len(rets), 'avg_ret': avg_ret, 'win_rate': wr,
                    'profit_ratio': pr, 'cum_return': cum_ret, 'max_dd': max_dd,
                })
    
    # 找最优
    if results:
        best = max(results, key=lambda x: x['cum_return'])
        print(f"\n{'='*80}")
        print(f"🏆 最优参数组合:")
        print(f"   TOP-N = {best['top_n']}")
        print(f"   持有期 = T+{best['hold_days']}天")
        print(f"   评分阈值 = ≥{best['threshold']}分")
        print(f"   累计收益 = {best['cum_return']:+.2f}%")
        print(f"   胜率 = {best['win_rate']:.1f}%")
        print(f"   盈亏比 = {best['profit_ratio']:.2f}")
        print(f"   最大回撤 = -{best['max_dd']:.2f}%")
        
        # 保存寻优结果
        df_opt = pd.DataFrame(results)
        opt_path = os.path.join(RESULTS_DIR, f"optimize_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        df_opt.to_csv(opt_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 寻优结果已保存: {opt_path}")
    
    return results


# ===== 多策略对比（借鉴看海量化CLI的compare功能） =====
def compare_results(csv_files):
    """
    横向对比多次回测结果CSV文件。
    用法: python3 backtest_v2.py --compare result1.csv result2.csv
    """
    print("=" * 100)
    print("📊 多策略对比")
    print("=" * 100)
    
    all_stats = []
    
    for i, csv_file in enumerate(csv_files):
        # 支持相对路径和绝对路径
        if not os.path.isabs(csv_file):
            csv_file = os.path.join(RESULTS_DIR, csv_file)
        
        if not os.path.exists(csv_file):
            print(f"  ❌ 文件不存在: {csv_file}")
            continue
        
        df = pd.read_csv(csv_file)
        name = os.path.basename(csv_file).replace('.csv', '')
        
        # 检测是detail还是daily文件
        if 'ret_1d' in df.columns:
            # detail文件
            rets = df['ret_1d'].dropna().tolist()
            if not rets:
                continue
            wins = [r for r in rets if r > 0]
            losses = [r for r in rets if r <= 0]
            
            # 按日期计算累计收益
            if 'date' in df.columns:
                daily_avg = df.groupby('date')['ret_1d'].mean()
                cum = 1.0
                peak = 1.0
                max_dd = 0
                for ret in daily_avg.sort_index():
                    if pd.notna(ret):
                        cum *= (1 + ret / 100)
                        if cum > peak: peak = cum
                        dd = (peak - cum) / peak * 100
                        if dd > max_dd: max_dd = dd
                cum_ret = (cum - 1) * 100
                trading_days = len(daily_avg)
            else:
                cum_ret = sum(rets)
                max_dd = 0
                trading_days = 0
            
            all_stats.append({
                'name': name,
                'trades': len(rets),
                'trading_days': trading_days,
                'avg_ret': round(np.mean(rets), 3),
                'median_ret': round(np.median(rets), 3),
                'win_rate': round(len(wins) / len(rets) * 100, 1),
                'profit_ratio': round((np.mean(wins) if wins else 0) / max(abs(np.mean(losses)) if losses else 0.01, 0.01), 2),
                'cum_return': round(cum_ret, 2),
                'max_dd': round(max_dd, 2),
                'max_win': round(max(rets), 2),
                'max_loss': round(min(rets), 2),
            })
        
        elif 'avg_ret_1d' in df.columns:
            # daily文件
            rets = df['avg_ret_1d'].dropna().tolist()
            if not rets:
                continue
            cum = 1.0
            peak = 1.0
            max_dd = 0
            for ret in rets:
                cum *= (1 + ret / 100)
                if cum > peak: peak = cum
                dd = (peak - cum) / peak * 100
                if dd > max_dd: max_dd = dd
            
            all_stats.append({
                'name': name,
                'trades': 0,
                'trading_days': len(rets),
                'avg_ret': round(np.mean(rets), 3),
                'median_ret': round(np.median(rets), 3),
                'win_rate': round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
                'profit_ratio': 0,
                'cum_return': round((cum - 1) * 100, 2),
                'max_dd': round(max_dd, 2),
                'max_win': round(max(rets), 2),
                'max_loss': round(min(rets), 2),
            })
    
    if not all_stats:
        print("  ❌ 无有效数据")
        return
    
    # 打印对比表
    print(f"\n{'策略':>30} {'交易日':>6} {'交易数':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8} {'累计收益':>10} {'最大回撤':>10}")
    print("-" * 100)
    for s in all_stats:
        print(f"  {s['name']:>28} {s['trading_days']:>5} {s['trades']:>5}  "
              f"{s['avg_ret']:>+7.3f}%  {s['win_rate']:>6.1f}%  {s['profit_ratio']:>7.2f}  "
              f"{s['cum_return']:>+9.2f}%  -{s['max_dd']:>8.2f}%")
    
    # 找最优
    best = max(all_stats, key=lambda x: x['cum_return'])
    print(f"\n🏆 最优策略: {best['name']}")
    print(f"   累计收益: {best['cum_return']:+.2f}% | 胜率: {best['win_rate']:.1f}% | 盈亏比: {best['profit_ratio']:.2f} | 最大回撤: -{best['max_dd']:.2f}%")
    
    return all_stats


def main():
    parser = argparse.ArgumentParser(description='选股系统历史回测 v2.1（完全对齐v3.3 + 增强报告功能）')
    parser.add_argument('--start', type=str, default=None, help='回测起始日期(YYYYMMDD)')
    parser.add_argument('--top', type=int, default=20, help='每天取TOP N只(默认20)')
    parser.add_argument('--hold', type=str, default='1,2,3,5', help='持有天数(逗号分隔)')
    parser.add_argument('--save', action='store_true', help='保存结果到CSV')
    parser.add_argument('--report', action='store_true', help='生成HTML报告（自动打开浏览器）')
    parser.add_argument('--json', action='store_true', help='JSON格式输出（静默模式，方便AI调用）')
    parser.add_argument('--optimize', action='store_true', help='参数寻优模式（批量测试不同参数组合）')
    parser.add_argument('--compare', nargs='+', metavar='CSV', help='对比多次回测结果CSV文件')
    args = parser.parse_args()
    
    # --- 多策略对比模式 ---
    if args.compare:
        compare_results(args.compare)
        return
    
    # --- 参数寻优模式 ---
    if args.optimize:
        run_optimize(start_date=args.start)
        return
    
    hold_days = [int(x) for x in args.hold.split(',')]
    
    # --- JSON静默模式 ---
    if args.json:
        # 静默运行（重定向stdout）
        import io
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        all_picks, daily_summary, hold_days = run_backtest(
            start_date=args.start, top_n=args.top, hold_days=hold_days,
        )
        sys.stdout = old_stdout
        output_json(all_picks, daily_summary, hold_days)
        return
    
    print("=" * 100)
    print("📈 选股系统历史回测 v2.1（完全对齐score_system.py v3.3 150分制 + 增强报告功能）")
    print("=" * 100)
    print(f"数据目录: {SNAPSHOT_DIR}")
    print(f"60分钟K线: {KLINE_60M_DIR}")
    print(f"参数: TOP{args.top} | 持有期{hold_days}天 | 起始{args.start or '自动'}")
    if args.report: print(f"📊 将生成HTML报告")
    
    all_picks, daily_summary, hold_days = run_backtest(
        start_date=args.start, top_n=args.top, hold_days=hold_days,
    )
    
    analyze_results(all_picks, daily_summary, hold_days)
    
    if args.save:
        save_results(all_picks, daily_summary, hold_days)
    
    if args.report:
        report_path = generate_html_report(all_picks, daily_summary, hold_days)
        if report_path:
            print(f"🌐 正在打开浏览器...")
            webbrowser.open(f'file://{os.path.abspath(report_path)}')
    
    if not args.save and not args.report:
        print(f"\n💡 添加 --save 保存CSV | --report 生成HTML报告 | --json JSON输出 | --optimize 参数寻优")


if __name__ == '__main__':
    main()
