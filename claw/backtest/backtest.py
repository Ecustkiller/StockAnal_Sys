#!/usr/bin/env python3
"""
选股系统历史回测 v1.0
======================
基于本地 ~/stock_data/daily_snapshot/ 的parquet快照数据，
离线模拟每天运行评分选股系统，统计推荐标的在未来N天的实际收益。

用法：
  python3 backtest.py                    # 默认回测最近可用的所有日期
  python3 backtest.py --start 20260320   # 从指定日期开始回测
  python3 backtest.py --top 10           # 每天取TOP10（默认TOP20）
  python3 backtest.py --hold 3           # 持有3天后卖出（默认1/2/3/5天都统计）

输出：
  1. 每日选股结果 + 未来收益
  2. 总体胜率/盈亏比
  3. 各评分维度的贡献度分析
  4. 各战法(WR/Mistery等)命中标的的独立胜率
"""
import os, sys, time, argparse, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ===== 配置 =====
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
RESULTS_DIR = os.path.expanduser("~/WorkBuddy/Claw/backtest_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# ===== 数据加载 =====
def get_available_dates():
    """获取所有可用的快照日期（已排序）"""
    files = [f for f in os.listdir(SNAPSHOT_DIR) 
             if f.endswith('.parquet') and f != 'stock_basic.parquet']
    dates = sorted([f.replace('.parquet', '') for f in files])
    return dates

def load_snapshot(date_str):
    """加载某天的全市场快照"""
    path = os.path.join(SNAPSHOT_DIR, f"{date_str}.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    # 统一数值类型
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'pct_chg',
                'vol', 'amount', 'pe_ttm', 'total_mv', 'circ_mv',
                'turnover_rate_f', 'net_mf_amount']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def build_history(dates, target_idx):
    """
    为target_idx对应的日期构建历史数据：
    返回 {date_str: DataFrame} 字典，包含target及之前的数据
    """
    # 需要T, T-1, T-2 的完整数据 + T-5, T-10, T-20的收盘价
    needed_offsets = [0, 1, 2, 5, 10, 20]
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

# ===== 简化版评分引擎（离线版，不调API） =====
def score_stocks_offline(dates, target_idx):
    """
    离线评分：模拟score_system.py的核心逻辑，但完全基于本地数据。
    
    简化版保留核心维度：
    D1: 多周期共振(15分)
    D2: 主线热点(20分) — 简化版不含BCI
    D3: 三Skill(35分) — Mistery+TDS+元子元
    D4: 安全边际(15分)
    D5: 基本面(15分)
    D7: 风险扣分(0~-30)
    D8: 保护因子(0~+15)
    
    满分: 100分（简化版不含WR的60分钟数据和BCI）
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
    
    # 构建收盘价字典 {ts_code: {date: close}}
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
    
    # 行业涨停统计（主线热度）
    ind_zt_count = defaultdict(int)
    zt_codes = set()
    for _, row in df_t0.iterrows():
        if row.get('pct_chg', 0) >= 9.5:
            ind = ind_map.get(row['ts_code'], '?')
            ind_zt_count[ind] += 1
            zt_codes.add(row['ts_code'])
    total_zt = len(zt_codes)
    
    # 主线热度分（简化版：基于近3天涨停数）
    mainline_scores = {}
    # 近3天行业涨停统计
    ind_perf = defaultdict(list)
    for offset in [0, 1, 2]:
        d = dates[target_idx - offset] if target_idx >= offset else None
        if d and d in history:
            df_d = history[d]
            df_d_ind = df_d[df_d['ts_code'].str.match(r'^(00|30|60|68)', na=False)]
            if 'name' in df_d_ind.columns:
                df_d_ind = df_d_ind[~df_d_ind['name'].str.contains('ST|退', na=False)]
            if 'industry' in df_d_ind.columns and 'pct_chg' in df_d_ind.columns:
                for ind, grp in df_d_ind.groupby('industry'):
                    avg_chg = grp['pct_chg'].mean()
                    lim_cnt = (grp['pct_chg'] >= 9.5).sum()
                    # 排名
                    all_ind_avg = df_d_ind.groupby('industry')['pct_chg'].mean().sort_values(ascending=False)
                    rk = list(all_ind_avg.index).index(ind) + 1 if ind in all_ind_avg.index else 99
                    ind_perf[ind].append({'avg': avg_chg, 'lim': lim_cnt, 'rk': rk})
    
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
        
        mainline_scores[ind] = min(score, 20)
    
    # 构建K线数据（近10天）
    kline_data = {}
    for offset in range(min(11, target_idx + 1)):
        d = dates[target_idx - offset]
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
                })
    # 按日期排序
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
        mv = float(row.get('total_mv', 0)) / 10000 if row.get('total_mv') else 0  # 万→亿
        tr = float(row.get('turnover_rate_f', 0)) if row.get('turnover_rate_f') else 0
        nb = float(row.get('net_mf_amount', 0)) if row.get('net_mf_amount') else 0
        nb_yi = nb / 10000  # 万→亿
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
        
        # ====== D1: 多周期共振 (15分) ======
        big = 1 if r20 > 5 else (-1 if r20 < -5 else 0)
        mid = 1 if r10 > 3 else (-1 if r10 < -3 else 0)
        small = 1 if r5 > 3 else (-1 if r5 < -2 else 0)
        period_raw = big * 3 + mid * 2 + small * 1
        d1 = int((period_raw + 6) / 12 * 15 + 0.5)
        d1 = max(0, min(15, d1))
        
        # ====== D2: 主线热点 (20分) ======
        d2 = mainline_scores.get(ind, 0)
        
        # ====== D3: 三Skill (35分) ======
        klines = kline_data.get(code, [])
        d3 = 0
        mistery = 0
        tds = 0
        yuanzi = 0
        is_ma_bull = False
        
        if len(klines) >= 5:
            kdf = pd.DataFrame(klines)
            cc = kdf['close'].values.astype(float)
            oo = kdf['open'].values.astype(float)
            hh = kdf['high'].values.astype(float)
            ll = kdf['low'].values.astype(float)
            vv = kdf['vol'].values.astype(float)
            n = len(cc)
            
            # MA
            ma5 = pd.Series(cc).rolling(5).mean()
            ma10 = pd.Series(cc).rolling(10).mean() if n >= 10 else pd.Series([np.nan]*n)
            
            # 多头排列
            if n >= 10 and not pd.isna(ma5.iloc[-1]) and not pd.isna(ma10.iloc[-1]):
                if ma5.iloc[-1] > ma10.iloc[-1] and cc[-1] > ma5.iloc[-1]:
                    is_ma_bull = True
            
            # Mistery (15分)
            # M1: 趋势
            if is_ma_bull: mistery += 4
            elif n >= 5 and cc[-1] > ma5.iloc[-1]: mistery += 2
            
            # M2: BBW收敛
            if n >= 10:
                bb_mid = pd.Series(cc).rolling(20).mean() if n >= 20 else ma10
                bb_std = pd.Series(cc).rolling(20).std() if n >= 20 else pd.Series(cc).rolling(10).std()
                if not pd.isna(bb_mid.iloc[-1]) and not pd.isna(bb_std.iloc[-1]) and bb_mid.iloc[-1] > 0:
                    bbw = bb_std.iloc[-1] * 2 / bb_mid.iloc[-1]
                    if bbw < 0.10: mistery += 4
                    elif bbw < 0.15: mistery += 3
                    elif bbw < 0.20: mistery += 2
            
            # M4: 量价配合
            if n >= 2:
                if cc[-1] > cc[-2] and vv[-1] > vv[-2]: mistery += 3
                elif cc[-1] > cc[-2]: mistery += 1
            
            # M5: 形态（阳线实体大于上影）
            if n >= 1:
                body = cc[-1] - oo[-1]
                upper = hh[-1] - max(cc[-1], oo[-1])
                if body > 0 and body > upper * 2: mistery += 2
                elif body > 0: mistery += 1
            
            mistery = min(mistery, 15)
            
            # TDS (10分)
            # 波峰波谷
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
            if mainline_scores.get(ind, 0) >= 10: yuanzi += 3
            elif mainline_scores.get(ind, 0) >= 5: yuanzi += 2
            yuanzi = min(yuanzi, 10)
            
            d3 = mistery + tds + yuanzi
        
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
        
        # ====== D7: 风险扣分 (0~-30) ======
        risk = 0
        risk_tags = []
        if r5 > 20:
            if is_ma_bull: risk += 3
            else: risk += 5
            risk_tags.append(f"超涨5日{r5:.0f}%")
        elif r5 > 15:
            risk += 3
        
        if r10 > 30:
            risk += 5; risk_tags.append(f"超涨10日{r10:.0f}%")
        elif r10 > 20:
            risk += 3
        
        if nb_yi < -1:
            risk += 3; risk_tags.append(f"净出{nb_yi:.1f}亿")
        elif nb_yi < -0.5:
            risk += 1
        
        if mv > 3000:
            risk += 3; risk_tags.append(f"超大盘{mv:.0f}亿")
        
        if ind_zt_count.get(ind, 0) < 2 and mainline_scores.get(ind, 0) < 5:
            risk += 3; risk_tags.append("板块弱")
        
        risk = min(risk, 30)
        
        # ====== D8: 保护因子 (0~+15) ======
        protect = 0
        if is_ma_bull: protect += 3
        if pct_last > 0 and len(klines) >= 3:
            kdf_p = pd.DataFrame(klines)
            cc_p = kdf_p['close'].values.astype(float)
            oo_p = kdf_p['open'].values.astype(float)
            if all(cc_p[i] > oo_p[i] for i in range(-3, 0) if -3 + len(cc_p) >= 0):
                protect += 2  # 连阳
        if mistery >= 12: protect += 2
        if is_zt: protect += 3
        if is_zt and ind_zt_count.get(ind, 0) >= 3: protect += 2
        if nb_yi > 2: protect += 2
        protect = min(protect, 15)
        
        # ====== 最终得分 ======
        raw_total = d1 + d2 + d3 + d4 + d5
        net_risk = max(risk - protect, 0)
        total = raw_total - net_risk
        
        results.append({
            'code': code,
            'name': nm,
            'industry': ind,
            'date': T,
            'close': c0,
            'pct_chg': pct_last,
            'is_zt': is_zt,
            'total': total,
            'd1': d1, 'd2': d2, 'd3': d3, 'd4': d4, 'd5': d5,
            'mistery': mistery, 'tds': tds, 'yuanzi': yuanzi,
            'risk': risk, 'protect': protect,
            'r5': r5, 'r10': r10, 'r20': r20,
            'pe': pe, 'mv': mv, 'tr': tr, 'nb_yi': nb_yi,
        })
    
    # 排序
    results.sort(key=lambda x: x['total'], reverse=True)
    return results


def calc_future_returns(dates, target_idx, picks, hold_days=[1, 2, 3, 5]):
    """
    计算选出标的在未来N天的实际收益。
    
    买入假设：T+1开盘价买入（选股日T收盘后出结果，次日开盘买入）
    卖出假设：T+1+hold_day的收盘价卖出
    """
    results = []
    
    for pick in picks:
        code = pick['code']
        entry = {}
        
        # T+1开盘价 = 买入价
        if target_idx + 1 < len(dates):
            d_t1 = dates[target_idx + 1]
            df_t1 = load_snapshot(d_t1)
            if df_t1 is not None:
                row_t1 = df_t1[df_t1['ts_code'] == code]
                if not row_t1.empty:
                    entry['buy_date'] = d_t1
                    entry['buy_price'] = float(row_t1.iloc[0]['open'])
                    entry['buy_close'] = float(row_t1.iloc[0]['close'])
        
        if 'buy_price' not in entry or entry['buy_price'] <= 0:
            # 无法买入（停牌/一字板等）
            entry['buy_price'] = None
            for hd in hold_days:
                entry[f'ret_{hd}d'] = None
            results.append({**pick, **entry})
            continue
        
        buy_price = entry['buy_price']
        
        # 各持有期收益
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
                        entry[f'sell_price_{hd}d'] = sell_price
                        entry[f'sell_date_{hd}d'] = d_sell
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
    
    # 确定回测起始日（需要至少20天历史数据）
    min_history = 20
    start_idx = min_history
    
    if start_date:
        if start_date in dates:
            start_idx = max(dates.index(start_date), min_history)
        else:
            # 找最近的日期
            for i, d in enumerate(dates):
                if d >= start_date:
                    start_idx = max(i, min_history)
                    break
    
    # 回测结束日：倒数第max(hold_days)天（需要未来数据验证）
    max_hold = max(hold_days)
    end_idx = len(dates) - max_hold - 1
    
    if start_idx > end_idx:
        print(f"⚠️ 数据不足！需要至少{min_history + max_hold + 1}天数据")
        print(f"  当前: {len(dates)}天, 起始idx={start_idx}, 结束idx={end_idx}")
        # 尝试缩短持有期
        end_idx = len(dates) - 2  # 至少能看T+1
        hold_days = [hd for hd in hold_days if target_idx + 1 + hd < len(dates) for target_idx in [end_idx]]
        if not hold_days:
            hold_days = [1]
        print(f"  调整持有期为: {hold_days}")
    
    backtest_dates = dates[start_idx:end_idx + 1]
    print(f"\n回测区间: {backtest_dates[0]} ~ {backtest_dates[-1]} ({len(backtest_dates)}天)")
    print(f"每天取TOP{top_n}, 持有期: {hold_days}天")
    print(f"买入假设: T+1开盘价 | 卖出假设: T+N收盘价")
    print("=" * 100)
    
    all_picks = []  # 所有选股结果
    daily_summary = []  # 每日汇总
    
    # 缓存已加载的快照
    snapshot_cache = {}
    
    for target_date in backtest_dates:
        target_idx = dates.index(target_date)
        
        print(f"\n📅 {target_date} 评分中...", end="", flush=True)
        t0 = time.time()
        
        # 评分
        scored = score_stocks_offline(dates, target_idx)
        if not scored:
            print(f" 无结果，跳过")
            continue
        
        # 取TOP N
        top_picks = scored[:top_n]
        
        # 计算未来收益
        picks_with_returns = calc_future_returns(dates, target_idx, top_picks, hold_days)
        
        # 统计当日
        valid_picks = [p for p in picks_with_returns if p.get('buy_price') is not None]
        
        if valid_picks and 'ret_1d' in valid_picks[0] and valid_picks[0]['ret_1d'] is not None:
            rets_1d = [p['ret_1d'] for p in valid_picks if p.get('ret_1d') is not None]
            if rets_1d:
                avg_ret = np.mean(rets_1d)
                win_rate = sum(1 for r in rets_1d if r > 0) / len(rets_1d) * 100
                print(f" {len(scored)}只评分 → TOP{len(top_picks)} → "
                      f"T+1均收益{avg_ret:+.2f}% 胜率{win_rate:.0f}% "
                      f"({time.time()-t0:.1f}s)")
                
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
                print(f" {len(scored)}只评分 → 无T+1数据")
        else:
            print(f" {len(scored)}只评分 → 无有效买入")
        
        all_picks.extend(picks_with_returns)
    
    return all_picks, daily_summary, hold_days


def analyze_results(all_picks, daily_summary, hold_days):
    """分析回测结果"""
    print("\n" + "=" * 100)
    print("📊 回测结果汇总")
    print("=" * 100)
    
    if not all_picks:
        print("无数据")
        return
    
    # ===== 1. 总体统计 =====
    print("\n### 1. 总体收益统计")
    print(f"{'持有期':>8} {'样本数':>8} {'均收益':>8} {'中位数':>8} {'胜率':>8} {'盈亏比':>8} {'最大盈':>8} {'最大亏':>8}")
    print("-" * 72)
    
    for hd in hold_days:
        key = f'ret_{hd}d'
        rets = [p[key] for p in all_picks if p.get(key) is not None]
        if not rets:
            continue
        
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        win_rate = len(wins) / len(rets) * 100
        avg_win = np.mean(wins) if wins else 0
        avg_loss = abs(np.mean(losses)) if losses else 0.01
        profit_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
        
        print(f"  T+{hd}日  {len(rets):>6}  {np.mean(rets):>+7.2f}%  {np.median(rets):>+7.2f}%  "
              f"{win_rate:>6.1f}%  {profit_ratio:>7.2f}  {max(rets):>+7.2f}%  {min(rets):>+7.2f}%")
    
    # ===== 2. 按评分区间统计 =====
    print("\n### 2. 按评分区间统计（T+1收益）")
    rets_1d = [(p['total'], p['ret_1d']) for p in all_picks if p.get('ret_1d') is not None]
    if rets_1d:
        bins = [(90, 999, '≥90分(强推)'), (75, 89, '75-89分(推荐)'), 
                (60, 74, '60-74分(关注)'), (0, 59, '<60分(弱)')]
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
    
    # ===== 3. 各维度贡献度分析 =====
    print("\n### 3. 各维度贡献度分析（高分 vs 低分的T+1收益差）")
    dims = [
        ('d1', '多周期共振', 15), ('d2', '主线热点', 20), ('d3', '三Skill', 35),
        ('d4', '安全边际', 15), ('d5', '基本面', 15),
    ]
    sub_dims = [
        ('mistery', 'Mistery', 15), ('tds', 'TDS', 10), ('yuanzi', '元子元', 10),
    ]
    
    print(f"{'维度':>12} {'高分均收益':>10} {'低分均收益':>10} {'差值':>8} {'高分胜率':>10} {'低分胜率':>10} {'判定':>6}")
    print("-" * 75)
    
    for key, name, max_score in dims + sub_dims:
        threshold = max_score * 0.6  # 60%以上为高分
        high = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) >= threshold]
        low = [p['ret_1d'] for p in all_picks if p.get('ret_1d') is not None and p.get(key, 0) < threshold]
        
        if high and low:
            avg_h = np.mean(high)
            avg_l = np.mean(low)
            wr_h = sum(1 for r in high if r > 0) / len(high) * 100
            wr_l = sum(1 for r in low if r > 0) / len(low) * 100
            diff = avg_h - avg_l
            verdict = "✅正贡献" if diff > 0.3 else ("⚠️弱" if diff > -0.3 else "❌负贡献")
            print(f"  {name:>10} {avg_h:>+9.2f}%  {avg_l:>+9.2f}%  {diff:>+7.2f}%  {wr_h:>8.1f}%  {wr_l:>8.1f}%  {verdict}")
    
    # ===== 4. 战法独立胜率 =====
    print("\n### 4. 战法独立胜率（T+1收益）")
    
    zt_picks = [p for p in all_picks if p.get('is_zt') and p.get('ret_1d') is not None]
    non_zt_picks = [p for p in all_picks if not p.get('is_zt') and p.get('ret_1d') is not None]
    
    strategies = [
        ('涨停票', zt_picks),
        ('非涨停票', non_zt_picks),
    ]
    
    # 按Mistery高分/低分
    mistery_high = [p for p in all_picks if p.get('mistery', 0) >= 10 and p.get('ret_1d') is not None]
    mistery_low = [p for p in all_picks if p.get('mistery', 0) < 5 and p.get('ret_1d') is not None]
    strategies.append(('Mistery高分(≥10)', mistery_high))
    strategies.append(('Mistery低分(<5)', mistery_low))
    
    # 按安全边际
    safe_high = [p for p in all_picks if p.get('d4', 0) >= 10 and p.get('ret_1d') is not None]
    safe_low = [p for p in all_picks if p.get('d4', 0) < 5 and p.get('ret_1d') is not None]
    strategies.append(('安全边际高(≥10)', safe_high))
    strategies.append(('安全边际低(<5)', safe_low))
    
    # 按主线热点
    hot_high = [p for p in all_picks if p.get('d2', 0) >= 12 and p.get('ret_1d') is not None]
    hot_low = [p for p in all_picks if p.get('d2', 0) < 5 and p.get('ret_1d') is not None]
    strategies.append(('主线热点高(≥12)', hot_high))
    strategies.append(('主线热点低(<5)', hot_low))
    
    print(f"{'战法':>18} {'样本':>6} {'均收益':>8} {'胜率':>8} {'盈亏比':>8} {'最佳':>8} {'最差':>8}")
    print("-" * 65)
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
    
    # ===== 5. 冲突分析 =====
    print("\n### 5. 战法冲突检测")
    
    # 涨停票 vs 非涨停票 在不同市场情绪下
    print(f"\n  涨停票 vs 非涨停票:")
    if zt_picks:
        zt_rets = [p['ret_1d'] for p in zt_picks]
        print(f"    涨停票: {len(zt_rets)}只 均收益{np.mean(zt_rets):+.2f}% 胜率{sum(1 for r in zt_rets if r>0)/len(zt_rets)*100:.0f}%")
    if non_zt_picks:
        nzt_rets = [p['ret_1d'] for p in non_zt_picks]
        print(f"    非涨停: {len(nzt_rets)}只 均收益{np.mean(nzt_rets):+.2f}% 胜率{sum(1 for r in nzt_rets if r>0)/len(nzt_rets)*100:.0f}%")
    
    # ===== 6. 每日收益曲线 =====
    if daily_summary:
        print("\n### 6. 每日收益明细")
        print(f"{'日期':>10} {'评分数':>6} {'TOP-N':>6} {'T+1均收益':>10} {'胜率':>8} {'最佳':>8} {'最差':>8}")
        print("-" * 65)
        for ds in daily_summary:
            print(f"  {ds['date']}  {ds['total_scored']:>5}  {ds['top_n']:>5}  "
                  f"{ds['avg_ret_1d']:>+9.2f}%  {ds['win_rate_1d']:>6.1f}%  "
                  f"{ds['best']:>+7.2f}%  {ds['worst']:>+7.2f}%")
        
        # 累计收益
        cum_ret = 0
        win_days = 0
        for ds in daily_summary:
            cum_ret += ds['avg_ret_1d']
            if ds['avg_ret_1d'] > 0: win_days += 1
        
        print(f"\n  累计收益: {cum_ret:+.2f}%")
        print(f"  日胜率: {win_days}/{len(daily_summary)} = {win_days/len(daily_summary)*100:.0f}%")
        print(f"  日均收益: {cum_ret/len(daily_summary):+.3f}%")


def save_results(all_picks, daily_summary, hold_days):
    """保存回测结果"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 保存详细数据
    if all_picks:
        df = pd.DataFrame(all_picks)
        out_path = os.path.join(RESULTS_DIR, f"backtest_detail_{timestamp}.csv")
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n💾 详细数据已保存: {out_path}")
    
    # 保存每日汇总
    if daily_summary:
        df_daily = pd.DataFrame(daily_summary)
        out_path2 = os.path.join(RESULTS_DIR, f"backtest_daily_{timestamp}.csv")
        df_daily.to_csv(out_path2, index=False, encoding='utf-8-sig')
        print(f"💾 每日汇总已保存: {out_path2}")


def main():
    parser = argparse.ArgumentParser(description='选股系统历史回测')
    parser.add_argument('--start', type=str, default=None, help='回测起始日期(YYYYMMDD)')
    parser.add_argument('--top', type=int, default=20, help='每天取TOP N只(默认20)')
    parser.add_argument('--hold', type=str, default='1,2,3,5', help='持有天数(逗号分隔,默认1,2,3,5)')
    parser.add_argument('--save', action='store_true', help='保存结果到CSV')
    args = parser.parse_args()
    
    hold_days = [int(x) for x in args.hold.split(',')]
    
    print("=" * 100)
    print("📈 选股系统历史回测 v1.0")
    print("=" * 100)
    print(f"数据目录: {SNAPSHOT_DIR}")
    print(f"参数: TOP{args.top} | 持有期{hold_days}天 | 起始{args.start or '自动'}")
    
    # 运行回测
    all_picks, daily_summary, hold_days = run_backtest(
        start_date=args.start,
        top_n=args.top,
        hold_days=hold_days,
    )
    
    # 分析结果
    analyze_results(all_picks, daily_summary, hold_days)
    
    # 保存
    if args.save:
        save_results(all_picks, daily_summary, hold_days)
    else:
        print(f"\n💡 添加 --save 参数可保存详细结果到CSV")


if __name__ == '__main__':
    main()
