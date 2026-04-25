#!/usr/bin/env python3
"""
aiTrader v3.7 全因子测试（5年数据，牛熊分化分析）
=================================================
从aiTrader的因子库中提取可用OHLCV计算的因子，
在Claw的TOP30池子上测试5年IC/IR表现和牛熊差异
"""
import pandas as pd
import numpy as np
import os, time, warnings, sys
warnings.filterwarnings('ignore')

print("="*80)
print("📊 aiTrader v3.7 全因子测试（5年数据 2021-2026）")
print("="*80)

# ============================================================
# 1. 加载数据
# ============================================================
print("\n[1/4] 加载数据...")
t0 = time.time()

# 加载detail数据（含收益率）
detail = pd.read_csv('backtest_results/backtest_v2_detail_20260420_180427.csv')
detail['date'] = detail['date'].astype(str)
all_codes = set(detail['code'].unique())
all_dates = sorted(detail['date'].unique())

# 加载全量snapshot数据（从2020年10月开始，给60天预热期）
snapshot_dir = '/Users/ecustkiller/stock_data/daily_snapshot/'
snapshot_files = sorted([f for f in os.listdir(snapshot_dir) if f.endswith('.parquet') and f != 'stock_basic.parquet'])

start_idx = 0
for i, f in enumerate(snapshot_files):
    if f >= '20201001.parquet':
        start_idx = i
        break

COLS_NEEDED = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 
               'turnover_rate', 'volume_ratio', 'total_mv', 'circ_mv', 'net_mf_amount']

dfs = []
for f in snapshot_files[start_idx:]:
    snap = pd.read_parquet(os.path.join(snapshot_dir, f))
    snap = snap[snap['ts_code'].isin(all_codes)]
    # 安全获取列
    cols_avail = [c for c in COLS_NEEDED if c in snap.columns]
    sub = snap[cols_avail].copy()
    for c in COLS_NEEDED:
        if c not in sub.columns:
            sub[c] = np.nan
    dfs.append(sub[COLS_NEEDED])

panel = pd.concat(dfs, ignore_index=True)
panel = panel.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
panel['trade_date'] = panel['trade_date'].astype(str)

t1 = time.time()
print(f"  数据加载完成: {len(panel)}行, {panel['ts_code'].nunique()}只股票, {panel['trade_date'].nunique()}天 ({t1-t0:.1f}秒)")

# ============================================================
# 2. 计算aiTrader因子（逐股票计算，需要时间序列）
# ============================================================
print("\n[2/4] 计算aiTrader因子...")
t0 = time.time()

def calc_factors_for_stock(stock_df):
    """为单只股票计算所有因子（输入已按日期排序）"""
    c = stock_df['close'].values.astype(float)
    o = stock_df['open'].values.astype(float)
    h = stock_df['high'].values.astype(float)
    l = stock_df['low'].values.astype(float)
    v = stock_df['vol'].values.astype(float)
    amt = stock_df['amount'].values.astype(float)
    tr = stock_df['turnover_rate'].values.astype(float)
    mv = stock_df['total_mv'].values.astype(float)
    mf = stock_df['net_mf_amount'].values.astype(float)
    n = len(c)
    
    results = {}
    
    # --- 动量/趋势类 ---
    for period in [5, 10, 20]:
        ma = pd.Series(c).rolling(period).mean().values
        results[f'ma{period}_bias'] = (c[-1] / ma[-1] - 1) if not np.isnan(ma[-1]) and ma[-1] > 0 else np.nan
    
    for period in [12, 26]:
        ema = pd.Series(c).ewm(span=period, adjust=False).mean().values
        results[f'ema{period}_bias'] = (c[-1] / ema[-1] - 1) if not np.isnan(ema[-1]) and ema[-1] > 0 else np.nan
    
    # MACD柱状图
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean().values
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
    macd_hist = dif[-1] - dea[-1] if not np.isnan(dea[-1]) else np.nan
    results['macd_hist'] = macd_hist / c[-1] if c[-1] > 0 and macd_hist is not None else np.nan
    
    # 突破位置 (20日)
    if n >= 20:
        high20 = np.max(h[-20:])
        low20 = np.min(l[-20:])
        rng = high20 - low20
        results['breakout_20'] = (c[-1] - low20) / rng if rng > 0 else np.nan
    else:
        results['breakout_20'] = np.nan
    
    # 动量加速度
    if n >= 31:
        roc10 = c[-1] / c[-11] - 1 if c[-11] > 0 else np.nan
        roc30 = c[-1] / c[-31] - 1 if c[-31] > 0 else np.nan
        if roc10 is not None and roc30 is not None and not np.isnan(roc10) and not np.isnan(roc30):
            results['momentum_accel'] = roc10 - roc30
        else:
            results['momentum_accel'] = np.nan
    else:
        results['momentum_accel'] = np.nan
    
    # --- 量价背离类（华泰核心因子）---
    if n >= 10:
        try:
            corr_ov = np.corrcoef(o[-10:], v[-10:])[0, 1]
            results['vol_price_corr10'] = -corr_ov if not np.isnan(corr_ov) else np.nan
        except:
            results['vol_price_corr10'] = np.nan
    else:
        results['vol_price_corr10'] = np.nan
    
    if n >= 20:
        try:
            corr_cv = np.corrcoef(c[-20:], v[-20:])[0, 1]
            results['vol_price_corr20'] = -corr_cv if not np.isnan(corr_cv) else np.nan
        except:
            results['vol_price_corr20'] = np.nan
    else:
        results['vol_price_corr20'] = np.nan
    
    # --- 波动率类 ---
    if n >= 21:
        log_c = np.log(np.maximum(c[-21:], 1e-8))
        rets = np.diff(log_c)
        results['realized_vol20'] = np.std(rets) * np.sqrt(250)
    else:
        results['realized_vol20'] = np.nan
    
    # ATR比率 (14日)
    if n >= 15:
        tr_arr = np.maximum(h[-14:] - l[-14:], 
                           np.maximum(np.abs(h[-14:] - c[-15:-1]), np.abs(l[-14:] - c[-15:-1])))
        atr14 = np.mean(tr_arr)
        results['atr_ratio'] = atr14 / c[-1] if c[-1] > 0 else np.nan
    else:
        results['atr_ratio'] = np.nan
    
    # 波动率变化
    if n >= 26:
        log_c = np.log(np.maximum(c[-26:], 1e-8))
        vol_recent = np.std(np.diff(log_c[-6:]))
        vol_prev = np.std(np.diff(log_c[:-5]))
        results['vol_change'] = vol_recent / vol_prev if vol_prev > 1e-8 else np.nan
    else:
        results['vol_change'] = np.nan
    
    # --- 成交量类 ---
    if n >= 20:
        vol5 = np.mean(v[-5:])
        vol20 = np.mean(v[-20:])
        results['vol_ratio_5_20'] = vol5 / vol20 if vol20 > 0 else np.nan
    else:
        results['vol_ratio_5_20'] = np.nan
    
    # OBV趋势
    if n >= 21:
        price_chg = np.diff(c[-21:])
        obv = np.cumsum(np.where(price_chg > 0, v[-20:], np.where(price_chg < 0, -v[-20:], 0)))
        x = np.arange(20)
        try:
            slope = np.polyfit(x, obv, 1)[0]
            results['obv_slope'] = slope / np.mean(v[-20:]) if np.mean(v[-20:]) > 0 else np.nan
        except:
            results['obv_slope'] = np.nan
    else:
        results['obv_slope'] = np.nan
    
    # MFI (14日)
    if n >= 15:
        tp = (h[-14:] + l[-14:] + c[-14:]) / 3
        tp_diff = np.diff(np.concatenate([[tp[0]], tp]))
        mf_pos = np.where(tp_diff > 0, tp * v[-14:], 0)
        mf_neg = np.where(tp_diff <= 0, tp * v[-14:], 0)
        mf_pos_sum = np.sum(mf_pos)
        mf_neg_sum = np.sum(mf_neg)
        results['mfi_14'] = 100 - 100 / (1 + mf_pos_sum / mf_neg_sum) if mf_neg_sum > 0 else 50
    else:
        results['mfi_14'] = np.nan
    
    # --- 价格形态类 ---
    results['close_position'] = (c[-1] - l[-1]) / (h[-1] - l[-1]) if h[-1] > l[-1] else 0.5
    
    if n >= 2:
        results['gap_strength'] = o[-1] / c[-2] - 1 if c[-2] > 0 else np.nan
    else:
        results['gap_strength'] = np.nan
    
    results['upper_shadow'] = (h[-1] - max(o[-1], c[-1])) / (h[-1] - l[-1]) if h[-1] > l[-1] else 0
    results['lower_shadow'] = (min(o[-1], c[-1]) - l[-1]) / (h[-1] - l[-1]) if h[-1] > l[-1] else 0
    
    # --- 资金流向类 ---
    if not np.isnan(mf[-1]) and amt[-1] > 0:
        results['mf_ratio'] = mf[-1] / amt[-1]
    else:
        results['mf_ratio'] = np.nan
    
    if n >= 5 and not np.any(np.isnan(mf[-5:])):
        results['mf_5d_sum'] = np.sum(mf[-5:]) / np.sum(amt[-5:]) if np.sum(amt[-5:]) > 0 else np.nan
    else:
        results['mf_5d_sum'] = np.nan
    
    # --- 反转/超跌类 ---
    if n >= 15:
        deltas = np.diff(c[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        results['rsi_14'] = 100 - 100 / (1 + rs)
    else:
        results['rsi_14'] = np.nan
    
    if n >= 14:
        high14 = np.max(h[-14:])
        low14 = np.min(l[-14:])
        results['willr_14'] = (high14 - c[-1]) / (high14 - low14) * (-100) if high14 > low14 else -50
    else:
        results['willr_14'] = np.nan
    
    # 下跌缓冲
    if n >= 10:
        max_price_10 = np.max(h[-10:])
        results['dn_buffer'] = (c[-1] - max_price_10) / max_price_10 if max_price_10 > 0 else np.nan
    else:
        results['dn_buffer'] = np.nan
    
    # --- 换手率类 ---
    if n >= 20 and not np.any(np.isnan(tr[-20:])):
        tr5 = np.mean(tr[-5:])
        tr20 = np.mean(tr[-20:])
        results['tr_change'] = tr5 / tr20 if tr20 > 0 else np.nan
        results['tr_relative'] = tr[-1] / tr20 if tr20 > 0 else np.nan
    else:
        results['tr_change'] = np.nan
        results['tr_relative'] = np.nan
    
    # --- 市值动量 ---
    if n >= 20 and mv[-20] > 0:
        results['mv_momentum'] = mv[-1] / mv[-20] - 1
    else:
        results['mv_momentum'] = np.nan
    
    # --- 高级因子 ---
    # 量价同步性
    if n >= 11:
        price_dir = np.sign(np.diff(c[-11:]))
        vol_dir = np.sign(np.diff(v[-11:]))
        results['pv_sync'] = np.mean(price_dir * vol_dir)
    else:
        results['pv_sync'] = np.nan
    
    # 收益偏度/峰度
    if n >= 21:
        rets20 = np.diff(c[-21:]) / np.maximum(c[-21:-1], 1e-8)
        results['ret_skew'] = float(pd.Series(rets20).skew())
        results['ret_kurt'] = float(pd.Series(rets20).kurtosis())
    else:
        results['ret_skew'] = np.nan
        results['ret_kurt'] = np.nan
    
    # 日内振幅均值 (5日)
    if n >= 5:
        intraday_range = (h[-5:] - l[-5:]) / np.maximum(c[-5:], 1e-8)
        results['avg_range_5'] = np.mean(intraday_range)
    else:
        results['avg_range_5'] = np.nan
    
    # 价格效率
    if n >= 10:
        net_move = abs(c[-1] - c[-10])
        total_path = np.sum(np.abs(np.diff(c[-10:])))
        results['price_efficiency'] = net_move / total_path if total_path > 0 else np.nan
    else:
        results['price_efficiency'] = np.nan
    
    return results

# 构建股票历史数据字典
print("  构建股票历史面板...")
stock_histories = {}
for code, grp in panel.groupby('ts_code'):
    stock_histories[code] = grp.sort_values('trade_date').reset_index(drop=True)

# 为detail中的每条记录计算因子
print("  逐条计算因子...")
factor_results = []
processed = 0
total = len(detail)

for idx, row in detail.iterrows():
    code = row['code']
    date = str(row['date'])
    
    if code not in stock_histories:
        factor_results.append({})
        continue
    
    hist = stock_histories[code]
    mask = hist['trade_date'] <= date
    hist_before = hist[mask]
    
    if len(hist_before) < 20:
        factor_results.append({})
        continue
    
    hist_window = hist_before.tail(60)
    factors = calc_factors_for_stock(hist_window)
    factor_results.append(factors)
    
    processed += 1
    if processed % 5000 == 0:
        elapsed = time.time() - t0
        pct = processed / total * 100
        eta = elapsed / processed * (total - processed)
        print(f"  进度: {processed}/{total} ({pct:.1f}%) 耗时:{elapsed:.0f}s ETA:{eta:.0f}s")

t1 = time.time()
print(f"  因子计算完成: {processed}条有效记录 ({t1-t0:.1f}秒)")

# 合并因子到detail
factor_df = pd.DataFrame(factor_results)
detail_with_factors = pd.concat([detail.reset_index(drop=True), factor_df.reset_index(drop=True)], axis=1)

# 保存中间结果
detail_with_factors.to_csv('backtest_results/aitrader_factors_5year.csv', index=False)
print(f"  已保存到 backtest_results/aitrader_factors_5year.csv")

# ============================================================
# 3. 计算IC/IR（全量 + 牛熊分化）
# ============================================================
print("\n[3/4] 计算因子IC/IR...")

# 市场状态判断
daily_pool_ret = detail.groupby('date')['ret_1d'].mean()
ma20 = daily_pool_ret.rolling(20).mean()
bull_dates = set(ma20[ma20 > 0.2].index.astype(str))
bear_dates = set(ma20[ma20 < -0.1].index.astype(str))

new_factors = [col for col in factor_df.columns if col in detail_with_factors.columns]
print(f"  新因子数: {len(new_factors)}")
print(f"  牛市天数: {len(bull_dates)}, 熊市天数: {len(bear_dates)}")

def calc_ic_stats(df, factor_col, dates_filter=None):
    """计算因子IC统计"""
    daily_ics = []
    for date, grp in df.groupby('date'):
        if dates_filter and str(date) not in dates_filter:
            continue
        fv = grp[factor_col].values.astype(float)
        ret = grp['ret_1d'].values.astype(float)
        valid = ~(np.isnan(fv) | np.isnan(ret))
        if valid.sum() < 5:
            continue
        try:
            ic = np.corrcoef(fv[valid], ret[valid])[0, 1]
            if not np.isnan(ic):
                daily_ics.append(ic)
        except:
            pass
    if not daily_ics:
        return {'mean_ic': 0, 'ir': 0, 'ic_pos': 0, 'n': 0}
    ic_arr = np.array(daily_ics)
    return {
        'mean_ic': ic_arr.mean(),
        'ir': ic_arr.mean() / ic_arr.std() if ic_arr.std() > 0 else 0,
        'ic_pos': (ic_arr > 0).mean(),
        'n': len(ic_arr)
    }

# 计算所有新因子的IC
results_all = {}
results_bull = {}
results_bear = {}

for f in new_factors:
    if detail_with_factors[f].notna().sum() < 1000:
        continue
    results_all[f] = calc_ic_stats(detail_with_factors, f)
    results_bull[f] = calc_ic_stats(detail_with_factors, f, bull_dates)
    results_bear[f] = calc_ic_stats(detail_with_factors, f, bear_dates)

# ============================================================
# 4. 输出结果
# ============================================================
print("\n[4/4] 输出结果...")
print()
print("="*100)
print("📊 aiTrader v3.7 因子5年IC/IR测试结果（2021-2026, TOP30池）")
print("="*100)
print(f"\n{'因子':<20} {'全量IR':>8} {'全量IC':>8} {'IC>0%':>7} {'牛市IR':>8} {'熊市IR':>8} {'牛熊差':>8} {'分类':>8}")
print("-"*95)

# 按全量IR绝对值排序
sorted_factors = sorted(results_all.keys(), key=lambda x: -abs(results_all[x]['ir']))

for f in sorted_factors:
    a = results_all[f]
    b = results_bull.get(f, {'ir': 0})
    br = results_bear.get(f, {'ir': 0})
    diff = b['ir'] - br['ir']
    
    if abs(a['ir']) < 0.02:
        tag = '❌无效'
    elif diff > 0.08:
        tag = '📈牛市'
    elif diff < -0.08:
        tag = '📉熊市'
    else:
        tag = '⚖️全天候'
    
    print(f"  {f:<18} {a['ir']:>+7.3f} {a['mean_ic']:>+7.4f} {a['ic_pos']:>6.1%} {b['ir']:>+7.3f} {br['ir']:>+7.3f} {diff:>+7.3f}  {tag}")

# 总结
print()
print("="*100)
print("📋 分类总结")
print("="*100)

effective = [(f, results_all[f]) for f in sorted_factors if abs(results_all[f]['ir']) >= 0.05]
print(f"\n✅ 有效因子（|IR|>=0.05）: {len(effective)}个")
for f, stats in effective:
    b_ir = results_bull.get(f, {'ir': 0})['ir']
    br_ir = results_bear.get(f, {'ir': 0})['ir']
    diff = b_ir - br_ir
    print(f"   {f:<20} 全量IR={stats['ir']:+.3f}  牛市IR={b_ir:+.3f}  熊市IR={br_ir:+.3f}  牛熊差={diff:+.3f}")

print(f"\n🏆 TOP5 最强因子:")
for i, (f, stats) in enumerate(effective[:5]):
    print(f"   {i+1}. {f:<18} IR={stats['ir']:+.3f}  IC={stats['mean_ic']:+.4f}  IC>0={stats['ic_pos']:.1%}")

# ============================================================
# 5. 与现有因子对比 + 组合测试
# ============================================================
print()
print("="*100)
print("🔬 与现有Claw因子对比")
print("="*100)

# 现有因子的IR（从之前的分析中已知）
existing_factors = {
    'r5(反向)': 0.134, 'd4': 0.106, 'r10(反向)': 0.106,
    'wr3': 0.071, 'd9': 0.066, 'mv(反向)': 0.025,
}

print("\n现有Claw因子IR:")
for f, ir in sorted(existing_factors.items(), key=lambda x: -x[1]):
    print(f"   {f:<15} IR={ir:+.3f}")

print("\n新aiTrader因子中超越现有最强因子(r5 IR=0.134)的:")
for f, stats in effective:
    if abs(stats['ir']) > 0.134:
        print(f"   🆕 {f:<18} IR={stats['ir']:+.3f} (超越r5!)")

print("\n新aiTrader因子中可补充现有体系的（与现有因子低相关）:")
# 计算新因子与现有因子的相关性
if len(effective) > 0:
    existing_cols = ['r5', 'd4', 'r10', 'wr3', 'd9', 'mv']
    for f, stats in effective[:10]:
        if f in detail_with_factors.columns:
            corrs = []
            for ec in existing_cols:
                if ec in detail_with_factors.columns:
                    valid = detail_with_factors[[f, ec]].dropna()
                    if len(valid) > 100:
                        corr = valid[f].corr(valid[ec])
                        corrs.append(abs(corr))
            if corrs:
                max_corr = max(corrs)
                avg_corr = np.mean(corrs)
                if max_corr < 0.5:
                    print(f"   🆕 {f:<18} IR={stats['ir']:+.3f}  与现有因子最大相关={max_corr:.2f} (低相关,可补充!)")

print("\n✅ 测试完成!")
