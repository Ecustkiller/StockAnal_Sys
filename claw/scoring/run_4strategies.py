#!/usr/bin/env python3
"""
4策略统一选股脚本 — 重点突出策略03/04的TOP10
================================================================
策略03（aiTrader因子增强）：5年回测累计+461.8%，Sharpe 0.97，Calmar 0.71 — 🥇收益最强
策略04（风控增强+动态仓位）：累计+341.4%，Sharpe 1.31，回撤仅-30.3% — 🥇风险调整最优
策略01（严格精选TOP5）：累计+391%，年化+113%，回撤-41.7% — 参考
策略02（主板严格精选TOP5）：累计+311%，Sharpe 1.74，回撤-32.8% — 参考
"""
import os, sys, json, glob
from datetime import datetime
import pandas as pd
import numpy as np

sys.path.insert(0, '/Users/ecustkiller/WorkBuddy/Claw')

from claw.strategies.strategy_01_strict_elite import pick_top5 as s1_top5
from claw.strategies.strategy_02_mainboard_elite import pick_top5 as s2_top5
from claw.strategies.strategy_03_optimized import (
    pick_top5 as s3_top5, pick_top10 as s3_top10, calc_aitrader_factors
)
from claw.strategies.strategy_04_risk_managed import (
    select_risk_managed, PositionManager,
)

STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'
TOP20_JSON = '/Users/ecustkiller/WorkBuddy/Claw/claw/scoring/综合评分TOP20.json'

def log(msg):
    print(msg, flush=True)

def load_hist_ohlcv(code, n_days=30):
    code_raw = code.split('.')[0]
    fs = glob.glob(os.path.join(STOCK_DATA_DIR, f'{code_raw}_*.csv'))
    if not fs:
        return None
    try:
        # on_bad_lines='skip' 兼容列数不一致的旧记录
        df = pd.read_csv(fs[0], on_bad_lines='skip', low_memory=False)
        if 'trade_date' in df.columns:
            df = df.rename(columns={'trade_date': 'date'})
        elif df.columns[0] != 'date':
            df = df.rename(columns={df.columns[0]: 'date'})
        df['date'] = df['date'].astype(str).str.replace('-','').str.replace('.0','', regex=False)
        df = df[df['date'].str.match(r'^\d{8}$', na=False)]  # 过滤非法日期
        df = df.sort_values('date').tail(n_days).copy()
        # 列名兼容：volume → vol
        if 'volume' in df.columns and 'vol' not in df.columns:
            df = df.rename(columns={'volume': 'vol'})
        for col in ['open','high','low','close','vol']:
            if col not in df.columns:
                return None
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # total_mv 映射：优先 total_market_cap，其次 circ_market_cap，再fallback
        if 'total_mv' not in df.columns:
            if 'total_market_cap' in df.columns:
                df['total_mv'] = pd.to_numeric(df['total_market_cap'], errors='coerce')
            elif 'circ_market_cap' in df.columns:
                df['total_mv'] = pd.to_numeric(df['circ_market_cap'], errors='coerce')
            else:
                df['total_mv'] = df['close'] * df['vol']
        df = df.dropna(subset=['open','high','low','close','vol'])
        return df
    except Exception as e:
        return None

def enrich_with_aitrader_factors(top_df):
    factor_rows = []
    for _, row in top_df.iterrows():
        code = row['code']
        hist = load_hist_ohlcv(code, n_days=30)
        if hist is None or len(hist) < 20:
            factors = {k: np.nan for k in ['close_position','upper_shadow','ret_skew',
                      'willr_14','mv_momentum','breakout_20','dn_buffer']}
        else:
            factors = calc_aitrader_factors(hist)
        factor_rows.append(factors)
    factor_df = pd.DataFrame(factor_rows, index=top_df.index)
    return pd.concat([top_df, factor_df], axis=1)

def normalize_for_strategies(top_list):
    df = pd.DataFrame(top_list)
    df['code'] = df['code'].str.split('.').str[0]
    if 'industry' not in df.columns:
        df['industry'] = df['ind']
    return df

def show_table(sel_df, title, show_factors=False):
    if len(sel_df) == 0:
        log(f"【{title}】无候选"); return
    log(f"\n🎯 【{title}】共 {len(sel_df)} 只")
    log('-'*110)
    if show_factors:
        header = f"{'#':>2}  {'股票':<20} {'行业':<10} {'收盘':>8} {'总分':>4} {'5日%':>6} {'净流亿':>7}  {'WR标签':<25}  因子分"
    else:
        header = f"{'#':>2}  {'股票':<20} {'行业':<10} {'收盘':>8} {'总分':>4} {'5日%':>6} {'净流亿':>7}  {'WR标签':<25}"
    log(header)
    log('-'*110)
    for i, (_, r) in enumerate(sel_df.iterrows(), 1):
        code = r.get('code','-'); name = r.get('name','-')
        ind = r.get('industry', r.get('ind','-'))
        close = r.get('close', 0); total = r.get('total', 0)
        r5 = r.get('r5', 0); nb = r.get('nb_yi', 0) or 0
        wr_tags = str(r.get('wr_tags', ''))[:25]
        name_code = f"{name}({code})"
        if show_factors:
            es = r.get('elite_score', 0)
            log(f"{i:>2}. {name_code:<20} {ind:<10} {close:>8.2f} {total:>4.0f} {r5:>+6.1f} {nb:>+7.2f}  {wr_tags:<25}  {es:.3f}")
        else:
            log(f"{i:>2}. {name_code:<20} {ind:<10} {close:>8.2f} {total:>4.0f} {r5:>+6.1f} {nb:>+7.2f}  {wr_tags:<25}")

def main():
    log(f"📊 4策略选股 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log('='*110)
    log("🥇 主推荐: 策略03(aiTrader因子) / 策略04(风控+仓位)")
    log("   参考: 策略01(全市场精选) / 策略02(主板精选)")
    log('='*110)
    
    with open(TOP20_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    log(f"📥 载入综合评分TOP20: {len(data)} 只")
    
    df = normalize_for_strategies(data)
    
    # 计算aiTrader因子（策略03/04都要用）
    log("🔧 计算aiTrader 7因子...")
    df_with_factors = enrich_with_aitrader_factors(df)
    log(f"   完成，有效因子股数: {(df_with_factors['close_position'].notna()).sum()}/{len(df_with_factors)}")
    
    # ===================================================================
    # 🥇 主推荐 1：策略03 aiTrader因子增强 TOP10
    # ===================================================================
    log('\n' + '='*110)
    log("🥇 主推荐 1️⃣：策略03（aiTrader 7因子增强） — 5年回测累计+461.8%，Sharpe 0.97")
    log("   过滤：非涨停 + r5<10% + (WR2≥3|M≥10) + net_risk≤2")
    log("   打分：close_position↓ + upper_shadow↑ + ret_skew↓ + willr_14↓ + mv_mom↓ + breakout_20↓ + dn_buffer↓")
    log('='*110)
    sel3_top10 = s3_top10(df_with_factors)
    show_table(sel3_top10, '策略03 aiTrader因子增强 TOP10', show_factors=True)
    sel3_top5 = s3_top5(df_with_factors)
    log('')
    show_table(sel3_top5, '策略03 aiTrader因子增强 TOP5（精选核心）', show_factors=True)
    
    # ===================================================================
    # 🥇 主推荐 2：策略04 风控增强 TOP10
    # ===================================================================
    log('\n' + '='*110)
    log("🥇 主推荐 2️⃣：策略04（风控增强+动态仓位） — 累计+341%，Sharpe 1.31，最大回撤仅-30.3%")
    log("   选股逻辑同策略03；额外叠加【5信号仓位管理】")
    log('='*110)
    # 预热/加载 PositionManager 状态
    log("🔥 加载 PositionManager 状态...")
    try:
        from pm_daily import load_or_preheat
        pm, pm_info = load_or_preheat()
        log(f"   ✓ 来源: {pm_info['source']}  最后日期: {pm_info['last_date']}  累计天数: {pm_info['n_days']}")
        log(f"   ✓ 上次保存: {pm_info['saved_at']}")
    except Exception as e:
        log(f"   ⚠ 加载/预热失败（{e}），尝试旧方式预热")
        from preheat_position_manager import build_preheated_pm
        try:
            pm, _ = build_preheated_pm(verbose=False)
            log(f"   ✓ 备用预热完成，历史交易日 {len(pm.returns_history)} 天")
        except Exception as e2:
            log(f"   ⚠ 预热彻底失败（{e2}），回退到全新 PM")
            pm = PositionManager(target_annual_vol=10.0)
    
    # 仓位信号分解展示
    n_hist = len(pm.returns_history)
    dd_now = (pm.peak - pm.nav) / pm.peak * 100 if pm.peak > 0 else 0
    log(f"\n   📊 PM 状态快照:")
    log(f"     历史日数:      {n_hist}")
    log(f"     当前 NAV:      {pm.nav:.4f}  (峰值 {pm.peak:.4f})")
    log(f"     当前回撤:      {dd_now:.2f}%")
    log(f"     连续亏损天数:  {pm.loss_streak}")
    if n_hist >= 20:
        recent20 = np.array(pm.returns_history[-20:])
        log(f"     近20日波动率:  {recent20.std():.3f}% (年化 {recent20.std()*np.sqrt(250):.1f}%)")
    if n_hist >= 10:
        win10 = (np.array(pm.returns_history[-10:]) > 0).sum() / 10 * 100
        log(f"     近10日胜率:    {win10:.0f}%")
    
    sel4_top10, pos10 = select_risk_managed(df_with_factors, n=10, max_per_ind=3, position_manager=pm)
    log('')
    show_table(sel4_top10, f'策略04 风控增强 TOP10  [建议仓位 {pos10*100:.1f}%]', show_factors=True)
    sel4_top5, pos5 = select_risk_managed(df_with_factors, n=5, max_per_ind=3, position_manager=pm)
    log('')
    show_table(sel4_top5, f'策略04 风控增强 TOP5  [建议仓位 {pos5*100:.1f}%]', show_factors=True)
    
    # 计算并展示5信号分解
    target_daily_vol = pm.target_daily_vol
    sig = {}
    if pm.loss_streak >= 3: sig['连亏控制'] = (0.2, 0.25)
    elif pm.loss_streak >= 2: sig['连亏控制'] = (0.4, 0.25)
    elif pm.loss_streak >= 1: sig['连亏控制'] = (0.6, 0.25)
    else: sig['连亏控制'] = (1.0, 0.25)
    if n_hist >= 20:
        v = np.array(pm.returns_history[-20:]).std()
        sig['波动率目标'] = (min(1.0, target_daily_vol/v) if v>0 else 1.0, 0.25)
    if dd_now > 18: sig['回撤控制'] = (0.2, 0.25)
    elif dd_now > 10: sig['回撤控制'] = (0.4, 0.25)
    elif dd_now > 5: sig['回撤控制'] = (0.7, 0.25)
    else: sig['回撤控制'] = (1.0, 0.25)
    if len(pm.navs) >= 20:
        navs = np.array(pm.navs)
        ma5, ma20 = navs[-5:].mean(), navs[-20:].mean()
        if ma5 > ma20 * 1.02: sig['净值动量'] = (1.0, 0.15)
        elif ma5 > ma20: sig['净值动量'] = (0.7, 0.15)
        else: sig['净值动量'] = (0.4, 0.15)
    if n_hist >= 10:
        wr = (np.array(pm.returns_history[-10:]) > 0).sum() / 10
        if wr < 0.3: sig['近期胜率'] = (0.2, 0.10)
        elif wr < 0.4: sig['近期胜率'] = (0.5, 0.10)
        elif wr > 0.6: sig['近期胜率'] = (1.0, 0.10)
        else: sig['近期胜率'] = (0.7, 0.10)
    
    log(f"\n   🔍 仓位信号分解:")
    log(f"     {'信号':<12s} {'权重':>6s} {'信号值':>8s}  可视化")
    for name, (v, w) in sig.items():
        bar = '█'*int(v*20)
        log(f"     {name:<12s} {w*100:>5.0f}%  {v*100:>7.1f}%  {bar}")
    log(f"   → 加权融合: {pos10*100:.1f}%  （已限制在 [10%, 100%] 范围）")
    # 动态提示 PM 数据新鲜度
    try:
        from pm_daily import load_state
        _state = load_state()
        if _state and _state.get('last_date'):
            log(f"\n   ⚠ PM 数据止于 {_state['last_date']}（上次保存 {_state.get('saved_at','N/A')}）")
            log(f"   ⚠ 如需更新：python3 claw/scoring/pm_daily.py --feed-from-detail <最新detail.csv>")
            log(f"   ⚠ 或每日盘后：python3 claw/scoring/pm_daily.py --feed-daily YYYYMMDD <当日策略03收益%>")
    except Exception:
        pass
    
    # ===================================================================
    # 📎 参考：策略01 / 策略02 TOP5
    # ===================================================================
    log('\n' + '='*110)
    log("📎 参考策略（胜率/均衡性视角）")
    log('='*110)
    sel1 = s1_top5(df)
    show_table(sel1, '策略01 严格精选 TOP5（全市场）— 累计+391%')
    log('')
    sel2 = s2_top5(df)
    show_table(sel2, '策略02 主板严格精选 TOP5（仅00/60开头）— Sharpe 1.74，回撤仅-32.8%')
    
    # ===================================================================
    # 共识分析（仅参考，不做推荐依据）
    # ===================================================================
    log('\n' + '='*110)
    log("🔍 共识参考（⚠ 回测显示：共识票数与收益无正相关，仅作参考）")
    log('='*110)
    
    codes_sets = {
        '03-TOP10': set(sel3_top10['code']) if len(sel3_top10)>0 else set(),
        '04-TOP10': set(sel4_top10['code']) if len(sel4_top10)>0 else set(),
        '01-TOP5':  set(sel1['code']) if len(sel1)>0 else set(),
        '02-TOP5':  set(sel2['code']) if len(sel2)>0 else set(),
    }
    from collections import Counter
    hit_counter = Counter()
    for s in codes_sets.values():
        for c in s: hit_counter[c] += 1
    
    code2name = dict(zip(df['code'], df['name']))
    code2ind = dict(zip(df['code'], df['industry']))
    code2total = dict(zip(df['code'], df['total']))
    
    # 只展示03+04都选中的（最有信心的）
    s3_codes = codes_sets['03-TOP10']
    s4_codes = codes_sets['04-TOP10']
    both = s3_codes & s4_codes
    log(f"\n💎 策略03与策略04【共同推荐】({len(both)}只) — 量化核心高信心标的:")
    for c in both:
        log(f"  • {code2name.get(c,'-')}({c})  {code2ind.get(c,'-')}  总分{code2total.get(c,0)}")
    
    only_03 = s3_codes - s4_codes
    only_04 = s4_codes - s3_codes
    if only_03:
        log(f"\n📈 仅策略03选中 ({len(only_03)}只) — 可能因子alpha更强:")
        for c in only_03:
            log(f"  • {code2name.get(c,'-')}({c})  {code2ind.get(c,'-')}  总分{code2total.get(c,0)}")
    if only_04:
        log(f"\n🛡 仅策略04选中 ({len(only_04)}只):")
        for c in only_04:
            log(f"  • {code2name.get(c,'-')}({c})  {code2ind.get(c,'-')}  总分{code2total.get(c,0)}")
    
    # 保存JSON
    def clean(o):
        if isinstance(o, dict): return {k: clean(v) for k,v in o.items()}
        if isinstance(o, list): return [clean(v) for v in o]
        if isinstance(o, float) and (np.isnan(o) or np.isinf(o)): return None
        if isinstance(o, (np.integer,)): return int(o)
        if isinstance(o, (np.floating,)):
            f = float(o)
            if np.isnan(f) or np.isinf(f): return None
            return f
        if isinstance(o, bool): return bool(o)
        return o
    
    out = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'primary': {
            '03_aitrader_top10': sel3_top10.to_dict('records') if len(sel3_top10)>0 else [],
            '03_aitrader_top5':  sel3_top5.to_dict('records')  if len(sel3_top5)>0  else [],
            '04_risk_managed_top10': {
                'stocks': sel4_top10.to_dict('records') if len(sel4_top10)>0 else [],
                'position': pos10,
            },
            '04_risk_managed_top5': {
                'stocks': sel4_top5.to_dict('records') if len(sel4_top5)>0 else [],
                'position': pos5,
            },
        },
        'reference': {
            '01_strict_elite_top5': sel1.to_dict('records') if len(sel1)>0 else [],
            '02_mainboard_elite_top5': sel2.to_dict('records') if len(sel2)>0 else [],
        },
        'consensus': {
            '03_04_both': [{'code': c, 'name': code2name.get(c), 'industry': code2ind.get(c),
                            'total': int(code2total.get(c,0))} for c in both],
            'only_03': [{'code': c, 'name': code2name.get(c), 'industry': code2ind.get(c),
                         'total': int(code2total.get(c,0))} for c in only_03],
            'only_04': [{'code': c, 'name': code2name.get(c), 'industry': code2ind.get(c),
                         'total': int(code2total.get(c,0))} for c in only_04],
        }
    }
    out = clean(out)
    out_path = '/Users/ecustkiller/WorkBuddy/Claw/data/json_results/4策略选股结果.json'
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    log('\n' + '='*110)
    log(f"💾 结果保存: {out_path}")
    log(f"✅ 全部策略运行完成 {datetime.now().strftime('%H:%M:%S')}")

if __name__ == '__main__':
    main()
