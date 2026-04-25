#!/usr/bin/env python3
"""
批量评分脚本 — 9 Skill v3.3
对全市场活跃股（涨幅>=3%）进行批量评分，输出TOP20

用法:
  python3 batch_score.py              # 默认涨幅>=3%
  python3 batch_score.py --min-chg 5  # 涨幅>=5%
  python3 batch_score.py --all        # 全市场（慢）
"""
import sys, os, time, json
import numpy as np
import pandas as pd

# 添加当前目录到path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import app

def batch_score(min_chg=3.0, max_count=500, skip_fina=False):
    """批量评分"""
    print("=" * 110)
    print(f"9 Skill 批量评分 v3.3 — 对齐 sector_deep_pick_v2.py")
    print("=" * 110)
    
    t_start = time.time()
    
    # ===== 1. 获取候选池 =====
    T0 = app.get_latest_trade_date()
    if not T0:
        print("❌ 无法获取最新交易日"); return []
    
    trade_dates = app.get_trade_dates()
    t0_idx = trade_dates.index(T0)
    
    snap = app.load_snapshot(T0)
    if snap is None:
        print("❌ 快照不可用"); return []
    
    # 过滤
    candidates = snap.copy()
    if 'name' in candidates.columns:
        candidates = candidates[~candidates['name'].str.contains('ST|退', na=False)]
    candidates = candidates[candidates['ts_code'].str.match(r'^(00|30|60|68)', na=False)]
    if min_chg > 0:
        candidates = candidates[candidates['pct_chg'] >= min_chg]
    candidates = candidates.sort_values('pct_chg', ascending=False).head(max_count)
    
    print(f"📅 数据日期: {T0}")
    print(f"📊 候选池: {len(candidates)}只 (涨幅>={min_chg}%)")
    print(f"⏱  开始评分...")
    print("-" * 110)
    
    # ===== 2. 预加载公共数据（避免重复计算） =====
    weights = app.load_skill_weights()
    max_total = sum(weights.values()) + 5  # 105
    
    # 主线数据
    ml_data = app.calc_mainline_scores(T0, trade_dates)
    ind_zt_map = ml_data["ind_zt_map"]
    
    # 涨停池数据
    zt_data = app.get_zt_data(T0, trade_dates)
    zt_cnt = zt_data.get('zt_cnt', 0)
    fbl = zt_data.get('fbl', 0)
    
    # 情绪阶段推断
    if zt_cnt >= 80 and fbl >= 75: emotion_stage = '起爆'
    elif zt_cnt >= 60 and fbl >= 60: emotion_stage = '一致'
    elif zt_cnt >= 40 and fbl >= 50: emotion_stage = '修复'
    elif zt_cnt >= 25 and fbl >= 40: emotion_stage = '分歧'
    elif zt_cnt >= 15: emotion_stage = '启动'
    elif zt_cnt < 15 and fbl < 40: emotion_stage = '退潮'
    else: emotion_stage = '分歧'
    
    print(f"🎭 情绪阶段: {emotion_stage} (涨停{zt_cnt}家, 封板率{fbl}%)")
    
    # 股票列表
    stock_list = app.get_stock_list()
    name_map = stock_list["name_map"]
    ind_map = stock_list["ind_map"]
    
    # 行业市值排名（从快照计算）
    industry_rank_map = {}
    if 'industry' in snap.columns and 'total_mv' in snap.columns:
        for ind_name, grp in snap.groupby('industry'):
            sorted_grp = grp.sort_values('total_mv', ascending=False).reset_index(drop=True)
            for rank, (_, row) in enumerate(sorted_grp.iterrows(), 1):
                industry_rank_map[row['ts_code']] = rank
    
    # ===== 3. 批量评分（快速模式：跳过Tushare财务查询） =====
    results = []
    total = len(candidates)
    errors = 0
    
    for i, (_, row) in enumerate(candidates.iterrows()):
        ts_code = row['ts_code']
        name = name_map.get(ts_code, row.get('name', '未知'))
        ind = ind_map.get(ts_code, row.get('industry', '未知'))
        
        try:
            # 获取K线数据
            kdf, kline_src = app.get_kline(ts_code, trade_dates, t0_idx, lookback=30)
            if kdf is None or kdf.empty or len(kdf) < 5:
                errors += 1; continue
            
            kdf_sorted = kdf.sort_values('trade_date').reset_index(drop=True)
            cc = kdf_sorted['close'].astype(float).values.tolist()
            hh = kdf_sorted['high'].astype(float).values.tolist()
            ll = kdf_sorted['low'].astype(float).values.tolist()
            vv = kdf_sorted['vol'].astype(float).values.tolist()
            oo = kdf_sorted['open'].astype(float).values.tolist() if 'open' in kdf_sorted.columns else cc[:]
            c0 = cc[-1]
            
            pct_last = float(kdf_sorted['pct_chg'].iloc[-1]) if 'pct_chg' in kdf_sorted.columns and pd.notna(kdf_sorted['pct_chg'].iloc[-1]) else 0
            chg5 = (cc[-1] / cc[-6] - 1) * 100 if len(cc) >= 6 and cc[-6] > 0 else 0
            chg10 = (cc[-1] / cc[-11] - 1) * 100 if len(cc) >= 11 and cc[-11] > 0 else 0
            
            ma5 = float(np.mean(cc[-5:])) if len(cc) >= 5 else cc[-1]
            ma10 = float(np.mean(cc[-10:])) if len(cc) >= 10 else float(np.mean(cc))
            ma20 = float(np.mean(cc[-20:])) if len(cc) >= 20 else float(np.mean(cc))
            
            if ma5 > ma10 > ma20: ma_tag = '多头'
            elif ma5 > ma10: ma_tag = '短多'
            else: ma_tag = '弱'
            
            avg_v5 = float(np.mean(vv[-6:-1])) if len(vv) >= 6 else float(np.mean(vv))
            vol_ratio = vv[-1] / avg_v5 if avg_v5 > 0 else 0
            
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
            
            # 基本面数据（从快照直接取，不调API）
            pe = float(row.get('pe_ttm', 0)) if pd.notna(row.get('pe_ttm')) else None
            circ_mv = float(row.get('circ_mv', 0)) if pd.notna(row.get('circ_mv')) else 0
            total_mv = float(row.get('total_mv', 0)) if pd.notna(row.get('total_mv')) else 0
            turnover = float(row.get('turnover_rate_f', 0)) if pd.notna(row.get('turnover_rate_f')) else 0
            
            c_data['pe'] = pe if pe and pe > 0 else None
            c_data['circ_mv'] = circ_mv
            c_data['total_mv'] = total_mv
            c_data['turnover'] = turnover
            c_data['industry_mv_rank'] = industry_rank_map.get(ts_code, 99)
            
            # 资金流向（从快照取）
            net_inflow = 0
            if 'net_mf_amount' in row.index and pd.notna(row.get('net_mf_amount')):
                net_inflow = float(row['net_mf_amount']) / 10000  # 万→亿
            c_data['net_inflow'] = net_inflow
            
            # 60分钟K线
            kdf_60, _ = app.get_kline_60min_local(ts_code, count=80)
            if kdf_60 is not None and not kdf_60.empty and len(kdf_60) >= 12:
                kdf_60 = kdf_60.sort_values('datetime').reset_index(drop=True)
                c_data['kline_60m'] = {
                    'closes': kdf_60['close'].astype(float).tolist(),
                    'highs': kdf_60['high'].astype(float).tolist(),
                    'lows': kdf_60['low'].astype(float).tolist(),
                    'vols': kdf_60['vol'].astype(float).tolist(),
                }
            
            # BCI
            ind_bci, ind_bci_concept = app.get_industry_bci(ind, app.BCI_DATA)
            bci_score = ind_bci
            sector_zt = ind_zt_map.get(ind, 0)
            
            # ===== 9 Skill 评分 =====
            s1, t1 = app._score_s1_txcg(c_data, sector_zt, emotion_stage, weights['TXCG'], bci_score)
            s2, t2 = app._score_s2_yuanziyuan(c_data, weights['元子元'])
            s3, t3 = app._score_s3_camellia(c_data, sector_zt, weights['山茶花'], bci_score)
            s4, t4 = app._score_s4_mistery(c_data, weights['Mistery'])
            s5, t5 = app._score_s5_tds(c_data, weights['TDS'])
            s6, t6 = app._score_s6_wr(c_data, weights['百胜WR'])
            s7, t7 = app._score_s7_event(c_data, sector_zt, weights['事件驱动'], bci_score)
            s8, t8 = app._score_s8_multi_period(c_data, weights['多周期'])
            s9, t9 = app._score_s9_fundamental(c_data, weights['基本面'])
            txcg_bonus, t_bonus = app._calc_txcg_bonus(c_data, sector_zt)
            
            total_score = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + txcg_bonus
            
            # 汇总标签
            all_tags = []
            for t in [t1, t2, t3, t4, t5, t6, t7, t8, t9, t_bonus]:
                all_tags.extend(t)
            key_tags = [t for t in all_tags if any(k in t for k in ['🔥', '超跌', 'BBW', '量', '突破', '趋势', 'WR', '涨停', '龙头', '冰点', '反转', 'BCI'])]
            
            results.append({
                'code': ts_code,
                'name': name,
                'industry': ind,
                'close': c0,
                'pct_chg': pct_last,
                'total': total_score,
                's1': s1, 's2': s2, 's3': s3, 's4': s4, 's5': s5,
                's6': s6, 's7': s7, 's8': s8, 's9': s9,
                'txcg_bonus': txcg_bonus,
                'chg5': chg5,
                'is_zt': is_zt,
                'ma_tag': ma_tag,
                'vol_ratio': round(vol_ratio, 2),
                'bbw': round(bbw, 3),
                'pe': pe,
                'mv_yi': circ_mv / 10000 if circ_mv else 0,
                'bci': bci_score,
                'sector_zt': sector_zt,
                'key_tags': key_tags[:5],
            })
            
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  ⚠ {ts_code} {name}: {e}")
        
        # 进度显示
        if (i + 1) % 50 == 0 or i == total - 1:
            elapsed = time.time() - t_start
            print(f"  [{i+1}/{total}] 已评分{len(results)}只, 错误{errors}只, 耗时{elapsed:.1f}s")
    
    # ===== 4. 排序输出TOP20 =====
    results.sort(key=lambda x: x['total'], reverse=True)
    
    print("\n" + "=" * 110)
    print(f"📊 9 Skill 综合评分 TOP20 — {T0} — 情绪:{emotion_stage}")
    print(f"   候选{total}只 → 有效{len(results)}只 → 满分{max_total}")
    print("=" * 110)
    
    # 表头
    print(f"{'排名':>3} {'代码':>10} {'名称':<8} {'行业':<8} {'收盘':>7} {'总分':>4} "
          f"{'S1':>3} {'S2':>3} {'S3':>3} {'S4':>3} {'S5':>3} {'S6':>3} {'S7':>3} {'S8':>3} {'S9':>3} {'加':>2} "
          f"{'涨幅':>6} {'5日':>6} {'量比':>5} {'BBW':>6} {'均线':<4} {'亮点标签'}")
    print("-" * 110)
    
    for rank, r in enumerate(results[:20], 1):
        zt_mark = "🔴" if r['is_zt'] else "  "
        tags_str = ' '.join(r['key_tags'][:3]) if r['key_tags'] else ''
        print(f"{rank:>3} {r['code']:>10} {r['name']:<8} {r['industry']:<8} {r['close']:>7.2f} {r['total']:>4} "
              f"{r['s1']:>3} {r['s2']:>3} {r['s3']:>3} {r['s4']:>3} {r['s5']:>3} {r['s6']:>3} {r['s7']:>3} {r['s8']:>3} {r['s9']:>3} {r['txcg_bonus']:>2} "
              f"{r['pct_chg']:>+5.1f}% {r['chg5']:>+5.1f}% {r['vol_ratio']:>5.2f} {r['bbw']:>6.3f} {r['ma_tag']:<4} {zt_mark}{tags_str}")
    
    # ===== 5. 行业分布 =====
    print(f"\n📈 行业分布 (TOP20)")
    ind_count = {}
    for r in results[:20]:
        ind_count[r['industry']] = ind_count.get(r['industry'], 0) + 1
    for ind, cnt in sorted(ind_count.items(), key=lambda x: x[1], reverse=True):
        names = [r['name'] for r in results[:20] if r['industry'] == ind]
        print(f"  {ind}: {cnt}只 — {', '.join(names)}")
    
    # ===== 6. 保存结果 =====
    elapsed = time.time() - t_start
    print(f"\n⏱ 总耗时: {elapsed:.1f}s")
    
    # 保存JSON
    output = {
        'date': T0,
        'emotion_stage': emotion_stage,
        'zt_cnt': zt_cnt,
        'fbl': fbl,
        'total_candidates': total,
        'valid_scored': len(results),
        'max_total': max_total,
        'top20': results[:20],
        'elapsed': round(elapsed, 1),
    }
    output_file = os.path.join(os.path.dirname(__file__), '..', f'9skill_TOP20_{T0}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"💾 已保存: {output_file}")
    
    return results


if __name__ == '__main__':
    min_chg = 3.0
    max_count = 500
    
    for arg in sys.argv[1:]:
        if arg.startswith('--min-chg'):
            min_chg = float(arg.split('=')[1]) if '=' in arg else float(sys.argv[sys.argv.index(arg) + 1])
        elif arg == '--all':
            min_chg = 0
            max_count = 9999
    
    batch_score(min_chg=min_chg, max_count=max_count)
