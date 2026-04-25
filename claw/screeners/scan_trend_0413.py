#!/usr/bin/env python3
"""非涨停趋势票扫描：WR-2+Mistery+TDS"""
import requests, json, time, numpy as np, warnings
warnings.filterwarnings('ignore')

token = 'ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59'
def ts(api, fields='', **params):
    d = {'api_name': api, 'token': token, 'params': params, 'fields': fields}
    try:
        r = requests.post('http://api.tushare.pro', json=d, timeout=15)
        j = r.json()
        if j.get('data'): return j['data']
    except: pass
    return {'items':[], 'fields':[]}

# 获取4/13涨3-9%的非涨停趋势票
daily = ts('daily','ts_code,trade_date,open,high,low,close,vol,amount,pct_chg',trade_date='20260413').get('items',[])
hot = [r for r in daily if r[8] and 3 <= r[8] < 9.5]
hot.sort(key=lambda x: x[8], reverse=True)
print(f"4/13涨3-9%: {len(hot)}只")

# 名称
name_all = ts('stock_basic','ts_code,name,industry',list_status='L').get('items',[])
name_map = {r[0]:{'name':r[1],'ind':r[2]} for r in name_all}

# 市值
mv_all = ts('daily_basic','ts_code,circ_mv,pe_ttm,turnover_rate',trade_date='20260413').get('items',[])
mv_map = {}
for r in mv_all:
    if r[1]: mv_map[r[0]] = {'mv':r[1]/10000, 'pe':r[2], 'turnover':r[3]}

results = []
checked = 0

for stk in hot:
    tc = stk[0]
    nm = name_map.get(tc,{})
    name = nm.get('name','')
    if 'ST' in name: continue
    
    mv_info = mv_map.get(tc,{})
    mv = mv_info.get('mv',0)
    pe = mv_info.get('pe',0)
    turnover = mv_info.get('turnover',0)
    
    # 市值过滤：50-2000亿（趋势票范围更宽）
    if mv < 50 or mv > 2000: continue
    
    # 获取近60日K线
    kd = ts('daily','ts_code,trade_date,open,high,low,close,vol,amount,pct_chg',
            ts_code=tc, start_date='20260120', end_date='20260413').get('items',[])
    if len(kd) < 30: continue
    kd.sort(key=lambda x: x[1])
    
    c = np.array([r[5] for r in kd], dtype=float)
    v = np.array([r[6] for r in kd], dtype=float)
    h = np.array([r[3] for r in kd], dtype=float)
    l = np.array([r[4] for r in kd], dtype=float)
    o = np.array([r[2] for r in kd], dtype=float)
    pcts = np.array([r[8] if r[8] else 0 for r in kd], dtype=float)
    
    n = len(c)
    
    # ===== 均线计算 =====
    ma5 = np.mean(c[-5:])
    ma10 = np.mean(c[-10:]) if n>=10 else ma5
    ma20 = np.mean(c[-20:]) if n>=20 else ma10
    ma60 = np.mean(c[-60:]) if n>=60 else np.mean(c[-30:])
    
    # ===== WR-2条件 =====
    av5 = np.mean(v[-6:-1]) if n>=6 else np.mean(v[:-1])
    vr = v[-1]/av5 if av5>0 else 0
    
    # 近30天波动（不含今天）
    if n >= 31:
        r30h = max(h[-31:-1]); r30l = min(l[-31:-1])
        range30 = (r30h-r30l)/r30h if r30h>0 else 1
    else: range30 = 1
    
    # BBW
    if n >= 21:
        bbw = np.std(c[-21:-1])/ma20*2 if ma20>0 else 1
    else: bbw = 1
    
    # M60斜率
    if n >= 70:
        m60_now = np.mean(c[-60:])
        m60_10ago = np.mean(c[-70:-60])
        m60_up = m60_now >= m60_10ago * 0.99
    else: m60_up = True
    
    wr2_checks = {
        '均线多头(5>20>60)': ma5 > ma20 > ma60,
        'M60向上': m60_up,
        '整理充分(波动<20%或BBW<0.15)': range30 < 0.20 or bbw < 0.15,
        '放量≥2.5倍': vr >= 2.5,
        '涨≥3%': stk[8] >= 3,
    }
    wr2_score = sum(wr2_checks.values())
    
    # ===== Mistery趋势买点 =====
    mistery_signals = []
    
    # 多头排列
    multi_head = ma5 > ma10 > ma20 > ma60 if n>=60 else ma5 > ma10 > ma20
    
    # 回调至MA20附近（BIAS20 < 5%）
    bias20 = (c[-1] - ma20) / ma20 * 100
    near_ma20 = 0 < bias20 < 8  # 站在MA20上方但不太远
    
    # MACD金叉（简化：DIF>DEA且DIF>0）
    if n >= 26:
        ema12 = c[-1]; ema26 = c[-1]
        for i in range(n-1, -1, -1):
            ema12 = c[i]*2/(12+1) + ema12*(1-2/(12+1))
            ema26 = c[i]*2/(26+1) + ema26*(1-2/(26+1))
        dif = ema12 - ema26
        # 前一天的
        ema12p = c[-2]; ema26p = c[-2]
        for i in range(n-2, -1, -1):
            ema12p = c[i]*2/(12+1) + ema12p*(1-2/(12+1))
            ema26p = c[i]*2/(26+1) + ema26p*(1-2/(26+1))
        dif_p = ema12p - ema26p
        macd_golden = dif > 0 and dif > dif_p  # DIF>0且上升
    else: macd_golden = False
    
    # RSI
    if n >= 15:
        gains = np.maximum(np.diff(c[-15:]), 0)
        losses = np.abs(np.minimum(np.diff(c[-15:]), 0))
        avg_gain = np.mean(gains) if len(gains)>0 else 0
        avg_loss = np.mean(losses) if len(losses)>0 else 0
        rsi14 = 100 - 100/(1+avg_gain/avg_loss) if avg_loss>0 else 100
    else: rsi14 = 50
    
    # 5日涨幅和10日涨幅（安全边际）
    pct_5d = (c[-1]/c[-6]-1)*100 if n>=6 else 0
    pct_10d = (c[-1]/c[-11]-1)*100 if n>=11 else 0
    
    # ===== TDS信号 =====
    # T1推进：连续多头推进（后一根高低都高于前一根）
    t1 = (h[-1]>h[-2] and l[-1]>l[-2]) if n>=2 else False
    # T2吞没：阳线吞没
    t2 = (c[-1]>o[-1] and c[-1]>h[-2] and o[-1]<l[-2]) if n>=2 else False
    # T3突破：突破近20日最高
    recent_high = max(h[-21:-1]) if n>=21 else max(h[:-1])
    t3 = c[-1] > recent_high
    
    tds_signals = []
    if t1: tds_signals.append('T1推进')
    if t2: tds_signals.append('T2吞没')
    if t3: tds_signals.append('T3突破')
    
    # ===== 综合评分 =====
    score = 0
    details = []
    
    # WR-2 (最高30分)
    if wr2_score >= 5: score += 30; details.append('WR-2满分')
    elif wr2_score >= 4: score += 22; details.append(f'WR-2({wr2_score}/5)')
    elif wr2_score >= 3: score += 12
    
    # Mistery趋势 (最高30分)
    if multi_head: score += 12; details.append('多头排列')
    if near_ma20: score += 6; details.append(f'BIAS20={bias20:.1f}%')
    if macd_golden: score += 6; details.append('MACD金叉')
    if rsi14 < 70: score += 3  # 未超买
    if rsi14 > 80: score -= 5; details.append('⚠️RSI超买')
    
    # TDS (最高20分)
    if tds_signals:
        score += len(tds_signals) * 7
        details.append('+'.join(tds_signals))
    
    # 安全边际 (最高20分)
    if pct_5d < 15: score += 8
    elif pct_5d < 25: score += 4
    else: score -= 5; details.append(f'⚠️5日涨{pct_5d:.0f}%')
    
    if pct_10d < 25: score += 6
    elif pct_10d < 40: score += 2
    else: score -= 5; details.append(f'⚠️10日涨{pct_10d:.0f}%')
    
    if bbw < 0.10: score += 6; details.append(f'BBW极低{bbw:.3f}')
    elif bbw < 0.15: score += 3
    
    if score >= 40:  # 只保留高分标的
        results.append({
            'ts_code': tc, 'name': name, 'ind': nm.get('ind',''),
            'close': round(c[-1],2), 'pct': round(stk[8],1),
            'mv': round(mv,0), 'pe': round(pe,1) if pe else 0,
            'turnover': round(turnover,1) if turnover else 0,
            'score': score,
            'wr2_score': wr2_score,
            'multi_head': multi_head,
            'bias20': round(bias20,1),
            'rsi14': round(rsi14,1),
            'vol_ratio': round(vr,1),
            'bbw': round(bbw,3),
            'pct_5d': round(pct_5d,1),
            'pct_10d': round(pct_10d,1),
            'tds': '+'.join(tds_signals) if tds_signals else '无',
            'details': ', '.join(details),
        })
    
    checked += 1
    if checked % 50 == 0:
        print(f"  已检查{checked}/{len(hot)}... 入选{len(results)}只")
    time.sleep(0.35)

results.sort(key=lambda x: x['score'], reverse=True)

print(f"\n{'='*70}")
print(f"非涨停趋势票扫描完成: 检查{checked}只, 入选{len(results)}只(综合评分≥40)")
print(f"{'='*70}")
print(f"\n{'名称':10s} {'代码':12s} {'行业':8s} {'涨幅':>5s} {'市值':>6s} {'评分':>4s} {'WR2':>4s} {'均线':4s} {'BIAS':>5s} {'RSI':>4s} {'量比':>4s} {'BBW':>5s} {'5日':>5s} {'TDS':10s} {'亮点'}")
print("-"*120)
for r in results[:30]:
    head = '✅' if r['multi_head'] else '❌'
    print(f"{r['name']:10s} {r['ts_code']:12s} {r['ind']:8s} {r['pct']:>+5.1f}% {r['mv']:>5.0f}亿 {r['score']:>4d} {r['wr2_score']:>3d}/5 {head} {r['bias20']:>4.1f}% {r['rsi14']:>4.1f} {r['vol_ratio']:>4.1f}x {r['bbw']:>5.3f} {r['pct_5d']:>+5.1f}% {r['tds']:10s} {r['details']}")

with open('trend_scan_0413.json','w',encoding='utf-8') as f:
    json.dump(results[:50], f, ensure_ascii=False, indent=2, default=str)
