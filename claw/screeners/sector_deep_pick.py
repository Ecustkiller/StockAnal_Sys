#!/usr/bin/env python3
"""
板块内遍历精选脚本 v1.0
解决的问题：之前选股从"已有候选池"凑标的→漏掉板块内最优标的（如鹭燕、南京医药）
正确流程：确定方向→遍历该方向全部涨停/大涨票→6维度排序→选TOP

用法：
  python3 sector_deep_pick.py 20260415 化学制药 医药商业 中成药 医疗保健 生物制药
  
  参数1: 交易日期(YYYYMMDD)
  参数2+: 目标行业名称（Tushare industry字段）
"""
import sys, requests, json, time
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts_api(api_name, **kwargs):
    params = {"api_name": api_name, "token": TOKEN, "params": kwargs, "fields": ""}
    r = requests.post("http://api.tushare.pro", json=params, timeout=30)
    d = r.json()
    if d.get('data') and d['data'].get('items'):
        return d['data']['fields'], d['data']['items']
    return [], []

def main():
    if len(sys.argv) < 3:
        print("用法: python3 sector_deep_pick.py 20260415 化学制药 医药商业 ...")
        return
    
    trade_date = sys.argv[1]
    target_industries = sys.argv[2:]
    
    print(f"="*80)
    print(f"板块内遍历精选 — {trade_date} — 方向：{', '.join(target_industries)}")
    print(f"="*80)
    
    # Step 1: 获取全市场日线
    print(f"\n[1/5] 获取{trade_date}全市场日线...")
    fields, items = ts_api("daily", trade_date=trade_date)
    if not items:
        print("无数据！"); return
    FI = {f:i for i,f in enumerate(fields)}
    print(f"  总{len(items)}只")
    
    # Step 2: 获取股票基本信息（名称+行业+市值）
    print(f"[2/5] 获取股票基本信息...")
    fn, ni = ts_api("stock_basic", exchange="", list_status="L", 
                     fields="ts_code,name,industry,market_cap")
    info_map = {}
    st_codes = set()
    if ni:
        fi_n = {f:i for i,f in enumerate(fn)}
        for item in ni:
            code = item[fi_n['ts_code']]
            name = item[fi_n.get('name','')]
            industry = item[fi_n.get('industry','')]
            info_map[code] = {'name': name, 'industry': industry}
            if name.startswith('ST') or name.startswith('*ST'):
                st_codes.add(code)
    print(f"  总{len(info_map)}只基本信息")
    
    # Step 3: 筛选目标行业的涨停+大涨标的
    print(f"[3/5] 筛选目标行业（{', '.join(target_industries)}）...")
    
    candidates = []
    for item in items:
        code = item[FI['ts_code']]
        if code in st_codes: continue  # 排除ST
        info = info_map.get(code, {})
        industry = info.get('industry', '')
        if industry not in target_industries: continue
        
        pct_chg = item[FI['pct_chg']] or 0
        if pct_chg < 3: continue  # 至少涨>3%才纳入
        
        close = item[FI['close']] or 0
        vol = item[FI['vol']] or 0
        amount = item[FI['amount']] or 0
        pre_close = item[FI['pre_close']] or 0
        
        candidates.append({
            'code': code,
            'name': info.get('name', ''),
            'industry': industry,
            'close': float(close),
            'pct_chg': float(pct_chg),
            'vol': float(vol),
            'amount': float(amount),
            'pre_close': float(pre_close),
            'is_zt': float(pct_chg) >= 9.5,
        })
    
    print(f"  目标行业涨>3%: {len(candidates)}只（涨停{sum(1 for c in candidates if c['is_zt'])}只）")
    
    # Step 4: 获取每只标的的历史数据（5日/10日/20日涨幅+均线+BBW）
    print(f"[4/5] 获取历史数据做6维度排序...")
    
    # 获取近30日日线做计算
    for c in candidates:
        try:
            f2, i2 = ts_api("daily", ts_code=c['code'], 
                           end_date=trade_date, limit=30,
                           fields="ts_code,trade_date,close,vol,amount")
            if not i2 or len(i2) < 5:
                c['valid'] = False; continue
            
            fi2 = {f:i for i,f in enumerate(f2)}
            closes = [float(row[fi2['close']]) for row in sorted(i2, key=lambda x: x[fi2['trade_date']])]
            vols = [float(row[fi2['vol']]) for row in sorted(i2, key=lambda x: x[fi2['trade_date']])]
            
            cur = closes[-1]
            c['chg5'] = (cur / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
            c['chg10'] = (cur / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
            c['chg20'] = (cur / closes[-21] - 1) * 100 if len(closes) >= 21 else 0
            
            c['ma5'] = np.mean(closes[-5:])
            c['ma10'] = np.mean(closes[-10:])
            c['ma20'] = np.mean(closes[-20:]) if len(closes) >= 20 else np.mean(closes)
            c['ma60'] = np.mean(closes[-60:]) if len(closes) >= 60 else np.mean(closes)
            
            # 均线排列
            if c['ma5'] > c['ma10'] > c['ma20']: c['ma_tag'] = '多头'
            elif c['ma5'] > c['ma10']: c['ma_tag'] = '短多'
            else: c['ma_tag'] = '弱'
            
            # 量比
            avg_v5 = np.mean(vols[-6:-1]) if len(vols) >= 6 else np.mean(vols)
            c['vol_ratio'] = vols[-1] / avg_v5 if avg_v5 > 0 else 0
            
            # BBW
            if len(closes) >= 20:
                std20 = np.std(closes[-20:])
                c['bbw'] = (4 * std20) / c['ma20'] if c['ma20'] > 0 else 0
            else:
                c['bbw'] = 0
            
            c['valid'] = True
        except:
            c['valid'] = False
        time.sleep(0.15)  # API限速
    
    valid = [c for c in candidates if c.get('valid')]
    print(f"  有效数据: {len(valid)}只")
    
    # Step 5: 获取资金流向（净流入）
    print(f"[5/5] 获取资金流向...")
    for c in valid:
        try:
            f3, i3 = ts_api("moneyflow", trade_date=trade_date, ts_code=c['code'])
            if i3:
                fi3 = {f:i for i,f in enumerate(f3)}
                # 净流入 = buy_md_amount + buy_lg_amount - sell_md_amount - sell_lg_amount (主力)
                buy_md = float(i3[0][fi3.get('buy_md_amount', 0)] or 0)
                buy_lg = float(i3[0][fi3.get('buy_lg_amount', 0)] or 0)
                sell_md = float(i3[0][fi3.get('sell_md_amount', 0)] or 0)
                sell_lg = float(i3[0][fi3.get('sell_lg_amount', 0)] or 0)
                c['net_inflow'] = (buy_md + buy_lg - sell_md - sell_lg) / 10000  # 万→亿
            else:
                c['net_inflow'] = 0
        except:
            c['net_inflow'] = 0
        time.sleep(0.1)
    
    # Step 6: 综合评分排序
    print(f"\n{'='*100}")
    print(f"板块内精选排序（6维度）")
    print(f"{'='*100}")
    
    for c in valid:
        score = 0
        reasons = []
        
        # 维度1: 超跌程度（5日涨幅越低越安全，负值=超跌反弹加分）
        if c['chg5'] < -10: score += 4; reasons.append(f"深度超跌{c['chg5']:+.1f}%")
        elif c['chg5'] < -5: score += 3; reasons.append(f"超跌{c['chg5']:+.1f}%")
        elif c['chg5'] < 0: score += 2; reasons.append(f"小跌{c['chg5']:+.1f}%")
        elif c['chg5'] < 5: score += 2; reasons.append(f"安全{c['chg5']:+.1f}%")
        elif c['chg5'] < 10: score += 1
        elif c['chg5'] < 15: score += 0
        else: score -= 2; reasons.append(f"⚠高位{c['chg5']:+.1f}%")
        
        # 维度2: 净流入（正=加分，大额负=大扣分）
        if c['net_inflow'] > 1.0: score += 3; reasons.append(f"大额净入{c['net_inflow']:+.1f}亿")
        elif c['net_inflow'] > 0.3: score += 2; reasons.append(f"净入{c['net_inflow']:+.1f}亿")
        elif c['net_inflow'] > 0: score += 1; reasons.append(f"净入{c['net_inflow']:+.1f}亿")
        elif c['net_inflow'] > -0.5: pass  # 小额流出不扣分
        elif c['net_inflow'] > -1: score -= 1
        else: score -= 2; reasons.append(f"❌净出{c['net_inflow']:.1f}亿")
        
        # 维度3: 量比
        if c['vol_ratio'] >= 5: score += 3; reasons.append(f"巨量{c['vol_ratio']:.1f}x")
        elif c['vol_ratio'] >= 3: score += 2; reasons.append(f"量{c['vol_ratio']:.1f}x")
        elif c['vol_ratio'] >= 2: score += 1; reasons.append(f"量{c['vol_ratio']:.1f}x")
        
        # 维度4: BBW收敛度
        if c['bbw'] < 0.12: score += 3; reasons.append(f"BBW={c['bbw']:.3f}极低")
        elif c['bbw'] < 0.15: score += 2; reasons.append(f"BBW={c['bbw']:.3f}低")
        elif c['bbw'] < 0.20: score += 1; reasons.append(f"BBW={c['bbw']:.3f}")
        
        # 维度5: 涨停+超跌组合（超跌反弹涨停=最佳信号，大幅加分）
        if c['is_zt'] and c['chg5'] < -5:
            score += 3; reasons.append("🔥超跌涨停")
        elif c['is_zt'] and c['chg5'] < 5:
            score += 2; reasons.append("涨停+安全")
        elif c['is_zt']:
            score += 1; reasons.append("涨停")
        elif c['pct_chg'] >= 7:
            score += 1; reasons.append(f"+{c['pct_chg']:.1f}%大阳")
        
        # 维度6: 均线排列
        if c['ma_tag'] == '多头': score += 1; reasons.append("多头")
        
        c['score'] = score
        c['reasons'] = reasons
    
    # 排序输出
    valid.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"\n{'#':>3} {'名称':<10} {'行业':<8} {'收盘':>7} {'涨跌':>6} {'5日':>6} {'净入':>6} {'量比':>5} {'BBW':>6} {'均线':>4} {'得分':>4} {'评价'}")
    print("-"*95)
    for i, c in enumerate(valid):
        zt = "🔴" if c['is_zt'] else ""
        print(f"{i+1:>3} {c['name']:<10} {c['industry']:<8} {c['close']:>7.2f} {c['pct_chg']:>+5.1f}%{zt} {c['chg5']:>+5.1f}% {c['net_inflow']:>+5.1f}亿 {c['vol_ratio']:>4.1f}x {c['bbw']:>5.3f} {c['ma_tag']:>4} {c['score']:>+4d} {','.join(c['reasons'])}")
    
    # TOP5推荐
    print(f"\n{'='*80}")
    print(f"⭐ 板块内TOP5推荐")
    print(f"{'='*80}")
    for i, c in enumerate(valid[:5]):
        print(f"\n  {i+1}. {c['name']}({c['code']}) — 得分{c['score']:+d}")
        print(f"     收{c['close']:.2f} 涨{c['pct_chg']:+.1f}% 5日{c['chg5']:+.1f}% 净入{c['net_inflow']:+.1f}亿 量{c['vol_ratio']:.1f}x BBW={c['bbw']:.3f} {c['ma_tag']}")
        print(f"     亮点: {', '.join(c['reasons'])}")

if __name__ == '__main__':
    main()
