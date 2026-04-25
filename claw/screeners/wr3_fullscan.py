#!/usr/bin/env python3
"""
WR-3全量扫描：本地5490只60分钟K线 + Ashare补充最新数据
"""
import sys; sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price
import pandas as pd
import numpy as np
import os, time, json, warnings
warnings.filterwarnings('ignore')

LOCAL_DIR = os.path.expanduser('~/Downloads/2026/60min')
OUTPUT_FILE = '/Users/ecustkiller/WorkBuddy/Claw/wr3_fullscan_results.json'

def load_local_60min(code_file):
    """读取本地60分钟K线"""
    path = os.path.join(LOCAL_DIR, code_file)
    try:
        df = pd.read_csv(path, encoding='utf-8')
        df.columns = ['date','time','open','high','low','close','volume','amount']
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df = df[['datetime','open','high','low','close','volume']].set_index('datetime')
        return df
    except:
        return None

def fetch_ashare_60min(ashare_code, count=80):
    """用Ashare获取最新60分钟K线"""
    try:
        df = get_price(ashare_code, frequency='60m', count=count)
        return df
    except:
        return None

def to_ashare_code(filename):
    """bj920000.csv → bj920000, sz000001.csv → sz000001"""
    return filename.replace('.csv', '')

def merge_data(local_df, remote_df):
    """合并本地+远程数据，去重"""
    if local_df is None and remote_df is None:
        return None
    if local_df is None:
        return remote_df
    if remote_df is None:
        return local_df
    
    # 统一列名
    combined = pd.concat([local_df, remote_df])
    combined = combined[~combined.index.duplicated(keep='last')]
    combined = combined.sort_index()
    return combined

def scan_wr3(vols, closes, opens, highs, lows):
    """扫描底倍量柱信号"""
    n = len(vols)
    if n < 12:
        return None
    
    # 从后往前找，只找最新的一个信号
    for i in range(n-2, 3, -1):
        cur_vol = vols[i]
        prev_vol = vols[i-1]
        
        if prev_vol <= 0 or cur_vol < prev_vol * 2:
            continue
        
        is_yang = closes[i] > opens[i]
        if not is_yang:
            continue
        
        # 相对低位检查
        recent_h = max(highs[max(0,i-8):i])
        recent_l = min(lows[max(0,i-8):i])
        mid = (recent_h + recent_l) / 2
        if closes[i] > mid * 1.08:
            continue
        
        support = lows[i]
        vol_ratio = round(cur_vol / prev_vol, 1)
        
        # 检查后续确认
        confirmed = False
        for j in range(i+1, min(i+6, n)):
            if vols[j] >= vols[j-1] * 1.8 and closes[j] > highs[i] and lows[j] >= support * 0.99:
                confirmed = True
                break
        
        return {
            'support': round(support, 2),
            'vol_ratio': vol_ratio,
            'confirmed': confirmed,
            'cur_close': round(closes[-1], 2),
            'signal_idx': i,
        }
    
    return None

def main():
    files = sorted(os.listdir(LOCAL_DIR))
    files = [f for f in files if f.endswith('.csv')]
    total = len(files)
    print(f"全量扫描: {total}只股票的60分钟K线")
    print(f"本地数据: {LOCAL_DIR}")
    print(f"Ashare补充: 最新数据")
    print("=" * 60)
    
    confirmed_list = []
    pending_list = []
    scanned = 0
    errors = 0
    skipped = 0
    
    for idx, f in enumerate(files):
        code = to_ashare_code(f)
        
        # 跳过北交所（数据少+流动性差）
        if code.startswith('bj'):
            skipped += 1
            continue
        
        # Step1: 读本地数据
        local_df = load_local_60min(f)
        
        # Step2: 用Ashare补充最新（本地截止3/16，需补到4/10）
        # 只对有本地数据的补充，减少API调用
        remote_df = None
        try:
            remote_df = fetch_ashare_60min(code, count=60)
        except:
            pass
        
        # Step3: 合并
        merged = merge_data(local_df, remote_df)
        if merged is None or len(merged) < 15:
            errors += 1
            if (idx+1) % 500 == 0:
                print(f"  进度 {idx+1}/{total} 确认{len(confirmed_list)} 待确认{len(pending_list)} 跳过{skipped} 错误{errors}")
            continue
        
        # Step4: 扫描WR-3
        vols = merged['volume'].values.astype(float)
        closes = merged['close'].values.astype(float)
        opens = merged['open'].values.astype(float)
        highs = merged['high'].values.astype(float)
        lows = merged['low'].values.astype(float)
        
        result = scan_wr3(vols, closes, opens, highs, lows)
        
        if result:
            entry = {
                'code': code,
                'file': f,
                **result,
            }
            if result['confirmed']:
                confirmed_list.append(entry)
            else:
                pending_list.append(entry)
        
        scanned += 1
        if (idx+1) % 500 == 0:
            print(f"  进度 {idx+1}/{total} 扫描{scanned} 确认{len(confirmed_list)} 待确认{len(pending_list)} 错误{errors}")
        
        # Ashare需要控速（0.15秒/只×5000只≈12.5分钟太慢）
        # 优化：只对本地数据截止太早的才调API
        if remote_df is not None:
            time.sleep(0.1)
    
    # 排序
    confirmed_list.sort(key=lambda x: x['vol_ratio'], reverse=True)
    pending_list.sort(key=lambda x: x['vol_ratio'], reverse=True)
    
    print(f"\n{'='*60}")
    print(f"全量扫描完成!")
    print(f"扫描: {scanned}只 | 跳过(北交所): {skipped} | 错误: {errors}")
    print(f"已确认: {len(confirmed_list)}只 | 待确认: {len(pending_list)}只")
    print(f"{'='*60}")
    
    # 保存结果
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as fp:
        json.dump({
            'scan_date': '20260410',
            'total_scanned': scanned,
            'confirmed_count': len(confirmed_list),
            'pending_count': len(pending_list),
            'confirmed': confirmed_list[:100],  # 保存TOP100
            'pending': pending_list[:50],
        }, fp, ensure_ascii=False, indent=2)
    
    # 打印TOP
    print(f"\n🎯 已确认TOP30:")
    for c in confirmed_list[:30]:
        print(f"  {c['code']} 量比{c['vol_ratio']}x 支撑{c['support']} 现价{c['cur_close']} {'✅' if c['confirmed'] else '⏳'}")
    
    print(f"\n⏳ 待确认TOP20:")
    for c in pending_list[:20]:
        print(f"  {c['code']} 量比{c['vol_ratio']}x 支撑{c['support']} 现价{c['cur_close']}")

if __name__ == '__main__':
    main()
