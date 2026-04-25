#!/usr/bin/env python3
"""
快速增量更新 — 只更新需要的周期
- 60min/30min 已到4/15，跳过
- 15min 停在3/16，需更新
- 日线 停在4/3，需更新
"""
import sys, os, time, glob, json
from datetime import datetime

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = '/Users/ecustkiller/Downloads/2026'
STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'
REPORT = {}

def get_local_latest(filepath, seek_bytes=300):
    try:
        fsize = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            f.seek(max(0, fsize - seek_bytes))
            lines = f.readlines()
            for line in reversed(lines):
                decoded = line.decode('utf-8', errors='ignore').strip()
                if decoded and not decoded.startswith('#') and ',' in decoded:
                    return decoded.split(',')[0]
    except:
        pass
    return None

def update_minutes(period_dir, ashare_freq, count):
    full_dir = os.path.join(DATA_DIR, period_dir)
    files = sorted(glob.glob(os.path.join(full_dir, '*.csv')))
    total = len(files)
    print(f"\n{'='*60}")
    print(f"更新 {period_dir} ({total}只, freq={ashare_freq})")
    print(f"{'='*60}")
    
    if files:
        sample_date = get_local_latest(files[0])
        print(f"  本地最新: {sample_date}")
    
    stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'new_rows': 0}
    
    for i, fp in enumerate(files):
        code = os.path.basename(fp).replace('.csv', '')
        local_latest = get_local_latest(fp)
        if not local_latest:
            stats['err'] += 1
            continue
        
        try:
            df = get_price(code, frequency=ashare_freq, count=count)
            if df is None or len(df) == 0:
                stats['err'] += 1
                continue
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date'] = df['datetime'].astype(str).str[:10]
            df['time'] = df['datetime'].astype(str).str[11:16].str.replace(':', '')
            new_data = df[df['date'] > local_latest]
            
            if len(new_data) == 0:
                stats['uptodate'] += 1
            else:
                rows = []
                for _, r in new_data.iterrows():
                    h = r['time'][:2] if r['time'] else '00'
                    m = r['time'][2:4] if len(r['time']) >= 4 else '00'
                    rows.append(f"{r['date']},{h}:{m},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
                with open(fp, 'a') as f:
                    f.write('\n' + '\n'.join(rows))
                stats['updated'] += 1
                stats['new_rows'] += len(new_data)
        except Exception as e:
            stats['err'] += 1
        
        if (i+1) % 200 == 0:
            print(f"  [{i+1}/{total}] 更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']}")
        time.sleep(0.06)
    
    summary = f"{period_dir}: 更新{stats['updated']}只(+{stats['new_rows']}条) 已最新{stats['uptodate']}只 错误{stats['err']}只"
    print(f"  ✅ {summary}")
    REPORT[period_dir] = stats
    return stats

def update_daily():
    files = sorted(glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')))
    total = len(files)
    print(f"\n{'='*60}")
    print(f"更新 日线 ({total}只)")
    print(f"{'='*60}")
    
    if files:
        sample_date = get_local_latest(files[0])
        print(f"  本地最新: {sample_date}")
    
    stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'skip': 0, 'new_rows': 0}
    
    for i, fp in enumerate(files):
        fname = os.path.basename(fp)
        code_raw = fname.split('_')[0]
        if not code_raw.isdigit() or len(code_raw) != 6:
            stats['skip'] += 1
            continue
        
        ashare_code = ('sh' if code_raw.startswith('6') else 'sz') + code_raw
        local_date = get_local_latest(fp)
        if not local_date:
            stats['err'] += 1
            continue
        
        try:
            df = get_price(ashare_code, frequency='1d', count=15)
            if df is None or len(df) == 0:
                stats['err'] += 1
                continue
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date_str'] = df['datetime'].astype(str).str[:10].str.replace('-', '')
            new_data = df[df['date_str'] > local_date]
            
            if len(new_data) == 0:
                stats['uptodate'] += 1
            else:
                ts_code = code_raw + ('.SH' if code_raw.startswith('6') else '.SZ')
                rows = []
                for _, r in new_data.iterrows():
                    row_str = f"{r['date_str']},{ts_code},{r['open']},{r['high']},{r['low']},{r['close']},,{r['volume']},{r['volume']*r['close']},,,,,,,,,,,,,,,,,,,,"
                    rows.append(row_str)
                with open(fp, 'a') as f:
                    f.write('\n' + '\n'.join(rows))
                stats['updated'] += 1
                stats['new_rows'] += len(new_data)
        except:
            stats['err'] += 1
        
        if (i+1) % 200 == 0:
            print(f"  [{i+1}/{total}] 更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']}")
        time.sleep(0.06)
    
    summary = f"日线: 更新{stats['updated']}只(+{stats['new_rows']}条) 已最新{stats['uptodate']}只 错误{stats['err']}只 跳过{stats['skip']}只"
    print(f"  ✅ {summary}")
    REPORT['daily'] = stats
    return stats

def check_60_30():
    """快速确认60min/30min是否已是今天"""
    today = datetime.now().strftime('%Y-%m-%d')
    for p in ['60min', '30min']:
        d = os.path.join(DATA_DIR, p)
        files = glob.glob(os.path.join(d, '*.csv'))
        sample = get_local_latest(files[0]) if files else None
        status = "已最新" if sample and sample >= today else f"需更新(最新{sample})"
        print(f"  {p}: {len(files)}只 → {status}")
        REPORT[p] = {'total': len(files), 'status': 'uptodate' if sample and sample >= today else 'need_update', 'latest': sample}
    return all(REPORT.get(p, {}).get('status') == 'uptodate' for p in ['60min', '30min'])

if __name__ == '__main__':
    t0 = time.time()
    print("=" * 60)
    print(f"📊 每日数据增量更新 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    # 1) 快速检查60min/30min
    print("\n📋 检查60min/30min状态:")
    skip_60_30 = check_60_30()
    if skip_60_30:
        print("  → 60min/30min 已是最新，跳过")
    else:
        print("  → 需要更新60min或30min")
        # 如果需要，这里也可以更新
    
    # 2) 更新15min
    update_minutes('15min', '15m', 200)
    
    # 3) 更新日线
    update_daily()
    
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"✅ 全部完成! 耗时 {elapsed/60:.1f} 分钟")
    print(f"{'='*60}")
    
    # 输出JSON报告
    print("\n__REPORT_JSON__")
    print(json.dumps(REPORT, ensure_ascii=False, default=str))
