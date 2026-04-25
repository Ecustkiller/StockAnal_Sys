#!/usr/bin/env python3
"""
只更新60min分钟线数据（精简版，带 flush）
"""
import sys, os, time, glob
from datetime import datetime

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = os.path.expanduser('~/Downloads/2026/60min')

def log(msg):
    print(msg, flush=True)

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

def update_60min():
    files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))
    total = len(files)
    log(f"开始更新 {total} 只股票的60min数据...")
    
    if files:
        sample_date = get_local_latest(files[0])
        log(f"本地最新: {sample_date}")
    
    stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'new_rows': 0}
    t0 = time.time()
    
    for i, fp in enumerate(files):
        code = os.path.basename(fp).replace('.csv', '')
        local_latest = get_local_latest(fp)
        if not local_latest:
            stats['err'] += 1
            continue
        
        try:
            df = get_price(code, frequency='60m', count=60)
            if df is None or len(df) == 0:
                stats['err'] += 1
                continue
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date'] = df['datetime'].astype(str).str[:10]
            df['time'] = df['datetime'].astype(str).str[11:16]
            new_data = df[df['date'] > local_latest]
            
            if len(new_data) == 0:
                stats['uptodate'] += 1
            else:
                rows = []
                for _, r in new_data.iterrows():
                    rows.append(f"{r['date']},{r['time']},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
                with open(fp, 'a') as f:
                    f.write('\n' + '\n'.join(rows))
                stats['updated'] += 1
                stats['new_rows'] += len(new_data)
        except Exception as e:
            stats['err'] += 1
        
        if (i+1) % 300 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            log(f"[{i+1}/{total}] 更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']} | {elapsed/60:.1f}min ETA {eta/60:.1f}min")
        
        time.sleep(0.03)
    
    elapsed = time.time() - t0
    log(f"✅ 60min更新完成！耗时 {elapsed/60:.1f} 分钟")
    log(f"   更新: {stats['updated']} 只 (+{stats['new_rows']} 行)")
    log(f"   已最新: {stats['uptodate']} 只")
    log(f"   错误: {stats['err']} 只")
    return stats

if __name__ == '__main__':
    log(f"📊 60min更新 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    update_60min()
    log(f"🎉 全部完成 {datetime.now().strftime('%H:%M:%S')}")
