#!/usr/bin/env python3
"""
只更新日线数据（精简版，带 flush）
跳过分钟线，只为评分系统准备日线数据
"""
import sys, os, time, glob, json
from datetime import datetime

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'

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

def update_daily():
    files = sorted(glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')))
    total = len(files)
    log(f"开始更新 {total} 只股票的日线...")
    
    if files:
        sample_date = get_local_latest(files[0])
        log(f"本地最新: {sample_date}")
    
    stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'skip': 0, 'new_rows': 0}
    t0 = time.time()
    
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
            df = get_price(ashare_code, frequency='1d', count=10)
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
        except Exception as e:
            stats['err'] += 1
        
        if (i+1) % 300 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            log(f"[{i+1}/{total}] 更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']} | {elapsed/60:.1f}min ETA {eta/60:.1f}min")
        
        time.sleep(0.03)  # 加快一倍
    
    elapsed = time.time() - t0
    log(f"✅ 日线更新完成！耗时 {elapsed/60:.1f} 分钟")
    log(f"   更新: {stats['updated']} 只 (+{stats['new_rows']} 行)")
    log(f"   已最新: {stats['uptodate']} 只")
    log(f"   错误: {stats['err']} 只 | 跳过: {stats['skip']} 只")
    return stats

if __name__ == '__main__':
    log(f"📊 日线更新 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    update_daily()
    log(f"🎉 全部完成 {datetime.now().strftime('%H:%M:%S')}")
