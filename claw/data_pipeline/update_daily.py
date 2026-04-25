#!/usr/bin/env python3
"""批量更新日线数据 - 5924只"""
import sys, os, time, glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'
TODAY_COMPACT = datetime.now().strftime('%Y%m%d')

def get_local_latest(fp):
    try:
        with open(fp, 'rb') as f:
            f.seek(-300, 2)
            lines = f.readlines()
            d = lines[-1].decode('utf-8').strip().split(',')[0]
            return d.replace('.0', '')
    except:
        return None

files = sorted(glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')))
total = len(files)
updated = 0
total_rows = 0
errors = 0
uptodate = 0
skipped = 0
err_list = []

print(f"日线更新: 共{total}只, 目标日期{TODAY_COMPACT}")

for i, fp in enumerate(files):
    fname = os.path.basename(fp)
    code_raw = fname.split('_')[0]
    if not code_raw.isdigit() or len(code_raw) != 6:
        skipped += 1
        continue
    
    local = get_local_latest(fp)
    if not local:
        errors += 1
        continue
    
    # 格式兼容：有些是2025-12-19格式，需要转为20251219
    if '-' in local:
        local = local.replace('-', '')
    
    if local >= TODAY_COMPACT:
        uptodate += 1
        continue
    
    if code_raw.startswith('6'):
        ashare_code = 'sh' + code_raw
    else:
        ashare_code = 'sz' + code_raw
    
    retry = 0
    while retry < 2:
        try:
            df = get_price(ashare_code, frequency='1d', count=30)
            if df is None or len(df) == 0:
                retry += 1
                if retry < 2:
                    time.sleep(3)
                continue
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date_str'] = df['datetime'].astype(str).str[:10].str.replace('-', '')
            new_data = df[df['date_str'] > local]
            if len(new_data) == 0:
                uptodate += 1
                break
            
            ts_code = code_raw + ('.SH' if code_raw.startswith('6') else '.SZ')
            rows = []
            for _, r in new_data.iterrows():
                row_str = f"{r['date_str']},{ts_code},{r['open']},{r['high']},{r['low']},{r['close']},,{r['volume']},{r['volume']*r['close']},,,,,,,,,,,,,,,,,,,,"
                rows.append(row_str)
            
            with open(fp, 'a') as f:
                f.write('\n' + '\n'.join(rows))
            
            updated += 1
            total_rows += len(new_data)
            break
        except Exception as e:
            retry += 1
            if retry < 2:
                time.sleep(3)
            else:
                errors += 1
                err_list.append(code_raw)
    
    if (i+1) % 500 == 0 or (i+1) == total:
        print(f"  进度 {i+1}/{total} | 更新{updated}只(+{total_rows}条) | 已最新{uptodate} | 错误{errors} | 跳过{skipped}")
        sys.stdout.flush()
    
    time.sleep(0.06)

print(f"\n日线汇总: 共{total}只 | 更新{updated}只(+{total_rows}条) | 已最新{uptodate} | 错误{errors} | 跳过{skipped}")
if err_list:
    print(f"  错误股票(前20): {err_list[:20]}")
