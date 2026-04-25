#!/usr/bin/env python3
"""批量更新15min数据 - 5490只全部需要更新(4/15->4/16)"""
import sys, os, time, glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = '/Users/ecustkiller/Downloads/2026/15min'
TODAY = datetime.now().strftime('%Y-%m-%d')

def get_local_latest(fp):
    try:
        with open(fp, 'rb') as f:
            f.seek(-300, 2)
            lines = f.readlines()
            return lines[-1].decode('utf-8').strip().split(',')[0]
    except:
        return None

files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))
total = len(files)
updated = 0
total_rows = 0
errors = 0
uptodate = 0
err_list = []

print(f"15min更新: 共{total}只, 目标日期{TODAY}")

for i, fp in enumerate(files):
    local = get_local_latest(fp)
    if not local:
        errors += 1
        continue
    if local >= TODAY:
        uptodate += 1
        continue
    
    code = os.path.basename(fp).replace('.csv', '')
    retry = 0
    success = False
    while retry < 2:
        try:
            df = get_price(code, frequency='15m', count=200)
            if df is None or len(df) == 0:
                retry += 1
                if retry < 2:
                    time.sleep(3)
                continue
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date'] = df['datetime'].astype(str).str[:10]
            df['time_'] = df['datetime'].astype(str).str[11:16].str.replace(':', '')
            new_data = df[df['date'] > local]
            if len(new_data) == 0:
                uptodate += 1
                success = True
                break
            rows = []
            for _, r in new_data.iterrows():
                h = r['time_'][:2] if r['time_'] else '00'
                m = r['time_'][2:4] if len(r['time_']) >= 4 else '00'
                rows.append(f"{r['date']},{h}:{m},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
            with open(fp, 'a') as f:
                f.write('\n' + '\n'.join(rows))
            updated += 1
            total_rows += len(new_data)
            success = True
            break
        except Exception as e:
            retry += 1
            if retry < 2:
                time.sleep(3)
            else:
                errors += 1
                err_list.append(code)
    
    if (i+1) % 500 == 0 or (i+1) == total:
        print(f"  进度 {i+1}/{total} | 更新{updated}只(+{total_rows}条) | 已最新{uptodate} | 错误{errors}")
        sys.stdout.flush()
    
    time.sleep(0.06)

print(f"\n15min汇总: 共{total}只 | 更新{updated}只(+{total_rows}条) | 已最新{uptodate} | 错误{errors}")
if err_list:
    print(f"  错误股票: {err_list[:20]}")
