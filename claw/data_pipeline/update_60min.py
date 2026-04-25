#!/usr/bin/env python3
"""快速更新60min - 只有11只需更新"""
import sys, os, time, glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = '/Users/ecustkiller/Downloads/2026'
TODAY = datetime.now().strftime('%Y-%m-%d')

def get_local_latest(fp):
    try:
        with open(fp, 'rb') as f:
            f.seek(-300, 2)
            lines = f.readlines()
            return lines[-1].decode('utf-8').strip().split(',')[0]
    except:
        return None

full_dir = os.path.join(DATA_DIR, '60min')
files = sorted(glob.glob(os.path.join(full_dir, '*.csv')))

updated = 0
total_rows = 0
errors = 0
uptodate = 0

for fp in files:
    local = get_local_latest(fp)
    if not local or local >= TODAY:
        if local and local >= TODAY:
            uptodate += 1
        continue
    
    code = os.path.basename(fp).replace('.csv', '')
    try:
        df = get_price(code, frequency='60m', count=50)
        if df is None or len(df) == 0:
            errors += 1
            continue
        df = df.reset_index()
        df.columns = ['datetime'] + list(df.columns[1:])
        df['date'] = df['datetime'].astype(str).str[:10]
        df['time_'] = df['datetime'].astype(str).str[11:16].str.replace(':', '')
        new_data = df[df['date'] > local]
        if len(new_data) == 0:
            uptodate += 1
            continue
        rows = []
        for _, r in new_data.iterrows():
            h = r['time_'][:2] if r['time_'] else '00'
            m = r['time_'][2:4] if len(r['time_']) >= 4 else '00'
            rows.append(f"{r['date']},{h}:{m},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
        with open(fp, 'a') as f:
            f.write('\n' + '\n'.join(rows))
        updated += 1
        total_rows += len(new_data)
        print(f"  ✅ {code}: +{len(new_data)}条 (本地停在{local})")
    except Exception as e:
        errors += 1
        print(f"  ❌ {code}: {str(e)[:50]}")
    time.sleep(0.08)

print(f"\n60min汇总: 共{len(files)}只 | 更新{updated}只(+{total_rows}条) | 已最新{uptodate} | 错误{errors}")
