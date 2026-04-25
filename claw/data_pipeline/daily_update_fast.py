#!/usr/bin/env python3
"""
快速增量更新脚本 - 优化版
1. 先扫描所有文件找出需要更新的
2. 只对需要更新的文件调API
3. 带重试逻辑
"""
import sys, os, time, glob, json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = '/Users/ecustkiller/Downloads/2026'
STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'
TODAY = datetime.now().strftime('%Y-%m-%d')
TODAY_COMPACT = datetime.now().strftime('%Y%m%d')

PERIODS = {
    '60min': ('60m', 50),
    '30min': ('30m', 100),
    '15min': ('15m', 200),
}

def get_local_latest(filepath, compact=False):
    """读取本地CSV最后一行的日期"""
    try:
        with open(filepath, 'rb') as f:
            f.seek(-300, 2)
            lines = f.readlines()
            last = lines[-1].decode('utf-8').strip()
            if last:
                date_str = last.split(',')[0]
                if compact:
                    return date_str.replace('.0', '')  # 处理20260403.0的情况
                return date_str
    except:
        pass
    return None

def scan_needs_update(directory, target_date):
    """扫描目录，返回需要更新的文件列表"""
    files = sorted(glob.glob(os.path.join(directory, '*.csv')))
    needs_update = []
    up_to_date = 0
    err = 0
    for f in files:
        local = get_local_latest(f, compact=(directory == STOCK_DATA_DIR))
        if local is None:
            err += 1
        elif local < target_date:
            needs_update.append((f, local))
        else:
            up_to_date += 1
    return needs_update, up_to_date, err, len(files)

def update_minute_stock(filepath, code, ashare_freq, count, max_retry=2):
    """更新单只分钟线股票，带重试"""
    for attempt in range(max_retry):
        try:
            df = get_price(code, frequency=ashare_freq, count=count)
            if df is None or len(df) == 0:
                if attempt < max_retry - 1:
                    time.sleep(2)
                    continue
                return 0, 'err_api'
            
            local_latest = get_local_latest(filepath)
            if not local_latest:
                return 0, 'err_read'
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date'] = df['datetime'].astype(str).str[:10]
            df['time'] = df['datetime'].astype(str).str[11:16].str.replace(':', '')
            
            new_data = df[df['date'] > local_latest]
            if len(new_data) == 0:
                return 0, 'uptodate'
            
            rows = []
            for _, r in new_data.iterrows():
                hour = r['time'][:2] if r['time'] else '00'
                minute = r['time'][2:4] if len(r['time']) >= 4 else '00'
                time_str = f"{hour}:{minute}"
                rows.append(f"{r['date']},{time_str},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
            
            with open(filepath, 'a') as f:
                f.write('\n')
                f.write('\n'.join(rows))
            
            return len(new_data), 'ok'
        except Exception as e:
            if attempt < max_retry - 1:
                time.sleep(2)
                continue
            return 0, f'err:{str(e)[:30]}'

def update_daily_stock(filepath, code_raw, max_retry=2):
    """更新单只日线股票，带重试"""
    if code_raw.startswith('6'):
        ashare_code = 'sh' + code_raw
    else:
        ashare_code = 'sz' + code_raw
    
    local_date = get_local_latest(filepath, compact=True)
    if not local_date:
        return 0, 'err_read'
    
    for attempt in range(max_retry):
        try:
            df = get_price(ashare_code, frequency='1d', count=30)
            if df is None or len(df) == 0:
                if attempt < max_retry - 1:
                    time.sleep(2)
                    continue
                return 0, 'err_api'
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date_str'] = df['datetime'].astype(str).str[:10].str.replace('-', '')
            
            new_data = df[df['date_str'] > local_date]
            if len(new_data) == 0:
                return 0, 'uptodate'
            
            ts_code = code_raw + ('.SH' if code_raw.startswith('6') else '.SZ')
            rows = []
            for _, r in new_data.iterrows():
                row_str = f"{r['date_str']},{ts_code},{r['open']},{r['high']},{r['low']},{r['close']},,{r['volume']},{r['volume']*r['close']},,,,,,,,,,,,,,,,,,,,"
                rows.append(row_str)
            
            with open(filepath, 'a') as f:
                f.write('\n')
                f.write('\n'.join(rows))
            
            return len(new_data), 'ok'
        except Exception as e:
            if attempt < max_retry - 1:
                time.sleep(2)
                continue
            return 0, f'err:{str(e)[:30]}'


def main():
    results = {}
    
    # ===== 分钟线更新 =====
    for period_dir, (ashare_freq, count) in PERIODS.items():
        full_dir = os.path.join(DATA_DIR, period_dir)
        if not os.path.isdir(full_dir):
            print(f"⚠️ {period_dir} 目录不存在")
            continue
        
        print(f"\n📊 扫描 {period_dir}...")
        needs, uptodate, scan_err, total = scan_needs_update(full_dir, TODAY)
        print(f"  共{total}只 | 需更新:{len(needs)} | 已最新:{uptodate} | 读取错误:{scan_err}")
        
        if not needs:
            results[period_dir] = {
                'total': total, 'updated': 0, 'rows': 0,
                'uptodate': uptodate, 'err': scan_err, 'skip': 0
            }
            print(f"  ✅ {period_dir} 全部已是最新，无需更新")
            continue
        
        updated = 0
        total_rows = 0
        errors = scan_err
        
        for i, (fp, local_date) in enumerate(needs):
            code = os.path.basename(fp).replace('.csv', '')
            n_rows, status = update_minute_stock(fp, code, ashare_freq, count)
            
            if status == 'ok':
                updated += 1
                total_rows += n_rows
            elif status != 'uptodate':
                errors += 1
            else:
                uptodate += 1
            
            if (i+1) % 200 == 0 or (i+1) == len(needs):
                print(f"  进度 {i+1}/{len(needs)} | 已更新{updated}只(+{total_rows}条) 错误{errors}")
            
            time.sleep(0.06)
        
        results[period_dir] = {
            'total': total, 'updated': updated, 'rows': total_rows,
            'uptodate': uptodate, 'err': errors, 'skip': 0
        }
        print(f"  ✅ {period_dir} 完成: 更新{updated}只(+{total_rows}条)")
    
    # ===== 日线更新 =====
    print(f"\n📈 扫描日线数据...")
    needs, uptodate, scan_err, total = scan_needs_update(STOCK_DATA_DIR, TODAY_COMPACT)
    print(f"  共{total}只 | 需更新:{len(needs)} | 已最新:{uptodate} | 读取错误:{scan_err}")
    
    if not needs:
        results['daily'] = {
            'total': total, 'updated': 0, 'rows': 0,
            'uptodate': uptodate, 'err': scan_err, 'skip': 0
        }
        print(f"  ✅ 日线全部已是最新")
    else:
        updated = 0
        total_rows = 0
        errors = scan_err
        skipped = 0
        
        for i, (fp, local_date) in enumerate(needs):
            fname = os.path.basename(fp)
            code_raw = fname.split('_')[0]
            if not code_raw.isdigit() or len(code_raw) != 6:
                skipped += 1
                continue
            
            n_rows, status = update_daily_stock(fp, code_raw)
            
            if status == 'ok':
                updated += 1
                total_rows += n_rows
            elif status != 'uptodate':
                errors += 1
            else:
                uptodate += 1
            
            if (i+1) % 500 == 0 or (i+1) == len(needs):
                print(f"  进度 {i+1}/{len(needs)} | 已更新{updated}只(+{total_rows}条) 错误{errors}")
            
            time.sleep(0.06)
        
        results['daily'] = {
            'total': total, 'updated': updated, 'rows': total_rows,
            'uptodate': uptodate, 'err': errors, 'skip': skipped
        }
        print(f"  ✅ 日线完成: 更新{updated}只(+{total_rows}条)")
    
    # ===== 汇总 =====
    print(f"\n{'='*60}")
    print(f"📋 更新汇总 ({TODAY})")
    print(f"{'='*60}")
    for k, v in results.items():
        print(f"  {k:8s}: 共{v['total']}只 | 更新{v['updated']}只(+{v['rows']}条) | 已最新{v['uptodate']} | 错误{v['err']} | 跳过{v['skip']}")
    print(f"{'='*60}")
    
    # 输出JSON供外部解析
    print(f"\n__RESULT_JSON__")
    print(json.dumps(results, ensure_ascii=False))

if __name__ == '__main__':
    main()
