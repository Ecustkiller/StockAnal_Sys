#!/usr/bin/env python3
"""
每日股票数据增量更新脚本
- 用Ashare获取最新K线数据，追加到本地CSV
- 支持日线 + 60min/30min/15min四个周期
- 增量更新：只追加本地缺失的新数据
"""
import sys, os, time, glob
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

DATA_DIR = '/Users/ecustkiller/Downloads/2026'
STOCK_DATA_DIR = '/Users/ecustkiller/stock_data'

PERIODS = {
    '60min': ('60m', 50),   # (Ashare频率, 获取根数)
    '30min': ('30m', 100),
    '15min': ('15m', 200),
}

def get_local_latest_date(filepath):
    """读取本地CSV最后一行的日期"""
    try:
        with open(filepath, 'rb') as f:
            f.seek(-200, 2)  # 从末尾往回200字节
            lines = f.readlines()
            last_line = lines[-1].decode('utf-8').strip()
            if last_line:
                date_str = last_line.split(',')[0]  # 2026-03-16
                return date_str
    except:
        pass
    return None

def update_stock(code, period_dir, ashare_freq, count):
    """增量更新单只股票"""
    filepath = os.path.join(DATA_DIR, period_dir, f'{code}.csv')
    if not os.path.exists(filepath):
        return 'skip'
    
    local_latest = get_local_latest_date(filepath)
    if not local_latest:
        return 'err_read'
    
    try:
        df = get_price(code, frequency=ashare_freq, count=count)
        if df is None or len(df) == 0:
            return 'err_api'
        
        # 筛选新数据（日期大于本地最新日期）
        df = df.reset_index()
        df.columns = ['datetime'] + list(df.columns[1:])
        df['date'] = df['datetime'].astype(str).str[:10]
        df['time'] = df['datetime'].astype(str).str[11:16].str.replace(':', '')
        
        # 只保留比本地最新日期更新的数据
        new_data = df[df['date'] > local_latest]
        
        if len(new_data) == 0:
            return 'uptodate'
        
        # 格式化为本地CSV格式: date,time,open,high,low,close,volume,amount
        rows = []
        for _, r in new_data.iterrows():
            hour = r['time'][:2] if r['time'] else '00'
            minute = r['time'][2:4] if len(r['time']) >= 4 else '00'
            time_str = f"{hour}:{minute}"
            rows.append(f"{r['date']},{time_str},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")
        
        # 追加到文件
        with open(filepath, 'a') as f:
            f.write('\n')
            f.write('\n'.join(rows))
        
        return f'+{len(new_data)}'
    except Exception as e:
        return f'err:{str(e)[:30]}'

def main():
    print("=" * 70)
    print("📊 每日股票数据增量更新")
    print("=" * 70)
    
    for period_dir, (ashare_freq, count) in PERIODS.items():
        full_dir = os.path.join(DATA_DIR, period_dir)
        if not os.path.isdir(full_dir):
            print(f"\n⚠️ 目录不存在: {full_dir}")
            continue
        
        files = sorted(glob.glob(os.path.join(full_dir, '*.csv')))
        total = len(files)
        print(f"\n{'='*50}")
        print(f"更新 {period_dir} ({total}只股票, Ashare {ashare_freq})")
        print(f"{'='*50}")
        
        # 先检查一只看本地最新日期
        sample = files[0] if files else None
        if sample:
            sample_date = get_local_latest_date(sample)
            print(f"  本地最新: {sample_date}")
        
        stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'skip': 0}
        updated_count = 0
        
        for i, filepath in enumerate(files):
            code = os.path.basename(filepath).replace('.csv', '')
            result = update_stock(code, period_dir, ashare_freq, count)
            
            if result == 'uptodate':
                stats['uptodate'] += 1
            elif result.startswith('+'):
                stats['updated'] += 1
                updated_count += int(result[1:])
            elif result == 'skip':
                stats['skip'] += 1
            else:
                stats['err'] += 1
            
            if (i+1) % 100 == 0:
                print(f"  进度 {i+1}/{total} | 已更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']}")
            
            time.sleep(0.08)  # 控制API频率
        
        print(f"  ✅ 完成: 更新{stats['updated']}只(+{updated_count}条) 已最新{stats['uptodate']}只 错误{stats['err']}只 跳过{stats['skip']}只")
    
    print(f"\n{'='*70}")
    print("全部更新完成!")
    print(f"{'='*70}")


def update_daily():
    """更新stock_data日线数据"""
    print(f"\n{'='*70}")
    print("📈 更新日线数据 (stock_data)")
    print(f"{'='*70}")
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"⚠️ 目录不存在: {STOCK_DATA_DIR}")
        return
    
    files = sorted(glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')))
    total = len(files)
    print(f"  共{total}只股票")
    
    # 检查样本的最新日期
    if files:
        try:
            with open(files[0], 'rb') as f:
                f.seek(-300, 2)
                lines = f.readlines()
                last = lines[-1].decode('utf-8').strip()
                sample_date = last.split(',')[0]  # 20260403
                print(f"  本地最新: {sample_date}")
        except:
            sample_date = "unknown"
            print(f"  本地最新: 未知")
    
    stats = {'uptodate': 0, 'updated': 0, 'err': 0, 'skip': 0}
    updated_rows = 0
    
    for i, filepath in enumerate(files):
        fname = os.path.basename(filepath)
        # 从文件名提取代码: 000001_平安银行.csv -> 000001
        code_raw = fname.split('_')[0]
        if not code_raw.isdigit() or len(code_raw) != 6:
            stats['skip'] += 1
            continue
        
        # 转Ashare格式
        if code_raw.startswith('6'):
            ashare_code = 'sh' + code_raw
        else:
            ashare_code = 'sz' + code_raw
        
        # 读本地最新日期
        try:
            with open(filepath, 'rb') as f:
                f.seek(-300, 2)
                lines = f.readlines()
                last = lines[-1].decode('utf-8').strip()
                local_date = last.split(',')[0]  # 20260403格式
        except:
            stats['err'] += 1
            continue
        
        # 用Ashare获取最新日线
        try:
            df = get_price(ashare_code, frequency='1d', count=10)
            if df is None or len(df) == 0:
                stats['err'] += 1
                continue
            
            df = df.reset_index()
            df.columns = ['datetime'] + list(df.columns[1:])
            df['date_str'] = df['datetime'].astype(str).str[:10].str.replace('-', '')
            
            # 筛选比本地更新的数据
            new_data = df[df['date_str'] > local_date]
            
            if len(new_data) == 0:
                stats['uptodate'] += 1
            else:
                # 追加到CSV（保持原格式: date,code,open,high,low,close,preclose,volume,amount,...后面补空）
                ts_code = code_raw + ('.SH' if code_raw.startswith('6') else '.SZ')
                rows = []
                for _, r in new_data.iterrows():
                    # 计算preclose（用前一天的close）
                    row_str = f"{r['date_str']},{ts_code},{r['open']},{r['high']},{r['low']},{r['close']},,{r['volume']},{r['volume']*r['close']},,,,,,,,,,,,,,,,,,,,"
                    rows.append(row_str)
                
                with open(filepath, 'a') as f:
                    f.write('\n')
                    f.write('\n'.join(rows))
                
                stats['updated'] += 1
                updated_rows += len(new_data)
        except:
            stats['err'] += 1
        
        if (i+1) % 200 == 0:
            print(f"  进度 {i+1}/{total} | 更新{stats['updated']} 最新{stats['uptodate']} 错误{stats['err']}")
        
        time.sleep(0.08)
    
    print(f"  ✅ 日线完成: 更新{stats['updated']}只(+{updated_rows}条) 已最新{stats['uptodate']}只 错误{stats['err']}只 跳过{stats['skip']}只")


if __name__ == '__main__':
    main()
    update_daily()
