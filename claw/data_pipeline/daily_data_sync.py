#!/usr/bin/env python3
"""
每日市场数据同步脚本 v1.0

功能：
1. 拉取当日全市场快照（日线+PE/PB/市值+资金流向+stock_basic）存为单日文件
2. 更新 ~/stock_data/ 下个股CSV的分钟线（可选）

数据存储：
  ~/stock_data/daily_snapshot/
    ├── 20260415.parquet    ← 当日全市场快照（5500+只，含PE/资金等）
    ├── 20260414.parquet
    ├── ...
    └── stock_basic.parquet ← 股票基本信息（不常变，每周更新）

用法：
  python3 daily_data_sync.py                # 同步最新交易日
  python3 daily_data_sync.py 20260415       # 同步指定日期
  python3 daily_data_sync.py 20260410 20260415  # 批量同步日期范围
"""
import sys, os, time, json, requests
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
STOCK_DATA_DIR = os.path.expanduser("~/stock_data")

os.makedirs(SNAPSHOT_DIR, exist_ok=True)

def ts_api(api_name, **kwargs):
    params = {"api_name": api_name, "token": TOKEN, "params": kwargs, "fields": ""}
    for retry in range(3):
        try:
            r = requests.post("http://api.tushare.pro", json=params, timeout=30)
            d = r.json()
            if d.get('data') and d['data'].get('items'):
                return pd.DataFrame(d['data']['items'], columns=d['data']['fields'])
            if d.get('code') and d['code'] != 0:
                print(f"  API错误: {d.get('msg','')}")
            return pd.DataFrame()
        except Exception as e:
            if retry < 2:
                time.sleep(2)
            else:
                print(f"  API失败: {e}")
    return pd.DataFrame()

def get_latest_trade_date():
    """获取最新交易日"""
    df = ts_api("trade_cal", exchange="SSE", is_open="1", 
                start_date="20260401", end_date="20260430")
    if df.empty: return None
    import datetime
    today = datetime.datetime.now().strftime("%Y%m%d")
    dates = sorted(df['cal_date'].tolist())
    # 取不超过今天的最新交易日
    valid = [d for d in dates if d <= today]
    return valid[-1] if valid else dates[-1]

def sync_one_day(trade_date):
    """同步一天的全市场快照"""
    out_file = os.path.join(SNAPSHOT_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        print(f"  {trade_date} 已存在，跳过（删除文件可强制重新同步）")
        return True
    
    print(f"\n{'='*60}")
    print(f"同步 {trade_date}")
    print(f"{'='*60}")
    
    t0 = time.time()
    
    # 1. 日线数据
    print(f"  [1/4] 日线...", end="", flush=True)
    df_daily = ts_api("daily", trade_date=trade_date)
    if df_daily.empty:
        print(f" 无数据（非交易日或数据未入库）")
        return False
    print(f" {len(df_daily)}只 ({time.time()-t0:.1f}s)")
    
    # 2. daily_basic（PE/PB/市值/换手）
    t1 = time.time()
    print(f"  [2/4] PE/PB/市值...", end="", flush=True)
    df_basic = ts_api("daily_basic", trade_date=trade_date,
                       fields="ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate,turnover_rate_f")
    if not df_basic.empty:
        # 去掉与daily重复的列（trade_date/close等），只保留基本面字段
        dup_cols = [c for c in df_basic.columns if c in df_daily.columns and c != 'ts_code']
        df_basic = df_basic.drop(columns=dup_cols, errors='ignore')
        df_daily = df_daily.merge(df_basic, on='ts_code', how='left')
    print(f" {len(df_basic)}条 ({time.time()-t1:.1f}s)")
    
    # 3. 资金流向
    t2 = time.time()
    print(f"  [3/4] 资金流向...", end="", flush=True)
    df_mf = ts_api("moneyflow", trade_date=trade_date)
    if not df_mf.empty:
        # 计算净流入（中+大单）
        for col in ['buy_md_amount', 'buy_lg_amount', 'sell_md_amount', 'sell_lg_amount']:
            if col in df_mf.columns:
                df_mf[col] = pd.to_numeric(df_mf[col], errors='coerce').fillna(0)
        df_mf['net_mf_amount'] = (
            df_mf.get('buy_md_amount', 0) + df_mf.get('buy_lg_amount', 0)
            - df_mf.get('sell_md_amount', 0) - df_mf.get('sell_lg_amount', 0)
        )
        df_mf_slim = df_mf[['ts_code', 'net_mf_amount']].copy()
        df_daily = df_daily.merge(df_mf_slim, on='ts_code', how='left')
    print(f" {len(df_mf)}条 ({time.time()-t2:.1f}s)")
    
    # 4. stock_basic（名称+行业）
    t3 = time.time()
    print(f"  [4/4] 基本信息...", end="", flush=True)
    basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
    need_refresh = True
    if os.path.exists(basic_file):
        age = time.time() - os.path.getmtime(basic_file)
        if age < 7 * 86400:  # 7天内不刷新
            df_info = pd.read_parquet(basic_file)
            need_refresh = False
            print(f" 缓存 ({len(df_info)}只)", end="")
    
    if need_refresh:
        df_info = ts_api("stock_basic", exchange="", list_status="L",
                          fields="ts_code,name,industry,market,list_date")
        if not df_info.empty:
            df_info.to_parquet(basic_file, index=False)
            print(f" 更新 ({len(df_info)}只)", end="")
    
    if not df_info.empty:
        df_daily = df_daily.merge(df_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    print(f" ({time.time()-t3:.1f}s)")
    
    # 转数值类型
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg',
                'vol', 'amount', 'pe_ttm', 'pb', 'total_mv', 'circ_mv',
                'turnover_rate', 'turnover_rate_f', 'net_mf_amount']
    for col in num_cols:
        if col in df_daily.columns:
            df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')
    
    # 保存
    df_daily.to_parquet(out_file, index=False)
    size_mb = os.path.getsize(out_file) / 1024 / 1024
    
    total = time.time() - t0
    print(f"\n  ✅ 保存 {out_file}")
    print(f"  {len(df_daily)}只 | {size_mb:.1f}MB | 耗时{total:.1f}s")
    
    # 统计
    if 'pct_chg' in df_daily.columns:
        up = (df_daily['pct_chg'] > 0).sum()
        dn = (df_daily['pct_chg'] < 0).sum()
        zt = (df_daily['pct_chg'] >= 9.5).sum()
        print(f"  涨{up} 跌{dn} 涨停{zt}")
    
    return True

def sync_ashare_kline(trade_date):
    """用Ashare补充1分钟线到日线快照（可选，盘后日线还没入库时用）"""
    # 暂不实现，保留接口
    pass

def main():
    args = sys.argv[1:]
    
    if len(args) == 0:
        # 同步最新交易日
        date = get_latest_trade_date()
        if date:
            print(f"最新交易日: {date}")
            sync_one_day(date)
        else:
            print("无法获取最新交易日")
    
    elif len(args) == 1:
        # 同步指定日期
        sync_one_day(args[0])
    
    elif len(args) == 2:
        # 批量同步日期范围
        start, end = args
        df = ts_api("trade_cal", exchange="SSE", is_open="1",
                     start_date=start, end_date=end)
        if not df.empty:
            dates = sorted(df['cal_date'].tolist())
            print(f"将同步 {len(dates)} 个交易日: {dates[0]} ~ {dates[-1]}")
            for d in dates:
                sync_one_day(d)
                time.sleep(0.5)
        else:
            print("未找到交易日")
    
    print(f"\n完成！数据目录: {SNAPSHOT_DIR}")
    # 列出已有快照
    files = sorted([f for f in os.listdir(SNAPSHOT_DIR) if f.endswith('.parquet') and f != 'stock_basic.parquet'])
    print(f"已有快照: {len(files)}天")
    if files:
        print(f"  最早: {files[0][:8]} | 最新: {files[-1][:8]}")

if __name__ == '__main__':
    main()
