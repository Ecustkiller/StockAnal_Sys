#!/usr/bin/env python3
"""
批量下载2025年全年数据（用于回测）
包含：日线快照 + 量比数据 + 60分钟K线

用法：
  python3 download_2025_data.py              # 下载所有缺失数据
  python3 download_2025_data.py --daily      # 只下载日线快照
  python3 download_2025_data.py --60min      # 只下载60分钟K线
  python3 download_2025_data.py --vr         # 只下载量比数据
"""
import sys, os, time, argparse
import pandas as pd
import numpy as np
import requests

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
TOKEN2 = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
VR_DIR = os.path.expanduser("~/stock_data/volume_ratio")
KLINE_60M_DIR_2025 = os.path.expanduser("~/Downloads/2025/60min")

os.makedirs(SNAPSHOT_DIR, exist_ok=True)
os.makedirs(VR_DIR, exist_ok=True)
os.makedirs(KLINE_60M_DIR_2025, exist_ok=True)

def ts_api(api_name, token=TOKEN, **kwargs):
    """调用tushare API，带重试"""
    params = {"api_name": api_name, "token": token, "params": kwargs, "fields": ""}
    for retry in range(5):
        try:
            r = requests.post("http://api.tushare.pro", json=params, timeout=60)
            d = r.json()
            if d.get('data') and d['data'].get('items'):
                return pd.DataFrame(d['data']['items'], columns=d['data']['fields'])
            if d.get('code') and d['code'] != 0:
                msg = d.get('msg', '')
                if '每分钟' in msg or '频率' in msg or 'limit' in msg.lower():
                    wait = 15 * (retry + 1)
                    print(f"    频率限制，等待{wait}秒...", flush=True)
                    time.sleep(wait)
                    continue
                print(f"    API错误: {msg}")
            return pd.DataFrame()
        except Exception as e:
            if retry < 4:
                time.sleep(3 * (retry + 1))
            else:
                print(f"    API失败: {e}")
    return pd.DataFrame()


def get_trade_dates(start, end):
    """获取交易日列表"""
    df = ts_api("trade_cal", exchange="SSE", is_open="1",
                start_date=start, end_date=end)
    if df.empty:
        return []
    return sorted(df['cal_date'].tolist())


def sync_daily_snapshot(trade_date):
    """同步一天的日线快照（含daily + daily_basic + moneyflow + stock_basic）"""
    out_file = os.path.join(SNAPSHOT_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        return True

    # 1. 日线数据
    df_daily = ts_api("daily", trade_date=trade_date)
    if df_daily.empty:
        return False
    time.sleep(0.3)

    # 2. daily_basic
    df_basic = ts_api("daily_basic", trade_date=trade_date,
                       fields="ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate,turnover_rate_f")
    if not df_basic.empty:
        dup_cols = [c for c in df_basic.columns if c in df_daily.columns and c != 'ts_code']
        df_basic = df_basic.drop(columns=dup_cols, errors='ignore')
        df_daily = df_daily.merge(df_basic, on='ts_code', how='left')
    time.sleep(0.3)

    # 3. 资金流向
    df_mf = ts_api("moneyflow", trade_date=trade_date)
    if not df_mf.empty:
        for col in ['buy_md_amount', 'buy_lg_amount', 'sell_md_amount', 'sell_lg_amount']:
            if col in df_mf.columns:
                df_mf[col] = pd.to_numeric(df_mf[col], errors='coerce').fillna(0)
        df_mf['net_mf_amount'] = (
            df_mf.get('buy_md_amount', 0) + df_mf.get('buy_lg_amount', 0)
            - df_mf.get('sell_md_amount', 0) - df_mf.get('sell_lg_amount', 0)
        )
        df_mf_slim = df_mf[['ts_code', 'net_mf_amount']].copy()
        df_daily = df_daily.merge(df_mf_slim, on='ts_code', how='left')
    time.sleep(0.3)

    # 4. stock_basic
    basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
    if os.path.exists(basic_file):
        df_info = pd.read_parquet(basic_file)
    else:
        df_info = ts_api("stock_basic", exchange="", list_status="L",
                          fields="ts_code,name,industry,market,list_date")
        if not df_info.empty:
            df_info.to_parquet(basic_file, index=False)
        time.sleep(0.3)

    if not df_info.empty:
        df_daily = df_daily.merge(df_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')

    # 转数值类型
    num_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg',
                'vol', 'amount', 'pe_ttm', 'pb', 'total_mv', 'circ_mv',
                'turnover_rate', 'turnover_rate_f', 'net_mf_amount']
    for col in num_cols:
        if col in df_daily.columns:
            df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

    df_daily.to_parquet(out_file, index=False)
    return True


def sync_volume_ratio(trade_date):
    """同步一天的量比数据"""
    out_file = os.path.join(VR_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        return True

    df = ts_api("daily_basic", trade_date=trade_date,
                fields="ts_code,volume_ratio")
    if df.empty:
        return False

    df.to_parquet(out_file, index=False)
    return True


def download_60min_kline_batch(ts_code, start_date, end_date, token=TOKEN2):
    """下载单只股票的60分钟K线"""
    code6 = ts_code[:6]
    prefix = 'sh' if ts_code.endswith('.SH') else ('sz' if ts_code.endswith('.SZ') else 'bj')
    csv_file = os.path.join(KLINE_60M_DIR_2025, f"{prefix}{code6}.csv")

    if os.path.exists(csv_file):
        return True

    # 用tushare的stk_mins接口获取60分钟K线
    df = ts_api("stk_mins", token=token, ts_code=ts_code, freq="60min",
                start_date=start_date, end_date=end_date)
    if df.empty:
        return False

    # 转换格式：trade_time -> date, time
    df['trade_time'] = df['trade_time'].astype(str)
    df['date'] = df['trade_time'].str[:10]  # YYYY-MM-DD HH:MM:SS -> YYYY-MM-DD
    df['time'] = df['trade_time'].str[11:16]  # HH:MM

    # 保存为CSV（和现有60min格式一致）
    out_df = df[['date', 'time', 'open', 'high', 'low', 'close', 'vol', 'amount']].copy()
    out_df.columns = ['日期', '时间', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
    out_df = out_df.sort_values(['date', 'time']).reset_index(drop=True)
    # 重命名回英文（和现有格式一致，header=中文）
    out_df.to_csv(csv_file, index=False, encoding='utf-8')
    return True


def main():
    parser = argparse.ArgumentParser(description='批量下载2025年全年数据')
    parser.add_argument('--daily', action='store_true', help='只下载日线快照')
    parser.add_argument('--vr', action='store_true', help='只下载量比数据')
    parser.add_argument('--60min', dest='min60', action='store_true', help='只下载60分钟K线')
    parser.add_argument('--start', default='20241101', help='起始日期')
    parser.add_argument('--end', default='20251130', help='结束日期')
    args = parser.parse_args()

    do_all = not (args.daily or args.vr or args.min60)

    print("=" * 80)
    print("📥 批量下载2025年全年数据（用于回测）")
    print("=" * 80)

    # 获取交易日列表
    dates = get_trade_dates(args.start, args.end)
    print(f"交易日: {len(dates)}天 ({dates[0]} ~ {dates[-1]})")

    # ===== 1. 日线快照 =====
    if do_all or args.daily:
        existing = set(f.replace('.parquet', '') for f in os.listdir(SNAPSHOT_DIR)
                       if f.endswith('.parquet') and f != 'stock_basic.parquet')
        need = [d for d in dates if d not in existing]
        print(f"\n📊 日线快照: 已有{len(dates)-len(need)}天, 需下载{len(need)}天")

        for i, d in enumerate(need):
            print(f"  [{i+1}/{len(need)}] {d}...", end="", flush=True)
            t0 = time.time()
            ok = sync_daily_snapshot(d)
            if ok:
                print(f" ✅ ({time.time()-t0:.1f}s)")
            else:
                print(f" ❌ 失败")
            time.sleep(0.5)

    # ===== 2. 量比数据 =====
    if do_all or args.vr:
        existing_vr = set(f.replace('.parquet', '') for f in os.listdir(VR_DIR)
                          if f.endswith('.parquet'))
        need_vr = [d for d in dates if d not in existing_vr]
        print(f"\n📊 量比数据: 已有{len(dates)-len(need_vr)}天, 需下载{len(need_vr)}天")

        for i, d in enumerate(need_vr):
            print(f"  [{i+1}/{len(need_vr)}] {d}...", end="", flush=True)
            t0 = time.time()
            ok = sync_volume_ratio(d)
            if ok:
                print(f" ✅ ({time.time()-t0:.1f}s)")
            else:
                print(f" ❌ 失败")
            time.sleep(0.3)

    # ===== 3. 60分钟K线 =====
    if do_all or args.min60:
        print(f"\n📊 60分钟K线下载")
        print(f"  注意: tushare stk_mins接口有积分限制，可能需要较长时间")
        print(f"  目标目录: {KLINE_60M_DIR_2025}")

        # 获取股票列表
        basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
        if os.path.exists(basic_file):
            df_basic = pd.read_parquet(basic_file)
            codes = df_basic[df_basic['ts_code'].str.match(r'^(00|30|60|68)')]['ts_code'].tolist()
        else:
            # 从API获取
            df_basic = ts_api("stock_basic", exchange="", list_status="L",
                              fields="ts_code,name,industry,market,list_date")
            codes = df_basic[df_basic['ts_code'].str.match(r'^(00|30|60|68)')]['ts_code'].tolist()

        # 检查已有
        existing_60 = set(os.listdir(KLINE_60M_DIR_2025))
        need_codes = []
        for code in codes:
            code6 = code[:6]
            prefix = 'sh' if code.endswith('.SH') else ('sz' if code.endswith('.SZ') else 'bj')
            fname = f"{prefix}{code6}.csv"
            if fname not in existing_60:
                need_codes.append(code)

        print(f"  总股票: {len(codes)}只, 已有: {len(codes)-len(need_codes)}只, 需下载: {len(need_codes)}只")

        if need_codes:
            print(f"  预计耗时: {len(need_codes) * 2 / 60:.0f}分钟（每只约2秒）")
            for i, code in enumerate(need_codes):
                if (i + 1) % 100 == 0 or i == 0:
                    print(f"  [{i+1}/{len(need_codes)}] {code}...", flush=True)
                ok = download_60min_kline_batch(code, "2025-01-01", "2025-12-31")
                time.sleep(1.5)  # 频率控制

    print(f"\n✅ 全部完成！")


if __name__ == '__main__':
    main()
