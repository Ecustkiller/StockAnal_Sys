#!/usr/bin/env python3
"""
历史数据批量下载 — 支持多年份、断点续传、后台运行
================================================================
下载三类数据：
  1. 日线快照 (daily_snapshot) — Tushare daily + daily_basic + moneyflow + stock_basic
  2. 量比数据 (volume_ratio) — Tushare daily_basic volume_ratio字段
  3. 60分钟K线 (60min) — baostock（免费、无限频、按日期范围精确查询）

支持功能：
  - 断点续传：已下载的文件自动跳过
  - 多年份：可指定任意年份范围
  - 后台运行：nohup 模式，日志输出到文件
  - 进度报告：定期输出进度和ETA
  - 频率控制：自动处理API限频

用法：
  python3 download_historical.py                    # 下载2024年全部数据
  python3 download_historical.py --year 2023        # 下载2023年
  python3 download_historical.py --year 2022-2024   # 下载2022~2024年
  python3 download_historical.py --daily            # 只下载日线快照
  python3 download_historical.py --60min            # 只下载60分钟K线
  python3 download_historical.py --vr               # 只下载量比数据
  python3 download_historical.py --status           # 查看所有年份下载进度

后台运行：
  nohup python3 download_historical.py --year 2022-2024 > download.log 2>&1 &
================================================================
"""

import sys, os, time, argparse, json, glob
import pandas as pd
import numpy as np
import requests
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================
TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
VR_DIR = os.path.expanduser("~/stock_data/volume_ratio")
KLINE_60M_BASE = os.path.expanduser("~/Downloads")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(BASE_DIR, ".download_progress.json")

os.makedirs(SNAPSHOT_DIR, exist_ok=True)
os.makedirs(VR_DIR, exist_ok=True)


# ============================================================
# Tushare API（带重试+频率控制）
# ============================================================
def ts_api(api_name, token=TOKEN, max_retry=5, **kwargs):
    """调用tushare API，带重试和频率控制"""
    params = {"api_name": api_name, "token": token, "params": kwargs, "fields": ""}
    for retry in range(max_retry):
        try:
            r = requests.post("http://api.tushare.pro", json=params, timeout=60)
            d = r.json()
            if d.get('data') and d['data'].get('items'):
                return pd.DataFrame(d['data']['items'], columns=d['data']['fields'])
            if d.get('code') and d['code'] != 0:
                msg = d.get('msg', '')
                if '每分钟' in msg or '频率' in msg or 'limit' in msg.lower():
                    wait = 15 * (retry + 1)
                    print(f"    ⏳ 频率限制，等待{wait}秒...", flush=True)
                    time.sleep(wait)
                    continue
                if retry < max_retry - 1:
                    time.sleep(3)
                    continue
                print(f"    ❌ API错误: {msg}")
            return pd.DataFrame()
        except Exception as e:
            if retry < max_retry - 1:
                time.sleep(3 * (retry + 1))
            else:
                print(f"    ❌ API失败: {e}")
    return pd.DataFrame()


# ============================================================
# 交易日历
# ============================================================
def get_trade_dates(start, end):
    """获取交易日列表"""
    df = ts_api("trade_cal", exchange="SSE", is_open="1",
                start_date=start, end_date=end)
    if df.empty:
        return []
    return sorted(df['cal_date'].tolist())


# ============================================================
# 日线快照下载
# ============================================================
def sync_daily_snapshot(trade_date):
    """同步一天的日线快照"""
    out_file = os.path.join(SNAPSHOT_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        return True, 'skip'

    # 1. 日线数据
    df_daily = ts_api("daily", trade_date=trade_date)
    if df_daily.empty:
        return False, 'empty'
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
    return True, f'{len(df_daily)}只'


# ============================================================
# 量比数据下载
# ============================================================
def sync_volume_ratio(trade_date):
    """同步一天的量比数据"""
    out_file = os.path.join(VR_DIR, f"{trade_date}.parquet")
    if os.path.exists(out_file):
        return True, 'skip'

    df = ts_api("daily_basic", trade_date=trade_date,
                fields="ts_code,volume_ratio")
    if df.empty:
        return False, 'empty'

    df.to_parquet(out_file, index=False)
    return True, f'{len(df)}只'


# ============================================================
# 60分钟K线下载（baostock — 免费、无限频、按日期范围精确查询）
# ============================================================
def _local_code_to_bs(code):
    """将本地代码格式(sh600000/sz000001)转为baostock格式(sh.600000/sz.000001)"""
    prefix = code[:2]  # sh / sz / bj
    num = code[2:]
    return f"{prefix}.{num}"


def _bs_time_to_hhmm(bs_time):
    """将baostock时间(20240102103000000)转为HH:MM格式(10:30)"""
    # 格式: YYYYMMDDHHmmSSsss
    return f"{bs_time[8:10]}:{bs_time[10:12]}"


def download_60min_for_year(year, stock_codes=None):
    """使用baostock下载指定年份的60分钟K线（免费、无限频、速度快）"""
    output_dir = os.path.join(KLINE_60M_BASE, str(year), '60min')
    os.makedirs(output_dir, exist_ok=True)

    year_start = f'{year}-01-01'
    year_end = f'{year}-12-31'

    # 获取股票列表
    if stock_codes is None:
        # 优先从2026年60min目录获取（最全的列表）
        ref_dir = os.path.join(KLINE_60M_BASE, '2026', '60min')
        if os.path.exists(ref_dir):
            stock_codes = sorted([f.replace('.csv', '') for f in os.listdir(ref_dir) if f.endswith('.csv')])
        else:
            # 从stock_basic获取
            basic_file = os.path.join(SNAPSHOT_DIR, "stock_basic.parquet")
            if os.path.exists(basic_file):
                df_basic = pd.read_parquet(basic_file)
                codes_ts = df_basic[df_basic['ts_code'].str.match(r'^(00|30|60|68)')]['ts_code'].tolist()
                stock_codes = []
                for code in codes_ts:
                    code6 = code[:6]
                    prefix = 'sh' if code.endswith('.SH') else ('sz' if code.endswith('.SZ') else 'bj')
                    stock_codes.append(f"{prefix}{code6}")
            else:
                print("  ❌ 无法获取股票列表")
                return 0, 0, 0

    # baostock 不支持北交所(bj开头)，过滤掉
    stock_codes_filtered = [c for c in stock_codes if c.startswith('sh') or c.startswith('sz')]
    bj_skipped = len(stock_codes) - len(stock_codes_filtered)
    if bj_skipped > 0:
        print(f"  ⚠️ 跳过{bj_skipped}只北交所股票（baostock不支持北交所）")

    # 检查已有
    existing = set(os.listdir(output_dir))
    need = [c for c in stock_codes_filtered if f"{c}.csv" not in existing]

    print(f"  60分钟K线 {year}年: 总{len(stock_codes_filtered)}只(沪深), 已有{len(stock_codes_filtered)-len(need)}只, 需下载{len(need)}只")

    if not need:
        print(f"  ✅ {year}年60分钟K线已全部下载完成！")
        return len(stock_codes_filtered), 0, 0

    # 导入并登录baostock
    try:
        import baostock as bs
    except ImportError:
        print("  ❌ baostock未安装，请执行: pip3 install baostock")
        return 0, 0, len(need)

    lg = bs.login()
    if lg.error_code != '0':
        print(f"  ❌ baostock登录失败: {lg.error_msg}")
        return 0, 0, len(need)
    print(f"  ✅ baostock已登录")

    # 预估时间：baostock约0.87秒/只
    est_min = len(need) * 0.87 / 60
    print(f"  预计耗时: {est_min:.0f}分钟 ({est_min/60:.1f}小时)")

    ok_count = 0
    err_count = 0
    empty_count = 0
    total_rows = 0
    t_start = time.time()

    for i, code in enumerate(need):
        output_path = os.path.join(output_dir, f"{code}.csv")
        bs_code = _local_code_to_bs(code)

        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,time,code,open,high,low,close,volume,amount",
                start_date=year_start,
                end_date=year_end,
                frequency="60",
                adjustflag="3"  # 不复权（与现有数据一致）
            )

            if rs.error_code != '0':
                err_count += 1
                if i < 10 or (i + 1) % 500 == 0:  # 前10只或每500只打印错误
                    print(f"    ❌ {code}: {rs.error_msg}", flush=True)
                continue

            # 读取数据
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                empty_count += 1
                continue

            # 转为CSV格式（与现有格式一致）
            # baostock返回: date, time(YYYYMMDDHHmmSSsss), code, open, high, low, close, volume, amount
            rows = []
            for row in data_list:
                date_str = row[0]           # 2024-01-02
                time_str = _bs_time_to_hhmm(row[1])  # 10:30
                open_p = row[3]
                high_p = row[4]
                low_p = row[5]
                close_p = row[6]
                volume = row[7]
                amount = row[8]
                rows.append(f"{date_str},{time_str},{open_p},{high_p},{low_p},{close_p},{volume},{amount}")

            with open(output_path, 'w') as f:
                f.write('日期,时间,开盘,最高,最低,收盘,成交量,成交额\n')
                f.write('\n'.join(rows))

            ok_count += 1
            total_rows += len(data_list)

        except Exception as e:
            err_count += 1
            if i < 10:  # 前10只打印错误详情
                print(f"    ❌ {code} 异常: {e}", flush=True)

        # 进度报告（每200只或最后一只时输出）
        if (i + 1) % 200 == 0 or (i + 1) == len(need):
            elapsed = time.time() - t_start
            speed = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(need) - i - 1) / speed / 60 if speed > 0 else 0
            print(f"  [{i+1}/{len(need)}] ✅{ok_count} ❌{err_count} ⬜{empty_count} | "
                  f"{total_rows}条 | {elapsed/60:.1f}min | ETA {eta:.1f}min", flush=True)

    # 登出baostock
    bs.logout()

    elapsed = time.time() - t_start
    print(f"  📊 {year}年60分钟K线完成: ✅{ok_count} ❌{err_count} ⬜{empty_count} | "
          f"{total_rows}条 | 耗时{elapsed/60:.1f}分钟")

    return ok_count, err_count, empty_count


# ============================================================
# 进度管理
# ============================================================
def save_progress(year, data_type, done, total, status='running'):
    """保存下载进度"""
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    key = f"{year}_{data_type}"
    progress[key] = {
        'year': year, 'type': data_type,
        'done': done, 'total': total,
        'status': status,
        'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def show_status():
    """显示所有年份的下载进度"""
    print("\n" + "=" * 100)
    print("📊 历史数据下载进度总览")
    print("=" * 100)

    # 日线快照
    snapshot_files = sorted([f.replace('.parquet', '') for f in os.listdir(SNAPSHOT_DIR)
                             if f.endswith('.parquet') and f != 'stock_basic.parquet'])
    if snapshot_files:
        # 按年份统计
        year_counts = {}
        for f in snapshot_files:
            y = f[:4]
            year_counts[y] = year_counts.get(y, 0) + 1
        print(f"\n📁 日线快照 ({SNAPSHOT_DIR})")
        print(f"   总计: {len(snapshot_files)}天 ({snapshot_files[0]} ~ {snapshot_files[-1]})")
        for y in sorted(year_counts.keys()):
            print(f"   {y}年: {year_counts[y]}天")

    # 量比数据
    vr_files = sorted([f.replace('.parquet', '') for f in os.listdir(VR_DIR)
                       if f.endswith('.parquet')])
    if vr_files:
        year_counts = {}
        for f in vr_files:
            y = f[:4]
            year_counts[y] = year_counts.get(y, 0) + 1
        print(f"\n📁 量比数据 ({VR_DIR})")
        print(f"   总计: {len(vr_files)}天 ({vr_files[0]} ~ {vr_files[-1]})")
        for y in sorted(year_counts.keys()):
            print(f"   {y}年: {year_counts[y]}天")

    # 60分钟K线
    print(f"\n📁 60分钟K线 ({KLINE_60M_BASE})")
    for year in range(2021, 2027):
        kline_dir = os.path.join(KLINE_60M_BASE, str(year), '60min')
        if os.path.exists(kline_dir):
            cnt = len([f for f in os.listdir(kline_dir) if f.endswith('.csv')])
            size = sum(os.path.getsize(os.path.join(kline_dir, f))
                       for f in os.listdir(kline_dir) if f.endswith('.csv'))
            print(f"   {year}年: {cnt}只 ({size/1024/1024:.1f}MB)")
        else:
            print(f"   {year}年: 未下载")

    # 回测数据覆盖范围
    print(f"\n📊 回测可用范围:")
    if snapshot_files:
        print(f"   日线: {snapshot_files[0]} ~ {snapshot_files[-1]} ({len(snapshot_files)}天)")
    for year in range(2021, 2027):
        kline_dir = os.path.join(KLINE_60M_BASE, str(year), '60min')
        if os.path.exists(kline_dir):
            cnt = len([f for f in os.listdir(kline_dir) if f.endswith('.csv')])
            if cnt > 0:
                print(f"   60min {year}年: {cnt}只 ✅")

    # 磁盘空间
    print(f"\n💾 磁盘空间:")
    for path, label in [
        (SNAPSHOT_DIR, "日线快照"),
        (VR_DIR, "量比数据"),
    ]:
        if os.path.exists(path):
            size = sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path))
            print(f"   {label}: {size/1024/1024:.1f}MB")
    for year in range(2021, 2027):
        kline_dir = os.path.join(KLINE_60M_BASE, str(year), '60min')
        if os.path.exists(kline_dir):
            size = sum(os.path.getsize(os.path.join(kline_dir, f))
                       for f in os.listdir(kline_dir) if f.endswith('.csv'))
            if size > 0:
                print(f"   60min {year}年: {size/1024/1024:.1f}MB")

    print()


# ============================================================
# 主程序
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='历史数据批量下载')
    parser.add_argument('--year', default='2024', help='年份或范围，如 2024 或 2022-2024')
    parser.add_argument('--daily', action='store_true', help='只下载日线快照')
    parser.add_argument('--vr', action='store_true', help='只下载量比数据')
    parser.add_argument('--60min', dest='min60', action='store_true', help='只下载60分钟K线')
    parser.add_argument('--status', action='store_true', help='查看下载进度')
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    # 解析年份
    if '-' in args.year:
        parts = args.year.split('-')
        years = list(range(int(parts[0]), int(parts[1]) + 1))
    else:
        years = [int(args.year)]

    do_all = not (args.daily or args.vr or args.min60)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("=" * 100)
    print(f"📥 历史数据批量下载")
    print(f"   时间: {now}")
    print(f"   年份: {', '.join(str(y) for y in years)}")
    print(f"   类型: {'全部' if do_all else ('日线' if args.daily else ('量比' if args.vr else '60分钟'))}")
    print("=" * 100)

    total_start = time.time()

    for year in years:
        year_start = time.time()
        print(f"\n{'='*80}")
        print(f"📅 {year}年数据下载")
        print(f"{'='*80}")

        # 确定日期范围
        start_date = f"{year}0101"
        end_date = f"{year}1231"

        # 获取交易日
        dates = get_trade_dates(start_date, end_date)
        if not dates:
            print(f"  ❌ 无法获取{year}年交易日历")
            continue
        print(f"  交易日: {len(dates)}天 ({dates[0]} ~ {dates[-1]})")

        # ===== 1. 日线快照 =====
        if do_all or args.daily:
            existing = set(f.replace('.parquet', '') for f in os.listdir(SNAPSHOT_DIR)
                           if f.endswith('.parquet') and f != 'stock_basic.parquet')
            need = [d for d in dates if d not in existing]
            print(f"\n  📊 日线快照: 已有{len(dates)-len(need)}天, 需下载{len(need)}天")

            if need:
                # 预估时间：每天约2秒（3个API调用+等待）
                print(f"  预计耗时: {len(need) * 2 / 60:.0f}分钟")
                ok = 0
                err = 0
                t0 = time.time()

                for i, d in enumerate(need):
                    success, info = sync_daily_snapshot(d)
                    if success:
                        ok += 1
                    else:
                        err += 1

                    if (i + 1) % 20 == 0 or (i + 1) == len(need):
                        elapsed = time.time() - t0
                        speed = (i + 1) / elapsed if elapsed > 0 else 0
                        eta = (len(need) - i - 1) / speed / 60 if speed > 0 else 0
                        print(f"    [{i+1}/{len(need)}] ✅{ok} ❌{err} | {elapsed/60:.1f}min | ETA {eta:.1f}min", flush=True)

                    time.sleep(0.5)

                save_progress(year, 'daily', ok, len(dates), 'done')
                print(f"  ✅ 日线快照完成: {ok}/{len(need)}天")
            else:
                print(f"  ✅ 日线快照已全部下载完成！")

        # ===== 2. 量比数据 =====
        if do_all or args.vr:
            existing_vr = set(f.replace('.parquet', '') for f in os.listdir(VR_DIR)
                              if f.endswith('.parquet'))
            need_vr = [d for d in dates if d not in existing_vr]
            print(f"\n  📊 量比数据: 已有{len(dates)-len(need_vr)}天, 需下载{len(need_vr)}天")

            if need_vr:
                print(f"  预计耗时: {len(need_vr) * 1 / 60:.0f}分钟")
                ok = 0
                err = 0
                t0 = time.time()

                for i, d in enumerate(need_vr):
                    success, info = sync_volume_ratio(d)
                    if success:
                        ok += 1
                    else:
                        err += 1

                    if (i + 1) % 30 == 0 or (i + 1) == len(need_vr):
                        elapsed = time.time() - t0
                        speed = (i + 1) / elapsed if elapsed > 0 else 0
                        eta = (len(need_vr) - i - 1) / speed / 60 if speed > 0 else 0
                        print(f"    [{i+1}/{len(need_vr)}] ✅{ok} ❌{err} | {elapsed/60:.1f}min | ETA {eta:.1f}min", flush=True)

                    time.sleep(0.3)

                save_progress(year, 'vr', ok, len(dates), 'done')
                print(f"  ✅ 量比数据完成: {ok}/{len(need_vr)}天")
            else:
                print(f"  ✅ 量比数据已全部下载完成！")

        # ===== 3. 60分钟K线 =====
        if do_all or args.min60:
            print(f"\n  📊 60分钟K线 {year}年:")
            ok, err, empty = download_60min_for_year(year)
            save_progress(year, '60min', ok, ok + err + empty, 'done')

        year_elapsed = time.time() - year_start
        print(f"\n  ⏱ {year}年总耗时: {year_elapsed/60:.1f}分钟")

    total_elapsed = time.time() - total_start
    print(f"\n{'='*100}")
    print(f"✅ 全部完成！总耗时: {total_elapsed/60:.1f}分钟")
    print(f"{'='*100}")

    # 显示最终状态
    show_status()


if __name__ == '__main__':
    main()
