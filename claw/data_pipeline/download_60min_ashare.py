#!/usr/bin/env python3
"""
用Ashare批量下载2025年全年60分钟K线数据
格式与 ~/Downloads/2026/60min/ 完全一致

用法：
  python3 download_60min_ashare.py           # 下载所有缺失的股票
  python3 download_60min_ashare.py --check   # 只检查进度不下载
"""
import sys, os, time, glob, argparse
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price

# 目标目录（2025年60分钟K线）
OUTPUT_DIR = os.path.expanduser('~/Downloads/2025/60min')
# 参考目录（2026年60分钟K线，用于获取股票列表）
REF_DIR = os.path.expanduser('~/Downloads/2026/60min')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ashare get_price count=1500 可获取到2024-09-26，完全覆盖2025全年
# 每天4根60分钟K线，2025年约250个交易日=1000根
# 我们只保留2025年的数据（2025-01-01 ~ 2025-12-31）
YEAR_START = '2025-01-01'
YEAR_END = '2025-12-31'
COUNT = 1500  # 足够覆盖2025全年


def get_stock_list():
    """从2026年60分钟目录获取股票列表"""
    files = sorted(os.listdir(REF_DIR))
    codes = [f.replace('.csv', '') for f in files if f.endswith('.csv')]
    return codes


def download_one(ashare_code, output_path):
    """下载单只股票的60分钟K线并保存为CSV"""
    try:
        df = get_price(ashare_code, frequency='60m', count=COUNT)
        if df is None or len(df) == 0:
            return 0, 'empty'

        df = df.reset_index()
        col0 = df.columns[0]  # index列名（可能是空字符串）
        df['dt'] = df[col0].astype(str)
        df['date'] = df['dt'].str[:10]
        df['time'] = df['dt'].str[11:16]

        # 只保留2025年的数据
        mask = (df['date'] >= YEAR_START) & (df['date'] <= YEAR_END)
        df_2025 = df[mask].copy()

        if len(df_2025) == 0:
            return 0, 'no_2025_data'

        # 格式与现有2026年60min完全一致：日期,时间,开盘,最高,最低,收盘,成交量,成交额
        # Ashare返回的列：open, high, low, close, volume（无成交额，填0）
        rows = []
        for _, r in df_2025.iterrows():
            # 时间格式：10:30, 11:30, 14:00, 15:00
            rows.append(f"{r['date']},{r['time']},{r['open']},{r['high']},{r['low']},{r['close']},{r['volume']},0")

        with open(output_path, 'w') as f:
            f.write('日期,时间,开盘,最高,最低,收盘,成交量,成交额\n')
            f.write('\n'.join(rows))

        return len(df_2025), 'ok'
    except Exception as e:
        return 0, f'err:{str(e)[:50]}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true', help='只检查进度')
    args = parser.parse_args()

    codes = get_stock_list()
    print(f"📊 Ashare批量下载2025年60分钟K线")
    print(f"  参考目录: {REF_DIR} ({len(codes)}只)")
    print(f"  输出目录: {OUTPUT_DIR}")
    print(f"  数据范围: {YEAR_START} ~ {YEAR_END}")

    # 检查已有
    existing = set(os.listdir(OUTPUT_DIR))
    need = [c for c in codes if f"{c}.csv" not in existing]
    print(f"  已下载: {len(codes)-len(need)}只, 需下载: {len(need)}只")

    if args.check:
        return

    if not need:
        print("  ✅ 全部已下载完成！")
        return

    # 预估时间：每只约0.15秒（Ashare很快），5000只≈12.5分钟
    print(f"  预计耗时: {len(need) * 0.2 / 60:.1f}分钟")
    print()

    ok_count = 0
    err_count = 0
    empty_count = 0
    total_rows = 0
    t_start = time.time()

    for i, code in enumerate(need):
        output_path = os.path.join(OUTPUT_DIR, f"{code}.csv")

        n_rows, status = download_one(code, output_path)

        if status == 'ok':
            ok_count += 1
            total_rows += n_rows
        elif status == 'empty' or status == 'no_2025_data':
            empty_count += 1
        else:
            err_count += 1

        # 进度报告
        if (i + 1) % 200 == 0 or (i + 1) == len(need):
            elapsed = time.time() - t_start
            speed = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(need) - i - 1) / speed / 60 if speed > 0 else 0
            print(f"  [{i+1}/{len(need)}] ✅{ok_count} ❌{err_count} ⬜{empty_count} | "
                  f"{total_rows}条 | {elapsed:.0f}s | ETA {eta:.1f}min")

        # Ashare控速（避免被封）
        time.sleep(0.1)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"✅ 完成！耗时{elapsed/60:.1f}分钟")
    print(f"  成功: {ok_count}只 ({total_rows}条)")
    print(f"  空数据: {empty_count}只")
    print(f"  错误: {err_count}只")
    print(f"  输出目录: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()
