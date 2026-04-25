#!/usr/bin/env python3
"""
Turbo 版并发增量更新脚本
========================
在 daily_update_fast.py 的基础上，用 ThreadPoolExecutor 并发请求 Ashare。
行为（扫描/过滤/写回格式）完全复用 fast 版的函数，只是把串行 for 改成了线程池。

速度对比（5490 只 × 4 种周期 ≈ 19000+ 次请求）:
  - fast(串行):   ~20-30 分钟
  - turbo(8并发): ~3-5 分钟（网络 IO 密集型，GIL 不是瓶颈）

保守默认：
  - 分钟线 8 并发，日线 10 并发
  - 单次失败重试 2 次（继承 fast 版）
  - 全局总请求间不再 sleep（并发已经自然限流）

用法：
    python3 -m claw.data_pipeline.daily_update_turbo
    python3 -m claw.data_pipeline.daily_update_turbo --workers 12     # 更激进
    python3 -m claw.data_pipeline.daily_update_turbo --only daily     # 只跑日线
    python3 -m claw.data_pipeline.daily_update_turbo --only 60min     # 只跑 60min
"""
import sys, os, json, time, argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 复用 fast 版的函数，避免重复实现
from claw.data_pipeline.daily_update_fast import (
    DATA_DIR, STOCK_DATA_DIR, TODAY, TODAY_COMPACT, PERIODS,
    scan_needs_update, update_minute_stock, update_daily_stock,
)

# 默认并发（保守）
DEFAULT_MIN_WORKERS = 8
DEFAULT_DAILY_WORKERS = 10

_print_lock = threading.Lock()

def _log(msg):
    """线程安全打印 + 立刻 flush（方便 dashboard 实时显示）"""
    with _print_lock:
        print(msg, flush=True)


def _run_minute_batch(period_dir, ashare_freq, count, workers):
    full_dir = os.path.join(DATA_DIR, period_dir)
    if not os.path.isdir(full_dir):
        _log(f"⚠️ {period_dir} 目录不存在")
        return {'total': 0, 'updated': 0, 'rows': 0, 'uptodate': 0, 'err': 0, 'skip': 0}

    _log(f"\n📊 扫描 {period_dir}...")
    needs, uptodate, scan_err, total = scan_needs_update(full_dir, TODAY)
    _log(f"  共{total}只 | 需更新:{len(needs)} | 已最新:{uptodate} | 读取错误:{scan_err}")

    if not needs:
        _log(f"  ✅ {period_dir} 全部已是最新，无需更新")
        return {'total': total, 'updated': 0, 'rows': 0, 'uptodate': uptodate,
                'err': scan_err, 'skip': 0}

    updated = 0
    total_rows = 0
    errors = scan_err
    done = 0
    n = len(needs)

    # 每 200 只或每 5 秒打印一次进度
    last_print_done = 0
    last_print_time = time.time()

    def task(item):
        fp, _local = item
        code = os.path.basename(fp).replace('.csv', '')
        n_rows, status = update_minute_stock(fp, code, ashare_freq, count)
        return status, n_rows

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(task, it) for it in needs]
        for fut in as_completed(futs):
            try:
                status, n_rows = fut.result()
            except Exception as e:
                errors += 1
                status = f'err:{e}'
                n_rows = 0

            if status == 'ok':
                updated += 1
                total_rows += n_rows
            elif status == 'uptodate':
                uptodate += 1
            else:
                errors += 1

            done += 1
            now = time.time()
            if done - last_print_done >= 200 or (now - last_print_time) >= 5 or done == n:
                _log(f"  [{period_dir}] 进度 {done}/{n} | 更新{updated}只(+{total_rows}条) 错误{errors}")
                last_print_done = done
                last_print_time = now

    _log(f"  ✅ {period_dir} 完成: 更新{updated}只(+{total_rows}条), 错误{errors}")
    return {'total': total, 'updated': updated, 'rows': total_rows,
            'uptodate': uptodate, 'err': errors, 'skip': 0}


def _run_daily_batch(workers):
    _log(f"\n📈 扫描日线数据...")
    needs, uptodate, scan_err, total = scan_needs_update(STOCK_DATA_DIR, TODAY_COMPACT)
    _log(f"  共{total}只 | 需更新:{len(needs)} | 已最新:{uptodate} | 读取错误:{scan_err}")

    if not needs:
        _log(f"  ✅ 日线全部已是最新")
        return {'total': total, 'updated': 0, 'rows': 0, 'uptodate': uptodate,
                'err': scan_err, 'skip': 0}

    updated = 0
    total_rows = 0
    errors = scan_err
    skipped = 0
    done = 0
    n = len(needs)

    last_print_done = 0
    last_print_time = time.time()

    def task(item):
        fp, _local = item
        fname = os.path.basename(fp)
        code_raw = fname.split('_')[0]
        if not code_raw.isdigit() or len(code_raw) != 6:
            return 'skip', 0
        n_rows, status = update_daily_stock(fp, code_raw)
        return status, n_rows

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(task, it) for it in needs]
        for fut in as_completed(futs):
            try:
                status, n_rows = fut.result()
            except Exception as e:
                errors += 1
                status = f'err:{e}'
                n_rows = 0

            if status == 'ok':
                updated += 1
                total_rows += n_rows
            elif status == 'uptodate':
                uptodate += 1
            elif status == 'skip':
                skipped += 1
            else:
                errors += 1

            done += 1
            now = time.time()
            if done - last_print_done >= 500 or (now - last_print_time) >= 5 or done == n:
                _log(f"  [daily] 进度 {done}/{n} | 更新{updated}只(+{total_rows}条) 错误{errors}")
                last_print_done = done
                last_print_time = now

    _log(f"  ✅ 日线完成: 更新{updated}只(+{total_rows}条), 错误{errors}")
    return {'total': total, 'updated': updated, 'rows': total_rows,
            'uptodate': uptodate, 'err': errors, 'skip': skipped}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=0,
                        help='并发线程数，0=用默认（分钟8/日线10）')
    parser.add_argument('--only', type=str, default='',
                        help='只跑某一项: daily / 60min / 30min / 15min')
    args = parser.parse_args()

    t0 = time.time()
    _log("=" * 60)
    _log(f"🚀 Turbo 并发增量更新 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _log("=" * 60)

    results = {}
    only = args.only.strip().lower()

    # 分钟线
    min_workers = args.workers or DEFAULT_MIN_WORKERS
    for period_dir, (ashare_freq, count) in PERIODS.items():
        if only and only != period_dir:
            continue
        results[period_dir] = _run_minute_batch(period_dir, ashare_freq, count, min_workers)

    # 日线
    daily_workers = args.workers or DEFAULT_DAILY_WORKERS
    if not only or only == 'daily':
        results['daily'] = _run_daily_batch(daily_workers)

    elapsed = time.time() - t0
    _log("\n" + "=" * 60)
    _log(f"📋 更新汇总 (耗时 {elapsed:.1f}s = {elapsed/60:.1f}min)")
    _log("=" * 60)
    for k, v in results.items():
        _log(f"  {k:8s}: 共{v['total']}只 | 更新{v['updated']}只(+{v['rows']}条) | "
             f"已最新{v['uptodate']} | 错误{v['err']} | 跳过{v['skip']}")
    _log("=" * 60)

    _log(f"\n__RESULT_JSON__")
    _log(json.dumps(results, ensure_ascii=False))


if __name__ == '__main__':
    main()
