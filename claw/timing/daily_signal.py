"""
claw.timing.daily_signal — 每日择时信号产出
================================================
用于实盘每日 8:30 前后调用，输出：
    - 大盘状态（BULL / RISK_ON / NEUTRAL / BEAR）
    - 目标仓位（1.0 / 0.7 / 0.3 / 0.0）
    - 近 5 日信号走势

输出文件：
    reports/timing/market_timing_YYYYMMDD.json
    reports/timing/market_timing_daily.csv   （整个时序，覆盖写）

用法：
    python -m claw.timing.daily_signal                     # 默认到最近交易日
    python -m claw.timing.daily_signal --end 20260420      # 指定到某日
    python -m claw.timing.daily_signal --sentiment         # 启用情绪因子
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from claw.timing.composer import compute_market_timing

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = PROJECT_ROOT / "reports" / "timing"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


POSITION_DESC = {
    "BULL":    "满仓 100% — 总分≥+3，5 因子中≥3 个看多",
    "RISK_ON": "七成 70% — 总分 +1~+2，偏多但非极端",
    "NEUTRAL": "三成 30% — 总分 -1~0，分歧期，小仓试错",
    "BEAR":    "空仓   0% — 总分≤-2，多因子共振看空",
}


def run(start: str, end: str, use_sentiment: bool, use_breadth: bool):
    print("=" * 80)
    print(f"📡 大盘择时每日信号  |  {start} ~ {end}")
    print(f"    因子: RSRS / 趋势 / 波动率"
          f"{' / 涨停情绪' if use_sentiment else ''}"
          f"{' / 市场宽度' if use_breadth else ''}")
    print("=" * 80)

    df = compute_market_timing(
        start=start, end=end,
        use_sentiment=use_sentiment,
        use_breadth=use_breadth,
    )
    if df is None or len(df) == 0:
        raise RuntimeError("compute_market_timing 返回空表")

    latest = df.iloc[-1]

    # 保存整个时序
    csv_path = REPORT_DIR / "market_timing_daily.csv"
    df.to_csv(csv_path, index=False)

    # 保存当日 JSON
    day_json = REPORT_DIR / f"market_timing_{latest['trade_date']}.json"
    payload = {
        "trade_date": str(latest["trade_date"]),
        "close": float(latest["close"]),
        "state": str(latest["state"]),
        "position": float(latest["position"]),
        "position_desc": POSITION_DESC.get(latest["state"], ""),
        "total_score": int(latest["total_score"]),
        "signals": {
            "rsrs":     int(latest["rsrs_signal"]),
            "trend":    int(latest["trend_signal"]),
            "vol":      int(latest["vol_signal"]),
            "sentiment": int(latest["senti_signal"]),
            "breadth":  int(latest["breadth_signal"]),
        },
        "detail": {
            "rsrs_z":       None if pd.isna(latest.get("rsrs_z")) else round(float(latest["rsrs_z"]), 3),
            "rsrs_r2":      None if pd.isna(latest.get("rsrs_r2")) else round(float(latest["rsrs_r2"]), 3),
            "ma250":        None if pd.isna(latest.get("ma250")) else round(float(latest["ma250"]), 2),
            "dev_pct":      None if pd.isna(latest.get("dev_pct")) else round(float(latest["dev_pct"]), 2),
            "mom20":        None if pd.isna(latest.get("mom20")) else round(float(latest["mom20"]), 2),
            "vol_pct":      None if pd.isna(latest.get("vol_pct")) else round(float(latest["vol_pct"]), 3),
        },
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    with open(day_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 打印
    print(f"\n📅 日期: {latest['trade_date']}  |  收盘: {latest['close']:.2f}")
    print(f"\n  🎯 状态：{latest['state']}   仓位：{latest['position']*100:.0f}%")
    print(f"      {POSITION_DESC.get(latest['state'], '')}")
    print(f"\n  📊 总分：{int(latest['total_score']):+d}")
    print(f"      RSRS={int(latest['rsrs_signal']):+d}  "
          f"趋势={int(latest['trend_signal']):+d}  "
          f"波动率={int(latest['vol_signal']):+d}  "
          f"涨停={int(latest['senti_signal']):+d}  "
          f"宽度={int(latest['breadth_signal']):+d}")

    print("\n  📈 关键指标：")
    if pd.notna(latest.get("rsrs_z")):
        print(f"      RSRS Z-score  = {latest['rsrs_z']:+.3f}   R²={latest['rsrs_r2']:.2f}")
    if pd.notna(latest.get("dev_pct")):
        print(f"      年线偏离度    = {latest['dev_pct']:+.2f}%   20日动量={latest['mom20']:+.2f}%")
    if pd.notna(latest.get("vol_pct")):
        print(f"      波动率分位    = {latest['vol_pct']*100:.1f}%")

    # 近 5 日走势
    print("\n  🕐 近 5 日信号：")
    tail = df.tail(5)
    print(f"      {'日期':<12}{'RSRS':>5}{'趋势':>5}{'波动':>5}"
          f"{'情绪':>5}{'宽度':>5}{'总分':>6}{'状态':>10}{'仓位':>7}")
    for _, r in tail.iterrows():
        print(f"      {r['trade_date']:<12}"
              f"{int(r['rsrs_signal']):>+5d}{int(r['trend_signal']):>+5d}"
              f"{int(r['vol_signal']):>+5d}{int(r['senti_signal']):>+5d}"
              f"{int(r['breadth_signal']):>+5d}{int(r['total_score']):>+6d}"
              f"{r['state']:>10}{r['position']*100:>6.0f}%")

    print(f"\n💾 结果已保存：")
    print(f"   - 当日 JSON : {day_json}")
    print(f"   - 时序 CSV  : {csv_path}")
    print("=" * 80)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default=None,
                    help="结束日（YYYYMMDD）；缺省=今天")
    ap.add_argument("--sentiment", action="store_true")
    ap.add_argument("--breadth", action="store_true")
    args = ap.parse_args()

    end = args.end or datetime.now().strftime("%Y%m%d")
    run(args.start, end, args.sentiment, args.breadth)


if __name__ == "__main__":
    main()
