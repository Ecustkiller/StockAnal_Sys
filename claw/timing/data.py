"""
claw.timing.data — 指数/涨跌停/市场宽度数据加载（带本地缓存）

关键设计：
    - 所有数据首次拉取后缓存到 data/cache/timing/ 下的 CSV
    - 大盘日线   → tushare index_daily  （1 次请求）
    - 涨停统计   → 从 tushare daily 聚合 pct_chg ≥ 9.5% 的股票数（免费，历史任意长）
    - 全市场日线 → tushare daily        （按日循环，约 1200 次请求，用于宽度）
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "timing"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 本地全历史股票 csv 仓库（aiTrader v3.7，含 2000 起所有 A 股日线，pct_chg 现成）
LOCAL_STOCKS_DIR = Path("/Users/ecustkiller/Desktop/aiTrader v3.7/data/stocks")


def _cache_path(name: str, start: str, end: str) -> Path:
    return CACHE_DIR / f"{name}_{start}_{end}.csv"


# ============================================================
# 本地全市场日线读取（aiTrader v3.7 仓库）
# ============================================================
def _load_all_stocks_daily(start: str, end: str,
                           cols: tuple = ("trade_date", "ts_code", "close", "pct_chg")
                           ) -> pd.DataFrame:
    """
    从本地 aiTrader v3.7 股票 csv 仓库读取全市场日线。

    - 目录: /Users/ecustkiller/Desktop/aiTrader v3.7/data/stocks/
    - 约 5500 只股票，每只一个 csv，时间跨度 2000~2026
    - 全量读取约 30~60s，已经比 tushare 按日循环快 10 倍以上

    返回合并后的长表 DataFrame[trade_date, ts_code, close, pct_chg, ...]
    """
    if not LOCAL_STOCKS_DIR.exists():
        raise FileNotFoundError(f"本地股票仓库不存在: {LOCAL_STOCKS_DIR}")

    import os
    files = [f for f in os.listdir(LOCAL_STOCKS_DIR) if f.endswith(".csv")]
    print(f"  📂 扫描本地股票仓库 {LOCAL_STOCKS_DIR} ...")
    print(f"     发现 {len(files)} 只股票，按日期范围 [{start}, {end}] 读取 ...")

    frames = []
    use_cols = list(cols)
    n_fail = 0
    import time as _t
    t0 = _t.time()
    for i, f in enumerate(files, 1):
        try:
            d = pd.read_csv(LOCAL_STOCKS_DIR / f,
                            usecols=lambda c: c in use_cols,
                            dtype={"trade_date": str, "ts_code": str})
            d = d[(d["trade_date"] >= start) & (d["trade_date"] <= end)]
            if len(d) > 0:
                frames.append(d)
        except Exception:
            n_fail += 1
        if i % 1000 == 0:
            print(f"     进度 {i}/{len(files)}  耗时 {_t.time()-t0:.1f}s")

    if not frames:
        raise RuntimeError("本地读取为空")

    big = pd.concat(frames, ignore_index=True)
    if "pct_chg" in big.columns:
        big["pct_chg"] = pd.to_numeric(big["pct_chg"], errors="coerce")
    if "close" in big.columns:
        big["close"] = pd.to_numeric(big["close"], errors="coerce")
    print(f"  ✅ 本地读取完成 rows={len(big)} 耗时 {_t.time()-t0:.1f}s failed={n_fail}")
    return big


def _find_superset_cache(name: str, start: str, end: str) -> Optional[Path]:
    """在已有缓存中寻找"区间范围包含 [start,end]"的更大文件。
    命中则返回路径，由调用方自行切片。未命中返回 None。
    """
    import re
    pat = re.compile(rf"^{name}_(\d{{8}})_(\d{{8}})\.csv$")
    best: Optional[Path] = None
    for fp in CACHE_DIR.glob(f"{name}_*.csv"):
        m = pat.match(fp.name)
        if not m:
            continue
        s0, e0 = m.group(1), m.group(2)
        if s0 <= start and e0 >= end:
            # 选覆盖最紧凑的那个（region 最短）
            if best is None or (int(e0) - int(s0)) < _span(best):
                best = fp
    return best


def _span(fp: Path) -> int:
    import re
    m = re.match(r"^[^_]+_(\d{8})_(\d{8})\.csv$", fp.name)
    if not m:
        return 10**9
    return int(m.group(2)) - int(m.group(1))


# ============================================================
# 指数日线
# ============================================================
def load_index_daily(ts_code: str = "000300.SH",
                     start: str = "20200101",
                     end: str = "20260430",
                     refresh: bool = False) -> pd.DataFrame:
    """
    获取指数日线（open/high/low/close/vol/amount），按 trade_date 升序。
    默认缓存到 data/cache/timing/
    """
    name = ts_code.replace(".", "_")
    fp = _cache_path(f"index_{name}", start, end)
    if fp.exists() and not refresh:
        return pd.read_csv(fp, dtype={"trade_date": str})

    from claw.core.tushare_client import TushareClient
    client = TushareClient(rate_limit_sleep=0.3)
    df = client.call(
        "index_daily",
        ts_code=ts_code,
        start_date=start,
        end_date=end,
        fields="trade_date,open,high,low,close,vol,amount,pct_chg",
    )
    if df is None or len(df) == 0:
        raise RuntimeError(f"拉取 {ts_code} 失败")
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = df["trade_date"].astype(str)
    df.to_csv(fp, index=False)
    return df


# ============================================================
# 涨跌停统计（情绪因子）—— 从全市场日线 pct_chg 聚合
# ============================================================
# 设计思路：
#   免费 tushare 权限无法访问 limit_list_d，akshare 涨停池只有最近交易日。
#   因此改为从 tushare daily(trade_date=d) 的 pct_chg 字段聚合等价指标：
#       up_count   = 当日 pct_chg >= 9.5 的股票数     ≈ 涨停数（主板10%、创业板20%）
#       down_count = 当日 pct_chg <= -9.5 的股票数    ≈ 跌停数
#       up_stat    = 当日最高连板数（跨日滚动计算）
#   这样下游 compute_sentiment_limit 无需修改，字段语义保持一致。
#   优势：完全免费、历史任意长、与已有 breadth 拉取可共用同一份缓存。
# ============================================================
def load_limit_stats(start: str = "20210101",
                     end: str = "20260430",
                     refresh: bool = False,
                     up_th: float = 9.5,
                     down_th: float = -9.5) -> pd.DataFrame:
    """
    获取涨跌停家数统计 —— 从 tushare daily 聚合。

    返回列：trade_date, up_count, down_count, up_stat（最高连板数）
    """
    fp = _cache_path("limit_stats", start, end)
    if fp.exists() and not refresh:
        return pd.read_csv(fp, dtype={"trade_date": str})

    # 命中更大区间的超集缓存 → 切片复用，避免重拉
    if not refresh:
        sup = _find_superset_cache("limit_stats", start, end)
        if sup is not None:
            df = pd.read_csv(sup, dtype={"trade_date": str})
            df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].reset_index(drop=True)
            print(f"  ♻️  复用超集缓存 {sup.name} → 切片 {len(df)} 行")
            return df

    from claw.core.tushare_client import TushareClient
    client = TushareClient(rate_limit_sleep=0.15)

    # ---------- 优先：从本地 aiTrader 全市场日线仓库聚合（秒级完成）----------
    try:
        big = _load_all_stocks_daily(start, end, cols=("trade_date", "ts_code", "pct_chg"))
    except Exception as e:
        print(f"  ⚠️  本地仓库读取失败（{e}），降级到 tushare 按日循环（约 3 分钟）")
        big = None

    if big is None:
        # ---------- 降级：按日 tushare daily 循环 ----------
        # 前置 30 日用来数"连板"
        from datetime import datetime, timedelta
        try:
            d0 = datetime.strptime(start, "%Y%m%d")
            pre_start = (d0 - timedelta(days=45)).strftime("%Y%m%d")
        except Exception:
            pre_start = start

        cal = client.trade_cal(pre_start, end)
        if cal is None or len(cal) == 0:
            raise RuntimeError("交易日历拉取失败")
        dates = cal["cal_date"].astype(str).tolist()

        all_frames = []
        n_fail = 0
        for i, d in enumerate(dates, 1):
            try:
                daily = client.call("daily", trade_date=d,
                                    fields="ts_code,trade_date,pct_chg")
                if daily is not None and len(daily) > 0:
                    all_frames.append(daily)
                if i % 50 == 0:
                    print(f"  [涨跌停-daily] {i}/{len(dates)}  {d}  stocks={len(daily) if daily is not None else 0}")
            except Exception as e:
                n_fail += 1
                if n_fail <= 5:
                    print(f"  ❌ {d} daily 失败: {e}")

        if not all_frames:
            raise RuntimeError("全市场日线拉取为空")

        big = pd.concat(all_frames, ignore_index=True)
        big["trade_date"] = big["trade_date"].astype(str)
        big["pct_chg"] = pd.to_numeric(big["pct_chg"], errors="coerce")

    # 标记个股当日涨停
    big["is_up"] = (big["pct_chg"] >= up_th).astype(int)
    big["is_down"] = (big["pct_chg"] <= down_th).astype(int)

    # 按 ts_code 滚动数"连续涨停天数"：
    # cum_up = 当日 is_up 的累计连续计数（断则归零）
    big = big.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    # 经典做法：cumsum 上一次 is_up==0 的位置，然后 cumcount
    grp = big.groupby("ts_code")
    reset = (1 - big["is_up"]).groupby(big["ts_code"]).cumsum()
    big["lb_run"] = big.groupby(["ts_code", reset]).cumcount() + 1
    big.loc[big["is_up"] == 0, "lb_run"] = 0

    # 按日聚合
    out = big.groupby("trade_date").agg(
        up_count=("is_up", "sum"),
        down_count=("is_down", "sum"),
        up_stat=("lb_run", "max"),
    ).reset_index()

    out = out[(out["trade_date"] >= start) & (out["trade_date"] <= end)].reset_index(drop=True)
    out = out.astype({"up_count": int, "down_count": int, "up_stat": int})

    out.to_csv(fp, index=False)
    print(f"  ✅ 保存涨跌停统计 {len(out)} 行 → {fp}")
    return out


# ============================================================
# 市场宽度（按日调 tushare daily，一次拉全市场）
# ============================================================
def load_breadth_from_tushare(start: str = "20210101",
                              end: str = "20260430",
                              refresh: bool = False) -> pd.DataFrame:
    """
    市场宽度 = 收盘价 > MA20 的股票占比（%）

    方法：
        1. 拉前置 30 日 + [start, end] 的交易日
        2. 对每个交易日调用 tushare daily(trade_date=d)，一次拿全市场
        3. 聚合计算 MA20 宽度

    返回: DataFrame[trade_date, breadth_ma20]
    """
    fp = _cache_path("breadth", start, end)
    if fp.exists() and not refresh:
        return pd.read_csv(fp, dtype={"trade_date": str})

    # 命中更大区间的超集缓存 → 切片复用
    if not refresh:
        sup = _find_superset_cache("breadth", start, end)
        if sup is not None:
            df = pd.read_csv(sup, dtype={"trade_date": str})
            df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].reset_index(drop=True)
            print(f"  ♻️  复用超集缓存 {sup.name} → 切片 {len(df)} 行")
            return df

    from claw.core.tushare_client import TushareClient
    client = TushareClient(rate_limit_sleep=0.15)

    # ---------- 优先：从本地 aiTrader 全市场日线仓库（秒级完成）----------
    # 为了算 MA20，需要前推 40 天
    from datetime import datetime, timedelta
    try:
        d0 = datetime.strptime(start, "%Y%m%d")
        pre_start = (d0 - timedelta(days=45)).strftime("%Y%m%d")
    except Exception:
        pre_start = start

    try:
        big = _load_all_stocks_daily(pre_start, end, cols=("trade_date", "ts_code", "close"))
    except Exception as e:
        print(f"  ⚠️  本地仓库读取失败（{e}），降级到 tushare 按日循环")
        big = None

    if big is None:
        # ---------- 降级：按日 tushare daily 循环 ----------
        cal = client.trade_cal(pre_start, end)
        if cal is None or len(cal) == 0:
            raise RuntimeError("交易日历拉取失败")
        dates = cal["cal_date"].astype(str).tolist()

        all_frames = []
        for i, d in enumerate(dates, 1):
            try:
                daily = client.call("daily", trade_date=d,
                                    fields="ts_code,trade_date,close")
                if daily is not None and len(daily) > 0:
                    all_frames.append(daily)
                if i % 50 == 0:
                    print(f"  [宽度-daily] {i}/{len(dates)}  {d}  stocks={len(daily) if daily is not None else 0}")
            except Exception as e:
                print(f"  ❌ {d} daily 失败: {e}")

        if not all_frames:
            raise RuntimeError("全市场日线拉取为空")

        big = pd.concat(all_frames, ignore_index=True)
        big["trade_date"] = big["trade_date"].astype(str)

    big = big.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    # MA20
    big["ma20"] = big.groupby("ts_code")["close"].transform(
        lambda s: s.rolling(20, min_periods=10).mean()
    )
    big["above"] = (big["close"] > big["ma20"]).astype(int)

    out = big.groupby("trade_date").agg(breadth_ma20=("above", "mean")).reset_index()
    out["breadth_ma20"] = out["breadth_ma20"] * 100
    out = out[(out["trade_date"] >= start) & (out["trade_date"] <= end)].reset_index(drop=True)

    out.to_csv(fp, index=False)
    print(f"  ✅ 保存市场宽度 {len(out)} 行 → {fp}")
    return out


# ============================================================
# 兼容旧接口：从本地全市场日线算宽度（如无则返回空）
# ============================================================
def load_market_breadth_from_local(start: str,
                                   end: str,
                                   cache_file: Optional[Path] = None
                                   ) -> pd.DataFrame:
    """
    基于项目已有的全市场日线缓存计算市场宽度（MA20 之上股票占比）。
    如果本地没有全市场日线，则返回空 DataFrame（宽度因子将降级为 0）。
    """
    fp = cache_file or (CACHE_DIR / f"breadth_{start}_{end}.csv")
    if fp.exists():
        return pd.read_csv(fp, dtype={"trade_date": str})

    # 尝试从 data/cache 找全市场日线
    candidates = [
        PROJECT_ROOT / "data" / "cache" / "all_daily.csv",
        PROJECT_ROOT / "data" / "cache" / "daily_all.parquet",
        PROJECT_ROOT / "data" / "daily.csv",
    ]
    daily_fp = next((p for p in candidates if p.exists()), None)
    if daily_fp is None:
        # 无本地数据，返回空，由上层降级
        return pd.DataFrame(columns=["trade_date", "breadth_ma20"])

    if daily_fp.suffix == ".parquet":
        df = pd.read_parquet(daily_fp)
    else:
        df = pd.read_csv(daily_fp, dtype={"trade_date": str})

    df["trade_date"] = df["trade_date"].astype(str)
    df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)]

    # 按股票计算 MA20，再按日期统计"收盘>MA20"的占比
    df = df.sort_values(["ts_code", "trade_date"])
    df["ma20"] = df.groupby("ts_code")["close"].transform(lambda s: s.rolling(20).mean())
    df["above"] = (df["close"] > df["ma20"]).astype(int)
    out = df.groupby("trade_date").agg(breadth_ma20=("above", "mean")).reset_index()
    out["breadth_ma20"] = out["breadth_ma20"] * 100  # 转为百分比
    out.to_csv(fp, index=False)
    return out
