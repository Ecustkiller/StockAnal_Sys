#!/usr/bin/env python3
"""A股选股筛选器 v3 - 基于4月9日数据"""
import requests, json, time, sys
import pandas as pd
import numpy as np
from datetime import datetime

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts(api, params=None, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params or {}}
    if fields: d["fields"] = fields
    try:
        r = requests.post("http://api.tushare.pro", json=d, timeout=30)
        j = r.json()
        if j.get("code") != 0:
            return None, j.get("msg","")[:100]
        df = pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
        return df, ""
    except Exception as e:
        return None, str(e)[:100]

print("="*80)
print("A股选股筛选器 v3")
print(f"运行: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("="*80)

# 使用确认有数据的交易日
D0 = "20260409"  # 最新有效数据日
D5 = "20260402"  # 约5个交易日前
D10 = "20260327" # 约10个交易日前
D20 = "20260313" # 约20个交易日前

# ===== Step 1: 基本面 =====
print(f"\n[Step 1] 基本面({D0})...")
bas, msg = ts("daily_basic", {"trade_date":D0},
              "ts_code,close,turnover_rate_f,volume_ratio,pe_ttm,pb,total_mv,circ_mv")
if bas is None or bas.empty:
    print(f"  ERR: {msg}")
    # 试前一日
    bas, msg = ts("daily_basic", {"trade_date":"20260408"},
                  "ts_code,close,turnover_rate_f,volume_ratio,pe_ttm,pb,total_mv,circ_mv")
print(f"  基本面: {len(bas) if bas is not None else 0}只")
time.sleep(1)

# ===== Step 2: 全市场日K =====
print(f"\n[Step 2] 全市场日K...")
d0k, _ = ts("daily", {"trade_date":D0}, "ts_code,open,high,low,close,pct_chg,vol,amount")
print(f"  {D0}: {len(d0k) if d0k is not None else 0}只")
time.sleep(1)

d5k, _ = ts("daily", {"trade_date":D5}, "ts_code,close")
print(f"  {D5}: {len(d5k) if d5k is not None else 0}只")
time.sleep(1)

d10k, _ = ts("daily", {"trade_date":D10}, "ts_code,close")
print(f"  {D10}: {len(d10k) if d10k is not None else 0}只")
time.sleep(1)

d20k, _ = ts("daily", {"trade_date":D20}, "ts_code,close")
print(f"  {D20}: {len(d20k) if d20k is not None else 0}只")
time.sleep(1)

# ===== Step 3: 股票名称 =====
print(f"\n[Step 3] 股票列表...")
stk, _ = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry,market")
print(f"  股票: {len(stk) if stk is not None else 0}只")
time.sleep(1)

# ===== Step 4: 涨停板 =====
print(f"\n[Step 4] 涨停板({D0})...")
lim, msg = ts("limit_list_d", {"trade_date":D0,"limit_type":"U"},
              "ts_code,trade_date,name,close,pct_chg,fc_ratio,fd_amount,open_times,limit_times")
if lim is not None and not lim.empty:
    print(f"  {D0}: {len(lim)}只涨停")
else:
    print(f"  涨停: {msg}")
    lim = pd.DataFrame()

# ===== Step 5: 资金流向 =====
print(f"\n[Step 5] 资金流向({D0})...")
time.sleep(1)
mf, msg = ts("moneyflow", {"trade_date":D0},
             "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,net_mf_amount")
if mf is not None and not mf.empty:
    mf["net_big"] = (mf["buy_elg_amount"].fillna(0)+mf["buy_lg_amount"].fillna(0)
                     -mf["sell_elg_amount"].fillna(0)-mf["sell_lg_amount"].fillna(0))
    print(f"  资金: {len(mf)}只")
else:
    print(f"  资金: {msg}")
    mf = pd.DataFrame()

# ===== Step 6: 合并 =====
print(f"\n[Step 6] 合并筛选...")
if d0k is None or d0k.empty:
    print("无日K数据！")
    sys.exit(1)

df = d0k.copy()

# 合并名称
if stk is not None:
    df = df.merge(stk[["ts_code","name","industry"]], on="ts_code", how="left")

# 合并基本面
if bas is not None:
    df = df.merge(bas[["ts_code","turnover_rate_f","volume_ratio","pe_ttm","pb","total_mv","circ_mv"]],
                  on="ts_code", how="left")

# 近5/10/20日涨幅
if d5k is not None and not d5k.empty:
    df = df.merge(d5k[["ts_code","close"]].rename(columns={"close":"c5"}), on="ts_code", how="left")
    df["r5"] = (df["close"]-df["c5"])/df["c5"]*100
if d10k is not None and not d10k.empty:
    df = df.merge(d10k[["ts_code","close"]].rename(columns={"close":"c10"}), on="ts_code", how="left")
    df["r10"] = (df["close"]-df["c10"])/df["c10"]*100
if d20k is not None and not d20k.empty:
    df = df.merge(d20k[["ts_code","close"]].rename(columns={"close":"c20"}), on="ts_code", how="left")
    df["r20"] = (df["close"]-df["c20"])/df["c20"]*100

# 合并资金
if not mf.empty:
    df = df.merge(mf[["ts_code","net_big","net_mf_amount"]], on="ts_code", how="left")

# 合并涨停
if not lim.empty:
    lim_info = lim[["ts_code","limit_times","fc_ratio","open_times"]].copy()
    lim_info.rename(columns={"limit_times":"连板","fc_ratio":"封板率","open_times":"开板次数"}, inplace=True)
    df = df.merge(lim_info, on="ts_code", how="left")
    df["is_limit"] = df["ts_code"].isin(lim["ts_code"])
else:
    df["连板"] = np.nan
    df["is_limit"] = False

# 过滤
df = df[df["ts_code"].str.match(r"^(00|30|60|68)")].copy()
if "name" in df.columns:
    df = df[~df["name"].str.contains("ST|退|B$", na=False)].copy()
df = df[df["total_mv"].notna() & (df["total_mv"] > 300000)].copy()
print(f"  过滤后: {len(df)}只")

# ===== 评分体系 =====
# 1. 趋势分(0-7)
df["s_trend"] = 0
r5 = df.get("r5", pd.Series(0, index=df.index)).fillna(0)
r10 = df.get("r10", pd.Series(0, index=df.index)).fillna(0)
r20 = df.get("r20", pd.Series(0, index=df.index)).fillna(0)
df.loc[(r5>=5)&(r5<=15), "s_trend"] += 3
df.loc[(r5>0)&(r5<5), "s_trend"] += 2
df.loc[(r5>15)&(r5<=25), "s_trend"] += 1
df.loc[(r10>=10)&(r10<=30), "s_trend"] += 2
df.loc[(r10>0)&(r10<10), "s_trend"] += 1
df.loc[(r20>=15)&(r20<=50), "s_trend"] += 1

# 2. 资金分(0-3)
df["s_fund"] = 0
if "net_big" in df.columns:
    nb = df["net_big"].fillna(0)
    df.loc[nb > 10000, "s_fund"] += 3
    df.loc[(nb>3000)&(nb<=10000), "s_fund"] += 2
    df.loc[(nb>0)&(nb<=3000), "s_fund"] += 1

# 3. 估值分(0-3)
df["s_val"] = 0
pe = df["pe_ttm"].fillna(0)
df.loc[(pe>0)&(pe<=30), "s_val"] += 3
df.loc[(pe>30)&(pe<=60), "s_val"] += 2
df.loc[(pe>60)&(pe<=100), "s_val"] += 1

# 4. 涨停/动量分(0-3)
df["s_mom"] = 0
df.loc[df["is_limit"]==True, "s_mom"] += 2
pct = df["pct_chg"].fillna(0)
df.loc[(pct>=3)&(pct<9.5), "s_mom"] += 1

# 5. 今日表现适中(0-2): 不追涨停，3-7%最佳
df["s_today"] = 0
df.loc[(pct>=2)&(pct<=7), "s_today"] += 2
df.loc[(pct>=0)&(pct<2), "s_today"] += 1

# 6. 换手/量比(0-2)
df["s_activity"] = 0
vr = df["volume_ratio"].fillna(0)
tr = df["turnover_rate_f"].fillna(0)
df.loc[(vr>=1.5)&(vr<=5), "s_activity"] += 1
df.loc[(tr>=3)&(tr<=15), "s_activity"] += 1

# 总分
df["score"] = df["s_trend"] + df["s_fund"] + df["s_val"] + df["s_mom"] + df["s_today"] + df["s_activity"]
df.sort_values("score", ascending=False, inplace=True)

# ===== 输出 =====
pd.set_option("display.max_rows",60)
pd.set_option("display.width",250)
pd.set_option("display.max_columns",25)

print(f"\n{'='*200}")
print(f"TOP 50 综合排名 (数据日: {D0})")
print(f"{'='*200}")

show = df.head(50).copy()
show["mv_yi"] = show["total_mv"]/10000
show["amt_yi"] = show["amount"]/100000 if "amount" in show.columns else 0
if "net_big" in show.columns:
    show["nb_yi"] = show["net_big"]/10000
else:
    show["nb_yi"] = 0

cols = ["ts_code","name","close","pct_chg","r5","r10","r20",
        "turnover_rate_f","volume_ratio","pe_ttm","pb","mv_yi","amt_yi","nb_yi",
        "is_limit","连板","score","s_trend","s_fund","s_val","s_mom"]
cols = [c for c in cols if c in show.columns]
rn = {"turnover_rate_f":"换手%","volume_ratio":"量比","mv_yi":"市值亿","amt_yi":"成交亿",
      "nb_yi":"主力亿","pct_chg":"今%","r5":"5d%","r10":"10d%","r20":"20d%",
      "score":"总分","s_trend":"趋势","s_fund":"资金","s_val":"估值","s_mom":"动量","is_limit":"涨停","连板":"板"}
show_r = show[cols].rename(columns=rn)
print(show_r.to_string(index=False))

# 精选推荐
print(f"\n{'='*200}")
print("⭐ 精选推荐")
print(f"{'='*200}")

# 策略A: 趋势强+资金好+估值合理（中线白马）
print("\n【A类-中线趋势】score>=9, 5日涨3-20%, PE合理, 主力净流入")
rca = (df["score"]>=9)
if "r5" in df.columns: rca = rca & (df["r5"]>=3) & (df["r5"]<=20)
if "pe_ttm" in df.columns: rca = rca & (df["pe_ttm"]>0) & (df["pe_ttm"]<100)
if "total_mv" in df.columns: rca = rca & (df["total_mv"]>500000)
if "net_big" in df.columns: rca = rca & (df["net_big"]>0)
recA = df[rca].head(8)
if recA.empty:
    rca2 = (df["score"]>=8) & (df["r5"]>=2) & (df["pe_ttm"]>0) & (df["total_mv"]>300000)
    recA = df[rca2].head(8)
if not recA.empty:
    ra = recA.copy()
    ra["mv_yi"] = ra["total_mv"]/10000
    ra["amt_yi"] = ra["amount"]/100000
    ra["nb_yi"] = ra["net_big"]/10000 if "net_big" in ra.columns else 0
    cols_a = [c for c in cols if c in ra.columns]
    print(ra[cols_a].rename(columns=rn).to_string(index=False))

# 策略B: 涨停回调（短线爆发力）
print("\n【B类-涨停回调】今日涨停或近期涨停, 连板<=2")
if not lim.empty:
    rcb = df["is_limit"] & (df["连板"].fillna(0)<=2) & (df["total_mv"]>300000)
    recB = df[rcb].sort_values("score", ascending=False).head(8)
    if not recB.empty:
        rb = recB.copy()
        rb["mv_yi"] = rb["total_mv"]/10000
        rb["amt_yi"] = rb["amount"]/100000
        rb["nb_yi"] = rb["net_big"]/10000 if "net_big" in rb.columns else 0
        cols_b = [c for c in cols if c in rb.columns]
        print(rb[cols_b].rename(columns=rn).to_string(index=False))
    else:
        print("  无符合条件标的")
else:
    print("  涨停数据缺失")

# 策略C: 大资金买入（主力看好）
print("\n【C类-大资金】主力净流入TOP, 市值>100亿")
if "net_big" in df.columns:
    rcc = (df["net_big"]>5000) & (df["total_mv"]>1000000)
    recC = df[rcc].nlargest(8,"net_big")
    if not recC.empty:
        rc = recC.copy()
        rc["mv_yi"] = rc["total_mv"]/10000
        rc["amt_yi"] = rc["amount"]/100000
        rc["nb_yi"] = rc["net_big"]/10000
        cols_c = [c for c in cols if c in rc.columns]
        print(rc[cols_c].rename(columns=rn).to_string(index=False))
    else:
        print("  无符合条件标的")

# 汇总精选代码
all_rec = set()
if not recA.empty: all_rec.update(recA["ts_code"].tolist())
if not lim.empty and "recB" in dir() and not recB.empty: all_rec.update(recB["ts_code"].tolist())
if "recC" in dir() and not recC.empty: all_rec.update(recC["ts_code"].tolist())
print(f"\n\n全部精选代码({len(all_rec)}只): {sorted(all_rec)}")

df.to_csv("/Users/ecustkiller/WorkBuddy/Claw/screener_full.csv", index=False, encoding="utf-8-sig")
print("已保存 screener_full.csv")
