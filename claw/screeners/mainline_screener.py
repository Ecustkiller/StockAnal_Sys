#!/usr/bin/env python3
"""
市场主线/热点题材识别 + 融入选股
思路：
1. 从全市场涨跌数据中，统计各行业的平均涨幅、涨停数量 → 识别热点行业
2. 从近5日数据中，统计哪些行业持续强势 → 识别主线（非一日游）
3. 将行业热度作为选股加分项
"""
import requests, time
import pandas as pd
import numpy as np
from collections import Counter

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0: return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

# ========== 1. 获取行业映射 ==========
print("="*80)
print("Step 1: 获取行业映射")
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
stk = stk[~stk["name"].str.contains("ST|退", na=False)]
ind_map = dict(zip(stk["ts_code"], stk["industry"]))
print(f"  有效股票: {len(stk)}只, 覆盖 {stk['industry'].nunique()} 个行业")
time.sleep(1)

# ========== 2. 获取近3个交易日全市场涨跌 ==========
print("\nStep 2: 获取近3日全市场数据")
dates = ["20260409", "20260408", "20260407"]
daily_data = {}
for d in dates:
    df = ts("daily", {"trade_date": d}, "ts_code,pct_chg,amount")
    time.sleep(1)
    if not df.empty:
        daily_data[d] = df
        print(f"  {d}: {len(df)}只")

# ========== 3. 行业热度分析 ==========
print("\nStep 3: 行业热度分析")
print("="*80)

for date_str, df in daily_data.items():
    df["industry"] = df["ts_code"].map(ind_map)
    df = df[df["industry"].notna()].copy()
    
    # 各行业平均涨幅
    ind_avg = df.groupby("industry").agg(
        avg_chg=("pct_chg", "mean"),
        median_chg=("pct_chg", "median"),
        count=("ts_code", "count"),
        up_pct=("pct_chg", lambda x: (x>0).sum()/len(x)*100),
        limit_up=("pct_chg", lambda x: (x>=9.5).sum()),
        total_amt=("amount", "sum"),
    ).reset_index()
    
    ind_avg.sort_values("avg_chg", ascending=False, inplace=True)
    
    print(f"\n--- {date_str} 行业涨幅排名 TOP20 ---")
    print(f"{'行业':<10} {'均涨%':>6} {'中位%':>6} {'上涨%':>6} {'涨停':>4} {'股数':>4}")
    print("-"*50)
    for _, row in ind_avg.head(20).iterrows():
        print(f"{row['industry']:<10} {row['avg_chg']:>+6.2f} {row['median_chg']:>+6.2f} "
              f"{row['up_pct']:>5.0f}% {row['limit_up']:>4.0f} {row['count']:>4.0f}")
    
    # 涨停股行业分布
    limits = df[df["pct_chg"]>=9.5]
    if not limits.empty:
        limit_ind = limits["industry"].value_counts().head(10)
        print(f"\n  涨停行业分布: {dict(limit_ind)}")

# ========== 4. 主线识别（近3日持续强势的行业）==========
print("\n\n" + "="*80)
print("Step 4: 主线识别（近3日持续强势）")
print("="*80)

# 计算每个行业在3天的排名
ind_ranks = {}
for date_str, df in daily_data.items():
    df["industry"] = df["ts_code"].map(ind_map)
    ind_avg = df.groupby("industry")["pct_chg"].mean().reset_index()
    ind_avg["rank"] = ind_avg["pct_chg"].rank(ascending=False)
    for _, row in ind_avg.iterrows():
        if row["industry"] not in ind_ranks:
            ind_ranks[row["industry"]] = {}
        ind_ranks[row["industry"]][date_str] = {
            "rank": int(row["rank"]),
            "chg": round(row["pct_chg"], 2)
        }

# 计算平均排名（越小越好）
ind_score = []
for ind, ranks in ind_ranks.items():
    avg_rank = np.mean([v["rank"] for v in ranks.values()])
    avg_chg = np.mean([v["chg"] for v in ranks.values()])
    consistency = sum(1 for v in ranks.values() if v["rank"] <= 20)  # 在TOP20的天数
    
    # 主线得分 = 一致性 * 3 + 平均涨幅 * 2 - 平均排名 * 0.5
    score = consistency * 3 + avg_chg * 2 - avg_rank * 0.1
    
    details = " | ".join([f"{d[-4:]}:#{v['rank']}/{v['chg']:+.1f}%" for d, v in sorted(ranks.items())])
    ind_score.append({"industry": ind, "avg_rank": avg_rank, "avg_chg": avg_chg, 
                       "consistency": consistency, "score": score, "details": details})

ind_score.sort(key=lambda x: x["score"], reverse=True)

print(f"\n{'行业':<10} {'得分':>5} {'均排名':>6} {'均涨%':>6} {'TOP20天数':>8}  每日详情")
print("-"*100)
for item in ind_score[:25]:
    marker = "🔥" if item["consistency"] >= 2 and item["avg_chg"] > 1 else "  "
    print(f"{marker}{item['industry']:<8} {item['score']:>5.1f} {item['avg_rank']:>6.1f} "
          f"{item['avg_chg']:>+6.2f} {item['consistency']:>8d}  {item['details']}")

# 标记主线行业
mainline = set()
for item in ind_score[:15]:
    if item["consistency"] >= 2 and item["avg_chg"] > 0.5:
        mainline.add(item["industry"])

print(f"\n🔥 识别出的主线行业({len(mainline)}个): {sorted(mainline)}")

# ========== 5. 将主线信息融入选股 ==========
print("\n\n" + "="*80)
print("Step 5: 主线加持选股")
print("="*80)

# 重新筛选，加入主线加分
# 获取基本面
time.sleep(1)
bas = ts("daily_basic", {"trade_date":"20260409"}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f")
bas_dict = {}
if not bas.empty:
    for _, row in bas.iterrows():
        bas_dict[row["ts_code"]] = row.to_dict()

# 资金
time.sleep(1)
mf = ts("moneyflow", {"trade_date":"20260409"}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
mf_dict = {}
if not mf.empty:
    for _, row in mf.iterrows():
        nb = row["buy_elg_amount"]+row["buy_lg_amount"]-row["sell_elg_amount"]-row["sell_lg_amount"]
        mf_dict[row["ts_code"]] = nb

# 多日收盘价
dates_ext = ["20260409","20260407","20260402","20260327","20260313"]
prices = {}
for d in dates_ext:
    df = ts("daily", {"trade_date":d}, "ts_code,close")
    time.sleep(1)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in prices: prices[row["ts_code"]] = {}
            prices[row["ts_code"]][d] = row["close"]

# 综合筛选
final = []
for code, p in prices.items():
    if not code[:2] in ["00","30","60","68"]: continue
    ds = sorted(p.keys(), reverse=True)
    if len(ds)<5: continue
    c0,c1,c2,c3,c4 = [p[ds[i]] for i in range(5)]
    r5=(c0-c2)/c2*100; r10=(c0-c3)/c3*100; r20=(c0-c4)/c4*100
    
    # 多周期评分
    big = "上涨" if r20>5 else ("下跌" if r20<-5 else "停顿")
    mid = "上涨" if r10>3 else ("下跌" if r10<-3 else "停顿")
    small = "上涨" if r5>2 else ("下跌" if r5<-2 else "停顿")
    s = {"上涨":1,"下跌":-1,"停顿":0}
    period_score = s[big]*3 + s[mid]*2 + s[small]*1
    if period_score < 5: continue
    
    nm_row = stk[stk["ts_code"]==code]
    if nm_row.empty: continue
    nm = nm_row["name"].iloc[0]; ind = nm_row["industry"].iloc[0]
    if "ST" in nm or "退" in nm: continue
    
    b = bas_dict.get(code, {})
    pe = b.get("pe_ttm"); mv = b.get("total_mv"); tr = b.get("turnover_rate_f")
    if not pe or pe<=0 or pe>60: continue
    if not mv or mv<500000: continue  # >50亿
    if r5>15 or r10>25: continue
    if tr and tr>15: continue
    
    nb = mf_dict.get(code, 0)
    
    # 主线加分
    is_mainline = ind in mainline
    mainline_bonus = 3 if is_mainline else 0
    
    # 综合得分 = 多周期 + 主线加分 + (净流入>0加1分)
    total = period_score + mainline_bonus + (1 if nb>0 else 0)
    
    final.append({
        "code":code, "name":nm, "ind":ind, "close":c0,
        "pe":pe, "mv":mv/10000, "tr":tr,
        "r5":r5, "r10":r10, "r20":r20,
        "period":period_score, "mainline":is_mainline, "bonus":mainline_bonus,
        "nb":nb/10000, "total":total
    })

final.sort(key=lambda x: (-x["total"], -x["nb"]))

print(f"\n精选结果: {len(final)}只 (多周期>=5 + PE<60 + 市值>50亿 + 涨幅可控)")
print(f"其中主线行业: {sum(1 for f in final if f['mainline'])}只")

print(f"\n{'排名':<3} {'股票':<18} {'行业':<10} {'主线':>4} {'收盘':>7} {'PE':>5} {'市值':>6} "
      f"{'5日%':>5} {'10日%':>6} {'净流亿':>6} {'周期':>3} {'加分':>3} {'总分':>4}")
print("-"*120)

for i, r in enumerate(final[:30], 1):
    ml = "🔥" if r["mainline"] else "  "
    print(f"{i:<3d} {r['name']}({r['code'][:6]}) {r['ind']:<10} {ml:>4} {r['close']:>7.2f} "
          f"{r['pe']:>5.0f} {r['mv']:>5.0f} {r['r5']:>+5.1f} {r['r10']:>+6.1f} "
          f"{r['nb']:>+6.2f} {r['period']:>+3d} {r['bonus']:>3d} {r['total']:>4d}")

print(f"\n\n主线行业内TOP10:")
ml_stocks = [f for f in final if f["mainline"]]
for i, r in enumerate(ml_stocks[:10], 1):
    print(f"  {i}. {r['name']}({r['code'][:6]}) {r['ind']} PE={r['pe']:.0f} "
          f"5日={r['r5']:+.1f}% 净流入={r['nb']:+.2f}亿 总分={r['total']}")
