#!/usr/bin/env python3
"""
A股综合评分选股系统 v3.0 — 基于4/16最新数据
用于生成4/17（明天）及下周一整周推荐标的
"""
import requests, time, json, sys
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
def ts(api, params={}, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params}
    if fields: d["fields"] = fields
    for retry in range(3):
        try:
            r = requests.post("http://api.tushare.pro", json=d, timeout=30)
            j = r.json()
            if j.get("code") != 0:
                print(f"  ⚠ API {api} 返回错误: {j.get('msg','')}")
                return pd.DataFrame()
            return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
        except Exception as e:
            print(f"  ⚠ API {api} 重试{retry+1}: {e}")
            time.sleep(2)
    return pd.DataFrame()

print("="*80)
print("A股综合评分选股系统 v3.0（风险扣分制）")
print("目标：4/17（明天）及下周一整周推荐")
print("="*80)

# ===== 交易日历 =====
print("\n[交易日历]")
cal = ts("trade_cal", {"exchange":"SSE","start_date":"20260301","end_date":"20260430"}, "cal_date,is_open")
cal = cal[cal["is_open"]==1].sort_values("cal_date").reset_index(drop=True)
trade_dates = cal["cal_date"].tolist()

# 最新交易日
T0 = "20260416"
t0_idx = trade_dates.index(T0)
T_1 = trade_dates[t0_idx-1]  # T-1
T_2 = trade_dates[t0_idx-2]  # T-2

# 多周期日期
T_5 = trade_dates[t0_idx-5]   # 5日前
T_10 = trade_dates[t0_idx-10] # 10日前
T_20 = trade_dates[t0_idx-20] # 20日前

print(f"  T0={T0}, T-1={T_1}, T-2={T_2}")
print(f"  T-5={T_5}, T-10={T_10}, T-20={T_20}")

# 下周交易日
next_week = [d for d in trade_dates if d > T0 and d <= "20260424"]
print(f"  明天: 20260417")
print(f"  下周: {next_week}")

# ===== 数据采集 =====
print("\n[数据采集]")

# 行业映射
stk = ts("stock_basic", {"list_status":"L"}, "ts_code,name,industry")
stk = stk[stk["ts_code"].str.match(r"^(00|30|60|68)")]
stk = stk[~stk["name"].str.contains("ST|退", na=False)]
ind_map = dict(zip(stk["ts_code"], stk["industry"]))
name_map = dict(zip(stk["ts_code"], stk["name"]))
print(f"  股票: {len(stk)}只（非ST）")
time.sleep(0.5)

# 近3日全市场行情
dates3 = [T0, T_1, T_2]
daily3 = {}
for d in dates3:
    df = ts("daily", {"trade_date":d}, "ts_code,pct_chg,amount,open,high,low,close,vol")
    time.sleep(0.8)
    daily3[d] = df
    print(f"  {d}: {len(df)}只")

# 多周期收盘价（T0, T-5, T-10, T-20）
dates_mp = [T0, T_5, T_10, T_20]
cp = {}
for d in dates_mp:
    if d in [T0, T_1, T_2]:
        df = daily3.get(d, pd.DataFrame())
    else:
        df = ts("daily", {"trade_date":d}, "ts_code,close")
        time.sleep(0.8)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] not in cp: cp[row["ts_code"]] = {}
            cp[row["ts_code"]][d] = row["close"]
    print(f"  价格{d}: {len(df)}只")

# 基本面
time.sleep(0.5)
bas = ts("daily_basic", {"trade_date":T0}, "ts_code,pe_ttm,pb,total_mv,turnover_rate_f,volume_ratio")
bas_d = {row["ts_code"]: row.to_dict() for _, row in bas.iterrows()} if not bas.empty else {}
print(f"  基本面: {len(bas)}只")

# 资金流向
time.sleep(0.5)
mf = ts("moneyflow", {"trade_date":T0}, "ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount")
mf_d = {}
if not mf.empty:
    for _, row in mf.iterrows():
        mf_d[row["ts_code"]] = row["buy_elg_amount"]+row["buy_lg_amount"]-row["sell_elg_amount"]-row["sell_lg_amount"]
print(f"  资金: {len(mf_d)}只")

# ===== 主线识别（近7天窗口） =====
print("\n[主线识别]")

# 扩展到7天窗口
dates7_start_idx = max(0, t0_idx - 6)
dates7 = trade_dates[dates7_start_idx:t0_idx+1]
print(f"  主线窗口: {dates7[0]}~{dates7[-1]} ({len(dates7)}天)")

# 获取7天数据（已有3天，补4天）
daily7 = dict(daily3)
for d in dates7:
    if d not in daily7:
        df = ts("daily", {"trade_date":d}, "ts_code,pct_chg,amount,open,high,low,close,vol")
        time.sleep(0.8)
        daily7[d] = df
        print(f"  补充{d}: {len(df)}只")

ind_perf = {}
for d in dates7:
    df = daily7.get(d, pd.DataFrame())
    if df.empty: continue
    df = df.copy()
    df["ind"] = df["ts_code"].map(ind_map)
    grp = df.groupby("ind").agg(avg=("pct_chg","mean"), lim=("pct_chg", lambda x:(x>=9.5).sum())).reset_index()
    grp["rk"] = grp["avg"].rank(ascending=False)
    for _, row in grp.iterrows():
        if row["ind"] not in ind_perf: ind_perf[row["ind"]] = []
        ind_perf[row["ind"]].append({"date":d, "avg":row["avg"], "rk":int(row["rk"]), "lim":int(row["lim"])})

mainline_scores = {}
for ind, perfs in ind_perf.items():
    avg_chg = np.mean([p["avg"] for p in perfs])
    avg_rk = np.mean([p["rk"] for p in perfs])
    total_lim = sum(p["lim"] for p in perfs)
    top20_days = sum(1 for p in perfs if p["rk"]<=20)
    
    score = 0
    if avg_rk <= 10: score += 8
    elif avg_rk <= 20: score += 5
    elif avg_rk <= 30: score += 3
    elif avg_rk <= 50: score += 1
    
    if top20_days >= 5: score += 6
    elif top20_days >= 3: score += 5
    elif top20_days >= 2: score += 4
    elif top20_days >= 1: score += 2
    
    if total_lim >= 30: score += 6
    elif total_lim >= 15: score += 5
    elif total_lim >= 10: score += 4
    elif total_lim >= 5: score += 3
    elif total_lim >= 2: score += 1
    
    mainline_scores[ind] = min(score, 20)

top_ind = sorted(mainline_scores.items(), key=lambda x:x[1], reverse=True)[:20]
print(f"  主线行业TOP20:")
for ind, sc in top_ind:
    print(f"    {ind}: {sc}分")

# 全市场成交额
total_market_amount = 0
if daily3.get(T0) is not None and not daily3[T0].empty:
    total_market_amount = daily3[T0]["amount"].sum() / 100000
print(f"  全市场成交额: {total_market_amount:.0f}亿")

if total_market_amount > 20000:
    dynamic_mv_cap = 500
elif total_market_amount > 15000:
    dynamic_mv_cap = 300
else:
    dynamic_mv_cap = 150
print(f"  动态市值上限: {dynamic_mv_cap}亿")

# ===== 市场情绪 =====
print("\n[市场情绪]")
d0_df = daily3.get(T0, pd.DataFrame())
if not d0_df.empty:
    d0_nst = d0_df[d0_df["ts_code"].map(lambda x: x in name_map)]  # 非ST
    zt_count = (d0_nst["pct_chg"] >= 9.5).sum()
    dt_count = (d0_nst["pct_chg"] <= -9.5).sum()
    up_count = (d0_nst["pct_chg"] > 0).sum()
    down_count = (d0_nst["pct_chg"] < 0).sum()
    total_count = len(d0_nst)
    earn_ratio = up_count / total_count * 100 if total_count > 0 else 0
    
    # 炸板数估算（涨幅7-9.5%中的一部分）
    near_zt = ((d0_nst["pct_chg"] >= 7) & (d0_nst["pct_chg"] < 9.5)).sum()
    est_zb = int(near_zt * 0.5)  # 粗估
    fb_rate = zt_count / (zt_count + est_zb) * 100 if (zt_count + est_zb) > 0 else 0
    
    print(f"  涨停: {zt_count}只 | 跌停: {dt_count}只")
    print(f"  上涨: {up_count}只 | 下跌: {down_count}只 | 赚钱效应: {earn_ratio:.0f}%")
    print(f"  封板率(估): {fb_rate:.0f}%")
    
    # 情绪判定
    if fb_rate >= 75 and dt_count <= 5 and earn_ratio >= 60:
        emotion = "亢奋期"
        suggest_pos = "6-8成"
    elif fb_rate >= 60 and dt_count <= 15 and earn_ratio >= 50:
        emotion = "正常期"
        suggest_pos = "5-6成"
    elif fb_rate >= 45 and earn_ratio >= 35:
        emotion = "修复期"
        suggest_pos = "3-4成"
    else:
        emotion = "防御期"
        suggest_pos = "1-2成"
    
    print(f"  情绪判定: {emotion} → 建议仓位{suggest_pos}")
else:
    emotion = "未知"
    suggest_pos = "3成"
    zt_count = 0

# ===== 粗筛候选 =====
print(f"\n[粗筛候选]")
rough_candidates = set()
for code, p in cp.items():
    if T0 not in p or T_5 not in p or T_10 not in p or T_20 not in p:
        continue
    c0 = p[T0]; c5 = p[T_5]; c10 = p[T_10]; c20 = p[T_20]
    r5 = (c0-c5)/c5*100
    r10 = (c0-c10)/c10*100
    r20 = (c0-c20)/c20*100
    
    big = 1 if r20>5 else (-1 if r20<-5 else 0)
    mid = 1 if r10>3 else (-1 if r10<-3 else 0)
    small = 1 if r5>2 else (-1 if r5<-2 else 0)
    ps = big*3+mid*2+small*1
    
    if ps < 1: continue  # v3.0: 放宽到ps>=1（原为>=4），让更多标的进入评分
    
    b = bas_d.get(code, {})
    mv = b.get("total_mv")
    if not mv or mv < 200000: continue  # >20亿
    if r5 > 40 or r10 > 50: continue  # 仅排除极端暴涨
    
    rough_candidates.add(code)

print(f"  粗筛候选: {len(rough_candidates)}只")

# ===== 获取K线数据 =====
print(f"\n[K线数据]")
kline_data = {}

# 获取更多历史K线（20天窗口）
kline_dates = trade_dates[max(0, t0_idx-25):t0_idx+1]
for d in kline_dates:
    if d in daily7:
        df = daily7[d]
    else:
        df = ts("daily", {"trade_date":d}, "ts_code,trade_date,open,high,low,close,pct_chg,vol")
        time.sleep(0.8)
    if not df.empty:
        for _, row in df.iterrows():
            if row["ts_code"] in rough_candidates:
                if row["ts_code"] not in kline_data:
                    kline_data[row["ts_code"]] = []
                kline_data[row["ts_code"]].append({
                    "ts_code":row["ts_code"],
                    "trade_date":row.get("trade_date", d),
                    "open":float(row.get("open",0)),
                    "high":float(row.get("high",0)),
                    "low":float(row.get("low",0)),
                    "close":float(row["close"]),
                    "pct_chg":float(row.get("pct_chg",0)),
                    "vol":float(row.get("vol",0))
                })
    print(f"  K线{d}: done")

print(f"  K线覆盖: {len(kline_data)}只")

# ===== 预计算板块涨停 =====
ind_zt_map = {}
if daily3.get(T0) is not None and not daily3[T0].empty:
    d0_df_c = daily3[T0].copy()
    d0_df_c["ind"] = d0_df_c["ts_code"].map(ind_map)
    for ind_name, grp in d0_df_c.groupby("ind"):
        zt_c = (grp["pct_chg"] >= 9.5).sum()
        ind_zt_map[ind_name] = zt_c

# ===== 综合评分 =====
print(f"\n[综合评分]")
results = []

for code in rough_candidates:
    nm = name_map.get(code, "?")
    ind = ind_map.get(code, "?")
    b = bas_d.get(code, {})
    pe = b.get("pe_ttm")
    mv = b.get("total_mv", 0)/10000
    tr = b.get("turnover_rate_f", 0)
    nb = mf_d.get(code, 0)
    nb_yi = nb/10000
    
    p = cp.get(code, {})
    if T0 not in p or T_5 not in p or T_10 not in p or T_20 not in p:
        continue
    c0 = p[T0]; c5 = p[T_5]; c10 = p[T_10]; c20 = p[T_20]
    r5 = (c0-c5)/c5*100
    r10 = (c0-c10)/c10*100
    r20 = (c0-c20)/c20*100
    
    # ====== 维度1: 多周期共振 (15分) ======
    big = 1 if r20>5 else (-1 if r20<-5 else 0)
    mid = 1 if r10>3 else (-1 if r10<-3 else 0)
    small = 1 if r5>2 else (-1 if r5<-2 else 0)
    period_raw = big*3+mid*2+small*1
    
    d1_score = 0
    if period_raw >= 6: d1_score = 15
    elif period_raw >= 5: d1_score = 12
    elif period_raw >= 4: d1_score = 9
    elif period_raw >= 3: d1_score = 6
    elif period_raw >= 1: d1_score = 3
    
    # ====== 维度2: 主线热点 (20分) ======
    d2_score = mainline_scores.get(ind, 0)
    
    # ====== 维度3: 三Skill (35分) ======
    klines = kline_data.get(code, [])
    d3_score = 0; mistery = 0; tds = 0
    is_ma_bull = False; consecutive_yang = 0
    
    if klines:
        kdf = pd.DataFrame(klines)
        if "trade_date" in kdf.columns:
            kdf = kdf.drop_duplicates(subset=["trade_date"]).copy()
            kdf.sort_values("trade_date", inplace=True)
            kdf.reset_index(drop=True, inplace=True)
            n = len(kdf)
            
            if n >= 10:
                cc = kdf["close"].astype(float).values
                hh = kdf["high"].astype(float).values
                ll = kdf["low"].astype(float).values
                oo = kdf["open"].astype(float).values
                vv = kdf["vol"].astype(float).values
                
                ma5 = pd.Series(cc).rolling(5).mean().iloc[-1]
                ma10 = pd.Series(cc).rolling(10).mean().iloc[-1]
                ma20 = pd.Series(cc).rolling(min(20,n)).mean().iloc[-1]
                
                ema12 = pd.Series(cc).ewm(span=12,adjust=False).mean()
                ema26 = pd.Series(cc).ewm(span=min(26,n),adjust=False).mean()
                dif = (ema12-ema26).iloc[-1]
                dea = (ema12-ema26).ewm(span=9,adjust=False).mean().iloc[-1]
                
                is_ma_bull = cc[-1]>ma5>ma10>ma20
                
                for k in range(n-1, -1, -1):
                    if cc[k] > oo[k]: consecutive_yang += 1
                    else: break
                
                # Mistery (15分)
                if cc[-1]>ma5>ma10>ma20: mistery += 5
                elif cc[-1]>ma5>ma10: mistery += 4
                elif cc[-1]>ma20: mistery += 3
                elif cc[-1]>ma5: mistery += 2
                
                ma5s = pd.Series(cc).rolling(5).mean()
                ma20s = pd.Series(cc).rolling(min(20,n)).mean()
                if n>=7 and not np.isnan(ma5s.iloc[-5]) and ma5s.iloc[-5]<=ma20s.iloc[-5] and ma5>ma20:
                    mistery += 3
                elif ma5>ma20:
                    mistery += 2
                if n>=5:
                    below = any(cc[i]<ma5s.iloc[i] for i in range(max(0,n-5),n-1) if not np.isnan(ma5s.iloc[i]))
                    if below and cc[-1]>ma5: mistery += 2
                
                vol5 = np.mean(vv[-5:]); vol10 = np.mean(vv[-min(10,n):])
                if vol5/vol10 > 1.3: mistery += 2
                elif vol5/vol10 > 1: mistery += 1
                if dif > dea: mistery += 2
                elif dif > 0: mistery += 1
                mistery = min(mistery, 15)
                
                # TDS (10分)
                peaks, troughs = [], []
                for i in range(3, n-3):
                    if hh[i]>max(hh[max(0,i-3):i]) and hh[i]>max(hh[i+1:min(n,i+4)]):
                        peaks.append(hh[i])
                    if ll[i]<min(ll[max(0,i-3):i]) and ll[i]<min(ll[i+1:min(n,i+4)]):
                        troughs.append(ll[i])
                
                if len(peaks)>=2 and len(troughs)>=2:
                    if peaks[-1]>peaks[-2] and troughs[-1]>troughs[-2]: tds += 4
                    elif peaks[-1]>peaks[-2] or troughs[-1]>troughs[-2]: tds += 2
                if n>=2 and hh[-1]>hh[-2] and ll[-1]>ll[-2]: tds += 2
                if peaks and cc[-1]>peaks[-1]: tds += 2
                if n>=2 and cc[-1]>oo[-1] and cc[-1]>hh[-2] and cc[-2]<oo[-2]: tds += 2
                tds = min(tds, 10)
                
                # 元子元情绪 (10分)
                yuanzi = 0
                pct_last = kdf["pct_chg"].astype(float).iloc[-1]
                if r5 > 10: yuanzi += 3
                elif r5 > 5: yuanzi += 4
                elif r5 > 0: yuanzi += 3
                if pct_last > 0: yuanzi += 3
                elif pct_last > -1: yuanzi += 2
                if mainline_scores.get(ind, 0) >= 10: yuanzi += 3
                elif mainline_scores.get(ind, 0) >= 5: yuanzi += 2
                yuanzi = min(yuanzi, 10)
                
                d3_score = mistery + tds + yuanzi
    
    # ====== 维度4: 安全边际 (15分) ======
    d4_score = 0
    if abs(r5) <= 5: d4_score += 5
    elif abs(r5) <= 10: d4_score += 3
    elif abs(r5) <= 15: d4_score += 1
    if abs(r10) <= 10: d4_score += 5
    elif abs(r10) <= 15: d4_score += 3
    elif abs(r10) <= 20: d4_score += 1
    if tr and tr <= 5: d4_score += 5
    elif tr and tr <= 10: d4_score += 3
    elif tr and tr <= 15: d4_score += 1
    d4_score = min(d4_score, 15)
    
    # ====== 维度5: 基本面 (15分) ======
    d5_score = 0
    if pe and pe > 0:
        if pe <= 15: d5_score += 6
        elif pe <= 25: d5_score += 5
        elif pe <= 40: d5_score += 4
        elif pe <= 60: d5_score += 3
        elif pe <= 100: d5_score += 1
    if 100 <= mv <= 500: d5_score += 3
    elif 500 < mv <= 2000: d5_score += 2
    elif mv > 2000: d5_score += 1
    elif 50 <= mv < 100: d5_score += 2
    if nb_yi > 1: d5_score += 6
    elif nb_yi > 0.3: d5_score += 4
    elif nb_yi > 0: d5_score += 2
    d5_score = min(d5_score, 15)
    
    # 判断是否涨停（供风险/保护因子使用）
    is_zt = False
    if klines:
        kdf_tmp = pd.DataFrame(klines)
        if "pct_chg" in kdf_tmp.columns and len(kdf_tmp) > 0:
            kdf_tmp.sort_values("trade_date", inplace=True)
            last_pct = float(kdf_tmp["pct_chg"].iloc[-1])
            is_zt = last_pct >= 9.5
    
    ind_zt_count = ind_zt_map.get(ind, 0)
    
    # ====== 维度6: 风险扣分 (0~-30分) ======
    risk_deduct = 0; risk_tags = []
    
    if r5 > 20:
        if is_ma_bull: risk_deduct += 3; risk_tags.append(f"超涨5日{r5:.0f}%-3")
        else: risk_deduct += 5; risk_tags.append(f"超涨5日{r5:.0f}%-5")
    elif r5 > 15:
        risk_deduct += 2; risk_tags.append(f"偏涨5日{r5:.0f}%-2")
    
    if r10 > 25:
        if is_ma_bull: risk_deduct += 3; risk_tags.append(f"超涨10日{r10:.0f}%-3")
        else: risk_deduct += 5; risk_tags.append(f"超涨10日{r10:.0f}%-5")
    elif r10 > 20:
        risk_deduct += 2; risk_tags.append(f"偏涨10日{r10:.0f}%-2")
    
    if r20 > 50:
        risk_deduct += 8; risk_tags.append(f"极端超涨20日{r20:.0f}%-8")
    elif r20 > 35:
        risk_deduct += 4; risk_tags.append(f"超涨20日{r20:.0f}%-4")
    
    if ind_zt_count < 3:
        if mainline_scores.get(ind, 0) >= 8:
            risk_deduct += 2; risk_tags.append(f"行业涨停{ind_zt_count}家-2")
        elif ind_zt_count == 0:
            risk_deduct += 5; risk_tags.append(f"行业涨停0家-5")
        else:
            risk_deduct += 3; risk_tags.append(f"行业涨停{ind_zt_count}家-3")
    
    if nb_yi < -2:
        if is_zt: risk_deduct += 1; risk_tags.append(f"涨停净流出{nb_yi:.1f}亿-1")
        else: risk_deduct += 3; risk_tags.append(f"净流出{nb_yi:.1f}亿-3")
    elif nb_yi < -0.5:
        risk_deduct += 1; risk_tags.append(f"小幅净流出{nb_yi:.1f}亿-1")
    
    if mv > dynamic_mv_cap:
        if mv > 1000: risk_deduct += 5; risk_tags.append(f"市值{mv:.0f}亿-5")
        else: risk_deduct += 3; risk_tags.append(f"市值{mv:.0f}亿-3")
    
    if tr and tr > 50:
        risk_deduct += 3; risk_tags.append(f"高换手{tr:.0f}%-3")
    elif tr and tr > 30:
        risk_deduct += 1; risk_tags.append(f"换手偏高{tr:.0f}%-1")
    
    risk_deduct = min(risk_deduct, 30)
    
    # ====== 保护因子 (0~+15分) ======
    protect_bonus = 0; protect_tags = []
    
    if is_ma_bull: protect_bonus += 3; protect_tags.append("趋势多头+3")
    if consecutive_yang >= 5: protect_bonus += 3; protect_tags.append(f"连阳{consecutive_yang}天+3")
    elif consecutive_yang >= 3: protect_bonus += 2; protect_tags.append(f"连阳{consecutive_yang}天+2")
    if mistery >= 12: protect_bonus += 2; protect_tags.append("Mistery高分+2")
    if is_zt: protect_bonus += 3; protect_tags.append("涨停+3")
    if is_zt and ind_zt_count >= 3: protect_bonus += 2; protect_tags.append("板块龙头+2")
    if nb_yi > 2: protect_bonus += 2; protect_tags.append(f"大单{nb_yi:.1f}亿+2")
    protect_bonus = min(protect_bonus, 15)
    
    # ====== 最终得分 ======
    raw_total = d1_score + d2_score + d3_score + d4_score + d5_score
    net_risk = max(risk_deduct - protect_bonus, 0)
    total = raw_total - net_risk
    
    results.append({
        "code":code, "name":nm, "ind":ind, "close":c0,
        "pe":pe, "mv":mv, "tr":tr, "nb_yi":nb_yi,
        "r5":r5, "r10":r10, "r20":r20,
        "d1":d1_score, "d2":d2_score, "d3":d3_score, "d4":d4_score, "d5":d5_score,
        "risk":risk_deduct, "protect":protect_bonus, "net_risk":net_risk,
        "risk_tags":"|".join(risk_tags) if risk_tags else "-",
        "protect_tags":"|".join(protect_tags) if protect_tags else "-",
        "raw_total":raw_total, "total":total
    })

results.sort(key=lambda x: x["total"], reverse=True)

# ===== 输出 =====
print(f"\n{'='*180}")
print(f"综合评分TOP40 v3.0（风险扣分制）— 基于{T0}数据")
print(f"最终得分 = 五维原始分(满分100) - max(风险扣分-保护因子, 0)")
print(f"市场情绪: {emotion} | 建议仓位: {suggest_pos} | 成交额: {total_market_amount:.0f}亿 | 动态市值上限: {dynamic_mv_cap}亿")
print(f"{'='*180}")
print(f"{'#':>2} {'股票':<18} {'行业':<8} {'收盘':>7} {'PE':>6} {'市值':>6} {'5日%':>6} {'10日%':>6} "
      f"{'净流亿':>6} {'五维':>4} {'风险':>4} {'保护':>4} {'净扣':>4} {'总分':>4} {'级别':<6} {'风险明细'}")
print("-"*160)

for i, r in enumerate(results[:40], 1):
    level = "⭐强推" if r["total"]>=85 else ("✅推荐" if r["total"]>=72 else ("👀关注" if r["total"]>=60 else "  "))
    pe_str = f"{r['pe']:.0f}" if r['pe'] and r['pe']>0 else "N/A"
    risk_detail = r.get('risk_tags', '-')
    protect_detail = r.get('protect_tags', '-')
    detail_str = risk_detail
    if protect_detail != "-":
        detail_str += " 🛡" + protect_detail
    print(f"{i:>2d} {r['name']}({r['code'][:6]}) {r['ind']:<8} {r['close']:>7.2f} {pe_str:>6} "
          f"{r['mv']:>6.0f} {r['r5']:>+6.1f} {r['r10']:>+6.1f} {r['nb_yi']:>+6.2f} "
          f"{r['raw_total']:>4d} {r['risk']:>4d} {r['protect']:>4d} {r['net_risk']:>4d} {r['total']:>4d} {level:<6} {detail_str}")

# 统计
strong = [r for r in results if r["total"]>=85]
good = [r for r in results if 72<=r["total"]<85]
watch = [r for r in results if 60<=r["total"]<72]

# v3.0新增统计
high_risk = [r for r in results if r["risk"] >= 10]
protected = [r for r in results if r["protect"] >= 5]
print(f"\n风险扣分统计:")
print(f"  高风险(扣≥10分): {len(high_risk)}只")
print(f"  有保护因子(≥5分): {len(protected)}只")
rescued = [r for r in results[:40] if r["risk"] >= 8 and r["total"] >= 60]
if rescued:
    print(f"  🆕 v2.0会被排除但v3.0保留的标的:")
    for r in rescued:
        print(f"    {r['name']}({r['code'][:6]}) 六维{r['raw_total']}分 风险-{r['risk']} 保护+{r['protect']} → 最终{r['total']}分 | {r['risk_tags']} 🛡{r['protect_tags']}")

print(f"\n统计: ⭐强推{len(strong)}只 | ✅推荐{len(good)}只 | 👀关注{len(watch)}只 | 总计{len(results)}只")

# 保存结果
with open("/Users/ecustkiller/WorkBuddy/Claw/v3_score_0416.json", "w") as f:
    json.dump(results[:40], f, ensure_ascii=False, indent=2)
print(f"\n已保存 v3_score_0416.json (TOP40)")

# ===== 生成推荐报告 =====
print(f"\n{'='*80}")
print(f"📋 4/17及下周推荐标的池")
print(f"{'='*80}")

# 按级别分组
print(f"\n⭐ 强推标的（≥85分）— 可重点关注:")
for r in strong[:10]:
    print(f"  {r['name']}({r['code'][:6]}) {r['ind']} | 总分{r['total']} | 5日{r['r5']:+.1f}% 10日{r['r10']:+.1f}% | 净流{r['nb_yi']:+.1f}亿")

print(f"\n✅ 推荐标的（72-84分）— 可适量参与:")
for r in good[:10]:
    print(f"  {r['name']}({r['code'][:6]}) {r['ind']} | 总分{r['total']} | 5日{r['r5']:+.1f}% 10日{r['r10']:+.1f}% | 净流{r['nb_yi']:+.1f}亿")

print(f"\n👀 关注标的（60-71分）— 观察为主:")
for r in watch[:10]:
    print(f"  {r['name']}({r['code'][:6]}) {r['ind']} | 总分{r['total']} | 5日{r['r5']:+.1f}% 10日{r['r10']:+.1f}% | 净流{r['nb_yi']:+.1f}亿")

# 方向聚焦
print(f"\n📊 方向聚焦（TOP40行业分布）:")
ind_dist = {}
for r in results[:40]:
    ind_dist[r['ind']] = ind_dist.get(r['ind'], 0) + 1
for ind, cnt in sorted(ind_dist.items(), key=lambda x:x[1], reverse=True)[:10]:
    ms = mainline_scores.get(ind, 0)
    zt = ind_zt_map.get(ind, 0)
    print(f"  {ind}: {cnt}只 | 主线{ms}分 | 涨停{zt}家")

print(f"\n⚠️ 风控提示:")
print(f"  市场情绪: {emotion} → 建议总仓位{suggest_pos}")
print(f"  风控红线: 跌停>20家→空仓 | 单日回撤>3%→停手 | 封板率<40%→减半仓")
print(f"  操作纪律: 强推标的可1-1.5成/只 | 推荐标的0.5-1成/只 | 关注标的仅观察")
