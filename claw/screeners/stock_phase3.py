#!/usr/bin/env python3
"""Phase 3: 获取4只精选股的完整数据（K线+基本面+资金+新闻概要）"""
import requests, json, time
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts(api, params=None, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params or {}}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0:
        print(f"  ERR {api}: {j.get('msg','')[:80]}")
        return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

stocks = {
    "002179.SZ": "中航光电",
    "601231.SH": "环旭电子",
    "002536.SZ": "飞龙股份",
    "002475.SZ": "立讯精密",
}

# 加一个备选
stocks["000977.SZ"] = "浪潮信息"

for code, name in stocks.items():
    print(f"\n{'='*80}")
    print(f"  {name}({code})")
    print(f"{'='*80}")
    
    # 1. 日K线(近80天)
    kdf = ts("daily", {"ts_code":code, "start_date":"20260101", "end_date":"20260409"},
             "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
    time.sleep(0.8)
    
    if kdf.empty:
        print("  无K线数据")
        continue
    
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    print(f"  K线: {len(kdf)}条 ({kdf['trade_date'].iloc[0]}~{kdf['trade_date'].iloc[-1]})")
    
    # 打印最近10日K线
    print("\n  近10日K线:")
    recent = kdf.tail(10)
    for _, row in recent.iterrows():
        print(f"    {row['trade_date']}: O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} "
              f"C={row['close']:.2f} Chg={row['pct_chg']:.2f}% Vol={row['vol']:.0f} Amt={row['amount']/1000:.1f}百万")
    
    # 2. 计算关键技术指标
    c = kdf["close"].values
    h = kdf["high"].values
    l = kdf["low"].values
    n = len(c)
    
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1]
    ma60 = pd.Series(c).rolling(60).mean().iloc[-1] if n>=60 else None
    
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean()
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean()
    dif_s = ema12 - ema26
    dea_s = dif_s.ewm(span=9, adjust=False).mean()
    macd_s = 2*(dif_s - dea_s)
    
    delta = pd.Series(c).diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = (100 - 100/(1+gain/loss)).iloc[-1]
    
    print(f"\n  技术指标:")
    print(f"    MA5={ma5:.2f} MA10={ma10:.2f} MA20={ma20:.2f} MA60={ma60:.2f}" if ma60 else f"    MA5={ma5:.2f} MA10={ma10:.2f} MA20={ma20:.2f} MA60=N/A")
    print(f"    DIF={dif_s.iloc[-1]:.3f} DEA={dea_s.iloc[-1]:.3f} MACD={macd_s.iloc[-1]:.3f}")
    print(f"    RSI14={rsi:.1f}")
    print(f"    BIAS20={(c[-1]-ma20)/ma20*100:.1f}%")
    
    # 3. 波峰波谷
    peaks, troughs = [], []
    for i in range(3, n-3):
        left3h = max(h[max(0,i-3):i])
        right3h = max(h[i+1:min(n,i+4)])
        if h[i] > left3h and h[i] > right3h:
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        left3l = min(l[max(0,i-3):i])
        right3l = min(l[i+1:min(n,i+4)])
        if l[i] < left3l and l[i] < right3l:
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    
    print(f"\n  波峰锚点: {peaks[-4:] if len(peaks)>=4 else peaks}")
    print(f"  波谷锚点: {troughs[-4:] if len(troughs)>=4 else troughs}")
    
    # 4. 近5日量能变化
    vol5 = kdf["vol"].tail(5).values
    vol20 = kdf["vol"].tail(20).mean()
    print(f"\n  量能: 近5日={[f'{v:.0f}' for v in vol5]}, 20日均量={vol20:.0f}, 比值={np.mean(vol5)/vol20:.2f}")
    
    # 5. 基本面(financial指标)
    time.sleep(0.8)
    fin = ts("fina_indicator", {"ts_code":code, "limit":"4"},
             "ts_code,ann_date,end_date,roe,roe_dt,grossprofit_margin,netprofit_margin,debt_to_assets,eps,bps,ocfps")
    if not fin.empty:
        print(f"\n  财务指标(最近{len(fin)}期):")
        for _, row in fin.iterrows():
            print(f"    {row['end_date']}: ROE={row.get('roe','N/A')} 毛利率={row.get('grossprofit_margin','N/A')} "
                  f"净利率={row.get('netprofit_margin','N/A')} 资产负债率={row.get('debt_to_assets','N/A')} "
                  f"EPS={row.get('eps','N/A')} BPS={row.get('bps','N/A')}")
    
    # 6. 基本信息
    time.sleep(0.5)
    info = ts("daily_basic", {"ts_code":code, "trade_date":"20260409"},
              "ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate_f,volume_ratio")
    if not info.empty:
        row = info.iloc[0]
        print(f"\n  估值: PE_TTM={row.get('pe_ttm','N/A')} PB={row.get('pb','N/A')} "
              f"总市值={row.get('total_mv',0)/10000:.1f}亿 换手率={row.get('turnover_rate_f','N/A')}%")
    
    # 7. 近30日资金流向
    time.sleep(0.5)
    mf_data = []
    for d in ["20260409","20260408","20260407","20260403","20260402"]:
        mfd = ts("moneyflow", {"ts_code":code, "trade_date":d},
                 "ts_code,trade_date,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,net_mf_amount")
        if not mfd.empty:
            mf_data.append(mfd)
        time.sleep(0.3)
    
    if mf_data:
        mf_df = pd.concat(mf_data)
        mf_df["net_big"] = (mf_df["buy_elg_amount"].fillna(0)+mf_df["buy_lg_amount"].fillna(0)
                            -mf_df["sell_elg_amount"].fillna(0)-mf_df["sell_lg_amount"].fillna(0))
        print(f"\n  近5日资金流向:")
        for _, row in mf_df.iterrows():
            print(f"    {row['trade_date']}: 大单净流入={row['net_big']:.1f}万 净流入={row['net_mf_amount']:.1f}万")
    
    print()

# 情绪周期数据（全市场涨跌统计）
print("\n" + "="*80)
print("市场情绪数据(4/9)")
print("="*80)

time.sleep(1)
# 获取4/9全市场数据
d09 = ts("daily", {"trade_date":"20260409"}, "ts_code,pct_chg,vol,amount")
if not d09.empty:
    total = len(d09)
    up = len(d09[d09["pct_chg"]>0])
    down = len(d09[d09["pct_chg"]<0])
    flat = total - up - down
    limit_up = len(d09[d09["pct_chg"]>=9.5])
    limit_down = len(d09[d09["pct_chg"]<=-9.5])
    total_amount = d09["amount"].sum()/100000  # 百万→亿
    print(f"  全市场: {total}只, 上涨{up}({up/total*100:.1f}%) 下跌{down}({down/total*100:.1f}%) 平{flat}")
    print(f"  涨停(>=9.5%): {limit_up}只, 跌停(<=-9.5%): {limit_down}只")
    print(f"  全市场成交额: {total_amount:.0f}亿")
    
    # 涨幅分布
    bins = [-100, -9.5, -5, -3, 0, 3, 5, 9.5, 100]
    labels = ["跌停","跌5-9.5%","跌3-5%","跌0-3%","涨0-3%","涨3-5%","涨5-9.5%","涨停"]
    d09["range"] = pd.cut(d09["pct_chg"], bins=bins, labels=labels)
    dist = d09["range"].value_counts().sort_index()
    print(f"\n  涨跌幅分布:")
    for label, count in dist.items():
        print(f"    {label}: {count}只")

# 4/8数据对比
time.sleep(1)
d08 = ts("daily", {"trade_date":"20260408"}, "ts_code,pct_chg,amount")
if not d08.empty:
    total8 = len(d08)
    up8 = len(d08[d08["pct_chg"]>0])
    limit_up8 = len(d08[d08["pct_chg"]>=9.5])
    total_amount8 = d08["amount"].sum()/100000
    print(f"\n  4/8对比: 全市场{total8}只, 上涨{up8}({up8/total8*100:.1f}%), 涨停{limit_up8}只, 成交{total_amount8:.0f}亿")

print("\n数据采集完成！")
