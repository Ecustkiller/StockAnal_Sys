#!/usr/bin/env python3
"""亨通光电(600487)完整数据采集 — 含实时行情+K线+基本面+资金流向+情绪周期数据"""
import requests, json, time, re
import pandas as pd
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts(api, params={}, fields=None):
    d = {"api_name": api, "token": TOKEN, "params": params}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0:
        print(f"  [API Error] {api}: {j.get('msg','')}")
        return pd.DataFrame()
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])

CODE = "600487.SH"
print("="*80)
print(f"亨通光电({CODE}) 完整数据采集")
print("="*80)

# ==================== 1. 实时行情 ====================
print("\n[1/7] 获取实时行情...")
try:
    # 新浪实时行情
    url = f"https://hq.sinajs.cn/list=sh600487"
    headers = {"Referer": "https://finance.sina.com.cn"}
    r = requests.get(url, headers=headers, timeout=10)
    raw = r.text
    match = re.search(r'"(.+)"', raw)
    if match:
        fields = match.group(1).split(",")
        if len(fields) > 30:
            rt = {
                "name": fields[0], "open": float(fields[1]), "pre_close": float(fields[2]),
                "price": float(fields[3]), "high": float(fields[4]), "low": float(fields[5]),
                "volume": float(fields[8]), "amount": float(fields[9]),
                "buy1_vol": float(fields[10]), "buy1_price": float(fields[11]),
                "sell1_vol": float(fields[18]), "sell1_price": float(fields[19]),
                "date": fields[30], "time": fields[31]
            }
            rt["pct_chg"] = (rt["price"] - rt["pre_close"]) / rt["pre_close"] * 100
            rt["turnover_est"] = rt["amount"] / 10000  # 万元
            print(f"  实时价格: {rt['price']:.2f} ({rt['pct_chg']:+.2f}%)")
            print(f"  今开: {rt['open']:.2f}  最高: {rt['high']:.2f}  最低: {rt['low']:.2f}")
            print(f"  昨收: {rt['pre_close']:.2f}")
            print(f"  成交量: {rt['volume']/10000:.0f}万手  成交额: {rt['amount']/100000000:.2f}亿")
            print(f"  买一: {rt['buy1_price']:.2f}({rt['buy1_vol']:.0f}手)  卖一: {rt['sell1_price']:.2f}({rt['sell1_vol']:.0f}手)")
            print(f"  更新时间: {rt['date']} {rt['time']}")
        else:
            print("  实时数据格式异常")
            rt = None
    else:
        print("  未获取到实时数据")
        rt = None
except Exception as e:
    print(f"  实时行情异常: {e}")
    rt = None

# ==================== 2. 历史K线(60日) ====================
print("\n[2/7] 获取历史K线(60日)...")
kdf = ts("daily", {"ts_code": CODE, "start_date": "20260101", "end_date": "20260410"},
          "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount")
time.sleep(0.5)

if not kdf.empty:
    kdf.sort_values("trade_date", inplace=True)
    kdf.reset_index(drop=True, inplace=True)
    c = kdf["close"].values; h = kdf["high"].values; l = kdf["low"].values; o = kdf["open"].values
    v = kdf["vol"].values; n = len(c)
    print(f"  获取{n}根K线: {kdf['trade_date'].iloc[0]} ~ {kdf['trade_date'].iloc[-1]}")
    
    # 均线
    ma5 = pd.Series(c).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(c).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(c).rolling(20).mean().iloc[-1]
    ma60 = pd.Series(c).rolling(60).mean().iloc[-1] if n >= 60 else None
    
    # MACD
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean()
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean()
    dif_series = ema12 - ema26
    dea_series = dif_series.ewm(span=9, adjust=False).mean()
    macd_bar = (dif_series - dea_series) * 2
    dif = dif_series.iloc[-1]; dea = dea_series.iloc[-1]; macd = macd_bar.iloc[-1]
    
    # RSI14
    delta = pd.Series(c).diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi14 = (100 - 100 / (1 + gain / loss)).iloc[-1]
    
    # BIAS
    bias20 = (c[-1] - ma20) / ma20 * 100
    bias5 = (c[-1] - ma5) / ma5 * 100
    
    # 近期涨幅
    r1 = kdf["pct_chg"].iloc[-1]
    r3 = (c[-1] / c[-4] - 1) * 100 if n >= 4 else 0
    r5 = (c[-1] / c[-6] - 1) * 100 if n >= 6 else 0
    r10 = (c[-1] / c[-11] - 1) * 100 if n >= 11 else 0
    r20 = (c[-1] / c[-21] - 1) * 100 if n >= 21 else 0
    
    # 量能分析
    vol5 = np.mean(v[-5:])
    vol20 = np.mean(v[-20:]) if n >= 20 else vol5
    vol_ratio = vol5 / vol20 if vol20 > 0 else 1
    
    # 输出K线数据
    print(f"\n  === 技术指标 ===")
    print(f"  收盘: {c[-1]:.2f}  今涨: {r1:+.2f}%")
    print(f"  MA5={ma5:.2f}  MA10={ma10:.2f}  MA20={ma20:.2f}  MA60={ma60:.2f}" if ma60 else
          f"  MA5={ma5:.2f}  MA10={ma10:.2f}  MA20={ma20:.2f}")
    print(f"  DIF={dif:.3f}  DEA={dea:.3f}  MACD={macd:.3f}")
    print(f"  RSI14={rsi14:.1f}  BIAS5={bias5:.1f}%  BIAS20={bias20:.1f}%")
    print(f"  近3日: {r3:+.1f}%  近5日: {r5:+.1f}%  近10日: {r10:+.1f}%  近20日: {r20:+.1f}%")
    print(f"  5日均量/20日均量: {vol_ratio:.2f}")
    
    # 均线排列
    if c[-1] > ma5 > ma10 > ma20:
        ma_status = "多头排列(完美)"
    elif c[-1] > ma5 and ma5 > ma10:
        ma_status = "多头排列(短中)"
    elif c[-1] < ma5 < ma10 < ma20:
        ma_status = "空头排列"
    else:
        ma_status = "缠绕/震荡"
    print(f"  均线状态: {ma_status}")
    
    # 波峰波谷
    peaks, troughs = [], []
    for i in range(3, n-3):
        if h[i] > max(h[max(0,i-3):i]) and h[i] > max(h[i+1:min(n,i+4)]):
            peaks.append((kdf["trade_date"].iloc[i], h[i]))
        if l[i] < min(l[max(0,i-3):i]) and l[i] < min(l[i+1:min(n,i+4)]):
            troughs.append((kdf["trade_date"].iloc[i], l[i]))
    
    print(f"\n  === 波峰波谷(TDS) ===")
    if peaks: print(f"  近期波峰: {peaks[-3:]}")
    if troughs: print(f"  近期波谷: {troughs[-3:]}")
    
    # K线组合判定
    print(f"\n  === 近5日K线 ===")
    for _, row in kdf.tail(5).iterrows():
        body = "阳" if row["close"] >= row["open"] else "阴"
        body_pct = abs(row["close"] - row["open"]) / row["pre_close"] * 100
        shadow_up = (row["high"] - max(row["close"], row["open"])) / row["pre_close"] * 100
        shadow_dn = (min(row["close"], row["open"]) - row["low"]) / row["pre_close"] * 100
        print(f"  {row['trade_date']}: O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} C={row['close']:.2f} "
              f"{row['pct_chg']:+.2f}% V={row['vol']:.0f} [{body}线 实体{body_pct:.1f}% 上影{shadow_up:.1f}% 下影{shadow_dn:.1f}%]")
    
    # 近3日K线组合
    if n >= 3:
        k1, k2, k3 = kdf.iloc[-3], kdf.iloc[-2], kdf.iloc[-1]
        combos = []
        # 多头推进
        if l[-1] > l[-2] and h[-1] > h[-2]:
            combos.append("多头推进")
        if l[-2] > l[-3] and h[-2] > h[-3]:
            combos.append("前日多头推进")
        # 空头推进
        if h[-1] < h[-2] and l[-1] < l[-2]:
            combos.append("空头推进")
        # 阳线吞没
        if c[-1] > o[-1] and c[-1] > h[-2]:
            combos.append("阳线吞没")
        # 阴线吞没
        if c[-1] < o[-1] and c[-1] < l[-2]:
            combos.append("阴线吞没")
        # 收缩K线
        if h[-1] < h[-2] and l[-1] > l[-2]:
            combos.append("收缩K线")
        # 扩张K线
        if h[-1] > h[-2] and l[-1] < l[-2]:
            combos.append("扩张K线")
        print(f"\n  K线组合: {combos if combos else '无特殊形态'}")
    
    # 支撑压力位
    high_20 = max(h[-20:]); low_20 = min(l[-20:])
    high_10 = max(h[-10:]); low_10 = min(l[-10:])
    print(f"\n  === 支撑压力 ===")
    print(f"  20日高低: {high_20:.2f}/{low_20:.2f}  10日高低: {high_10:.2f}/{low_10:.2f}")
    if peaks: print(f"  最近波峰压力: {peaks[-1][1]:.2f}")
    if troughs: print(f"  最近波谷支撑: {troughs[-1][1]:.2f}")

# ==================== 3. 基本面 ====================
print("\n[3/7] 获取基本面...")
time.sleep(1)
bas = ts("daily_basic", {"ts_code": CODE, "trade_date": "20260409"},
         "ts_code,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate_f,volume_ratio")
if not bas.empty:
    pe = bas["pe_ttm"].iloc[0]; pb = bas["pb"].iloc[0]
    mv = bas["total_mv"].iloc[0] / 10000; cmv = bas["circ_mv"].iloc[0] / 10000
    tr = bas["turnover_rate_f"].iloc[0]; vr = bas["volume_ratio"].iloc[0]
    print(f"  PE(TTM)={pe:.1f}  PB={pb:.2f}  PS(TTM)={bas['ps_ttm'].iloc[0]:.1f}")
    print(f"  总市值: {mv:.0f}亿  流通市值: {cmv:.0f}亿")
    print(f"  换手率: {tr:.2f}%  量比: {vr:.2f}")

# ==================== 4. 资金流向 ====================
print("\n[4/7] 获取资金流向...")
time.sleep(1)
mf = ts("moneyflow", {"ts_code": CODE, "trade_date": "20260409"},
        "ts_code,trade_date,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,buy_md_amount,sell_md_amount,buy_sm_amount,sell_sm_amount,net_mf_amount")
if not mf.empty:
    net_elg = mf["buy_elg_amount"].iloc[0] - mf["sell_elg_amount"].iloc[0]
    net_lg = mf["buy_lg_amount"].iloc[0] - mf["sell_lg_amount"].iloc[0]
    net_md = mf["buy_md_amount"].iloc[0] - mf["sell_md_amount"].iloc[0]
    net_sm = mf["buy_sm_amount"].iloc[0] - mf["sell_sm_amount"].iloc[0]
    net_big = net_elg + net_lg
    print(f"  超大单净额: {net_elg:.0f}万  大单净额: {net_lg:.0f}万")
    print(f"  中单净额: {net_md:.0f}万  小单净额: {net_sm:.0f}万")
    print(f"  主力(超大+大)净流入: {net_big:.0f}万 ({net_big/10000:.2f}亿)")
    print(f"  总净额: {mf['net_mf_amount'].iloc[0]:.0f}万")

# ==================== 5. 近5日资金流向趋势 ====================
print("\n[5/7] 获取近5日资金流向趋势...")
time.sleep(1)
mf5 = ts("moneyflow", {"ts_code": CODE, "start_date": "20260401", "end_date": "20260409"},
         "ts_code,trade_date,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,net_mf_amount")
if not mf5.empty:
    mf5.sort_values("trade_date", inplace=True)
    for _, row in mf5.iterrows():
        nb = (row["buy_elg_amount"] + row["buy_lg_amount"] - row["sell_elg_amount"] - row["sell_lg_amount"])
        print(f"  {row['trade_date']}: 主力净{nb:+.0f}万 总净{row['net_mf_amount']:+.0f}万")

# ==================== 6. 市场情绪数据(4/9) ====================
print("\n[6/7] 获取4/9市场情绪数据...")
time.sleep(1)
# 涨跌家数
daily_all = ts("daily", {"trade_date": "20260409"}, "ts_code,pct_chg")
if not daily_all.empty:
    up = len(daily_all[daily_all["pct_chg"] > 0])
    down = len(daily_all[daily_all["pct_chg"] < 0])
    flat = len(daily_all[daily_all["pct_chg"] == 0])
    limit_up = len(daily_all[daily_all["pct_chg"] >= 9.5])
    limit_dn = len(daily_all[daily_all["pct_chg"] <= -9.5])
    total = len(daily_all)
    print(f"  涨: {up}家({up/total*100:.0f}%)  跌: {down}家({down/total*100:.0f}%)  平: {flat}家")
    print(f"  涨停: {limit_up}家  跌停: {limit_dn}家")
    print(f"  赚钱效应: {up/total*100:.0f}%")

# ==================== 7. 公司基本信息 ====================
print("\n[7/7] 获取公司信息...")
time.sleep(1)
info = ts("stock_basic", {"ts_code": CODE}, "ts_code,name,industry,area,market,list_date")
if not info.empty:
    print(f"  名称: {info['name'].iloc[0]}  行业: {info['industry'].iloc[0]}")
    print(f"  地区: {info['area'].iloc[0]}  市场: {info['market'].iloc[0]}  上市: {info['list_date'].iloc[0]}")

# 获取行业涨幅
time.sleep(1)
industry = info["industry"].iloc[0] if not info.empty else ""
if industry:
    ind_stocks = ts("stock_basic", {"industry": industry, "list_status": "L"}, "ts_code,name")
    if not ind_stocks.empty:
        print(f"\n  同行业({industry})公司数: {len(ind_stocks)}")

print("\n" + "="*80)
print("数据采集完成!")
