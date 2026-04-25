#!/usr/bin/env python3
"""快速检查Tushare最新可用数据日期"""
import requests, pandas as pd

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts(api, params=None, fields=None):
    d = {"api_name":api, "token":TOKEN, "params":params or {}}
    if fields: d["fields"] = fields
    r = requests.post("http://api.tushare.pro", json=d, timeout=30)
    j = r.json()
    if j.get("code") != 0:
        return None, j.get("msg","")
    return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"]), ""

# 检查交易日历
cal, _ = ts("trade_cal", {"exchange":"SSE","is_open":"1","start_date":"20260401","end_date":"20260410"})
print("4月交易日:")
print(cal.sort_values("cal_date", ascending=False).to_string())

# 逐日检查哪天有数据
for d in ["20260410","20260409","20260408","20260407","20260403","20260402","20260401"]:
    df, msg = ts("daily", {"trade_date": d, "ts_code": "000001.SZ"}, "ts_code,trade_date,close,pct_chg")
    if df is not None and not df.empty:
        print(f"\n{d}: 有数据 -> {df.iloc[0].to_dict()}")
    else:
        print(f"\n{d}: 无数据 ({msg})")
