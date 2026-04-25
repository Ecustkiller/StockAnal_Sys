#!/usr/bin/env python3
"""
历史选股计划最新数据复盘脚本
================================
读取本地 daily_snapshot parquet，对所有已推荐股票从推荐日至最新交易日（4/20）
计算真实表现（涨跌幅、是否跌破止损、是否达到目标等），并按计划分组汇总。
"""
import os, glob
import pandas as pd
import numpy as np
from collections import defaultdict

SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")

# 可用的交易日快照
AVAILABLE_DATES = sorted([os.path.basename(f).replace('.parquet','')
                          for f in glob.glob(os.path.join(SNAPSHOT_DIR, '2*.parquet'))])
LATEST_DATE = AVAILABLE_DATES[-1]  # 20260420
print(f"✅ 本地快照 {len(AVAILABLE_DATES)} 天, 最新={LATEST_DATE}")

# 加载需要的交易日快照
def load_snap(date):
    p = os.path.join(SNAPSHOT_DIR, f"{date}.parquet")
    if not os.path.exists(p): return None
    df = pd.read_parquet(p)
    return df

SNAPS = {}
for d in ['20260410','20260413','20260414','20260415','20260416','20260417','20260420']:
    if d in AVAILABLE_DATES:
        SNAPS[d] = load_snap(d)
print(f"✅ 加载快照: {list(SNAPS.keys())}")

# 快照字段名探测
sample = SNAPS[LATEST_DATE]
print(f"快照列: {list(sample.columns)[:12]}")
print(f"样本行数: {len(sample)}")

# =========================================
# 所有历史选股计划推荐股票池（code, name, plan_date, ref_price, plan_name, priority）
# =========================================
PICKS = [
    # ===== 4/10 TOP5操作建议（基于4/9收盘，4/10盘前） =====
    ('002938', '鹏鼎控股', '20260409', None, '0410_TOP5', 'A'),
    ('002475', '立讯精密', '20260409', None, '0410_TOP5', 'A'),
    ('688019', '安集科技', '20260409', None, '0410_TOP5', 'C'),
    ('301571', '国科天成', '20260409', None, '0410_TOP5', 'A'),
    ('688120', '华海清科', '20260409', None, '0410_TOP5', 'C'),

    # ===== 4/11 作战计划（基于4/10收盘，4/11执行） =====
    ('002580', '圣阳股份', '20260410', None, '0411计划', 'S'),
    ('000889', '中嘉博创', '20260410', None, '0411计划', 'S'),
    ('002824', '和胜股份', '20260410', None, '0411计划', 'A'),
    ('002263', '大东南',   '20260410', None, '0411计划', 'B'),
    ('605117', '德业股份', '20260410', None, '0411计划', 'C'),

    # ===== 4/14 明日选股池（基于4/13） =====
    ('002580', '圣阳股份', '20260413', None, '0414池', 'S'),
    ('002418', '康盛股份', '20260413', None, '0414池', 'A'),
    ('603950', '长源东谷', '20260413', None, '0414池', 'B'),

    # ===== 4/16 最终执行版（基于4/15，4/16执行） =====
    ('002294', '信立泰',   '20260415', None, '0416计划', 'S'),
    ('002788', '鹭燕医药', '20260415', None, '0416计划', 'S'),
    ('600267', '海正药业', '20260415', None, '0416计划', 'S'),
    ('000963', '华东医药', '20260415', None, '0416计划', 'S'),
    ('600572', '康恩贝',   '20260415', None, '0416计划', 'A'),
    ('600713', '南京医药', '20260415', None, '0416计划', 'A'),
    ('002589', '瑞康医药', '20260415', None, '0416计划', 'A'),
    ('603538', '美诺华',   '20260415', None, '0416计划', 'C'),
    ('002229', '鸿博股份', '20260415', None, '0416计划', 'A'),
    ('001208', '华菱线缆', '20260415', None, '0416计划', 'B'),
    ('601179', '中国西电', '20260415', None, '0416计划', 'B'),
    ('603127', '昭衍新药', '20260415', None, '0416计划', 'C'),

    # ===== 4/17及下周选股 v3.2（基于4/16，4/17执行） =====
    ('603920', '世运电路', '20260416', None, '0417计划', 'S'),
    ('600875', '东方电气', '20260416', None, '0417计划', 'S'),
    ('002421', '达实智能', '20260416', None, '0417计划', 'S'),
    ('600666', '奥瑞德',   '20260416', None, '0417计划', 'S'),
    ('002850', '科达利',   '20260416', None, '0417计划', 'A'),
    ('001230', '劲旅环境', '20260416', None, '0417计划', 'A'),
    ('300352', '北信源',   '20260416', None, '0417计划', 'A'),
    ('603906', '龙蟠科技', '20260416', None, '0417计划', 'A'),
    ('000070', '特发信息', '20260416', None, '0417计划', 'A'),
    ('002859', '洁美科技', '20260416', None, '0417计划', 'A'),
    ('300290', '荣科科技', '20260416', None, '0417计划', 'A'),
    ('002771', '真视通',   '20260416', None, '0417计划', 'S'),
    ('301123', '奕东电子', '20260416', None, '0417计划', 'A'),
    ('000815', '美利云',   '20260416', None, '0417计划', 'A'),
    ('300088', '长信科技', '20260416', None, '0417计划', 'A'),
    ('603052', '可川科技', '20260416', None, '0417计划', 'A'),
    # 主板补充
    ('002595', '豪迈科技', '20260416', None, '0417计划_主板', 'S'),
    ('001268', '联合精密', '20260416', None, '0417计划_主板', 'S'),
    ('002491', '通鼎互联', '20260416', None, '0417计划_主板', 'A'),
    ('000657', '中钨高新', '20260416', None, '0417计划_主板', 'A'),
    ('002613', '北玻股份', '20260416', None, '0417计划_主板', 'B'),
    ('002757', '南兴股份', '20260416', None, '0417计划_主板', 'B'),
    ('002334', '英威腾',   '20260416', None, '0417计划_主板', 'C'),
    ('002368', '太极股份', '20260416', None, '0417计划_主板', 'C'),
]

# =========================================
# 快照列名自动探测
# =========================================
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

# 探测字段
CODE_COL = pick_col(sample, ['ts_code', 'code', 'symbol'])
CLOSE_COL = pick_col(sample, ['close', 'CLOSE', 'Close'])
OPEN_COL = pick_col(sample, ['open', 'OPEN', 'Open'])
HIGH_COL = pick_col(sample, ['high', 'HIGH', 'High'])
LOW_COL = pick_col(sample, ['low', 'LOW', 'Low'])
print(f"字段: code={CODE_COL}, close={CLOSE_COL}, open={OPEN_COL}, high={HIGH_COL}, low={LOW_COL}")

def normalize_code(code):
    """000001 -> 000001.SZ / 600000.SH / 688001.SH / 300001.SZ / 301xxx.SZ"""
    if len(code) != 6: return code
    if code.startswith(('60', '68')): return f"{code}.SH"
    return f"{code}.SZ"

def get_price(date, code, field='close'):
    snap = SNAPS.get(date)
    if snap is None: return None
    col = {'close': CLOSE_COL, 'open': OPEN_COL, 'high': HIGH_COL, 'low': LOW_COL}[field]
    if col is None: return None
    full_code = normalize_code(code)
    row = snap[snap[CODE_COL] == full_code]
    if len(row) == 0:
        # 尝试不带后缀
        row = snap[snap[CODE_COL] == code]
    if len(row) == 0: return None
    return float(row[col].iloc[0])

# =========================================
# 计算每只股票表现
# =========================================
results = []
for code, name, plan_date, ref_price, plan_name, prio in PICKS:
    # 选股日收盘价（作为参考基准）
    base_price = get_price(plan_date, code, 'close')
    if base_price is None:
        results.append({
            'plan': plan_name, 'code': code, 'name': name, 'prio': prio,
            'plan_date': plan_date, 'base_price': None, 'latest': None,
            'total_pct': None, 't1_pct': None, 'max_gain': None, 'max_drawdown': None,
            'note': '❌数据缺失'
        })
        continue

    # 最新收盘价（4/20）
    latest = get_price(LATEST_DATE, code, 'close')

    # 推荐日的次日（T+1）表现，找到plan_date之后的第一个交易日
    next_dates = [d for d in AVAILABLE_DATES if d > plan_date]
    t1_date = next_dates[0] if next_dates else None
    t1_close = get_price(t1_date, code, 'close') if t1_date else None
    t1_pct = (t1_close/base_price - 1)*100 if t1_close else None

    # 总涨跌幅
    total_pct = (latest/base_price - 1)*100 if latest else None

    # 最大涨幅/回撤：从推荐日往后每天的高低
    max_gain = -999
    max_drawdown = 999
    for d in next_dates:
        h = get_price(d, code, 'high')
        l = get_price(d, code, 'low')
        if h: max_gain = max(max_gain, (h/base_price - 1)*100)
        if l: max_drawdown = min(max_drawdown, (l/base_price - 1)*100)

    results.append({
        'plan': plan_name, 'code': code, 'name': name, 'prio': prio,
        'plan_date': plan_date, 'base_price': base_price, 'latest': latest,
        't1_date': t1_date, 't1_pct': t1_pct,
        'total_pct': total_pct,
        'max_gain': max_gain if max_gain > -999 else None,
        'max_drawdown': max_drawdown if max_drawdown < 999 else None,
        'note': ''
    })

df = pd.DataFrame(results)

# =========================================
# 分计划汇总
# =========================================
print("\n" + "="*100)
print(f"📊 历史选股计划最新数据复盘（最新交易日：{LATEST_DATE}）")
print("="*100)

# 每只股票明细
print("\n## 全部标的明细")
print(f"{'计划':<16}{'代码':<8}{'名称':<10}{'优':<3}{'基准日':<10}{'基准价':>8}{'最新价':>8}{'T+1%':>8}{'总涨跌%':>8}{'最大涨%':>8}{'最大跌%':>8}")
print('-'*110)
for _, r in df.iterrows():
    bp = f"{r['base_price']:.2f}" if r['base_price'] else '-'
    lp = f"{r['latest']:.2f}" if r['latest'] else '-'
    t1 = f"{r['t1_pct']:+.2f}" if r['t1_pct'] is not None else '-'
    tp = f"{r['total_pct']:+.2f}" if r['total_pct'] is not None else '-'
    mg = f"{r['max_gain']:+.1f}" if r['max_gain'] is not None else '-'
    md = f"{r['max_drawdown']:+.1f}" if r['max_drawdown'] is not None else '-'
    print(f"{r['plan']:<16}{r['code']:<8}{r['name']:<10}{r['prio']:<3}{r['plan_date']:<10}{bp:>8}{lp:>8}{t1:>8}{tp:>8}{mg:>8}{md:>8}")

# 分计划统计
print("\n" + "="*100)
print("## 分计划表现汇总")
print("="*100)
print(f"{'计划':<20}{'数量':>5}{'胜率':>8}{'均T+1%':>10}{'均总%':>10}{'最佳':>22}{'最差':>22}")
print('-'*100)
for plan in df['plan'].unique():
    sub = df[df['plan'] == plan]
    sub_valid = sub.dropna(subset=['total_pct'])
    n = len(sub_valid)
    if n == 0: continue
    win = len(sub_valid[sub_valid['total_pct'] > 0])
    win_rate = win/n*100
    avg_t1 = sub_valid['t1_pct'].mean() if sub_valid['t1_pct'].notna().any() else 0
    avg_total = sub_valid['total_pct'].mean()
    best = sub_valid.loc[sub_valid['total_pct'].idxmax()]
    worst = sub_valid.loc[sub_valid['total_pct'].idxmin()]
    best_s = f"{best['name']}{best['total_pct']:+.1f}%"
    worst_s = f"{worst['name']}{worst['total_pct']:+.1f}%"
    print(f"{plan:<20}{n:>5}{win_rate:>7.1f}%{avg_t1:>9.2f}%{avg_total:>9.2f}%{best_s:>22}{worst_s:>22}")

# 保存CSV
out_path = '/Users/ecustkiller/WorkBuddy/Claw/review_all_plans_latest.csv'
df.to_csv(out_path, index=False)
print(f"\n✅ 明细已保存: {out_path}")
