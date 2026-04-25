#!/usr/bin/env python3
"""
全战法全维度完整分析 — 覆盖所有9大维度+所有子战法
"""
import pandas as pd
import numpy as np
import sys

df = pd.read_csv('backtest_results/backtest_v2_detail_20260419_085002.csv')
df = df[df['ret_1d'].notna()].copy()

# === 主板筛选：只保留00和60开头的股票（排除创业板30x、科创板68x、北交所） ===
MAIN_BOARD_ONLY = True  # 设为False则分析全部
if MAIN_BOARD_ONLY:
    total_before = len(df)
    main_mask = df['code'].str[:3].isin(['600','601','603','605','000','001','002','003'])
    df = df[main_mask].copy()
    print(f"🔹 主板筛选：{total_before} → {len(df)}条（排除创业板/科创板 {total_before-len(df)}条）")

print(f"总样本: {len(df)}条, 交易日: {df['date'].nunique()}天")
print(f"回测区间: {df['date'].min()} ~ {df['date'].max()}")

def stats(sub):
    if len(sub) < 3: return None
    rets = sub['ret_1d']
    wins = rets[rets > 0]; losses = rets[rets <= 0]
    avg_w = wins.mean() if len(wins) > 0 else 0
    avg_l = abs(losses.mean()) if len(losses) > 0 else 0.01
    pr = avg_w / avg_l if avg_l > 0 else 99
    daily = sub.groupby('date')['ret_1d'].mean().sort_index()
    cum = 1.0; peak = 1.0; mdd = 0
    for r in daily:
        cum *= (1 + r/100)
        if cum > peak: peak = cum
        dd = (peak - cum)/peak*100
        if dd > mdd: mdd = dd
    # 多持有期
    r2 = sub['ret_2d'].mean() if 'ret_2d' in sub.columns and sub['ret_2d'].notna().sum() > 3 else None
    r3 = sub['ret_3d'].mean() if 'ret_3d' in sub.columns and sub['ret_3d'].notna().sum() > 3 else None
    r5 = sub['ret_5d'].mean() if 'ret_5d' in sub.columns and sub['ret_5d'].notna().sum() > 3 else None
    return {'n': len(sub), 'avg': rets.mean(), 'med': rets.median(), 'wr': (rets>0).mean()*100, 
            'pr': pr, 'cum': (cum-1)*100, 'mdd': mdd, 'r2': r2, 'r3': r3, 'r5': r5}

def prt(name, r):
    if r is None: return
    tag = '🏆' if r['cum']>50 else ('✅' if r['cum']>0 else '❌')
    r2s = f"{r['r2']:>+6.2f}%" if r['r2'] is not None else "   N/A"
    r3s = f"{r['r3']:>+6.2f}%" if r['r3'] is not None else "   N/A"
    r5s = f"{r['r5']:>+6.2f}%" if r['r5'] is not None else "   N/A"
    print(f"{tag} {name:<40} {r['n']:>5} {r['avg']:>+7.2f}% {r['wr']:>6.1f}% {r['pr']:>6.2f} {r['cum']:>+9.1f}% {r['mdd']:>-7.1f}% | {r2s} {r3s} {r5s}")

hdr = f"{'战法/维度':<42} {'样本':>5} {'T+1均收':>8} {'胜率':>7} {'盈亏比':>7} {'累计收益':>10} {'回撤':>8} | {'T+2':>7} {'T+3':>7} {'T+5':>7}"
sep = "=" * 130

# ============================================================
print(f"\n{sep}")
print("📊 一、7大评分维度贡献度（高分 vs 低分）")
print(sep)
print(f"{'维度':<20} {'阈值':>6} | {'高分N':>6} {'高分收':>7} {'高分胜':>7} | {'低分N':>6} {'低分收':>7} {'低分胜':>7} | {'差值':>7} {'判定':>8}")
print("-" * 110)

dims = [('d1','多周期共振',15),('d2','主线热点',25),('d3','三Skill',47),('d4','安全边际',15),('d5','基本面',15),('d9','百胜WR',15)]  # d6(BJCJ)已移除
for key, name, mx in dims:
    th = mx * 0.6
    hi = df[df[key] >= th]; lo = df[df[key] < th]
    if len(hi)>5 and len(lo)>5:
        ah=hi['ret_1d'].mean(); al=lo['ret_1d'].mean(); wh=(hi['ret_1d']>0).mean()*100; wl=(lo['ret_1d']>0).mean()*100
        d=ah-al; v="✅正贡献" if d>0.3 else ("⚠️弱" if d>-0.3 else "❌负贡献")
        print(f"  {name:<18} ≥{th:>4.0f} | {len(hi):>5} {ah:>+6.2f}% {wh:>6.1f}% | {len(lo):>5} {al:>+6.2f}% {wl:>6.1f}% | {d:>+6.2f}% {v}")

# ============================================================
print(f"\n{sep}")
print("📊 二、8大子维度贡献度")
print(sep)
print(f"{'子维度':<20} {'阈值':>6} | {'高分N':>6} {'高分收':>7} {'高分胜':>7} | {'低分N':>6} {'低分收':>7} {'低分胜':>7} | {'差值':>7} {'判定':>8}")
print("-" * 110)

sub_dims = [('mistery','Mistery',20),('tds','TDS',12),('yuanzi','元子元',10),('txcg','TXCG六模型',5),('wr1','WR-1首板',7),('wr2','WR-2起爆',5),('wr3','WR-3底倍量',4),('bci','BCI板块',100)]
for key, name, mx in sub_dims:
    th = mx * 0.6
    hi = df[df[key] >= th]; lo = df[df[key] < th]
    if len(hi)>5 and len(lo)>5:
        ah=hi['ret_1d'].mean(); al=lo['ret_1d'].mean(); wh=(hi['ret_1d']>0).mean()*100; wl=(lo['ret_1d']>0).mean()*100
        d=ah-al; v="✅正贡献" if d>0.3 else ("⚠️弱" if d>-0.3 else "❌负贡献")
        print(f"  {name:<18} ≥{th:>4.0f} | {len(hi):>5} {ah:>+6.2f}% {wh:>6.1f}% | {len(lo):>5} {al:>+6.2f}% {wl:>6.1f}% | {d:>+6.2f}% {v}")

# ============================================================
print(f"\n{sep}")
print("📊 三、全部战法独立表现（含多持有期）")
print(sep)
print(hdr)
print("-" * 130)

prt('📌 全部样本（基准）', stats(df))
print("-" * 130)

# === BJCJ系列 ===
print("【BJCJ系列 — 5大战法】")
for tag, name in [('BJCJ-1','BJCJ-1 首板打板'),('BJCJ-2','BJCJ-2 高效低吸'),('BJCJ-3','BJCJ-3 情绪仓位'),('BJCJ-5','BJCJ-5 板块共振'),('BJCJ-6','BJCJ-6 做T适配')]:
    sub = df[df['bjcj_tags'].str.contains(tag, na=False)]
    prt(f'  {name}', stats(sub))
print("-" * 130)

# === WR系列 ===
print("【百胜WR系列 — 3大战法】")
for col, name, thresholds in [
    ('wr1','WR-1 首板放量',[(3,'≥3'),(5,'≥5推荐'),(6,'≥6高分'),(7,'=7满分')]),
    ('wr2','WR-2 右侧起爆',[(2,'≥2'),(3,'≥3'),(4,'≥4推荐'),(5,'=5满分')]),
    ('wr3','WR-3 底倍量柱',[(1,'≥1'),(2,'≥2'),(3,'≥3推荐'),(4,'=4满分')]),
]:
    for th, label in thresholds:
        sub = df[df[col] >= th]
        prt(f'  {name} {label}', stats(sub))
    print()
print("-" * 130)

# === Mistery ===
print("【Mistery — 技术趋势综合】")
for th, label in [(5,'≥5'),(8,'≥8'),(10,'≥10'),(12,'≥12高分'),(15,'≥15'),(18,'≥18极高')]:
    sub = df[df['mistery'] >= th]
    prt(f'  Mistery {label}', stats(sub))
print("-" * 130)

# === TDS ===
print("【TDS — 疯极派趋势信号】")
for th, label in [(2,'≥2'),(3,'≥3'),(4,'≥4'),(5,'≥5高分'),(6,'≥6'),(8,'=8极高')]:
    sub = df[df['tds'] >= th]
    prt(f'  TDS {label}', stats(sub))
print("-" * 130)

# === 元子元 ===
print("【元子元 — 情绪周期判定】")
for th, label in [(2,'≥2'),(4,'≥4'),(5,'≥5'),(6,'≥6高分'),(8,'≥8极高')]:
    sub = df[df['yuanzi'] >= th]
    prt(f'  元子元 {label}', stats(sub))
print("-" * 130)

# === TXCG ===
print("【TXCG六大模型 — 天时地利人和】")
for th, label in [(1,'≥1'),(2,'≥2'),(3,'≥3高分'),(4,'=4极高')]:
    sub = df[df['txcg'] >= th]
    prt(f'  TXCG {label}', stats(sub))
print("-" * 130)

# === BCI ===
print("【BCI板块完整性指数】")
for th, label in [(30,'≥30'),(50,'≥50'),(60,'≥60'),(70,'≥70'),(80,'≥80'),(90,'≥90')]:
    sub = df[df['bci'] >= th]
    prt(f'  BCI {label}', stats(sub))
prt('  BCI <30（板块不完整）', stats(df[df['bci'] < 30]))
print("-" * 130)

# === D1多周期 ===
print("【D1多周期共振】")
for th, label in [(12,'≥12'),(13,'≥13'),(14,'≥14'),(15,'=15满分')]:
    sub = df[df['d1'] >= th]
    prt(f'  多周期 {label}', stats(sub))
print("-" * 130)

# === D2主线热点 ===
print("【D2主线热点】")
for th, label in [(10,'≥10'),(15,'≥15'),(20,'≥20高分'),(25,'=25满分')]:
    sub = df[df['d2'] >= th]
    prt(f'  主线热点 {label}', stats(sub))
print("-" * 130)

# === D4安全边际 ===
print("【D4安全边际】")
for th, label in [(5,'≥5'),(8,'≥8'),(10,'≥10高分'),(12,'≥12'),(15,'=15满分')]:
    sub = df[df['d4'] >= th]
    prt(f'  安全边际 {label}', stats(sub))
print("-" * 130)

# === D5基本面 ===
print("【D5基本面】")
for th, label in [(5,'≥5'),(8,'≥8'),(10,'≥10高分'),(12,'≥12')]:
    sub = df[df['d5'] >= th]
    prt(f'  基本面 {label}', stats(sub))
print("-" * 130)

# === 风险/保护 ===
print("【风险扣分 & 保护因子】")
prt('  风险=0（无风险）', stats(df[df['risk'] == 0]))
prt('  风险>0（有风险）', stats(df[df['risk'] > 0]))
prt('  风险≥5（高风险）', stats(df[df['risk'] >= 5]))
prt('  保护≥10（高保护）', stats(df[df['protect'] >= 10]))
prt('  保护<5（低保护）', stats(df[df['protect'] < 5]))
prt('  净风险=0', stats(df[df['net_risk'] == 0]))
prt('  净风险>0', stats(df[df['net_risk'] > 0]))
print("-" * 130)

# === 涨停/非涨停 ===
print("【涨停 vs 非涨停】")
prt('  涨停票', stats(df[df['is_zt'] == True]))
prt('  非涨停票', stats(df[df['is_zt'] == False]))
print("-" * 130)

# === 市值 ===
print("【市值分层】")
prt('  小盘(30-100亿)', stats(df[(df['mv']>=30)&(df['mv']<100)]))
prt('  中盘(100-300亿)', stats(df[(df['mv']>=100)&(df['mv']<300)]))
prt('  中大盘(300-1000亿)', stats(df[(df['mv']>=300)&(df['mv']<1000)]))
prt('  大盘(>1000亿)', stats(df[df['mv']>=1000]))
print("-" * 130)

# === 换手率 ===
print("【换手率分层】")
prt('  低换手(<5%)', stats(df[df['tr']<5]))
prt('  中换手(5-15%)', stats(df[(df['tr']>=5)&(df['tr']<15)]))
prt('  高换手(15-30%)', stats(df[(df['tr']>=15)&(df['tr']<30)]))
prt('  极高换手(>30%)', stats(df[df['tr']>=30]))
print("-" * 130)

# === 资金 ===
print("【主力资金流向】")
prt('  净流入(>0亿)', stats(df[df['nb_yi']>0]))
prt('  净流入(>2亿)', stats(df[df['nb_yi']>2]))
prt('  净流出(<0亿)', stats(df[df['nb_yi']<0]))
prt('  大幅净流出(<-3亿)', stats(df[df['nb_yi']<-3]))
print("-" * 130)

# === 涨幅分层 ===
print("【近期涨幅分层】")
prt('  5日涨幅<5%', stats(df[df['r5']<5]))
prt('  5日涨幅5-10%', stats(df[(df['r5']>=5)&(df['r5']<10)]))
prt('  5日涨幅10-20%', stats(df[(df['r5']>=10)&(df['r5']<20)]))
prt('  5日涨幅>20%', stats(df[df['r5']>=20]))
print("-" * 130)

# === 评分分层 ===
print("【综合评分分层】")
prt('  评分≥110', stats(df[df['total']>=110]))
prt('  评分100-109', stats(df[(df['total']>=100)&(df['total']<110)]))
prt('  评分90-99', stats(df[(df['total']>=90)&(df['total']<100)]))
prt('  评分80-89', stats(df[(df['total']>=80)&(df['total']<90)]))
prt('  评分75-79', stats(df[(df['total']>=75)&(df['total']<80)]))
prt('  评分<75', stats(df[df['total']<75]))

# ============================================================
print(f"\n{sep}")
print("📊 四、战法组合效果排名TOP20")
print(sep)
print(hdr)
print("-" * 130)

combos = [
    ('WR2≥4+非涨停', df[(df['wr2']>=4)&(df['is_zt']==False)]),
    ('WR2≥3+非涨停', df[(df['wr2']>=3)&(df['is_zt']==False)]),
    ('WR3≥3+非涨停', df[(df['wr3']>=3)&(df['is_zt']==False)]),
    ('WR3≥4+非涨停', df[(df['wr3']>=4)&(df['is_zt']==False)]),
    ('WR3≥4+BCI≥70', df[(df['wr3']>=4)&(df['bci']>=70)]),
    ('WR3≥3+BCI≥70+非涨停', df[(df['wr3']>=3)&(df['bci']>=70)&(df['is_zt']==False)]),
    ('WR2≥4+WR3≥3', df[(df['wr2']>=4)&(df['wr3']>=3)]),
    ('Mistery≥12+非涨停', df[(df['mistery']>=12)&(df['is_zt']==False)]),
    ('Mistery≥12+WR2≥3', df[(df['mistery']>=12)&(df['wr2']>=3)]),
    ('TDS≥5+非涨停', df[(df['tds']>=5)&(df['is_zt']==False)]),
    ('TDS≥5+WR2≥3', df[(df['tds']>=5)&(df['wr2']>=3)]),
    ('元子元≥6+非涨停', df[(df['yuanzi']>=6)&(df['is_zt']==False)]),
    ('元子元≥6+WR2≥3', df[(df['yuanzi']>=6)&(df['wr2']>=3)]),
    ('BJCJ-2+非涨停', df[df['bjcj_tags'].str.contains('BJCJ-2',na=False)&(df['is_zt']==False)]),
    ('BJCJ-6+WR2≥3', df[df['bjcj_tags'].str.contains('BJCJ-6',na=False)&(df['wr2']>=3)]),
    ('BJCJ-6+WR3≥3', df[df['bjcj_tags'].str.contains('BJCJ-6',na=False)&(df['wr3']>=3)]),
    ('非涨停+BCI≥70+评分≥85', df[(df['is_zt']==False)&(df['bci']>=70)&(df['total']>=85)]),
    ('非涨停+安全边际≥10', df[(df['is_zt']==False)&(df['d4']>=10)]),
    ('非涨停+基本面≥10', df[(df['is_zt']==False)&(df['d5']>=10)]),
    ('非涨停+Mistery≥12+TDS≥4', df[(df['is_zt']==False)&(df['mistery']>=12)&(df['tds']>=4)]),
    ('非涨停+元子元≥5+WR2≥3', df[(df['is_zt']==False)&(df['yuanzi']>=5)&(df['wr2']>=3)]),
    ('5日涨<10%+非涨停+BCI≥60', df[(df['r5']<10)&(df['is_zt']==False)&(df['bci']>=60)]),
    ('PE<50+非涨停+WR2≥3', df[(df['pe']>0)&(df['pe']<50)&(df['is_zt']==False)&(df['wr2']>=3)]),
    ('小盘+WR2≥4', df[(df['mv']<100)&(df['wr2']>=4)]),
    ('中盘+WR2≥4', df[(df['mv']>=100)&(df['mv']<300)&(df['wr2']>=4)]),
    ('净流入+非涨停+WR2≥3', df[(df['nb_yi']>0)&(df['is_zt']==False)&(df['wr2']>=3)]),
    ('TXCG≥2+非涨停', df[(df['txcg']>=2)&(df['is_zt']==False)]),
    ('保护≥10+非涨停', df[(df['protect']>=10)&(df['is_zt']==False)]),
    ('D2≥20+非涨停', df[(df['d2']>=20)&(df['is_zt']==False)]),
    ('D1=15+WR2≥3', df[(df['d1']==15)&(df['wr2']>=3)]),
]

results = []
for name, sub in combos:
    r = stats(sub)
    if r: results.append((name, r))

results.sort(key=lambda x: x[1]['cum'], reverse=True)
for name, r in results[:25]:
    prt(name, r)

print(f"\n✅ 分析完成！共覆盖 6大维度(BJCJ已移除) + 8子维度 + 5个BJCJ战法 + 3个WR战法 + Mistery/TDS/元子元/TXCG/BCI + 30种组合")
