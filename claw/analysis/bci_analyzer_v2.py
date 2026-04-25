#!/usr/bin/env python3
"""
BCI板块完整性分析系统 v2.0
改进：概念归类、消息面驱动、斜率衰减、可参与性、封板时间分析
"""
import akshare as ak
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime
import time
import warnings
warnings.filterwarnings('ignore')

# ====== 概念映射表 ======
# 由于深夜API不稳定，手动建立核心概念映射
# 原则：把行业分类映射到更有意义的"题材概念"
CONCEPT_MAP = {
    # 通信+AI数据中心大方向
    '通信设备': 'AI数据中心(通信)',
    '通信服务': 'AI数据中心(通信)',
    '光学光电': 'AI数据中心(光模块)',
    '其他电源': 'AI数据中心(电力)',
    '半导体':   'AI数据中心(芯片)',
    # 新能源方向
    '电池':     '新能源(电池)',
    '光伏设备': '新能源(光伏)',
    '风电设备': '新能源(风电)',
    # 汽车方向
    '汽车零部': '汽车产业链',
    # 消费
    '一般零售': '消费(零售)',
    '休闲食品': '消费(食品)',
    '家居用品': '消费(家居)',
    '家电零部': '消费(家电)',
    '白色家电': '消费(家电)',
    '服装家纺': '消费(纺织)',
    '纺织制造': '消费(纺织)',
    '医药商业': '医药',
    '化学制药': '医药',
    '生物制品': '医药',
    '医疗器械': '医药',
    # 其他
    '其他电子': '电子制造',
    '消费电子': '电子制造',
    '元件':     '电子制造',
    '自动化设': '电子制造',
    '工业金属': '有色金属',
    '能源金属': '有色金属',
    '通用设备': '高端制造',
    '专用设备': '高端制造',
    '计算机设': '信息技术',
    '出版':     '传媒',
    '多元金融': '金融',
    '房地产开': '地产',
    '环境治理': '环保',
    '环保设备': '环保',
}

# 消息面驱动评分（手动标注当期热点）
NEWS_DRIVEN = {
    'AI数据中心(通信)': 3,   # 光纤涨价400%+AI数据中心需求爆发
    'AI数据中心(光模块)': 3,
    'AI数据中心(芯片)': 2,
    'AI数据中心(电力)': 2,   # 算力需要电力
    '新能源(电池)': 2,       # 固态电池+储能政策
    '新能源(光伏)': 1,
    '汽车产业链': 2,         # 新能源车+智能驾驶
    '有色金属': 1,           # 铝涨价
    '电子制造': 1,
}

def fetch_data(dates):
    """采集多日涨停+炸板数据"""
    all_zt = {}
    all_zb = {}
    for d in dates:
        try:
            zt = ak.stock_zt_pool_em(date=d)
            all_zt[d] = zt
            print(f"  {d} 涨停 {len(zt)}家")
        except:
            all_zt[d] = pd.DataFrame()
        time.sleep(0.3)
        try:
            zb = ak.stock_zt_pool_zbgc_em(date=d)
            all_zb[d] = zb
            print(f"  {d} 炸板 {len(zb)}家")
        except:
            all_zb[d] = pd.DataFrame()
        time.sleep(0.3)
    return all_zt, all_zb

def map_to_concept(industry):
    """行业映射到概念"""
    return CONCEPT_MAP.get(industry, industry)

def group_by_concept(zt_df):
    """按概念分组"""
    groups = defaultdict(list)
    if len(zt_df) == 0:
        return groups
    for _, row in zt_df.iterrows():
        industry = row.get('所属行业', '未知')
        concept = map_to_concept(industry)
        groups[concept].append(row.to_dict())
    return groups

def calc_bci_v2(concept, stocks, zb_df, all_zt, dates, target_date):
    """
    BCI v2.0 评分 — 100分满分
    """
    n = len(stocks)
    score = 0
    detail = {}
    
    # === 1. 涨停数量（20分）===
    if n >= 6: s1 = 20
    elif n >= 4: s1 = 17
    elif n >= 3: s1 = 14
    elif n >= 2: s1 = 10
    else: s1 = 4
    score += s1
    detail['涨停数量'] = f"{s1}/20 ({n}家)"
    
    # === 2. 梯队层次（20分）===
    板位 = [s.get('连板数', 1) for s in stocks]
    层级集 = set(板位)
    层级数 = len(层级集)
    最高板 = max(板位)
    
    s2 = 0
    if 层级数 >= 3: s2 += 14
    elif 层级数 >= 2: s2 += 10
    else: s2 += 4
    # 最高板加分（每板+2，上限6）
    s2 += min(最高板 * 2, 6)
    s2 = min(s2, 20)
    score += s2
    detail['梯队层次'] = f"{s2}/20 ({层级数}层级,最高{最高板}板)"
    
    # === 3. 龙头强度（15分）===
    封单列表 = [s.get('封板资金', 0) for s in stocks]
    max_fund = max(封单列表) if 封单列表 else 0
    
    if max_fund > 5e8: s3 = 15
    elif max_fund > 2e8: s3 = 12
    elif max_fund > 1e8: s3 = 9
    elif max_fund > 0.5e8: s3 = 6
    else: s3 = 3
    score += s3
    detail['龙头强度'] = f"{s3}/15 (最大封单{max_fund/1e8:.1f}亿)"
    
    # === 4. 可参与性——换手板比例（10分）===
    换手板数 = sum(1 for s in stocks if s.get('换手率', 0) > 5)
    一字板数 = sum(1 for s in stocks if s.get('换手率', 0) <= 3)
    换手比 = 换手板数 / n if n > 0 else 0
    
    s4 = int(换手比 * 10)
    # 全是一字板额外扣分
    if 一字板数 == n:
        s4 = max(s4 - 3, 0)
    score += s4
    detail['可参与性'] = f"{s4}/10 (换手板{换手板数}/{n})"
    
    # === 5. 炸板率（10分）===
    # 统计概念内炸板数
    板块炸板 = 0
    if len(zb_df) > 0 and '所属行业' in zb_df.columns:
        for _, row in zb_df.iterrows():
            zb_concept = map_to_concept(row.get('所属行业', ''))
            if zb_concept == concept:
                板块炸板 += 1
    
    总尝试 = n + 板块炸板
    封板率 = n / 总尝试 if 总尝试 > 0 else 1
    
    if 板块炸板 == 0: s5 = 10
    elif 封板率 > 0.8: s5 = 8
    elif 封板率 > 0.6: s5 = 5
    elif 封板率 > 0.4: s5 = 3
    else: s5 = 1
    score += s5
    detail['炸板率'] = f"{s5}/10 (封板率{封板率*100:.0f}%,炸{板块炸板}家)"
    
    # === 6. 持续性趋势（10分）—— 用斜率 ===
    day_counts = []
    for d in dates:
        df = all_zt.get(d, pd.DataFrame())
        if len(df) == 0 or '所属行业' not in df.columns:
            day_counts.append(0)
            continue
        cnt = 0
        for _, row in df.iterrows():
            if map_to_concept(row.get('所属行业', '')) == concept:
                cnt += 1
        day_counts.append(cnt)
    
    持续天数 = sum(1 for c in day_counts if c > 0)
    
    # 计算斜率：正=升温，负=降温
    if len(day_counts) >= 2 and any(c > 0 for c in day_counts):
        # 简单用最后一天vs第一天的变化
        有效天 = [(i, c) for i, c in enumerate(day_counts) if c > 0]
        if len(有效天) >= 2:
            first_i, first_c = 有效天[0]
            last_i, last_c = 有效天[-1]
            斜率 = (last_c - first_c) / (last_i - first_i + 1) if last_i > first_i else 0
        else:
            斜率 = 0
    else:
        斜率 = 0
    
    s6 = 0
    if 持续天数 >= 3:
        s6 = 7
    elif 持续天数 == 2:
        s6 = 4
    elif 持续天数 == 1:
        s6 = 1
    
    # 斜率修正
    if 斜率 > 0: s6 += 3     # 升温加分
    elif 斜率 == 0: s6 += 1   # 持平
    else: s6 += 0              # 降温不加分
    
    s6 = min(s6, 10)
    score += s6
    
    趋势标 = '📈升温' if 斜率 > 0 else ('📉降温' if 斜率 < 0 else '➡️持平')
    detail['持续性'] = f"{s6}/10 ({持续天数}天,{趋势标},{'/'.join(str(c) for c in day_counts)})"
    
    # === 7. 消息面驱动（10分）===
    news_score = NEWS_DRIVEN.get(concept, 0)
    s7 = min(news_score * 3 + 1, 10) if news_score > 0 else 0
    score += s7
    detail['消息面'] = f"{s7}/10 ({'有催化' if news_score > 0 else '无明确催化'})"
    
    # === 8. 概念纯度（5分）===
    # 如果映射前的行业都是同一个行业，说明纯度高
    原始行业 = set(s.get('所属行业', '') for s in stocks)
    if len(原始行业) == 1:
        s8 = 5  # 纯行业板块
    elif len(原始行业) <= 2:
        s8 = 3  # 2个子行业汇聚
    else:
        s8 = 2  # 多行业混合
    score += s8
    detail['概念纯度'] = f"{s8}/5 ({len(原始行业)}个子行业)"
    
    return min(score, 100), detail

def analyze_leader(stocks):
    """分析板块内角色"""
    if not stocks:
        return []
    
    # 计算龙头得分
    for s in stocks:
        zb = s.get('连板数', 1)
        hs = s.get('换手率', 0)
        fund = s.get('封板资金', 0)
        
        # 首次封板时间评分（越早越高）
        ft = s.get('首次封板时间', '')
        if ft and ':' in str(ft):
            parts = str(ft).split(':')
            try:
                minutes = int(parts[0]) * 60 + int(parts[1])
                # 9:30=570, 10:00=600, 11:30=690, 15:00=900
                time_score = max(0, (700 - minutes) / 10)  # 越早分越高
            except:
                time_score = 0
        else:
            time_score = 0
        
        s['龙头得分'] = (
            zb * 25 +                          # 高度权重最大
            time_score * 1.5 +                  # 封板时间
            min(fund / 1e8, 10) * 2 +          # 封单
            (5 if hs > 5 else 0)               # 换手板加分
        )
    
    sorted_stocks = sorted(stocks, key=lambda x: x['龙头得分'], reverse=True)
    
    for i, s in enumerate(sorted_stocks):
        zb = s.get('连板数', 1)
        hs = s.get('换手率', 0)
        max_zb = sorted_stocks[0].get('连板数', 1)
        
        if i == 0:
            s['角色'] = '🏆空间龙头' + ('(换手)' if hs > 5 else '(一字)')
        elif i == 1:
            if zb == max_zb:
                s['角色'] = '🥈换手龙' if hs > 5 else '🥈跟风龙'
            else:
                s['角色'] = '🔵跟风助攻'
        else:
            if zb == 1:
                s['角色'] = '⚪补涨/套利'
            else:
                s['角色'] = '🔵跟风助攻'
    
    return sorted_stocks

def generate_report_v2(all_zt, all_zb, dates, target_date):
    """生成v2.0报告"""
    target_zt = all_zt.get(target_date, pd.DataFrame())
    target_zb = all_zb.get(target_date, pd.DataFrame())
    
    if len(target_zt) == 0:
        return "无数据"
    
    # 按概念分组
    concept_groups = group_by_concept(target_zt)
    
    # 计算BCI v2
    results = []
    for concept, stocks in concept_groups.items():
        bci, detail = calc_bci_v2(concept, stocks, target_zb, all_zt, dates, target_date)
        最高板 = max(s.get('连板数', 1) for s in stocks)
        龙头 = max(stocks, key=lambda x: x.get('连板数', 1))
        max_fund = max(s.get('封板资金', 0) for s in stocks)
        
        results.append({
            '概念': concept,
            'BCI': bci,
            '涨停数': len(stocks),
            '最高板': 最高板,
            '龙头': 龙头.get('名称', ''),
            '龙头代码': 龙头.get('代码', ''),
            '最大封单': max_fund / 1e8,
            'detail': detail,
            'stocks': stocks,
        })
    
    results.sort(key=lambda x: x['BCI'], reverse=True)
    
    # 板块内角色
    role_data = {}
    for r in results:
        role_data[r['概念']] = analyze_leader(r['stocks'])
    
    # === 输出报告 ===
    L = []
    L.append("# BCI板块完整性分析报告 v2.0")
    L.append(f"\n> 日期：{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}")
    L.append(f"> 数据范围：{dates[0]}~{dates[-1]}（{len(dates)}日）")
    L.append(f"> 涨停：{len(target_zt)}家 | 炸板：{len(target_zb)}家 | 封板率：{len(target_zt)/(len(target_zt)+len(target_zb))*100:.1f}%")
    L.append(f"> 模型版本：BCI v2.0（概念归类+消息面+斜率+可参与性）")
    
    L.append("\n---\n")
    L.append("## 一、板块完整性排名\n")
    L.append("| # | 概念方向 | BCI | 评级 | 涨停 | 高度 | 龙头 | 封单 | 趋势 |")
    L.append("|---|---------|-----|------|------|------|------|------|------|")
    
    for i, r in enumerate(results):
        bci = r['BCI']
        if bci >= 80: 级 = '⭐5极完整'
        elif bci >= 65: 级 = '⭐4较完整'
        elif bci >= 50: 级 = '⭐3一般'
        elif bci >= 35: 级 = '⭐2较弱'
        else: 级 = '⭐1弱'
        
        # 趋势
        d = r['detail']
        持续 = d.get('持续性','')
        趋势 = '📈' if '升温' in 持续 else ('📉' if '降温' in 持续 else '➡️')
        
        L.append(f"| {i+1} | **{r['概念']}** | **{bci}** | {级} | {r['涨停数']}家 | {r['最高板']}板 | {r['龙头']} | {r['最大封单']:.1f}亿 | {趋势} |")
    
    # TOP板块详解
    L.append("\n---\n")
    L.append("## 二、TOP板块详解\n")
    
    for r in results:
        if r['BCI'] < 50:
            continue
        concept = r['概念']
        roles = role_data.get(concept, [])
        detail = r['detail']
        
        bci = r['BCI']
        if bci >= 80: 级 = '⭐⭐⭐⭐⭐'
        elif bci >= 65: 级 = '⭐⭐⭐⭐'
        elif bci >= 50: 级 = '⭐⭐⭐'
        else: 级 = '⭐⭐'
        
        L.append(f"### 【{concept}】BCI={bci} {级}\n")
        
        # 评分细节
        L.append("**评分明细**：")
        for k, v in detail.items():
            L.append(f"- {k}：{v}")
        
        # 梯队表
        L.append(f"\n**梯队成员**：\n")
        L.append("| 角色 | 名称(代码) | 连板 | 换手 | 封单 | 首封时间 |")
        L.append("|------|-----------|------|------|------|---------|")
        for s in roles:
            ft = s.get('首次封板时间', '')
            L.append(f"| {s.get('角色','')} | {s.get('名称','')}({s.get('代码','')}) | {s.get('连板数',1)}板 | {s.get('换手率',0):.1f}% | {s.get('封板资金',0)/1e8:.1f}亿 | {ft} |")
        
        # 完整性诊断
        诊断 = []
        if r['涨停数'] >= 3: 诊断.append("✅ 跟风充足")
        else: 诊断.append("⚠️ 跟风偏少")
        if r['最高板'] >= 3: 诊断.append("✅ 高度足够")
        elif r['最高板'] == 2: 诊断.append("⚠️ 高度一般")
        else: 诊断.append("❌ 高度不足")
        
        有换手 = any(s.get('换手率',0) > 5 for s in roles)
        if 有换手: 诊断.append("✅ 有换手可做")
        else: 诊断.append("❌ 一字板难参与")
        
        news = NEWS_DRIVEN.get(concept, 0)
        if news >= 2: 诊断.append("✅ 消息面强驱动")
        elif news == 1: 诊断.append("⚠️ 消息面弱驱动")
        else: 诊断.append("❌ 无消息面")
        
        L.append(f"\n**完整性诊断**：{'  |  '.join(诊断)}\n")
    
    # 主线判定
    L.append("\n---\n")
    L.append("## 三、主线方向判定\n")
    
    # 跨日统计
    concept_daily = defaultdict(lambda: {d: 0 for d in dates})
    for d, df in all_zt.items():
        if len(df) == 0 or '所属行业' not in df.columns:
            continue
        for _, row in df.iterrows():
            c = map_to_concept(row.get('所属行业', ''))
            concept_daily[c][d] += 1
    
    # 排序
    主线候选 = []
    for concept, daily in concept_daily.items():
        持续天 = sum(1 for v in daily.values() if v > 0)
        总数 = sum(daily.values())
        counts = [daily.get(d, 0) for d in dates]
        # 斜率
        有效 = [(i, c) for i, c in enumerate(counts) if c > 0]
        if len(有效) >= 2:
            斜率 = (有效[-1][1] - 有效[0][1]) / (有效[-1][0] - 有效[0][0] + 1)
        else:
            斜率 = 0
        主线候选.append((concept, 持续天, 总数, 斜率, counts))
    
    主线候选.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    L.append(f"| # | 概念方向 | 持续 | 累计涨停 | 趋势 | {dates[0][-4:]}→{dates[1][-4:]}→{dates[2][-4:]} |")
    L.append("|---|---------|------|---------|------|--------------|")
    
    for i, (concept, days, total, slope, counts) in enumerate(主线候选[:15]):
        if days < 2:
            continue
        趋势 = '🔥升温' if slope > 0 else ('📉降温' if slope < 0 else '➡️持平')
        tag = '🏆主线' if i == 0 else ('🥈次线' if i <= 2 else '📊支线')
        ct = '→'.join(str(c) for c in counts)
        L.append(f"| {i+1} | **{concept}** | {days}天 | {total}家 | {趋势} | {ct} |")
    
    # 连板传导链
    L.append("\n---\n")
    L.append("## 四、连板梯队传导链\n")
    L.append("```")
    
    # 找所有2板+的标的
    high_stocks = [s for _, s in target_zt.iterrows() if s.get('连板数', 1) >= 2]
    high_stocks.sort(key=lambda x: x.get('连板数', 1), reverse=True)
    
    concept_chains = defaultdict(list)
    for s in high_stocks:
        c = map_to_concept(s.get('所属行业', ''))
        concept_chains[c].append(s)
    
    for concept, chain in concept_chains.items():
        names = [f"{s.get('名称','')}({s.get('连板数',1)}板)" for s in chain]
        L.append(f"  [{concept}] {' → '.join(names)}")
    
    L.append("```\n")
    
    # 操作建议
    L.append("\n---\n")
    L.append("## 五、操作建议\n")
    
    L.append("### 5.1 板块强度×可操作性矩阵\n")
    L.append("| 概念方向 | BCI | 可参与 | 消息面 | 趋势 | 综合建议 |")
    L.append("|---------|-----|--------|--------|------|---------|")
    
    for r in results[:10]:
        roles = role_data.get(r['概念'], [])
        有换手 = any(s.get('换手率',0) > 5 for s in roles)
        参与 = '✅可做' if 有换手 else '❌排不到'
        news = NEWS_DRIVEN.get(r['概念'], 0)
        消息 = '🔥强' if news >= 2 else ('⚠️弱' if news == 1 else '❌无')
        d = r['detail']
        趋势 = '📈' if '升温' in d.get('持续性','') else ('📉' if '降温' in d.get('持续性','') else '➡️')
        
        # 综合建议
        if r['BCI'] >= 65 and 有换手 and news >= 2:
            建议 = '⭐⭐⭐ 重点关注'
        elif r['BCI'] >= 65 and 有换手:
            建议 = '⭐⭐ 可以参与'
        elif r['BCI'] >= 50 and 有换手:
            建议 = '⭐ 轻仓试错'
        elif not 有换手:
            建议 = '观望(排不到)'
        else:
            建议 = '观望'
        
        L.append(f"| {r['概念']} | {r['BCI']} | {参与} | {消息} | {趋势} | {建议} |")
    
    L.append("\n### 5.2 重点标的清单\n")
    L.append("从BCI≥50的板块中，筛选出**有换手板+有消息面+有持续性**的可操作标的：\n")
    
    推荐 = []
    for r in results:
        if r['BCI'] < 50:
            continue
        roles = role_data.get(r['概念'], [])
        for s in roles:
            if s.get('换手率', 0) > 5:
                推荐.append({
                    '概念': r['概念'],
                    'BCI': r['BCI'],
                    **s,
                })
    
    推荐.sort(key=lambda x: (x['BCI'], x.get('连板数',1)), reverse=True)
    
    if 推荐:
        L.append("| 概念 | BCI | 角色 | 名称(代码) | 连板 | 换手 | 封单 |")
        L.append("|------|-----|------|-----------|------|------|------|")
        for s in 推荐[:15]:
            L.append(f"| {s['概念']} | {s['BCI']} | {s.get('角色','')} | {s.get('名称','')}({s.get('代码','')}) | {s.get('连板数',1)}板 | {s.get('换手率',0):.1f}% | {s.get('封板资金',0)/1e8:.1f}亿 |")
    
    # 自检
    L.append("\n---\n")
    L.append("## 六、模型自检\n")
    L.append("### v1.0→v2.0 改进对比\n")
    L.append("| 问题 | v1.0 | v2.0 | 解决情况 |")
    L.append("|------|------|------|---------|")
    L.append("| 行业≠题材 | 用行业分类 | 概念映射表 | ✅ 通信设备+光学光电合并为\"AI数据中心\" |")
    L.append("| 缺消息面 | 无 | 消息面驱动评分(10分) | ✅ AI/新能源等有催化的板块得分更高 |")
    L.append("| 持续性粗糙 | 只看天数 | 斜率衰减 | ✅ 降温板块不再与升温板块同分 |")
    L.append("| 一字板虚高 | 同等评分 | 可参与性维度(10分) | ✅ 全一字板板块在\"可参与性\"上得分低 |")
    L.append("| 无纯度检查 | 无 | 概念纯度评分(5分) | ✅ 混合概念板块扣分 |")
    L.append("| 龙头判定粗 | 只看连板数 | 多维龙头得分(连板+封板时间+封单+换手) | ✅ 封板时间纳入 |")
    
    return '\n'.join(L)

# ====== 主程序 ======
if __name__ == '__main__':
    print("BCI v2.0 — 开始采集数据...\n")
    dates = ['20260415', '20260416', '20260417']
    all_zt, all_zb = fetch_data(dates)
    
    print("\n开始分析...\n")
    report = generate_report_v2(all_zt, all_zb, dates, '20260417')
    
    output = '/Users/ecustkiller/WorkBuddy/Claw/BCI板块完整性分析v2_20260417.md'
    with open(output, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"报告已输出: {output}")
    print(f"报告长度: {len(report)} 字符")
