#!/usr/bin/env python3
"""
板块完整性(BCI)量化分析系统 — 用真实数据跑
"""
import akshare as ak
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime
import time
import warnings
warnings.filterwarnings('ignore')

def fetch_data(dates=['20260415','20260416','20260417']):
    """采集多日涨停+炸板数据"""
    all_zt = {}
    all_zb = {}
    for d in dates:
        try:
            zt = ak.stock_zt_pool_em(date=d)
            all_zt[d] = zt
            print(f"  {d} 涨停 {len(zt)}家")
        except Exception as e:
            print(f"  {d} 涨停获取失败: {e}")
            all_zt[d] = pd.DataFrame()
        time.sleep(0.5)
        try:
            zb = ak.stock_zt_pool_zbgc_em(date=d)
            all_zb[d] = zb
            print(f"  {d} 炸板 {len(zb)}家")
        except Exception as e:
            all_zb[d] = pd.DataFrame()
        time.sleep(0.5)
    return all_zt, all_zb

def calc_bci(industry, stocks_df, zb_df, multi_day_zt):
    """计算板块完整性指数BCI"""
    score = 0
    n = len(stocks_df)
    
    # 1. 涨停数量（25分）
    if n >= 5: score += 25
    elif n >= 3: score += 20
    elif n >= 2: score += 12
    elif n == 1: score += 5
    
    # 2. 梯队层次（25分）
    if '连板数' in stocks_df.columns:
        板位集合 = set(stocks_df['连板数'].tolist())
        层级数 = len(板位集合)
        最高板 = max(板位集合)
    else:
        层级数 = 1
        最高板 = 1
    
    if 层级数 >= 3: score += 25
    elif 层级数 >= 2: score += 18
    else: score += 8
    # 最高板额外加分
    score += min(最高板 * 3, 12)
    
    # 3. 龙头封单强度（20分）
    if '封板资金' in stocks_df.columns:
        max_fund = stocks_df['封板资金'].max()
        if max_fund > 5e8: score += 20
        elif max_fund > 2e8: score += 15
        elif max_fund > 1e8: score += 10
        elif max_fund > 0.5e8: score += 7
        else: score += 3
    else:
        score += 5
    
    # 4. 炸板情况（15分）
    if len(zb_df) > 0 and '所属行业' in zb_df.columns:
        板块炸板 = len(zb_df[zb_df['所属行业'] == industry])
    else:
        板块炸板 = 0
    
    total_try = n + 板块炸板
    if 板块炸板 == 0:
        score += 15
    elif total_try > 0 and n / total_try > 0.7:
        score += 10
    elif total_try > 0 and n / total_try > 0.5:
        score += 6
    else:
        score += 2
    
    # 5. 换手板比例（10分）—— 换手>5%为换手板
    if '换手率' in stocks_df.columns:
        换手板 = (stocks_df['换手率'] > 5).sum()
        换手比例 = 换手板 / n if n > 0 else 0
        score += int(换手比例 * 10)
    
    # 6. 持续性（bonus，跨日统计）
    持续天数 = 0
    for d, df in multi_day_zt.items():
        if len(df) > 0 and '所属行业' in df.columns:
            if (df['所属行业'] == industry).any():
                持续天数 += 1
    if 持续天数 >= 3: score += 8
    elif 持续天数 >= 2: score += 5
    elif 持续天数 == 1: score += 2
    
    return min(score, 100)

def identify_roles(stocks_df):
    """识别板块内各标的角色"""
    if len(stocks_df) == 0:
        return []
    
    results = []
    df = stocks_df.sort_values('连板数', ascending=False) if '连板数' in stocks_df.columns else stocks_df
    
    for i, (_, row) in enumerate(df.iterrows()):
        role = ''
        zb = row.get('连板数', 1)
        hs = row.get('换手率', 0)
        fund = row.get('封板资金', 0)
        
        if i == 0:
            if hs < 3:
                role = '空间龙头(一字)'
            else:
                role = '空间龙头(换手)'
        elif i == 1:
            if zb == df.iloc[0].get('连板数', 1):
                role = '换手龙头' if hs > 5 else '跟风龙头'
            else:
                role = '跟风助攻'
        else:
            if zb == 1:
                role = '补涨/套利'
            else:
                role = '跟风助攻'
        
        results.append({
            '名称': row.get('名称', ''),
            '代码': row.get('代码', ''),
            '连板数': zb,
            '换手率': hs,
            '封板资金': fund,
            '角色': role,
        })
    
    return results

def analyze_block_relations(block_data, target_date):
    """分析板块间关系"""
    # 简化版：看涨停个股的概念交叉
    zt_df = block_data.get(target_date, pd.DataFrame())
    if len(zt_df) == 0:
        return {}
    
    # 按行业分组后，看两两之间是否有共同涨停标的日期
    industries = zt_df['所属行业'].unique() if '所属行业' in zt_df.columns else []
    
    # 用跨日共振来衡量：两个行业是否同时有涨停
    relations = {}
    for d, df in block_data.items():
        if len(df) == 0 or '所属行业' not in df.columns:
            continue
        day_industries = set(df['所属行业'].unique())
        for ind in day_industries:
            if ind not in relations:
                relations[ind] = {'共振天数': 0, '独立天数': 0}
            relations[ind]['共振天数'] += 1
    
    return relations

def generate_report(all_zt, all_zb, target_date='20260417', dates=None):
    """生成完整报告"""
    if dates is None:
        dates = sorted(all_zt.keys())
    target_zt = all_zt.get(target_date, pd.DataFrame())
    target_zb = all_zb.get(target_date, pd.DataFrame())
    
    if len(target_zt) == 0:
        return "无数据"
    
    # 按行业分组
    if '所属行业' not in target_zt.columns:
        return "数据中无行业字段"
    
    block_groups = {}
    for industry, group in target_zt.groupby('所属行业'):
        block_groups[industry] = group
    
    # 计算BCI
    bci_results = []
    for industry, group in block_groups.items():
        bci = calc_bci(industry, group, target_zb, all_zt)
        最高板 = group['连板数'].max() if '连板数' in group.columns else 1
        龙头 = group.sort_values('连板数', ascending=False).iloc[0]
        max_fund = group['封板资金'].max() / 1e8 if '封板资金' in group.columns else 0
        
        bci_results.append({
            '行业': industry,
            'BCI': bci,
            '涨停数': len(group),
            '最高板': 最高板,
            '龙头': 龙头.get('名称', ''),
            '龙头代码': 龙头.get('代码', ''),
            '最大封单': max_fund,
        })
    
    bci_results.sort(key=lambda x: x['BCI'], reverse=True)
    
    # 板块内角色分析
    role_analysis = {}
    for industry, group in block_groups.items():
        roles = identify_roles(group)
        role_analysis[industry] = roles
    
    # 生成报告文本
    lines = []
    lines.append("# 板块完整性(BCI)实盘量化分析报告")
    lines.append(f"\n> 分析日期：{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}")
    lines.append(f"> 数据来源：AKShare（东方财富）")
    lines.append(f"> 当日涨停：{len(target_zt)}家 | 炸板：{len(target_zb)}家 | 封板率：{len(target_zt)/(len(target_zt)+len(target_zb))*100:.1f}%")
    
    # 总览
    lines.append("\n---\n")
    lines.append("## 一、板块完整性排名（BCI得分）\n")
    lines.append("| 排名 | 板块 | BCI | 评级 | 涨停 | 最高板 | 龙头 | 最大封单 |")
    lines.append("|------|------|-----|------|------|--------|------|---------|")
    
    for i, r in enumerate(bci_results):
        if r['BCI'] >= 80: 评级 = '⭐⭐⭐⭐⭐'
        elif r['BCI'] >= 60: 评级 = '⭐⭐⭐⭐'
        elif r['BCI'] >= 40: 评级 = '⭐⭐⭐'
        elif r['BCI'] >= 20: 评级 = '⭐⭐'
        else: 评级 = '⭐'
        lines.append(f"| {i+1} | {r['行业']} | **{r['BCI']}** | {评级} | {r['涨停数']}家 | {r['最高板']}板 | {r['龙头']} | {r['最大封单']:.1f}亿 |")
    
    # TOP板块详细分析
    lines.append("\n---\n")
    lines.append("## 二、TOP板块梯队详解\n")
    
    for r in bci_results[:8]:  # TOP8
        industry = r['行业']
        roles = role_analysis.get(industry, [])
        if not roles:
            continue
        
        if r['BCI'] >= 80: 评级 = '⭐⭐⭐⭐⭐ 极完整'
        elif r['BCI'] >= 60: 评级 = '⭐⭐⭐⭐ 较完整'
        elif r['BCI'] >= 40: 评级 = '⭐⭐⭐ 一般'
        elif r['BCI'] >= 20: 评级 = '⭐⭐ 较弱'
        else: 评级 = '⭐ 很弱'
        
        lines.append(f"### 【{industry}】BCI={r['BCI']} {评级}\n")
        lines.append(f"涨停{r['涨停数']}家 | 最高{r['最高板']}板 | 最大封单{r['最大封单']:.1f}亿\n")
        lines.append("| 角色 | 名称(代码) | 连板 | 换手率 | 封单(亿) |")
        lines.append("|------|-----------|------|--------|---------|")
        for role in roles:
            lines.append(f"| {role['角色']} | {role['名称']}({role['代码']}) | {role['连板数']}板 | {role['换手率']:.1f}% | {role['封板资金']/1e8:.1f} |")
        
        # 评语
        完整性分析 = []
        if r['涨停数'] >= 3:
            完整性分析.append("✅ 跟风数量充足")
        else:
            完整性分析.append("⚠️ 跟风偏少")
        
        if r['最高板'] >= 3:
            完整性分析.append("✅ 龙头打出高度")
        else:
            完整性分析.append("⚠️ 高度有限")
        
        有换手板 = any(role['换手率'] > 5 for role in roles)
        if 有换手板:
            完整性分析.append("✅ 有换手板可参与")
        else:
            完整性分析.append("❌ 全是一字板，难参与")
        
        层级 = len(set(role['连板数'] for role in roles))
        if 层级 >= 2:
            完整性分析.append(f"✅ {层级}个板位层级")
        else:
            完整性分析.append("⚠️ 单一板位")
        
        lines.append(f"\n**完整性诊断**：{'  |  '.join(完整性分析)}\n")
    
    # 板块间关系
    lines.append("\n---\n")
    lines.append("## 三、板块间关系分析\n")
    lines.append("### 3.1 跨日共振统计\n")
    lines.append(f"近3日（{dates[0][-4:]}~{dates[-1][-4:]}）都有涨停的行业 = 持续性强的方向\n")
    
    # 统计每个行业出现在几天的涨停中
    industry_days = defaultdict(set)
    for d, df in all_zt.items():
        if len(df) > 0 and '所属行业' in df.columns:
            for ind in df['所属行业'].unique():
                industry_days[ind].add(d)
    
    lines.append("| 行业 | 出现天数 | 持续性 | 各日涨停数 |")
    lines.append("|------|---------|--------|-----------|")
    
    sorted_inds = sorted(industry_days.items(), key=lambda x: len(x[1]), reverse=True)
    for ind, days in sorted_inds:
        n_days = len(days)
        if n_days >= 2:
            持续 = '🔥 强' if n_days >= 3 else '✅ 中'
            day_counts = []
            for d in dates:
                df = all_zt.get(d, pd.DataFrame())
                if len(df) > 0 and '所属行业' in df.columns:
                    cnt = (df['所属行业'] == ind).sum()
                    day_counts.append(f"{d[-2:]}日:{cnt}家")
                else:
                    day_counts.append(f"{d[-2:]}日:0")
            lines.append(f"| {ind} | {n_days}天 | {持续} | {' / '.join(day_counts)} |")
    
    # 主线判定
    lines.append("\n### 3.2 主线方向判定\n")
    
    # 找持续3天+涨停数最多的
    主线候选 = []
    for ind, days in sorted_inds:
        if len(days) >= 2:
            total_zt = 0
            for d in days:
                df = all_zt.get(d, pd.DataFrame())
                if len(df) > 0 and '所属行业' in df.columns:
                    total_zt += (df['所属行业'] == ind).sum()
            主线候选.append((ind, len(days), total_zt))
    
    主线候选.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    if 主线候选:
        lines.append("按「持续天数 × 涨停总数」排序的主线方向：\n")
        for i, (ind, days, total) in enumerate(主线候选[:10]):
            marker = '🏆 主线' if i == 0 else ('🥈 次线' if i <= 2 else '📊 支线')
            lines.append(f"{i+1}. **{ind}** — {days}天持续 / 累计{total}家涨停 → {marker}")
    
    # 龙头→跟风传导
    lines.append("\n### 3.3 龙头→跟风传导链\n")
    lines.append("根据连板高度和封板时间推断的板块传导关系：\n")
    lines.append("```")
    
    # 找所有3日数据中的高标连板股
    all_high = []
    target_df = all_zt.get(target_date, pd.DataFrame())
    if len(target_df) > 0 and '连板数' in target_df.columns:
        high_df = target_df[target_df['连板数'] >= 2].sort_values('连板数', ascending=False)
        for _, row in high_df.iterrows():
            all_high.append(row)
    
    if all_high:
        # 按行业分组画传导链
        ind_leaders = defaultdict(list)
        for row in all_high:
            ind_leaders[row.get('所属行业','未知')].append(row)
        
        for ind, leaders in ind_leaders.items():
            leader = leaders[0]
            chain = f"{leader.get('名称','')}({leader.get('连板数',1)}板)"
            for f in leaders[1:]:
                chain += f" → {f.get('名称','')}({f.get('连板数',1)}板)"
            lines.append(f"  [{ind}] {chain}")
    
    lines.append("```\n")
    
    # 操作建议
    lines.append("\n---\n")
    lines.append("## 四、基于BCI的操作建议\n")
    
    if bci_results:
        top = bci_results[0]
        lines.append(f"### 当日最强板块：{top['行业']}（BCI={top['BCI']}）\n")
        
        if top['BCI'] >= 70:
            lines.append(f"- ✅ BCI≥70，板块完整性好，可以参与")
            lines.append(f"- 龙头：{top['龙头']}（{top['最高板']}板），如果一字板则等开板日")
            lines.append(f"- 优先找板块内的**换手板低吸**机会")
        elif top['BCI'] >= 50:
            lines.append(f"- ⚠️ BCI 50-70，完整性一般，轻仓参与")
            lines.append(f"- 只做龙头，不做跟风")
        else:
            lines.append(f"- ❌ BCI<50，完整性不足，建议观望")
        
        # 比较TOP3
        lines.append(f"\n### 板块强度对比\n")
        lines.append("| 板块 | BCI | 可操作性 | 建议 |")
        lines.append("|------|-----|---------|------|")
        for r in bci_results[:5]:
            有换手 = any(role['换手率'] > 5 for role in role_analysis.get(r['行业'], []))
            可操作 = '可做' if 有换手 else '一字板难参与'
            if r['BCI'] >= 70 and 有换手:
                建议 = '✅ 重点关注'
            elif r['BCI'] >= 50:
                建议 = '⚠️ 轻仓试错'
            else:
                建议 = '观望'
            lines.append(f"| {r['行业']} | {r['BCI']} | {可操作} | {建议} |")
    
    # BCI趋势（如果有多日数据）
    lines.append("\n### BCI跨日趋势\n")
    d1, d2, d3 = dates[0], dates[1], dates[2]
    lines.append(f"对比{d1[-4:]}→{d2[-4:]}→{d3[-4:]}的板块变化：\n")
    
    # 计算每日每个行业的涨停数
    daily_counts = defaultdict(lambda: defaultdict(int))
    for d, df in all_zt.items():
        if len(df) > 0 and '所属行业' in df.columns:
            for ind, cnt in df['所属行业'].value_counts().items():
                daily_counts[ind][d] = cnt
    
    # 找变化明显的
    lines.append(f"| 行业 | {d1[-2:]}日 | {d2[-2:]}日 | {d3[-2:]}日 | 趋势 |")
    lines.append("|------|-----|-----|------|------|")
    for ind in [r['行业'] for r in bci_results[:10]]:
        c8 = daily_counts[ind].get(d1, 0)
        c9 = daily_counts[ind].get(d2, 0)
        c10 = daily_counts[ind].get(d3, 0)
        if c10 > c9: 趋势 = '📈 升温'
        elif c10 < c9: 趋势 = '📉 降温'
        elif c10 == c9 and c10 > 0: 趋势 = '➡️ 持平'
        else: 趋势 = '🆕 新出现'
        lines.append(f"| {ind} | {c8} | {c9} | {c10} | {趋势} |")
    
    return '\n'.join(lines)

# ====== 主程序 ======
if __name__ == '__main__':
    print("开始采集数据...\n")
    dates = ['20260415', '20260416', '20260417']
    all_zt, all_zb = fetch_data(dates)
    
    print("\n开始分析...\n")
    report = generate_report(all_zt, all_zb, '20260417', dates)
    
    output_file = '/Users/ecustkiller/WorkBuddy/Claw/BCI板块完整性分析_20260417.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n报告已输出: {output_file}")
    print(f"报告长度: {len(report)} 字符")
