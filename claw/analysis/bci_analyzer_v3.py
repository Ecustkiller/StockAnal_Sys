#!/usr/bin/env python3
"""
BCI v3.0 — 个股概念标签驱动的板块完整性分析
核心改进：不再用行业归类，直接用东方财富个股概念标签
"""
import akshare as ak
import requests, json, time
from collections import defaultdict, Counter
import warnings
warnings.filterwarnings('ignore')

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}

# 过滤掉的非题材概念
SKIP_CONCEPTS = {
    # === 指数/成分股 ===
    '融资融券', '深股通', '沪股通', '标准普尔', '富时罗素',
    'MSCI中国', 'HS300_', '上证180_', '上证50_', '中证500_',
    '中证1000', '中证100', '上证380', '创业板综',
    # === 机构持仓 ===
    '基金重仓', '机构重仓', 'QFII重仓', '社保重仓', '险资重仓',
    '北向资金重仓', '外资重仓',
    # === 市值/价格分类 ===
    '百元股', '大盘股', '小盘股', '微盘股',
    '大盘成长', '大盘价值', '小盘成长', '小盘价值',
    '低价股', '高价股',
    # === 涨跌/技术面标签 ===
    '昨日涨停', '昨日涨停_含一字', '昨日连板', '昨日连板_含一字',
    '昨日高振幅', '昨日高换手', '昨日触板',
    '百日新高', '百日新低', '近期新高', '近期新低',
    '历史新高', '历史新低',
    '最近多板', '连续涨停', '放量突破',
    '东方财富热股', '人气榜',
    # === 财务/业绩标签 ===
    '转债标的', '可转债',
    '2025中报预增', '2025中报扭亏', '2026中报预增', '2026中报扭亏',
    '年报预增', '年报扭亏', '中报预增', '中报扭亏',
    '高股息', '高分红', '高送转',
    # === 属性/治理标签 ===
    '央国企改革', '央企改革', '国企改革',
    '股权激励', '员工持股', '回购',
    '举牌', '壳资源', 'ST摘帽', 'ST板块',
    '破净股', '破发股',
    # === 其他非题材 ===
    '次新股', '新股与次新股', 'IPO受益',
    '参股金融', '参股券商', '参股银行', '参股保险',
    '创投概念', '独角兽',
}

# 地域概念和其他模糊匹配过滤
SKIP_KEYWORDS = [
    '板块', '长江三角', '西部大开发', '一带一路',
    '省份', '地区', '昨日', '近期', '百日',
    '历史新', '预增', '预减', '扭亏',
    '重仓', '成分股',
    # 地域概念
    '特区', '经济区', '自贸区', '粤港澳', '京津冀',
    '长三角', '珠三角', '成渝', '海南',
    '深圳', '上海', '北京', '广东', '浙江', '江苏',
    '福建', '四川', '湖南', '湖北', '安徽', '山东',
    '河南', '河北', '陕西', '新疆', '西藏', '云南',
    '贵州', '广西', '江西', '辽宁', '吉林', '黑龙江',
    '内蒙', '甘肃', '青海', '宁夏', '天津', '重庆',
]

# 消息面强催化的概念（手动标注当期热点，权重1-3）
HOT_CONCEPTS = {
    '光伏概念': 2, '逆变器': 3, '储能概念': 3,
    '固态电池': 3, '锂电池概念': 2, '电池技术': 2, '钠离子电池': 3,
    '5G概念': 2, '通信技术': 2, '数据中心': 3,
    '华为概念': 2, '半导体概念': 2, '国产芯片': 2,
    '新能源车': 2, '汽车零部件': 2, '汽车': 1,
    '玻璃基板': 2, '低空经济': 2,
    '小米概念': 1, '电商概念': 1,
}

def fetch_zt_data(dates):
    """采集涨停+炸板"""
    all_zt, all_zb = {}, {}
    for d in dates:
        try:
            all_zt[d] = ak.stock_zt_pool_em(date=d)
            print(f"  {d} 涨停{len(all_zt[d])}家")
        except:
            all_zt[d] = __import__('pandas').DataFrame()
        time.sleep(0.3)
        try:
            all_zb[d] = ak.stock_zt_pool_zbgc_em(date=d)
            print(f"  {d} 炸板{len(all_zb[d])}家")
        except:
            all_zb[d] = __import__('pandas').DataFrame()
        time.sleep(0.3)
    return all_zt, all_zb

def fetch_stock_concepts(codes):
    """批量获取个股概念标签"""
    result = {}
    for i, code in enumerate(codes):
        market = '1' if code.startswith('6') else '0'
        secid = f'{market}.{code}'
        url = f'https://push2.eastmoney.com/api/qt/slist/get?spt=3&fltt=2&invt=2&fid=f3&fields=f12,f13,f14,f3&secid={secid}&po=1&pn=1&pz=30&np=1'
        try:
            r = requests.get(url, headers=headers, timeout=8)
            items = r.json().get('data', {}).get('diff', [])
            concepts = []
            for item in items:
                c = item.get('f14', '')
                if c and c not in SKIP_CONCEPTS and not any(k in c for k in SKIP_KEYWORDS):
                    concepts.append(c)
            result[code] = concepts
        except:
            result[code] = []
        if (i+1) % 15 == 0:
            print(f"  概念查询 {i+1}/{len(codes)}...")
        time.sleep(0.12)
    return result

def build_concept_blocks(zt_df, concept_map):
    """按概念标签构建板块（一股多概念→出现在多个板块中）"""
    blocks = defaultdict(list)
    for _, row in zt_df.iterrows():
        code = row['代码']
        concepts = concept_map.get(code, [])
        stock_info = row.to_dict()
        for c in concepts:
            blocks[c].append(stock_info)
    return blocks

def calc_bci_v3(concept, stocks, zb_concepts, all_concept_daily, dates, concept_stock_codes=None):
    """BCI v3.0评分 — 含独占度修正"""
    n = len(stocks)
    score = 0
    detail = {}

    # 计算独占度：本概念中有多少股票不属于BCI更高的其他概念
    # concept_stock_codes: {concept: set(codes)} 用于交叉比对
    独占数 = n  # 默认全部独占
    if concept_stock_codes is not None:
        my_codes = set(s.get('代码', '') for s in stocks)
        # 被更强概念覆盖的股票数
        被覆盖 = 0
        for other_concept, other_codes in concept_stock_codes.items():
            if other_concept == concept:
                continue
            overlap = my_codes & other_codes
            被覆盖 = max(被覆盖, len(overlap))
        独占数 = n - 被覆盖 if 被覆盖 < n else max(1, n - 被覆盖)
    独占率 = 独占数 / n if n > 0 else 1

    # 1. 涨停数量(20分)
    if n >= 8: s1 = 20
    elif n >= 5: s1 = 17
    elif n >= 3: s1 = 13
    elif n >= 2: s1 = 8
    else: s1 = 3
    score += s1
    detail['数量'] = f"{s1}/20({n}家)"

    # 2. 梯队层次(18分)
    板位 = [s.get('连板数', 1) for s in stocks]
    层级 = len(set(板位))
    最高 = max(板位)
    s2 = min(层级 * 5, 12) + min(最高 * 2, 6)
    s2 = min(s2, 18)
    score += s2
    detail['梯队'] = f"{s2}/18({层级}层/{最高}板)"

    # 3. 龙头强度(12分) — 封单+市值加权
    funds = [s.get('封板资金', 0) for s in stocks]
    max_fund = max(funds) if funds else 0
    # 找龙头的流通市值
    龙头 = max(stocks, key=lambda x: x.get('连板数',1)*1e10 + x.get('封板资金',0))
    龙头市值 = 龙头.get('流通市值', 0)
    # 千亿市值涨停加分
    市值加分 = 2 if 龙头市值 > 1e11 else (1 if 龙头市值 > 5e10 else 0)
    if max_fund > 5e8: s3 = 10
    elif max_fund > 2e8: s3 = 8
    elif max_fund > 1e8: s3 = 6
    elif max_fund > 5e7: s3 = 4
    else: s3 = 2
    s3 = min(s3 + 市值加分, 12)
    score += s3
    detail['龙头'] = f"{s3}/12(封{max_fund/1e8:.1f}亿)"

    # 4. 可参与性(12分) — 换手板比例+弱封单高板惩罚
    换手板 = sum(1 for s in stocks if s.get('换手率', 0) > 5)
    换手比 = 换手板 / n if n > 0 else 0
    s4 = int(换手比 * 10)
    # 独有：对弱封单(<0.5亿)+高板(>=3板)的标的，不给板位加分
    for s in stocks:
        if s.get('连板数',1) >= 3 and s.get('封板资金',0) < 5e7:
            s4 = max(s4 - 2, 0)  # 弱封单高板扣分
    # 孤军惩罚(只有1家涨停)
    if n == 1: s4 = max(s4 - 2, 0)
    s4 = min(s4, 12)
    score += s4
    detail['参与'] = f"{s4}/12(换手板{换手板}/{n})"

    # 5. 炸板率(8分)
    概念炸板 = zb_concepts.get(concept, 0)
    总尝试 = n + 概念炸板
    封板率 = n / 总尝试 if 总尝试 > 0 else 1
    if 概念炸板 == 0: s5 = 8
    elif 封板率 > 0.75: s5 = 6
    elif 封板率 > 0.5: s5 = 4
    else: s5 = 1
    score += s5
    detail['炸板'] = f"{s5}/8(率{封板率*100:.0f}%)"

    # 6. 持续性+斜率(10分)
    day_counts = [all_concept_daily.get(d, {}).get(concept, 0) for d in dates]
    持续天 = sum(1 for c in day_counts if c > 0)
    # 斜率
    有效 = [(i, c) for i, c in enumerate(day_counts) if c > 0]
    if len(有效) >= 2:
        斜率 = (有效[-1][1] - 有效[0][1]) / max(有效[-1][0] - 有效[0][0], 1)
    else:
        斜率 = 0

    s6 = 0
    if 持续天 >= 3: s6 = 6
    elif 持续天 == 2: s6 = 3
    else: s6 = 1
    if 斜率 > 1: s6 += 4      # 强升温
    elif 斜率 > 0: s6 += 3    # 升温
    elif 斜率 == 0: s6 += 1   # 持平
    # 降温不加分
    s6 = min(s6, 10)
    score += s6
    趋势 = '📈强升' if 斜率 > 1 else ('📈升温' if 斜率 > 0 else ('📉降温' if 斜率 < 0 else '➡️'))
    detail['持续'] = f"{s6}/10({持续天}天/{趋势}/{'/'.join(str(c) for c in day_counts)})"

    # 7. 消息面(10分)
    hot = HOT_CONCEPTS.get(concept, 0)
    s7 = min(hot * 3 + 1, 10) if hot > 0 else 0
    score += s7
    detail['消息'] = f"{s7}/10({'🔥' + str(hot) if hot else '无'})"

    # 8. 板块内聚度(10分) — 有多少标的同时属于另一个热门概念
    # 用概念交叉来衡量"这些股票是不是真的一伙的"
    if n >= 2:
        # 计算任意两只股共享的概念数
        from itertools import combinations
        pair_overlap = []
        for s1_info, s2_info in combinations(stocks, 2):
            c1 = set(s1_info.get('_concepts', []))
            c2 = set(s2_info.get('_concepts', []))
            overlap = len(c1 & c2)
            pair_overlap.append(overlap)
        avg_overlap = sum(pair_overlap) / len(pair_overlap) if pair_overlap else 0
        s8 = min(int(avg_overlap * 2), 10)
    else:
        s8 = 3  # 孤军
    score += s8
    detail['内聚'] = f"{s8}/10"

    # === 独占度修正 ===
    # 如果本概念的涨停股大部分都被更强概念覆盖，说明这个概念缺乏独立性
    # 独占率<30%时扣分，避免"电池技术"这种大杂烩概念虚高
    if 独占率 < 0.3:
        score = int(score * 0.90)  # 扣10%
    elif 独占率 < 0.5:
        score = int(score * 0.95)  # 扣5%
    detail['独占'] = f"{独占数}/{n}({独占率*100:.0f}%)"

    return min(score, 100), detail

def analyze_roles(stocks):
    """龙头-跟风角色识别 v3"""
    for s in stocks:
        zb = s.get('连板数', 1)
        fund = s.get('封板资金', 0)
        hs = s.get('换手率', 0)
        mv = s.get('流通市值', 0)
        ft = str(s.get('首次封板时间', '150000'))
        try:
            ft_min = int(ft[:2]) * 60 + int(ft[2:4])
        except:
            ft_min = 900

        s['_leader_score'] = (
            zb * 20 +
            max(0, (660 - ft_min)) * 0.15 +    # 封板时间(9:30=570→660-570=90*0.15=13.5)
            min(fund / 1e8, 12) * 1.5 +          # 封单
            (3 if hs > 5 else 0) +                # 换手
            (3 if mv > 1e11 else (1.5 if mv > 3e10 else 0))  # 市值
        )

    sorted_s = sorted(stocks, key=lambda x: x['_leader_score'], reverse=True)
    for i, s in enumerate(sorted_s):
        hs = s.get('换手率', 0)
        zb = s.get('连板数', 1)
        top_zb = sorted_s[0].get('连板数', 1)
        if i == 0:
            s['角色'] = '🏆龙头' + ('(换手)' if hs > 5 else '(一字)')
        elif i == 1 and zb == top_zb:
            s['角色'] = '🥈换手龙' if hs > 5 else '🥈跟风龙'
        elif zb >= 2:
            s['角色'] = '🔵助攻'
        else:
            s['角色'] = '⚪补涨'
    return sorted_s

def main():
    # 使用3个连续交易日（跳过周末）：4/16、4/17、4/21
    dates = ['20260416', '20260417', '20260421']
    target = '20260421'

    print("Step 1: 采集涨停/炸板数据")
    all_zt, all_zb = fetch_zt_data(dates)

    print("\nStep 2: 查询个股概念标签")
    # 收集所有涨停过的个股代码
    all_codes = set()
    for d, df in all_zt.items():
        if len(df) > 0:
            all_codes.update(df['代码'].tolist())
    print(f"  共{len(all_codes)}只涨停股需查概念")
    concept_map = fetch_stock_concepts(list(all_codes))
    print(f"  完成，平均每股{sum(len(v) for v in concept_map.values())/max(len(concept_map),1):.1f}个概念")

    # 给炸板池也查概念
    zb_codes = set()
    for d, df in all_zb.items():
        if len(df) > 0 and '代码' in df.columns:
            zb_codes.update(df['代码'].tolist())
    zb_codes -= all_codes  # 去掉已查的
    if zb_codes:
        print(f"  补查{len(zb_codes)}只炸板股概念")
        zb_map = fetch_stock_concepts(list(zb_codes))
        concept_map.update(zb_map)

    print("\nStep 3: 按概念构建板块")
    target_zt = all_zt[target]
    target_zb = all_zb.get(target, __import__('pandas').DataFrame())

    # 给每行附加概念
    for idx, row in target_zt.iterrows():
        target_zt.at[idx, '_concepts_str'] = ','.join(concept_map.get(row['代码'], []))

    # 构建概念板块
    concept_blocks = defaultdict(list)
    for _, row in target_zt.iterrows():
        code = row['代码']
        concepts = concept_map.get(code, [])
        stock_dict = row.to_dict()
        stock_dict['_concepts'] = concepts
        for c in concepts:
            concept_blocks[c].append(stock_dict)

    # 炸板按概念统计
    zb_by_concept = Counter()
    if len(target_zb) > 0 and '代码' in target_zb.columns:
        for _, row in target_zb.iterrows():
            code = row['代码']
            for c in concept_map.get(code, []):
                zb_by_concept[c] += 1

    # 多日概念统计
    all_concept_daily = {}
    for d, df in all_zt.items():
        day_concepts = Counter()
        if len(df) > 0:
            for _, row in df.iterrows():
                for c in concept_map.get(row['代码'], []):
                    day_concepts[c] += 1
        all_concept_daily[d] = dict(day_concepts)

    print(f"  共{len(concept_blocks)}个概念板块")

    # 过滤：只保留涨停>=2家的概念
    valid_blocks = {k: v for k, v in concept_blocks.items() if len(v) >= 2}
    print(f"  涨停>=2家的概念: {len(valid_blocks)}个")

    print("\nStep 4: 计算BCI v3.0")

    # === 两轮评分：先初评排序，再用排序结果计算独占度做终评 ===
    # 第一轮：不带独占度修正的初评
    pre_results = []
    for concept, stocks in valid_blocks.items():
        bci, _ = calc_bci_v3(concept, stocks, zb_by_concept, all_concept_daily, dates)
        pre_results.append((concept, bci, stocks))
    pre_results.sort(key=lambda x: x[1], reverse=True)

    # 构建每个概念的股票代码集合（按初评BCI排序，用于独占度计算）
    # 只有BCI排名更高的概念才会"覆盖"当前概念
    concept_stock_codes_ranked = {}
    for concept, bci, stocks in pre_results:
        concept_stock_codes_ranked[concept] = set(s.get('代码', '') for s in stocks)

    # 第二轮：带独占度修正的终评
    results = []
    for rank_idx, (concept, _, stocks) in enumerate(pre_results):
        # 只传入BCI排名更高的概念作为"更强概念"
        stronger_concepts = {c: codes for i, (c, _, _) in enumerate(pre_results)
                           for codes in [concept_stock_codes_ranked[c]]
                           if i < rank_idx}
        bci, detail = calc_bci_v3(concept, stocks, zb_by_concept, all_concept_daily, dates,
                                  concept_stock_codes=stronger_concepts if stronger_concepts else None)
        最高 = max(s.get('连板数', 1) for s in stocks)
        龙头 = max(stocks, key=lambda x: x.get('连板数',1)*1e10 + x.get('封板资金',0))
        max_fund = max(s.get('封板资金', 0) for s in stocks)
        # 计算独占股票数（不属于任何BCI更高概念的股票）
        my_codes = set(s.get('代码', '') for s in stocks)
        stronger_all_codes = set()
        for c, codes in stronger_concepts.items():
            stronger_all_codes |= codes
        unique_codes = my_codes - stronger_all_codes
        results.append({
            'concept': concept,
            'bci': bci,
            'count': len(stocks),
            'unique_count': len(unique_codes),  # 独占涨停数
            'max_zb': 最高,
            'leader': 龙头.get('名称', ''),
            'leader_code': 龙头.get('代码', ''),
            'max_fund': max_fund / 1e8,
            'detail': detail,
            'stocks': stocks,
            'codes': my_codes,
        })

    results.sort(key=lambda x: x['bci'], reverse=True)

    # === 全局去重统计 ===
    all_zt_codes_in_concepts = set()
    for r in results:
        all_zt_codes_in_concepts |= r['codes']
    去重涨停数 = len(all_zt_codes_in_concepts)

    # ====== 生成报告 ======
    print("\nStep 5: 生成报告")
    L = []
    L.append("# BCI板块完整性分析 v3.0 — 概念标签驱动")
    L.append(f"\n> 日期：{target[:4]}-{target[4:6]}-{target[6:]}")
    L.append(f"> 涨停：{len(target_zt)}家 | 炸板：{len(target_zb)}家 | 封板率：{len(target_zt)/(len(target_zt)+len(target_zb))*100:.1f}%")
    L.append(f"> 概念板块：{len(valid_blocks)}个（涨停≥2家）| 去重涨停股：{去重涨停数}家 | 个股概念标签来源：东方财富")
    L.append(f"> 改进要点：v3.1修复重复计数——每个概念标注独占/去重数，BCI含独占度修正")

    # TOP排名
    L.append("\n---\n## 一、概念板块BCI排名（TOP25）\n")
    L.append("| # | 概念方向 | BCI | 涨停 | 独占 | 高度 | 龙头 | 封单 | 趋势 |")
    L.append("|---|---------|-----|------|------|------|------|------|------|")

    for i, r in enumerate(results[:25]):
        d = r['detail']
        趋势 = '📈' if '升温' in d.get('持续','') or '强升' in d.get('持续','') else ('📉' if '降温' in d.get('持续','') else '➡️')
        if r['bci'] >= 78: tag = '⭐5'
        elif r['bci'] >= 65: tag = '⭐4'
        elif r['bci'] >= 50: tag = '⭐3'
        else: tag = '⭐2'
        独占标记 = f"{r['unique_count']}/{r['count']}"
        L.append(f"| {i+1} | **{r['concept']}** | **{r['bci']}** {tag} | {r['count']}家 | {独占标记} | {r['max_zb']}板 | {r['leader']} | {r['max_fund']:.1f}亿 | {趋势} |")

    # TOP板块详解
    L.append("\n---\n## 二、TOP板块梯队详解\n")
    shown = set()
    for r in results[:12]:
        if r['bci'] < 55:
            break
        concept = r['concept']
        if concept in shown:
            continue
        shown.add(concept)

        d = r['detail']
        L.append(f"### 【{concept}】BCI={r['bci']}\n")
        L.append("**评分明细**：" + " | ".join(f"{k}:{v}" for k, v in d.items()))

        roles = analyze_roles(r['stocks'])
        L.append(f"\n| 角色 | 名称(代码) | 板 | 换手 | 封单 | 首封 |")
        L.append("|------|-----------|---|------|------|------|")
        for s in roles[:8]:
            ft = str(s.get('首次封板时间','')).zfill(6)
            ft_fmt = f"{ft[:2]}:{ft[2:4]}:{ft[4:6]}" if len(ft)==6 else ft
            L.append(f"| {s.get('角色','')} | {s.get('名称','')}({s.get('代码','')}) | {s.get('连板数',1)} | {s.get('换手率',0):.1f}% | {s.get('封板资金',0)/1e8:.1f}亿 | {ft_fmt} |")
        L.append("")

    # 主线判定
    L.append("\n---\n## 三、主线方向判定\n")
    L.append("综合BCI排名+涨停数量+持续性+消息面催化，判定当前主线：\n")

    # 按大方向聚合
    大方向 = defaultdict(list)
    方向关键词 = {
        '新能源': ['新能源车','电池技术','锂电池','储能','光伏','逆变器','固态电池','钠离子','燃料电池'],
        'AI/科技': ['5G','通信','数据中心','半导体','国产芯片','华为','玻璃基板','电子'],
        '汽车': ['汽车零部件','汽车','低空经济'],
        '消费': ['电商','内贸','零售','食品'],
    }
    for r in results[:20]:
        c = r['concept']
        matched = False
        for 方向, keywords in 方向关键词.items():
            if any(k in c for k in keywords):
                大方向[方向].append(r)
                matched = True
                break
        if not matched:
            大方向['其他'].append(r)

    for 方向, items in sorted(大方向.items(), key=lambda x: max(r['bci'] for r in x[1]), reverse=True):
        if 方向 == '其他':
            continue
        top_bci = max(r['bci'] for r in items)
        # 去重涨停合计：合并该方向下所有概念的股票代码后去重
        方向所有代码 = set()
        for r in items:
            方向所有代码 |= r['codes']
        去重合计 = len(方向所有代码)
        total_zt_raw = sum(r['count'] for r in items)
        concepts_str = ', '.join(f"{r['concept']}({r['bci']})" for r in sorted(items, key=lambda x: x['bci'], reverse=True)[:5])
        tag = '🏆 核心主线' if top_bci >= 75 and 去重合计 >= 10 else ('🥈 重要方向' if top_bci >= 65 else '📊 辅助方向')
        L.append(f"**{方向}** {tag}")
        L.append(f"- 最高BCI: {top_bci} | 去重涨停: {去重合计}家（概念交叉前{total_zt_raw}家）")
        L.append(f"- 子概念: {concepts_str}")
        L.append("")

    # 操作建议
    L.append("\n---\n## 四、操作建议\n")
    L.append("### 可操作标的（BCI≥55 + 换手板 + 有消息催化）\n")
    L.append("| 概念 | BCI | 角色 | 名称(代码) | 板 | 换手 | 封单 | 操作性 |")
    L.append("|------|-----|------|-----------|---|------|------|--------|")

    seen_stocks = set()
    for r in results:
        if r['bci'] < 55:
            continue
        hot = HOT_CONCEPTS.get(r['concept'], 0)
        if hot == 0:
            continue
        roles = analyze_roles(r['stocks'])
        for s in roles:
            code = s.get('代码', '')
            if code in seen_stocks:
                continue
            if s.get('换手率', 0) < 5:
                continue
            seen_stocks.add(code)
            操作 = '✅可低吸' if s.get('换手率',0) > 8 else '⚠️需确认'
            L.append(f"| {r['concept']} | {r['bci']} | {s.get('角色','')} | {s.get('名称','')}({code}) | {s.get('连板数',1)} | {s.get('换手率',0):.1f}% | {s.get('封板资金',0)/1e8:.1f}亿 | {操作} |")

    # v2→v3对比
    L.append("\n---\n## 五、v2.0→v3.0 关键变化\n")
    L.append("| 对比项 | v2.0 | v3.0 |")
    L.append("|--------|------|------|")
    L.append("| 板块归类 | 行业→概念硬编码(27个) | 个股概念标签(东方财富,50+个) |")
    L.append("| 大东南 | 归入\"塑料\"BCI=46 | 归入新能源车/新材料/光伏等多概念 |")
    L.append("| 德业股份 | 归入\"光伏设备\"3家 | 归入光伏/储能/逆变器/电力设备等 |")
    L.append("| 新能源方向 | 电池4家独立评分 | 电池技术13家+储能10家+光伏9家整体呈现 |")
    L.append("| 板块数量 | 27个(行业) | 50+个(概念),但只展示≥2家的 |")
    L.append("| 内聚度 | 无 | 新增：板块内股票概念交叉度评分(10分) |")
    L.append("| 龙头判定 | 偏重换手率 | 加入市值权重(千亿涨停加分) |")
    L.append("| 重复计数 | 同一股在多概念中重复统计 | v3.1修复：独占度修正+去重统计 |")

    return '\n'.join(L)

if __name__ == '__main__':
    report = main()
    out = '/Users/ecustkiller/WorkBuddy/Claw/reports/bci_analysis/BCI板块完整性分析v3_20260421.md'
    with open(out, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n报告已输出: {out} ({len(report)}字符)")
