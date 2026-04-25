#!/usr/bin/env python3
"""
v4.0 优化模块集成示例 & 端到端 Demo
=======================================
用 4/16 真实场景完整演示：
  - v3 评分结果
  - v4 优化后的评分
  - 两者对比，展示优化效果

运行：
  python3 optimizations/optimization_demo.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from optimizations.sector_oneday_detector import SectorOnedayDetector
from optimizations.zt_continuation_risk import ZtContinuationRisk
from optimizations.early_seal_factor import EarlySealFactor
from optimizations.top_score_reversal import TopScoreReversal
from optimizations.sector_next_day_verify import SectorNextDayVerify


# ============================================================
# 4/16 场景数据（用 4/15 的涨停数据为基础计算 v4 得分）
# ============================================================

# 4/16 所有候选股票的 v3 评分数据（模拟 score_system v3 输出）
V3_CANDIDATES = [
    # (code, name, industry, v3_total, is_zt_4_15, consec_zt, seal_time, r5, r5_to_date)
    # 医药类（4/15 涨停，4/16 前被 v3 高度推荐）
    ('002294', '信立泰',   '化学制药', 92, False, 0, None,     4.7,  -16.5),  # 大阳非涨停
    ('002788', '鹭燕医药', '医药商业', 90, True,  1, '10:15', -12.0, -5.1),
    ('600267', '海正药业', '化学制药', 88, True,  1, '10:30',  9.9,  -1.9),
    ('600572', '康恩贝',   '中成药',  86, True,  1, '09:45',  8.1,  -2.1),
    ('600713', '南京医药', '医药商业', 85, False, 0, None,   10.5,  -9.7),
    ('002589', '瑞康医药', '医药商业', 85, True,  1, '13:20',  5.0,  -4.5),

    # 科技类（4/15 涨停且 4/16 仍强）
    ('603920', '世运电路', '元器件',   99, True,  1, '09:35',  3.1,  -1.2),  # 次日+5后-6
    ('600875', '东方电气', '电气设备', 97, True,  1, '09:50',  9.7,   1.1),
    ('002421', '达实智能', '软件服务', 96, True,  1, '09:45',  5.8,   0.0),
    ('603052', '可川科技', '元器件',   90, True,  1, '09:45', 14.4,  21.0),
    ('000657', '中钨高新', '小金属',   88, True,  1, '10:40',  5.6,  12.8),
    ('001268', '联合精密', '机械基件', 88, True,  1, '10:05',  7.0,   7.9),
    ('002491', '通鼎互联', '通信设备', 87, True,  1, '11:15', -3.2,   6.5),

    # 连续涨停风险票
    ('300000', '某连板妖', '传媒',     85, True,  4, '09:40', 50.0, -15.0),  # 4板妖股，示例
]

# 4/15 vs 4/16 各板块涨停数（真实数据）
SECTOR_ZT_DAY1 = {  # 4/15
    '化学制药': 7, '医药商业': 5, '中成药': 2, '医疗保健': 3,
    '元器件': 8, '电气设备': 5, '软件服务': 6, '通信设备': 4,
    '小金属': 3, '机械基件': 5, '传媒': 3,
}
SECTOR_ZT_DAY2 = {  # 4/16
    '化学制药': 0, '医药商业': 0, '中成药': 0, '医疗保健': 0,
    '元器件': 13, '电气设备': 9, '软件服务': 8, '通信设备': 9,
    '小金属': 3, '机械基件': 5, '传媒': 2,
}


# ============================================================
# 应用 v4 优化
# ============================================================

def apply_v4(df, day1_zt, day2_zt, sector_leaders_next_avg=None):
    """一键应用 v4 全部优化到评分 DataFrame"""
    detector = SectorOnedayDetector()
    zt_risk = ZtContinuationRisk()
    seal = EarlySealFactor()
    reversal = TopScoreReversal()

    sector_leaders_next_avg = sector_leaders_next_avg or {}

    df = df.copy()

    df['oneday_penalty'] = df['industry'].apply(
        lambda s: detector.penalty(s, day1_zt, day2_zt,
                                    sector_leaders_next_avg.get(s))
    )
    df['zt_risk_penalty'] = df.apply(
        lambda r: zt_risk.penalty(r['consec_zt'],
                                   is_zt=r['is_zt_4_15'],
                                   r5=r['r5']), axis=1
    )
    df['early_seal_bonus'] = df.apply(
        lambda r: seal.bonus(r['seal_time'], is_zt=r['is_zt_4_15']), axis=1
    )
    df['reversal_adj'] = reversal.adjust(df['v3_total'])

    df['v4_score'] = (df['v3_total']
                     - df['oneday_penalty']
                     - df['zt_risk_penalty']
                     + df['early_seal_bonus']
                     + df['reversal_adj'])

    return df


def main():
    # 构造 DataFrame
    df = pd.DataFrame(V3_CANDIDATES, columns=[
        'code', 'name', 'industry', 'v3_total',
        'is_zt_4_15', 'consec_zt', 'seal_time', 'r5', 'r5_to_date'
    ])

    # 板块溢价：4/15 涨停票今日均表现
    sector_avg_next = {
        '化学制药': -3.0, '医药商业': -2.0, '中成药': -0.5,
        '元器件': 2.5, '电气设备': 3.0, '软件服务': 2.0, '通信设备': 1.8,
        '小金属': 2.0, '机械基件': 1.5, '传媒': -1.0,
    }

    # 应用 v4 优化
    df_v4 = apply_v4(df, SECTOR_ZT_DAY1, SECTOR_ZT_DAY2, sector_avg_next)

    # 4/16 场景对比
    print("=" * 120)
    print("📊 v3 vs v4 评分对比  —  4/16 场景（基于 4/15 收盘 + 4/16 验证信号）")
    print("=" * 120)

    df_v4 = df_v4.sort_values('v3_total', ascending=False).reset_index(drop=True)

    print(f"\n{'排名':<4}{'代码':<8}{'名称':<8}{'行业':<8}"
          f"{'v3分':>5}{'一日扣':>6}{'连板扣':>6}{'封板加':>6}{'反向':>6}"
          f"{'v4分':>6}{'4日实涨%':>9}")
    print("-" * 120)
    for i, r in df_v4.iterrows():
        print(f"#{i+1:<3}{r['code']:<8}{r['name']:<8}{r['industry']:<8}"
              f"{r['v3_total']:>5.0f}{r['oneday_penalty']:>+6.1f}{-r['zt_risk_penalty']:>+6.1f}"
              f"{r['early_seal_bonus']:>+6.1f}{r['reversal_adj']:>+6.1f}"
              f"{r['v4_score']:>6.1f}{r['r5_to_date']:>+9.1f}")

    # TOP-N 对比
    print("\n" + "=" * 120)
    print("🏆 TOP-N 对比（v3 vs v4，看平均实际涨跌）")
    print("=" * 120)

    df_valid = df_v4[df_v4['r5_to_date'].notna()].copy()
    sorted_v3 = df_valid.sort_values('v3_total', ascending=False).reset_index(drop=True)
    sorted_v4 = df_valid.sort_values('v4_score', ascending=False).reset_index(drop=True)

    for n in [3, 5, 6, 8]:
        v3_top = sorted_v3.head(n)
        v4_top = sorted_v4.head(n)
        v3_avg = v3_top['r5_to_date'].mean()
        v4_avg = v4_top['r5_to_date'].mean()
        v3_win = (v3_top['r5_to_date'] > 0).sum() / n * 100
        v4_win = (v4_top['r5_to_date'] > 0).sum() / n * 100

        print(f"\nTOP-{n}:")
        print(f"  v3: 均涨 {v3_avg:+.2f}%, 胜率 {v3_win:.1f}%  ({', '.join(v3_top['name'].tolist())})")
        print(f"  v4: 均涨 {v4_avg:+.2f}%, 胜率 {v4_win:.1f}%  ({', '.join(v4_top['name'].tolist())})")
        improve = v4_avg - v3_avg
        if improve > 0:
            print(f"  ✅ 改善 {improve:+.2f}% (胜率 {v4_win-v3_win:+.1f}pp)")
        elif improve < 0:
            print(f"  ⚠️ 降低 {improve:+.2f}%")
        else:
            print(f"  ➖ 持平")

    # 关键股票前后变化
    print("\n" + "=" * 120)
    print("🔍 关键股票 v3→v4 变化")
    print("=" * 120)

    key_cases = {
        '信立泰': '医药一日游风险，v3重推荐→v4应降级',
        '南京医药': '医药一日游风险',
        '世运电路': 'v3 TOP1 (99分) 但实际-1.2%，反向修正应降级',
        '可川科技': '实际+21% 大牛，v3只有90分，v4应保留高位',
        '中钨高新': '实际+12.8%，v3仅88分，v4应升级',
        '联合精密': '实际+7.9%，v3仅88分，v4应升级',
        '某连板妖': '4板+超涨，连板扣分应显著',
    }

    for name, desc in key_cases.items():
        row = df_v4[df_v4['name'] == name]
        if len(row) == 0: continue
        r = row.iloc[0]
        v3_rank = (df_v4['v3_total'] > r['v3_total']).sum() + 1
        v4_rank = (df_v4['v4_score'] > r['v4_score']).sum() + 1
        change = v3_rank - v4_rank  # 正值=升
        arrow = '↑' if change > 0 else ('↓' if change < 0 else '→')

        print(f"\n  📍 {name}  ({desc})")
        print(f"     v3分 {r['v3_total']:.0f} 排名 #{v3_rank}")
        print(f"     v4分 {r['v4_score']:.1f} 排名 #{v4_rank}  {arrow} {abs(change)}位")
        print(f"     调整明细: 一日游{r['oneday_penalty']:+.1f} "
              f"连板{-r['zt_risk_penalty']:+.1f} "
              f"封板{r['early_seal_bonus']:+.1f} "
              f"反向{r['reversal_adj']:+.1f}")
        print(f"     实际涨跌: {r['r5_to_date']:+.1f}%")

    print("\n" + "=" * 120)
    print("✅ 集成演示完成！")
    print("=" * 120)


if __name__ == '__main__':
    main()
