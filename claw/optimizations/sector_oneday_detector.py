#!/usr/bin/env python3
"""
板块一日游识别器 v4.0
=====================
【修复的实盘漏洞】
  4/15 医药板块 16 只涨停 → 4/16 仅剩 1 只（衰减率 93.75%）
  → 信立泰 -16.5%、南京医药 -9.7%、鹭燕医药 -5.1% ...
  v3 把"首日爆发"直接升级为"主线"，没有次日验证机制。

【核心原理】
  一日游板块的特征：
  1. 爆发日涨停数 ≥ 5（引人关注）
  2. 次日涨停数 / 爆发日涨停数 < 30%  ← 关键指标
  3. 爆发日涨停票次日平均收益 < 0%
  4. 爆发日高潮龙头次日跌停/大跌

  → 识别后对该板块所有标的**扣 10~20 分**

【数据对比验证】
              爆发日涨停    次日涨停    衰减率      次日均涨    判定
  医药(4/15)   16        1         93.8%    -1.70%      ❌ 一日游
  元器件(4/16) 13        >10       <30%     +2.5%       ✅ 真主线
  通信(4/20)   14        10+       保持     +1.2%       ✅ 真主线
"""

from typing import Optional, Dict


class SectorOnedayDetector:
    """板块一日游识别器"""

    # 衰减率阈值（次日涨停/爆发日涨停 低于该值即判定一日游）
    DECAY_THRESHOLD = 0.30  # <30% 算衰减严重
    DECAY_DANGER = 0.50     # <50% 算衰减偏大
    EXPLODE_MIN = 5         # 爆发日至少 5 只涨停才算"爆发"

    # 次日涨停票的平均收益阈值
    NEXT_DAY_RETURN_BAD = -0.5   # 平均 <-0.5% 视为"溢价失败"
    NEXT_DAY_RETURN_DANGER = 1.0 # 平均 <1% 视为"溢价偏弱"

    def detect(self, sector: str,
               prev_zt: Dict[str, int],
               curr_zt: Dict[str, int],
               avg_next_return: Optional[float] = None) -> dict:
        """
        识别板块是否一日游

        参数:
            sector: 板块名
            prev_zt: 前一日各板块涨停数 {sector: count}
            curr_zt: 当日各板块涨停数 {sector: count}
            avg_next_return: 前日涨停票今日的平均涨跌幅（%）

        返回:
            dict: {
                'is_oneday': bool,      # 是否一日游
                'severity': str,        # 'severe' / 'moderate' / 'normal' / 'healthy'
                'decay_rate': float,    # 衰减率 0~1
                'prev_count': int,
                'curr_count': int,
                'reason': str,
            }
        """
        prev_count = prev_zt.get(sector, 0)
        curr_count = curr_zt.get(sector, 0)

        # 未爆发过，正常板块
        if prev_count < self.EXPLODE_MIN:
            return {
                'is_oneday': False, 'severity': 'normal',
                'decay_rate': 0, 'prev_count': prev_count, 'curr_count': curr_count,
                'reason': '非爆发板块（前日涨停数<5）'
            }

        # 计算衰减率
        decay_rate = 1 - (curr_count / prev_count)

        # 严重衰减：衰减 >70% 且次日涨停<3 → 一日游确认
        if decay_rate >= (1 - self.DECAY_THRESHOLD) and curr_count < 3:
            sev = 'severe'
            is_oneday = True
            reason = f"严重衰减：前日{prev_count}家→今日{curr_count}家（衰减{decay_rate*100:.0f}%）"
        # 中度衰减：衰减 50-70% 或 次日涨停票溢价<-0.5%
        elif decay_rate >= (1 - self.DECAY_DANGER) or (
            avg_next_return is not None and avg_next_return < self.NEXT_DAY_RETURN_BAD
        ):
            sev = 'moderate'
            is_oneday = True
            reason = f"中度衰减：{prev_count}→{curr_count}（衰减{decay_rate*100:.0f}%）" + \
                     (f"+溢价{avg_next_return:.1f}%" if avg_next_return is not None else "")
        # 溢价偏弱：数量维持但赚钱效应差
        elif avg_next_return is not None and avg_next_return < self.NEXT_DAY_RETURN_DANGER:
            sev = 'moderate'
            is_oneday = False  # 还未完全确认一日游，但需警惕
            reason = f"溢价偏弱：今日涨停票次日均涨{avg_next_return:.1f}%（<1%警戒线）"
        # 健康延续
        elif decay_rate < 0.3:  # 衰减<30% = 主线延续
            sev = 'healthy'
            is_oneday = False
            reason = f"主线延续：{prev_count}→{curr_count}（衰减仅{decay_rate*100:.0f}%）"
        else:
            sev = 'normal'
            is_oneday = False
            reason = f"正常回落：{prev_count}→{curr_count}"

        return {
            'is_oneday': is_oneday,
            'severity': sev,
            'decay_rate': decay_rate,
            'prev_count': prev_count,
            'curr_count': curr_count,
            'reason': reason,
        }

    def penalty(self, sector: str,
                prev_zt: Dict[str, int],
                curr_zt: Dict[str, int],
                prev_zt_perf: Optional[float] = None) -> float:
        """
        返回该板块的扣分（0~20分）

        - 一日游严重：扣 20 分（该板块内标的几乎不应入选）
        - 一日游中度：扣 12 分
        - 溢价偏弱：扣 5 分
        - 健康/正常：不扣分
        """
        result = self.detect(sector, prev_zt, curr_zt, prev_zt_perf)

        if result['severity'] == 'severe':
            return 20.0
        elif result['severity'] == 'moderate':
            return 12.0 if result['is_oneday'] else 5.0
        elif result['severity'] == 'healthy':
            return -3.0  # 主线延续反而加分
        return 0.0


# ============================================================
# 便捷函数
# ============================================================
def check_sector_oneday(sector, prev_zt_dict, curr_zt_dict, prev_perf=None):
    """便捷函数：直接返回检测结果"""
    return SectorOnedayDetector().detect(sector, prev_zt_dict, curr_zt_dict, prev_perf)


# ============================================================
# 实盘数据回放测试
# ============================================================
if __name__ == '__main__':
    detector = SectorOnedayDetector()

    print("=" * 70)
    print("🔬 板块一日游识别器 — 实盘数据回放测试")
    print("=" * 70)

    # 测试1：4/15→4/16 医药板块（已知答案：一日游确认）
    prev = {'医药': 16, '化学制药': 7, '医药商业': 5, '中成药': 2,
            '元器件': 8, '电气设备': 5}
    curr = {'医药': 1, '化学制药': 0, '医药商业': 0, '中成药': 0,
            '元器件': 13, '电气设备': 9}

    print("\n【案例1】4/15→4/16 医药板块（真实：一日游）")
    r = detector.detect('医药', prev, curr, avg_next_return=-1.7)
    print(f"  是否一日游: {r['is_oneday']}")
    print(f"  严重程度:   {r['severity']}")
    print(f"  衰减率:     {r['decay_rate']*100:.1f}%")
    print(f"  原因:       {r['reason']}")
    print(f"  扣分:       {detector.penalty('医药', prev, curr, -1.7)}")
    assert r['is_oneday'] and r['severity'] == 'severe', "医药应识别为严重一日游"
    print("  ✅ PASS")

    print("\n【案例2】4/15→4/16 元器件板块（真实：主线延续）")
    r = detector.detect('元器件', prev, curr, avg_next_return=2.5)
    print(f"  是否一日游: {r['is_oneday']}")
    print(f"  严重程度:   {r['severity']}")
    print(f"  衰减率:     {r['decay_rate']*100:.1f}%")
    print(f"  原因:       {r['reason']}")
    assert not r['is_oneday'], "元器件不应被判为一日游"
    print("  ✅ PASS")

    # 测试3：化学制药（医药子方向，7→0 最极端）
    print("\n【案例3】4/15→4/16 化学制药（子方向）")
    r = detector.detect('化学制药', prev, curr, avg_next_return=-3.0)
    print(f"  是否一日游: {r['is_oneday']}")
    print(f"  严重程度:   {r['severity']}")
    print(f"  衰减率:     {r['decay_rate']*100:.1f}%")
    print(f"  扣分:       {detector.penalty('化学制药', prev, curr, -3.0)}")
    assert r['is_oneday'] and r['severity'] == 'severe'
    print("  ✅ PASS")

    print("\n" + "=" * 70)
    print("✅ 所有测试通过！板块一日游识别器工作正常")
    print("=" * 70)
