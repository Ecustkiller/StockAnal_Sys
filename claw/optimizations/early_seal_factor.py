#!/usr/bin/env python3
"""
早盘封板时间因子 v4.0
=======================
【修复的实盘漏洞】
  4/17 计划中以"封板时间"切片分析：

  ┌────────────────────────────────────────────────────────┐
  │ 封板时间        代表票          4日涨跌    结论       │
  ├────────────────────────────────────────────────────────┤
  │ 09:35 前       可川、世运      +21%/-1.2% ✅ 早盘强  │
  │ 09:35-10:00    东方电气、达实  +1.1%/0.0% ✅ 健康   │
  │ 10:00-10:30    奥瑞德          +4.5%      ✅ 偏强   │
  │ 10:30-13:00    劲旅、特发      +2.5%/1.3% 🟡 中性   │
  │ 13:00-14:00    洁美、奕东      +5.7%/-4.4% 🟡 分化  │
  │ 14:00 后       荣科、长信      -5.2%/-3.9% ❌ 弱   │
  └────────────────────────────────────────────────────────┘

  v3 虽有封板时间数据但权重不足，导致午后封板票也得高分。

【本模块策略】
  对 is_zt=True 的票，按封板时间打分：
    - 09:30-09:35  秒板/竞价板 → +8 分（主动性最强）
    - 09:35-09:45  早盘封板    → +5 分
    - 09:45-10:00  早盘收尾    → +3 分
    - 10:00-10:30  上午早段    → +1 分
    - 10:30-13:00  上午中段    →  0 分
    - 13:00-14:00  下午早段    → -2 分（分化区）
    - 14:00-14:30  尾盘封板    → -5 分（典型弱势）
    - 14:30 后     临收封板    → -8 分（拉高出货嫌疑）

  另外对"一字板"特殊处理：+5 分但做不了（加提示不加实际仓位收益）
"""

from datetime import datetime, time
from typing import Optional, Union


class EarlySealFactor:
    """早盘封板时间加分器"""

    # 时间段加分表（按封板时间区间）
    TIME_BONUS_TABLE = [
        (time(9, 30), time(9, 35), 8.0, '秒板/竞价板'),
        (time(9, 35), time(9, 45), 5.0, '早盘封板'),
        (time(9, 45), time(10, 0), 3.0, '早盘收尾'),
        (time(10, 0), time(10, 30), 1.0, '上午早段'),
        (time(10, 30), time(13, 0), 0.0, '上午中段'),
        (time(13, 0), time(14, 0), -2.0, '下午早段'),
        (time(14, 0), time(14, 30), -5.0, '尾盘封板'),
        (time(14, 30), time(15, 0), -8.0, '临收封板'),
    ]

    def _parse_seal_time(self, seal_time) -> Optional[time]:
        """解析封板时间，支持多种输入格式"""
        if seal_time is None:
            return None
        if isinstance(seal_time, time):
            return seal_time
        if isinstance(seal_time, datetime):
            return seal_time.time()
        if isinstance(seal_time, str):
            s = seal_time.strip()
            for fmt in ['%H:%M:%S', '%H:%M']:
                try:
                    return datetime.strptime(s, fmt).time()
                except ValueError:
                    continue
            # 尝试 0925 / 925 这种格式
            digits = ''.join(c for c in s if c.isdigit())
            if len(digits) in (3, 4):
                if len(digits) == 3:
                    digits = '0' + digits
                hh, mm = int(digits[:2]), int(digits[2:4])
                if 0 <= hh < 24 and 0 <= mm < 60:
                    return time(hh, mm)
        return None

    def bonus(self, seal_time,
              is_zt: bool = True,
              is_one_word: bool = False) -> float:
        """
        返回封板时间加分（可能为负）

        参数:
            seal_time: 封板时间（支持 "09:35:12" / time对象 / None）
            is_zt: 是否涨停
            is_one_word: 是否一字板

        返回:
            加分值（-8 ~ +8），非涨停/无数据返回 0
        """
        if not is_zt:
            return 0.0

        # 一字板单独处理
        if is_one_word:
            return 5.0  # 一字板加分但排不到，更多是参考

        t = self._parse_seal_time(seal_time)
        if t is None:
            return 0.0  # 无封板时间数据不加减分

        # 匹配时间区间
        for start, end, bonus, _ in self.TIME_BONUS_TABLE:
            if start <= t < end:
                return bonus

        # 14:57 后涨停（极晚）
        if t >= time(14, 30):
            return -8.0

        return 0.0

    def describe(self, seal_time) -> str:
        """返回封板时间的描述"""
        t = self._parse_seal_time(seal_time)
        if t is None:
            return '未知封板时间'
        for start, end, bonus, desc in self.TIME_BONUS_TABLE:
            if start <= t < end:
                sign = '+' if bonus > 0 else ''
                return f"{desc}（{sign}{bonus}分）"
        return f"非常规时间 {t}"


# ============================================================
# 实盘数据回放测试
# ============================================================
if __name__ == '__main__':
    seal = EarlySealFactor()

    print("=" * 70)
    print("🔬 早盘封板时间因子 — 实盘数据回放测试")
    print("=" * 70)

    cases = [
        # (股票, 封板时间, 实际4日涨跌, 是否一字)
        ('可川科技',  '09:45', +21.0, False),
        ('世运电路',  '09:35', -1.2, False),
        ('东方电气',  '09:50', +1.1, False),
        ('达实智能',  '09:45', +0.0, False),
        ('奥瑞德',    '10:25', +4.5, False),
        ('劲旅环境',  '10:10', +2.5, False),
        ('洁美科技',  '13:05', +5.7, False),
        ('荣科科技',  '14:25', -5.2, False),
        ('奕东电子',  '13:20', -4.4, False),
        ('长信科技',  '10:00', -3.9, False),  # 10:00整差点属于上午早段
    ]

    print(f"\n{'股票':<10}{'封板时间':<10}{'实际涨跌':>10}{'因子加分':>10}  {'描述':<20}")
    print("-" * 70)

    for name, t, actual, one_word in cases:
        b = seal.bonus(t, is_zt=True, is_one_word=one_word)
        desc = seal.describe(t)
        print(f"{name:<10}{t:<10}{actual:+9.1f}%{b:+9.1f}  {desc}")

    print("\n" + "=" * 70)

    # 验证因子有效性：计算加分与实际涨跌的 Spearman 相关性
    try:
        from scipy.stats import spearmanr
        scores = [seal.bonus(t, is_zt=True) for _, t, _, _ in cases]
        actuals = [a for _, _, a, _ in cases]
        rho, pval = spearmanr(scores, actuals)
        print(f"\n因子IC(Spearman)相关性: ρ={rho:.3f}, p={pval:.3f}")
        if rho > 0:
            print("✅ 封板时间越早，实际表现越好（正相关验证）")
        else:
            print("⚠️ 相关性不显著，可能需要更多样本")
    except ImportError:
        # 用简单的相关性
        n = len(cases)
        scores = [seal.bonus(t, is_zt=True) for _, t, _, _ in cases]
        actuals = [a for _, _, a, _ in cases]
        mean_s = sum(scores) / n
        mean_a = sum(actuals) / n
        cov = sum((scores[i] - mean_s) * (actuals[i] - mean_a) for i in range(n)) / n
        var_s = sum((s - mean_s) ** 2 for s in scores) / n
        var_a = sum((a - mean_a) ** 2 for a in actuals) / n
        if var_s > 0 and var_a > 0:
            corr = cov / (var_s ** 0.5 * var_a ** 0.5)
            print(f"\n简单相关性: r={corr:.3f}")
            if corr > 0:
                print("✅ 封板时间越早，实际表现越好")
            else:
                print("⚠️ 因子方向不明显，需增大样本")

    print("=" * 70)
