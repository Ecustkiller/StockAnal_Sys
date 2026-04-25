#!/usr/bin/env python3
"""
连续涨停风险衰减器 v4.0
=========================
【修复的实盘漏洞】
  4/13-14 美能能源连续2涨停 → 4/15 跌停 -10%
  4/13-14 康盛股份连续2涨停 → 4/15 -9.8%
  4/13-14 华远控股连续2涨停 → 4/15 跌停 -10%

  v3 评分系统没有"涨停衰减"惩罚——连板越多反而分越高（"龙头加分"），
  但实盘数据显示：连续2板后第3天跌停概率急剧升高。

【理论基础】
  退学炒股："一鼓作气，再而衰，三而竭"
  北京炒家："连板3板以上不碰，除非是市场最高板龙头"
  元子元：    "高潮次日必是分化"

【本模块的策略】
  对 is_zt=True 且 consecutive_zt>=N 的票，阶梯式扣分：
    1 板（首板）    → 不扣分（首板是买点）
    2 板            → 扣  3 分（警惕）
    3 板            → 扣  8 分（显著风险）
    4 板            → 扣 15 分（高度风险）
    5 板+           → 扣 25 分（极高风险，除非是市场独苗龙头）

  特殊加成（抵消扣分）：
    - 若该股是市场最高板龙头，且同等级板位仅此1只 → 减免 50% 扣分
    - 若5日涨幅 > 40%（明显超涨） → 额外扣 5 分
"""

from typing import Optional


class ZtContinuationRisk:
    """连续涨停风险扣分器"""

    # 阶梯扣分表
    PENALTY_TABLE = {
        0: 0,     # 未涨停
        1: 0,     # 首板 - 不扣分
        2: 3,     # 2 板
        3: 8,     # 3 板
        4: 15,    # 4 板
        5: 25,    # 5 板+
    }

    # 超涨附加扣分阈值
    OVERBOUGHT_5D_THRESHOLD = 40.0   # 5日涨>40% 额外扣5分
    OVERBOUGHT_5D_PENALTY = 5.0

    # 独苗龙头减免
    SOLO_DRAGON_DISCOUNT = 0.5  # 50% 扣分减免

    def penalty(self, consecutive_zt: int,
                is_zt: bool = True,
                r5: Optional[float] = None,
                is_market_top_board: bool = False,
                peers_at_same_level: int = 99) -> float:
        """
        计算连续涨停风险扣分

        参数:
            consecutive_zt: 连续涨停数（含今日）
            is_zt: 今日是否涨停
            r5: 5日涨幅(%)
            is_market_top_board: 是否市场最高板
            peers_at_same_level: 同板位的票数量（若=1=独苗龙头）

        返回:
            扣分值（正数）
        """
        if not is_zt or consecutive_zt <= 0:
            return 0.0

        # 基础扣分
        n = min(consecutive_zt, 5)
        base = self.PENALTY_TABLE.get(n, 25)

        # 超涨附加
        extra = 0
        if r5 is not None and r5 > self.OVERBOUGHT_5D_THRESHOLD:
            extra = self.OVERBOUGHT_5D_PENALTY

        total_penalty = base + extra

        # 独苗龙头减免
        if is_market_top_board and peers_at_same_level <= 1:
            total_penalty *= self.SOLO_DRAGON_DISCOUNT

        return round(total_penalty, 1)

    def risk_level(self, consecutive_zt: int) -> str:
        """返回风险等级描述"""
        if consecutive_zt <= 1:
            return '🟢 低风险（首板）'
        elif consecutive_zt == 2:
            return '🟡 中低风险（2板，警惕分歧）'
        elif consecutive_zt == 3:
            return '🟠 中高风险（3板，减仓信号）'
        elif consecutive_zt == 4:
            return '🔴 高风险（4板，只留底仓）'
        else:
            return '💀 极高风险（5板+，除非独苗龙头否则必减）'

    def position_advice(self, consecutive_zt: int, is_solo_dragon: bool = False) -> float:
        """
        根据连板数给出建议仓位上限（基础仓位的百分比）

        返回: 0.0 ~ 1.0
        """
        if is_solo_dragon:
            # 独苗龙头可按正常仓位
            return max(0.3, 1.0 - consecutive_zt * 0.1)

        table = {0: 1.0, 1: 1.0, 2: 0.7, 3: 0.4, 4: 0.2, 5: 0.0}
        return table.get(min(consecutive_zt, 5), 0.0)


# ============================================================
# 实盘数据回放测试
# ============================================================
if __name__ == '__main__':
    zt_risk = ZtContinuationRisk()

    print("=" * 70)
    print("🔬 连续涨停风险衰减器 — 实盘数据回放测试")
    print("=" * 70)

    # 案例1：美能能源 4/15 已经2板（截至4/14是2板），第3天跌停-10%
    print("\n【案例1】美能能源 4/13-14 连续2涨停后第3日")
    p = zt_risk.penalty(consecutive_zt=2, is_zt=True, r5=22.0)
    print(f"  连板数: 2")
    print(f"  5日涨幅: +22%")
    print(f"  风险扣分: {p}分")
    print(f"  风险等级: {zt_risk.risk_level(2)}")
    print(f"  建议仓位: {zt_risk.position_advice(2)*100:.0f}% of 正常仓位")
    assert p >= 3, "2板至少扣3分"
    print("  ✅ 正确识别为警惕级别")

    # 案例2：华远控股 5板妖股
    print("\n【案例2】华远控股 5板妖股 (超涨60%)")
    p = zt_risk.penalty(consecutive_zt=5, is_zt=True, r5=60.0)
    print(f"  连板数: 5")
    print(f"  5日涨幅: +60%")
    print(f"  风险扣分: {p}分")
    print(f"  风险等级: {zt_risk.risk_level(5)}")
    print(f"  建议仓位: {zt_risk.position_advice(5)*100:.0f}% of 正常仓位")
    assert p >= 25, "5板应重扣"
    print("  ✅ 正确识别为极高风险")

    # 案例3：圣阳股份 4板独苗龙头（4/14后）— 虽连板但应该减免
    print("\n【案例3】圣阳股份 4板独苗龙头")
    p = zt_risk.penalty(consecutive_zt=4, is_zt=True, r5=45.0,
                        is_market_top_board=True, peers_at_same_level=1)
    print(f"  连板数: 4")
    print(f"  5日涨幅: +45%")
    print(f"  独苗龙头: ✅")
    print(f"  风险扣分: {p}分（含独苗减免50%）")
    print(f"  建议仓位: {zt_risk.position_advice(4, is_solo_dragon=True)*100:.0f}%")
    assert p < 15, "独苗龙头应减免"
    print("  ✅ 独苗龙头减免生效")

    # 案例4：首板（不应扣分）
    print("\n【案例4】康恩贝 首板（4/15）")
    p = zt_risk.penalty(consecutive_zt=1, is_zt=True, r5=8.0)
    print(f"  连板数: 1（首板）")
    print(f"  风险扣分: {p}分")
    assert p == 0, "首板不应扣分"
    print("  ✅ 首板不扣分（首板是买点）")

    print("\n" + "=" * 70)
    print("✅ 所有测试通过！连续涨停风险衰减器工作正常")
    print("=" * 70)
