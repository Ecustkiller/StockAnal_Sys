#!/usr/bin/env python3
"""
板块次日封板率验证器 v4.0
==========================
【修复的实盘漏洞】
  4/15 医药板块 16 只涨停→报告直接升级为"新主线"
  4/16 医药仅剩 1 只涨停（衰减 93.75%）
  4/16 的 0416 计划按"医药主线"配置重仓 → 亏损 -3.64%

  v3 的主线识别逻辑：
    if 当日板块涨停 >= 5 then 主线

  v4 改进：主线标签必须经过"次日验证"才能生效

【验证规则】
  新方向首次爆发（当日涨停≥5）时，暂时只打"候选主线"标签，
  不直接升级为"主线"。需满足次日其中一条才升级：

  ✅ 条件1（数量验证）：
     次日涨停数 / 当日涨停数 >= 50%

  ✅ 条件2（溢价验证）：
     当日涨停票的次日平均涨跌 >= +0.5%

  ✅ 条件3（龙头验证）：
     当日龙头（涨幅最高的涨停票）次日未跌停、未暴跌<-3%

  满足任一即升级；全部不满足则**降级为"一日游"**并启动扣分。

【配合 sector_oneday_detector 使用】
  本模块主要产出"状态标签"（候选/确认/一日游），
  sector_oneday_detector 产出"扣分"。
  两者数据口径一致，形成完整链路。

【状态流转】
  无信号 ──(涨停≥5)──> 候选主线 ──(次日验证通过)──> 确认主线
                           │
                           └────(次日验证失败)────> 一日游 (扣分)
  确认主线 ──(数量跌>70%)──> 降级回调
"""

from typing import Optional, Dict, List
from enum import Enum


class SectorStatus(Enum):
    """板块状态"""
    UNKNOWN = 'unknown'             # 未激活
    CANDIDATE = 'candidate'          # 候选主线（首日爆发）
    CONFIRMED = 'confirmed'          # 确认主线（次日验证通过）
    ONEDAY = 'oneday'                # 一日游（次日验证失败）
    COOLING = 'cooling'              # 降温中（确认主线后持续走弱）


class SectorNextDayVerify:
    """板块次日封板率验证器"""

    # 阈值
    EXPLODE_MIN = 5          # 至少5只涨停算"爆发"
    NEXT_ZT_RATIO_MIN = 0.50 # 次日涨停数/首日 >= 50% = 数量验证通过
    NEXT_RETURN_MIN = 0.5    # 首日涨停票次日均涨>=0.5% = 溢价通过
    LEADER_DROP_MAX = -3.0   # 龙头跌<-3% = 溢价失败

    # 冷却阈值
    COOLING_DECAY = 0.30     # 数量跌超30%算降温

    def verify(self,
               sector: str,
               day1_zt_count: int,
               day2_zt_count: int,
               day1_leaders_day2_avg_return: Optional[float] = None,
               day1_leader_day2_return: Optional[float] = None) -> dict:
        """
        主验证函数

        参数:
            sector: 板块名
            day1_zt_count: 爆发日（T日）涨停数
            day2_zt_count: 次日（T+1日）涨停数
            day1_leaders_day2_avg_return: T 日所有涨停票在 T+1 的平均涨跌%
            day1_leader_day2_return: T 日龙头（涨幅最大）在 T+1 的涨跌%

        返回:
            dict: {
                'sector': str,
                'status': SectorStatus,
                'verdict': str,       # 人类可读判定
                'rules_passed': list, # 通过的验证规则
                'should_trade': bool, # 是否可做
            }
        """
        # 1) 还没爆发过
        if day1_zt_count < self.EXPLODE_MIN:
            return self._build_result(sector, SectorStatus.UNKNOWN,
                                      '未爆发', [], should_trade=False)

        # 2) 尚无次日数据 → 候选主线
        if day2_zt_count is None:
            return self._build_result(sector, SectorStatus.CANDIDATE,
                                      f'首日爆发{day1_zt_count}只→候选主线，待次日验证',
                                      [], should_trade=False)  # 候选不立即重仓

        # 3) 次日验证规则
        passed = []

        # 规则1：数量验证
        ratio = day2_zt_count / day1_zt_count if day1_zt_count > 0 else 0
        if ratio >= self.NEXT_ZT_RATIO_MIN:
            passed.append(f'数量✅({day2_zt_count}/{day1_zt_count}={ratio*100:.0f}%)')

        # 规则2：溢价验证
        if day1_leaders_day2_avg_return is not None:
            if day1_leaders_day2_avg_return >= self.NEXT_RETURN_MIN:
                passed.append(f'溢价✅({day1_leaders_day2_avg_return:+.1f}%)')

        # 规则3：龙头验证
        if day1_leader_day2_return is not None:
            if day1_leader_day2_return > self.LEADER_DROP_MAX:
                passed.append(f'龙头✅({day1_leader_day2_return:+.1f}%)')

        # 4) 结论
        if len(passed) >= 2 or (len(passed) >= 1 and ratio >= 0.7):
            # 至少2条通过，或单条但数量>70%
            status = SectorStatus.CONFIRMED
            verdict = f"次日验证通过 ({'; '.join(passed)}) → 确认主线"
            should_trade = True
        elif len(passed) == 1:
            # 只有1条通过 → 降温/谨慎
            status = SectorStatus.COOLING
            verdict = f"次日仅 1 条通过 → 降温，谨慎参与"
            should_trade = False
        else:
            # 全部失败 → 一日游
            status = SectorStatus.ONEDAY
            ratio_str = f"({day2_zt_count}/{day1_zt_count}={ratio*100:.0f}%)"
            verdict = f"次日全面失败 {ratio_str} → 一日游，回避"
            should_trade = False

        return self._build_result(sector, status, verdict, passed, should_trade)

    def _build_result(self, sector, status, verdict, passed, should_trade):
        return {
            'sector': sector,
            'status': status.value,
            'verdict': verdict,
            'rules_passed': passed,
            'should_trade': should_trade,
        }

    def batch_verify(self, sectors_data: List[dict]) -> List[dict]:
        """批量验证多个板块

        参数:
            sectors_data: [
                {'sector': str, 'day1_zt': int, 'day2_zt': int,
                 'day1_leaders_day2_avg': float,
                 'day1_leader_day2': float}, ...
            ]
        """
        return [self.verify(
            sector=d['sector'],
            day1_zt_count=d['day1_zt'],
            day2_zt_count=d.get('day2_zt'),
            day1_leaders_day2_avg_return=d.get('day1_leaders_day2_avg'),
            day1_leader_day2_return=d.get('day1_leader_day2')
        ) for d in sectors_data]


# ============================================================
# 实盘数据回放测试
# ============================================================
if __name__ == '__main__':
    verifier = SectorNextDayVerify()

    print("=" * 78)
    print("🔬 板块次日封板率验证器 — 实盘数据回放")
    print("=" * 78)

    # 批量数据：4/15 → 4/16 多个板块
    sectors_data = [
        # 医药板块（实际：一日游）
        {'sector': '医药', 'day1_zt': 16, 'day2_zt': 1,
         'day1_leaders_day2_avg': -1.7, 'day1_leader_day2': -10.0},  # 信立泰跌停
        {'sector': '化学制药', 'day1_zt': 7, 'day2_zt': 0,
         'day1_leaders_day2_avg': -3.0, 'day1_leader_day2': -10.0},
        {'sector': '医药商业', 'day1_zt': 5, 'day2_zt': 0,
         'day1_leaders_day2_avg': -2.0, 'day1_leader_day2': -1.6},

        # 元器件（实际：真主线，次日 5→13）
        {'sector': '元器件', 'day1_zt': 8, 'day2_zt': 13,
         'day1_leaders_day2_avg': 2.5, 'day1_leader_day2': 8.0},

        # 电气设备
        {'sector': '电气设备', 'day1_zt': 5, 'day2_zt': 9,
         'day1_leaders_day2_avg': 3.0, 'day1_leader_day2': 10.0},

        # 首日爆发但数据不全（仅当日数据）
        {'sector': '低空经济', 'day1_zt': 5},
    ]

    results = verifier.batch_verify(sectors_data)

    print(f"\n{'板块':<12}{'D1涨停':>8}{'D2涨停':>8}{'状态':<15}{'判定':<45}")
    print("-" * 95)
    for d, r in zip(sectors_data, results):
        d2 = d.get('day2_zt', '-')
        print(f"{r['sector']:<12}{d['day1_zt']:>8}{str(d2):>8}  {r['status']:<14}{r['verdict']}")

    # 可操作性汇总
    print("\n📊 可操作性汇总")
    print("-" * 78)
    tradable = [r for r in results if r['should_trade']]
    not_tradable = [r for r in results if not r['should_trade']]
    print(f"  ✅ 可操作板块 ({len(tradable)}):")
    for r in tradable:
        print(f"     - {r['sector']}: {r['verdict']}")
    print(f"  ❌ 不可操作板块 ({len(not_tradable)}):")
    for r in not_tradable:
        print(f"     - {r['sector']}: {r['status']}")

    # 断言测试
    print("\n🧪 关键断言测试")
    print("-" * 78)
    med = next(r for r in results if r['sector'] == '医药')
    elec = next(r for r in results if r['sector'] == '元器件')
    low = next(r for r in results if r['sector'] == '低空经济')

    assert med['status'] == 'oneday', '医药应识别为一日游'
    print(f"  ✅ 医药 → {med['status']} (一日游识别)")
    assert elec['status'] == 'confirmed', '元器件应识别为确认主线'
    print(f"  ✅ 元器件 → {elec['status']} (主线确认)")
    assert low['status'] == 'candidate', '低空经济应为候选主线'
    print(f"  ✅ 低空经济 → {low['status']} (待次日验证)")

    print("\n" + "=" * 78)
    print("✅ 全部测试通过！板块次日验证器工作正常")
    print("=" * 78)
