#!/usr/bin/env python3
"""
v4.1 优化整合适配器
=====================
【目标】
无侵入地把 3 个 v4.1 优化模块整合到现有评分结果中：
  1. 板块单日行情过滤（claw.optimizations.sector_oneday_detector）
  2. 龙头地位量化（claw.analysis.stock_leader_quantitative）
  3. 风险收益比优化（claw.analysis.risk_reward_optimizer）

【使用方式】
from claw.scoring.v41_integrator import enhance_results
enhanced = enhance_results(results, trade_date=TARGET, ind_bci_map=ind_bci_map)

【输出字段（在原 results 每条记录上追加）】
- oneday_risk:        单日行情风险等级（无/低/中/高）
- oneday_penalty:     单日行情扣分（0 ~ -10）
- leader_score:       龙头地位得分（0-100）
- leader_level:       龙头等级
- leader_bonus:       龙头加分（0 ~ +10）
- risk_reward_ratio:  风险收益比
- sharpe_ratio:       夏普比率
- rr_bonus:           风险收益加分（-7 ~ +13）
- position_weight:    建议仓位权重（0-1）
- total_v41:          v4.1 调整后最终得分
"""

from __future__ import annotations

from typing import Dict, List, Optional
import traceback

from claw.core.logging import get_logger

log = get_logger("v41_integrator")


# ---- 安全导入三个模块（任一缺失时自动降级） ----
try:
    from claw.optimizations.sector_oneday_detector import SectorOneDayDetector
    _HAS_ONEDAY = True
except Exception as e:
    log.warning(f"sector_oneday_detector 不可用：{e}")
    _HAS_ONEDAY = False

try:
    from claw.analysis.stock_leader_quantitative import (
        StockLeaderAnalyzer,
        integrate_with_scoring_system as leader_to_score,
    )
    _HAS_LEADER = True
except Exception as e:
    log.warning(f"stock_leader_quantitative 不可用：{e}")
    _HAS_LEADER = False

try:
    from claw.analysis.risk_reward_optimizer import (
        RiskRewardOptimizer,
        integrate_with_scoring_system as rr_to_score,
    )
    _HAS_RR = True
except Exception as e:
    log.warning(f"risk_reward_optimizer 不可用：{e}")
    _HAS_RR = False


# ============================================================
# 1) 板块单日行情过滤 → 扣分
# ============================================================
def _apply_oneday_filter(
    results: List[Dict],
    trade_date: str,
    ind_perf_map: Optional[Dict] = None,
) -> None:
    """
    对每条记录追加：oneday_risk, oneday_penalty
    规则：
      - 高风险（单日暴涨型）：-10
      - 中风险（连板不足）：  -5
      - 低风险：             -2
      - 无风险：              0
    """
    if not _HAS_ONEDAY:
        for r in results:
            r["oneday_risk"] = "未检测"
            r["oneday_penalty"] = 0
        return

    try:
        detector = SectorOneDayDetector()
    except Exception as e:
        log.warning(f"SectorOneDayDetector 初始化失败：{e}")
        for r in results:
            r["oneday_risk"] = "初始化失败"
            r["oneday_penalty"] = 0
        return

    # 按板块缓存检测结果，避免重复查询
    sector_cache: Dict[str, Dict] = {}

    for r in results:
        ind = r.get("ind", "")
        if not ind:
            r["oneday_risk"] = "无板块"
            r["oneday_penalty"] = 0
            continue

        if ind not in sector_cache:
            try:
                sector_cache[ind] = detector.detect(ind, trade_date)
            except Exception as e:
                log.debug(f"板块单日检测失败 {ind}: {e}")
                sector_cache[ind] = {"risk_level": "未知", "score": 0}

        d = sector_cache[ind]
        risk_level = d.get("risk_level", "未知")

        if risk_level in ("高", "高风险"):
            penalty = -10
        elif risk_level in ("中", "中风险"):
            penalty = -5
        elif risk_level in ("低", "低风险"):
            penalty = -2
        else:
            penalty = 0

        r["oneday_risk"] = risk_level
        r["oneday_penalty"] = penalty


# ============================================================
# 2) 龙头地位量化 → 加分
# ============================================================
def _apply_leader_score(results: List[Dict], trade_date: str) -> None:
    """
    对每条记录追加：leader_score, leader_level, leader_bonus
    """
    if not _HAS_LEADER:
        for r in results:
            r["leader_score"] = 0
            r["leader_level"] = "未检测"
            r["leader_bonus"] = 0
        return

    try:
        analyzer = StockLeaderAnalyzer()
    except Exception as e:
        log.warning(f"StockLeaderAnalyzer 初始化失败：{e}")
        for r in results:
            r["leader_score"] = 0
            r["leader_level"] = "初始化失败"
            r["leader_bonus"] = 0
        return

    # 只对涨停 / 准涨停 / 连板股运行龙头分析（性能考虑）
    for r in results:
        code = r.get("code", "")
        ind = r.get("ind", "")

        # 仅对疑似龙头的股票运行（涨停或 raw_total>=90）
        is_zt = r.get("is_zt", False)
        raw_total = r.get("raw_total", 0)
        if not (is_zt or raw_total >= 90):
            r["leader_score"] = 0
            r["leader_level"] = "不适用"
            r["leader_bonus"] = 0
            continue

        try:
            res = analyzer.get_leader_score(code, trade_date, ind)
            lscore = res.get("total_score", 0.0)
            r["leader_score"] = round(lscore, 1)
            r["leader_level"] = res.get("leader_level", "未知")
            r["leader_bonus"] = leader_to_score(lscore)
        except Exception as e:
            log.debug(f"龙头分析失败 {code}: {e}")
            r["leader_score"] = 0
            r["leader_level"] = "失败"
            r["leader_bonus"] = 0


# ============================================================
# 3) 风险收益比 → 加/减分 + 仓位建议
# ============================================================
def _apply_risk_reward(results: List[Dict], trade_date: str) -> None:
    """
    对每条记录追加：risk_reward_ratio, sharpe_ratio, rr_bonus, position_weight
    """
    if not _HAS_RR:
        for r in results:
            r["risk_reward_ratio"] = 0
            r["sharpe_ratio"] = 0
            r["rr_bonus"] = 0
            r["position_weight"] = 0
        return

    try:
        optimizer = RiskRewardOptimizer(lookback_days=60)
    except Exception as e:
        log.warning(f"RiskRewardOptimizer 初始化失败：{e}")
        for r in results:
            r["risk_reward_ratio"] = 0
            r["sharpe_ratio"] = 0
            r["rr_bonus"] = 0
            r["position_weight"] = 0
        return

    # 性能保护：默认只跑 top 60，其余置 0
    TOP_N = 60
    ranked = sorted(results, key=lambda x: x.get("total", 0), reverse=True)
    run_codes = set(r["code"] for r in ranked[:TOP_N])

    for r in results:
        code = r.get("code", "")
        if code not in run_codes:
            r["risk_reward_ratio"] = 0
            r["sharpe_ratio"] = 0
            r["rr_bonus"] = 0
            r["position_weight"] = 0
            continue

        try:
            p = optimizer.calculate_risk_reward_profile(code, trade_date)
            rr = p.get("risk_reward_ratio", 0.0)
            sr = p.get("sharpe_ratio", 0.0)
            r["risk_reward_ratio"] = round(rr, 2)
            r["sharpe_ratio"] = round(sr, 2)
            r["rr_bonus"] = rr_to_score(rr, sr)
            r["position_weight"] = round(p.get("position_weight", 0.0), 4)
        except Exception as e:
            log.debug(f"风险收益比失败 {code}: {e}")
            r["risk_reward_ratio"] = 0
            r["sharpe_ratio"] = 0
            r["rr_bonus"] = 0
            r["position_weight"] = 0


# ============================================================
# 对外主入口
# ============================================================
def enhance_results(
    results: List[Dict],
    trade_date: str,
    ind_bci_map: Optional[Dict] = None,
    enable_oneday: bool = True,
    enable_leader: bool = True,
    enable_rr: bool = True,
) -> List[Dict]:
    """
    无侵入地为 results 追加 v4.1 优化字段，并计算 total_v41。

    参数：
        results:     score_system / elite_picker 输出的结果列表
        trade_date:  交易日期（如 "20260420"）
        ind_bci_map: 板块 BCI 得分映射（可选）
        enable_*:    各优化模块开关

    返回：
        同一 results 列表（原地修改）+ 排序后引用
    """
    if not results:
        return results

    log.info(f"v4.1 增强开始：{len(results)} 条，trade_date={trade_date}")

    if enable_oneday:
        try:
            _apply_oneday_filter(results, trade_date)
        except Exception:
            log.error(f"单日行情过滤异常：\n{traceback.format_exc()}")

    if enable_leader:
        try:
            _apply_leader_score(results, trade_date)
        except Exception:
            log.error(f"龙头地位量化异常：\n{traceback.format_exc()}")

    if enable_rr:
        try:
            _apply_risk_reward(results, trade_date)
        except Exception:
            log.error(f"风险收益比异常：\n{traceback.format_exc()}")

    # ---- 计算 v4.1 最终得分 ----
    for r in results:
        base = r.get("total", r.get("raw_total", 0))
        adj = (
            r.get("oneday_penalty", 0)
            + r.get("leader_bonus", 0)
            + r.get("rr_bonus", 0)
        )
        r["total_v41"] = round(base + adj, 2)
        r["v41_adjust"] = round(adj, 2)

    results.sort(key=lambda x: x["total_v41"], reverse=True)

    # ---- 统计摘要 ----
    n_high_risk = sum(1 for r in results if r.get("oneday_risk") in ("高", "高风险"))
    n_leader = sum(1 for r in results if r.get("leader_level") == "龙头")
    n_good_rr = sum(1 for r in results if r.get("risk_reward_ratio", 0) >= 2.0)

    log.info(
        f"v4.1 增强完成：单日高风险板块 {n_high_risk} 只，"
        f"明确龙头 {n_leader} 只，优质风险收益比 {n_good_rr} 只"
    )

    return results


# ============================================================
# CLI 独立调试入口
# ============================================================
if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    if len(sys.argv) < 3:
        print("Usage: python v41_integrator.py <results.json> <trade_date>")
        sys.exit(1)

    path = Path(sys.argv[1])
    date = sys.argv[2]

    with open(path, "r") as f:
        rs = json.load(f)

    out = enhance_results(rs, date)

    out_path = path.with_name(path.stem + "_v41.json")
    with open(out_path, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ v4.1 增强结果已写入：{out_path}")
    print(f"TOP10 (按 total_v41)：")
    for r in out[:10]:
        print(
            f"  {r.get('name','?')}({r.get('code','?')[:6]}) "
            f"total={r.get('total',0)} → total_v41={r.get('total_v41',0)} "
            f"[Δ={r.get('v41_adjust',0):+}] "
            f"单日={r.get('oneday_risk','-')} "
            f"龙头={r.get('leader_level','-')} "
            f"R/R={r.get('risk_reward_ratio',0)}"
        )
