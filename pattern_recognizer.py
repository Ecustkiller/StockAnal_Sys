# -*- coding: utf-8 -*-
"""
技术形态识别引擎
================
自动识别K线技术形态，包括：
- 头肩顶/底 (Head & Shoulders)
- 双顶/底 (Double Top/Bottom)
- 三角形整理 (Triangle: 对称/上升/下降)
- 旗形 (Flag)
- 楔形 (Wedge: 上升/下降)
- 缺口 (Gap: 突破/持续/衰竭)
- W底/M顶
- 圆弧底/顶

每个形态返回：名称、方向(看涨/看跌)、完成度、目标价位、可靠性评分

创建时间: 2026-04-25
"""

import logging
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict


@dataclass
class PatternResult:
    """形态识别结果"""
    name: str                    # 形态名称
    direction: str               # 'bullish' / 'bearish' / 'neutral'
    completion: float            # 完成度 0-100%
    reliability: float           # 可靠性评分 0-100
    target_price: Optional[float] = None   # 目标价位
    stop_loss: Optional[float] = None      # 止损价位
    entry_price: Optional[float] = None    # 建议入场价
    description: str = ''        # 形态描述
    key_points: List[Dict] = field(default_factory=list)  # 关键点位 [{index, price, label}]
    neckline: Optional[float] = None       # 颈线价位
    start_index: int = 0         # 形态起始索引
    end_index: int = 0           # 形态结束索引

    def to_dict(self):
        return asdict(self)


class PatternRecognizer:
    """K线技术形态识别引擎"""

    def __init__(self, min_pattern_bars: int = 10, max_pattern_bars: int = 120):
        """
        参数:
            min_pattern_bars: 形态最少K线数
            max_pattern_bars: 形态最多K线数
        """
        self.logger = logging.getLogger(__name__)
        self.min_bars = min_pattern_bars
        self.max_bars = max_pattern_bars

    def analyze(self, df: pd.DataFrame) -> Dict:
        """
        综合形态分析入口

        参数:
            df: 包含 open/high/low/close/volume 列的DataFrame

        返回:
            {
                'patterns': [PatternResult, ...],
                'summary': str,
                'dominant_signal': 'bullish'/'bearish'/'neutral',
                'signal_strength': 0-100
            }
        """
        if df is None or len(df) < self.min_bars:
            return {
                'patterns': [],
                'summary': '数据不足，无法进行形态分析',
                'dominant_signal': 'neutral',
                'signal_strength': 0
            }

        try:
            # 确保数据类型正确
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            patterns = []

            # 检测各类形态
            patterns.extend(self._detect_head_shoulders(df))
            patterns.extend(self._detect_double_top_bottom(df))
            patterns.extend(self._detect_triangles(df))
            patterns.extend(self._detect_wedges(df))
            patterns.extend(self._detect_flags(df))
            patterns.extend(self._detect_gaps(df))
            patterns.extend(self._detect_rounding(df))

            # 按可靠性排序
            patterns.sort(key=lambda p: p.reliability, reverse=True)

            # 去重（保留可靠性最高的）
            patterns = self._deduplicate_patterns(patterns)

            # 生成综合信号
            dominant_signal, signal_strength = self._calculate_dominant_signal(patterns)

            # 生成文本摘要
            summary = self._generate_summary(patterns, dominant_signal, signal_strength)

            return {
                'patterns': [p.to_dict() for p in patterns],
                'summary': summary,
                'dominant_signal': dominant_signal,
                'signal_strength': signal_strength
            }

        except Exception as e:
            self.logger.error(f"形态分析出错: {e}")
            return {
                'patterns': [],
                'summary': f'形态分析出错: {str(e)}',
                'dominant_signal': 'neutral',
                'signal_strength': 0
            }

    # ==================== 辅助方法 ====================

    def _find_local_extrema(self, series: np.ndarray, order: int = 5) -> Tuple[List[int], List[int]]:
        """
        找到局部极大值和极小值的索引

        参数:
            series: 价格序列
            order: 比较窗口大小（前后各order个点）

        返回:
            (maxima_indices, minima_indices)
        """
        maxima = []
        minima = []
        n = len(series)

        for i in range(order, n - order):
            # 局部极大值
            if all(series[i] >= series[i - j] for j in range(1, order + 1)) and \
               all(series[i] >= series[i + j] for j in range(1, order + 1)):
                maxima.append(i)
            # 局部极小值
            if all(series[i] <= series[i - j] for j in range(1, order + 1)) and \
               all(series[i] <= series[i + j] for j in range(1, order + 1)):
                minima.append(i)

        return maxima, minima

    def _linear_regression(self, x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
        """简单线性回归，返回 (斜率, 截距)"""
        n = len(x)
        if n < 2:
            return 0.0, y[0] if len(y) > 0 else 0.0
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        ss_xy = np.sum((x - x_mean) * (y - y_mean))
        ss_xx = np.sum((x - x_mean) ** 2)
        if ss_xx == 0:
            return 0.0, y_mean
        slope = ss_xy / ss_xx
        intercept = y_mean - slope * x_mean
        return slope, intercept

    def _trendline_fit_quality(self, x: np.ndarray, y: np.ndarray, slope: float, intercept: float) -> float:
        """计算趋势线拟合质量 R²"""
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        if ss_tot == 0:
            return 1.0
        return max(0, 1 - ss_res / ss_tot)

    # ==================== 头肩顶/底 ====================

    def _detect_head_shoulders(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测头肩顶和头肩底形态"""
        results = []
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        n = len(df)

        for order in [5, 8, 12]:
            maxima, minima = self._find_local_extrema(highs, order)
            _, minima_low = self._find_local_extrema(lows, order)

            # 头肩顶：需要3个极大值（左肩、头、右肩）+ 2个极小值（颈线）
            if len(maxima) >= 3 and len(minima) >= 2:
                for i in range(len(maxima) - 2):
                    ls_idx, h_idx, rs_idx = maxima[i], maxima[i + 1], maxima[i + 2]
                    ls_price, h_price, rs_price = highs[ls_idx], highs[h_idx], highs[rs_idx]

                    # 头部必须高于两肩
                    if h_price <= ls_price or h_price <= rs_price:
                        continue

                    # 两肩高度接近（差异<头部高度的30%）
                    shoulder_diff = abs(ls_price - rs_price)
                    head_height = h_price - min(ls_price, rs_price)
                    if head_height == 0 or shoulder_diff / head_height > 0.30:
                        continue

                    # 找颈线（两肩之间的低点）
                    neck_candidates = [m for m in minima if ls_idx < m < rs_idx]
                    if not neck_candidates:
                        continue
                    neckline_price = min(lows[m] for m in neck_candidates)

                    # 形态宽度检查
                    pattern_width = rs_idx - ls_idx
                    if pattern_width < self.min_bars or pattern_width > self.max_bars:
                        continue

                    # 计算完成度
                    current_price = closes[-1]
                    if current_price < neckline_price:
                        completion = 100.0  # 已跌破颈线
                    else:
                        completion = min(95, (h_price - current_price) / (h_price - neckline_price) * 100)

                    # 目标价位 = 颈线 - (头部 - 颈线)
                    target = neckline_price - (h_price - neckline_price)

                    # 可靠性评分
                    reliability = 60
                    if shoulder_diff / head_height < 0.15:
                        reliability += 10  # 两肩对称性好
                    if completion >= 100:
                        reliability += 15  # 已确认突破
                    if pattern_width >= 20:
                        reliability += 10  # 形态足够宽
                    # 成交量确认：头部缩量
                    if 'volume' in df.columns and h_idx > 0:
                        vol_head = df['volume'].iloc[h_idx]
                        vol_ls = df['volume'].iloc[ls_idx]
                        if vol_head < vol_ls:
                            reliability += 5  # 头部缩量确认

                    reliability = min(95, reliability)

                    results.append(PatternResult(
                        name='头肩顶',
                        direction='bearish',
                        completion=round(completion, 1),
                        reliability=round(reliability, 1),
                        target_price=round(target, 2),
                        stop_loss=round(h_price * 1.02, 2),
                        entry_price=round(neckline_price, 2),
                        neckline=round(neckline_price, 2),
                        description=f'头肩顶形态，头部{h_price:.2f}，颈线{neckline_price:.2f}，'
                                    f'目标价{target:.2f}',
                        key_points=[
                            {'index': int(ls_idx), 'price': round(float(ls_price), 2), 'label': '左肩'},
                            {'index': int(h_idx), 'price': round(float(h_price), 2), 'label': '头部'},
                            {'index': int(rs_idx), 'price': round(float(rs_price), 2), 'label': '右肩'},
                        ],
                        start_index=int(ls_idx),
                        end_index=int(min(rs_idx + order, n - 1))
                    ))

            # 头肩底（反转形态）
            if len(minima_low) >= 3 and len(maxima) >= 2:
                for i in range(len(minima_low) - 2):
                    ls_idx, h_idx, rs_idx = minima_low[i], minima_low[i + 1], minima_low[i + 2]
                    ls_price, h_price, rs_price = lows[ls_idx], lows[h_idx], lows[rs_idx]

                    # 头部必须低于两肩
                    if h_price >= ls_price or h_price >= rs_price:
                        continue

                    shoulder_diff = abs(ls_price - rs_price)
                    head_depth = max(ls_price, rs_price) - h_price
                    if head_depth == 0 or shoulder_diff / head_depth > 0.30:
                        continue

                    neck_candidates = [m for m in maxima if ls_idx < m < rs_idx]
                    if not neck_candidates:
                        continue
                    neckline_price = max(highs[m] for m in neck_candidates)

                    pattern_width = rs_idx - ls_idx
                    if pattern_width < self.min_bars or pattern_width > self.max_bars:
                        continue

                    current_price = closes[-1]
                    if current_price > neckline_price:
                        completion = 100.0
                    else:
                        completion = min(95, (current_price - h_price) / (neckline_price - h_price) * 100)

                    target = neckline_price + (neckline_price - h_price)

                    reliability = 60
                    if shoulder_diff / head_depth < 0.15:
                        reliability += 10
                    if completion >= 100:
                        reliability += 15
                    if pattern_width >= 20:
                        reliability += 10
                    reliability = min(95, reliability)

                    results.append(PatternResult(
                        name='头肩底',
                        direction='bullish',
                        completion=round(completion, 1),
                        reliability=round(reliability, 1),
                        target_price=round(target, 2),
                        stop_loss=round(h_price * 0.98, 2),
                        entry_price=round(neckline_price, 2),
                        neckline=round(neckline_price, 2),
                        description=f'头肩底形态，头部{h_price:.2f}，颈线{neckline_price:.2f}，'
                                    f'目标价{target:.2f}',
                        key_points=[
                            {'index': int(ls_idx), 'price': round(float(ls_price), 2), 'label': '左肩'},
                            {'index': int(h_idx), 'price': round(float(h_price), 2), 'label': '头部'},
                            {'index': int(rs_idx), 'price': round(float(rs_price), 2), 'label': '右肩'},
                        ],
                        start_index=int(ls_idx),
                        end_index=int(min(rs_idx + order, n - 1))
                    ))

        return results

    # ==================== 双顶/双底 ====================

    def _detect_double_top_bottom(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测双顶(M顶)和双底(W底)形态"""
        results = []
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        n = len(df)

        for order in [5, 8, 12]:
            maxima, _ = self._find_local_extrema(highs, order)
            _, minima = self._find_local_extrema(lows, order)

            # 双顶 (M顶)
            if len(maxima) >= 2:
                for i in range(len(maxima) - 1):
                    p1_idx, p2_idx = maxima[i], maxima[i + 1]
                    p1_price, p2_price = highs[p1_idx], highs[p2_idx]

                    # 两个顶部价格接近（差异<5%）
                    avg_price = (p1_price + p2_price) / 2
                    if avg_price == 0:
                        continue
                    price_diff_pct = abs(p1_price - p2_price) / avg_price * 100
                    if price_diff_pct > 5:
                        continue

                    pattern_width = p2_idx - p1_idx
                    if pattern_width < self.min_bars // 2 or pattern_width > self.max_bars:
                        continue

                    # 找中间低点（颈线）
                    mid_lows = [m for m in minima if p1_idx < m < p2_idx]
                    if not mid_lows:
                        # 用区间最低点
                        mid_idx = p1_idx + np.argmin(lows[p1_idx:p2_idx + 1])
                        neckline = lows[mid_idx]
                    else:
                        mid_idx = min(mid_lows, key=lambda m: lows[m])
                        neckline = lows[mid_idx]

                    # 颈线不能太接近顶部
                    height = avg_price - neckline
                    if height <= 0 or height / avg_price < 0.02:
                        continue

                    current_price = closes[-1]
                    if current_price < neckline:
                        completion = 100.0
                    else:
                        completion = min(95, (avg_price - current_price) / height * 100)

                    target = neckline - height
                    reliability = 55
                    if price_diff_pct < 2:
                        reliability += 10
                    if completion >= 100:
                        reliability += 15
                    if pattern_width >= 15:
                        reliability += 10
                    # 第二个顶部成交量缩小
                    if 'volume' in df.columns:
                        if df['volume'].iloc[p2_idx] < df['volume'].iloc[p1_idx]:
                            reliability += 10
                    reliability = min(90, reliability)

                    results.append(PatternResult(
                        name='双顶(M顶)',
                        direction='bearish',
                        completion=round(completion, 1),
                        reliability=round(reliability, 1),
                        target_price=round(target, 2),
                        stop_loss=round(max(p1_price, p2_price) * 1.02, 2),
                        entry_price=round(neckline, 2),
                        neckline=round(neckline, 2),
                        description=f'双顶形态，顶部均价{avg_price:.2f}，颈线{neckline:.2f}，'
                                    f'目标价{target:.2f}',
                        key_points=[
                            {'index': int(p1_idx), 'price': round(float(p1_price), 2), 'label': '第一顶'},
                            {'index': int(mid_idx), 'price': round(float(neckline), 2), 'label': '颈线'},
                            {'index': int(p2_idx), 'price': round(float(p2_price), 2), 'label': '第二顶'},
                        ],
                        start_index=int(p1_idx),
                        end_index=int(min(p2_idx + order, n - 1))
                    ))

            # 双底 (W底)
            if len(minima) >= 2:
                for i in range(len(minima) - 1):
                    p1_idx, p2_idx = minima[i], minima[i + 1]
                    p1_price, p2_price = lows[p1_idx], lows[p2_idx]

                    avg_price = (p1_price + p2_price) / 2
                    if avg_price == 0:
                        continue
                    price_diff_pct = abs(p1_price - p2_price) / avg_price * 100
                    if price_diff_pct > 5:
                        continue

                    pattern_width = p2_idx - p1_idx
                    if pattern_width < self.min_bars // 2 or pattern_width > self.max_bars:
                        continue

                    mid_highs = [m for m in maxima if p1_idx < m < p2_idx]
                    if not mid_highs:
                        mid_idx = p1_idx + np.argmax(highs[p1_idx:p2_idx + 1])
                        neckline = highs[mid_idx]
                    else:
                        mid_idx = max(mid_highs, key=lambda m: highs[m])
                        neckline = highs[mid_idx]

                    height = neckline - avg_price
                    if height <= 0 or height / neckline < 0.02:
                        continue

                    current_price = closes[-1]
                    if current_price > neckline:
                        completion = 100.0
                    else:
                        completion = min(95, (current_price - avg_price) / height * 100)

                    target = neckline + height
                    reliability = 55
                    if price_diff_pct < 2:
                        reliability += 10
                    if completion >= 100:
                        reliability += 15
                    if pattern_width >= 15:
                        reliability += 10
                    if 'volume' in df.columns:
                        if df['volume'].iloc[p2_idx] > df['volume'].iloc[p1_idx]:
                            reliability += 10  # 第二底放量
                    reliability = min(90, reliability)

                    results.append(PatternResult(
                        name='双底(W底)',
                        direction='bullish',
                        completion=round(completion, 1),
                        reliability=round(reliability, 1),
                        target_price=round(target, 2),
                        stop_loss=round(min(p1_price, p2_price) * 0.98, 2),
                        entry_price=round(neckline, 2),
                        neckline=round(neckline, 2),
                        description=f'双底形态，底部均价{avg_price:.2f}，颈线{neckline:.2f}，'
                                    f'目标价{target:.2f}',
                        key_points=[
                            {'index': int(p1_idx), 'price': round(float(p1_price), 2), 'label': '第一底'},
                            {'index': int(mid_idx), 'price': round(float(neckline), 2), 'label': '颈线'},
                            {'index': int(p2_idx), 'price': round(float(p2_price), 2), 'label': '第二底'},
                        ],
                        start_index=int(p1_idx),
                        end_index=int(min(p2_idx + order, n - 1))
                    ))

        return results

    # ==================== 三角形整理 ====================

    def _detect_triangles(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测三角形整理形态（对称三角形、上升三角形、下降三角形）"""
        results = []
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        n = len(df)

        for order in [5, 8]:
            maxima, minima = self._find_local_extrema(highs, order)
            _, minima_low = self._find_local_extrema(lows, order)

            # 需要至少2个高点和2个低点
            if len(maxima) < 2 or len(minima_low) < 2:
                continue

            # 取最近的几个极值点
            recent_maxima = maxima[-4:] if len(maxima) >= 4 else maxima
            recent_minima = minima_low[-4:] if len(minima_low) >= 4 else minima_low

            if len(recent_maxima) < 2 or len(recent_minima) < 2:
                continue

            # 高点趋势线
            x_max = np.array(recent_maxima, dtype=float)
            y_max = np.array([highs[i] for i in recent_maxima])
            slope_max, intercept_max = self._linear_regression(x_max, y_max)
            r2_max = self._trendline_fit_quality(x_max, y_max, slope_max, intercept_max)

            # 低点趋势线
            x_min = np.array(recent_minima, dtype=float)
            y_min = np.array([lows[i] for i in recent_minima])
            slope_min, intercept_min = self._linear_regression(x_min, y_min)
            r2_min = self._trendline_fit_quality(x_min, y_min, slope_min, intercept_min)

            # 拟合质量检查
            if r2_max < 0.5 or r2_min < 0.5:
                continue

            # 判断三角形类型
            avg_price = np.mean(closes[-20:])
            slope_threshold = avg_price * 0.0005  # 斜率阈值

            pattern_start = min(recent_maxima[0], recent_minima[0])
            pattern_end = max(recent_maxima[-1], recent_minima[-1])
            pattern_width = pattern_end - pattern_start

            if pattern_width < self.min_bars:
                continue

            # 两条线必须收敛
            range_start = highs[pattern_start] - lows[pattern_start] if pattern_start < n else 0
            range_end = (slope_max * pattern_end + intercept_max) - (slope_min * pattern_end + intercept_min)
            if range_end >= range_start:
                continue  # 不收敛，不是三角形

            if abs(slope_max) < slope_threshold and slope_min > slope_threshold:
                # 上升三角形：上边平，下边上升
                triangle_type = '上升三角形'
                direction = 'bullish'
                reliability_base = 65
            elif slope_max < -slope_threshold and abs(slope_min) < slope_threshold:
                # 下降三角形：上边下降，下边平
                triangle_type = '下降三角形'
                direction = 'bearish'
                reliability_base = 65
            elif slope_max < -slope_threshold and slope_min > slope_threshold:
                # 对称三角形：上边下降，下边上升
                triangle_type = '对称三角形'
                direction = 'neutral'
                reliability_base = 55
            else:
                continue

            # 计算目标价位
            height = y_max[0] - y_min[0]  # 三角形入口高度
            current_price = closes[-1]
            upper_line = slope_max * (n - 1) + intercept_max
            lower_line = slope_min * (n - 1) + intercept_min

            if direction == 'bullish':
                target = upper_line + height * 0.618
                stop_loss = lower_line * 0.98
            elif direction == 'bearish':
                target = lower_line - height * 0.618
                stop_loss = upper_line * 1.02
            else:
                # 对称三角形，根据当前价格位置判断
                if current_price > (upper_line + lower_line) / 2:
                    target = upper_line + height * 0.5
                    stop_loss = lower_line * 0.98
                else:
                    target = lower_line - height * 0.5
                    stop_loss = upper_line * 1.02

            # 完成度（越接近三角形顶点，完成度越高）
            convergence_point = pattern_start + (range_start / (range_start - range_end + 0.001)) * pattern_width
            if convergence_point > pattern_start:
                completion = min(95, (n - 1 - pattern_start) / (convergence_point - pattern_start) * 100)
            else:
                completion = 50

            reliability = reliability_base
            if r2_max > 0.8 and r2_min > 0.8:
                reliability += 15
            if pattern_width >= 20:
                reliability += 10
            reliability = min(90, reliability)

            results.append(PatternResult(
                name=triangle_type,
                direction=direction,
                completion=round(max(0, completion), 1),
                reliability=round(reliability, 1),
                target_price=round(target, 2),
                stop_loss=round(stop_loss, 2),
                entry_price=round(upper_line if direction == 'bullish' else lower_line, 2),
                description=f'{triangle_type}整理，上轨斜率{slope_max:.4f}，下轨斜率{slope_min:.4f}，'
                            f'收敛度{completion:.0f}%',
                key_points=[
                    {'index': int(recent_maxima[0]), 'price': round(float(y_max[0]), 2), 'label': '上轨起点'},
                    {'index': int(recent_maxima[-1]), 'price': round(float(y_max[-1]), 2), 'label': '上轨终点'},
                    {'index': int(recent_minima[0]), 'price': round(float(y_min[0]), 2), 'label': '下轨起点'},
                    {'index': int(recent_minima[-1]), 'price': round(float(y_min[-1]), 2), 'label': '下轨终点'},
                ],
                start_index=int(pattern_start),
                end_index=int(pattern_end)
            ))

        return results

    # ==================== 楔形 ====================

    def _detect_wedges(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测上升楔形和下降楔形"""
        results = []
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        n = len(df)

        for order in [5, 8]:
            maxima, _ = self._find_local_extrema(highs, order)
            _, minima = self._find_local_extrema(lows, order)

            if len(maxima) < 3 or len(minima) < 3:
                continue

            recent_maxima = maxima[-4:]
            recent_minima = minima[-4:]

            x_max = np.array(recent_maxima, dtype=float)
            y_max = np.array([highs[i] for i in recent_maxima])
            slope_max, intercept_max = self._linear_regression(x_max, y_max)
            r2_max = self._trendline_fit_quality(x_max, y_max, slope_max, intercept_max)

            x_min = np.array(recent_minima, dtype=float)
            y_min = np.array([lows[i] for i in recent_minima])
            slope_min, intercept_min = self._linear_regression(x_min, y_min)
            r2_min = self._trendline_fit_quality(x_min, y_min, slope_min, intercept_min)

            if r2_max < 0.6 or r2_min < 0.6:
                continue

            avg_price = np.mean(closes[-20:])
            slope_threshold = avg_price * 0.0003

            # 上升楔形：两条线都向上，但收敛（上轨斜率 < 下轨斜率）
            if slope_max > slope_threshold and slope_min > slope_threshold and slope_max < slope_min:
                wedge_type = '上升楔形'
                direction = 'bearish'  # 上升楔形通常看跌
            # 下降楔形：两条线都向下，但收敛（上轨斜率 > 下轨斜率，即下降更慢）
            elif slope_max < -slope_threshold and slope_min < -slope_threshold and slope_max > slope_min:
                wedge_type = '下降楔形'
                direction = 'bullish'  # 下降楔形通常看涨
            else:
                continue

            pattern_start = min(recent_maxima[0], recent_minima[0])
            pattern_end = max(recent_maxima[-1], recent_minima[-1])
            pattern_width = pattern_end - pattern_start

            if pattern_width < self.min_bars:
                continue

            height = y_max[0] - y_min[0]
            current_price = closes[-1]

            if direction == 'bearish':
                target = current_price - height * 0.618
                stop_loss = (slope_max * (n - 1) + intercept_max) * 1.02
            else:
                target = current_price + height * 0.618
                stop_loss = (slope_min * (n - 1) + intercept_min) * 0.98

            completion = min(90, pattern_width / self.max_bars * 100 + 30)
            reliability = 55
            if r2_max > 0.8 and r2_min > 0.8:
                reliability += 15
            if pattern_width >= 20:
                reliability += 10
            reliability = min(85, reliability)

            results.append(PatternResult(
                name=wedge_type,
                direction=direction,
                completion=round(completion, 1),
                reliability=round(reliability, 1),
                target_price=round(target, 2),
                stop_loss=round(stop_loss, 2),
                description=f'{wedge_type}，上轨斜率{slope_max:.4f}，下轨斜率{slope_min:.4f}',
                key_points=[
                    {'index': int(recent_maxima[0]), 'price': round(float(y_max[0]), 2), 'label': '上轨起点'},
                    {'index': int(recent_maxima[-1]), 'price': round(float(y_max[-1]), 2), 'label': '上轨终点'},
                    {'index': int(recent_minima[0]), 'price': round(float(y_min[0]), 2), 'label': '下轨起点'},
                    {'index': int(recent_minima[-1]), 'price': round(float(y_min[-1]), 2), 'label': '下轨终点'},
                ],
                start_index=int(pattern_start),
                end_index=int(pattern_end)
            ))

        return results

    # ==================== 旗形 ====================

    def _detect_flags(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测旗形（上升旗形和下降旗形）"""
        results = []
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        n = len(df)

        if n < 30:
            return results

        # 寻找旗杆（急涨/急跌段）+ 旗面（小幅整理段）
        for lookback in [30, 50, 70]:
            if n < lookback:
                continue

            segment = closes[-lookback:]

            # 找旗杆：前半段的最大涨跌幅
            half = lookback // 2
            first_half = segment[:half]
            second_half = segment[half:]

            # 上升旗形：先急涨（旗杆），后小幅回调整理（旗面）
            pole_change = (first_half[-1] - first_half[0]) / first_half[0] * 100
            flag_change = (second_half[-1] - second_half[0]) / second_half[0] * 100 if second_half[0] != 0 else 0
            flag_range = (max(second_half) - min(second_half)) / np.mean(second_half) * 100 if np.mean(second_half) != 0 else 0

            start_idx = n - lookback

            if pole_change > 8 and flag_change < 0 and abs(flag_change) < pole_change * 0.5 and flag_range < 10:
                # 上升旗形
                pole_height = first_half[-1] - first_half[0]
                target = second_half[-1] + pole_height
                stop_loss = min(second_half) * 0.98

                completion = min(90, len(second_half) / 20 * 100)
                reliability = 50
                if abs(flag_change) < pole_change * 0.3:
                    reliability += 15  # 回调幅度小
                if flag_range < 5:
                    reliability += 10  # 整理幅度小
                reliability = min(85, reliability)

                results.append(PatternResult(
                    name='上升旗形',
                    direction='bullish',
                    completion=round(completion, 1),
                    reliability=round(reliability, 1),
                    target_price=round(target, 2),
                    stop_loss=round(stop_loss, 2),
                    description=f'上升旗形，旗杆涨幅{pole_change:.1f}%，旗面回调{flag_change:.1f}%',
                    start_index=int(start_idx),
                    end_index=int(n - 1)
                ))

            elif pole_change < -8 and flag_change > 0 and abs(flag_change) < abs(pole_change) * 0.5 and flag_range < 10:
                # 下降旗形
                pole_height = abs(first_half[0] - first_half[-1])
                target = second_half[-1] - pole_height
                stop_loss = max(second_half) * 1.02

                completion = min(90, len(second_half) / 20 * 100)
                reliability = 50
                if abs(flag_change) < abs(pole_change) * 0.3:
                    reliability += 15
                if flag_range < 5:
                    reliability += 10
                reliability = min(85, reliability)

                results.append(PatternResult(
                    name='下降旗形',
                    direction='bearish',
                    completion=round(completion, 1),
                    reliability=round(reliability, 1),
                    target_price=round(target, 2),
                    stop_loss=round(stop_loss, 2),
                    description=f'下降旗形，旗杆跌幅{pole_change:.1f}%，旗面反弹{flag_change:.1f}%',
                    start_index=int(start_idx),
                    end_index=int(n - 1)
                ))

        return results

    # ==================== 缺口 ====================

    def _detect_gaps(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测缺口（突破缺口、持续缺口、衰竭缺口）"""
        results = []
        n = len(df)

        if n < 5:
            return results

        # 只检测最近30根K线的缺口
        check_range = min(30, n - 1)
        closes = df['close'].values
        current_price = closes[-1]

        for i in range(n - check_range, n):
            prev_high = df['high'].iloc[i - 1]
            prev_low = df['low'].iloc[i - 1]
            curr_high = df['high'].iloc[i]
            curr_low = df['low'].iloc[i]
            curr_close = df['close'].iloc[i]

            gap_size = 0
            gap_type = ''
            direction = ''

            # 向上缺口：当日最低 > 前日最高
            if curr_low > prev_high:
                gap_size = curr_low - prev_high
                gap_pct = gap_size / prev_high * 100
                if gap_pct < 0.5:
                    continue  # 忽略太小的缺口
                direction = 'bullish'

                # 判断缺口类型
                days_from_gap = n - 1 - i
                if days_from_gap <= 3:
                    gap_type = '向上跳空缺口'
                elif current_price > curr_close:
                    gap_type = '持续缺口(向上)'
                else:
                    gap_type = '衰竭缺口(向上)'

            # 向下缺口：当日最高 < 前日最低
            elif curr_high < prev_low:
                gap_size = prev_low - curr_high
                gap_pct = gap_size / prev_low * 100
                if gap_pct < 0.5:
                    continue
                direction = 'bearish'

                days_from_gap = n - 1 - i
                if days_from_gap <= 3:
                    gap_type = '向下跳空缺口'
                elif current_price < curr_close:
                    gap_type = '持续缺口(向下)'
                else:
                    gap_type = '衰竭缺口(向下)'
            else:
                continue

            # 检查缺口是否已被回补
            filled = False
            if direction == 'bullish':
                for j in range(i + 1, n):
                    if df['low'].iloc[j] <= prev_high:
                        filled = True
                        break
            else:
                for j in range(i + 1, n):
                    if df['high'].iloc[j] >= prev_low:
                        filled = True
                        break

            reliability = 45
            if not filled:
                reliability += 20  # 未回补的缺口更有意义
            if gap_pct > 2:
                reliability += 10  # 大缺口
            if '衰竭' in gap_type:
                # 衰竭缺口方向反转
                direction = 'bearish' if 'bullish' in direction else 'bullish'
                reliability += 5

            gap_low = min(prev_high, curr_low) if direction == 'bullish' else min(curr_high, prev_low)
            gap_high = max(prev_high, curr_low) if direction == 'bullish' else max(curr_high, prev_low)

            results.append(PatternResult(
                name=gap_type,
                direction=direction,
                completion=100.0 if not filled else 50.0,
                reliability=round(min(80, reliability), 1),
                target_price=None,
                description=f'{gap_type}，缺口大小{gap_pct:.2f}%，'
                            f'缺口区间[{gap_low:.2f}, {gap_high:.2f}]，'
                            f'{"未回补" if not filled else "已回补"}',
                key_points=[
                    {'index': int(i - 1), 'price': round(float(prev_high if direction == 'bullish' else prev_low), 2),
                     'label': '缺口下沿' if direction == 'bullish' else '缺口上沿'},
                    {'index': int(i), 'price': round(float(curr_low if direction == 'bullish' else curr_high), 2),
                     'label': '缺口上沿' if direction == 'bullish' else '缺口下沿'},
                ],
                start_index=int(i - 1),
                end_index=int(i)
            ))

        return results

    # ==================== 圆弧底/顶 ====================

    def _detect_rounding(self, df: pd.DataFrame) -> List[PatternResult]:
        """检测圆弧底和圆弧顶"""
        results = []
        closes = df['close'].values
        n = len(df)

        for window in [30, 50, 80]:
            if n < window:
                continue

            segment = closes[-window:]
            x = np.arange(window, dtype=float)

            # 用二次多项式拟合
            try:
                coeffs = np.polyfit(x, segment, 2)
            except Exception:
                continue

            a, b, c = coeffs
            y_fit = np.polyval(coeffs, x)
            residuals = segment - y_fit
            ss_res = np.sum(residuals ** 2)
            ss_tot = np.sum((segment - np.mean(segment)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            if r2 < 0.6:
                continue

            current_price = closes[-1]
            vertex_x = -b / (2 * a) if a != 0 else window / 2

            if a > 0 and 0 < vertex_x < window:
                # 圆弧底（U形）
                vertex_price = np.polyval(coeffs, vertex_x)
                height = current_price - vertex_price
                if height <= 0 or height / vertex_price < 0.03:
                    continue

                target = current_price + height * 0.618
                completion = min(95, (x[-1] - vertex_x) / (window - vertex_x) * 100) if vertex_x < window else 50

                reliability = 50
                if r2 > 0.8:
                    reliability += 15
                if window >= 50:
                    reliability += 10  # 大周期更可靠
                reliability = min(85, reliability)

                results.append(PatternResult(
                    name='圆弧底',
                    direction='bullish',
                    completion=round(max(0, completion), 1),
                    reliability=round(reliability, 1),
                    target_price=round(target, 2),
                    stop_loss=round(vertex_price * 0.97, 2),
                    description=f'圆弧底形态（{window}日），底部约{vertex_price:.2f}，'
                                f'拟合R²={r2:.3f}',
                    start_index=int(n - window),
                    end_index=int(n - 1)
                ))

            elif a < 0 and 0 < vertex_x < window:
                # 圆弧顶（倒U形）
                vertex_price = np.polyval(coeffs, vertex_x)
                height = vertex_price - current_price
                if height <= 0 or height / vertex_price < 0.03:
                    continue

                target = current_price - height * 0.618
                completion = min(95, (x[-1] - vertex_x) / (window - vertex_x) * 100) if vertex_x < window else 50

                reliability = 50
                if r2 > 0.8:
                    reliability += 15
                if window >= 50:
                    reliability += 10
                reliability = min(85, reliability)

                results.append(PatternResult(
                    name='圆弧顶',
                    direction='bearish',
                    completion=round(max(0, completion), 1),
                    reliability=round(reliability, 1),
                    target_price=round(target, 2),
                    stop_loss=round(vertex_price * 1.03, 2),
                    description=f'圆弧顶形态（{window}日），顶部约{vertex_price:.2f}，'
                                f'拟合R²={r2:.3f}',
                    start_index=int(n - window),
                    end_index=int(n - 1)
                ))

        return results

    # ==================== 综合处理 ====================

    def _deduplicate_patterns(self, patterns: List[PatternResult]) -> List[PatternResult]:
        """去重：同类型形态只保留可靠性最高的"""
        seen = {}
        for p in patterns:
            key = p.name
            if key not in seen or p.reliability > seen[key].reliability:
                seen[key] = p
        return list(seen.values())

    def _calculate_dominant_signal(self, patterns: List[PatternResult]) -> Tuple[str, float]:
        """计算主导信号方向和强度"""
        if not patterns:
            return 'neutral', 0

        bullish_score = 0
        bearish_score = 0

        for p in patterns:
            weight = p.reliability * (p.completion / 100)
            if p.direction == 'bullish':
                bullish_score += weight
            elif p.direction == 'bearish':
                bearish_score += weight

        total = bullish_score + bearish_score
        if total == 0:
            return 'neutral', 0

        if bullish_score > bearish_score:
            return 'bullish', min(100, round(bullish_score / total * 100))
        elif bearish_score > bullish_score:
            return 'bearish', min(100, round(bearish_score / total * 100))
        else:
            return 'neutral', 50

    def _generate_summary(self, patterns: List[PatternResult], dominant: str, strength: float) -> str:
        """生成文本摘要"""
        if not patterns:
            return '未检测到明显的技术形态'

        direction_text = {'bullish': '看涨', 'bearish': '看跌', 'neutral': '中性'}
        parts = [f'共检测到 {len(patterns)} 个技术形态，主导信号：{direction_text.get(dominant, "中性")}（强度{strength:.0f}%）']

        for p in patterns[:5]:  # 最多展示5个
            dir_text = direction_text.get(p.direction, '中性')
            parts.append(
                f'  • {p.name}（{dir_text}）- 完成度{p.completion:.0f}%，可靠性{p.reliability:.0f}分'
                f'{f"，目标价{p.target_price}" if p.target_price else ""}'
            )

        return '\n'.join(parts)
