#!/usr/bin/env python3
"""
多周期共振评分选股器 v2.0
升级内容：
1. 四种状态：上涨/下跌/停顿/破坏（v1只有3种）
2. Fibonacci支撑阻力位计算（382/50/618/886）
3. 走势类型判定（标准趋势/类趋势/停顿/破坏/扩散）
4. 安全边际过滤（5日涨幅<15% + 10日涨幅<25%）
"""
import sys
sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
from Ashare import get_price
from MyTT import *
import numpy as np
import pandas as pd
import time, json, warnings, requests
warnings.filterwarnings('ignore')

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def ts_api(api, fields='', **params):
    d = {'api_name': api, 'token': TOKEN, 'params': params, 'fields': fields}
    try:
        r = requests.post('http://api.tushare.pro', json=d, timeout=15)
        j = r.json()
        if j.get('data'): return j['data']
    except: pass
    return {'items':[], 'fields':[]}


# ==================== 核心函数 ====================

def find_swing_points(highs, lows, closes, window=5):
    """寻找波峰波谷（TDS核心）"""
    peaks = []   # (index, price)
    troughs = [] # (index, price)
    n = len(highs)
    
    for i in range(window, n - window):
        # 波峰：高点是window内最高
        if highs[i] == max(highs[i-window:i+window+1]):
            peaks.append((i, highs[i]))
        # 波谷：低点是window内最低
        if lows[i] == min(lows[i-window:i+window+1]):
            troughs.append((i, lows[i]))
    
    return peaks, troughs


def calc_fibonacci(high, low):
    """计算Fibonacci关键位"""
    diff = high - low
    return {
        '0%': low,
        '23.6%': low + diff * 0.236,
        '38.2%': low + diff * 0.382,
        '50%': low + diff * 0.500,
        '61.8%': low + diff * 0.618,
        '78.6%': low + diff * 0.786,
        '88.6%': low + diff * 0.886,
        '100%': high,
        # 扩展位
        '127.2%': low + diff * 1.272,
        '161.8%': low + diff * 1.618,
    }


def judge_trend_v2(closes, highs, lows, volumes, period_name):
    """
    v2趋势判定：四种状态 上涨/下跌/停顿/破坏
    
    破坏判定：
    - 价格跌破前一波谷（上升趋势破坏）
    - 价格突破前一波峰（下降趋势破坏）
    - 结构破坏≠趋势破坏（关键区分！）
    """
    n = len(closes)
    if n < 25:
        return "停顿", 0, {}
    
    c = np.array(closes, dtype=float)
    h = np.array(highs, dtype=float)
    l = np.array(lows, dtype=float)
    v = np.array(volumes, dtype=float)
    
    # 根据周期选择参数
    if period_name == "大周期":
        ma_period, lookback = 20, 10
    elif period_name == "中周期":
        ma_period, lookback = 10, 5
    else:  # 小周期
        ma_period, lookback = 5, 3
    
    # 均线
    ma_vals = pd.Series(c).rolling(ma_period).mean().values
    last_ma = ma_vals[-1]
    if np.isnan(last_ma):
        return "停顿", 0, {}
    
    prev_ma = ma_vals[-lookback] if not np.isnan(ma_vals[-lookback]) else last_ma
    ma_up = last_ma > prev_ma * 1.001
    ma_down = last_ma < prev_ma * 0.999
    
    # 价格位置
    price_above = c[-1] > last_ma
    price_below = c[-1] < last_ma
    
    # MACD
    ema12 = pd.Series(c).ewm(span=12, adjust=False).mean().values
    ema26 = pd.Series(c).ewm(span=26, adjust=False).mean().values
    dif_arr = ema12 - ema26
    dea_arr = pd.Series(dif_arr).ewm(span=9, adjust=False).mean().values
    macd_bull = dif_arr[-1] > dea_arr[-1]
    macd_bear = dif_arr[-1] < dea_arr[-1]
    
    # 波峰波谷（用于破坏判定）
    peaks, troughs = find_swing_points(h, l, c, window=3)
    
    # 基础趋势判定
    bull_count = sum([ma_up, price_above, macd_bull])
    bear_count = sum([ma_down, price_below, macd_bear])
    
    # === 破坏检测 ===
    is_broken = False
    break_type = None
    
    if len(troughs) >= 2 and len(peaks) >= 1:
        last_trough = troughs[-1][1]
        prev_trough = troughs[-2][1] if len(troughs) >= 2 else troughs[-1][1]
        last_peak = peaks[-1][1]
        
        # 上升趋势中：如果最新价跌破前一个波谷 = 结构破坏
        if prev_trough > 0 and c[-1] < prev_trough * 0.99:
            if bull_count >= 1:  # 之前是偏多的
                is_broken = True
                break_type = "上升结构破坏"
        
        # 下降趋势中：如果最新价突破前一个波峰 = 结构破坏
        if len(peaks) >= 2:
            prev_peak = peaks[-2][1]
            if prev_peak > 0 and c[-1] > prev_peak * 1.01:
                if bear_count >= 1:  # 之前是偏空的
                    is_broken = True
                    break_type = "下降结构破坏"
    
    # 动量
    chg = (c[-1] / c[max(0, -ma_period-1)] - 1) * 100 if n > ma_period else 0
    
    detail = {
        'ma_up': bool(ma_up), 'price_above': bool(price_above), 
        'macd_bull': bool(macd_bull), 'chg': round(chg, 1),
        'ma_val': round(last_ma, 2), 'close': round(c[-1], 2),
    }
    
    if is_broken:
        detail['break_type'] = break_type
        return "破坏", detail.get('chg', 0), detail
    elif bull_count >= 2:
        return "上涨", detail, detail
    elif bear_count >= 2:
        return "下跌", detail, detail
    else:
        return "停顿", detail, detail


def judge_structure_type(closes, highs, lows):
    """
    走势类型判定（5种）：
    1. 标准趋势：高点更高+低点更高（上升）或反之（下降）
    2. 类趋势：方向对但力度弱（三浪不打满1号位）
    3. 停顿：收敛/横盘
    4. 破坏：结构被打破
    5. 扩散：波动放大，高点更高且低点更低
    """
    c = np.array(closes, dtype=float)
    h = np.array(highs, dtype=float)
    l = np.array(lows, dtype=float)
    
    if len(c) < 20:
        return "未知", {}
    
    peaks, troughs = find_swing_points(h, l, c, window=3)
    
    if len(peaks) < 2 or len(troughs) < 2:
        return "停顿", {"reason": "波峰波谷不足"}
    
    # 取最近3个波峰和3个波谷
    recent_peaks = peaks[-3:] if len(peaks) >= 3 else peaks
    recent_troughs = troughs[-3:] if len(troughs) >= 3 else troughs
    
    # 波峰方向
    if len(recent_peaks) >= 2:
        peak_rising = recent_peaks[-1][1] > recent_peaks[-2][1]
        peak_falling = recent_peaks[-1][1] < recent_peaks[-2][1]
    else:
        peak_rising = peak_falling = False
    
    # 波谷方向
    if len(recent_troughs) >= 2:
        trough_rising = recent_troughs[-1][1] > recent_troughs[-2][1]
        trough_falling = recent_troughs[-1][1] < recent_troughs[-2][1]
    else:
        trough_rising = trough_falling = False
    
    # 波动幅度变化
    if len(recent_peaks) >= 2 and len(recent_troughs) >= 2:
        prev_range = recent_peaks[-2][1] - recent_troughs[-2][1]
        curr_range = recent_peaks[-1][1] - recent_troughs[-1][1]
        range_expanding = curr_range > prev_range * 1.2
        range_contracting = curr_range < prev_range * 0.7
    else:
        range_expanding = range_contracting = False
    
    # 回调深度（Fibonacci检测）
    if len(recent_peaks) >= 1 and len(recent_troughs) >= 2:
        last_peak = recent_peaks[-1][1]
        last_trough = recent_troughs[-1][1]
        prev_trough = recent_troughs[-2][1]
        if last_peak > prev_trough:
            retrace = (last_peak - last_trough) / (last_peak - prev_trough)
        else:
            retrace = 0
    else:
        retrace = 0
    
    detail = {
        'peak_rising': peak_rising, 'trough_rising': trough_rising,
        'range_expanding': range_expanding, 'range_contracting': range_contracting,
        'retrace_depth': round(retrace, 3),
    }
    
    # 判定
    if peak_rising and trough_rising:
        if retrace < 0.5:
            return "标准上升趋势", detail  # 强势回调<50%
        elif retrace < 0.786:
            return "类趋势(上升偏弱)", detail  # 回调深但未破
        else:
            return "破坏(上升)", detail  # 回调>78.6%接近破坏
    elif peak_falling and trough_falling:
        return "标准下降趋势", detail
    elif peak_rising and trough_falling:
        return "扩散", detail  # 高更高+低更低
    elif range_contracting:
        return "停顿(收敛)", detail
    elif not peak_rising and not peak_falling:
        return "停顿(横盘)", detail
    else:
        return "停顿", detail


def calc_fib_levels(closes, highs, lows):
    """计算当前股票的Fibonacci支撑阻力位"""
    h = np.array(highs, dtype=float)
    l = np.array(lows, dtype=float)
    
    if len(h) < 10:
        return {}
    
    # 找近期波段高低点
    peaks, troughs = find_swing_points(h, l, np.array(closes, dtype=float), window=3)
    
    if not peaks or not troughs:
        # 用近30日最高最低
        period_high = max(h[-30:]) if len(h) >= 30 else max(h)
        period_low = min(l[-30:]) if len(l) >= 30 else min(l)
    else:
        period_high = peaks[-1][1]
        period_low = troughs[-1][1] if troughs[-1][0] < peaks[-1][0] else (
            troughs[-2][1] if len(troughs) >= 2 else min(l[-20:]))
    
    if period_high <= period_low:
        period_high = max(h[-20:])
        period_low = min(l[-20:])
    
    fib = calc_fibonacci(period_high, period_low)
    fib['swing_high'] = round(period_high, 2)
    fib['swing_low'] = round(period_low, 2)
    
    return {k: round(v, 2) for k, v in fib.items()}


def multi_period_score_v2(closes, highs, lows, volumes):
    """v2.0多周期共振评分"""
    big_state, big_detail, big_raw = judge_trend_v2(closes, highs, lows, volumes, "大周期")
    mid_state, mid_detail, mid_raw = judge_trend_v2(closes, highs, lows, volumes, "中周期")
    small_state, small_detail, small_raw = judge_trend_v2(closes, highs, lows, volumes, "小周期")
    
    # 四种状态评分: 上涨=+1, 下跌=-1, 停顿=0, 破坏=特殊处理
    score_map = {"上涨": 1, "下跌": -1, "停顿": 0, "破坏": 0}
    weight = {"大周期": 3, "中周期": 2, "小周期": 1}
    
    total = (score_map[big_state] * weight["大周期"] +
             score_map[mid_state] * weight["中周期"] +
             score_map[small_state] * weight["小周期"])
    
    # 破坏惩罚：任何周期出现破坏，总分减2
    broken_count = sum(1 for s in [big_state, mid_state, small_state] if s == "破坏")
    if broken_count > 0:
        total -= broken_count * 2
    
    # 走势类型
    structure = judge_structure_type(closes, highs, lows)
    
    # Fibonacci
    fib = calc_fib_levels(closes, highs, lows)
    
    # 当前价相对Fib的位置
    cur_price = closes[-1]
    fib_position = "未知"
    if fib:
        if cur_price >= fib.get('100%', 0):
            fib_position = "突破前高"
        elif cur_price >= fib.get('61.8%', 0):
            fib_position = "强势区(>61.8%)"
        elif cur_price >= fib.get('50%', 0):
            fib_position = "中位(50-61.8%)"
        elif cur_price >= fib.get('38.2%', 0):
            fib_position = "回调区(38.2-50%)"
        elif cur_price >= fib.get('0%', 0):
            fib_position = "深度回调(<38.2%)"
        else:
            fib_position = "跌破前低"
    
    # 安全边际
    n = len(closes)
    chg_5d = (closes[-1] / closes[-6] - 1) * 100 if n >= 6 else 0
    chg_10d = (closes[-1] / closes[-11] - 1) * 100 if n >= 11 else 0
    safe = chg_5d < 15 and chg_10d < 25
    
    return {
        "大周期": big_state, "中周期": mid_state, "小周期": small_state,
        "得分": total,
        "结构类型": structure[0],
        "结构详情": structure[1],
        "Fibonacci": fib,
        "Fib位置": fib_position,
        "5日涨幅": round(chg_5d, 1),
        "10日涨幅": round(chg_10d, 1),
        "安全边际": safe,
        "破坏数": broken_count,
        "detail": {"大": big_raw, "中": mid_raw, "小": small_raw},
    }


# ==================== 趋势表（v2.0含破坏状态）====================
TREND_TABLE_V2 = {
    6: "🔴 三周期共振强势上涨",
    5: "🔴 强势上涨（大中上涨）",
    4: "🟠 上涨（回调中）",
    3: "🟡 偏多",
    2: "🟡 弱多",
    1: "⚪ 微多/震荡",
    0: "⚪ 震荡/无方向",
    -1: "⚪ 微空/震荡",
    -2: "🟢 弱空",
    -3: "🟢 偏空",
    -4: "🔵 下跌",
    -5: "🔵 强势下跌",
    -6: "🔵 三周期共振强势下跌",
}
# 破坏导致的负分
for i in range(-8, -6):
    TREND_TABLE_V2[i] = "⚠️ 结构破坏+下跌"


# ==================== 主流程 ====================
if __name__ == "__main__":
    print("=" * 100)
    print("多周期共振评分选股器 v2.0")
    print("改进: +破坏状态 +Fibonacci支撑阻力 +走势类型(5种) +安全边际")
    print("评分: 大±3 + 中±2 + 小±1 = -6~+6, 破坏每个-2")
    print("=" * 100)
    
    # Step 1: 获取候选池（4/13涨幅>0的活跃股）
    print("\nStep 1: 获取4/13全市场日线数据...")
    daily_data = ts_api('daily', 'ts_code,trade_date,close,pct_chg,vol', trade_date='20260413')
    items = daily_data.get('items', [])
    print(f"  4/13全市场: {len(items)}只")
    
    if not items:
        print("  Tushare数据未更新，尝试用Ashare...")
        # 用之前选股池的候选标的
        target_codes = [
            'sz002580','sz002418','sh603803','sh600654','sz002980','sz002929',
            'sh605117','sz002263','sz001299','sh600176','sz000791','sz002957',
            'sz300628','sh600864','sz000938','sz301228','sz002648','sz301179',
            'sz002299','sz301216','sz002487','sz002384','sh603950','sz002364',
            'sz002328','sz002988','sz300866','sz002222','sz002975','sh600707',
        ]
        items = []
        for code in target_codes:
            df = get_price(code, frequency='1d', count=1)
            if df is not None and len(df) > 0:
                pct = (df.iloc[-1]['close'] / df.iloc[-1]['open'] - 1) * 100
                ts_code = code[2:] + ('.SH' if code.startswith('sh') else '.SZ')
                items.append([ts_code, str(df.index[-1])[:10].replace('-',''), 
                             df.iloc[-1]['close'], pct, df.iloc[-1]['volume']])
            time.sleep(0.1)
        print(f"  Ashare获取: {len(items)}只")
    
    # 分层：涨幅>0的活跃股
    active = [r for r in items if r[3] and r[3] > 0 and r[4] and r[4] > 30000]
    active.sort(key=lambda x: x[3], reverse=True)
    print(f"  活跃股(涨幅>0): {len(active)}只")
    
    # 取TOP200做详细分析
    scan_pool = active[:200]
    
    # Step 2: 获取名称映射
    print("\nStep 2: 获取股票名称...")
    names_data = ts_api('stock_basic', 'ts_code,name,industry', list_status='L')
    name_map = {}
    for r in names_data.get('items', []):
        name_map[r[0]] = {'name': r[1], 'industry': r[2]}
    
    # Step 3: 逐只拉K线做多周期分析
    print(f"\nStep 3: 逐只分析（{len(scan_pool)}只）...")
    results = []
    
    for i, stock in enumerate(scan_pool):
        ts_code = stock[0]
        cur_pct = stock[3]
        
        # 转Ashare格式
        parts = ts_code.split('.')
        prefix = 'sh' if parts[1] == 'SH' else 'sz'
        ashare_code = prefix + parts[0]
        
        try:
            df = get_price(ashare_code, frequency='1d', count=60)
            if df is None or len(df) < 25:
                continue
            
            closes = df['close'].values.astype(float)
            highs = df['high'].values.astype(float)
            lows = df['low'].values.astype(float)
            volumes = df['volume'].values.astype(float)
            
            result = multi_period_score_v2(closes, highs, lows, volumes)
            
            info = name_map.get(ts_code, {})
            name = info.get('name', '?')
            industry = info.get('industry', '?')
            
            if 'ST' in name or '退' in name:
                continue
            
            result['code'] = ts_code
            result['ashare_code'] = ashare_code
            result['name'] = name
            result['industry'] = industry
            result['pct_today'] = round(cur_pct, 1)
            result['close'] = round(closes[-1], 2)
            
            results.append(result)
        except Exception as e:
            pass
        
        if (i+1) % 50 == 0:
            print(f"  已分析 {i+1}/{len(scan_pool)}... 有效{len(results)}只")
        time.sleep(0.12)
    
    print(f"\n分析完成: {len(results)}只")
    
    # Step 4: 排序输出
    results.sort(key=lambda x: (x['得分'], -x['5日涨幅']), reverse=True)
    
    # === 得分>=5的强势股 ===
    strong = [r for r in results if r['得分'] >= 5]
    safe_strong = [r for r in strong if r['安全边际']]
    
    print(f"\n{'='*120}")
    print(f"得分>=5强势股: {len(strong)}只 (安全边际内: {len(safe_strong)}只)")
    print(f"{'='*120}")
    print(f"{'名称':<10} {'行业':<8} {'收':>6} {'今涨':>5} {'大':>4} {'中':>4} {'小':>4} {'分':>3} {'结构类型':<16} {'Fib位置':<14} {'5日':>5} {'10日':>5} {'安全':>4}")
    print("-" * 120)
    for r in strong[:40]:
        safe_tag = "✅" if r['安全边际'] else "⚠️"
        broken_tag = f"💥{r['破坏数']}" if r['破坏数'] > 0 else ""
        print(f"{r['name']:<10} {r['industry']:<8} {r['close']:>6.2f} {r['pct_today']:>+5.1f} "
              f"{r['大周期']:>4} {r['中周期']:>4} {r['小周期']:>4} {r['得分']:>+3d} "
              f"{r['结构类型']:<16} {r['Fib位置']:<14} {r['5日涨幅']:>+5.1f} {r['10日涨幅']:>+5.1f} {safe_tag}{broken_tag}")
    
    # === 得分=6的三周期共振 ===
    perfect = [r for r in results if r['得分'] == 6]
    print(f"\n{'='*120}")
    print(f"🔴 三周期共振(得分=6): {len(perfect)}只")
    print(f"{'='*120}")
    for r in perfect:
        fib = r['Fibonacci']
        support = fib.get('38.2%', '—') if fib else '—'
        resist = fib.get('127.2%', '—') if fib else '—'
        print(f"  {r['name']}({r['code'][:6]}) {r['industry']} 收{r['close']} 今{r['pct_today']:+.1f}% "
              f"结构:{r['结构类型']} Fib:{r['Fib位置']} 支撑{support} 阻力{resist} "
              f"5日{r['5日涨幅']:+.1f}% {'✅安全' if r['安全边际'] else '⚠️超买'}")
    
    # === 得分>=5 + 安全边际 + 标准趋势 ===
    best = [r for r in results if r['得分'] >= 5 and r['安全边际'] and '标准' in r['结构类型']]
    print(f"\n{'='*120}")
    print(f"⭐ 最优选（得分>=5 + 安全边际 + 标准趋势）: {len(best)}只")
    print(f"{'='*120}")
    for r in best[:20]:
        fib = r['Fibonacci']
        support = fib.get('38.2%', '—') if fib else '—'
        print(f"  {r['name']}({r['code'][:6]}) {r['industry']} 收{r['close']} 今{r['pct_today']:+.1f}% "
              f"分{r['得分']:+d} 5日{r['5日涨幅']:+.1f}% Fib支撑{support}")
    
    # === 结构破坏的标的（风险预警）===
    broken = [r for r in results if r['破坏数'] > 0]
    if broken:
        print(f"\n{'='*120}")
        print(f"⚠️ 结构破坏预警: {len(broken)}只")
        print(f"{'='*120}")
        for r in broken[:15]:
            detail = r['detail']
            break_info = ""
            for period in ['大','中','小']:
                if isinstance(detail.get(period), dict) and detail[period].get('break_type'):
                    break_info += f" {period}:{detail[period]['break_type']}"
            print(f"  {r['name']}({r['code'][:6]}) 分{r['得分']:+d} 结构:{r['结构类型']} 破坏:{break_info}")
    
    # === 保存结果 ===
    save_data = []
    for r in results:
        save_item = {k: v for k, v in r.items() if k not in ['detail', '结构详情', 'Fibonacci']}
        if r.get('Fibonacci'):
            save_item['fib_382'] = r['Fibonacci'].get('38.2%')
            save_item['fib_50'] = r['Fibonacci'].get('50%')
            save_item['fib_618'] = r['Fibonacci'].get('61.8%')
            save_item['fib_support'] = r['Fibonacci'].get('swing_low')
            save_item['fib_resist'] = r['Fibonacci'].get('swing_high')
        save_data.append(save_item)
    
    with open('/Users/ecustkiller/WorkBuddy/Claw/multi_period_v2_results.json', 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n结果已保存到 multi_period_v2_results.json")
    print(f"\n{'='*120}")
    print(f"扫描完成! 总计{len(results)}只 | 得分>=5: {len(strong)}只 | 最优选: {len(best)}只 | 破坏预警: {len(broken)}只")
    print(f"{'='*120}")
