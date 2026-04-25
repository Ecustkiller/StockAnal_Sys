#!/usr/bin/env python3
"""
板块内遍历精选 v3.3 — 9 Skill评分（BCI板块完整性整合） + 权重迭代 + 批量数据

v3.3 vs v3.2 改进（BCI板块完整性整合）：
1. ★ 新增BCI板块完整性指数计算，传入各评分函数
2. S1-TXCG人和维度：BCI加权替代简单涨停数计数
3. S3-山茶花带动性：BCI加权提升板块带动性评分精度
4. S7-事件驱动板块效应：BCI加权替代简单涨停数计数
5. score_all新增bci_score参数，从外部传入BCI得分

v3.2 vs v3.1 改进（全面提升覆盖率）：
1. S4-Mistery新增M6仓位管理(金字塔加仓+半仓滚动+止损纪律+时间止损)
2. S5-TDS新增T4三K反转信号(逆趋势+中间K不是最长+后两根吞没)
3. S5-TDS新增T6双向突破信号(趋势方向连续两次改变后的突破)
4. TXCG六大模型量化加分(连板竞争/分歧策略/反包修复/承接/大长腿/唯一性)
5. 满分从100→105（含TXCG六大模型5分加分）

v3.1 改进（对齐SOP全流程）：
1. S1-TXCG：天时地利人和9分制量化（天时=情绪周期+地利=筹码结构+人和=板块效应）
2. S2-元子元：个股情绪6阶段判定（冰点启动/发酵确认/主升加速/分歧换手/高潮见顶/退潮补跌）
3. S3-山茶花：龙头三维评分15分制（主动性+带动性+抗跌性）+ 垃圾时间5指标检测
4. S4-Mistery：M1趋势+M2买点(520金叉/破五反五/BBW)+M3卖点扣分+M4量价+M5形态
5. S5-TDS：波峰波谷窗口扩大到±5 + T1推进+T2吞没+T3突破+T5反转(锤子线/底部反转)
6. S6-百胜WR：WR-1完整7条件逐条检测(含封板时间) + WR-2完整5条件逐条检测 + WR-3底倍量柱4条件(60分钟K线)
7. S7-事件驱动：低位埋伏评分(近期涨幅) + 股性弹性(涨停历史)
8. S8-多周期：大(±3)+中(±2)+小(±1)三周期独立判定，总分-6~+6

用法：python3 sector_deep_pick_v2.py 20260415 化学制药 医药商业 ... [情绪阶段]
"""
import sys, os, requests, json, time
import numpy as np

TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"
WEIGHTS_FILE = os.path.expanduser("~/WorkBuddy/Claw/track/skill_weights.json")

# ========== 默认权重（可被复盘迭代覆盖）==========
DEFAULT_WEIGHTS = {
    "TXCG": 15,     # S1
    "元子元": 10,    # S2
    "山茶花": 15,    # S3
    "Mistery": 10,  # S4
    "TDS": 10,      # S5
    "百胜WR": 15,   # S6
    "事件驱动": 10,  # S7
    "多周期": 5,     # S8
    "基本面": 10,    # S9 ← NEW
}

def load_weights():
    """加载动态权重（复盘后更新的），没有就用默认值"""
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE, 'r') as f:
                data = json.load(f)
                w = data.get('weights', DEFAULT_WEIGHTS)
                # 确保所有key都在
                for k, v in DEFAULT_WEIGHTS.items():
                    if k not in w:
                        w[k] = v
                return w, data.get('version', 'default'), data.get('last_review', 'N/A')
        except:
            pass
    return DEFAULT_WEIGHTS.copy(), 'default', 'N/A'

def ts_api(api_name, **kwargs):
    params = {"api_name": api_name, "token": TOKEN, "params": kwargs, "fields": ""}
    for retry in range(2):
        try:
            r = requests.post("http://api.tushare.pro", json=params, timeout=30)
            d = r.json()
            if d.get('data') and d['data'].get('items'):
                return d['data']['fields'], d['data']['items']
            return [], []
        except:
            if retry == 0: time.sleep(1)
    return [], []

# ========== 评分函数 ==========

def score_txcg(c, sector_zt, emotion_stage, max_score, bci_score=0):
    """S1-TXCG(天时+地利+人和) — 9分制量化
    天时(0-3)：情绪周期阶段+涨停指数位置
    地利(0-3)：日线形态+换手充分+做空动能衰竭+前面未被搅浑
    人和(0-3)：市场记忆+板块效应(★BCI加权)+消息催化+股性好
    三者缺一不可。总分≥7分可参与，≥8分重仓。
    """
    tags = []
    chg5 = c.get('chg5', 0); chg1 = c.get('pct_chg', 0)
    is_zt = c.get('is_zt', False); ma_tag = c.get('ma_tag', '弱')
    highs = c.get('highs', []); lows = c.get('lows', []); closes = c.get('closes', [])
    
    # === 天时(0-3)：情绪周期阶段 ===
    tianshi = 0
    if emotion_stage == '起爆': tianshi = 3
    elif emotion_stage == '一致': tianshi = 3
    elif emotion_stage == '修复': tianshi = 2
    elif emotion_stage == '分歧': tianshi = 2
    elif emotion_stage == '启动': tianshi = 2
    elif emotion_stage == '冰点': tianshi = 1  # 冰点有火种也给1分
    elif emotion_stage == '退潮': tianshi = 0
    elif emotion_stage == '主升': tianshi = 3
    # 涨停指数辅助：板块涨停多=情绪好
    if sector_zt >= 10 and tianshi < 3: tianshi = min(3, tianshi + 1)
    
    # === 地利(0-3)：筹码结构+日线形态+做空动能衰竭 ===
    dili = 0
    # 均线多头=形态好
    if ma_tag == '多头': dili += 1
    # 超跌=做空动能衰竭（安全边际高）
    if chg5 < -10: dili += 1; tags.append(f"深超跌{chg5:+.0f}%")
    elif chg5 < -5: dili += 1; tags.append(f"超跌{chg5:+.0f}%")
    elif -5 <= chg5 <= 5: dili += 1; tags.append(f"安全{chg5:+.0f}%")
    # 前面未被搅浑：近5日无大阴线（跌>5%的日子）
    if len(closes) >= 5:
        big_drop = 0
        for i in range(-5, 0):
            idx = i + len(closes)
            if idx >= 1 and closes[idx] < closes[idx-1] * 0.95:
                big_drop += 1
        if big_drop == 0: dili += 1
        elif big_drop >= 2: tags.append("⚠前面搅浑")
    else:
        dili += 1  # 无数据默认不扣
    dili = min(3, dili)
    
    # === 人和(0-3)：板块效应(★BCI加权)+股性+市场记忆 ===
    renhe = 0
    # v3.3: 用BCI板块完整性替代简单涨停数计数
    # BCI综合了梯队层次+龙头强度+封板率+持续性+换手板比例
    if bci_score >= 70:
        renhe += 2; tags.append(f"BCI={bci_score}板块极完整")
    elif bci_score >= 50:
        renhe += 2; tags.append(f"BCI={bci_score}板块较完整")
    elif bci_score >= 30:
        renhe += 1; tags.append(f"BCI={bci_score}板块一般")
    elif sector_zt >= 10:
        renhe += 2; tags.append(f"板块{sector_zt}家涨停")
    elif sector_zt >= 5:
        renhe += 1; tags.append(f"板块{sector_zt}家涨停")
    elif sector_zt >= 3:
        renhe += 1
    # 股性好（有涨停历史=市场记忆）
    if is_zt: renhe += 1; tags.append("涨停=股性好")
    elif chg1 >= 7: renhe += 1
    renhe = min(3, renhe)
    
    # === 总分映射到max_score ===
    raw_9 = tianshi + dili + renhe  # 0-9分
    # 映射到权重分
    s = int(raw_9 / 9 * max_score + 0.5)
    
    if raw_9 >= 8: tags.append(f"天地人{raw_9}/9🔥重仓")
    elif raw_9 >= 7: tags.append(f"天地人{raw_9}/9✅可参与")
    else: tags.append(f"天地人{raw_9}/9")
    
    return min(max_score, s), tags

def score_yuanziyuan(c, max_score):
    """S2-元子元(情绪周期8阶段+个股情绪状态)
    三维度：指数情绪+题材赚钱效应+龙头空间
    个股6阶段：冰点启动/发酵确认/主升加速/分歧换手/高潮见顶/退潮补跌
    """
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0)
    chg10 = c.get('chg10', 0); is_zt = c.get('is_zt', False)
    closes = c.get('closes', []); vols = c.get('vols', c.get('lows', []))  # 兼容
    
    # === 个股情绪阶段判定 ===
    stage = '未知'
    if is_zt and chg5 < -5:
        stage = '冰点启动'; s += 5; tags.append("🔥冰点涨停=最佳买点")
    elif is_zt and chg5 < 5:
        stage = '发酵确认'; s += 4; tags.append("发酵确认")
    elif chg1 >= 5 and chg5 < 0:
        stage = '冰点启动'; s += 4; tags.append("超跌大涨")
    elif is_zt and 5 <= chg5 < 15:
        stage = '主升加速'; s += 3; tags.append("主升涨停")
    elif chg1 >= 5 and 0 <= chg5 < 10:
        stage = '发酵确认'; s += 3; tags.append("大阳安全")
    elif is_zt and chg5 >= 15:
        # 高位涨停：检查是否爆量分歧（高潮见顶信号）
        if len(closes) >= 5:
            vol_ratio = c.get('vol_ratio', 1)
            if vol_ratio >= 3:
                stage = '高潮见顶'; s += 0; tags.append("⚠爆量高位涨停=见顶")
            else:
                stage = '主升加速'; s += 2; tags.append("⚠高位涨停")
        else:
            stage = '主升加速'; s += 2
    elif chg1 > 0 and chg5 < 10:
        stage = '发酵确认'; s += 2
    elif chg1 > 0 and chg5 >= 10:
        stage = '主升加速'; s += 1
    elif chg1 < -3 and chg5 > 15:
        stage = '退潮补跌'; s += 0; tags.append("⚠退潮补跌")
    elif chg1 <= 0:
        if chg5 < -10:
            stage = '冰点启动'; s += 2; tags.append("深跌待启动")
        else:
            stage = '分歧换手'; s += 1
    
    # === 量价关系加分 ===
    # 缩量上涨=筹码锁定好
    vol_ratio = c.get('vol_ratio', 1)
    if chg1 > 3 and vol_ratio < 1.2:
        s += 2; tags.append("缩量上涨")
    # 放量滞涨=见顶信号
    elif chg1 < 2 and vol_ratio > 2.5:
        s -= 1; tags.append("⚠放量滞涨")
    
    # === 连板接力节点加分 ===
    # 如果是涨停且5日涨幅适中=发酵/主升最佳阶段
    if is_zt and 0 <= chg5 <= 10:
        s += 1  # 低位涨停额外加分
    
    tags.append(f"情绪:{stage}")
    return min(max_score, max(0, s)), tags

def score_camellia(c, sector_zt, max_score, market_data=None, bci_score=0):
    """S3-山茶花(龙头三维评分15分制 + 垃圾时间检测 + ★BCI带动性加权)
    主动性(1-5)：是否率先启动、领涨板块
    带动性(1-5)：龙头涨时小弟是否跟涨（★BCI加权）
    抗跌性(1-5)：情绪分歧时是否最后倒下
    ≥12分=确认龙头，9-11=准龙头，<9=非龙头
    
    垃圾时间5指标（满足任意2条→强制空仓）：
    1. 自然涨停<25家  2. 赚钱效应<35%  3. 最高连板≤2板
    4. 炸板率>60%  5. 连续2天出现最贵一碗面(跌>15%)
    """
    tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0); is_zt = c.get('is_zt', False)
    
    # === 龙头三维评分(15分制) ===
    # 主动性(1-5)：涨停速度+领涨程度
    active = 1
    if is_zt and chg1 >= 19:  # 20cm涨停
        active = 5; tags.append("主动5:20cm涨停")
    elif is_zt:
        active = 4; tags.append("主动4:涨停")
    elif chg1 >= 7:
        active = 3; tags.append("主动3:大涨")
    elif chg1 >= 5:
        active = 2
    
    # 带动性(1-5)：板块涨停数=龙头带动效果 (★BCI加权)
    drive = 1
    # v3.3: 优先用BCI板块完整性评估带动性
    if bci_score >= 70:
        drive = 5; tags.append(f"带动5:BCI={bci_score}板块极完整")
    elif bci_score >= 55:
        drive = 4; tags.append(f"带动4:BCI={bci_score}")
    elif bci_score >= 40:
        drive = 3; tags.append(f"带动3:BCI={bci_score}")
    elif sector_zt >= 10:
        drive = 5; tags.append(f"带动5:板块{sector_zt}家涨停")
    elif sector_zt >= 7:
        drive = 4
    elif sector_zt >= 5:
        drive = 3
    elif sector_zt >= 3:
        drive = 2
    
    # 抗跌性(1-5)：逆势表现
    resist = 1
    if is_zt and chg5 < -5:
        resist = 5; tags.append("抗跌5:超跌涨停穿越")
    elif is_zt and chg5 < 5:
        resist = 4; tags.append("抗跌4:低位涨停")
    elif chg1 > 0 and chg5 < -3:
        resist = 4; tags.append("抗跌4:逆势上涨")
    elif chg1 > 0 and chg5 < 0:
        resist = 3
    elif chg1 > 0:
        resist = 2
    
    dragon_score = active + drive + resist  # 3-15分
    if dragon_score >= 12:
        tags.append(f"🐉龙头确认{dragon_score}/15")
    elif dragon_score >= 9:
        tags.append(f"准龙头{dragon_score}/15")
    else:
        tags.append(f"非龙头{dragon_score}/15")
    
    # === 垃圾时间检测（市场级别，影响所有标的） ===
    garbage_count = 0
    if market_data:
        total_zt = market_data.get('total_zt', 50)
        earn_ratio = market_data.get('earn_ratio', 50)  # 赚钱效应%
        max_board = market_data.get('max_board', 5)  # 最高连板
        zb_rate = market_data.get('zb_rate', 30)  # 炸板率%
        has_big_loss = market_data.get('has_big_loss', False)  # 最贵一碗面
        
        if total_zt < 25: garbage_count += 1
        if earn_ratio < 35: garbage_count += 1
        if max_board <= 2: garbage_count += 1
        if zb_rate > 60: garbage_count += 1
        if has_big_loss: garbage_count += 1
        
        if garbage_count >= 2:
            tags.append(f"⚠垃圾时间{garbage_count}/5→空仓")
    
    # === 映射到max_score ===
    # 龙头评分15分→映射到权重分，垃圾时间扣分
    s = int(dragon_score / 15 * max_score + 0.5)
    if garbage_count >= 2:
        s = max(0, s - int(max_score * 0.3))  # 垃圾时间扣30%
    
    return min(max_score, s), tags

def score_mistery(c, max_score):
    """S4-Mistery(M1趋势+M2买点+M3卖点+M4量价+M5形态)
    M1趋势(0-3)：均线多头排列
    M2买点(0-3)：520金叉/破五反五/BBW起爆
    M3卖点(-2~0)：放量滞涨/3天不创新高/MACD顶背离→扣分
    M4量价(0-2)：量价齐升确认
    M5形态(0-2)：空中加油/仙人指路等
    """
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); vr = c.get('vol_ratio', 0)
    bbw = c.get('bbw', 0); ma_tag = c.get('ma_tag', '弱')
    closes = c.get('closes', []); highs = c.get('highs', [])
    
    # === M1趋势(0-3)：均线多头 ===
    if ma_tag == '多头': s += 3
    elif ma_tag == '短多': s += 2
    elif chg1 > 0: s += 1  # 至少在涨
    
    # === M2买点(0-3)：520金叉/破五反五/BBW起爆 ===
    m2 = 0
    ma5 = c.get('ma5', 0); ma20 = c.get('ma20', 0)
    # 520金叉检测：MA5刚上穿MA20
    if len(closes) >= 7 and ma5 > 0 and ma20 > 0:
        # 简化：当前MA5>MA20且5日前MA5<=MA20
        old_closes = closes[-7:-2]
        if len(old_closes) >= 5:
            old_ma5 = np.mean(old_closes[-5:])
            old_ma20 = np.mean(closes[-min(20, len(closes)):-5]) if len(closes) > 5 else old_ma5
            if old_ma5 <= old_ma20 and ma5 > ma20:
                m2 += 2; tags.append("520金叉")
    # 破五反五：近5日有跌破MA5后重新站上
    if len(closes) >= 5 and ma5 > 0:
        below_ma5 = any(closes[i] < np.mean(closes[max(0,i-4):i+1]) for i in range(-5, -1) if i+len(closes) >= 0)
        if below_ma5 and closes[-1] > ma5:
            m2 += 1; tags.append("破五反五")
    # BBW收缩起爆（WR-2核心信号）
    if bbw < 0.12 and chg1 > 3:
        m2 += 2; tags.append(f"BBW={bbw:.3f}极低起爆")
    elif bbw < 0.15 and chg1 > 3:
        m2 += 1; tags.append(f"BBW={bbw:.3f}低")
    s += min(3, m2)
    
    # === M3卖点信号(-2~0)：扣分项 ===
    # 放量滞涨
    if chg1 < 2 and vr > 2.5:
        s -= 1; tags.append("⚠M3放量滞涨")
    # 3天不创新高
    if len(highs) >= 4:
        recent_high = max(highs[-4:-1])  # 前3天最高
        if highs[-1] < recent_high and chg1 < 1:
            s -= 1; tags.append("⚠M3滞涨不创新高")
    
    # === M4量价(0-2)：量价齐升确认 ===
    if chg1 > 3 and vr >= 2:
        s += 2; tags.append("量价齐升")
    elif chg1 > 0 and vr >= 1.5:
        s += 1
    
    # === M5形态(0-2)：特殊K线形态 ===
    if len(closes) >= 3 and len(highs) >= 3:
        # 空中加油：前一天缩量小阴/十字星，今天放量大阳
        if chg1 > 5 and vr > 1.5:
            prev_range = abs(closes[-2] - closes[-3]) / closes[-3] * 100 if closes[-3] > 0 else 0
            if prev_range < 2:  # 前一天窄幅
                s += 2; tags.append("M5空中加油")
        # 仙人指路：长上影线后次日收复
        if len(highs) >= 2:
            prev_upper = (highs[-2] - closes[-2]) / closes[-2] * 100 if closes[-2] > 0 else 0
            if prev_upper > 3 and closes[-1] > highs[-2] * 0.98:
                s += 1; tags.append("M5仙人指路收复")
    
    # === M6仓位管理(0-5)：金字塔加仓条件+半仓滚动适配性 ===
    m6 = 0
    ma5_val = c.get('ma5', 0); ma10_val = c.get('ma10', 0); ma20_val = c.get('ma20', 0)
    # 金字塔加仓条件：趋势确认+突破关键位
    if len(closes) >= 1 and ma5_val > 0 and ma10_val > 0 and ma20_val > 0:
        if closes[-1] > ma5_val > ma10_val > ma20_val and chg1 > 0:
            m6 += 2  # 趋势确认可加仓
        elif closes[-1] > ma5_val > ma10_val:
            m6 += 1  # 短期多头可试探
    # 半仓滚动适配性：有明确支撑压力位
    if ma20_val > 0 and len(closes) >= 1 and abs(closes[-1] - ma20_val) / ma20_val < 0.08:
        m6 += 1  # 靠近MA20=有明确支撑/压力位可做T
    # 止损纪律检测
    if len(closes) >= 20:
        lows_list = c.get('lows', [])
        if lows_list and len(lows_list) >= 20:
            support_20 = min(lows_list[-20:])
            if closes[-1] > support_20 * 1.05:
                m6 += 1  # 距离支撑位还有空间=止损可控
    # 时间止损检测
    if len(closes) >= 7:
        chg_7d = (closes[-1] - closes[-7]) / closes[-7] * 100 if closes[-7] > 0 else 0
        if chg_7d > 5: m6 += 1
        elif chg_7d < -3: m6 -= 1
    s += min(5, max(0, m6))
    
    return min(max_score, max(0, s)), tags

def score_tds(c, max_score):
    """S5-TDS(波峰波谷趋势+T1推进+T2吞没+T3突破+T4三K反转+T5反转+T6双向突破)
    波峰波谷(0-3)：高点抬高+低点抬高=上升趋势
    T1推进(0-2)：今日高低点均高于昨日
    T2吞没(0-1)：阳线吞没前一根阴线
    T3突破(0-2)：突破前高/前波峰
    T4三K反转(0-2)：逆趋势+中间K不是最长+后两根吞没
    T5反转(0-2)：底部反转信号（锤子线/早晨之星）
    T6双向突破(0-2)：趋势方向连续两次改变后的突破
    """
    s = 0; tags = []
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    highs = c.get('highs', []); lows = c.get('lows', []); closes = c.get('closes', [])
    
    # === 波峰波谷趋势(0-3) ===
    # 扩大窗口到±5根K线（原来±3太窄）
    if len(highs) >= 15:
        # 找波峰波谷
        peaks = []; troughs = []
        for i in range(5, len(highs)-5):
            if highs[i] >= max(highs[max(0,i-5):i]) and highs[i] >= max(highs[i+1:min(len(highs),i+6)]):
                peaks.append(highs[i])
            if lows[i] <= min(lows[max(0,i-5):i]) and lows[i] <= min(lows[i+1:min(len(lows),i+6)]):
                troughs.append(lows[i])
        if len(peaks) >= 2 and len(troughs) >= 2:
            if peaks[-1] > peaks[-2] and troughs[-1] > troughs[-2]:
                s += 3; tags.append("上升趋势(峰谷抬高)")
            elif peaks[-1] > peaks[-2] or troughs[-1] > troughs[-2]:
                s += 2; tags.append("趋势转折")
        elif len(peaks) >= 2 and peaks[-1] > peaks[-2]:
            s += 2
    elif len(highs) >= 10:
        rh = max(highs[-5:]); ph = max(highs[-10:-5])
        rl = min(lows[-5:]); pl = min(lows[-10:-5])
        if rh > ph and rl > pl: s += 3; tags.append("上升趋势")
        elif rh > ph: s += 2
        elif rl > pl: s += 1
    
    # === T1推进(0-2)：今日高低点均高于昨日 ===
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            s += 2; tags.append("T1推进")
        elif highs[-1] > highs[-2]:
            s += 1
    
    # === T2吞没(0-1)：阳线吞没前一根阴线 ===
    if len(closes) >= 3:
        # 前一天阴线，今天阳线且收盘>前天高点
        if closes[-2] < closes[-3] and closes[-1] > closes[-2] and closes[-1] > highs[-2]:
            s += 1; tags.append("T2吞没")
    
    # === T3突破(0-2)：突破前高/前波峰 ===
    if is_zt:
        s += 2; tags.append("T3涨停突破")
    elif chg1 >= 7:
        s += 1; tags.append("T3大阳突破")
    
    if len(closes) >= 20 and len(highs) >= 20:
        prev_high = max(highs[-20:-1])
        if closes[-1] >= prev_high:
            s += 1; tags.append("突破20日前高")
    
    # === T5反转(0-2)：底部反转信号 ===
    if len(closes) >= 5 and len(lows) >= 5:
        chg5 = c.get('chg5', 0)
        if chg5 < -10 and chg1 > 3:
            # 深跌后大涨=底部反转
            s += 2; tags.append("T5底部反转")
        elif chg5 < -5 and chg1 > 0:
            # 锤子线检测：下影线长
            if len(closes) >= 1 and len(lows) >= 1:
                body = abs(closes[-1] - closes[-2]) if len(closes) >= 2 else 0
                lower_shadow = closes[-1] - lows[-1] if closes[-1] > lows[-1] else 0
                if lower_shadow > body * 2 and lower_shadow > 0:
                    s += 1; tags.append("T5锤子线")
    
    # === T4三K反转(0-2)：逆趋势+中间K不是最长+后两根吞没 ===
    if len(closes) >= 4 and len(highs) >= 4:
        opens = c.get('opens', [])
        if len(opens) >= 4:
            k1_body = abs(closes[-3] - opens[-3])
            k2_body = abs(closes[-2] - opens[-2])
            k3_body = abs(closes[-1] - opens[-1])
            chg5_val = c.get('chg5', 0)
            # 下跌后的看涨三K反转
            if chg5_val < -3:
                if k2_body <= max(k1_body, k3_body):
                    if closes[-1] > opens[-1] and closes[-1] > highs[-2]:
                        s += 2; tags.append("T4看涨三K反转")
            # 上涨后的看跌三K反转（扣分）
            elif chg5_val > 10:
                if k2_body <= max(k1_body, k3_body):
                    if closes[-1] < opens[-1] and closes[-1] < lows[-2]:
                        s -= 1; tags.append("⚠T4看跌三K反转")
    
    # === T6双向突破(0-2)：趋势方向连续两次改变后的突破 ===
    # 需要用到前面计算的peaks和troughs
    if len(highs) >= 15:
        # 重新获取peaks（如果前面已经计算过）
        _peaks = []; _troughs = []
        for i in range(5, len(highs)-5):
            if highs[i] >= max(highs[max(0,i-5):i]) and highs[i] >= max(highs[i+1:min(len(highs),i+6)]):
                _peaks.append(highs[i])
            if lows[i] <= min(lows[max(0,i-5):i]) and lows[i] <= min(lows[i+1:min(len(lows),i+6)]):
                _troughs.append(lows[i])
        if len(_peaks) >= 3 and len(_troughs) >= 3:
            p_trend1 = 1 if _peaks[-2] > _peaks[-3] else -1
            p_trend2 = 1 if _peaks[-1] > _peaks[-2] else -1
            t_trend1 = 1 if _troughs[-2] > _troughs[-3] else -1
            t_trend2 = 1 if _troughs[-1] > _troughs[-2] else -1
            if p_trend1 != p_trend2 or t_trend1 != t_trend2:
                if closes[-1] > _peaks[-1]:
                    s += 2; tags.append("T6看涨双向突破")
                elif chg1 > 5:
                    s += 1; tags.append("T6突破信号")
    
    return min(max_score, max(0, s)), tags

def score_wr(c, max_score):
    """S6-百胜WR(WR-1首板放量7条件 + WR-2右侧趋势起爆5条件 + WR-3底倍量柱)
    
    WR-1(0-7)：首次涨停+量比≥3+换手≥8%+MA30>60>120+市值30-150亿+资金净流入+封板时间≤10:30
    WR-2(0-5)：BBW<0.15+整理≥20天+倍量2.5×+突破前高+板块效应
    WR-3(0-4)：底倍量柱出现+第二倍量确认+支撑不破+阳线形态（需60分钟K线）
    三者取高分（同一只票通常只满足一个模型）
    """
    tags = []
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    vr = c.get('vol_ratio', 0); bbw = c.get('bbw', 0)
    ma_tag = c.get('ma_tag', '弱'); net = c.get('net_inflow', 0)
    turnover = c.get('turnover', 0)
    circ_mv = c.get('circ_mv', 0)
    mv_yi = circ_mv / 10000 if circ_mv else 0  # 万→亿
    closes = c.get('closes', []); highs = c.get('highs', [])
    
    # === WR-1 首板放量涨停模型(0-7) ===
    wr1 = 0
    wr1_detail = []
    if is_zt:
        # 条件1：涨停（已满足）
        wr1 += 1; wr1_detail.append("涨停✅")
        # 条件2：量比≥3
        if vr >= 3: wr1 += 1; wr1_detail.append(f"量比{vr:.1f}✅")
        else: wr1_detail.append(f"量比{vr:.1f}❌")
        # 条件3：换手率≥8%
        if turnover and turnover >= 8: wr1 += 1; wr1_detail.append(f"换手{turnover:.0f}%✅")
        else: wr1_detail.append(f"换手{turnover:.0f}%❌" if turnover else "换手?")
        # 条件4：均线多头(MA30>MA60>MA120，简化用ma_tag)
        if '多' in ma_tag: wr1 += 1; wr1_detail.append("均线多头✅")
        else: wr1_detail.append(f"均线{ma_tag}❌")
        # 条件5：市值30-150亿
        if 30 <= mv_yi <= 150: wr1 += 1; wr1_detail.append(f"市值{mv_yi:.0f}亿✅")
        else: wr1_detail.append(f"市值{mv_yi:.0f}亿❌")
        # 条件6：资金净流入
        if net > 0: wr1 += 1; wr1_detail.append(f"净入{net:+.1f}亿✅")
        else: wr1_detail.append(f"净入{net:+.1f}亿❌")
        # 条件7：封板时间≤10:30（从5分钟K线检测）
        zt_time = c.get('zt_time', None)  # 格式: "HH:MM" 或 None
        if zt_time:
            if zt_time <= "10:30":
                wr1 += 1; wr1_detail.append(f"封板{zt_time}✅")
            elif zt_time <= "11:30":
                wr1_detail.append(f"封板{zt_time}午前⚠")
            else:
                wr1_detail.append(f"封板{zt_time}偏晚❌")
        else:
            wr1_detail.append("封板时间?")
        
        tags.append(f"WR1={wr1}/7({'|'.join(wr1_detail)})")
        if wr1 >= 6: tags.append("🔥WR1高分")
        elif wr1 >= 5: tags.append("WR1较强")
    
    # === WR-2 右侧趋势起爆模型(0-5) ===
    wr2 = 0
    wr2_detail = []
    # 条件1：BBW收缩<0.15（波动率收窄=充分整理）
    if bbw < 0.12: wr2 += 1; wr2_detail.append(f"BBW={bbw:.3f}极低✅")
    elif bbw < 0.15: wr2 += 1; wr2_detail.append(f"BBW={bbw:.3f}低✅")
    else: wr2_detail.append(f"BBW={bbw:.3f}❌")
    # 条件2：倍量突破（量比≥2.5）
    if vr >= 2.5: wr2 += 1; wr2_detail.append(f"倍量{vr:.1f}x✅")
    elif vr >= 2: wr2_detail.append(f"量{vr:.1f}x接近")
    else: wr2_detail.append(f"量{vr:.1f}x❌")
    # 条件3：突破形态（涨停或涨≥7%大阳）
    if is_zt: wr2 += 1; wr2_detail.append("涨停突破✅")
    elif chg1 >= 7: wr2 += 1; wr2_detail.append(f"大阳{chg1:+.1f}%✅")
    else: wr2_detail.append(f"涨{chg1:+.1f}%❌")
    # 条件4：均线多头
    if '多' in ma_tag: wr2 += 1; wr2_detail.append("均线多头✅")
    else: wr2_detail.append(f"均线{ma_tag}❌")
    # 条件5：突破前高
    if len(closes) >= 20 and len(highs) >= 20:
        prev_high = max(highs[-20:-1])
        if closes[-1] >= prev_high:
            wr2 += 1; wr2_detail.append("突破前高✅")
        else:
            wr2_detail.append("未破前高❌")
    
    tags.append(f"WR2={wr2}/5({'|'.join(wr2_detail)})")
    if wr2 >= 4: tags.append("🔥WR2起爆")
    
    # === WR-3 底倍量柱短线模型(0-4)（需60分钟K线） ===
    wr3 = 0
    wr3_detail = []
    kline_60m = c.get('kline_60m', None)  # 60分钟K线数据 {closes, highs, lows, vols}
    
    if kline_60m and len(kline_60m.get('vols', [])) >= 12:
        vols_60 = kline_60m['vols']
        closes_60 = kline_60m['closes']
        highs_60 = kline_60m['highs']
        lows_60 = kline_60m['lows']
        n60 = len(vols_60)
        
        # 第一步：寻找底倍量柱（低位+成交量≥前一根2倍+阳线）
        first_dbl_idx = None
        for i in range(max(1, n60-20), n60):  # 在最近20根60分钟K线中找
            if vols_60[i] >= vols_60[i-1] * 2 and closes_60[i] > closes_60[i-1]:
                # 检查是否在相对低位（近20根的下半区）
                recent_range = closes_60[max(0, i-20):i+1]
                mid_price = (max(recent_range) + min(recent_range)) / 2
                if closes_60[i] <= mid_price * 1.05:  # 在中位以下=低位
                    first_dbl_idx = i
                    break
        
        if first_dbl_idx is not None:
            wr3 += 1; wr3_detail.append("底倍量柱✅")
            first_low = lows_60[first_dbl_idx]  # 支撑位
            first_high = highs_60[first_dbl_idx]
            
            # 第二步：寻找第二倍量柱确认
            second_confirmed = False
            for j in range(first_dbl_idx + 1, n60):
                if vols_60[j] >= vols_60[j-1] * 2:
                    # 条件1：第二根倍量K线收盘>第一根最高价（量价齐升突破）
                    if closes_60[j] > first_high:
                        wr3 += 1; wr3_detail.append("二次倍量确认✅")
                        second_confirmed = True
                    # 条件2：不跌破第一根最低价（支撑不破）
                    if lows_60[j] >= first_low:
                        wr3 += 1; wr3_detail.append("支撑不破✅")
                    else:
                        wr3_detail.append("⚠破支撑")
                    break
            
            if not second_confirmed:
                wr3_detail.append("待二次确认")
            
            # 第三步：当前价格是否仍在支撑位上方
            if closes_60[-1] >= first_low:
                wr3 += 1; wr3_detail.append(f"支撑{first_low:.2f}上方✅")
            else:
                wr3_detail.append(f"⚠已破支撑{first_low:.2f}")
            
            tags.append(f"WR3={wr3}/4({'|'.join(wr3_detail)})")
            if wr3 >= 3: tags.append("🔥WR3底倍量确认")
        else:
            wr3_detail.append("无底倍量柱")
            tags.append(f"WR3=0/4(无信号)")
    else:
        tags.append("WR3=N/A(无60m数据)")
    
    # === 取三个模型的高分映射到max_score ===
    best_raw = max(wr1, wr2, wr3)
    if best_raw == wr1:
        best_max = 7
    elif best_raw == wr2:
        best_max = 5
    else:
        best_max = 4
    s = int(best_raw / best_max * max_score + 0.5) if best_max > 0 else 0
    
    return min(max_score, s), tags

def score_event(c, sector_zt, max_score, bci_score=0):
    """S7-事件驱动(板块效应+低位埋伏+资金流向+连板历史)
    核心：消息面选方向→历史复盘选票→低位埋伏→事件催化兑现
    评分维度：
    1. 板块效应(0-3)：板块涨停数/BCI=事件催化强度
    2. 低位埋伏(0-3)：近20日涨幅<10%=低位安全
    3. 资金流向(0-2)：大单净流入=真金白银
    4. 股性弹性(0-2)：有涨停历史=弹性好
    """
    s = 0; tags = []
    chg5 = c.get('chg5', 0); chg10 = c.get('chg10', 0)
    net = c.get('net_inflow', 0); is_zt = c.get('is_zt', False)
    chg1 = c.get('pct_chg', 0)
    
    # === 板块效应(0-3)：事件催化强度 (★BCI加权) ===
    # v3.3: 优先用BCI板块完整性评估事件催化强度
    if bci_score >= 60:
        s += 3; tags.append(f"BCI={bci_score}事件催化强")
    elif bci_score >= 40:
        s += 2; tags.append(f"BCI={bci_score}事件催化中")
    elif sector_zt >= 10: s += 3; tags.append(f"事件催化强({sector_zt}家涨停)")
    elif sector_zt >= 7: s += 2
    elif sector_zt >= 5: s += 2
    elif sector_zt >= 3: s += 1
    
    # === 低位埋伏(0-3)：近期涨幅=安全边际 ===
    # "板块里面的核心票并近期涨幅不大的票"
    if chg5 < -5: s += 3; tags.append(f"低位埋伏{chg5:+.0f}%")
    elif chg5 < 0: s += 2; tags.append(f"低位{chg5:+.0f}%")
    elif chg5 < 5: s += 2
    elif chg5 < 10: s += 1
    elif chg5 >= 15: s -= 1; tags.append(f"⚠已涨{chg5:+.0f}%非低位")
    
    # === 资金流向(0-2)：真金白银 ===
    if net > 1: s += 2; tags.append(f"大资金{net:+.1f}亿")
    elif net > 0.3: s += 1
    elif net < -1: s -= 1; tags.append(f"❌净出{net:.1f}亿")
    
    # === 股性弹性(0-2)：有涨停=市场记忆 ===
    if is_zt: s += 2; tags.append("涨停=弹性好")
    elif chg1 >= 7: s += 1; tags.append("大涨=有弹性")
    
    return min(max_score, max(0, s)), tags

def score_multi_period(c, max_score):
    """S8-多周期共振(大周期+中周期+小周期)
    大周期(日线/MA20)：±3分
    中周期(5日/MA10)：±2分
    小周期(2日/MA5)：±1分
    总分-6~+6，6=三周期共振强势上涨
    """
    tags = []
    chg1 = c.get('pct_chg', 0); chg5 = c.get('chg5', 0)
    chg10 = c.get('chg10', 0); ma_tag = c.get('ma_tag', '弱')
    closes = c.get('closes', [])
    ma5 = c.get('ma5', 0); ma10 = c.get('ma10', 0); ma20 = c.get('ma20', 0)
    
    # === 大周期(日线/MA20)：±3 ===
    big = 0
    if ma_tag == '多头': big = 3  # 价格>MA5>MA10>MA20
    elif ma_tag == '短多': big = 1  # MA5>MA10但不完全多头
    elif len(closes) >= 1 and ma20 > 0:
        if closes[-1] > ma20: big = 1
        elif closes[-1] < ma20 * 0.95: big = -2
        else: big = -1
    
    # === 中周期(5日/MA10)：±2 ===
    mid = 0
    if chg5 > 5: mid = 2
    elif chg5 > 2: mid = 1
    elif chg5 > -2: mid = 0
    elif chg5 > -5: mid = -1
    else: mid = -2
    
    # === 小周期(2日/MA5)：±1 ===
    small = 0
    if chg1 > 3: small = 1
    elif chg1 > 0: small = 0
    elif chg1 > -2: small = 0
    else: small = -1
    
    raw_score = big + mid + small  # -6~+6
    
    if raw_score >= 5: tags.append(f"三周期共振{raw_score:+d}🔥")
    elif raw_score >= 3: tags.append(f"多周期偏多{raw_score:+d}")
    elif raw_score <= -3: tags.append(f"多周期偏空{raw_score:+d}⚠")
    
    # 映射到max_score（-6~+6 → 0~max_score）
    s = int((raw_score + 6) / 12 * max_score + 0.5)
    
    return min(max_score, max(0, s)), tags

def score_fundamental(c, max_score):
    """S9-基本面(PE+市值+利润增速+行业地位) — v3新增"""
    s = 0; tags = []
    pe = c.get('pe', None)
    mv = c.get('total_mv', None)  # 总市值（万元）
    circ_mv = c.get('circ_mv', None)  # 流通市值（万元）
    profit_yoy = c.get('profit_yoy', None)  # 净利润同比增速
    roe = c.get('roe', None)
    industry_rank = c.get('industry_mv_rank', 99)  # 行业内市值排名
    
    # 维度1: PE估值（0<PE<20最佳）
    if pe is not None and pe > 0:
        if pe < 15: s += 3; tags.append(f"PE={pe:.0f}低估")
        elif pe < 25: s += 2; tags.append(f"PE={pe:.0f}")
        elif pe < 40: s += 1
        elif pe > 200: s -= 1; tags.append(f"PE={pe:.0f}⚠")
    elif pe is not None and pe < 0:
        s -= 1; tags.append("PE<0亏损")
    
    # 维度2: 流通市值甜区（30-150亿最佳）
    if circ_mv is not None:
        mv_yi = circ_mv / 10000  # 万→亿
        if 30 <= mv_yi <= 150: s += 2; tags.append(f"市值{mv_yi:.0f}亿✅")
        elif 15 <= mv_yi < 30: s += 1; tags.append(f"市值{mv_yi:.0f}亿小")
        elif 150 < mv_yi <= 500: s += 1; tags.append(f"市值{mv_yi:.0f}亿")
        elif mv_yi > 500: s += 0; tags.append(f"市值{mv_yi:.0f}亿大")
        else: s += 0; tags.append(f"市值{mv_yi:.0f}亿微")
    
    # 维度3: 净利润增速
    if profit_yoy is not None:
        if profit_yoy > 50: s += 2; tags.append(f"利润+{profit_yoy:.0f}%🔥")
        elif profit_yoy > 20: s += 2; tags.append(f"利润+{profit_yoy:.0f}%")
        elif profit_yoy > 0: s += 1
        elif profit_yoy < -20: s -= 1; tags.append(f"利润{profit_yoy:.0f}%⚠")
    
    # 维度4: 行业地位（市值排名）
    if industry_rank <= 3: s += 2; tags.append(f"行业TOP{industry_rank}")
    elif industry_rank <= 10: s += 1; tags.append(f"行业TOP{industry_rank}")
    
    # 维度5: ROE质量
    if roe is not None:
        if roe > 15: s += 1; tags.append(f"ROE={roe:.0f}%优")
        elif roe < 0: s -= 1
    
    return min(max_score, max(0, s)), tags


def score_all(c, sector_zt, emotion_stage, weights, market_data=None, bci_score=0):
    """9 Skill综合评分（v3.3 — BCI板块完整性整合）"""
    all_tags = []
    
    s1, t1 = score_txcg(c, sector_zt, emotion_stage, weights['TXCG'], bci_score)
    s2, t2 = score_yuanziyuan(c, weights['元子元'])
    s3, t3 = score_camellia(c, sector_zt, weights['山茶花'], market_data, bci_score)
    s4, t4 = score_mistery(c, weights['Mistery'])
    s5, t5 = score_tds(c, weights['TDS'])
    s6, t6 = score_wr(c, weights['百胜WR'])
    s7, t7 = score_event(c, sector_zt, weights['事件驱动'], bci_score)
    s8, t8 = score_multi_period(c, weights['多周期'])
    s9, t9 = score_fundamental(c, weights['基本面'])
    
    for t in [t1, t2, t3, t4, t5, t6, t7, t8, t9]:
        all_tags.extend(t)
    
    # === TXCG六大模型量化加分(0-5) ===
    txcg_bonus = 0
    chg1 = c.get('pct_chg', 0); is_zt = c.get('is_zt', False)
    chg5 = c.get('chg5', 0)
    closes = c.get('closes', []); highs = c.get('highs', [])
    opens = c.get('opens', []); lows = c.get('lows', [])
    
    # 模型1：连板竞争（涨停+板块内有竞争=晋级机会）
    if is_zt and sector_zt >= 3:
        txcg_bonus += 1  # 板块内多只涨停=连板竞争激烈
    # 模型2：分歧期策略（超跌轮动/新方向首板）
    if chg5 < -5 and chg1 > 3:
        txcg_bonus += 1  # 超跌人气轮动
    # 模型3：反包修复（前一天大阴线+今天反包）
    if len(closes) >= 3 and len(opens) >= 3:
        prev_chg = (closes[-2] - closes[-3]) / closes[-3] * 100 if closes[-3] > 0 else 0
        if prev_chg < -3 and chg1 > 2:
            txcg_bonus += 1; all_tags.append("TXCG反包")
    # 模型4：承接战法（均线附近+小涨=承接）
    ma5 = c.get('ma5', 0)
    if ma5 > 0 and len(closes) >= 1 and abs(closes[-1] - ma5) / ma5 < 0.02 and chg1 > 0:
        txcg_bonus += 1
    # 模型5：上影线/大长腿（长下影线=资金回流）
    if len(closes) >= 2 and len(opens) >= 2 and len(lows) >= 2:
        body_prev = abs(closes[-2] - opens[-2])
        lower_shadow_prev = min(closes[-2], opens[-2]) - lows[-2]
        if lower_shadow_prev > body_prev * 2 and lower_shadow_prev > 0 and chg1 > 0:
            txcg_bonus += 1; all_tags.append("TXCG大长腿")
    # 模型6：唯一性（板块内唯一涨停=辨识度最高）
    if is_zt and sector_zt == 1:
        txcg_bonus += 1; all_tags.append("TXCG唯一涨停")
    txcg_bonus = min(txcg_bonus, 5)
    
    total = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + txcg_bonus
    max_total = sum(weights.values()) + 5  # 加上TXCG六大模型的5分
    
    return total, max_total, s1, s2, s3, s4, s5, s6, s7, s8, s9, all_tags


def main():
    if len(sys.argv) < 3:
        print("用法: python3 sector_deep_pick_v2.py 20260415 化学制药 医药商业 ... [情绪阶段]")
        return
    
    trade_date = sys.argv[1]
    args = sys.argv[2:]
    emotion_stage = '分歧'
    if args[-1] in ['起爆', '修复', '分歧', '退潮', '主升']:
        emotion_stage = args[-1]
        args = args[:-1]
    target_industries = args
    
    weights, w_ver, w_review = load_weights()
    max_total = sum(weights.values())
    
    print(f"=" * 110)
    print(f"板块内遍历精选 v3.2 — 9 Skill评分（全面覆盖率提升） — {trade_date}")
    print(f"方向: {', '.join(target_industries)} | 情绪: {emotion_stage}")
    print(f"权重: {w_ver} | 满分: {max_total} | 上次复盘: {w_review}")
    print(f"=" * 110)
    
    t0 = time.time()
    
    # ===== 数据获取（本地优先，API兜底） =====
    SNAPSHOT_DIR = os.path.expanduser("~/stock_data/daily_snapshot")
    snapshot_file = os.path.join(SNAPSHOT_DIR, f"{trade_date}.parquet")
    
    # 这些是最终需要的数据结构
    info_map = {}      # {code: {name, industry}}
    st_codes = set()
    basic_map = {}     # {code: {pe, circ_mv, total_mv, turnover}}
    mf_map = {}        # {code: net_inflow_亿}
    industry_rank_map = {}
    all_daily = []     # [{code, pct_chg, close, vol, amount, ...}]
    
    if os.path.exists(snapshot_file):
        # ===== 本地模式 ⚡ =====
        import pandas as pd
        print(f"\n📂 读取本地快照...", end="", flush=True)
        df_snap = pd.read_parquet(snapshot_file)
        
        for _, row in df_snap.iterrows():
            code = str(row['ts_code'])
            name = str(row.get('name', ''))
            industry = str(row.get('industry', ''))
            info_map[code] = {'name': name, 'industry': industry}
            if name.startswith(('ST', '*ST')): st_codes.add(code)
            
            pe = row.get('pe_ttm', None)
            circ_mv = row.get('circ_mv', None)
            total_mv = row.get('total_mv', None)
            basic_map[code] = {
                'pe': float(pe) if pd.notna(pe) and pe != 0 else 0,
                'circ_mv': float(circ_mv) if pd.notna(circ_mv) else 0,
                'total_mv': float(total_mv) if pd.notna(total_mv) else 0,
                'turnover': float(row.get('turnover_rate', 0)) if pd.notna(row.get('turnover_rate')) else 0,
            }
            
            net_mf = row.get('net_mf_amount', None)
            mf_map[code] = float(net_mf) / 10000 if pd.notna(net_mf) else 0
            
            all_daily.append({
                'code': code, 'pct_chg': float(row.get('pct_chg', 0) or 0),
                'close': float(row.get('close', 0) or 0),
                'vol': float(row.get('vol', 0) or 0),
                'amount': float(row.get('amount', 0) or 0),
            })
        
        print(f" {len(all_daily)}只 ({time.time()-t0:.2f}s) ⚡本地模式")
    
    else:
        # ===== API模式（兜底） =====
        print(f"\n⚠️ 本地快照不存在，走API（建议先运行 daily_data_sync.py {trade_date}）")
        
        print(f"[1/4] 日线...", end="", flush=True)
        f_d, items_d = ts_api("daily", trade_date=trade_date)
        fi_d = {f: i for i, f in enumerate(f_d)}
        for item in items_d:
            code = item[fi_d['ts_code']]
            all_daily.append({
                'code': code,
                'pct_chg': float(item[fi_d['pct_chg']] or 0),
                'close': float(item[fi_d['close']] or 0),
                'vol': float(item[fi_d['vol']] or 0),
                'amount': float(item[fi_d['amount']] or 0),
            })
        print(f" {len(all_daily)}只 ({time.time()-t0:.1f}s)")
        
        print(f"[2/4] 基本信息...", end="", flush=True)
        t1 = time.time()
        fn, ni = ts_api("stock_basic", exchange="", list_status="L", fields="ts_code,name,industry")
        if ni:
            fi_n = {f: i for i, f in enumerate(fn)}
            for item in ni:
                code = item[fi_n['ts_code']]
                name = item[fi_n.get('name', '')]
                info_map[code] = {'name': name, 'industry': item[fi_n.get('industry', '')]}
                if name.startswith(('ST', '*ST')): st_codes.add(code)
        print(f" {len(info_map)}只 ({time.time()-t1:.1f}s)")
        
        print(f"[3/4] PE/市值...", end="", flush=True)
        t2 = time.time()
        f_db, items_db = ts_api("daily_basic", trade_date=trade_date,
                                 fields="ts_code,pe_ttm,pb,total_mv,circ_mv,turnover_rate")
        if items_db:
            fi_db = {f: i for i, f in enumerate(f_db)}
            for item in items_db:
                code = item[fi_db['ts_code']]
                try:
                    basic_map[code] = {
                        'pe': float(item[fi_db.get('pe_ttm', 0)] or 0),
                        'circ_mv': float(item[fi_db.get('circ_mv', 0)] or 0),
                        'total_mv': float(item[fi_db.get('total_mv', 0)] or 0),
                        'turnover': float(item[fi_db.get('turnover_rate', 0)] or 0),
                    }
                except: pass
        print(f" {len(basic_map)}条 ({time.time()-t2:.1f}s)")
        
        print(f"[4/4] 资金流向...", end="", flush=True)
        t3 = time.time()
        f_mf, items_mf = ts_api("moneyflow", trade_date=trade_date)
        if items_mf:
            fi_mf = {f: i for i, f in enumerate(f_mf)}
            for item in items_mf:
                code = item[fi_mf['ts_code']]
                try:
                    buy = float(item[fi_mf.get('buy_md_amount', 0)] or 0) + float(item[fi_mf.get('buy_lg_amount', 0)] or 0)
                    sell = float(item[fi_mf.get('sell_md_amount', 0)] or 0) + float(item[fi_mf.get('sell_lg_amount', 0)] or 0)
                    mf_map[code] = (buy - sell) / 10000
                except: mf_map[code] = 0
        print(f" {len(mf_map)}条 ({time.time()-t3:.1f}s)")
    
    # ===== 行业市值排名（两种模式通用） =====
    industry_mv = {}
    for code, bm in basic_map.items():
        ind = info_map.get(code, {}).get('industry', '')
        if ind:
            if ind not in industry_mv: industry_mv[ind] = []
            industry_mv[ind].append((code, bm.get('total_mv', 0)))
    for ind, items_list in industry_mv.items():
        for rank, (code, mv) in enumerate(sorted(items_list, key=lambda x: x[1], reverse=True), 1):
            industry_rank_map[code] = rank
    
    # ===== 筛选候选 =====
    candidates = []
    sector_zt = 0
    for d in all_daily:
        code = d['code']
        if code in st_codes: continue
        info = info_map.get(code, {})
        if info.get('industry', '') not in target_industries: continue
        if d['pct_chg'] >= 9.5: sector_zt += 1
        if d['pct_chg'] < 1: continue
        
        bm = basic_map.get(code, {})
        c = {
            'code': code, 'name': info.get('name', ''), 'industry': info.get('industry', ''),
            'close': d['close'], 'pct_chg': d['pct_chg'],
            'vol': d['vol'], 'amount': d['amount'],
            'is_zt': d['pct_chg'] >= 9.5,
            'pe': bm.get('pe', None) if bm.get('pe', 0) != 0 else None,
            'circ_mv': bm.get('circ_mv', None) if bm.get('circ_mv', 0) != 0 else None,
            'total_mv': bm.get('total_mv', None),
            'turnover': bm.get('turnover', None),
            'net_inflow': mf_map.get(code, 0),
            'industry_mv_rank': industry_rank_map.get(code, 99),
        }
        candidates.append(c)
    
    print(f"\n候选: {len(candidates)}只 | 板块涨停: {sector_zt}只")
    
    # ===== K线数据（Ashare，读最近30天的本地快照或API） =====
    t_kl = time.time()
    
    # 优先尝试从多天快照拼K线（超快！）
    snapshot_dates = []
    if os.path.exists(SNAPSHOT_DIR):
        import pandas as pd
        all_snaps = sorted([f[:8] for f in os.listdir(SNAPSHOT_DIR) if f.endswith('.parquet') and f != 'stock_basic.parquet' and f[:8] <= trade_date])
        snapshot_dates = all_snaps[-30:]  # 最多取30天
    
    if len(snapshot_dates) >= 10:
        # 有足够多的本地快照，从中拼K线
        print(f"📂 从{len(snapshot_dates)}天本地快照拼K线...", end="", flush=True)
        kline_data = {}  # {code: {closes:[], highs:[], lows:[], vols:[]}}
        
        for snap_date in snapshot_dates:
            sf = os.path.join(SNAPSHOT_DIR, f"{snap_date}.parquet")
            try:
                df_s = pd.read_parquet(sf, columns=['ts_code', 'close', 'high', 'low', 'vol'])
                for _, row in df_s.iterrows():
                    code = str(row['ts_code'])
                    if code not in kline_data:
                        kline_data[code] = {'closes': [], 'highs': [], 'lows': [], 'vols': []}
                    kline_data[code]['closes'].append(float(row.get('close', 0) or 0))
                    kline_data[code]['highs'].append(float(row.get('high', 0) or 0))
                    kline_data[code]['lows'].append(float(row.get('low', 0) or 0))
                    kline_data[code]['vols'].append(float(row.get('vol', 0) or 0))
            except: pass
        
        valid_count = 0
        for c in candidates:
            kd = kline_data.get(c['code'])
            if kd and len(kd['closes']) >= 5:
                closes = kd['closes']; highs = kd['highs']; lows = kd['lows']; vols = kd['vols']
                c['closes'] = closes; c['highs'] = highs; c['lows'] = lows
                c['chg5'] = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
                c['chg10'] = (closes[-1] / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
                c['ma5'] = np.mean(closes[-5:]); c['ma10'] = np.mean(closes[-10:])
                c['ma20'] = np.mean(closes[-20:]) if len(closes) >= 20 else np.mean(closes)
                if c['ma5'] > c['ma10'] > c['ma20']: c['ma_tag'] = '多头'
                elif c['ma5'] > c['ma10']: c['ma_tag'] = '短多'
                else: c['ma_tag'] = '弱'
                avg_v5 = np.mean(vols[-6:-1]) if len(vols) >= 6 else np.mean(vols)
                c['vol_ratio'] = vols[-1] / avg_v5 if avg_v5 > 0 else 0
                std20 = np.std(closes[-20:]) if len(closes) >= 20 else np.std(closes)
                c['bbw'] = (4 * std20) / c['ma20'] if c['ma20'] > 0 else 0
                c['valid'] = True; valid_count += 1
            else:
                c['valid'] = False
        
        print(f" 有效{valid_count}/{len(candidates)} ({time.time()-t_kl:.1f}s) ⚡")
    
    else:
        # 本地快照不够，用Ashare逐只拉
        print(f"获取K线(Ashare)...", end="", flush=True)
        sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
        import warnings; warnings.filterwarnings('ignore')
        from Ashare import get_price
        
        valid_count = 0
        for c in candidates:
            try:
                ts_code = c['code']
                ashare_code = ('sh' if ts_code.endswith('.SH') else 'sz') + ts_code[:6]
                df = get_price(ashare_code, frequency='1d', count=30)
                if df is None or len(df) < 5: c['valid'] = False; continue
                closes = df.close.values.astype(float).tolist()
                highs = df.high.values.astype(float).tolist()
                lows = df.low.values.astype(float).tolist()
                vols = df.volume.values.astype(float).tolist()
                c['closes'] = closes; c['highs'] = highs; c['lows'] = lows
                c['chg5'] = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
                c['chg10'] = (closes[-1] / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
                c['ma5'] = np.mean(closes[-5:]); c['ma10'] = np.mean(closes[-10:])
                c['ma20'] = np.mean(closes[-20:]) if len(closes) >= 20 else np.mean(closes)
                if c['ma5'] > c['ma10'] > c['ma20']: c['ma_tag'] = '多头'
                elif c['ma5'] > c['ma10']: c['ma_tag'] = '短多'
                else: c['ma_tag'] = '弱'
                avg_v5 = np.mean(vols[-6:-1]) if len(vols) >= 6 else np.mean(vols)
                c['vol_ratio'] = vols[-1] / avg_v5 if avg_v5 > 0 else 0
                std20 = np.std(closes[-20:]) if len(closes) >= 20 else np.std(closes)
                c['bbw'] = (4 * std20) / c['ma20'] if c['ma20'] > 0 else 0
                c['valid'] = True; valid_count += 1
            except: c['valid'] = False
            time.sleep(0.08)
        print(f" 有效{valid_count}/{len(candidates)} ({time.time()-t_kl:.1f}s)")
    
    # ===== 评分（第一轮） =====
    valid = [c for c in candidates if c.get('valid')]
    
    # ===== 60分钟K线获取（用于WR-3底倍量柱检测） =====
    LOCAL_60M_DIR = os.path.expanduser('~/Downloads/2026/60min')
    t_60m = time.time()
    wr3_count = 0
    
    # 优先从本地CSV读取60分钟K线
    if os.path.exists(LOCAL_60M_DIR):
        print(f"📂 读取60分钟K线(WR-3)...", end="", flush=True)
        for c in valid:
            ts_code = c['code']
            # ts_code格式: 000001.SZ → 文件名: sz000001.csv
            code6 = ts_code[:6]
            prefix = 'sh' if ts_code.endswith('.SH') else ('sz' if ts_code.endswith('.SZ') else 'bj')
            csv_file = os.path.join(LOCAL_60M_DIR, f"{prefix}{code6}.csv")
            
            if os.path.exists(csv_file):
                try:
                    import pandas as pd
                    df_60 = pd.read_csv(csv_file, encoding='utf-8')
                    df_60.columns = ['date','time','open','high','low','close','volume','amount']
                    # 取最近30根60分钟K线
                    df_60 = df_60.tail(30)
                    if len(df_60) >= 12:
                        c['kline_60m'] = {
                            'closes': df_60['close'].astype(float).tolist(),
                            'highs': df_60['high'].astype(float).tolist(),
                            'lows': df_60['low'].astype(float).tolist(),
                            'vols': df_60['volume'].astype(float).tolist(),
                        }
                        wr3_count += 1
                except:
                    pass
        print(f" {wr3_count}/{len(valid)}只有60m数据 ({time.time()-t_60m:.1f}s)")
    
    # 本地没有的，用Ashare补充
    need_ashare_60m = [c for c in valid if 'kline_60m' not in c]
    if need_ashare_60m and len(need_ashare_60m) <= 30:  # 只补少量，避免太慢
        try:
            sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
            import warnings; warnings.filterwarnings('ignore')
            from Ashare import get_price as get_price_60m
            print(f"🌐 Ashare补充60m K线({len(need_ashare_60m)}只)...", end="", flush=True)
            for c in need_ashare_60m:
                try:
                    ts_code = c['code']
                    ashare_code = ('sh' if ts_code.endswith('.SH') else 'sz') + ts_code[:6]
                    df_60 = get_price_60m(ashare_code, frequency='60m', count=30)
                    if df_60 is not None and len(df_60) >= 12:
                        c['kline_60m'] = {
                            'closes': df_60.close.values.astype(float).tolist(),
                            'highs': df_60.high.values.astype(float).tolist(),
                            'lows': df_60.low.values.astype(float).tolist(),
                            'vols': df_60.volume.values.astype(float).tolist(),
                        }
                        wr3_count += 1
                except:
                    pass
                time.sleep(0.1)
            print(f" 补充后{wr3_count}/{len(valid)}只 ({time.time()-t_60m:.1f}s)")
        except ImportError:
            print(f" ⚠ Ashare不可用，跳过60m补充")
    
    # ===== 涨停股封板时间检测（用Ashare 5分钟K线，WR-1条件7） =====
    zt_stocks = [c for c in valid if c.get('is_zt')]
    if zt_stocks:
        try:
            sys.path.insert(0, '/Users/ecustkiller/.workbuddy/skills/ashare-data/scripts')
            import warnings; warnings.filterwarnings('ignore')
            from Ashare import get_price as get_price_5m
            print(f"⏱ 检测封板时间({len(zt_stocks)}只涨停)...", end="", flush=True)
            t_zt = time.time()
            zt_detected = 0
            for c in zt_stocks:
                try:
                    ts_code = c['code']
                    ashare_code = ('sh' if ts_code.endswith('.SH') else 'sz') + ts_code[:6]
                    df_5m = get_price_5m(ashare_code, frequency='5m', count=48)
                    if df_5m is not None and len(df_5m) >= 5:
                        # 涨停价 = 收盘价（因为已确认涨停）
                        zt_price = c['close']
                        # 找第一根 high 触及涨停价的5分钟K线
                        for idx_row in range(len(df_5m)):
                            row_high = float(df_5m.iloc[idx_row]['high'])
                            if abs(row_high - zt_price) / zt_price < 0.002:  # 0.2%容差
                                # 获取时间
                                ts_idx = df_5m.index[idx_row]
                                time_str = str(ts_idx)
                                # 提取 HH:MM
                                if ' ' in time_str:
                                    hm = time_str.split(' ')[1][:5]  # "10:30"
                                else:
                                    hm = time_str[11:16] if len(time_str) > 16 else time_str[:5]
                                c['zt_time'] = hm
                                zt_detected += 1
                                break
                except:
                    pass
                time.sleep(0.1)
            print(f" {zt_detected}/{len(zt_stocks)}只检测到 ({time.time()-t_zt:.1f}s)")
        except ImportError:
            print(f" ⚠ Ashare不可用，跳过封板时间检测")
    
    # 构建市场级别数据（用于山茶花垃圾时间检测等）
    # 注意：这些数据在板块内遍历时可能不完整，需要外部传入或从全市场数据计算
    market_data = None  # 默认不传，由外部调用时注入
    
    # ===== BCI板块完整性指数计算（v3.3新增） =====
    # 在板块内遍历模式下，计算当前板块的BCI得分
    bci_score_val = 0
    n_zt = sector_zt
    if n_zt > 0:
        bci_score_val = 0
        # BCI-1: 涨停数量(0-20)
        if n_zt >= 8: bci_score_val += 20
        elif n_zt >= 5: bci_score_val += 17
        elif n_zt >= 3: bci_score_val += 13
        elif n_zt >= 2: bci_score_val += 8
        else: bci_score_val += 3
        
        # BCI-2: 梯队层次(0-20) — 用涨停股的连板情况估算
        zt_valid = [c for c in valid if c.get('is_zt')]
        首板数 = len(zt_valid)
        连板数_est = 0
        # 检查前一天也涨停的（简化：用5日涨幅>15%估算连板）
        for c_zt in zt_valid:
            if c_zt.get('chg5', 0) > 15:
                连板数_est += 1
        层级数 = (1 if 首板数 > 连板数_est else 0) + (1 if 连板数_est > 0 else 0)
        bci_score_val += min(层级数 * 6 + min(连板数_est, 3) * 2, 20)
        
        # BCI-3: 龙头强度(0-15) — 用最大成交额估算
        max_amt = max((c.get('amount', 0) for c in zt_valid), default=0)
        if max_amt > 500000: bci_score_val += 15
        elif max_amt > 200000: bci_score_val += 12
        elif max_amt > 100000: bci_score_val += 9
        elif max_amt > 50000: bci_score_val += 6
        else: bci_score_val += 3
        
        # BCI-4: 换手板比例(0-10)
        换手板 = sum(1 for c_zt in zt_valid if c_zt.get('turnover', 0) and c_zt['turnover'] > 8)
        换手比 = 换手板 / max(len(zt_valid), 1)
        bci_score_val += min(int(换手比 * 10), 10)
        
        # BCI-5: 板块内聚度(0-15) — 涨停股涨幅一致性
        pct_list = [c.get('pct_chg', 0) for c in zt_valid]
        if len(pct_list) >= 2:
            pct_std = np.std(pct_list)
            if pct_std < 1: bci_score_val += 15
            elif pct_std < 2: bci_score_val += 10
            elif pct_std < 3: bci_score_val += 7
            else: bci_score_val += 3
        else:
            bci_score_val += 5
        
        bci_score_val = min(bci_score_val, 100)
    
    print(f"  BCI板块完整性: {bci_score_val}/100 ({'⭐5极完整' if bci_score_val >= 80 else ('⭐4较完整' if bci_score_val >= 60 else ('⭐3一般' if bci_score_val >= 40 else '⭐2较弱'))})")
    
    for c in valid:
        result = score_all(c, sector_zt, emotion_stage, weights, market_data, bci_score_val)
        c['total'] = result[0]
        c['max_total'] = result[1]
        c['s1'], c['s2'], c['s3'], c['s4'], c['s5'] = result[2:7]
        c['s6'], c['s7'], c['s8'], c['s9'] = result[7:11]
        c['tags'] = result[11]
    
    valid.sort(key=lambda x: x['total'], reverse=True)
    
    # ===== 对TOP25补查财务数据 =====
    t6 = time.time()
    top_n = min(25, len(valid))
    print(f"补查TOP{top_n}财务...", end="", flush=True)
    fina_count = 0
    year = int(trade_date[:4])
    for c in valid[:top_n]:
        for period in [f"{year}0331", f"{year-1}1231", f"{year-1}0930"]:
            try:
                f_fi, items_fi = ts_api("fina_indicator", ts_code=c['code'], period=period,
                                         fields="ts_code,roe,netprofit_yoy,q_profit_yoy")
                if items_fi:
                    fi_fi = {f_: i for i, f_ in enumerate(f_fi)}
                    item = items_fi[0]
                    roe_val = item[fi_fi.get('roe', 0)] if 'roe' in fi_fi else None
                    profit_val = item[fi_fi.get('netprofit_yoy', 0)] if 'netprofit_yoy' in fi_fi else None
                    if not profit_val:
                        profit_val = item[fi_fi.get('q_profit_yoy', 0)] if 'q_profit_yoy' in fi_fi else None
                    if roe_val: c['roe'] = float(roe_val)
                    if profit_val: c['profit_yoy'] = float(profit_val)
                    fina_count += 1; break
            except: pass
            time.sleep(0.15)
    
    for c in valid[:top_n]:
        s9_new, t9_new = score_fundamental(c, weights['基本面'])
        c['total'] = c['total'] - c['s9'] + s9_new
        c['s9'] = s9_new
        c['tags'] = [t for t in c['tags'] if 'ROE' not in t and '利润' not in t] + t9_new
    
    valid.sort(key=lambda x: x['total'], reverse=True)
    print(f" {fina_count}/{top_n}只 ({time.time()-t6:.1f}s)")
    
    total_time = time.time() - t0
    print(f"\n总耗时: {total_time:.1f}s")
    
    # ===== 输出 =====
    w = weights
    print(f"\n{'#':>3} {'名称':<10} {'行业':<6} {'收盘':>6} {'涨跌':>6} {'5日':>6} {'PE':>6} {'市值亿':>6} {'净入':>5} {'总分':>4} {'TXCG':>4} {'元子':>4} {'山茶':>4} {'Mis':>4} {'TDS':>4} {'WR':>4} {'事件':>4} {'多周':>3} {'基本':>3}")
    print("-" * 120)
    for i, c in enumerate(valid[:30]):
        zt = "🔴" if c['is_zt'] else ""
        pe_s = f"{c['pe']:.0f}" if c.get('pe') else "—"
        mv_s = f"{c['circ_mv']/10000:.0f}" if c.get('circ_mv') and c['circ_mv'] > 0 else "—"
        print(f"{i+1:>3} {c['name']:<10} {c['industry']:<6} {c['close']:>6.2f} {c['pct_chg']:>+5.1f}%{zt} {c.get('chg5',0):>+5.1f}% {pe_s:>6} {mv_s:>6} {c.get('net_inflow',0):>+4.1f}亿 {c['total']:>4d} {c['s1']:>4d} {c['s2']:>4d} {c['s3']:>4d} {c['s4']:>4d} {c['s5']:>4d} {c['s6']:>4d} {c['s7']:>4d} {c['s8']:>3d} {c['s9']:>3d}")
    
    # TOP5
    print(f"\n{'=' * 100}")
    print(f"⭐ 板块内TOP5（9 Skill综合评分 / 满分{max_total}）")
    print(f"{'=' * 100}")
    for i, c in enumerate(valid[:5]):
        pe_s = f"PE={c['pe']:.0f}" if c.get('pe') else "PE=—"
        mv_s = f"流通{c['circ_mv']/10000:.0f}亿" if c.get('circ_mv') and c['circ_mv'] > 0 else "市值=—"
        roe_s = f"ROE={c['roe']:.0f}%" if c.get('roe') else ""
        profit_s = f"利润增速{c['profit_yoy']:+.0f}%" if c.get('profit_yoy') else ""
        rank_s = f"行业TOP{c.get('industry_mv_rank',99)}" if c.get('industry_mv_rank', 99) <= 20 else ""
        
        print(f"\n  {i+1}. {c['name']}({c['code']}) — 总分{c['total']}/{max_total}")
        print(f"     收{c['close']:.2f} 涨{c['pct_chg']:+.1f}% 5日{c.get('chg5',0):+.1f}% 净入{c.get('net_inflow',0):+.1f}亿 量{c.get('vol_ratio',0):.1f}x BBW={c.get('bbw',0):.3f} {c.get('ma_tag','')}")
        print(f"     {pe_s} {mv_s} {roe_s} {profit_s} {rank_s}")
        print(f"     TXCG={c['s1']}/{w['TXCG']} 元子={c['s2']}/{w['元子元']} 山茶={c['s3']}/{w['山茶花']} Mis={c['s4']}/{w['Mistery']} TDS={c['s5']}/{w['TDS']} WR={c['s6']}/{w['百胜WR']} 事件={c['s7']}/{w['事件驱动']} 多周={c['s8']}/{w['多周期']} 基本={c['s9']}/{w['基本面']}")
        key_tags = [t for t in c['tags'] if any(k in t for k in ['🔥', '超跌', 'BBW', '量', '净', '突破', '趋势', 'WR', '涨停', '安全', '大阳', '多头', 'PE', '市值', '利润', '行业TOP', 'ROE'])]
        print(f"     关键: {', '.join(key_tags[:8])}")

if __name__ == '__main__':
    main()
