#!/usr/bin/env python3
"""
市场情绪数据统一获取模块 (market_sentiment.py)
==============================================
所有脚本共用此模块获取市场情绪数据，确保数据口径一致。

数据口径说明：
- 涨停/炸板/跌停：使用 AKShare 东方财富涨停板行情接口
  - stock_zt_pool_em: 涨停池（不含ST、不含北交所）
  - stock_zt_pool_zbgc_em: 炸板池（不含ST、不含北交所）
  - stock_zt_pool_dtgc_em: 跌停池（不含ST、不含北交所）
- ST涨停/跌停：使用 Tushare daily 接口补充计算
- 赚钱效应：使用 Tushare daily 全市场涨跌统计
- 封板率 = 涨停数 / (涨停数 + 炸板数)
- 行业涨停统计：使用 AKShare 涨停池的"所属行业"字段

注意：
- 封板率和BJCJ情绪判定基于【非ST口径】（AKShare），这是短线交易的标准口径
- ST涨停/跌停单独统计，仅作参考
- 所有数据均为收盘后数据

版本: v2.0
日期: 2026-04-15
"""

import requests
import time
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ===== Tushare API =====
TUSHARE_TOKEN = "ad56243b601d82fd5c4aaf04b72d4d9d567401898d46c20f4d905d59"

def _ts_api(api_name, params=None, fields=None):
    """Tushare API 统一调用"""
    d = {"api_name": api_name, "token": TUSHARE_TOKEN, "params": params or {}}
    if fields:
        d["fields"] = fields
    try:
        r = requests.post("http://api.tushare.pro", json=d, timeout=30)
        j = r.json()
        if j.get("code") != 0:
            print(f"  [Tushare Error] {api_name}: {j.get('msg', '')[:80]}")
            return pd.DataFrame()
        return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
    except Exception as e:
        print(f"  [Tushare Exception] {api_name}: {e}")
        return pd.DataFrame()


# ===== 交易日历 =====
def get_trade_dates(start_date="20260101", end_date=None):
    """获取交易日历，返回排序后的交易日列表"""
    if end_date is None:
        end_date = time.strftime("%Y%m%d")
    cal = _ts_api("trade_cal",
                  {"exchange": "SSE", "start_date": start_date, "end_date": end_date, "is_open": "1"},
                  "cal_date")
    time.sleep(0.5)
    if cal.empty:
        return []
    return sorted(cal["cal_date"].tolist())


def get_t_minus_n(trade_dates, base_date, n):
    """获取base_date往前第n个交易日"""
    if base_date not in trade_dates:
        # 找最近的交易日
        earlier = [d for d in trade_dates if d <= base_date]
        if not earlier:
            return trade_dates[0]
        base_date = earlier[-1]
    idx = trade_dates.index(base_date)
    target_idx = max(0, idx - n)
    return trade_dates[target_idx]


# ===== AKShare行业映射 =====
# AKShare(东方财富)行业 ↔ Tushare行业 双向映射表
# 用于涨停池行业统计与个股行业匹配
INDUSTRY_MAP_AK_TO_TS = {
    "中药Ⅱ": ["中成药", "中药"],
    "化学制药": ["化学制药", "西药"],
    "医疗器械": ["医疗器械"],
    "医疗服务": ["医疗服务", "医院"],
    "生物制品": ["生物制品", "疫苗"],
    "医药商业": ["医药商业", "医药流通"],
    "消费电子": ["元器件", "电子元件", "消费电子"],
    "半导体": ["半导体", "芯片"],
    "光学光电": ["光学光电", "LED"],
    "通信设备": ["通信设备"],
    "通信服务": ["通信服务"],
    "计算机设备": ["计算机设备", "IT设备"],
    "软件开发": ["软件服务", "软件开发"],
    "互联网服务": ["互联网", "网络服务"],
    "电力设备": ["电气设备", "电力设备"],
    "电网设备": ["电网设备", "输变电"],
    "储能设备": ["储能", "电池"],
    "光伏设备": ["光伏", "太阳能"],
    "风电设备": ["风电", "风能"],
    "汽车零部件": ["汽车配件", "汽车零部件"],
    "汽车整车": ["汽车整车", "乘用车"],
    "专用设备": ["专用机械", "专用设备"],
    "通用设备": ["通用机械", "通用设备"],
    "工程机械": ["工程机械"],
    "航天航空": ["航天装备", "航空"],
    "国防军工": ["军工", "国防"],
    "船舶制造": ["船舶", "船舶制造"],
    "工业金属": ["工业金属", "铝", "铜"],
    "贵金属": ["贵金属", "黄金"],
    "钢铁": ["钢铁", "特钢"],
    "煤炭开采": ["煤炭开采"],
    "石油开采": ["石油开采"],
    "化学原料": ["化工原料"],
    "化学制品": ["化工", "化学制品"],
    "农化制品": ["农药", "化肥"],
    "食品加工": ["食品", "食品加工"],
    "饮料制造": ["饮料", "白酒"],
    "纺织服饰": ["纺织", "服装"],
    "家用电器": ["家电", "白色家电"],
    "房地产开": ["房地产", "地产"],
    "房地产服": ["物业", "房地产服务"],
    "银行": ["银行"],
    "证券": ["证券"],
    "保险": ["保险"],
    "旅游及景": ["旅游", "景区"],
    "酒店餐饮": ["酒店", "餐饮"],
    "教育": ["教育"],
    "游戏": ["游戏"],
    "影视院线": ["影视", "传媒"],
    "广告营销": ["广告", "营销"],
    "包装印刷": ["包装", "印刷"],
    "造纸": ["造纸"],
    "环保": ["环保", "环境治理"],
    "水务": ["水务"],
    "电力": ["电力", "火电", "水电"],
    "燃气": ["燃气"],
    "交通运输": ["交通运输", "物流"],
    "航运港口": ["航运", "港口"],
    "铁路公路": ["铁路", "公路"],
    "航空机场": ["航空", "机场"],
    "农牧饲渔": ["农业", "畜牧", "渔业"],
    "饲料": ["饲料"],
    "种植业": ["种植", "粮食"],
    "装修装饰": ["装修", "装饰"],
    "工程建设": ["建筑", "工程"],
    "水泥建材": ["水泥", "建材"],
    "专业工程": ["专业工程"],
    "照明设备": ["照明"],
}

# 反向映射：Tushare行业 → AKShare行业
INDUSTRY_MAP_TS_TO_AK = {}
for ak_ind, ts_inds in INDUSTRY_MAP_AK_TO_TS.items():
    for ts_ind in ts_inds:
        INDUSTRY_MAP_TS_TO_AK[ts_ind] = ak_ind


def match_industry(ts_industry, ak_ind_zt_dict):
    """
    将Tushare行业名匹配到AKShare涨停池行业统计中
    返回匹配到的涨停家数
    """
    if not ts_industry:
        return 0

    # 1. 直接匹配
    if ts_industry in ak_ind_zt_dict:
        return ak_ind_zt_dict[ts_industry]

    # 2. 通过映射表匹配
    ak_ind = INDUSTRY_MAP_TS_TO_AK.get(ts_industry)
    if ak_ind and ak_ind in ak_ind_zt_dict:
        return ak_ind_zt_dict[ak_ind]

    # 3. 模糊匹配（前2字）
    if len(ts_industry) >= 2:
        for ak_name, cnt in ak_ind_zt_dict.items():
            if ts_industry[:2] in ak_name or ak_name[:2] in ts_industry:
                return cnt

    return 0


# ===== 核心：市场情绪数据获取 =====
def get_market_sentiment(date_str):
    """
    获取指定日期的完整市场情绪数据

    参数:
        date_str: 日期字符串，格式 'YYYYMMDD'

    返回:
        dict: 包含以下字段
        {
            # === 核心指标（非ST口径，用于BJCJ判定）===
            'zt_cnt': int,          # 涨停家数（不含ST）
            'zb_cnt': int,          # 炸板家数（不含ST）
            'dt_cnt': int,          # 跌停家数（不含ST）
            'fbl': float,           # 封板率 = zt/(zt+zb) * 100
            'earn_rate': float,     # 赚钱效应 = 上涨家数/总家数 * 100
            'total_amount': float,  # 全市场成交额（亿）

            # === ST补充数据 ===
            'st_zt_cnt': int,       # ST涨停家数
            'st_dt_cnt': int,       # ST跌停家数

            # === 全口径汇总（含ST，仅供参考）===
            'zt_cnt_all': int,      # 全口径涨停 = zt_cnt + st_zt_cnt
            'dt_cnt_all': int,      # 全口径跌停 = dt_cnt + st_dt_cnt

            # === 全市场涨跌统计 ===
            'total_stocks': int,    # 全市场股票数
            'up_cnt': int,          # 上涨家数
            'down_cnt': int,        # 下跌家数
            'flat_cnt': int,        # 平盘家数

            # === BJCJ-3 情绪判定 ===
            'bjcj3_phase': str,     # 情绪阶段
            'bjcj3_pos': str,       # 建议仓位
            'bjcj3_max_pct': int,   # 最大仓位百分比

            # === 板块涨停统计 ===
            'ind_zt_dict': dict,    # {行业: 涨停家数}
            'ind_zt_top10': list,   # [(行业, 家数), ...] TOP10

            # === 连板统计 ===
            'board_dist': dict,     # {连板数: 家数}
            'max_board': int,       # 最高连板数
            'max_board_stocks': list, # 最高连板股票列表

            # === 涨停池原始数据 ===
            'zt_df': DataFrame,     # 涨停池完整数据
            'zb_df': DataFrame,     # 炸板池完整数据
            'dt_df': DataFrame,     # 跌停池完整数据
            'zt_codes': set,        # 涨停股代码集合（6位）
        }
    """
    import akshare as ak

    result = {
        'date': date_str,
        'zt_cnt': 0, 'zb_cnt': 0, 'dt_cnt': 0,
        'fbl': 0.0, 'earn_rate': 0.0, 'total_amount': 0.0,
        'st_zt_cnt': 0, 'st_dt_cnt': 0,
        'zt_cnt_all': 0, 'dt_cnt_all': 0,
        'total_stocks': 0, 'up_cnt': 0, 'down_cnt': 0, 'flat_cnt': 0,
        'bjcj3_phase': '未知', 'bjcj3_pos': '0成', 'bjcj3_max_pct': 0,
        'ind_zt_dict': {}, 'ind_zt_top10': [],
        'board_dist': {}, 'max_board': 0, 'max_board_stocks': [],
        'zt_df': pd.DataFrame(), 'zb_df': pd.DataFrame(), 'dt_df': pd.DataFrame(),
        'zt_codes': set(),
    }

    # ===== 1. AKShare 涨停/炸板/跌停池（非ST口径）=====
    print(f"  [情绪] 获取 {date_str} AKShare涨停/炸板/跌停池...")
    try:
        zt_df = ak.stock_zt_pool_em(date=date_str)
        time.sleep(0.3)
        zb_df = ak.stock_zt_pool_zbgc_em(date=date_str)
        time.sleep(0.3)
        dt_df = ak.stock_zt_pool_dtgc_em(date=date_str)
        time.sleep(0.3)

        result['zt_df'] = zt_df
        result['zb_df'] = zb_df
        result['dt_df'] = dt_df
        result['zt_cnt'] = len(zt_df)
        result['zb_cnt'] = len(zb_df)
        result['dt_cnt'] = len(dt_df)

        # 封板率
        total_touch = result['zt_cnt'] + result['zb_cnt']
        result['fbl'] = round(result['zt_cnt'] / total_touch * 100, 1) if total_touch > 0 else 0.0

        # 涨停股代码集合
        if '代码' in zt_df.columns:
            result['zt_codes'] = set(zt_df['代码'].tolist())

        # 板块涨停统计
        if '所属行业' in zt_df.columns:
            ind_zt = zt_df['所属行业'].value_counts().to_dict()
            result['ind_zt_dict'] = ind_zt
            result['ind_zt_top10'] = sorted(ind_zt.items(), key=lambda x: x[1], reverse=True)[:10]

        # 连板统计
        if '连板数' in zt_df.columns:
            result['board_dist'] = zt_df['连板数'].value_counts().sort_index().to_dict()
            result['max_board'] = int(zt_df['连板数'].max())
            max_b = result['max_board']
            result['max_board_stocks'] = zt_df[zt_df['连板数'] == max_b][['代码', '名称', '连板数']].values.tolist()

        print(f"  [情绪] AKShare: 涨停{result['zt_cnt']} 炸板{result['zb_cnt']} "
              f"封板率{result['fbl']:.0f}% 跌停{result['dt_cnt']}")

    except Exception as e:
        print(f"  [情绪] ⚠️ AKShare获取失败: {e}")

    # ===== 2. Tushare 全市场涨跌统计 + ST补充 =====
    print(f"  [情绪] 获取 {date_str} Tushare全市场数据...")
    daily = _ts_api("daily", {"trade_date": date_str}, "ts_code,pct_chg,vol,amount")
    time.sleep(0.8)

    if not daily.empty:
        # 全市场涨跌
        result['total_stocks'] = len(daily)
        result['up_cnt'] = len(daily[daily['pct_chg'] > 0])
        result['down_cnt'] = len(daily[daily['pct_chg'] < 0])
        result['flat_cnt'] = len(daily[daily['pct_chg'] == 0])
        result['earn_rate'] = round(result['up_cnt'] / result['total_stocks'] * 100, 1) \
            if result['total_stocks'] > 0 else 0.0
        result['total_amount'] = round(daily['amount'].sum() / 100000, 0)  # 亿

        # ST涨停/跌停补充
        stk_basic = _ts_api("stock_basic", {"list_status": "L"}, "ts_code,name")
        time.sleep(0.8)
        if not stk_basic.empty:
            name_map = dict(zip(stk_basic['ts_code'], stk_basic['name']))
            daily['name'] = daily['ts_code'].map(name_map)

            # ST股筛选
            st_mask = daily['name'].str.contains('ST|st', na=False)
            st_daily = daily[st_mask]

            # ST涨停（5%涨停）
            result['st_zt_cnt'] = len(st_daily[st_daily['pct_chg'] >= 4.9])
            # ST跌停（-5%跌停）
            result['st_dt_cnt'] = len(st_daily[st_daily['pct_chg'] <= -4.9])

        # 全口径汇总
        result['zt_cnt_all'] = result['zt_cnt'] + result['st_zt_cnt']
        result['dt_cnt_all'] = result['dt_cnt'] + result['st_dt_cnt']

        print(f"  [情绪] Tushare: 全市场{result['total_stocks']}只 "
              f"上涨{result['up_cnt']}({result['earn_rate']:.0f}%) "
              f"下跌{result['down_cnt']} 成交{result['total_amount']:.0f}亿")
        print(f"  [情绪] ST补充: ST涨停{result['st_zt_cnt']} ST跌停{result['st_dt_cnt']}")
        print(f"  [情绪] 全口径: 涨停{result['zt_cnt_all']}(非ST{result['zt_cnt']}+ST{result['st_zt_cnt']}) "
              f"跌停{result['dt_cnt_all']}(非ST{result['dt_cnt']}+ST{result['st_dt_cnt']})")

    # ===== 3. BJCJ-3 情绪阶段判定（基于非ST口径）=====
    zt = result['zt_cnt']
    dt = result['dt_cnt']
    fbl = result['fbl']
    earn = result['earn_rate']

    if dt > 20:
        phase, pos, max_pct = '空仓期', '0成', 0
    elif fbl < 50 or dt > 15:
        phase, pos, max_pct = '防御期', '2-3成', 30
    elif 50 <= fbl < 60 or earn < 40:
        phase, pos, max_pct = '修复期', '3-4成', 40
    elif 60 <= fbl <= 80:
        phase, pos, max_pct = '正常期', '5-6成', 60
    elif fbl > 80:
        phase, pos, max_pct = '进攻期', '8成', 80
    else:
        phase, pos, max_pct = '观望期', '3成', 30

    result['bjcj3_phase'] = phase
    result['bjcj3_pos'] = pos
    result['bjcj3_max_pct'] = max_pct

    print(f"  [情绪] BJCJ-3判定: {phase} | 封板率{fbl:.0f}% 跌停{dt} 赚钱效应{earn:.0f}% → 建议仓位{pos}")

    return result


def print_sentiment_summary(s):
    """打印情绪数据摘要"""
    print(f"\n{'='*80}")
    print(f"📊 市场情绪数据 ({s['date']})")
    print(f"{'='*80}")
    print(f"  涨停: {s['zt_cnt']}只(非ST) + {s['st_zt_cnt']}只(ST) = {s['zt_cnt_all']}只(全口径)")
    print(f"  炸板: {s['zb_cnt']}只")
    print(f"  跌停: {s['dt_cnt']}只(非ST) + {s['st_dt_cnt']}只(ST) = {s['dt_cnt_all']}只(全口径)")
    print(f"  封板率: {s['fbl']:.0f}% (非ST口径: {s['zt_cnt']}/{s['zt_cnt']+s['zb_cnt']})")
    print(f"  赚钱效应: {s['earn_rate']:.0f}% ({s['up_cnt']}/{s['total_stocks']})")
    print(f"  成交额: {s['total_amount']:.0f}亿")
    print(f"  连板分布: {s['board_dist']}")
    if s['max_board_stocks']:
        print(f"  最高连板: {s['max_board']}板 {s['max_board_stocks']}")
    print(f"  涨停行业TOP10: {s['ind_zt_top10']}")
    print(f"  ─────────────────────────────────────")
    print(f"  🎯 BJCJ-3情绪判定: 【{s['bjcj3_phase']}】 建议仓位: {s['bjcj3_pos']}")
    print(f"{'='*80}\n")


def get_multi_day_sentiment(dates):
    """获取多日情绪数据，用于趋势判断"""
    results = {}
    for d in dates:
        print(f"\n--- 获取 {d} 情绪数据 ---")
        results[d] = get_market_sentiment(d)
        time.sleep(0.5)
    return results


def format_sentiment_for_plan(s):
    """
    格式化情绪数据，用于作战计划文档

    返回 Markdown 格式的情绪数据表格
    """
    lines = []
    lines.append(f"### 市场情绪数据 ({s['date']})")
    lines.append("")
    lines.append("| 指标 | 数值 | 口径说明 |")
    lines.append("|------|------|----------|")
    lines.append(f"| 涨停家数 | **{s['zt_cnt']}只** | 非ST口径(AKShare东方财富) |")
    lines.append(f"| 炸板家数 | {s['zb_cnt']}只 | 非ST口径 |")
    lines.append(f"| 封板率 | **{s['fbl']:.0f}%** | {s['zt_cnt']}/({s['zt_cnt']}+{s['zb_cnt']}) |")
    lines.append(f"| 跌停家数 | **{s['dt_cnt']}只** | 非ST口径 |")
    lines.append(f"| 赚钱效应 | **{s['earn_rate']:.0f}%** | 全市场{s['up_cnt']}/{s['total_stocks']} |")
    lines.append(f"| 成交额 | {s['total_amount']:.0f}亿 | 全市场 |")
    lines.append(f"| ST涨停 | {s['st_zt_cnt']}只 | Tushare补充 |")
    lines.append(f"| ST跌停 | {s['st_dt_cnt']}只 | Tushare补充 |")
    lines.append(f"| 全口径涨停 | {s['zt_cnt_all']}只 | 非ST{s['zt_cnt']}+ST{s['st_zt_cnt']} |")
    lines.append(f"| 全口径跌停 | {s['dt_cnt_all']}只 | 非ST{s['dt_cnt']}+ST{s['st_dt_cnt']} |")
    lines.append("")
    lines.append(f"**BJCJ-3情绪判定: 【{s['bjcj3_phase']}】 建议仓位: {s['bjcj3_pos']}**")
    lines.append("")

    # 连板分布
    if s['board_dist']:
        lines.append(f"连板分布: {s['board_dist']}")
        if s['max_board_stocks']:
            lines.append(f"最高连板: {s['max_board']}板 → {', '.join([f'{x[1]}({x[0]})' for x in s['max_board_stocks']])}")
        lines.append("")

    # 行业TOP10
    if s['ind_zt_top10']:
        lines.append("涨停行业TOP10:")
        lines.append("")
        lines.append("| 行业 | 涨停数 |")
        lines.append("|------|--------|")
        for ind, cnt in s['ind_zt_top10']:
            lines.append(f"| {ind} | {cnt} |")
        lines.append("")

    return "\n".join(lines)


# ===== 主函数：独立运行时输出当日情绪 =====
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        # 默认获取最近交易日
        trade_dates = get_trade_dates("20260401")
        date = trade_dates[-1] if trade_dates else "20260415"

    print(f"获取 {date} 市场情绪数据...")
    s = get_market_sentiment(date)
    print_sentiment_summary(s)

    # 输出Markdown格式
    print("\n" + "="*80)
    print("📝 Markdown格式（可直接粘贴到作战计划）:")
    print("="*80)
    print(format_sentiment_for_plan(s))
