# news_stock_linker.py
# -*- coding: utf-8 -*-
"""
新闻股票关联器 - 从新闻内容中提取关联的股票
功能:
1. 基于全A股名称库的精确匹配
2. 基于行业/概念关键词的模糊关联
3. 缓存机制避免重复计算
"""

import re
import logging
import time
import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger('news_stock_linker')

# Tushare API配置
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
            logger.warning(f"Tushare API {api_name} 错误: {j.get('msg', '')[:80]}")
            return pd.DataFrame()
        return pd.DataFrame(j["data"]["items"], columns=j["data"]["fields"])
    except Exception as e:
        logger.error(f"Tushare API {api_name} 异常: {e}")
        return pd.DataFrame()


# ==================== 行业/概念关键词 → 龙头股映射 ====================
# 当新闻中出现这些关键词时，关联对应的龙头股
CONCEPT_STOCK_MAP = {
    # AI/人工智能
    "人工智能": [("002230", "科大讯飞"), ("300474", "景嘉微"), ("688787", "海天瑞声"), ("300496", "中科创达")],
    "AI": [("002230", "科大讯飞"), ("300474", "景嘉微"), ("688787", "海天瑞声")],
    "大模型": [("002230", "科大讯飞"), ("300454", "深信服"), ("688083", "中望软件")],
    "ChatGPT": [("002230", "科大讯飞"), ("300624", "万兴科技")],
    "DeepSeek": [("002230", "科大讯飞"), ("300496", "中科创达")],
    "算力": [("002049", "紫光国微"), ("688256", "寒武纪"), ("300474", "景嘉微")],
    "GPU": [("300474", "景嘉微"), ("688256", "寒武纪")],
    
    # 半导体/芯片
    "半导体": [("688981", "中芯国际"), ("002049", "紫光国微"), ("603501", "韦尔股份")],
    "芯片": [("688981", "中芯国际"), ("002049", "紫光国微"), ("603501", "韦尔股份")],
    "光刻机": [("688012", "中微公司"), ("688072", "拓荆科技")],
    "EDA": [("688083", "中望软件"), ("688296", "华大九天")],
    
    # 新能源
    "光伏": [("601012", "隆基绿能"), ("002459", "晶澳科技"), ("688599", "天合光能")],
    "锂电池": [("300750", "宁德时代"), ("002594", "比亚迪"), ("300014", "亿纬锂能")],
    "新能源汽车": [("002594", "比亚迪"), ("300750", "宁德时代"), ("601238", "广汽集团")],
    "储能": [("300750", "宁德时代"), ("300014", "亿纬锂能"), ("002074", "国轩高科")],
    "风电": [("601899", "紫金矿业"), ("601016", "节能风电")],
    "氢能": [("600089", "特变电工"), ("300471", "厚普股份")],
    
    # 消费
    "白酒": [("600519", "贵州茅台"), ("000858", "五粮液"), ("000568", "泸州老窖")],
    "医药": [("600276", "恒瑞医药"), ("000538", "云南白药"), ("300760", "迈瑞医疗")],
    "创新药": [("600276", "恒瑞医药"), ("688180", "君实生物"), ("300347", "泰格医药")],
    "中药": [("000538", "云南白药"), ("600085", "同仁堂"), ("002603", "以岭药业")],
    "食品饮料": [("600519", "贵州茅台"), ("000858", "五粮液"), ("603288", "海天味业")],
    
    # 金融
    "银行": [("601398", "工商银行"), ("600036", "招商银行"), ("601166", "兴业银行")],
    "证券": [("600030", "中信证券"), ("601211", "国泰君安"), ("600837", "海通证券")],
    "保险": [("601318", "中国平安"), ("601628", "中国人寿"), ("601601", "中国太保")],
    
    # 房地产/基建
    "房地产": [("001979", "招商蛇口"), ("600048", "保利发展"), ("000002", "万科A")],
    "基建": [("601668", "中国建筑"), ("601390", "中国中铁"), ("601186", "中国铁建")],
    
    # 军工
    "军工": [("600893", "航发动力"), ("600760", "中航沈飞"), ("002179", "中航光电")],
    "航天": [("600118", "中国卫星"), ("600879", "航天电子")],
    
    # 科技/通信
    "5G": [("600941", "中国移动"), ("000063", "中兴通讯"), ("600050", "中国联通")],
    "华为": [("002502", "鼎信通讯"), ("300628", "亿联网络")],
    "鸿蒙": [("300496", "中科创达"), ("002405", "四维图新")],
    "数据要素": [("002555", "三七互娱"), ("300229", "拓尔思")],
    "数字经济": [("300229", "拓尔思"), ("300496", "中科创达")],
    "信创": [("002268", "卫士通"), ("688111", "金山办公")],
    
    # 机器人
    "机器人": [("300024", "机器人"), ("002747", "埃斯顿"), ("688169", "石头科技")],
    "人形机器人": [("300024", "机器人"), ("002747", "埃斯顿")],
    
    # 其他热门概念
    "黄金": [("600489", "中金黄金"), ("601899", "紫金矿业"), ("002155", "湖南黄金")],
    "稀土": [("600111", "北方稀土"), ("600392", "盛和资源")],
    "碳中和": [("601012", "隆基绿能"), ("300750", "宁德时代")],
    "元宇宙": [("002624", "完美世界"), ("300052", "中青宝")],
    "低空经济": [("002097", "山河智能"), ("688122", "西部超导")],
    "卫星互联网": [("600118", "中国卫星"), ("600879", "航天电子")],
}


class NewsStockLinker:
    """新闻股票关联器"""
    
    def __init__(self, cache_dir="data/cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # 股票名称 → 代码映射
        self.name_to_code = {}
        # 股票代码 → 名称映射
        self.code_to_name = {}
        # 股票代码 → 行业映射
        self.code_to_industry = {}
        # 行业 → 股票列表映射
        self.industry_to_stocks = defaultdict(list)
        
        # 新闻关联缓存 (hash → related_stocks)
        self._link_cache = {}
        
        # 加载股票名称库
        self._load_stock_names()
    
    def _load_stock_names(self):
        """加载全A股名称库，建立映射关系"""
        cache_file = os.path.join(self.cache_dir, "stock_names.json")
        
        # 检查缓存是否存在且未过期（每天更新一次）
        if os.path.exists(cache_file):
            try:
                mtime = os.path.getmtime(cache_file)
                if time.time() - mtime < 86400:  # 24小时内
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self.name_to_code = data.get('name_to_code', {})
                        self.code_to_name = data.get('code_to_name', {})
                        self.code_to_industry = data.get('code_to_industry', {})
                        # 重建行业映射
                        for code, ind in self.code_to_industry.items():
                            if ind:
                                self.industry_to_stocks[ind].append(code)
                        logger.info(f"从缓存加载 {len(self.name_to_code)} 只股票名称映射")
                        return
            except Exception as e:
                logger.warning(f"加载缓存失败: {e}")
        
        # 从Tushare获取
        logger.info("从Tushare获取全A股名称库...")
        df = _ts_api("stock_basic", {"list_status": "L"}, "ts_code,name,industry")
        time.sleep(0.5)
        
        if df.empty:
            logger.error("获取股票名称库失败")
            return
        
        # 过滤掉ST和退市股
        df = df[~df['name'].str.contains('ST|退', na=False)]
        # 只保留主板、创业板、科创板
        df = df[df['ts_code'].str.match(r'^(00|30|60|68)')]
        
        for _, row in df.iterrows():
            ts_code = row['ts_code']
            name = row['name']
            industry = row.get('industry', '')
            
            # 6位纯数字代码
            code = ts_code.split('.')[0]
            
            self.name_to_code[name] = code
            self.code_to_name[code] = name
            self.code_to_industry[code] = industry
            
            if industry:
                self.industry_to_stocks[industry].append(code)
        
        # 保存缓存
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'name_to_code': self.name_to_code,
                    'code_to_name': self.code_to_name,
                    'code_to_industry': self.code_to_industry,
                    'updated_at': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"已缓存 {len(self.name_to_code)} 只股票名称映射")
        except Exception as e:
            logger.warning(f"保存缓存失败: {e}")
    
    def link_news_to_stocks(self, news_item):
        """
        为单条新闻关联相关股票
        
        参数:
            news_item: dict, 包含 'title', 'content' 等字段
            
        返回:
            list: [{'code': '600519', 'name': '贵州茅台', 'reason': '新闻提及', 'match_type': 'name'}, ...]
        """
        content = news_item.get('content', '') or ''
        title = news_item.get('title', '') or ''
        text = title + ' ' + content
        
        # 检查缓存
        import hashlib
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        if text_hash in self._link_cache:
            return self._link_cache[text_hash]
        
        related_stocks = []
        seen_codes = set()
        
        # 1. 精确匹配股票代码（6位数字）
        code_pattern = re.compile(r'(?<!\d)(\d{6})(?!\d)')
        for match in code_pattern.finditer(text):
            code = match.group(1)
            if code in self.code_to_name and code not in seen_codes:
                seen_codes.add(code)
                related_stocks.append({
                    'code': code,
                    'name': self.code_to_name[code],
                    'reason': '代码匹配',
                    'match_type': 'code'
                })
        
        # 2. 精确匹配股票名称（至少2个字的名称）
        for name, code in self.name_to_code.items():
            if len(name) >= 2 and name in text and code not in seen_codes:
                seen_codes.add(code)
                related_stocks.append({
                    'code': code,
                    'name': name,
                    'reason': '名称匹配',
                    'match_type': 'name'
                })
        
        # 3. 概念/行业关键词匹配
        for keyword, stocks in CONCEPT_STOCK_MAP.items():
            if keyword in text:
                for code, name in stocks:
                    if code not in seen_codes:
                        seen_codes.add(code)
                        related_stocks.append({
                            'code': code,
                            'name': name,
                            'reason': f'概念[{keyword}]',
                            'match_type': 'concept'
                        })
        
        # 限制最多返回8只关联股票，优先级：代码匹配 > 名称匹配 > 概念匹配
        priority = {'code': 0, 'name': 1, 'concept': 2}
        related_stocks.sort(key=lambda x: priority.get(x['match_type'], 3))
        related_stocks = related_stocks[:8]
        
        # 缓存结果
        self._link_cache[text_hash] = related_stocks
        
        return related_stocks
    
    def batch_link_news(self, news_list):
        """
        批量为新闻关联股票
        
        参数:
            news_list: list of dict
            
        返回:
            list of dict: 每条新闻增加 'related_stocks' 字段
        """
        for news in news_list:
            news['related_stocks'] = self.link_news_to_stocks(news)
        return news_list


# 单例
_linker_instance = None

def get_linker():
    """获取全局单例"""
    global _linker_instance
    if _linker_instance is None:
        _linker_instance = NewsStockLinker()
    return _linker_instance
