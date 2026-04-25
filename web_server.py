# -*- coding: utf-8 -*-
"""
智能分析系统（股票） - 股票市场数据分析系统
修改：熊猫大侠
版本：v2.1.0
"""
# web_server.py

import numpy as np
import pandas as pd
from flask import Flask, render_template, request, jsonify, redirect, url_for
from stock_analyzer import StockAnalyzer
from us_stock_service import USStockService
import threading
import logging
from logging.handlers import RotatingFileHandler
import traceback
import os
import json
from datetime import date, datetime, timedelta
from flask_cors import CORS
import time
from flask_caching import Cache
import threading
import sys
from flask_swagger_ui import get_swaggerui_blueprint
from database import get_session, StockInfo, AnalysisResult, Portfolio, USE_DATABASE
from dotenv import load_dotenv
from industry_analyzer import IndustryAnalyzer
from fundamental_analyzer import FundamentalAnalyzer
from capital_flow_analyzer import CapitalFlowAnalyzer
from scenario_predictor import ScenarioPredictor
from stock_qa import StockQA
from risk_monitor import RiskMonitor
from index_industry_analyzer import IndexIndustryAnalyzer
from news_fetcher import news_fetcher, start_news_scheduler
from news_stock_linker import get_linker as get_news_linker
from market_sentiment_api import get_market_sentiment_summary, get_hot_sectors
from structured_report_generator import StructuredReportGenerator
from pattern_recognizer import PatternRecognizer
from industry_comparator import IndustryComparator
from multi_factor_selector import MultiFactorSelector
from watchlist_manager import WatchlistManager
from alert_manager import AlertManager
from portfolio_manager import PortfolioManager
from daily_briefing import DailyBriefing

# 导入 Claw 评分系统 Blueprint
try:
    from claw_routes import claw_bp, start_auto_sync
    CLAW_AVAILABLE = True
    print("✅ Claw 评分系统 Blueprint 已加载")
except ImportError as e:
    CLAW_AVAILABLE = False
    start_auto_sync = None
    print(f"⚠️ Claw 评分系统不可用: {e}")

# 加载环境变量
load_dotenv()

# 检查是否需要初始化数据库
if USE_DATABASE:
    try:
        from database import init_db
        init_db()
    except:
        pass

# 配置Swagger
SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "股票智能分析系统 API文档"
    }
)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
analyzer = StockAnalyzer()
us_stock_service = USStockService()

# 配置缓存
cache_config = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# 如果配置了Redis，使用Redis作为缓存后端
if os.getenv('USE_REDIS_CACHE', 'False').lower() == 'true' and os.getenv('REDIS_URL'):
    cache_config = {
        'CACHE_TYPE': 'RedisCache',
        'CACHE_REDIS_URL': os.getenv('REDIS_URL'),
        'CACHE_DEFAULT_TIMEOUT': 300
    }

cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
cache.init_app(app)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# 注册 Claw 评分系统 Blueprint
if CLAW_AVAILABLE:
    app.register_blueprint(claw_bp)
    print("✅ Claw 评分系统路由已注册 (前缀: /claw)")

# 确保全局变量在重新加载时不会丢失
if 'analyzer' not in globals():
    try:
        from stock_analyzer import StockAnalyzer

        analyzer = StockAnalyzer()
        print("成功初始化全局StockAnalyzer实例")
    except Exception as e:
        print(f"初始化StockAnalyzer时出错: {e}", file=sys.stderr)
        raise

# 初始化模块实例
fundamental_analyzer = FundamentalAnalyzer()
capital_flow_analyzer = CapitalFlowAnalyzer()
scenario_predictor = ScenarioPredictor(analyzer, os.getenv('OPENAI_API_KEY'), os.getenv('OPENAI_API_MODEL'))
stock_qa = StockQA(analyzer, os.getenv('OPENAI_API_KEY'), os.getenv('OPENAI_API_MODEL'))
risk_monitor = RiskMonitor(analyzer)
index_industry_analyzer = IndexIndustryAnalyzer(analyzer)
industry_analyzer = IndustryAnalyzer()
watchlist_manager = WatchlistManager(analyzer)
watchlist_manager.ensure_default_group()
alert_manager = AlertManager(analyzer)
alert_manager.start_scheduler(1800)  # 30分钟扫描一次
portfolio_manager = PortfolioManager(analyzer)
portfolio_manager.ensure_default_account()
daily_briefing = DailyBriefing(analyzer)
daily_briefing.start_scheduler()

start_news_scheduler()

# 线程本地存储
thread_local = threading.local()


def get_analyzer():
    """获取线程本地的分析器实例"""
    # 如果线程本地存储中没有分析器实例，创建一个新的
    if not hasattr(thread_local, 'analyzer'):
        thread_local.analyzer = StockAnalyzer()
    return thread_local.analyzer


# 配置日志
logging.basicConfig(level=logging.INFO)
handler = RotatingFileHandler('flask_app.log', maxBytes=10000000, backupCount=5)
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))
app.logger.addHandler(handler)

# 扩展任务管理系统以支持不同类型的任务
task_types = {
    'analysis': 'stock_analysis'  # 个股分析任务
}

# 任务数据存储
tasks = {
    'stock_analysis': {}  # 个股分析任务
}


def get_task_store(task_type):
    """获取指定类型的任务存储"""
    return tasks.get(task_type, {})


def generate_task_key(task_type, **params):
    """生成任务键"""
    if task_type == 'stock_analysis':
        # 对于个股分析，使用股票代码和市场类型作为键
        return f"{params.get('stock_code')}_{params.get('market_type', 'A')}"
    return None  # 其他任务类型不使用预生成的键


def get_or_create_task(task_type, **params):
    """获取或创建任务"""
    store = get_task_store(task_type)
    task_key = generate_task_key(task_type, **params)

    # 检查是否有现有任务
    if task_key and task_key in store:
        task = store[task_key]
        # 检查任务是否仍然有效
        if task['status'] in [TASK_PENDING, TASK_RUNNING]:
            return task['id'], task, False
        if task['status'] == TASK_COMPLETED and 'result' in task:
            # 任务已完成且有结果，重用它
            return task['id'], task, False

    # 创建新任务
    task_id = generate_task_id()
    task = {
        'id': task_id,
        'key': task_key,  # 存储任务键以便以后查找
        'type': task_type,
        'status': TASK_PENDING,
        'progress': 0,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'params': params
    }

    with task_lock:
        if task_key:
            store[task_key] = task
        store[task_id] = task

    return task_id, task, True


# 任务管理系统
task_lock = threading.Lock()  # 用于线程安全操作

# 任务状态常量
TASK_PENDING = 'pending'
TASK_RUNNING = 'running'
TASK_COMPLETED = 'completed'
TASK_FAILED = 'failed'


def generate_task_id():
    """生成唯一的任务ID"""
    import uuid
    return str(uuid.uuid4())


def update_task_status(task_type, task_id, status, progress=None, result=None, error=None):
    """更新任务状态"""
    store = get_task_store(task_type)
    with task_lock:
        if task_id in store:
            task = store[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 更新键索引的任务
            if 'key' in task and task['key'] in store:
                store[task['key']] = task


analysis_tasks = {}


def get_or_create_analysis_task(stock_code, market_type='A'):
    """获取或创建个股分析任务"""
    # 创建一个键，用于查找现有任务
    task_key = f"{stock_code}_{market_type}"

    with task_lock:
        # 检查是否有现有任务
        for task_id, task in analysis_tasks.items():
            if task.get('key') == task_key:
                # 检查任务是否仍然有效
                if task['status'] in [TASK_PENDING, TASK_RUNNING]:
                    return task_id, task, False
                if task['status'] == TASK_COMPLETED and 'result' in task:
                    # 任务已完成且有结果，重用它
                    return task_id, task, False

        # 创建新任务
        task_id = generate_task_id()
        task = {
            'id': task_id,
            'key': task_key,
            'status': TASK_PENDING,
            'progress': 0,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': {
                'stock_code': stock_code,
                'market_type': market_type
            }
        }

        analysis_tasks[task_id] = task

        return task_id, task, True


def update_analysis_task(task_id, status, progress=None, result=None, error=None):
    """更新个股分析任务状态"""
    with task_lock:
        if task_id in analysis_tasks:
            task = analysis_tasks[task_id]
            task['status'] = status
            if progress is not None:
                task['progress'] = progress
            if result is not None:
                task['result'] = result
            if error is not None:
                task['error'] = error
            task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# 定义自定义JSON编码器


# 在web_server.py中，更新convert_numpy_types函数以处理NaN值

# 将NumPy类型转换为Python原生类型的函数
def convert_numpy_types(obj):
    """递归地将字典和列表中的NumPy类型转换为Python原生类型"""
    try:
        import numpy as np
        import math

        if isinstance(obj, dict):
            return {key: convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            # Handle NaN and Infinity specifically
            if np.isnan(obj):
                return None
            elif np.isinf(obj):
                return None if obj < 0 else 1e308  # Use a very large number for +Infinity
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        # Handle Python's own float NaN and Infinity
        elif isinstance(obj, float):
            if math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            return obj
        # 添加对date和datetime类型的处理
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        else:
            return obj
    except ImportError:
        # 如果没有安装numpy，但需要处理date和datetime
        import math
        if isinstance(obj, dict):
            return {key: convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        # Handle Python's own float NaN and Infinity
        elif isinstance(obj, float):
            if math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            return obj
        return obj


# 同样更新 NumpyJSONEncoder 类
class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # For NumPy data types
        try:
            import numpy as np
            import math
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                # Handle NaN and Infinity specifically
                if np.isnan(obj):
                    return None
                elif np.isinf(obj):
                    return None
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, np.bool_):
                return bool(obj)
            # Handle Python's own float NaN and Infinity
            elif isinstance(obj, float):
                if math.isnan(obj):
                    return None
                elif math.isinf(obj):
                    return None
                return obj
        except ImportError:
            # Handle Python's own float NaN and Infinity if numpy is not available
            import math
            if isinstance(obj, float):
                if math.isnan(obj):
                    return None
                elif math.isinf(obj):
                    return None

        # 添加对date和datetime类型的处理
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        return super(NumpyJSONEncoder, self).default(obj)


# 使用我们的编码器的自定义 jsonify 函数
def custom_jsonify(data):
    return app.response_class(
        json.dumps(convert_numpy_types(data), cls=NumpyJSONEncoder),
        mimetype='application/json'
    )


# 保持API兼容的路由
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        stock_codes = data.get('stock_codes', [])
        market_type = data.get('market_type', 'A')

        if not stock_codes:
            return jsonify({'error': '请输入代码'}), 400

        app.logger.info(f"分析股票请求: {stock_codes}, 市场类型: {market_type}")

        # 设置最大处理时间，每只股票10秒
        max_time_per_stock = 10  # 秒
        max_total_time = max(30, min(60, len(stock_codes) * max_time_per_stock))  # 至少30秒，最多60秒

        start_time = time.time()
        results = []

        for stock_code in stock_codes:
            try:
                # 检查是否已超时
                if time.time() - start_time > max_total_time:
                    app.logger.warning(f"分析股票请求已超过{max_total_time}秒，提前返回已处理的{len(results)}只股票")
                    break

                # 使用线程本地缓存的分析器实例
                current_analyzer = get_analyzer()
                result = current_analyzer.quick_analyze_stock(stock_code.strip(), market_type)

                app.logger.info(
                    f"分析结果: 股票={stock_code}, 名称={result.get('stock_name', '未知')}, 行业={result.get('industry', '未知')}")
                results.append(result)
            except Exception as e:
                app.logger.error(f"分析股票 {stock_code} 时出错: {str(e)}")
                results.append({
                    'stock_code': stock_code,
                    'error': str(e),
                    'stock_name': '分析失败',
                    'industry': '未知'
                })

        return jsonify({'results': results})
    except Exception as e:
        app.logger.error(f"分析股票时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/north_flow_history', methods=['POST'])
def api_north_flow_history():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        days = data.get('days', 10)  # 默认为10天，对应前端的默认选项

        # 计算 end_date 为当前时间
        end_date = datetime.now().strftime('%Y%m%d')

        # 计算 start_date 为 end_date 减去指定的天数
        start_date = (datetime.now() - timedelta(days=int(days))).strftime('%Y%m%d')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 调用北向资金历史数据方法
        from capital_flow_analyzer import CapitalFlowAnalyzer

        analyzer = CapitalFlowAnalyzer()
        result = analyzer.get_north_flow_history(stock_code, start_date, end_date)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取北向资金历史数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/search_us_stocks', methods=['GET'])
def search_us_stocks():
    try:
        keyword = request.args.get('keyword', '')
        if not keyword:
            return jsonify({'error': '请输入搜索关键词'}), 400

        results = us_stock_service.search_us_stocks(keyword)
        return jsonify({'results': results})

    except Exception as e:
        app.logger.error(f"搜索美股代码时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500


# 新增可视化分析页面路由
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@app.route('/stock_detail/<string:stock_code>')
def stock_detail(stock_code):
    market_type = request.args.get('market_type', 'A')
    return render_template('stock_detail.html', stock_code=stock_code, market_type=market_type)


@app.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')


@app.route('/watchlist')
def watchlist_page():
    return render_template('watchlist.html')


# 基本面分析页面
@app.route('/fundamental')
def fundamental():
    return render_template('fundamental.html')


# 资金流向页面
@app.route('/capital_flow')
def capital_flow():
    return render_template('capital_flow.html')


# 情景预测页面
@app.route('/scenario_predict')
def scenario_predict():
    return render_template('scenario_predict.html')


# 风险监控页面
@app.route('/risk_monitor')
def risk_monitor_page():
    return render_template('risk_monitor.html')


# 智能问答页面
@app.route('/qa')
def qa_page():
    return render_template('qa.html')


# 行业分析页面
@app.route('/industry_analysis')
def industry_analysis():
    return render_template('industry_analysis.html')

# 多模型分析页面
@app.route('/multi_model_analysis')
def multi_model_analysis_page():
    return render_template('multi_model_analysis.html')

# AI辩论分析页面
@app.route('/debate_analysis')
def debate_analysis_page():
    return render_template('debate_analysis.html')

# 增强版AI辩论分析页面
@app.route('/enhanced_debate')
def enhanced_debate_page():
    return render_template('enhanced_debate.html')

@app.route('/claw_score')
def claw_score_page():
    """Claw A股量化评分系统页面"""
    return render_template('claw_score.html')


def make_cache_key_with_stock():
    """创建包含股票代码的自定义缓存键"""
    path = request.path

    # 从请求体中获取股票代码
    stock_code = None
    if request.is_json:
        stock_code = request.json.get('stock_code')

    # 构建包含股票代码的键
    if stock_code:
        return f"{path}_{stock_code}"
    else:
        return path


@app.route('/api/start_stock_analysis', methods=['POST'])
def start_stock_analysis():
    """启动个股分析任务"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return jsonify({'error': '请输入股票代码'}), 400

        app.logger.info(f"准备分析股票: {stock_code}")

        # 获取或创建任务
        task_id, task, is_new = get_or_create_task(
            'stock_analysis',
            stock_code=stock_code,
            market_type=market_type
        )

        # 如果是已完成的任务，直接返回结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            app.logger.info(f"使用缓存的分析结果: {stock_code}")
            return jsonify({
                'task_id': task_id,
                'status': task['status'],
                'result': task['result']
            })

        # 如果是新创建的任务，启动后台处理
        if is_new:
            app.logger.info(f"创建新的分析任务: {task_id}")

            # 启动后台线程执行分析
            def run_analysis():
                try:
                    update_task_status('stock_analysis', task_id, TASK_RUNNING, progress=10)

                    # 执行分析
                    result = analyzer.perform_enhanced_analysis(stock_code, market_type)

                    # 更新任务状态为完成
                    update_task_status('stock_analysis', task_id, TASK_COMPLETED, progress=100, result=result)
                    app.logger.info(f"分析任务 {task_id} 完成")

                except Exception as e:
                    app.logger.error(f"分析任务 {task_id} 失败: {str(e)}")
                    app.logger.error(traceback.format_exc())
                    update_task_status('stock_analysis', task_id, TASK_FAILED, error=str(e))

            # 启动后台线程
            thread = threading.Thread(target=run_analysis)
            thread.daemon = True
            thread.start()

        # 返回任务ID和状态
        return jsonify({
            'task_id': task_id,
            'status': task['status'],
            'message': f'已启动分析任务: {stock_code}'
        })

    except Exception as e:
        app.logger.error(f"启动个股分析任务时出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/analysis_status/<task_id>', methods=['GET'])
def get_analysis_status(task_id):
    """获取个股分析任务状态"""
    store = get_task_store('stock_analysis')
    with task_lock:
        if task_id not in store:
            return jsonify({'error': '找不到指定的分析任务'}), 404

        task = store[task_id]

        # 基本状态信息
        status = {
            'id': task['id'],
            'status': task['status'],
            'progress': task.get('progress', 0),
            'created_at': task['created_at'],
            'updated_at': task['updated_at']
        }

        # 如果任务完成，包含结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            status['result'] = task['result']

        # 如果任务失败，包含错误信息
        if task['status'] == TASK_FAILED and 'error' in task:
            status['error'] = task['error']

        return custom_jsonify(status)


@app.route('/api/cancel_analysis/<task_id>', methods=['POST'])
def cancel_analysis(task_id):
    """取消个股分析任务"""
    store = get_task_store('stock_analysis')
    with task_lock:
        if task_id not in store:
            return jsonify({'error': '找不到指定的分析任务'}), 404

        task = store[task_id]

        if task['status'] in [TASK_COMPLETED, TASK_FAILED]:
            return jsonify({'message': '任务已完成或失败，无法取消'})

        # 更新状态为失败
        task['status'] = TASK_FAILED
        task['error'] = '用户取消任务'
        task['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 更新键索引的任务
        if 'key' in task and task['key'] in store:
            store[task['key']] = task

        return jsonify({'message': '任务已取消'})


# 保留原有API用于向后兼容
@app.route('/api/quick_analysis', methods=['POST'])
def quick_analysis():
    """
    快速分析API - 纯代码计算，不依赖AI，<3秒返回
    返回8维度评分 + 结构化分析报告
    """
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return custom_jsonify({'error': '请输入股票代码'}), 400

        start_time = time.time()
        current_analyzer = get_analyzer()

        # 1. 获取股票数据和技术指标
        df = current_analyzer.get_stock_data(stock_code, market_type)
        df = current_analyzer.calculate_indicators(df)

        # 2. 计算8维度综合评分
        score = current_analyzer.calculate_score(df, market_type, stock_code)
        score_details = getattr(current_analyzer, 'score_details', {'total': score})

        # 3. 获取股票信息
        stock_info = current_analyzer.get_stock_info(stock_code)

        # 4. 获取增强数据（用于报告生成）
        enhanced_data = {}
        try:
            from enhanced_data_collector import get_collector, ENHANCED_COLLECTOR_AVAILABLE
            if ENHANCED_COLLECTOR_AVAILABLE:
                collector = get_collector()
                enhanced_data = collector.collect_comprehensive_data(stock_code, market_type)
        except Exception as e:
            app.logger.warning(f"快速分析-增强数据收集失败: {e}")

        # 5. 获取最新价格数据
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        price_data = {
            'current_price': float(latest['close']),
            'price_change': float((latest['close'] - prev['close']) / prev['close'] * 100),
            'high': float(latest['high']),
            'low': float(latest['low']),
            'volume': float(latest['volume']),
        }

        # 6. 技术分析数据
        technical_analysis = {
            'ma5': float(latest['MA5']),
            'ma20': float(latest['MA20']),
            'ma60': float(latest['MA60']),
            'rsi': float(latest['RSI']),
            'macd': float(latest['MACD']),
            'signal': float(latest['Signal']),
            'bb_upper': float(latest['BB_upper']),
            'bb_lower': float(latest['BB_lower']),
        }

        # 7. 生成结构化报告
        report_gen = StructuredReportGenerator()
        structured_report = report_gen.generate_report(
            score_details=score_details,
            enhanced_data=enhanced_data,
            stock_info=stock_info,
            price_data=price_data,
            technical_analysis=technical_analysis
        )

        # 8. 技术形态识别
        pattern_result = {}
        try:
            recognizer = PatternRecognizer()
            pattern_result = recognizer.analyze(df)
        except Exception as e:
            app.logger.warning(f"形态识别失败: {e}")
            pattern_result = {'patterns': [], 'summary': '形态识别暂不可用', 'dominant_signal': 'neutral', 'signal_strength': 0}

        elapsed = time.time() - start_time
        app.logger.info(f"快速分析完成: {stock_code}，耗时 {elapsed:.2f}秒")

        return custom_jsonify({
            'result': {
                'basic_info': {
                    'stock_code': stock_code,
                    'stock_name': stock_info.get('股票名称', '未知'),
                    'industry': stock_info.get('行业', '未知'),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                },
                'price_data': price_data,
                'technical_analysis': technical_analysis,
                'comprehensive_scores': score_details,
                'structured_report': structured_report,
                'pattern_analysis': pattern_result,
                'analysis_type': 'quick',
                'elapsed_seconds': round(elapsed, 2)
            }
        })

    except Exception as e:
        app.logger.error(f"快速分析出错: {traceback.format_exc()}")
        return custom_jsonify({'error': f'快速分析出错: {str(e)}'}), 500


@app.route('/api/pattern_analysis', methods=['POST'])
def pattern_analysis():
    """独立的技术形态识别API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return custom_jsonify({'error': '请输入股票代码'}), 400

        current_analyzer = get_analyzer()
        df = current_analyzer.get_stock_data(stock_code, market_type)
        df = current_analyzer.calculate_indicators(df)

        recognizer = PatternRecognizer()
        result = recognizer.analyze(df)

        return custom_jsonify({'result': result})

    except Exception as e:
        app.logger.error(f"形态识别出错: {traceback.format_exc()}")
        return custom_jsonify({'error': f'形态识别出错: {str(e)}'}), 500


@app.route('/api/multi_factor_select', methods=['POST'])
def multi_factor_select():
    """多因子选股API"""
    try:
        data = request.json or {}
        strategy = data.get('strategy', 'balanced')
        custom_weights = data.get('custom_weights')
        filters = data.get('filters')
        top_n = min(data.get('top_n', 20), 50)

        selector = MultiFactorSelector()
        result = selector.select_stocks(
            strategy=strategy,
            custom_weights=custom_weights,
            filters=filters,
            top_n=top_n
        )

        return custom_jsonify({'result': result})

    except Exception as e:
        app.logger.error(f"多因子选股出错: {traceback.format_exc()}")
        return custom_jsonify({'error': f'多因子选股出错: {str(e)}'}), 500


@app.route('/api/multi_factor_strategies', methods=['GET'])
def multi_factor_strategies():
    """获取多因子选股预设策略列表"""
    try:
        from multi_factor_selector import STRATEGY_TEMPLATES
        return custom_jsonify({'strategies': STRATEGY_TEMPLATES})
    except Exception as e:
        return custom_jsonify({'error': str(e)}), 500


@app.route('/api/industry_compare', methods=['POST'])
def industry_compare():
    """同行业横向对比API - 5维度雷达图对比"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        top_n = data.get('top_n', 10)

        if not stock_code:
            return custom_jsonify({'error': '请输入股票代码'}), 400

        comparator = IndustryComparator()
        result = comparator.compare(stock_code, top_n=top_n)

        if result.get('error'):
            return custom_jsonify({'error': result['error']}), 400

        return custom_jsonify({'result': result})

    except Exception as e:
        app.logger.error(f"同行业对比出错: {traceback.format_exc()}")
        return custom_jsonify({'error': f'同行业对比出错: {str(e)}'}), 500


@app.route('/api/enhanced_analysis', methods=['POST'])
def enhanced_analysis():
    """原增强分析API的向后兼容版本"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return custom_jsonify({'error': '请输入股票代码'}), 400

        # 调用新的任务系统，但模拟同步行为
        # 这会导致和之前一样的超时问题，但保持兼容
        timeout = 300
        start_time = time.time()

        # 获取或创建任务
        task_id, task, is_new = get_or_create_task(
            'stock_analysis',
            stock_code=stock_code,
            market_type=market_type
        )

        # 如果是已完成的任务，直接返回结果
        if task['status'] == TASK_COMPLETED and 'result' in task:
            app.logger.info(f"使用缓存的分析结果: {stock_code}")
            return custom_jsonify({'result': task['result']})

        # 启动分析（如果是新任务）
        if is_new:
            # 同步执行分析
            try:
                result = analyzer.perform_enhanced_analysis(stock_code, market_type)
                update_task_status('stock_analysis', task_id, TASK_COMPLETED, progress=100, result=result)
                app.logger.info(f"分析完成: {stock_code}，耗时 {time.time() - start_time:.2f} 秒")
                return custom_jsonify({'result': result})
            except Exception as e:
                app.logger.error(f"分析过程中出错: {str(e)}")
                update_task_status('stock_analysis', task_id, TASK_FAILED, error=str(e))
                return custom_jsonify({'error': f'分析过程中出错: {str(e)}'}), 500
        else:
            # 已存在正在处理的任务，等待其完成
            max_wait = timeout - (time.time() - start_time)
            wait_interval = 0.5
            waited = 0

            while waited < max_wait:
                with task_lock:
                    current_task = store[task_id]
                    if current_task['status'] == TASK_COMPLETED and 'result' in current_task:
                        return custom_jsonify({'result': current_task['result']})
                    if current_task['status'] == TASK_FAILED:
                        error = current_task.get('error', '任务失败，无详细信息')
                        return custom_jsonify({'error': error}), 500

                time.sleep(wait_interval)
                waited += wait_interval

            # 超时
            return custom_jsonify({'error': '处理超时，请稍后重试'}), 504

    except Exception as e:
        app.logger.error(f"执行增强版分析时出错: {traceback.format_exc()}")
        return custom_jsonify({'error': str(e)}), 500


# 添加在web_server.py主代码中
@app.errorhandler(404)
def not_found(error):
    """处理404错误"""
    if request.path.startswith('/api/'):
        # 为API请求返回JSON格式的错误
        return jsonify({
            'error': '找不到请求的API端点',
            'path': request.path,
            'method': request.method
        }), 404
    # 为网页请求返回HTML错误页
    return render_template('error.html', error_code=404, message="找不到请求的页面"), 404


@app.errorhandler(500)
def server_error(error):
    """处理500错误"""
    app.logger.error(f"服务器错误: {str(error)}")
    if request.path.startswith('/api/'):
        # 为API请求返回JSON格式的错误
        return jsonify({
            'error': '服务器内部错误',
            'message': str(error)
        }), 500
    # 为网页请求返回HTML错误页
    return render_template('error.html', error_code=500, message="服务器内部错误"), 500


# Update the get_stock_data function in web_server.py to handle date formatting properly
@app.route('/api/stock_data', methods=['GET'])
@cache.cached(timeout=300, query_string=True)
def get_stock_data():
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', 'A')
        period = request.args.get('period', '1y')  # 默认1年

        if not stock_code:
            return custom_jsonify({'error': '请提供股票代码'}), 400

        # 根据period计算start_date
        end_date = datetime.now().strftime('%Y%m%d')
        if period == '1m':
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        elif period == '3m':
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        elif period == '6m':
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
        elif period == '1y':
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

        # 获取股票历史数据
        app.logger.info(
            f"获取股票 {stock_code} 的历史数据，市场: {market_type}, 起始日期: {start_date}, 结束日期: {end_date}")
        df = analyzer.get_stock_data(stock_code, market_type, start_date, end_date)

        # 计算技术指标
        app.logger.info(f"计算股票 {stock_code} 的技术指标")
        df = analyzer.calculate_indicators(df)

        # 检查数据是否为空
        if df.empty:
            app.logger.warning(f"股票 {stock_code} 的数据为空")
            return custom_jsonify({'error': '未找到股票数据'}), 404

        # 将DataFrame转为JSON格式
        app.logger.info(f"将数据转换为JSON格式，行数: {len(df)}")

        # 确保日期列是字符串格式 - 修复缓存问题
        if 'date' in df.columns:
            try:
                if pd.api.types.is_datetime64_any_dtype(df['date']):
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                else:
                    df = df.copy()
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            except Exception as e:
                app.logger.error(f"处理日期列时出错: {str(e)}")
                df['date'] = df['date'].astype(str)

        # 将NaN值替换为None
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        records = df.to_dict('records')

        app.logger.info(f"数据处理完成，返回 {len(records)} 条记录")
        return custom_jsonify({'data': records})
    except Exception as e:
        app.logger.error(f"获取股票数据时出错: {str(e)}")
        app.logger.error(traceback.format_exc())
        return custom_jsonify({'error': str(e)}), 500


# 定期运行任务清理，并在每天 16:30 左右清理所有缓存
def run_task_cleaner():
    """定期运行任务清理，并在每天 16:30 左右清理所有缓存"""
    while True:
        try:
            now = datetime.now()
            # 判断是否在收盘时间附近（16:25-16:35）
            is_market_close_time = (now.hour == 16 and 25 <= now.minute <= 35)

            # 如果是收盘时间，清理所有缓存
            if is_market_close_time:
                # 清理分析器的数据缓存
                analyzer.data_cache.clear()

                # 清理 Flask 缓存
                cache.clear()

                # 清理任务存储
                with task_lock:
                    for task_type in tasks:
                        task_store = tasks[task_type]
                        completed_tasks = [task_id for task_id, task in task_store.items()
                                           if task['status'] == TASK_COMPLETED]
                        for task_id in completed_tasks:
                            del task_store[task_id]

                app.logger.info("市场收盘时间检测到，已清理所有缓存数据")
        except Exception as e:
            app.logger.error(f"任务清理出错: {str(e)}")

        # 每 5 分钟运行一次
        time.sleep(600)


# 基本面分析路由
@app.route('/api/fundamental_analysis', methods=['POST'])
def api_fundamental_analysis():
    try:
        data = request.json
        stock_code = data.get('stock_code')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取基本面分析结果
        result = fundamental_analyzer.calculate_fundamental_score(stock_code)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"基本面分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/comprehensive_fundamental', methods=['POST'])
def api_comprehensive_fundamental():
    """全面基本面分析API - 包含现金流、杜邦分析、同行业对比、财务趋势、分红历史"""
    try:
        data = request.json
        stock_code = data.get('stock_code')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        result = fundamental_analyzer.get_comprehensive_fundamental(stock_code)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"全面基本面分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/cash_flow_analysis', methods=['POST'])
def api_cash_flow_analysis():
    """现金流分析API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        result = fundamental_analyzer.get_cash_flow_analysis(stock_code)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"现金流分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/dupont_analysis', methods=['POST'])
def api_dupont_analysis():
    """杜邦分析API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        result = fundamental_analyzer.get_dupont_analysis(stock_code)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"杜邦分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_comparison', methods=['POST'])
def api_industry_comparison():
    """同行业对比API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        result = fundamental_analyzer.get_industry_comparison(stock_code)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"同行业对比出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/dividend_history', methods=['POST'])
def api_dividend_history():
    """分红历史API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        result = fundamental_analyzer.get_dividend_history(stock_code)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"分红历史出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 资金流向分析路由
# Add to web_server.py

# 获取概念资金流向的API端点
@app.route('/api/concept_fund_flow', methods=['GET'])
def api_concept_fund_flow():
    try:
        period = request.args.get('period', '10日排行')  # Default to 10-day ranking

        # Get concept fund flow data
        result = capital_flow_analyzer.get_concept_fund_flow(period)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting concept fund flow: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 获取个股资金流向排名的API端点
@app.route('/api/individual_fund_flow_rank', methods=['GET'])
def api_individual_fund_flow_rank():
    try:
        period = request.args.get('period', '10日')  # Default to today

        # Get individual fund flow ranking data
        result = capital_flow_analyzer.get_individual_fund_flow_rank(period)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting individual fund flow ranking: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 获取个股资金流向的API端点
@app.route('/api/individual_fund_flow', methods=['GET'])
def api_individual_fund_flow():
    try:
        stock_code = request.args.get('stock_code')
        market_type = request.args.get('market_type', '')  # Auto-detect if not provided
        re_date = request.args.get('period-select')

        if not stock_code:
            return jsonify({'error': 'Stock code is required'}), 400

        # Get individual fund flow data
        result = capital_flow_analyzer.get_individual_fund_flow(stock_code, market_type, re_date)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting individual fund flow: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 获取板块内股票的API端点
@app.route('/api/sector_stocks', methods=['GET'])
def api_sector_stocks():
    try:
        sector = request.args.get('sector')

        if not sector:
            return jsonify({'error': 'Sector name is required'}), 400

        # Get sector stocks data
        result = capital_flow_analyzer.get_sector_stocks(sector)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error getting sector stocks: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# Update the existing capital flow API endpoint
@app.route('/api/capital_flow', methods=['POST'])
def api_capital_flow():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', '')  # Auto-detect if not provided

        if not stock_code:
            return jsonify({'error': 'Stock code is required'}), 400

        # Calculate capital flow score
        result = capital_flow_analyzer.calculate_capital_flow_score(stock_code, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"Error calculating capital flow score: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 情景预测路由
@app.route('/api/scenario_predict', methods=['POST'])
def api_scenario_predict():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        days = data.get('days', 60)

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取情景预测结果
        result = scenario_predictor.generate_scenarios(stock_code, market_type, days)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"情景预测出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 智能问答路由
@app.route('/api/qa', methods=['POST'])
def api_qa():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        question = data.get('question')
        market_type = data.get('market_type', 'A')

        if not stock_code or not question:
            return jsonify({'error': '请提供股票代码和问题'}), 400

        # 获取智能问答结果
        result = stock_qa.answer_question(stock_code, question, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"智能问答出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 风险分析路由
@app.route('/api/risk_analysis', methods=['POST'])
def api_risk_analysis():
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')

        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 获取风险分析结果
        result = risk_monitor.analyze_stock_risk(stock_code, market_type)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"风险分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 投资组合风险分析路由
@app.route('/api/portfolio_risk', methods=['POST'])
def api_portfolio_risk():
    try:
        data = request.json
        portfolio = data.get('portfolio', [])

        if not portfolio:
            return jsonify({'error': '请提供投资组合'}), 400

        # 获取投资组合风险分析结果
        result = risk_monitor.analyze_portfolio_risk(portfolio)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"投资组合风险分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# ==================== 自选股管理API ====================

@app.route('/api/watchlist/groups', methods=['GET'])
def watchlist_get_groups():
    """获取所有自选股分组"""
    try:
        groups = watchlist_manager.get_groups()
        return custom_jsonify({'groups': groups})
    except Exception as e:
        app.logger.error(f"获取分组失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/groups', methods=['POST'])
def watchlist_create_group():
    """创建自选股分组"""
    try:
        data = request.json
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': '分组名称不能为空'}), 400
        result = watchlist_manager.create_group(
            name=name,
            description=data.get('description', ''),
            color=data.get('color', '#4e73df')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"创建分组失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/groups/<int:group_id>', methods=['PUT'])
def watchlist_update_group(group_id):
    """更新分组"""
    try:
        data = request.json
        result = watchlist_manager.update_group(group_id, **data)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"更新分组失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/groups/<int:group_id>', methods=['DELETE'])
def watchlist_delete_group(group_id):
    """删除分组"""
    try:
        result = watchlist_manager.delete_group(group_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"删除分组失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/stocks', methods=['POST'])
def watchlist_add_stock():
    """添加股票到自选"""
    try:
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        group_id = data.get('group_id')
        if not group_id:
            group_id = watchlist_manager.ensure_default_group()
        result = watchlist_manager.add_stock(
            group_id=group_id,
            stock_code=stock_code,
            stock_name=data.get('stock_name', ''),
            market_type=data.get('market_type', 'A'),
            cost_price=data.get('cost_price', 0),
            target_price=data.get('target_price', 0),
            stop_loss_price=data.get('stop_loss_price', 0),
            notes=data.get('notes', '')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"添加自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/stocks/<int:stock_id>', methods=['PUT'])
def watchlist_update_stock(stock_id):
    """更新自选股信息"""
    try:
        data = request.json
        result = watchlist_manager.update_stock(stock_id, **data)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"更新自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/stocks/<int:stock_id>', methods=['DELETE'])
def watchlist_remove_stock(stock_id):
    """移除自选股"""
    try:
        result = watchlist_manager.remove_stock(stock_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"移除自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/stocks/move', methods=['POST'])
def watchlist_move_stock():
    """移动股票到其他分组"""
    try:
        data = request.json
        stock_id = data.get('stock_id')
        target_group_id = data.get('target_group_id')
        if not stock_id or not target_group_id:
            return jsonify({'error': '参数不完整'}), 400
        result = watchlist_manager.move_stock(stock_id, target_group_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"移动自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/group/<int:group_id>/stocks', methods=['GET'])
def watchlist_get_group_stocks(group_id):
    """获取分组中的股票列表"""
    try:
        stocks = watchlist_manager.get_stocks_by_group(group_id)
        return custom_jsonify({'stocks': stocks})
    except Exception as e:
        app.logger.error(f"获取分组股票失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/all_stocks', methods=['GET'])
def watchlist_get_all_stocks():
    """获取所有自选股"""
    try:
        stocks = watchlist_manager.get_all_stocks()
        return custom_jsonify({'stocks': stocks})
    except Exception as e:
        app.logger.error(f"获取所有自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/overview', methods=['GET'])
def watchlist_overview():
    """获取自选股概览"""
    try:
        overview = watchlist_manager.get_overview()
        return custom_jsonify(overview)
    except Exception as e:
        app.logger.error(f"获取概览失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/realtime', methods=['GET'])
def watchlist_realtime():
    """批量获取自选股实时行情"""
    try:
        group_id = request.args.get('group_id', type=int)
        results = watchlist_manager.batch_get_realtime(group_id)
        return custom_jsonify({'stocks': results})
    except Exception as e:
        app.logger.error(f"获取实时行情失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/batch_score', methods=['GET'])
def watchlist_batch_score():
    """批量快速评分"""
    try:
        group_id = request.args.get('group_id', type=int)
        results = watchlist_manager.batch_quick_score(group_id)
        return custom_jsonify({'scores': results})
    except Exception as e:
        app.logger.error(f"批量评分失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist/search', methods=['GET'])
def watchlist_search():
    """搜索自选股"""
    try:
        keyword = request.args.get('keyword', '').strip()
        if not keyword:
            return jsonify({'error': '请输入搜索关键词'}), 400
        results = watchlist_manager.search_stocks(keyword)
        return custom_jsonify({'stocks': results})
    except Exception as e:
        app.logger.error(f"搜索自选股失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# ==================== 风险预警API ====================

@app.route('/api/alerts/rule_types', methods=['GET'])
def alert_rule_types():
    """获取支持的预警规则类型"""
    try:
        return custom_jsonify({'rule_types': alert_manager.get_rule_types()})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/rules', methods=['GET'])
def alert_get_rules():
    """获取预警规则列表"""
    try:
        stock_code = request.args.get('stock_code')
        active_only = request.args.get('active_only', 'true').lower() == 'true'
        rules = alert_manager.get_rules(stock_code, active_only)
        return custom_jsonify({'rules': rules})
    except Exception as e:
        app.logger.error(f"获取预警规则失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/rules', methods=['POST'])
def alert_create_rule():
    """创建预警规则"""
    try:
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        rule_type = data.get('rule_type', '').strip()
        if not stock_code or not rule_type:
            return jsonify({'error': '股票代码和规则类型不能为空'}), 400
        result = alert_manager.create_rule(
            stock_code=stock_code,
            rule_type=rule_type,
            condition_value=data.get('condition_value', 0),
            stock_name=data.get('stock_name', ''),
            description=data.get('description', '')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"创建预警规则失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/rules/<int:rule_id>', methods=['PUT'])
def alert_update_rule(rule_id):
    """更新预警规则"""
    try:
        data = request.json
        result = alert_manager.update_rule(rule_id, **data)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"更新预警规则失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/rules/<int:rule_id>', methods=['DELETE'])
def alert_delete_rule(rule_id):
    """删除预警规则"""
    try:
        result = alert_manager.delete_rule(rule_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"删除预警规则失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/rules/<int:rule_id>/reset', methods=['POST'])
def alert_reset_rule(rule_id):
    """重置规则触发状态"""
    try:
        result = alert_manager.reset_rule(rule_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"重置规则失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/quick_setup', methods=['POST'])
def alert_quick_setup():
    """为股票快速创建一组常用预警规则"""
    try:
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        result = alert_manager.create_watchlist_rules(stock_code, data.get('stock_name', ''))
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"快速创建预警失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/logs', methods=['GET'])
def alert_get_logs():
    """获取预警日志"""
    try:
        limit = request.args.get('limit', 50, type=int)
        unread_only = request.args.get('unread_only', 'false').lower() == 'true'
        stock_code = request.args.get('stock_code')
        logs = alert_manager.get_alert_logs(limit, unread_only, stock_code)
        return custom_jsonify({'logs': logs, 'unread_count': alert_manager.get_unread_count()})
    except Exception as e:
        app.logger.error(f"获取预警日志失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/unread_count', methods=['GET'])
def alert_unread_count():
    """获取未读预警数量"""
    try:
        count = alert_manager.get_unread_count()
        return custom_jsonify({'unread_count': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/mark_read', methods=['POST'])
def alert_mark_read():
    """标记预警为已读"""
    try:
        data = request.json or {}
        log_id = data.get('log_id')
        result = alert_manager.mark_read(log_id)
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"标记已读失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/scan_now', methods=['POST'])
def alert_scan_now():
    """立即执行一次预警扫描"""
    try:
        count = alert_manager.scan_all_rules()
        return custom_jsonify({'success': True, 'triggered_count': count})
    except Exception as e:
        app.logger.error(f"手动扫描失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/status', methods=['GET'])
def alert_status():
    """获取预警系统状态"""
    try:
        status = alert_manager.get_status()
        return custom_jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ==================== 投资组合管理API ====================

@app.route('/api/portfolio/accounts', methods=['GET'])
def portfolio_get_accounts():
    """获取所有投资组合账户"""
    try:
        accounts = portfolio_manager.get_accounts()
        return custom_jsonify({'accounts': accounts})
    except Exception as e:
        app.logger.error(f"获取账户失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/accounts', methods=['POST'])
def portfolio_create_account():
    """创建投资组合账户"""
    try:
        data = request.json
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': '账户名称不能为空'}), 400
        result = portfolio_manager.create_account(
            name=name,
            initial_capital=data.get('initial_capital', 0),
            description=data.get('description', '')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"创建账户失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/accounts/<int:account_id>', methods=['DELETE'])
def portfolio_delete_account(account_id):
    """删除投资组合账户"""
    try:
        result = portfolio_manager.delete_account(account_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"删除账户失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/buy', methods=['POST'])
def portfolio_buy():
    """买入股票"""
    try:
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        quantity = data.get('quantity', 0)
        price = data.get('price', 0)
        if quantity <= 0 or price <= 0:
            return jsonify({'error': '数量和价格必须大于0'}), 400

        account_id = data.get('account_id')
        if not account_id:
            account_id = portfolio_manager.ensure_default_account()

        result = portfolio_manager.buy(
            account_id=account_id,
            stock_code=stock_code,
            quantity=quantity,
            price=price,
            stock_name=data.get('stock_name', ''),
            market_type=data.get('market_type', 'A'),
            commission=data.get('commission', 0),
            tax=data.get('tax', 0),
            notes=data.get('notes', '')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"买入失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/sell', methods=['POST'])
def portfolio_sell():
    """卖出股票"""
    try:
        data = request.json
        stock_code = data.get('stock_code', '').strip()
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        quantity = data.get('quantity', 0)
        price = data.get('price', 0)
        if quantity <= 0 or price <= 0:
            return jsonify({'error': '数量和价格必须大于0'}), 400

        account_id = data.get('account_id')
        if not account_id:
            account_id = portfolio_manager.ensure_default_account()

        result = portfolio_manager.sell(
            account_id=account_id,
            stock_code=stock_code,
            quantity=quantity,
            price=price,
            commission=data.get('commission', 0),
            tax=data.get('tax', 0),
            notes=data.get('notes', '')
        )
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"卖出失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/holdings/<int:account_id>', methods=['GET'])
def portfolio_get_holdings(account_id):
    """获取账户持仓"""
    try:
        refresh = request.args.get('refresh', 'true').lower() == 'true'
        result = portfolio_manager.get_holdings(account_id, refresh_price=refresh)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取持仓失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/transactions/<int:account_id>', methods=['GET'])
def portfolio_get_transactions(account_id):
    """获取交易记录"""
    try:
        limit = request.args.get('limit', 100, type=int)
        stock_code = request.args.get('stock_code')
        txs = portfolio_manager.get_transactions(account_id, limit, stock_code)
        return custom_jsonify({'transactions': txs})
    except Exception as e:
        app.logger.error(f"获取交易记录失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/risk_attribution/<int:account_id>', methods=['GET'])
def portfolio_risk_attribution(account_id):
    """投资组合风险归因分析"""
    try:
        result = portfolio_manager.analyze_risk_attribution(account_id)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"风险归因分析失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/rebalance/<int:account_id>', methods=['POST'])
def portfolio_rebalance(account_id):
    """获取再平衡建议"""
    try:
        data = request.json or {}
        target_weights = data.get('target_weights')
        result = portfolio_manager.get_rebalance_suggestions(account_id, target_weights)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"再平衡建议失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/deposit', methods=['POST'])
def portfolio_deposit():
    """入金"""
    try:
        data = request.json
        account_id = data.get('account_id')
        amount = data.get('amount', 0)
        if not account_id or amount <= 0:
            return jsonify({'error': '参数不完整'}), 400
        result = portfolio_manager.deposit(account_id, amount)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"入金失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/portfolio/withdraw', methods=['POST'])
def portfolio_withdraw():
    """出金"""
    try:
        data = request.json
        account_id = data.get('account_id')
        amount = data.get('amount', 0)
        if not account_id or amount <= 0:
            return jsonify({'error': '参数不完整'}), 400
        result = portfolio_manager.withdraw(account_id, amount)
        if 'error' in result:
            return jsonify(result), 400
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"出金失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# ==================== 每日市场简报API ====================

@app.route('/api/daily_brief/generate', methods=['POST'])
def daily_brief_generate():
    """手动生成每日简报"""
    try:
        data = request.json or {}
        target_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
        result = daily_briefing.generate_brief(target_date)
        if 'error' in result:
            return jsonify(result), 500
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"生成简报失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/daily_brief/<string:brief_date>', methods=['GET'])
def daily_brief_get(brief_date):
    """获取指定日期的简报"""
    try:
        result = daily_briefing.get_brief(brief_date)
        if 'error' in result:
            return jsonify(result), 404
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取简报失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/daily_brief/recent', methods=['GET'])
def daily_brief_recent():
    """获取最近的简报列表"""
    try:
        limit = request.args.get('limit', 7, type=int)
        briefs = daily_briefing.get_recent_briefs(limit)
        return custom_jsonify({'briefs': briefs})
    except Exception as e:
        app.logger.error(f"获取简报列表失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 指数分析路由
    try:
        index_code = request.args.get('index_code')
        limit = int(request.args.get('limit', 30))

        if not index_code:
            return jsonify({'error': '请提供指数代码'}), 400

        # 获取指数分析结果
        result = index_industry_analyzer.analyze_index(index_code, limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"指数分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 行业分析路由
@app.route('/api/industry_analysis', methods=['GET'])
def api_industry_analysis():
    try:
        industry = request.args.get('industry')
        limit = int(request.args.get('limit', 30))

        if not industry:
            return jsonify({'error': '请提供行业名称'}), 400

        # 获取行业分析结果
        result = index_industry_analyzer.analyze_industry(industry, limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"行业分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_fund_flow', methods=['GET'])
def api_industry_fund_flow():
    """获取行业资金流向数据"""
    try:
        symbol = request.args.get('symbol', '即时')

        result = industry_analyzer.get_industry_fund_flow(symbol)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取行业资金流向数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/industry_detail', methods=['GET'])
def api_industry_detail():
    """获取行业详细信息"""
    try:
        industry = request.args.get('industry')

        if not industry:
            return jsonify({'error': '请提供行业名称'}), 400

        result = industry_analyzer.get_industry_detail(industry)

        app.logger.info(f"返回前 (result)：{result}")
        if not result:
            return jsonify({'error': f'未找到行业 {industry} 的详细信息'}), 404

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取行业详细信息出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 行业比较路由
@app.route('/api/industry_compare', methods=['GET'])
def api_industry_compare():
    try:
        limit = int(request.args.get('limit', 10))

        # 获取行业比较结果
        result = index_industry_analyzer.compare_industries(limit)

        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"行业比较出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


# 保存股票分析结果到数据库
def save_analysis_result(stock_code, market_type, result):
    """保存分析结果到数据库"""
    if not USE_DATABASE:
        return

    try:
        session = get_session()

        # 创建新的分析结果记录
        analysis = AnalysisResult(
            stock_code=stock_code,
            market_type=market_type,
            score=result.get('scores', {}).get('total', 0),
            recommendation=result.get('recommendation', {}).get('action', ''),
            technical_data=result.get('technical_analysis', {}),
            fundamental_data=result.get('fundamental_data', {}),
            capital_flow_data=result.get('capital_flow_data', {}),
            ai_analysis=result.get('ai_analysis', '')
        )

        session.add(analysis)
        session.commit()

    except Exception as e:
        app.logger.error(f"保存分析结果到数据库时出错: {str(e)}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()


# 从数据库获取历史分析结果
@app.route('/api/history_analysis', methods=['GET'])
def get_history_analysis():
    """获取股票的历史分析结果"""
    if not USE_DATABASE:
        return jsonify({'error': '数据库功能未启用'}), 400

    stock_code = request.args.get('stock_code')
    limit = int(request.args.get('limit', 10))

    if not stock_code:
        return jsonify({'error': '请提供股票代码'}), 400

    try:
        session = get_session()

        # 查询历史分析结果
        results = session.query(AnalysisResult) \
            .filter(AnalysisResult.stock_code == stock_code) \
            .order_by(AnalysisResult.analysis_date.desc()) \
            .limit(limit) \
            .all()

        # 转换为字典列表
        history = [result.to_dict() for result in results]

        return jsonify({'history': history})

    except Exception as e:
        app.logger.error(f"获取历史分析结果时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if session:
            session.close()

# 添加新闻API端点
# 添加到web_server.py文件中
@app.route('/api/latest_news', methods=['GET'])
def get_latest_news():
    try:
        days = int(request.args.get('days', 1))  # 默认获取1天的新闻
        limit = int(request.args.get('limit', 1000))  # 默认最多获取1000条
        only_important = request.args.get('important', '0') == '1'  # 是否只看重要新闻
        news_type = request.args.get('type', 'all')  # 新闻类型，可选值: all, hotspot

        # 从news_fetcher模块获取新闻数据
        news_data = news_fetcher.get_latest_news(days=days, limit=limit)

        # 过滤新闻
        if only_important:
            # 根据关键词过滤重要新闻
            important_keywords = ['重要', '利好', '重磅', '突发', '关注']
            news_data = [news for news in news_data if
                         any(keyword in (news.get('content', '') or '') for keyword in important_keywords)]

        if news_type == 'hotspot':
            # 过滤舆情热点相关新闻
            hotspot_keywords = [
                # 舆情直接相关词
                '舆情', '舆论', '热点', '热议', '热搜', '话题',

                # 关注度相关词
                '关注度', '高度关注', '引发关注', '市场关注', '持续关注', '重点关注',
                '密切关注', '广泛关注', '集中关注', '投资者关注',

                # 传播相关词
                '爆文', '刷屏', '刷爆', '冲上热搜', '纷纷转发', '广泛传播',
                '热传', '病毒式传播', '迅速扩散', '高度转发',

                # 社交媒体相关词
                '微博热搜', '微博话题', '知乎热议', '抖音热门', '今日头条', '朋友圈热议',
                '微信热文', '社交媒体热议', 'APP热榜',

                # 情绪相关词
                '情绪高涨', '市场情绪', '投资情绪', '恐慌情绪', '亢奋情绪',
                '乐观情绪', '悲观情绪', '投资者情绪', '公众情绪',

                # 突发事件相关
                '突发', '紧急', '爆发', '突现', '紧急事态', '快讯', '突发事件',
                '重大事件', '意外事件', '突发新闻',

                # 行业动态相关
                '行业动向', '市场动向', '板块轮动', '资金流向', '产业趋势',
                '政策导向', '监管动态', '风口', '市场风向',

                # 舆情分析相关
                '舆情分析', '舆情监测', '舆情报告', '舆情数据', '舆情研判',
                '舆情趋势', '舆情预警', '舆情通报', '舆情简报',

                # 市场焦点相关
                '市场焦点', '焦点话题', '焦点股', '焦点事件', '投资焦点',
                '关键词', '今日看点', '重点关切', '核心议题',

                # 传统媒体相关
                '头版头条', '财经头条', '要闻', '重磅新闻', '独家报道',
                '深度报道', '特别关注', '重点报道', '专题报道',

                # 特殊提示词
                '投资舆情', '今日舆情', '今日热点', '投资热点', '市场热点',
                '每日热点', '关注要点', '交易热点', '今日重点',

                # AI基础技术
                '人工智能', 'AI', '机器学习', '深度学习', '神经网络', '大模型',
                'LLM', '大语言模型', '生成式AI', '生成式人工智能', '算法',

                # AI细分技术
                '自然语言处理', 'NLP', '计算机视觉', 'CV', '语音识别',
                '图像生成', '多模态', '强化学习', '联邦学习', '知识图谱',
                '边缘计算', '量子计算', '类脑计算', '神经形态计算',

                # 热门AI模型/产品
                'GPT', 'GPT-4', 'GPT-5', 'GPT-4o', 'ChatGPT', 'Claude',
                'Gemini', 'Llama', 'Llama3', 'Stable Diffusion', 'DALL-E',
                'Midjourney', 'Sora', 'Anthropic', 'Runway', 'Copilot',
                'Bard', 'GLM', 'Ernie', '文心一言', '通义千问', '讯飞星火','DeepSeek',

                # AI应用领域
                'AIGC', '智能驾驶', '自动驾驶', '智能助手', '智能医疗',
                '智能制造', '智能客服', '智能金融', '智能教育',
                '智能家居', '机器人', 'RPA', '数字人', '虚拟人',
                '智能安防', '计算机辅助',

                # AI硬件
                'AI芯片', 'GPU', 'TPU', 'NPU', 'FPGA', '算力', '推理芯片',
                '训练芯片', 'NVIDIA', '英伟达', 'AMD', '高性能计算',

                # AI企业
                'OpenAI', '微软AI', '谷歌AI', 'Google DeepMind', 'Meta AI',
                '百度智能云', '阿里云AI', '腾讯AI', '华为AI', '商汤科技',
                '旷视科技', '智源人工智能', '云从科技', '科大讯飞',

                # AI监管/伦理
                'AI监管', 'AI伦理', 'AI安全', 'AI风险', 'AI治理',
                'AI对齐', 'AI偏见', 'AI隐私', 'AGI', '通用人工智能',
                '超级智能', 'AI法规', 'AI责任', 'AI透明度',

                # AI市场趋势
                'AI创业', 'AI投资', 'AI融资', 'AI估值', 'AI泡沫',
                'AI风口', 'AI赛道', 'AI产业链', 'AI应用落地', 'AI转型',
                'AI红利', 'AI市值', 'AI概念股',

                # 新兴AI概念
                'AI Agent', 'AI智能体', '多智能体', '自主AI',
                'AI搜索引擎', 'RAG', '检索增强生成', '思维链', 'CoT',
                '大模型微调', '提示工程', 'Prompt Engineering',
                '基础模型', 'Foundation Model', '小模型', '专用模型',

                # 人工智能舆情专用
                'AI热点', 'AI风潮', 'AI革命', 'AI热议', 'AI突破',
                'AI进展', 'AI挑战', 'AI竞赛', 'AI战略', 'AI政策',
                'AI风险', 'AI恐慌', 'AI威胁', 'AI机遇'
            ]

            # 在API处理中使用
            if news_type == 'hotspot':
                # 过滤舆情热点相关新闻
                def has_keyword(item):
                    title = item.get('title', '')
                    content = item.get('content', '')
                    return any(keyword in title for keyword in hotspot_keywords) or \
                        any(keyword in content for keyword in hotspot_keywords)

                news_data = [news for news in news_data if has_keyword(news)]

        # 为新闻关联股票
        try:
            linker = get_news_linker()
            news_data = linker.batch_link_news(news_data)
        except Exception as link_err:
            app.logger.warning(f"新闻关联股票失败: {link_err}")

        return jsonify({'success': True, 'news': news_data})
    except Exception as e:
        app.logger.error(f"获取最新新闻数据时出错: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== 市场情绪/择时/热点API ====================

@app.route('/api/market_sentiment', methods=['GET'])
def api_market_sentiment():
    """获取市场情绪摘要数据（涨跌停/封板率/赚钱效应/仓位建议/择时信号）"""
    try:
        result = get_market_sentiment_summary()
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取市场情绪数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e), 'status': 'error'}), 500


@app.route('/api/hot_sectors', methods=['GET'])
def api_hot_sectors():
    """获取热点板块数据（近3日行业涨幅排名+主线识别）"""
    try:
        result = get_hot_sectors()
        return custom_jsonify(result)
    except Exception as e:
        app.logger.error(f"获取热点板块数据出错: {traceback.format_exc()}")
        return jsonify({'error': str(e), 'status': 'error'}), 500


# 导入多模型分析器
from multi_model_analyzer import MultiModelAnalyzer
from debate_multi_model_analyzer import DebateMultiModelAnalyzer
from enhanced_debate_analyzer import EnhancedDebateAnalyzer

# 初始化多模型分析器
multi_model_analyzer = MultiModelAnalyzer()
debate_analyzer = DebateMultiModelAnalyzer()
enhanced_analyzer = EnhancedDebateAnalyzer()

# 多模型协作分析API
@app.route('/api/multi_model_analysis', methods=['POST'])
def api_multi_model_analysis():
    """多模型协作分析API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        models = data.get('models')  # 可选：指定要使用的模型
        
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        
        app.logger.info(f"开始多模型分析: {stock_code}")
        
        # 执行多模型分析
        result = multi_model_analyzer.multi_model_analysis(
            stock_code=stock_code, 
            market_type=market_type,
            models=models
        )
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        
        return custom_jsonify(result)
        
    except Exception as e:
        app.logger.error(f"多模型分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/available_models', methods=['GET'])
def api_available_models():
    """获取可用模型列表"""
    try:
        return jsonify({
            'models': multi_model_analyzer.available_models,
            'total_count': len(multi_model_analyzer.available_models),
            'model_companies': debate_analyzer.available_models
        })
    except Exception as e:
        app.logger.error(f"获取模型列表出错: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 带辩论机制的多模型分析API
@app.route('/api/debate_analysis', methods=['POST'])
def api_debate_analysis():
    """带辩论机制的多模型分析API"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        models = data.get('models')  # 可选：指定要使用的模型
        
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        
        app.logger.info(f"开始带辩论机制的多模型分析: {stock_code}")
        
        # 执行带辩论机制的分析
        result = debate_analyzer.debate_analysis(
            stock_code=stock_code, 
            market_type=market_type,
            models=models
        )
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        
        return custom_jsonify(result)
        
    except Exception as e:
        app.logger.error(f"辩论分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

# 增强版辩论分析API - 基于FinGenius思想优化
@app.route('/api/enhanced_debate_analysis', methods=['POST'])
def api_enhanced_debate_analysis():
    """增强版辩论分析API - 严格两阶段分离"""
    try:
        data = request.json
        stock_code = data.get('stock_code')
        market_type = data.get('market_type', 'A')
        
        if not stock_code:
            return jsonify({'error': '请提供股票代码'}), 400
        
        app.logger.info(f"开始增强版辩论分析: {stock_code}")
        
        # 使用asyncio运行异步分析
        import asyncio
        
        async def run_analysis():
            return await enhanced_analyzer.enhanced_analysis(
                stock_code=stock_code,
                market_type=market_type
            )
        
        # 使用asyncio.run运行异步分析
        try:
            result = asyncio.run(run_analysis())
        except Exception as e:
            app.logger.error(f"异步执行失败: {e}")
            return jsonify({'error': f'异步执行失败: {str(e)}'}), 500
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        
        return custom_jsonify(result)
        
    except Exception as e:
        app.logger.error(f"增强版辩论分析出错: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

# 在应用启动时启动清理线程（保持原有代码不变）
cleaner_thread = threading.Thread(target=run_task_cleaner)
cleaner_thread.daemon = True
cleaner_thread.start()

# 启动 Claw 数据自动同步
if CLAW_AVAILABLE and start_auto_sync:
    start_auto_sync()

if __name__ == '__main__':
    # 将 host 设置为 '0.0.0.0' 使其支持所有网络接口访问
    app.run(host='0.0.0.0', port=8890, debug=False)