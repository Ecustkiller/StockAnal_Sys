#!/usr/bin/env python3
"""
A股量化日报 Dashboard — 独立 Flask 应用（端口 5099）
=====================================================

与原 app.py（端口 5088）互不干扰。

路由:
  GET  /                         → dashboard.html
  GET  /api/dashboard/status     → 数据源状态 + 最新报告
  GET  /api/dashboard/tasks      → 所有任务状态
  POST /api/dashboard/run        → 触发任务   body: {"job": "update_daily"}
  GET  /api/dashboard/logs       → 获取某任务的实时日志  ?job=xxx&tail=200
  POST /api/dashboard/cancel     → 取消任务   body: {"job": "xxx"}
  GET  /api/dashboard/reports    → 报告列表   ?kind=bci|daily
  GET  /api/dashboard/report     → 单个报告内容 ?path=relpath

启动:
  python3 -m claw.web.dashboard_app
  # 或
  bash claw/web/start_dashboard.sh
"""
from __future__ import annotations

import os
import sys
import json
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# 确保可以 import claw.*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from claw.web.task_runner import get_runner
from claw.web import dashboard_utils as du


# ==================== 任务定义 ====================
# 每个任务都是一个 shell 命令，用 python3 调用已有模块
# 注意：必须用 python3（而非 py/python），遵循项目约定
TASK_SPEC = {
    'update_daily': {
        'name': '📥 更新日线数据',
        'cmd': ['python3', '-m', 'claw.data_pipeline.update_daily'],
        'desc': '从 Ashare 增量更新 ~/stock_data 下约 5900 只股票的日线',
    },
    'update_60min': {
        'name': '📥 更新60分钟K线',
        'cmd': ['python3', '-m', 'claw.data_pipeline.update_60min'],
        'desc': '更新 60min K线数据',
    },
    'update_all': {
        'name': '⚡ 一键全量更新（并发）',
        'cmd': ['python3', '-m', 'claw.data_pipeline.daily_update_turbo', '--workers', '10'],
        'desc': '并发更新日线+分钟数据（约 3-5 分钟，推荐）',
    },
    'update_all_slow': {
        'name': '🐢 一键全量更新（串行·稳妥）',
        'cmd': ['python3', '-m', 'claw.data_pipeline.daily_update_fast'],
        'desc': '串行更新（约 20 分钟），仅在并发版本触发限流时使用',
    },
    'run_score': {
        'name': '🧮 运行九维评分',
        'cmd': ['python3', '-m', 'claw.scoring.score_system'],
        'desc': '九维评分 → TOP30 评分池',
    },
    'run_elite': {
        'name': '🎯 精选选股 (策略1/2)',
        'cmd': ['python3', '-m', 'claw.scoring.elite_picker'],
        'desc': '严格精选 TOP5/10（策略01 + 策略02 主板）',
    },
    'run_bci': {
        'name': '🧩 BCI 板块完整性分析',
        'cmd': ['python3', '-m', 'claw.analysis.bci_analyzer_v3'],
        'desc': 'BCI v3 板块完整性分析（东方财富概念标签）',
    },
}


# ==================== Flask 初始化 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path='/static')
CORS(app)


# ==================== 路由 ====================
@app.route('/')
def home():
    """Dashboard 主页"""
    return send_from_directory(STATIC_DIR, 'dashboard.html')


@app.route('/api/dashboard/tasks_spec', methods=['GET'])
def api_tasks_spec():
    """返回所有可用任务的元数据（前端渲染按钮用）"""
    return jsonify({
        'tasks': [
            {'job': k, 'name': v['name'], 'desc': v['desc']}
            for k, v in TASK_SPEC.items()
        ]
    })


@app.route('/api/dashboard/status', methods=['GET'])
def api_status():
    """数据源状态 + 最新报告摘要"""
    try:
        return jsonify(du.full_status())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/tasks', methods=['GET'])
def api_tasks():
    """所有任务的当前状态"""
    runner = get_runner()
    status = runner.get_all_status()
    # 合并 spec 信息，给没运行过的任务也返回 idle 状态
    result = {}
    for job, spec in TASK_SPEC.items():
        s = status.get(job)
        if s is None:
            s = {
                'job_key': job, 'name': spec['name'], 'cmd': spec['cmd'],
                'status': 'idle', 'pid': None, 'started_at': None,
                'ended_at': None, 'exit_code': None, 'last_msg': '(尚未运行)',
                'log_file': None,
            }
        else:
            # 补齐 name（历史记录里可能是旧名字）
            s['name'] = spec['name']
        result[job] = s
    return jsonify({'tasks': result})


@app.route('/api/dashboard/run', methods=['POST'])
def api_run():
    """触发一个任务"""
    data = request.get_json(silent=True) or {}
    job = data.get('job', '').strip()
    if job not in TASK_SPEC:
        return jsonify({'error': f'未知任务: {job}'}), 400

    spec = TASK_SPEC[job]
    runner = get_runner()

    try:
        info = runner.start(job, spec['name'], spec['cmd'])
        return jsonify({'ok': True, 'info': info.to_dict()})
    except RuntimeError as e:
        return jsonify({'ok': False, 'error': str(e)}), 409
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/dashboard/logs', methods=['GET'])
def api_logs():
    """实时日志（前端轮询）"""
    job = request.args.get('job', '').strip()
    tail = int(request.args.get('tail', 200))
    if job not in TASK_SPEC:
        return jsonify({'error': f'未知任务: {job}'}), 400
    runner = get_runner()
    return jsonify({
        'job': job,
        'status': runner.get_status(job),
        'logs': runner.get_logs(job, tail=tail),
    })


@app.route('/api/dashboard/cancel', methods=['POST'])
def api_cancel():
    data = request.get_json(silent=True) or {}
    job = data.get('job', '').strip()
    ok = get_runner().cancel(job)
    return jsonify({'ok': ok})


@app.route('/api/dashboard/reports', methods=['GET'])
def api_reports():
    """报告列表"""
    kind = request.args.get('kind', 'bci')
    limit = int(request.args.get('limit', 10))
    if kind == 'bci':
        return jsonify({'kind': 'bci', 'items': du.list_bci_reports(limit)})
    elif kind == 'daily':
        return jsonify({'kind': 'daily', 'items': du.list_daily_plans(limit)})
    else:
        return jsonify({'error': 'kind 必须是 bci 或 daily'}), 400


@app.route('/api/dashboard/report', methods=['GET'])
def api_report():
    """单个报告内容（Markdown）"""
    rel = request.args.get('path', '').strip()
    if not rel:
        return jsonify({'error': '缺少 path 参数'}), 400
    content = du.read_report(rel)
    if content is None:
        return jsonify({'error': f'无法读取: {rel}'}), 404
    return jsonify({'path': rel, 'content': content})


# ==================== 启动 ====================
if __name__ == '__main__':
    port = int(os.environ.get('DASHBOARD_PORT', 5099))
    print('=' * 60)
    print('📊 A股量化日报 Dashboard')
    print(f'  项目根:   {du.PROJECT_ROOT}')
    print(f'  日线目录: {du.STOCK_DATA_DIR}')
    print(f'  60min:    {du.MIN60_DIR}')
    print(f'  BCI 报告: {du.BCI_DIR}')
    print(f'  可用任务: {len(TASK_SPEC)} 个')
    print(f'  访问地址: http://localhost:{port}')
    print('=' * 60)
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
