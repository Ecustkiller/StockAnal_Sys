#!/usr/bin/env python3
"""
Dashboard 辅助工具 — 数据状态检查 + 报告索引
============================================

数据源:
  - ~/stock_data/         日线 CSV 目录（逐只股票的历史数据）
  - ~/Downloads/2026/60min/  60分钟K线目录
  - <project>/reports/bci_analysis/   BCI 报告
  - <project>/reports/daily_plans/    日度选股计划

所有函数都是只读的、无副作用，可以被 Flask 路由安全调用。
"""
from __future__ import annotations

import os
import glob
import re
from datetime import datetime, date
from typing import List, Dict, Optional


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
STOCK_DATA_DIR = os.path.expanduser('~/stock_data')
MIN60_DIR = os.path.expanduser('~/Downloads/2026/60min')

BCI_DIR = os.path.join(PROJECT_ROOT, 'reports', 'bci_analysis')
DAILY_PLANS_DIR = os.path.join(PROJECT_ROOT, 'reports', 'daily_plans')


def _read_last_line(fp: str, read_bytes: int = 400) -> Optional[str]:
    """读取文件最后一行（高效：从尾部读取）"""
    try:
        size = os.path.getsize(fp)
        with open(fp, 'rb') as f:
            f.seek(-min(read_bytes, size), 2)
            lines = f.readlines()
        if not lines:
            return None
        return lines[-1].decode('utf-8', errors='replace').strip()
    except Exception:
        return None


# ==================== 日线数据 ====================
def check_daily_status() -> Dict:
    """检查 ~/stock_data/ 下日线数据的最新日期"""
    if not os.path.isdir(STOCK_DATA_DIR):
        return {
            'ok': False,
            'msg': f'目录不存在: {STOCK_DATA_DIR}',
            'file_count': 0,
            'latest_date': None,
        }

    files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not files:
        return {
            'ok': False,
            'msg': '目录为空',
            'file_count': 0,
            'latest_date': None,
        }

    # 抽样 20 只股票看最新日期（全扫5900+个太慢）
    sample = sorted(files)[::max(1, len(files) // 20)][:30]
    latest_dates = []
    for fp in sample:
        last = _read_last_line(fp)
        if not last:
            continue
        parts = last.split(',')
        if parts and parts[0]:
            d = parts[0].replace('-', '').replace('.0', '')
            if re.fullmatch(r'\d{8}', d):
                latest_dates.append(d)

    if not latest_dates:
        return {
            'ok': False,
            'msg': '无法解析数据日期',
            'file_count': len(files),
            'latest_date': None,
        }

    # 取众数作为"整体最新日期"（避免个别股票停牌影响）
    from collections import Counter
    mode_date, _ = Counter(latest_dates).most_common(1)[0]

    today = datetime.now().strftime('%Y%m%d')
    is_today = mode_date >= today

    return {
        'ok': True,
        'file_count': len(files),
        'latest_date': mode_date,
        'latest_date_display': f"{mode_date[:4]}-{mode_date[4:6]}-{mode_date[6:]}",
        'is_today': is_today,
        'days_behind': _days_between(mode_date, today),
    }


def check_60min_status() -> Dict:
    """检查 60min 数据的最新日期"""
    if not os.path.isdir(MIN60_DIR):
        return {
            'ok': False,
            'msg': f'目录不存在: {MIN60_DIR}',
            'file_count': 0,
            'latest_date': None,
        }

    files = glob.glob(os.path.join(MIN60_DIR, '*.csv'))
    if not files:
        return {
            'ok': False,
            'msg': '目录为空',
            'file_count': 0,
            'latest_date': None,
        }

    sample = sorted(files)[:30]
    latest_dates = []
    for fp in sample:
        last = _read_last_line(fp)
        if not last:
            continue
        parts = last.split(',')
        if parts and parts[0]:
            d = parts[0]  # 通常是 2026-04-21 格式
            latest_dates.append(d)

    if not latest_dates:
        return {'ok': False, 'msg': '无法解析', 'file_count': len(files), 'latest_date': None}

    from collections import Counter
    mode_date, _ = Counter(latest_dates).most_common(1)[0]
    today = datetime.now().strftime('%Y-%m-%d')
    return {
        'ok': True,
        'file_count': len(files),
        'latest_date': mode_date,
        'latest_date_display': mode_date,
        'is_today': mode_date >= today,
        'days_behind': _days_between(mode_date.replace('-', ''), today.replace('-', '')),
    }


def _days_between(d1: str, d2: str) -> int:
    """两个 yyyymmdd 字符串的日期差"""
    try:
        dt1 = datetime.strptime(d1, '%Y%m%d')
        dt2 = datetime.strptime(d2, '%Y%m%d')
        return (dt2 - dt1).days
    except Exception:
        return -1


# ==================== BCI 报告 ====================
def list_bci_reports(limit: int = 10) -> List[Dict]:
    """列出最新的 BCI 报告"""
    if not os.path.isdir(BCI_DIR):
        return []
    files = glob.glob(os.path.join(BCI_DIR, '*.md'))
    results = []
    for fp in files:
        name = os.path.basename(fp)
        # 从文件名提取日期 yyyymmdd
        m = re.search(r'(\d{8})', name)
        dt = m.group(1) if m else ''
        results.append({
            'name': name,
            'path': fp,
            'rel_path': os.path.relpath(fp, PROJECT_ROOT),
            'date': dt,
            'size': os.path.getsize(fp),
            'mtime': datetime.fromtimestamp(os.path.getmtime(fp)).strftime('%Y-%m-%d %H:%M'),
        })
    # 按日期降序 + 文件名版本号（v3 > v2 > 无版本）
    def ver(name):
        if 'v3' in name: return 3
        if 'v2' in name: return 2
        return 1
    results.sort(key=lambda x: (x['date'], ver(x['name'])), reverse=True)
    return results[:limit]


def get_latest_bci_report() -> Optional[Dict]:
    reports = list_bci_reports(limit=1)
    return reports[0] if reports else None


# ==================== 日度选股计划 ====================
def list_daily_plans(limit: int = 20) -> List[Dict]:
    """列出最新的选股/作战计划报告"""
    if not os.path.isdir(DAILY_PLANS_DIR):
        return []
    files = glob.glob(os.path.join(DAILY_PLANS_DIR, '*.md'))
    results = []
    for fp in files:
        name = os.path.basename(fp)
        m = re.search(r'(\d{8})', name) or re.search(r'(\d{4})[_年](\d{1,2})', name)
        dt = m.group(1) if m else ''
        results.append({
            'name': name,
            'path': fp,
            'rel_path': os.path.relpath(fp, PROJECT_ROOT),
            'date': dt,
            'size': os.path.getsize(fp),
            'mtime': datetime.fromtimestamp(os.path.getmtime(fp)).strftime('%Y-%m-%d %H:%M'),
        })
    results.sort(key=lambda x: x['mtime'], reverse=True)
    return results[:limit]


# ==================== 报告内容读取 ====================
def read_report(rel_path: str) -> Optional[str]:
    """
    读取 reports/ 目录下的一个 .md 文件。
    出于安全考虑，限制 rel_path 必须在 reports/ 下。
    """
    full = os.path.normpath(os.path.join(PROJECT_ROOT, rel_path))
    reports_root = os.path.normpath(os.path.join(PROJECT_ROOT, 'reports'))
    if not full.startswith(reports_root):
        return None
    if not os.path.isfile(full):
        return None
    try:
        with open(full, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return None


# ==================== 综合状态 ====================
def full_status() -> Dict:
    """给前端主页用的完整状态"""
    bci_latest = get_latest_bci_report()
    return {
        'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'daily': check_daily_status(),
        'min60': check_60min_status(),
        'bci_latest': bci_latest,
        'daily_plans_count': len(list_daily_plans(limit=999)),
    }


if __name__ == '__main__':
    import json
    print(json.dumps(full_status(), ensure_ascii=False, indent=2))
