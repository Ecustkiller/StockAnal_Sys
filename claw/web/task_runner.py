#!/usr/bin/env python3
"""
后台任务执行器 — 负责管理 Dashboard 触发的长耗时任务
==================================================

特性:
  1. 每个 job_key（例如 'update_daily'）同时只能有一个任务在跑
  2. 任务用 subprocess 启动，通过管道实时收集 stdout/stderr
  3. 保留最近 N 行日志在内存里，前端可以轮询拿到
  4. 任务历史保存到 data/dashboard_tasks.json（进程重启后能恢复状态）

使用:
    runner = get_runner()
    job = runner.start('update_daily', ['python3', '-m', 'claw.data_pipeline.update_daily'])
    # ...
    status = runner.get_status('update_daily')
    logs   = runner.get_logs('update_daily', tail=100)
"""
from __future__ import annotations

import os
import json
import time
import threading
import subprocess
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional


# ==== 常量 ====
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
TASK_LOG_DIR = os.path.join(PROJECT_ROOT, 'data', 'dashboard_logs')
TASK_STATE_FILE = os.path.join(PROJECT_ROOT, 'data', 'dashboard_tasks.json')
MAX_LOG_LINES = 2000   # 每个任务在内存里保留的最大日志行数

os.makedirs(TASK_LOG_DIR, exist_ok=True)


@dataclass
class TaskInfo:
    """任务描述与当前状态"""
    job_key: str                      # 任务唯一键（例如 'update_daily'）
    name: str                         # 人类可读的名字
    cmd: List[str]                    # subprocess 命令
    status: str = 'idle'              # idle / running / success / failed
    pid: Optional[int] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    exit_code: Optional[int] = None
    last_msg: str = ''                # 最后一行日志（给前端做进度展示）
    log_file: Optional[str] = None    # 本次运行的完整日志文件

    def to_dict(self) -> dict:
        return asdict(self)


class TaskRunner:
    """线程安全的任务管理器（单例）"""

    def __init__(self):
        # 用 RLock 支持可重入（start() 内会调用 is_running() 等带锁方法）
        self._lock = threading.RLock()
        self._tasks: Dict[str, TaskInfo] = {}
        # job_key -> 内存日志 deque
        self._log_buffers: Dict[str, deque] = {}
        # job_key -> Popen 对象
        self._procs: Dict[str, subprocess.Popen] = {}
        self._load_state()

    # ------------------ 持久化 ------------------
    def _load_state(self):
        if not os.path.exists(TASK_STATE_FILE):
            return
        try:
            with open(TASK_STATE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for key, info in data.items():
                # 如果上次进程被强杀，状态停在 running，这里纠正为 failed
                if info.get('status') == 'running':
                    info['status'] = 'failed'
                    info['last_msg'] = '进程异常终止（Dashboard 重启恢复）'
                self._tasks[key] = TaskInfo(**{k: v for k, v in info.items()
                                               if k in TaskInfo.__annotations__})
        except Exception as e:
            print(f"[TaskRunner] 加载历史状态失败: {e}")

    def _save_state(self):
        try:
            data = {k: v.to_dict() for k, v in self._tasks.items()}
            with open(TASK_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[TaskRunner] 保存状态失败: {e}")

    # ------------------ 日志 ------------------
    def _append_log(self, job_key: str, line: str, log_file_path: str):
        """
        追加一行日志。
        线程安全：只在持锁时修改 dict/deque，写磁盘前释放锁。
        """
        ts_line = f"[{datetime.now().strftime('%H:%M:%S')}] {line.rstrip()}"
        with self._lock:
            buf = self._log_buffers.setdefault(job_key, deque(maxlen=MAX_LOG_LINES))
            buf.append(ts_line)
            t = self._tasks.get(job_key)
            if t is not None:
                t.last_msg = line.rstrip()[:200]
        # 写磁盘放在锁外（I/O 不应阻塞其他 HTTP 请求）
        try:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                f.write(ts_line + '\n')
        except Exception:
            pass

    # ------------------ 任务控制 ------------------
    def is_running(self, job_key: str) -> bool:
        with self._lock:
            t = self._tasks.get(job_key)
            return bool(t and t.status == 'running')

    def start(self, job_key: str, name: str, cmd: List[str],
              cwd: Optional[str] = None) -> TaskInfo:
        """启动一个任务（同名任务若在跑会抛错）"""
        with self._lock:
            if self.is_running(job_key):
                raise RuntimeError(f"任务 {job_key} 正在运行中，请等待完成")

            ts_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = os.path.join(TASK_LOG_DIR, f"{job_key}_{ts_tag}.log")

            info = TaskInfo(
                job_key=job_key,
                name=name,
                cmd=list(cmd),
                status='running',
                started_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                ended_at=None,
                exit_code=None,
                last_msg='启动中...',
                log_file=log_file,
            )
            self._tasks[job_key] = info
            # 重置日志
            self._log_buffers[job_key] = deque(maxlen=MAX_LOG_LINES)
            self._save_state()

        # 后台线程跑
        thread = threading.Thread(
            target=self._run_task,
            args=(job_key, cmd, cwd or PROJECT_ROOT, log_file),
            daemon=True,
        )
        thread.start()
        return info

    def _run_task(self, job_key: str, cmd: List[str], cwd: str, log_file: str):
        """实际执行任务的线程函数"""
        self._append_log(job_key, f"$ cd {cwd}", log_file)
        self._append_log(job_key, f"$ {' '.join(cmd)}", log_file)

        try:
            # 用 unbuffered 模式 + 合并 stderr 到 stdout，实时拿输出
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'
            proc = subprocess.Popen(
                cmd,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env,
            )

            with self._lock:
                self._procs[job_key] = proc
                t = self._tasks[job_key]
                t.pid = proc.pid

            # 按行读取
            assert proc.stdout is not None
            for line in proc.stdout:
                if not line:
                    continue
                self._append_log(job_key, line, log_file)

            proc.wait()
            code = proc.returncode

            with self._lock:
                t = self._tasks[job_key]
                t.status = 'success' if code == 0 else 'failed'
                t.exit_code = code
                t.ended_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if code == 0:
                    t.last_msg = '✅ 任务完成'
                else:
                    t.last_msg = f'❌ 任务失败（exit={code}）'
                self._procs.pop(job_key, None)
                self._save_state()

            self._append_log(job_key, f"[结束] exit_code={code}", log_file)

        except Exception as e:
            self._append_log(job_key, f"[异常] {e}", log_file)
            with self._lock:
                t = self._tasks.get(job_key)
                if t:
                    t.status = 'failed'
                    t.ended_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    t.last_msg = f'❌ 异常: {e}'
                self._procs.pop(job_key, None)
                self._save_state()

    def cancel(self, job_key: str) -> bool:
        """终止一个运行中的任务"""
        with self._lock:
            proc = self._procs.get(job_key)
            if proc is None:
                return False
            try:
                proc.terminate()
            except Exception:
                pass
            return True

    # ------------------ 查询 ------------------
    def get_status(self, job_key: str) -> Optional[dict]:
        with self._lock:
            t = self._tasks.get(job_key)
            return t.to_dict() if t else None

    def get_all_status(self) -> Dict[str, dict]:
        with self._lock:
            return {k: v.to_dict() for k, v in self._tasks.items()}

    def get_logs(self, job_key: str, tail: int = 200) -> List[str]:
        with self._lock:
            buf = self._log_buffers.get(job_key)
            if buf is None:
                # 尝试从磁盘加载
                t = self._tasks.get(job_key)
                if t and t.log_file and os.path.exists(t.log_file):
                    try:
                        with open(t.log_file, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                        return [ln.rstrip() for ln in lines[-tail:]]
                    except Exception:
                        return []
                return []
            return list(buf)[-tail:]


# ============ 单例入口 ============
_runner_instance: Optional[TaskRunner] = None
_runner_lock = threading.Lock()


def get_runner() -> TaskRunner:
    global _runner_instance
    if _runner_instance is None:
        with _runner_lock:
            if _runner_instance is None:
                _runner_instance = TaskRunner()
    return _runner_instance


if __name__ == '__main__':
    # 简单自测
    r = get_runner()
    info = r.start('test_echo', '测试任务',
                   ['python3', '-c', 'import time; [print(f"step {i}") or time.sleep(0.3) for i in range(5)]'])
    print('started:', info.to_dict())
    while r.is_running('test_echo'):
        time.sleep(0.5)
        print('  ...', r.get_status('test_echo')['last_msg'])
    print('final:', r.get_status('test_echo'))
    print('logs:')
    for ln in r.get_logs('test_echo'):
        print(' ', ln)
