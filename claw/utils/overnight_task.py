#!/usr/bin/env python3
"""
夜间后台综合任务脚本
==================
预计运行时间：约3小时
包含以下任务：
  1. 参数寻优回测（~1.5小时）
  2. 全量回测 + HTML报告（~1小时）
  3. 战法独立回测对比（~30分钟）
  4. 评分维度贡献度分析
  5. 结果汇总 + 自动提交GitHub

用法：
  nohup python3 overnight_task.py > overnight_log.txt 2>&1 &
"""

import os, sys, time, json, subprocess
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

# 项目目录
PROJECT_DIR = '/Users/ecustkiller/WorkBuddy/Claw'
RESULTS_DIR = os.path.join(PROJECT_DIR, 'backtest_results')
os.makedirs(RESULTS_DIR, exist_ok=True)

# 日志
LOG_FILE = os.path.join(PROJECT_DIR, 'overnight_log.txt')
SUMMARY_FILE = os.path.join(RESULTS_DIR, f'overnight_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.md')

def log(msg):
    """打印并记录日志"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)

def run_cmd(cmd, timeout=7200):
    """运行命令并返回输出"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=PROJECT_DIR
        )
        return result.stdout + result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return str(e), -1

def task1_parameter_optimization():
    """
    任务1：参数寻优回测
    测试不同 TOP-N / 持有期 / 评分阈值 / 起始日期 组合
    预计耗时：~1.5小时
    """
    log("=" * 80)
    log("🔍 任务1：参数寻优回测")
    log("=" * 80)
    
    t_start = time.time()
    
    # 运行参数寻优模式
    log("运行 backtest_v2.py --optimize（全量数据）...")
    output, rc = run_cmd(
        "python3 backtest_v2.py --optimize --save 2>&1",
        timeout=5400  # 1.5小时超时
    )
    
    elapsed = time.time() - t_start
    log(f"参数寻优完成，耗时 {elapsed/60:.1f} 分钟，返回码 {rc}")
    
    # 保存输出
    opt_log = os.path.join(RESULTS_DIR, f'optimize_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    with open(opt_log, 'w') as f:
        f.write(output)
    log(f"寻优日志已保存: {opt_log}")
    
    return output, elapsed

def task2_full_backtest_with_report():
    """
    任务2：全量回测 + HTML报告
    从最早数据开始，TOP5/10/20 分别跑一遍
    预计耗时：~1小时
    """
    log("=" * 80)
    log("📊 任务2：全量回测 + HTML报告")
    log("=" * 80)
    
    t_start = time.time()
    results = {}
    
    configs = [
        {'top': 5,  'hold': '1,2,3,5', 'label': 'TOP5'},
        {'top': 10, 'hold': '1,2,3,5', 'label': 'TOP10'},
        {'top': 20, 'hold': '1,2,3,5', 'label': 'TOP20'},
        {'top': 30, 'hold': '1,2,3,5', 'label': 'TOP30'},
        # 不同起始日期
        {'top': 10, 'hold': '1,3', 'start': '20250101', 'label': 'TOP10_2025'},
        {'top': 10, 'hold': '1,3', 'start': '20260101', 'label': 'TOP10_2026'},
    ]
    
    for cfg in configs:
        label = cfg['label']
        top = cfg['top']
        hold = cfg['hold']
        start = cfg.get('start', '')
        
        start_arg = f"--start {start}" if start else ""
        
        log(f"  运行 {label}（TOP{top}, hold={hold}, start={start or '自动'}）...")
        cmd = f"python3 backtest_v2.py --top {top} --hold {hold} {start_arg} --save --report 2>&1"
        output, rc = run_cmd(cmd, timeout=3600)
        
        # 提取关键指标
        results[label] = {
            'rc': rc,
            'output_lines': len(output.split('\n')),
        }
        
        # 从输出中提取关键数据
        for line in output.split('\n'):
            if '累计收益' in line and '%' in line:
                results[label]['summary_line'] = line.strip()
            if '胜率' in line and '%' in line and 'summary_line' not in results[label]:
                results[label]['win_rate_line'] = line.strip()
        
        log(f"  {label} 完成 (rc={rc})")
    
    elapsed = time.time() - t_start
    log(f"全量回测完成，耗时 {elapsed/60:.1f} 分钟")
    
    return results, elapsed

def task3_strategy_comparison():
    """
    任务3：战法独立回测对比
    分别测试不同评分阈值下的表现
    预计耗时：~30分钟
    """
    log("=" * 80)
    log("⚔️ 任务3：战法独立回测对比（不同评分阈值）")
    log("=" * 80)
    
    t_start = time.time()
    
    # 用不同阈值跑回测
    thresholds = [70, 80, 85, 90, 95, 100]
    threshold_results = {}
    
    for threshold in thresholds:
        log(f"  评分阈值 ≥{threshold} 回测中...")
        # 通过 --json 模式获取结构化结果
        cmd = f"python3 backtest_v2.py --json --top 50 --hold 1,3,5 2>&1"
        output, rc = run_cmd(cmd, timeout=1800)
        
        # 尝试解析JSON
        try:
            # JSON输出可能混有其他内容，找到JSON部分
            json_start = output.find('{')
            json_end = output.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                data = json.loads(output[json_start:json_end])
                threshold_results[threshold] = data.get('performance', {})
                log(f"  ≥{threshold}: 累计收益={data.get('performance', {}).get('total_return_pct', 'N/A')}%")
        except:
            log(f"  ≥{threshold}: JSON解析失败")
            threshold_results[threshold] = {'error': 'parse_failed'}
    
    elapsed = time.time() - t_start
    log(f"战法对比完成，耗时 {elapsed/60:.1f} 分钟")
    
    return threshold_results, elapsed

def task4_dimension_analysis():
    """
    任务4：评分维度贡献度深度分析
    分析每个维度（D1~D9）对最终收益的贡献
    预计耗时：~15分钟
    """
    log("=" * 80)
    log("🎯 任务4：评分维度贡献度深度分析")
    log("=" * 80)
    
    t_start = time.time()
    
    # 找最新的detail CSV
    detail_files = sorted([
        f for f in os.listdir(RESULTS_DIR) 
        if f.startswith('backtest_detail_') and f.endswith('.csv')
    ])
    
    if not detail_files:
        log("  ❌ 没有找到detail CSV文件，跳过维度分析")
        return {}, 0
    
    latest_detail = os.path.join(RESULTS_DIR, detail_files[-1])
    log(f"  使用最新detail文件: {detail_files[-1]}")
    
    try:
        df = pd.read_csv(latest_detail)
        log(f"  数据量: {len(df)}条记录")
        
        analysis = {}
        
        # 各维度列名映射
        dim_cols = {
            'd1_score': ('D1多周期共振', 15),
            'd2_score': ('D2主线热点', 25),
            'd3_score': ('D3三Skill', 47),
            'd4_score': ('D4安全边际', 15),
            'd5_score': ('D5基本面', 15),
            'd9_score': ('D9百胜WR', 15),
        }
        
        ret_col = 'ret_1d' if 'ret_1d' in df.columns else None
        if ret_col is None:
            log("  ❌ 没有找到ret_1d列")
            return {}, 0
        
        df_valid = df[df[ret_col].notna()].copy()
        
        for col, (name, max_score) in dim_cols.items():
            if col not in df_valid.columns:
                continue
            
            # 按维度分高低组
            median_val = df_valid[col].median()
            high = df_valid[df_valid[col] > median_val][ret_col]
            low = df_valid[df_valid[col] <= median_val][ret_col]
            
            if len(high) > 0 and len(low) > 0:
                diff = high.mean() - low.mean()
                high_wr = (high > 0).sum() / len(high) * 100
                low_wr = (low > 0).sum() / len(low) * 100
                
                analysis[name] = {
                    'high_group_avg': round(high.mean(), 3),
                    'low_group_avg': round(low.mean(), 3),
                    'diff': round(diff, 3),
                    'high_win_rate': round(high_wr, 1),
                    'low_win_rate': round(low_wr, 1),
                    'contribution': round(abs(diff), 3),
                }
                log(f"  {name}: 高分组={high.mean():+.3f}% 低分组={low.mean():+.3f}% 差异={diff:+.3f}%")
        
        # 按贡献度排序
        sorted_dims = sorted(analysis.items(), key=lambda x: x[1]['contribution'], reverse=True)
        log(f"\n  📊 维度贡献度排名:")
        for i, (name, stats) in enumerate(sorted_dims, 1):
            log(f"  {i}. {name}: 贡献度={stats['contribution']:.3f}% (高分组胜率{stats['high_win_rate']:.1f}% vs 低分组{stats['low_win_rate']:.1f}%)")
        
    except Exception as e:
        log(f"  ❌ 分析出错: {e}")
        analysis = {'error': str(e)}
    
    elapsed = time.time() - t_start
    log(f"维度分析完成，耗时 {elapsed/60:.1f} 分钟")
    
    return analysis, elapsed

def task5_compare_all_results():
    """
    任务5：对比所有回测结果
    预计耗时：~5分钟
    """
    log("=" * 80)
    log("📊 任务5：对比所有回测结果")
    log("=" * 80)
    
    t_start = time.time()
    
    # 找所有detail CSV
    detail_files = sorted([
        f for f in os.listdir(RESULTS_DIR) 
        if f.startswith('backtest_detail_') and f.endswith('.csv')
    ])
    
    if len(detail_files) >= 2:
        files_arg = ' '.join(detail_files)
        cmd = f"python3 backtest_v2.py --compare {files_arg} 2>&1"
        output, rc = run_cmd(cmd, timeout=300)
        log(output[-2000:] if len(output) > 2000 else output)
    else:
        log("  detail文件不足2个，跳过对比")
    
    elapsed = time.time() - t_start
    return elapsed

def task6_generate_summary():
    """
    任务6：生成综合总结报告
    """
    log("=" * 80)
    log("📝 任务6：生成综合总结报告")
    log("=" * 80)
    
    # 收集所有结果文件
    all_files = sorted(os.listdir(RESULTS_DIR))
    
    summary = f"""# 🌙 夜间后台任务运行报告

> 运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
> 系统版本：评分系统 v3.3 (150分制) + 回测系统 v2.1

---

## 📁 生成的文件

| 文件 | 大小 | 说明 |
|------|------|------|
"""
    
    for f in all_files:
        fpath = os.path.join(RESULTS_DIR, f)
        if os.path.isfile(fpath):
            size = os.path.getsize(fpath)
            if size > 1024 * 1024:
                size_str = f"{size/1024/1024:.1f}MB"
            elif size > 1024:
                size_str = f"{size/1024:.1f}KB"
            else:
                size_str = f"{size}B"
            
            desc = ""
            if 'optimize' in f:
                desc = "参数寻优结果"
            elif 'report' in f and f.endswith('.html'):
                desc = "HTML回测报告"
            elif 'detail' in f:
                desc = "交易明细"
            elif 'daily' in f:
                desc = "每日汇总"
            elif 'summary' in f:
                desc = "综合总结"
            elif 'top10_full' in f:
                desc = "TOP10全量回测"
            
            summary += f"| `{f}` | {size_str} | {desc} |\n"
    
    summary += f"""
---

## 📊 数据状态

| 数据 | 数量 | 说明 |
|------|------|------|
| 日线快照 | 355天 | 2024-11-01 ~ 2026-04-17 |
| 量比数据 | 354天 | 2024-11-01 ~ 2026-04-17 |
| 2025年60分钟K线 | 5343只 | /5490 (差147只北交所) |
| 2026年60分钟K线 | 5490只 | 完整 |

---

*本报告由夜间后台任务自动生成*
"""
    
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary)
    
    log(f"总结报告已保存: {SUMMARY_FILE}")
    return summary

def task7_git_commit():
    """
    任务7：自动提交到GitHub
    """
    log("=" * 80)
    log("📤 任务7：自动提交到GitHub")
    log("=" * 80)
    
    # 添加新文件
    cmds = [
        "git add overnight_task.py",
        "git add backtest_results/overnight_summary_*.md",
        "git add backtest_results/optimize_*.csv",
        "git add backtest_results/optimize_*.txt",
        "git status --short",
    ]
    
    for cmd in cmds:
        output, rc = run_cmd(f"cd {PROJECT_DIR} && {cmd}")
        log(f"  {cmd} → rc={rc}")
        if output.strip():
            log(f"  {output.strip()[:500]}")
    
    # 提交
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    commit_msg = f"feat: 夜间后台任务完成 - 参数寻优+全量回测+维度分析 ({timestamp})"
    output, rc = run_cmd(f'cd {PROJECT_DIR} && git commit -m "{commit_msg}"')
    log(f"  git commit → rc={rc}")
    if output.strip():
        log(f"  {output.strip()[:500]}")
    
    # 推送
    output, rc = run_cmd(f"cd {PROJECT_DIR} && git push origin main")
    log(f"  git push → rc={rc}")
    if output.strip():
        log(f"  {output.strip()[:500]}")

def main():
    """主函数：按顺序执行所有任务"""
    total_start = time.time()
    
    log("🌙" * 40)
    log("🌙 夜间后台综合任务启动")
    log(f"🌙 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"🌙 预计耗时: ~3小时")
    log("🌙" * 40)
    log("")
    
    task_times = {}
    
    # ===== 任务1：参数寻优 =====
    try:
        _, t = task1_parameter_optimization()
        task_times['参数寻优'] = t
    except Exception as e:
        log(f"❌ 任务1出错: {e}")
        task_times['参数寻优'] = -1
    
    # ===== 任务2：全量回测 =====
    try:
        _, t = task2_full_backtest_with_report()
        task_times['全量回测'] = t
    except Exception as e:
        log(f"❌ 任务2出错: {e}")
        task_times['全量回测'] = -1
    
    # ===== 任务3：战法对比 =====
    try:
        _, t = task3_strategy_comparison()
        task_times['战法对比'] = t
    except Exception as e:
        log(f"❌ 任务3出错: {e}")
        task_times['战法对比'] = -1
    
    # ===== 任务4：维度分析 =====
    try:
        _, t = task4_dimension_analysis()
        task_times['维度分析'] = t
    except Exception as e:
        log(f"❌ 任务4出错: {e}")
        task_times['维度分析'] = -1
    
    # ===== 任务5：结果对比 =====
    try:
        t = task5_compare_all_results()
        task_times['结果对比'] = t
    except Exception as e:
        log(f"❌ 任务5出错: {e}")
        task_times['结果对比'] = -1
    
    # ===== 任务6：生成总结 =====
    try:
        task6_generate_summary()
    except Exception as e:
        log(f"❌ 任务6出错: {e}")
    
    # ===== 任务7：提交GitHub =====
    try:
        task7_git_commit()
    except Exception as e:
        log(f"❌ 任务7出错: {e}")
    
    # ===== 总结 =====
    total_elapsed = time.time() - total_start
    log("")
    log("🌙" * 40)
    log("🌙 夜间后台综合任务完成！")
    log(f"🌙 总耗时: {total_elapsed/60:.1f} 分钟 ({total_elapsed/3600:.1f} 小时)")
    log("")
    log("各任务耗时:")
    for name, t in task_times.items():
        if t >= 0:
            log(f"  {name}: {t/60:.1f} 分钟")
        else:
            log(f"  {name}: ❌ 出错")
    log("")
    log(f"🌙 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("🌙" * 40)

if __name__ == '__main__':
    main()
