#!/usr/bin/env python3
"""
Skill权重迭代更新器

用法：python3 update_skill_weights.py <复盘数据JSON>

复盘数据JSON格式示例：
{
  "batch": "20260416",
  "skill_results": {
    "TXCG": {"avg_return": 14.2, "win_rate": 67, "count": 3},
    "元子元": {"avg_return": 17.2, "win_rate": 100, "count": 3},
    "山茶花": {"avg_return": 10.5, "win_rate": 80, "count": 5},
    "Mistery": {"avg_return": 2.7, "win_rate": 80, "count": 5},
    "TDS": {"avg_return": 10.0, "win_rate": 100, "count": 4},
    "百胜WR": {"avg_return": 17.0, "win_rate": 100, "count": 4},
    "事件驱动": {"avg_return": 5.3, "win_rate": 75, "count": 4},
    "多周期": {"avg_return": 8.0, "win_rate": 70, "count": 3},
    "基本面": {"avg_return": 6.0, "win_rate": 60, "count": 3}
  }
}

权重调整公式：新权重 = 旧权重 × (1 + 超额收益率 / 10)
- 超额收益率 = 该Skill平均收益 - 全部Skill平均收益
- 权重上下限：[5, 25]
- 胜率<50%的Skill额外惩罚：权重×0.9
"""
import sys, json, os
from datetime import datetime

WEIGHTS_FILE = os.path.expanduser("~/WorkBuddy/Claw/track/skill_weights.json")
MIN_WEIGHT = 5
MAX_WEIGHT = 25

def update_weights(review_data):
    # 读取当前权重
    with open(WEIGHTS_FILE, 'r') as f:
        data = json.load(f)
    
    old_weights = data['weights']
    skill_results = review_data['skill_results']
    batch = review_data['batch']
    
    # 计算全局平均收益
    all_returns = [v['avg_return'] for v in skill_results.values() if v.get('count', 0) > 0]
    global_avg = sum(all_returns) / len(all_returns) if all_returns else 0
    
    # 调整权重
    new_weights = {}
    adjustments = []
    
    for skill, old_w in old_weights.items():
        if skill in skill_results and skill_results[skill].get('count', 0) > 0:
            sr = skill_results[skill]
            excess_return = sr['avg_return'] - global_avg
            
            # 核心公式
            multiplier = 1 + excess_return / 10
            new_w = old_w * multiplier
            
            # 胜率惩罚
            if sr.get('win_rate', 0) < 50:
                new_w *= 0.9
            
            # 上下限
            new_w = max(MIN_WEIGHT, min(MAX_WEIGHT, round(new_w, 1)))
            new_weights[skill] = new_w
            
            adjustments.append({
                'skill': skill,
                'old': old_w,
                'new': new_w,
                'change': new_w - old_w,
                'excess_return': excess_return,
                'win_rate': sr.get('win_rate', 0),
            })
        else:
            new_weights[skill] = old_w  # 无数据的保持不变
    
    # 更新文件
    data['weights'] = new_weights
    data['version'] = f"v3.0_review_{batch}"
    data['last_review'] = batch
    data['review_count'] = data.get('review_count', 0) + 1
    
    # 记录历史
    if 'weight_history' not in data:
        data['weight_history'] = []
    data['weight_history'].append({
        'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'batch': batch,
        'old_weights': old_weights,
        'new_weights': new_weights,
        'global_avg_return': global_avg,
    })
    
    # 记录Skill表现
    if 'skill_performance' not in data:
        data['skill_performance'] = {}
    for skill, sr in skill_results.items():
        if skill not in data['skill_performance']:
            data['skill_performance'][skill] = []
        data['skill_performance'][skill].append({
            'batch': batch,
            'avg_return': sr['avg_return'],
            'win_rate': sr.get('win_rate', 0),
            'count': sr.get('count', 0),
        })
    
    with open(WEIGHTS_FILE, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    # 输出
    print(f"\n{'='*70}")
    print(f"Skill权重迭代更新 — 批次{batch}")
    print(f"{'='*70}")
    print(f"全局平均收益: {global_avg:+.1f}%")
    print(f"\n{'Skill':<10} {'旧权重':>6} {'新权重':>6} {'变化':>6} {'超额':>6} {'胜率':>5}")
    print(f"-"*45)
    for adj in sorted(adjustments, key=lambda x: x['change'], reverse=True):
        arrow = "↑" if adj['change'] > 0 else ("↓" if adj['change'] < 0 else "→")
        print(f"{adj['skill']:<10} {adj['old']:>6.1f} {adj['new']:>6.1f} {adj['change']:>+5.1f}{arrow} {adj['excess_return']:>+5.1f}% {adj['win_rate']:>4.0f}%")
    
    print(f"\n满分: {sum(old_weights.values())} → {sum(new_weights.values())}")
    print(f"权重文件已更新: {WEIGHTS_FILE}")
    
    return new_weights

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 update_skill_weights.py '<JSON字符串>'")
        print("或:   python3 update_skill_weights.py review_data.json")
        sys.exit(1)
    
    arg = sys.argv[1]
    if os.path.exists(arg):
        with open(arg) as f:
            review_data = json.load(f)
    else:
        review_data = json.loads(arg)
    
    update_weights(review_data)
