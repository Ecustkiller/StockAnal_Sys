# -*- coding: utf-8 -*-
import baostock as bs
import pandas as pd
import os
import sys
import io
import time
from datetime import datetime, timedelta

# 设置标准输出编码为UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 配置参数
STOCK_DATA_DIR = "/Users/ecustkiller/stock_data"
ADJUST_FLAG = "2"  # 前复权

def login_baostock():
    """登录BaoStock系统"""
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        sys.exit(1)
    else:
        print("登录成功")
        return lg

def logout_baostock():
    """登出BaoStock系统"""
    bs.logout()
    print("登出成功")

def get_latest_trading_date():
    """获取最新交易日"""
    today = datetime.now()
    # 向前查找最近的工作日
    for i in range(7):  # 最多查找7天
        check_date = today - timedelta(days=i)
        # 排除周六（5）和周日（6）
        if check_date.weekday() < 5:
            return check_date.strftime('%Y-%m-%d')
    return today.strftime('%Y-%m-%d')

def get_last_date_from_file(file_path):
    """从CSV文件获取最后一条记录的日期"""
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            return df.iloc[-1]['date']
    except Exception as e:
        print(f"读取文件失败 {file_path}: {e}")
    return None

def update_stock_data(stock_code, stock_name, file_path):
    """更新单只股票数据"""
    try:
        # 获取文件中的最后日期
        last_date = get_last_date_from_file(file_path)
        if not last_date:
            print(f"无法获取 {stock_code} 的最后日期，跳过更新")
            return False
        
        # 计算更新起始日期（最后日期的下一天）
        last_datetime = datetime.strptime(last_date, '%Y-%m-%d')
        start_date = (last_datetime + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = get_latest_trading_date()
        
        # 如果起始日期大于等于结束日期，说明数据已是最新
        if start_date > end_date:
            return True  # 数据已是最新
        
        print(f"更新 {stock_code} {stock_name} 从 {start_date} 到 {end_date}")
        
        # 获取新数据
        rs = bs.query_history_k_data_plus(
            stock_code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=ADJUST_FLAG
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list:
            # 创建新数据DataFrame
            new_df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 追加到现有文件
            new_df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"{stock_code} 新增 {len(new_df)} 条记录")
            return True
        else:
            # 无新数据，但不算失败
            return True
            
    except Exception as e:
        print(f"更新 {stock_code} 失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 50)
    print("股票数据增量更新程序")
    print("=" * 50)
    print(f"数据目录: {STOCK_DATA_DIR}")
    print(f"目标日期: {get_latest_trading_date()}")
    print("-" * 50)
    
    if not os.path.exists(STOCK_DATA_DIR):
        print(f"数据目录不存在: {STOCK_DATA_DIR}")
        return
    
    # 登录
    lg = login_baostock()
    
    try:
        # 获取所有CSV文件
        csv_files = [f for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv')]
        print(f"发现 {len(csv_files)} 个数据文件")
        
        success_count = 0
        update_count = 0
        
        start_time = time.time()
        
        for i, filename in enumerate(csv_files):
            # 解析文件名获取股票代码和名称
            try:
                base_name = filename[:-4]  # 去掉.csv
                parts = base_name.split('_', 1)
                if len(parts) >= 2:
                    code_part = parts[0]
                    name_part = parts[1] if len(parts) > 1 else ""
                    
                    # 推断市场前缀
                    if code_part.startswith('0') or code_part.startswith('3'):
                        stock_code = f"sz.{code_part}"
                    elif code_part.startswith('6'):
                        stock_code = f"sh.{code_part}"
                    elif code_part.startswith('8') or code_part.startswith('4'):
                        stock_code = f"bj.{code_part}"
                    else:
                        # 可能是指数，尝试深圳
                        stock_code = f"sz.{code_part}"
                    
                    file_path = os.path.join(STOCK_DATA_DIR, filename)
                    
                    # 显示进度
                    progress = (i + 1) / len(csv_files) * 100
                    print(f"[{progress:.1f}%] 检查 {stock_code} {name_part}")
                    
                    # 获取更新前的最后日期
                    old_last_date = get_last_date_from_file(file_path)
                    
                    if update_stock_data(stock_code, name_part, file_path):
                        success_count += 1
                        
                        # 检查是否有实际更新
                        new_last_date = get_last_date_from_file(file_path)
                        if new_last_date and new_last_date != old_last_date:
                            update_count += 1
                    
                else:
                    print(f"跳过文件名格式异常的文件: {filename}")
                    
            except Exception as e:
                print(f"处理文件 {filename} 时出错: {e}")
                continue
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print("-" * 50)
        print("更新完成!")
        print(f"总文件数: {len(csv_files)}")
        print(f"成功检查: {success_count}")
        print(f"实际更新: {update_count}")
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print("=" * 50)
        
    finally:
        logout_baostock()

if __name__ == "__main__":
    main()
