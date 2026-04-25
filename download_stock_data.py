# -*- coding: utf-8 -*-
import baostock as bs
import pandas as pd
import os
import sys
import io
import time
from datetime import datetime

# 设置标准输出编码为UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 定义全局常量
START_DATE = "1999-01-01"
END_DATE = "2025-09-29"
STOCK_LIST_DATE = "2023-06-30"  # 用于获取股票列表的日期
OUTPUT_DIR = "stock_data"
ADJUST_FLAG = "2"  # 前复权


def login_baostock():
    """
    登录BaoStock系统
    
    Returns:
        bs.Baostock: 登录后的BaoStock对象
    """
    lg = bs.login()
    
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        sys.exit(1)
    else:
        print("登录成功")
        return lg


def logout_baostock(lg):
    """
    登出BaoStock系统
    
    Args:
        lg (bs.Baostock): BaoStock对象
    """
    bs.logout()
    print("登出成功")


def get_stock_list():
    """
    获取所有A股股票列表
    
    Returns:
        pandas.DataFrame: 包含股票代码和股票名称的DataFrame
    """
    print("正在获取股票列表...")
    
    # 获取证券基本资料
    rs = bs.query_all_stock(day=STOCK_LIST_DATE)
    
    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    
    # 转换为DataFrame
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    print(f"获取到 {len(result)} 条原始股票记录")
    
    # 过滤掉非A股股票（code以sh.6、sz.0、sz.3、bj.开头的为A股）
    # 先查看有哪些类型的股票代码
    print("股票代码前缀统计:")
    if not result.empty:
        print(result['code'].str[:3].value_counts())
    
    # 过滤A股股票，确保包含北京证券交易所股票
    result = result[
        (result['code'].str.startswith('sh.6')) |  # 上海证券交易所A股
        (result['code'].str.startswith('sz.0')) |  # 深圳证券交易所A股
        (result['code'].str.startswith('sz.3')) |  # 深圳证券交易所创业板
        (result['code'].str.startswith('bj.'))     # 北京证券交易所股票
    ]
    
    print(f"过滤后共获取到 {len(result)} 只A股股票")
    return result


def download_stock_data(lg, stock_code, stock_name):
    """
    下载单只股票的历史日线数据
    
    Args:
        lg (bs.Baostock): 登录后的BaoStock对象
        stock_code (str): 股票代码
        stock_name (str): 股票名称
        
    Returns:
        pandas.DataFrame: 股票历史数据
    """
    print(f"正在下载 {stock_code} {stock_name} 的历史数据...")
    
    # 获取历史K线数据
    rs = bs.query_history_k_data_plus(
        stock_code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=START_DATE,
        end_date=END_DATE,
        frequency="d",
        adjustflag=ADJUST_FLAG  # 前复权
    )
    
    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    
    # 转换为DataFrame
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    print(f"{stock_code} {stock_name} 数据下载完成，共 {len(result)} 条记录")
    return result


def save_to_csv(df, stock_code, stock_name):
    """
    将股票数据保存为CSV文件
    
    Args:
        df (pandas.DataFrame): 股票数据
        stock_code (str): 股票代码
        stock_name (str): 股票名称
    """
    # 清理股票代码，去掉市场前缀(sh./sz./bj.)
    clean_code = stock_code.split('.')[-1]
    
    # 创建文件名，清理股票名称中的特殊字符
    clean_name = stock_name.replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace(':', '_').replace('<', '_').replace('>', '_').replace('|', '_').replace('"', '_')
    filename = f"{clean_code}_{clean_name}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # 保存到CSV文件
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"已保存至 {filepath}")


def main():
    """
    主函数
    """
    print("=" * 50)
    print("A股历史日线数据下载程序")
    print("=" * 50)
    print(f"数据时间范围: {START_DATE} 至 {END_DATE}")
    print(f"复权类型: 前复权")
    print(f"输出目录: {OUTPUT_DIR}")
    print("-" * 50)
    
    # 记录开始时间
    start_time = time.time()
    start_datetime = datetime.now()
    print(f"开始时间: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 登录BaoStock
    lg = login_baostock()
    
    try:
        # 获取股票列表
        stock_list = get_stock_list()
        
        # 显示前几只股票作为示例
        print("前5只股票示例:")
        print(stock_list.head())
        
        # 创建输出目录
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            print(f"已创建输出目录: {OUTPUT_DIR}")
        
        # 统计变量
        success_count = 0
        fail_count = 0
        total_stocks = len(stock_list)
        
        print(f"\n开始下载 {total_stocks} 只股票的数据...")
        print("-" * 50)
        
        # 遍历股票列表下载数据
        for index, row in stock_list.iterrows():
            stock_code = row['code']
            stock_name = row['code_name']
            
            # 显示进度
            progress = (index + 1) / total_stocks * 100
            print(f"[{progress:.1f}%] 正在处理第 {index + 1}/{total_stocks} 只股票: {stock_code} {stock_name}")
            
            try:
                # 下载股票数据
                stock_data = download_stock_data(lg, stock_code, stock_name)
                
                # 检查是否有数据
                if not stock_data.empty:
                    # 保存到CSV文件
                    save_to_csv(stock_data, stock_code, stock_name)
                    success_count += 1
                else:
                    print(f"{stock_code} {stock_name} 无数据，跳过")
                    fail_count += 1
                    
            except Exception as e:
                print(f"下载 {stock_code} {stock_name} 数据时发生错误: {e}")
                fail_count += 1
                continue
        
        # 记录结束时间
        end_time = time.time()
        end_datetime = datetime.now()
        elapsed_time = end_time - start_time
        
        # 打印总结信息
        print("-" * 50)
        print("下载完成!")
        print(f"开始时间: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"结束时间: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print(f"成功下载: {success_count} 只股票")
        print(f"下载失败: {fail_count} 只股票")
        print(f"总计处理: {success_count + fail_count} 只股票")
        print("=" * 50)
        
    finally:
        # 登出BaoStock
        logout_baostock(lg)


if __name__ == "__main__":
    main()
