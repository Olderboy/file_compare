#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试数据生成脚本
用于生成大容量的CSV测试文件，用于验证CSV对比工具的性能
"""

import pandas as pd
import numpy as np
import random
import string
import argparse
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_random_string(length: int = 10) -> str:
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_test_data(rows: int, columns: int, output_file: str, 
                      include_duplicates: bool = True, 
                      randomize_order: bool = True) -> None:
    """
    生成测试数据
    
    Args:
        rows: 行数
        columns: 列数
        output_file: 输出文件路径
        include_duplicates: 是否包含重复数据
        randomize_order: 是否随机化数据顺序
    """
    logger.info(f"开始生成 {rows:,} 行 {columns} 列的测试数据...")
    
    # 生成列名
    column_names = [f"col_{i}" for i in range(columns)]
    
    # 生成数据
    data = {}
    for col in column_names:
        if col == 'id':
            data[col] = list(range(1, rows + 1))
        elif col == 'name':
            data[col] = [f"user_{i:06d}" for i in range(1, rows + 1)]
        elif col == 'email':
            data[col] = [f"user_{i:06d}@example.com" for i in range(1, rows + 1)]
        elif col == 'age':
            data[col] = np.random.randint(18, 80, rows)
        elif col == 'salary':
            data[col] = np.random.uniform(30000, 150000, rows).round(2)
        elif col == 'department':
            depts = ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Engineering']
            data[col] = np.random.choice(depts, rows)
        elif col == 'status':
            statuses = ['Active', 'Inactive', 'Pending', 'Suspended']
            data[col] = np.random.choice(statuses, rows)
        else:
            # 生成随机字符串
            data[col] = [generate_random_string(random.randint(5, 15)) for _ in range(rows)]
    
    # 创建DataFrame
    df = pd.DataFrame(data)
    
    # 添加重复数据（如果需要）
    if include_duplicates and rows > 1000:
        duplicate_count = rows // 100  # 1%的重复数据
        logger.info(f"添加 {duplicate_count} 行重复数据...")
        
        # 随机选择行进行复制
        duplicate_indices = np.random.choice(rows, duplicate_count, replace=False)
        duplicate_rows = df.iloc[duplicate_indices].copy()
        
        # 稍微修改一些值以创建相似但不完全相同的数据
        for col in column_names[1:]:  # 跳过ID列
            if col in ['age', 'salary']:
                # 数值列添加小的随机变化
                duplicate_rows[col] = duplicate_rows[col] + np.random.normal(0, 1, duplicate_count)
            else:
                # 字符串列随机替换一些字符
                for idx in range(len(duplicate_rows)):
                    if random.random() < 0.3:  # 30%概率修改
                        original = str(duplicate_rows.iloc[idx][col])
                        if len(original) > 1:
                            pos = random.randint(0, len(original) - 1)
                            new_char = random.choice(string.ascii_letters)
                            duplicate_rows.iloc[idx, duplicate_rows.columns.get_loc(col)] = original[:pos] + new_char + original[pos+1:]
        
        # 合并数据
        df = pd.concat([df, duplicate_rows], ignore_index=True)
        logger.info(f"最终数据行数: {len(df):,}")
    
    # 随机化顺序（如果需要）
    if randomize_order:
        logger.info("随机化数据顺序...")
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # 保存到CSV文件
    logger.info(f"保存数据到: {output_file}")
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    # 显示文件信息
    file_size = Path(output_file).stat().st_size
    logger.info(f"文件大小: {file_size / (1024*1024):.2f} MB")
    logger.info(f"平均每行大小: {file_size / len(df):.2f} 字节")
    
    logger.info("测试数据生成完成!")

def generate_identical_files(rows: int, columns: int, output_dir: str = ".") -> None:
    """生成两个完全相同的测试文件"""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    file1 = output_dir / "test_data_identical_1.csv"
    file2 = output_dir / "test_data_identical_2.csv"
    
    # 生成第一个文件
    generate_test_data(rows, columns, str(file1), include_duplicates=True, randomize_order=True)
    
    # 复制第二个文件
    logger.info("生成完全相同的第二个文件...")
    df = pd.read_csv(file1)
    df.to_csv(file2, index=False, encoding='utf-8')
    
    logger.info(f"已生成两个完全相同的文件:")
    logger.info(f"  {file1}")
    logger.info(f"  {file2}")

def generate_different_files(rows: int, columns: int, output_dir: str = ".", 
                           difference_ratio: float = 0.01) -> None:
    """生成两个有差异的测试文件"""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    file1 = output_dir / "test_data_different_1.csv"
    file2 = output_dir / "test_data_different_2.csv"
    
    # 生成第一个文件
    generate_test_data(rows, columns, str(file1), include_duplicates=True, randomize_order=True)
    
    # 生成第二个文件（基于第一个文件，但有一些差异）
    logger.info(f"生成有 {difference_ratio*100:.1f}% 差异的第二个文件...")
    df = pd.read_csv(file1)
    
    # 计算需要修改的行数
    modify_count = int(len(df) * difference_ratio)
    modify_indices = np.random.choice(len(df), modify_count, replace=False)
    
    # 随机修改一些行
    for idx in modify_indices:
        col = np.random.choice(df.columns[1:])  # 跳过第一列
        if col in ['age', 'salary']:
            # 数值列
            df.iloc[idx, df.columns.get_loc(col)] = df.iloc[idx, df.columns.get_loc(col)] + np.random.normal(0, 10)
        else:
            # 字符串列
            original = str(df.iloc[idx][col])
            if len(original) > 1:
                pos = random.randint(0, len(original) - 1)
                new_char = random.choice(string.ascii_letters)
                df.iloc[idx, df.columns.get_loc(col)] = original[:pos] + new_char + original[pos+1:]
    
    # 保存第二个文件
    df.to_csv(file2, index=False, encoding='utf-8')
    
    logger.info(f"已生成两个有差异的文件:")
    logger.info(f"  {file1}")
    logger.info(f"  {file2}")
    logger.info(f"  差异比例: {difference_ratio*100:.1f}%")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='测试数据生成脚本')
    parser.add_argument('--rows', type=int, default=1000000, 
                       help='生成的行数 (默认: 1000000)')
    parser.add_argument('--columns', type=int, default=10, 
                       help='生成的列数 (默认: 10)')
    parser.add_argument('--output-dir', default='.', 
                       help='输出目录 (默认: 当前目录)')
    parser.add_argument('--identical', action='store_true', 
                       help='生成两个完全相同的文件')
    parser.add_argument('--different', action='store_true', 
                       help='生成两个有差异的文件')
    parser.add_argument('--difference-ratio', type=float, default=0.01, 
                       help='差异比例 (默认: 0.01, 即1%%)')
    
    args = parser.parse_args()
    
    try:
        if args.identical:
            generate_identical_files(args.rows, args.columns, args.output_dir)
        elif args.different:
            generate_different_files(args.rows, args.columns, args.output_dir, args.difference_ratio)
        else:
            # 默认生成单个文件
            output_file = Path(args.output_dir) / f"test_data_{args.rows:,}_rows.csv"
            generate_test_data(args.rows, args.columns, str(output_file))
            
    except Exception as e:
        logger.error(f"生成测试数据时出错: {e}")
        exit(1)

if __name__ == "__main__":
    main()
