#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试混合对比工具
"""

import pandas as pd
import hashlib
from hybrid_compare import HybridComparator

def debug_hash_comparison():
    """调试Hash对比"""
    print("=== 调试Hash对比 ===")
    
    # 读取两个文件
    df1 = pd.read_csv('test1.csv', dtype=str)
    df2 = pd.read_csv('test2.csv', dtype=str)
    
    print(f"文件1行数: {len(df1)}")
    print(f"文件2行数: {len(df2)}")
    
    # 计算每行的哈希值（使用所有列）
    for i in range(len(df1)):
        row1 = df1.iloc[i]
        row2 = df2.iloc[i]
        
        # 计算哈希值
        row1_str = '|'.join([f"{col}:{val}" for col, val in row1.items()])
        row2_str = '|'.join([f"{col}:{val}" for col, val in row2.items()])
        
        hash1 = hashlib.md5(row1_str.encode()).hexdigest()
        hash2 = hashlib.md5(row2_str.encode()).hexdigest()
        
        print(f"\n第{i+1}行:")
        print(f"  文件1 col_0: {row1['col_0']}, col_1: {row1['col_1']}")
        print(f"  文件2 col_0: {row2['col_0']}, col_1: {row2['col_1']}")
        print(f"  文件1哈希: {hash1}")
        print(f"  文件2哈希: {hash2}")
        print(f"  哈希相同: {hash1 == hash2}")
        
        # 检查哪些列不同
        differences = []
        for col in df1.columns:
            if row1[col] != row2[col]:
                differences.append(f"{col}: {row1[col]} vs {row2[col]}")
        
        if differences:
            print(f"  差异列: {differences}")
        else:
            print("  无差异")

def debug_hybrid_comparison():
    """调试混合对比"""
    print("\n=== 调试混合对比 ===")
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000)
    
    # 执行混合对比
    result = comparator.compare_files_hybrid('test1.csv', 'test2.csv', ['col_1'])
    
    print(f"对比结果: {result['is_identical']}")
    print(f"Hash对比差异数量: {result['hash_result']['total_differences']}")
    
    # 显示Hash对比的详细信息
    for diff in result['hash_result']['differences']:
        print(f"\n差异类型: {diff['type']}")
        if diff['type'] == 'count_mismatch':
            print(f"  文件1行号: {diff['file1_lines']}")
            print(f"  文件2行号: {diff['file2_lines']}")
    
    if result['detailed_result']:
        print(f"\n详细对比差异数量: {result['detailed_result']['total_differences']}")
        print(f"差异类型统计: {result['detailed_result']['type_counts']}")
        
        for i, diff in enumerate(result['detailed_result']['differences']):
            print(f"\n详细差异 {i+1}:")
            print(f"  行号: {diff['line_number']}")
            print(f"  类型: {diff['type']}")
            print(f"  关键列: {diff['key_columns']}")
            if diff['type'] == 'data_mismatch':
                print(f"  列级差异: {diff['column_differences']}")

if __name__ == "__main__":
    try:
        debug_hash_comparison()
        debug_hybrid_comparison()
    except Exception as e:
        print(f"调试出错: {e}")
        import traceback
        traceback.print_exc()

