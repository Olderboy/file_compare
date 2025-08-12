#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试子表提取和行匹配问题
"""

import pandas as pd
from hybrid_compare import HybridComparator

def debug_subset_extraction():
    """调试子表提取过程"""
    print("=== 调试子表提取过程 ===")
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000)
    
    # 执行Hash对比
    hash_result = comparator._fast_hash_compare('test1.csv', 'test2.csv', ['col_0', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 'col_7', 'col_8', 'col_9', 'col_10', 'col_11', 'col_12', 'col_13', 'col_14', 'col_15', 'col_16', 'col_17', 'col_18', 'col_19', 'col_20', 'col_21', 'col_22', 'col_23', 'col_24', 'col_25', 'col_26', 'col_27', 'col_28', 'col_29', 'col_30', 'col_31', 'col_32', 'col_33', 'col_34', 'col_35', 'col_36', 'col_37', 'col_38', 'col_39', 'col_40', 'col_41', 'col_42', 'col_43', 'col_44', 'col_45', 'col_46', 'col_47', 'col_48', 'col_49'])
    
    print(f"Hash对比结果:")
    print(f"  文件是否相同: {hash_result['is_identical']}")
    print(f"  差异数量: {hash_result['total_differences']}")
    
    # 显示每个差异的详细信息
    print(f"差异详情:")
    for i, diff in enumerate(hash_result['differences']):
        print(f"\n差异 {i+1}:")
        print(f"  类型: {diff['type']}")
        print(f"  哈希: {diff['hash'][:16]}...")
        print(f"  文件1计数: {diff['file1_count']}, 行号: {diff['file1_lines']}")
        print(f"  文件2计数: {diff['file2_count']}, 行号: {diff['file2_lines']}")
    
    # 显示完整的差异列表
    print(f"\n完整差异列表:")
    print(f"differences: {hash_result['differences']}")
    
    # 手动模拟子表提取过程
    print(f"\n=== 手动模拟子表提取过程 ===")
    
    # 收集所有需要处理的行号
    all_file1_lines = set()
    all_file2_lines = set()
    
    for diff in hash_result['differences']:
        if diff['type'] == 'count_mismatch':
            all_file1_lines.update(diff['file1_lines'])
            all_file2_lines.update(diff['file2_lines'])
        elif diff['type'] == 'file1_only':
            all_file1_lines.update(diff['file1_lines'])
        elif diff['type'] == 'file2_only':
            all_file2_lines.update(diff['file2_lines'])
    
    print(f"收集到的行号:")
    print(f"  文件1行号: {sorted(all_file1_lines)}")
    print(f"  文件2行号: {sorted(all_file2_lines)}")
    
    # 提取子表
    file1_subset = []
    file2_subset = []
    
    print(f"\n提取文件1子表:")
    for line_num in sorted(all_file1_lines):
        try:
            df1 = pd.read_csv('test1.csv', skiprows=range(1, line_num), nrows=1, dtype=str)
            if len(df1) > 0:
                row_data = df1.iloc[0].to_dict()
                row_data['_line_number'] = line_num
                file1_subset.append(row_data)
                print(f"  第{line_num}行: col_0={row_data['col_0']}, col_1={row_data['col_1']}")
        except Exception as e:
            print(f"  读取第{line_num}行出错: {e}")
    
    print(f"\n提取文件2子表:")
    for line_num in sorted(all_file2_lines):
        try:
            df2 = pd.read_csv('test2.csv', skiprows=range(1, line_num), nrows=1, dtype=str)
            if len(df2) > 0:
                row_data = df2.iloc[0].to_dict()
                row_data['_line_number'] = line_num
                file2_subset.append(row_data)
                print(f"  第{line_num}行: col_0={row_data['col_0']}, col_1={row_data['col_1']}")
        except Exception as e:
            print(f"  读取第{line_num}行出错: {e}")
    
    print(f"\n子表大小: 文件1={len(file1_subset)}, 文件2={len(file2_subset)}")
    
    # 基于关键列进行行匹配
    key_columns = ['col_1']
    print(f"\n=== 基于关键列进行行匹配 ===")
    
    # 创建关键列映射
    file1_key_map = {}
    file2_key_map = {}
    
    print(f"构建文件1子表的关键列映射:")
    for row_data in file1_subset:
        key_values = tuple(row_data[col] for col in key_columns if col in row_data)
        if key_values:
            if key_values not in file1_key_map:
                file1_key_map[key_values] = []
            file1_key_map[key_values].append(row_data)
            print(f"  关键列值 {key_values}: 行号 {row_data['_line_number']}")
    
    print(f"构建文件2子表的关键列映射:")
    for row_data in file2_subset:
        key_values = tuple(row_data[col] for col in key_columns if col in row_data)
        if key_values:
            if key_values not in file2_key_map:
                file2_key_map[key_values] = []
            file2_key_map[key_values].append(row_data)
            print(f"  关键列值 {key_values}: 行号 {row_data['_line_number']}")
    
    # 获取所有唯一的关键列组合
    all_keys = set(file1_key_map.keys()) | set(file2_key_map.keys())
    print(f"\n唯一关键列组合数: {len(all_keys)}")
    
    # 对每个关键列组合进行行匹配和比较
    detailed_differences = []
    for key_values in all_keys:
        file1_rows = file1_key_map.get(key_values, [])
        file2_rows = file2_key_map.get(key_values, [])
        
        print(f"\n关键列值 {key_values}:")
        print(f"  文件1匹配行数: {len(file1_rows)}")
        print(f"  文件2匹配行数: {len(file2_rows)}")
        
        if file1_rows and file2_rows:
            # 两个文件都有该行，检查是否有差异
            row1_data = file1_rows[0]
            row2_data = file2_rows[0]
            
            print(f"  两个文件都有该行:")
            print(f"    文件1行号: {row1_data['_line_number']}, col_0: {row1_data['col_0']}")
            print(f"    文件2行号: {row2_data['_line_number']}, col_0: {row2_data['col_0']}")
            
            # 检查列级差异
            column_diffs = {}
            has_differences = False
            
            for col in ['col_0', 'col_1', 'col_2']:  # 只检查前几列
                if col in row1_data and col in row2_data:
                    val1 = str(row1_data[col]) if pd.notna(row1_data[col]) else ''
                    val2 = str(row2_data[col]) if pd.notna(row2_data[col]) else ''
                    
                    if val1 != val2:
                        column_diffs[col] = (val1, val2)
                        has_differences = True
                        print(f"    列 {col} 有差异: {val1} vs {val2}")
            
            if has_differences:
                print(f"    识别为数据不匹配")
                detailed_differences.append({
                    'line_number': row1_data['_line_number'],
                    'type': 'data_mismatch',
                    'column_differences': column_diffs,
                    'key_columns': dict(zip(key_columns, key_values))
                })
            else:
                print(f"    无列级差异")
                
        elif file1_rows and not file2_rows:
            print(f"  仅在文件1中存在")
            detailed_differences.append({
                'line_number': file1_rows[0]['_line_number'],
                'type': 'file1_only'
            })
            
        elif not file1_rows and file2_rows:
            print(f"  仅在文件2中存在")
            detailed_differences.append({
                'line_number': file2_rows[0]['_line_number'],
                'type': 'file2_only'
            })
    
    print(f"\n=== 最终结果 ===")
    print(f"详细差异数量: {len(detailed_differences)}")
    for i, diff in enumerate(detailed_differences):
        print(f"差异 {i+1}: 类型={diff['type']}, 行号={diff.get('line_number', 'N/A')}")

if __name__ == "__main__":
    try:
        debug_subset_extraction()
    except Exception as e:
        print(f"调试出错: {e}")
        import traceback
        traceback.print_exc()
