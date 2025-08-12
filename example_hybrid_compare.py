#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混合对比工具使用示例
展示如何使用HybridComparator进行高效的CSV文件对比
"""

import os
import pandas as pd
from hybrid_compare import HybridComparator
import time

def create_test_files():
    """创建测试文件"""
    print("创建测试文件...")
    
    # 创建第一个测试文件
    data1 = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou', 'Chengdu', 'Xian', 'Nanjing', 'Wuhan', 'Chongqing'],
        'salary': [5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000],
        'department': ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'IT', 'HR', 'Finance', 'Marketing', 'Sales']
    }
    
    df1 = pd.DataFrame(data1)
    df1.to_csv('test_file1.csv', index=False, encoding='utf-8')
    print("已创建 test_file1.csv")
    
    # 创建第二个测试文件（有一些差异）
    data2 = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],  # 多了一行
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack', 'Kate'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75],
        'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou', 'Chengdu', 'Xian', 'Nanjing', 'Wuhan', 'Chongqing', 'Tianjin'],
        'salary': [5000, 6000, 7500, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000],  # 第3行和第11行有差异
        'department': ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'IT', 'HR', 'Finance', 'Marketing', 'Sales', 'IT']
    }
    
    df2 = pd.DataFrame(data2)
    df2.to_csv('test_file2.csv', index=False, encoding='utf-8')
    print("已创建 test_file2.csv")
    
    print("测试文件创建完成！")
    print()

def example_basic_usage():
    """基础用法示例"""
    print("=" * 60)
    print("基础用法示例")
    print("=" * 60)
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000, hash_algorithm='md5')
    
    # 执行混合对比，使用id作为关键列
    result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id'])
    
    # 生成报告
    report = comparator.generate_hybrid_report(result)
    print(report)
    
    print()

def example_multiple_key_columns():
    """多关键列示例"""
    print("=" * 60)
    print("多关键列示例")
    print("=" * 60)
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000, hash_algorithm='md5')
    
    # 使用多个关键列（id + name）
    result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id', 'name'])
    
    # 生成报告
    report = comparator.generate_hybrid_report(result)
    print(report)
    
    print()

def example_custom_parameters():
    """自定义参数示例"""
    print("=" * 60)
    print("自定义参数示例")
    print("=" * 60)
    
    # 创建对比器，使用较小的分块大小和SHA1算法
    comparator = HybridComparator(chunk_size=100, hash_algorithm='sha1')
    
    # 执行对比
    result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id'])
    
    print(f"使用分块大小: {comparator.chunk_size}")
    print(f"使用哈希算法: {comparator.hash_algorithm}")
    print(f"Hash对比耗时: {result['hash_processing_time']:.2f}秒")
    print(f"详细对比耗时: {result['detailed_processing_time']:.2f}秒")
    print(f"总耗时: {result['total_processing_time']:.2f}秒")
    print()

def example_save_report():
    """保存报告示例"""
    print("=" * 60)
    print("保存报告示例")
    print("=" * 60)
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000, hash_algorithm='md5')
    
    # 执行对比
    result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id'])
    
    # 保存报告
    report_file = 'hybrid_compare_report.txt'
    comparator.generate_hybrid_report(result, report_file)
    print(f"混合对比报告已保存到: {report_file}")
    
    print()

def example_analyze_differences():
    """分析差异示例"""
    print("=" * 60)
    print("分析差异示例")
    print("=" * 60)
    
    # 创建对比器
    comparator = HybridComparator(chunk_size=1000, hash_algorithm='md5')
    
    # 执行对比
    result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id'])
    
    if not result['is_identical']:
        print("发现差异，开始分析...")
        
        # 分析Hash对比结果
        hash_result = result['hash_result']
        print(f"\nHash对比结果:")
        print(f"  文件1行数: {hash_result['file1_lines']:,}")
        print(f"  文件2行数: {hash_result['file2_lines']:,}")
        print(f"  差异数量: {hash_result['total_differences']:,}")
        
        # 分析详细对比结果
        detailed_result = result['detailed_result']
        if detailed_result:
            print(f"\n详细对比结果:")
            print(f"  差异总数: {detailed_result['total_differences']:,}")
            print(f"  处理行数: {detailed_result['total_processed']:,}")
            
            # 差异类型统计
            if detailed_result['type_counts']:
                print(f"  差异类型统计:")
                for diff_type, count in detailed_result['type_counts'].items():
                    type_name = {
                        'data_mismatch': '数据不匹配',
                        'file1_only': '仅在文件1中',
                        'file2_only': '仅在文件2中'
                    }.get(diff_type, diff_type)
                    print(f"    {type_name}: {count}")
            
            # 显示前几个差异的详细信息
            if detailed_result['differences']:
                print(f"\n前3个差异详情:")
                for i, diff in enumerate(detailed_result['differences'][:3], 1):
                    print(f"\n{i}. 第 {diff['line_number']} 行")
                    print(f"   类型: {diff['type']}")
                    print(f"   关键列: {diff['key_columns']}")
                    
                    if diff['type'] == 'data_mismatch':
                        for col, (val1, val2) in diff['column_differences'].items():
                            print(f"     {col}: {val1} vs {val2}")
                    elif diff['type'] == 'file2_only':
                        for col, val in diff['file2_data'].items():
                            if col in result['key_columns']:
                                print(f"     {col}: {val} (关键列)")
                            else:
                                print(f"     {col}: {val}")
    
    print()

def example_performance_comparison():
    """性能对比示例"""
    print("=" * 60)
    print("性能对比示例")
    print("=" * 60)
    
    # 测试不同的哈希算法
    algorithms = ['md5', 'sha1', 'sha256']
    
    for algo in algorithms:
        print(f"\n使用 {algo.upper()} 算法:")
        
        # 创建对比器
        comparator = HybridComparator(chunk_size=1000, hash_algorithm=algo)
        
        # 执行对比
        start_time = time.time()
        result = comparator.compare_files_hybrid('test_file1.csv', 'test_file2.csv', ['id'])
        total_time = time.time() - start_time
        
        print(f"  总耗时: {total_time:.3f}秒")
        print(f"  Hash对比耗时: {result['hash_processing_time']:.3f}秒")
        print(f"  详细对比耗时: {result['detailed_processing_time']:.3f}秒")
        print(f"  差异数量: {result['detailed_result']['total_differences'] if result['detailed_result'] else 0}")
    
    print()

def cleanup_test_files():
    """清理测试文件"""
    print("清理测试文件...")
    
    test_files = ['test_file1.csv', 'test_file2.csv', 'hybrid_compare_report.txt']
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"已删除 {file}")
    
    print("清理完成！")

def main():
    """主函数"""
    print("CSV文件混合对比工具使用示例")
    print("=" * 60)
    
    try:
        # 创建测试文件
        create_test_files()
        
        # 运行各种示例
        example_basic_usage()
        example_multiple_key_columns()
        example_custom_parameters()
        example_save_report()
        example_analyze_differences()
        
        print("所有示例运行完成！")
        
    except Exception as e:
        print(f"运行示例时出错: {e}")
    
    finally:
        # 询问是否清理测试文件
        try:
            choice = input("\n是否清理测试文件？(y/n): ").lower().strip()
            if choice in ['y', 'yes', '是']:
                cleanup_test_files()
        except:
            pass

if __name__ == "__main__":
    main()
