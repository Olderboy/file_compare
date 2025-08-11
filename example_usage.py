#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV对比工具使用示例
展示如何使用不同的CSV对比工具进行文件对比
"""

import os
import sys
from pathlib import Path

def example_basic_comparison():
    """基础版本对比示例"""
    print("=" * 60)
    print("基础版本CSV对比示例")
    print("=" * 60)
    
    # 检查是否有测试文件
    test_file1 = "test_data_identical_1.csv"
    test_file2 = "test_data_identical_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("测试文件不存在，请先生成测试数据:")
        print("python generate_test_data.py --rows 100000 --columns 5 --identical")
        return
    
    print(f"对比文件: {test_file1} vs {test_file2}")
    
    # 基础对比
    cmd = f"python csv_compare.py {test_file1} {test_file2} --chunk-size 50000"
    print(f"\n执行命令: {cmd}")
    
    # 这里可以添加实际的命令执行代码
    print("请手动执行上述命令来查看对比结果")

def example_fast_comparison():
    """高性能版本对比示例"""
    print("\n" + "=" * 60)
    print("高性能版本CSV对比示例")
    print("=" * 60)
    
    test_file1 = "test_data_identical_1.csv"
    test_file2 = "test_data_identical_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("测试文件不存在，请先生成测试数据")
        return
    
    print(f"对比文件: {test_file1} vs {test_file2}")
    
    # pandas版本对比
    cmd1 = f"python csv_compare_fast.py {test_file1} {test_file2} --chunk-size 100000"
    print(f"\n1. pandas版本对比:")
    print(f"   命令: {cmd1}")
    
    # dask版本对比
    cmd2 = f"python csv_compare_fast.py {test_file1} {test_file2} --use-dask --chunk-size 200000"
    print(f"\n2. dask版本对比:")
    print(f"   命令: {cmd2}")
    
    print("\n请手动执行上述命令来查看对比结果")

def example_different_files():
    """有差异文件的对比示例"""
    print("\n" + "=" * 60)
    print("有差异文件的对比示例")
    print("=" * 60)
    
    test_file1 = "test_data_different_1.csv"
    test_file2 = "test_data_different_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("差异测试文件不存在，请先生成:")
        print("python generate_test_data.py --rows 100000 --columns 5 --different --difference-ratio 0.01")
        return
    
    print(f"对比文件: {test_file1} vs {test_file2}")
    
    # 使用详细模式对比
    cmd = f"python csv_compare_fast.py {test_file1} {test_file2} --verbose --output-report diff_report.txt"
    print(f"\n执行命令: {cmd}")
    print("这将生成详细的差异报告")

def example_performance_test():
    """性能测试示例"""
    print("\n" + "=" * 60)
    print("性能测试示例")
    print("=" * 60)
    
    test_file1 = "test_data_identical_1.csv"
    test_file2 = "test_data_identical_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("测试文件不存在，请先生成测试数据")
        return
    
    print(f"测试文件: {test_file1} vs {test_file2}")
    
    # 基础性能测试
    cmd1 = f"python performance_test.py {test_file1} {test_file2} --output-dir ./performance_results"
    print(f"\n1. 基础性能测试:")
    print(f"   命令: {cmd1}")
    
    # 内存测试
    cmd2 = f"python performance_test.py {test_file1} {test_file2} --memory-test"
    print(f"\n2. 内存使用测试:")
    print(f"   命令: {cmd2}")
    
    print("\n性能测试将生成详细的性能报告")

def example_custom_parameters():
    """自定义参数示例"""
    print("\n" + "=" * 60)
    print("自定义参数示例")
    print("=" * 60)
    
    test_file1 = "test_data_identical_1.csv"
    test_file2 = "test_data_identical_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("测试文件不存在，请先生成测试数据")
        return
    
    print("不同配置的对比示例:")
    
    # 不同分块大小
    print(f"\n1. 小分块处理 (适合内存受限):")
    print(f"   python csv_compare.py {test_file1} {test_file2} --chunk-size 10000")
    
    print(f"\n2. 大分块处理 (适合内存充足):")
    print(f"   python csv_compare.py {test_file1} {test_file2} --chunk-size 500000")
    
    # 不同哈希算法
    print(f"\n3. 使用SHA256哈希 (更高安全性):")
    print(f"   python csv_compare_fast.py {test_file1} {test_file2} --hash-algorithm sha256")
    
    # 内存限制
    print(f"\n4. 限制内存使用:")
    print(f"   python csv_compare_fast.py {test_file1} {test_file2} --memory-limit 1GB")

def example_enhanced_reporting():
    """增强版报告示例"""
    print("\n" + "=" * 60)
    print("增强版报告示例")
    print("=" * 60)
    
    test_file1 = "test_data_different_1.csv"
    test_file2 = "test_data_different_2.csv"
    
    if not (os.path.exists(test_file1) and os.path.exists(test_file2)):
        print("差异测试文件不存在，请先生成:")
        print("python generate_test_data.py --rows 100000 --columns 5 --different --difference-ratio 0.01")
        return
    
    print(f"对比文件: {test_file1} vs {test_file2}")
    
    # 基础对比（包含行号信息）
    print(f"\n1. 基础对比（包含行号）:")
    print(f"   python csv_compare_fast.py {test_file1} {test_file2} --output-report diff_with_lines.txt")
    
    # 增强版摘要报告
    print(f"\n2. 增强版摘要报告:")
    print(f"   python enhanced_report.py {test_file1} {test_file2}")
    
    # 增强版详细报告
    print(f"\n3. 增强版详细报告（包含行内容）:")
    print(f"   python enhanced_report.py {test_file1} {test_file2} --detailed")
    
    print("\n这些报告将显示具体哪些行存在差异，以及差异的详细内容")

def main():
    """主函数"""
    print("CSV对比工具使用示例")
    print("本脚本展示了如何使用CSV对比工具进行各种场景的文件对比")
    
    # 检查必要文件是否存在
    required_files = ['csv_compare.py', 'csv_compare_fast.py']
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        print(f"\n错误: 缺少必要的文件: {', '.join(missing_files)}")
        print("请确保所有Python脚本文件都在当前目录中")
        return
    
    # 显示各种使用示例
    example_basic_comparison()
    example_fast_comparison()
    example_different_files()
    example_performance_test()
    example_custom_parameters()
    example_enhanced_reporting()
    
    print("\n" + "=" * 60)
    print("使用建议")
    print("=" * 60)
    print("1. 对于小文件 (< 100MB): 使用基础版本即可")
    print("2. 对于中等文件 (100MB - 1GB): 推荐使用pandas版本")
    print("3. 对于大文件 (> 1GB): 推荐使用dask版本")
    print("4. 根据系统内存调整分块大小")
    print("5. 使用性能测试工具找到最适合的配置")
    
    print("\n更多详细信息请参考 README.md 文件")

if __name__ == "__main__":
    main()
