#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能测试脚本
用于测试不同配置下的CSV对比性能
"""

import time
import subprocess
import sys
import os
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(cmd: list, timeout: int = 3600) -> dict:
    """运行命令并返回结果"""
    try:
        start_time = time.time()
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=timeout,
            encoding='utf-8'
        )
        end_time = time.time()
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
            'execution_time': end_time - start_time
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'stdout': '',
            'stderr': 'Command timed out',
            'returncode': -1,
            'execution_time': timeout
        }
    except Exception as e:
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1,
            'execution_time': 0
        }

def test_csv_compare_performance(file1: str, file2: str, output_dir: str = "."):
    """测试CSV对比工具的性能"""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    logger.info("开始性能测试...")
    logger.info(f"文件1: {file1}")
    logger.info(f"文件2: {file2}")
    
    # 测试配置
    test_configs = [
        {
            'name': '基础版本 (标准配置)',
            'cmd': ['python', 'csv_compare.py', file1, file2, '--chunk-size', '100000']
        },
        {
            'name': '基础版本 (大分块)',
            'cmd': ['python', 'csv_compare.py', file1, file2, '--chunk-size', '500000']
        },
        {
            'name': 'pandas版本 (标准配置)',
            'cmd': ['python', 'csv_compare_fast.py', file1, file2, '--chunk-size', '500000']
        },
        {
            'name': 'pandas版本 (大分块)',
            'cmd': ['python', 'csv_compare_fast.py', file1, file2, '--chunk-size', '1000000']
        },
        {
            'name': 'pandas版本 (dask模式)',
            'cmd': ['python', 'csv_compare_fast.py', file1, file2, '--use-dask', '--chunk-size', '1000000']
        }
    ]
    
    results = []
    
    for config in test_configs:
        logger.info(f"\n测试配置: {config['name']}")
        logger.info(f"命令: {' '.join(config['cmd'])}")
        
        # 运行测试
        result = run_command(config['cmd'])
        
        if result['success']:
            logger.info(f"执行成功，耗时: {result['execution_time']:.2f}秒")
            
            # 解析输出获取处理时间
            processing_time = None
            for line in result['stdout'].split('\n'):
                if '处理耗时:' in line:
                    try:
                        processing_time = float(line.split(':')[1].replace('秒', '').strip())
                        break
                    except:
                        pass
            
            results.append({
                'config': config['name'],
                'execution_time': result['execution_time'],
                'processing_time': processing_time,
                'success': True
            })
        else:
            logger.error(f"执行失败: {result['stderr']}")
            results.append({
                'config': config['name'],
                'execution_time': result['execution_time'],
                'processing_time': None,
                'success': False,
                'error': result['stderr']
            })
    
    # 生成性能报告
    generate_performance_report(results, output_dir)
    
    return results

def generate_performance_report(results: list, output_dir: Path):
    """生成性能测试报告"""
    report_file = output_dir / "performance_report.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("CSV对比工具性能测试报告\n")
        f.write("=" * 80 + "\n")
        f.write(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 成功的结果
        successful_results = [r for r in results if r['success']]
        if successful_results:
            f.write("成功完成的测试:\n")
            f.write("-" * 50 + "\n")
            
            # 按处理时间排序
            successful_results.sort(key=lambda x: x['processing_time'] or float('inf'))
            
            for i, result in enumerate(successful_results, 1):
                f.write(f"{i}. {result['config']}\n")
                f.write(f"   总执行时间: {result['execution_time']:.2f}秒\n")
                if result['processing_time']:
                    f.write(f"   数据处理时间: {result['processing_time']:.2f}秒\n")
                f.write("\n")
            
            # 性能排名
            f.write("性能排名 (按处理时间):\n")
            f.write("-" * 30 + "\n")
            for i, result in enumerate(successful_results, 1):
                if result['processing_time']:
                    f.write(f"{i}. {result['config']}: {result['processing_time']:.2f}秒\n")
        
        # 失败的结果
        failed_results = [r for r in results if not r['success']]
        if failed_results:
            f.write("\n失败的测试:\n")
            f.write("-" * 30 + "\n")
            for result in failed_results:
                f.write(f"- {result['config']}: {result['error']}\n")
    
    logger.info(f"性能报告已保存到: {report_file}")

def test_memory_usage(file1: str, file2: str):
    """测试内存使用情况"""
    logger.info("测试内存使用情况...")
    
    try:
        import psutil
        import os
        
        # 获取当前进程
        process = psutil.Process(os.getpid())
        
        # 记录初始内存
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"初始内存使用: {initial_memory:.2f} MB")
        
        # 运行对比
        cmd = ['python', 'csv_compare_fast.py', file1, file2, '--chunk-size', '500000']
        result = run_command(cmd)
        
        if result['success']:
            # 记录峰值内存
            peak_memory = process.memory_info().rss / 1024 / 1024  # MB
            logger.info(f"峰值内存使用: {peak_memory:.2f} MB")
            logger.info(f"内存增长: {peak_memory - initial_memory:.2f} MB")
        
        return result
        
    except ImportError:
        logger.warning("psutil未安装，无法测试内存使用情况")
        return run_command(['python', 'csv_compare_fast.py', file1, file2])

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CSV对比工具性能测试')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--output-dir', default='.', help='输出目录')
    parser.add_argument('--memory-test', action='store_true', help='测试内存使用情况')
    
    args = parser.parse_args()
    
    # 检查文件是否存在
    if not os.path.exists(args.file1):
        logger.error(f"文件1不存在: {args.file1}")
        sys.exit(1)
    
    if not os.path.exists(args.file2):
        logger.error(f"文件2不存在: {args.file2}")
        sys.exit(1)
    
    try:
        if args.memory_test:
            # 内存测试
            test_memory_usage(args.file1, args.file2)
        else:
            # 性能测试
            test_csv_compare_performance(args.file1, args.file2, args.output_dir)
            
    except KeyboardInterrupt:
        logger.info("测试被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"测试过程中出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
