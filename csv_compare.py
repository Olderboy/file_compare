#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能CSV文件对比工具
支持1000万行以上数据的快速对比，使用哈希算法和分块处理优化性能
"""

import csv
import hashlib
import os
import time
from typing import Dict, Set, List, Tuple, Optional
from collections import defaultdict
import argparse
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVComparator:
    """CSV文件对比器"""
    
    def __init__(self, chunk_size: int = 100000, hash_algorithm: str = 'md5'):
        """
        初始化对比器
        
        Args:
            chunk_size: 分块大小，用于内存优化
            hash_algorithm: 哈希算法，支持md5, sha1, sha256
        """
        self.chunk_size = chunk_size
        self.hash_algorithm = hash_algorithm
        self.hash_func = getattr(hashlib, hash_algorithm)
        
    def _get_hash(self, data: str) -> str:
        """计算字符串的哈希值"""
        return self.hash_func(data.encode('utf-8')).hexdigest()
    
    def _process_csv_chunk(self, file_path: str, columns: List[str], start_line: int, end_line: int) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """
        处理CSV文件的一个分块
        
        Args:
            file_path: CSV文件路径
            columns: 要处理的列名列表
            start_line: 开始行号
            end_line: 结束行号
            
        Returns:
            (哈希值到行数的映射字典, 哈希值到行号列表的映射字典)
        """
        hash_counts = defaultdict(int)
        hash_line_numbers = defaultdict(list)
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as file:
                reader = csv.DictReader(file)
                
                # 跳过到指定行
                for _ in range(start_line):
                    next(reader, None)
                
                # 处理指定行数的数据
                for i, row in enumerate(reader):
                    if i >= (end_line - start_line):
                        break
                    
                    # 计算实际行号（包含标题行）
                    actual_line_number = start_line + i + 1
                    
                    # 构建行的哈希键
                    row_data = []
                    for col in columns:
                        value = row.get(col, '')
                        row_data.append(f"{col}:{value}")
                    
                    # 排序确保一致性
                    row_data.sort()
                    row_hash = self._get_hash('|'.join(row_data))
                    hash_counts[row_hash] += 1
                    hash_line_numbers[row_hash].append(actual_line_number)
                    
        except Exception as e:
            logger.error(f"处理文件 {file_path} 分块时出错: {e}")
            raise
            
        return hash_counts, hash_line_numbers
    
    def _get_common_columns(self, file1: str, file2: str) -> List[str]:
        """获取两个文件的公共列名"""
        try:
            with open(file1, 'r', encoding='utf-8', newline='') as f1, \
                 open(file2, 'r', encoding='utf-8', newline='') as f2:
                
                reader1 = csv.DictReader(f1)
                reader2 = csv.DictReader(f2)
                
                columns1 = set(reader1.fieldnames or [])
                columns2 = set(reader2.fieldnames or [])
                
                common_columns = list(columns1.intersection(columns2))
                logger.info(f"文件1列数: {len(columns1)}, 文件2列数: {len(columns2)}")
                logger.info(f"公共列数: {len(common_columns)}")
                
                return common_columns
                
        except Exception as e:
            logger.error(f"获取公共列时出错: {e}")
            raise
    
    def _get_file_line_count(self, file_path: str) -> int:
        """获取文件行数（快速估算）"""
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as file:
                return sum(1 for _ in file)
        except Exception as e:
            logger.error(f"计算文件行数时出错: {e}")
            raise
    
    def compare_files(self, file1: str, file2: str) -> Dict:
        """
        对比两个CSV文件
        
        Args:
            file1: 第一个CSV文件路径
            file2: 第二个CSV文件路径
            
        Returns:
            对比结果字典
        """
        start_time = time.time()
        
        # 检查文件存在性
        if not os.path.exists(file1):
            raise FileNotFoundError(f"文件1不存在: {file1}")
        if not os.path.exists(file2):
            raise FileNotFoundError(f"文件2不存在: {file2}")
        
        # 获取公共列
        common_columns = self._get_common_columns(file1, file2)
        if not common_columns:
            return {
                'is_identical': False,
                'reason': '没有找到公共列',
                'processing_time': 0,
                'details': {}
            }
        
        # 获取文件行数
        lines1 = self._get_file_line_count(file1)
        lines2 = self._get_file_line_count(file2)
        
        logger.info(f"文件1行数: {lines1:,}")
        logger.info(f"文件2行数: {lines2:,}")
        logger.info(f"使用列: {common_columns}")
        
        # 分块处理文件1
        logger.info("开始处理文件1...")
        hash_counts1 = defaultdict(int)
        hash_line_numbers1 = defaultdict(list)
        for start in range(0, lines1, self.chunk_size):
            end = min(start + self.chunk_size, lines1)
            chunk_hashes, chunk_line_numbers = self._process_csv_chunk(file1, common_columns, start, end)
            for hash_val, count in chunk_hashes.items():
                hash_counts1[hash_val] += count
            for hash_val, line_nums in chunk_line_numbers.items():
                hash_line_numbers1[hash_val].extend(line_nums)
            logger.info(f"文件1进度: {min(end, lines1):,}/{lines1:,}")
        
        # 分块处理文件2
        logger.info("开始处理文件2...")
        hash_counts2 = defaultdict(int)
        hash_line_numbers2 = defaultdict(list)
        for start in range(0, lines2, self.chunk_size):
            end = min(start + self.chunk_size, lines2)
            chunk_hashes, chunk_line_numbers = self._process_csv_chunk(file2, common_columns, start, end)
            for hash_val, count in chunk_hashes.items():
                hash_counts2[hash_val] += count
            for hash_val, line_nums in chunk_line_numbers.items():
                hash_line_numbers2[hash_val].extend(line_nums)
            logger.info(f"文件2进度: {min(end, lines2):,}/{lines2:,}")
        
        # 对比结果
        logger.info("开始对比结果...")
        is_identical = True
        differences = []
        
        # 检查文件1中独有的哈希值
        for hash_val, count1 in hash_counts1.items():
            count2 = hash_counts2.get(hash_val, 0)
            if count1 != count2:
                is_identical = False
                differences.append({
                    'hash': hash_val,
                    'file1_count': count1,
                    'file2_count': count2,
                    'type': 'count_mismatch',
                    'file1_lines': hash_line_numbers1[hash_val],
                    'file2_lines': hash_line_numbers2.get(hash_val, [])
                })
        
        # 检查文件2中独有的哈希值
        for hash_val, count2 in hash_counts2.items():
            if hash_val not in hash_counts1:
                is_identical = False
                differences.append({
                    'hash': hash_val,
                    'file1_count': 0,
                    'file2_count': count2,
                    'type': 'file2_only',
                    'file1_lines': [],
                    'file2_lines': hash_line_numbers2[hash_val]
                })
        
        processing_time = time.time() - start_time
        
        result = {
            'is_identical': is_identical,
            'processing_time': processing_time,
            'file1_lines': lines1,
            'file2_lines': lines2,
            'common_columns': common_columns,
            'total_differences': len(differences),
            'differences': differences[:100],  # 只返回前100个差异
            'hash_algorithm': self.hash_algorithm,
            'chunk_size': self.chunk_size
        }
        
        logger.info(f"对比完成，耗时: {processing_time:.2f}秒")
        logger.info(f"文件是否相同: {is_identical}")
        logger.info(f"差异数量: {len(differences)}")
        
        return result
    
    def generate_report(self, result: Dict, output_file: str = None) -> str:
        """生成对比报告"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("CSV文件对比报告")
        report_lines.append("=" * 60)
        report_lines.append(f"对比时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"处理耗时: {result['processing_time']:.2f}秒")
        report_lines.append(f"哈希算法: {result['hash_algorithm']}")
        report_lines.append(f"分块大小: {result['chunk_size']:,}")
        report_lines.append("")
        
        report_lines.append("文件信息:")
        report_lines.append(f"  文件1行数: {result['file1_lines']:,}")
        report_lines.append(f"  文件2行数: {result['file2_lines']:,}")
        report_lines.append(f"  公共列数: {len(result['common_columns'])}")
        report_lines.append(f"  公共列: {', '.join(result['common_columns'])}")
        report_lines.append("")
        
        report_lines.append("对比结果:")
        report_lines.append(f"  文件是否相同: {'是' if result['is_identical'] else '否'}")
        report_lines.append(f"  差异数量: {result['total_differences']}")
        report_lines.append("")
        
        if result['differences']:
            report_lines.append("差异详情 (前100个):")
            for i, diff in enumerate(result['differences'], 1):
                if diff['type'] == 'count_mismatch':
                    report_lines.append(f"  {i}. 哈希值 {diff['hash'][:16]}...: 文件1({diff['file1_count']}) vs 文件2({diff['file2_count']})")
                    if 'file1_lines' in diff and diff['file1_lines']:
                        report_lines.append(f"     文件1行号: {', '.join(map(str, sorted(diff['file1_lines'])))}")
                    if 'file2_lines' in diff and diff['file2_lines']:
                        report_lines.append(f"     文件2行号: {', '.join(map(str, sorted(diff['file2_lines'])))}")
                elif diff['type'] == 'file2_only':
                    report_lines.append(f"  {i}. 哈希值 {diff['hash'][:16]}...: 仅在文件2中出现({diff['file2_count']}次)")
                    if 'file2_lines' in diff and diff['file2_lines']:
                        report_lines.append(f"     文件2行号: {', '.join(map(str, sorted(diff['file2_lines'])))}")
        
        report = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                logger.info(f"报告已保存到: {output_file}")
            except Exception as e:
                logger.error(f"保存报告时出错: {e}")
        
        return report


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='高性能CSV文件对比工具')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--chunk-size', type=int, default=100000, 
                       help='分块大小 (默认: 100000)')
    parser.add_argument('--hash-algorithm', choices=['md5', 'sha1', 'sha256'], 
                       default='md5', help='哈希算法 (默认: md5)')
    parser.add_argument('--output-report', help='输出报告文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # 创建对比器
        comparator = CSVComparator(
            chunk_size=args.chunk_size,
            hash_algorithm=args.hash_algorithm
        )
        
        # 执行对比
        result = comparator.compare_files(args.file1, args.file2)
        
        # 生成报告
        report = comparator.generate_report(result, args.output_report)
        
        # 输出结果
        print(report)
        
        # 返回退出码
        exit(0 if result['is_identical'] else 1)
        
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        exit(1)


if __name__ == "__main__":
    main()
