#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
超高性能CSV文件对比工具 (pandas版本)
支持1000万行以上数据的快速对比，使用pandas和numpy优化性能
"""

import pandas as pd
import numpy as np
import hashlib
import os
import time
from typing import Dict, List, Tuple, Optional
import argparse
import logging
from pathlib import Path
import gc

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FastCSVComparator:
    """超高性能CSV文件对比器"""
    
    def __init__(self, chunk_size: int = 500000, hash_algorithm: str = 'md5', 
                 use_dask: bool = False, memory_limit: str = '2GB'):
        """
        初始化对比器
        
        Args:
            chunk_size: 分块大小，用于内存优化
            hash_algorithm: 哈希算法，支持md5, sha1, sha256
            use_dask: 是否使用dask进行分布式处理
            memory_limit: 内存限制
        """
        self.chunk_size = chunk_size
        self.hash_algorithm = hash_algorithm
        self.hash_func = getattr(hashlib, hash_algorithm)
        self.use_dask = use_dask
        self.memory_limit = memory_limit
        
        # 尝试导入dask
        if use_dask:
            try:
                import dask.dataframe as dd
                self.dd = dd
                logger.info("使用dask进行分布式处理")
            except ImportError:
                logger.warning("dask未安装，回退到pandas")
                self.use_dask = False
    
    def _get_hash(self, data: str) -> str:
        """计算字符串的哈希值"""
        return self.hash_func(data.encode('utf-8')).hexdigest()
    
    def _hash_row(self, row: pd.Series, columns: List[str]) -> str:
        """计算单行数据的哈希值"""
        row_data = []
        for col in columns:
            value = str(row.get(col, ''))
            row_data.append(f"{col}:{value}")
        
        # 排序确保一致性
        row_data.sort()
        return self._get_hash('|'.join(row_data))
    
    def _process_csv_chunk_pandas(self, file_path: str, columns: List[str], 
                                 start_line: int, end_line: int) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """使用pandas处理CSV文件的一个分块"""
        hash_counts = {}
        hash_line_numbers = {}
        
        try:
            # 读取指定行范围
            df_chunk = pd.read_csv(file_path, usecols=columns, skiprows=range(1, start_line + 1), 
                                  nrows=end_line - start_line, dtype=str)
            
            # 计算每行的哈希值
            for idx, row in df_chunk.iterrows():
                row_hash = self._hash_row(row, columns)
                hash_counts[row_hash] = hash_counts.get(row_hash, 0) + 1
                
                # 计算实际行号（包含标题行）
                actual_line_number = start_line + idx + 1
                if row_hash not in hash_line_numbers:
                    hash_line_numbers[row_hash] = []
                hash_line_numbers[row_hash].append(actual_line_number)
            
            # 清理内存
            del df_chunk
            gc.collect()
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} 分块时出错: {e}")
            raise
            
        return hash_counts, hash_line_numbers
    
    def _process_csv_chunk_dask(self, file_path: str, columns: List[str]) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """使用dask处理CSV文件"""
        try:
            # 使用dask读取文件
            ddf = self.dd.read_csv(file_path, usecols=columns, dtype=str)
            
            # 计算哈希值并统计
            def hash_partition(df):
                hash_counts = {}
                hash_line_numbers = {}
                for idx, row in df.iterrows():
                    row_hash = self._hash_row(row, columns)
                    hash_counts[row_hash] = hash_counts.get(row_hash, 0) + 1
                    
                    # 注意：dask中的行号可能不准确，这里只统计计数
                    if row_hash not in hash_line_numbers:
                        hash_line_numbers[row_hash] = []
                    hash_line_numbers[row_hash].append(1)  # 占位符
                
                return pd.Series([hash_counts, hash_line_numbers])
            
            # 应用哈希函数到每个分区
            hash_series = ddf.map_partitions(hash_partition).compute()
            
            # 合并结果
            final_hash_counts = {}
            final_hash_line_numbers = {}
            for series in hash_series:
                if isinstance(series, list) and len(series) == 2:
                    hash_counts, hash_line_numbers = series
                    if isinstance(hash_counts, dict):
                        for hash_val, count in hash_counts.items():
                            final_hash_counts[hash_val] = final_hash_counts.get(hash_val, 0) + count
                            if hash_val not in final_hash_line_numbers:
                                final_hash_line_numbers[hash_val] = []
                            final_hash_line_numbers[hash_val].extend([1] * count)  # 占位符
            
            return final_hash_counts, final_hash_line_numbers
            
        except Exception as e:
            logger.error(f"使用dask处理文件时出错: {e}")
            raise
    
    def _get_common_columns(self, file1: str, file2: str) -> List[str]:
        """获取两个文件的公共列名"""
        try:
            # 只读取列名，不读取数据
            df1_cols = pd.read_csv(file1, nrows=0).columns.tolist()
            df2_cols = pd.read_csv(file2, nrows=0).columns.tolist()
            
            common_columns = list(set(df1_cols).intersection(set(df2_cols)))
            logger.info(f"文件1列数: {len(df1_cols)}, 文件2列数: {len(df2_cols)}")
            logger.info(f"公共列数: {len(common_columns)}")
            
            return common_columns
                
        except Exception as e:
            logger.error(f"获取公共列时出错: {e}")
            raise
    
    def _get_file_line_count(self, file_path: str) -> int:
        """获取文件行数（快速估算）"""
        try:
            # 使用pandas快速计算行数
            df_info = pd.read_csv(file_path, nrows=1)
            # 获取文件大小并估算行数
            file_size = os.path.getsize(file_path)
            sample_size = len(df_info.to_string())
            estimated_lines = max(1, file_size // sample_size)
            
            # 验证估算结果
            if estimated_lines < 1000000:  # 如果估算结果较小，使用实际计算
                with open(file_path, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)
            
            return estimated_lines
            
        except Exception as e:
            logger.error(f"计算文件行数时出错: {e}")
            # 回退到传统方法
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)
            except:
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
        
        # 选择处理方式
        if self.use_dask and lines1 > 5000000:  # 500万行以上使用dask
            logger.info("使用dask处理大文件...")
            hash_counts1, hash_line_numbers1 = self._process_csv_chunk_dask(file1, common_columns)
            hash_counts2, hash_line_numbers2 = self._process_csv_chunk_dask(file2, common_columns)
        else:
            # 分块处理文件1
            logger.info("开始处理文件1...")
            hash_counts1 = {}
            hash_line_numbers1 = {}
            for start in range(0, lines1, self.chunk_size):
                end = min(start + self.chunk_size, lines1)
                chunk_hashes, chunk_line_numbers = self._process_csv_chunk_pandas(file1, common_columns, start, end)
                
                # 合并哈希计数和行号
                for hash_val, count in chunk_hashes.items():
                    hash_counts1[hash_val] = hash_counts1.get(hash_val, 0) + count
                for hash_val, line_nums in chunk_line_numbers.items():
                    if hash_val not in hash_line_numbers1:
                        hash_line_numbers1[hash_val] = []
                    hash_line_numbers1[hash_val].extend(line_nums)
                
                logger.info(f"文件1进度: {min(end, lines1):,}/{lines1:,}")
            
            # 分块处理文件2
            logger.info("开始处理文件2...")
            hash_counts2 = {}
            hash_line_numbers2 = {}
            for start in range(0, lines2, self.chunk_size):
                end = min(start + self.chunk_size, lines2)
                chunk_hashes, chunk_line_numbers = self._process_csv_chunk_pandas(file2, common_columns, start, end)
                
                # 合并哈希计数和行号
                for hash_val, count in chunk_hashes.items():
                    hash_counts2[hash_val] = hash_counts2.get(hash_val, 0) + count
                for hash_val, line_nums in chunk_line_numbers.items():
                    if hash_val not in hash_line_numbers2:
                        hash_line_numbers2[hash_val] = []
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
                    'file1_lines': hash_line_numbers1.get(hash_val, []),
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
                    'file2_lines': hash_line_numbers2.get(hash_val, [])
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
            'chunk_size': self.chunk_size,
            'processing_method': 'dask' if self.use_dask else 'pandas'
        }
        
        logger.info(f"对比完成，耗时: {processing_time:.2f}秒")
        logger.info(f"文件是否相同: {is_identical}")
        logger.info(f"差异数量: {len(differences)}")
        
        return result
    
    def generate_report(self, result: Dict, output_file: str = None) -> str:
        """生成对比报告"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("超高性能CSV文件对比报告")
        report_lines.append("=" * 60)
        report_lines.append(f"对比时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"处理耗时: {result['processing_time']:.2f}秒")
        report_lines.append(f"哈希算法: {result['hash_algorithm']}")
        report_lines.append(f"分块大小: {result['chunk_size']:,}")
        report_lines.append(f"处理方式: {result['processing_method']}")
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
                        line_nums_str = ', '.join(map(str, sorted(diff['file1_lines'])))
                        report_lines.append(f"     文件1行号: {line_nums_str}")
                    if 'file2_lines' in diff and diff['file2_lines']:
                        line_nums_str = ', '.join(map(str, sorted(diff['file2_lines'])))
                        report_lines.append(f"     文件2行号: {line_nums_str}")
                elif diff['type'] == 'file2_only':
                    report_lines.append(f"  {i}. 哈希值 {diff['hash'][:16]}...: 仅在文件2中出现({diff['file2_count']}次)")
                    if 'file2_lines' in diff and diff['file2_lines']:
                        line_nums_str = ', '.join(map(str, sorted(diff['file2_lines'])))
                        report_lines.append(f"     文件2行号: {line_nums_str}")
        
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
    parser = argparse.ArgumentParser(description='超高性能CSV文件对比工具 (pandas版本)')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--chunk-size', type=int, default=500000, 
                       help='分块大小 (默认: 500000)')
    parser.add_argument('--hash-algorithm', choices=['md5', 'sha1', 'sha256'], 
                       default='md5', help='哈希算法 (默认: md5)')
    parser.add_argument('--use-dask', action='store_true', 
                       help='使用dask进行分布式处理 (适用于超大文件)')
    parser.add_argument('--memory-limit', default='2GB', 
                       help='内存限制 (默认: 2GB)')
    parser.add_argument('--output-report', help='输出报告文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # 创建对比器
        comparator = FastCSVComparator(
            chunk_size=args.chunk_size,
            hash_algorithm=args.hash_algorithm,
            use_dask=args.use_dask,
            memory_limit=args.memory_limit
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
