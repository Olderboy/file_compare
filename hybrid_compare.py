#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混合对比工具
结合hash对比的快速性和全量对比的精确性
先使用hash对比快速定位差异行，再对差异行进行全量对比
"""

import pandas as pd
import numpy as np
import hashlib
import os
import time
from typing import Dict, List, Tuple, Optional, Set
import argparse
import logging
from pathlib import Path
import gc
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HybridComparator:
    """混合对比器：hash对比 + 差异行全量对比"""
    
    def __init__(self, chunk_size: int = 500000, hash_algorithm: str = 'md5', 
                 use_dask: bool = False, memory_limit: str = '2GB'):
        """
        初始化混合对比器
        
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
    
    def _get_common_columns(self, file1: str, file2: str) -> List[str]:
        """获取两个文件的公共列名"""
        try:
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
        """获取文件行数（不包括标题行）"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f) - 1
        except Exception as e:
            logger.error(f"计算文件行数时出错: {e}")
            raise
    
    def _process_csv_chunk_pandas(self, file_path: str, columns: List[str], 
                                 start_line: int, end_line: int) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """使用pandas处理CSV文件的一个分块"""
        hash_counts = {}
        hash_line_numbers = {}
        
        try:
            # 跳过标题行和前面的行
            skip_rows = list(range(1, start_line + 1)) if start_line > 0 else [1]
            nrows = end_line - start_line
            
            df_chunk = pd.read_csv(file_path, usecols=columns, skiprows=skip_rows, 
                                  nrows=nrows, dtype=str)
            
            # 重置索引并添加行号信息
            df_chunk = df_chunk.reset_index(drop=True)
            actual_rows = len(df_chunk)
            if actual_rows > 0:
                df_chunk['_file_line_number'] = range(start_line + 1, start_line + 1 + actual_rows)
            
            # 计算每行的哈希值
            for idx, row in df_chunk.iterrows():
                row_hash = self._hash_row(row, columns)
                hash_counts[row_hash] = hash_counts.get(row_hash, 0) + 1
                
                # 记录行号
                actual_line_number = row['_file_line_number']
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
    
    def _fast_hash_compare(self, file1: str, file2: str, common_columns: List[str]) -> Dict:
        """快速hash对比，定位差异行"""
        start_time = time.time()
        
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
            'differences': differences,
            'hash_algorithm': self.hash_algorithm,
            'chunk_size': self.chunk_size,
            'processing_method': 'dask' if self.use_dask else 'pandas'
        }
        
        logger.info(f"Hash对比完成，耗时: {processing_time:.2f}秒")
        logger.info(f"文件是否相同: {is_identical}")
        logger.info(f"差异数量: {len(differences)}")
        
        return result
    
    def _get_row_by_key_columns(self, file_path: str, key_columns: List[str], 
                                key_values: Dict[str, str]) -> Optional[Dict[str, str]]:
        """根据关键列值获取行数据"""
        try:
            # 读取文件，需要读取所有列来进行对比
            df = pd.read_csv(file_path, dtype=str)
            
            # 构建查询条件
            query_conditions = []
            for col, val in key_values.items():
                if col in df.columns:
                    query_conditions.append(f"`{col}` == '{val}'")
            
            if not query_conditions:
                return None
            
            # 执行查询
            query_str = ' and '.join(query_conditions)
            result = df.query(query_str)
            
            if len(result) > 0:
                return result.iloc[0].to_dict()
            return None
            
        except Exception as e:
            logger.error(f"根据关键列获取行数据时出错: {e}")
            return None
    
    def _detailed_compare_differences(self, file1: str, file2: str, 
                                     hash_result: Dict, key_columns: List[str]) -> Dict:
        """对差异行进行详细对比"""
        start_time = time.time()
        
        logger.info("开始对差异行进行详细对比...")
        
        detailed_differences = []
        total_processed = 0
        
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
        
        logger.info(f"需要处理的行数: 文件1={len(all_file1_lines)}, 文件2={len(all_file2_lines)}")
        
        # 处理文件1中的行
        for line_num in all_file1_lines:
            try:
                df1 = pd.read_csv(file1, skiprows=range(1, line_num), nrows=1, dtype=str)
                if len(df1) > 0:
                    row1_data = df1.iloc[0].to_dict()
                    key_values = {col: row1_data[col] for col in key_columns if col in row1_data}
                    
                    if key_values:
                        # 尝试在文件2中找到对应的行
                        row2_data = self._get_row_by_key_columns(file2, key_columns, key_values)
                        
                        if row2_data is None:
                            # 文件2中没有对应的行
                            detailed_differences.append({
                                'line_number': line_num,
                                'type': 'file1_only',
                                'file1_data': row1_data,
                                'file2_data': None,
                                'key_columns': key_values
                            })
                        else:
                            # 检查列级差异
                            column_diffs = {}
                            has_differences = False
                            
                            for col in hash_result['common_columns']:
                                if col in row1_data and col in row2_data:
                                    val1 = str(row1_data[col]) if pd.notna(row1_data[col]) else ''
                                    val2 = str(row2_data[col]) if pd.notna(row2_data[col]) else ''
                                    
                                    if val1 != val2:
                                        column_diffs[col] = (val1, val2)
                                        has_differences = True
                            
                            if has_differences:
                                detailed_differences.append({
                                    'line_number': line_num,
                                    'type': 'data_mismatch',
                                    'file1_data': row1_data,
                                    'file2_data': row2_data,
                                    'column_differences': column_diffs,
                                    'key_columns': key_values
                                })
                    
                    total_processed += 1
                    
            except Exception as e:
                logger.error(f"处理文件1第{line_num}行时出错: {e}")
                continue
        
        # 处理文件2中独有的行（不在文件1中对应行范围内的）
        for line_num in all_file2_lines:
            if line_num not in all_file1_lines:  # 避免重复处理
                try:
                    df2 = pd.read_csv(file2, skiprows=range(1, line_num), nrows=1, dtype=str)
                    if len(df2) > 0:
                        row2_data = df2.iloc[0].to_dict()
                        key_values = {col: row2_data[col] for col in key_columns if col in row2_data}
                        
                        if key_values:
                            # 尝试在文件1中找到对应的行
                            row1_data = self._get_row_by_key_columns(file1, key_columns, key_values)
                            
                            if row1_data is None:
                                detailed_differences.append({
                                    'line_number': line_num,
                                    'type': 'file2_only',
                                    'file1_data': None,
                                    'file2_data': row2_data,
                                    'key_columns': key_values
                                })
                        
                        total_processed += 1
                        
                except Exception as e:
                    logger.error(f"处理文件2第{line_num}行时出错: {e}")
                    continue
        
        processing_time = time.time() - start_time
        
        # 统计差异类型
        type_counts = defaultdict(int)
        for diff in detailed_differences:
            type_counts[diff['type']] += 1
        
        result = {
            'is_identical': len(detailed_differences) == 0,
            'processing_time': processing_time,
            'total_differences': len(detailed_differences),
            'differences': detailed_differences,
            'type_counts': dict(type_counts),
            'total_processed': total_processed,
            'key_columns': key_columns
        }
        
        logger.info(f"详细对比完成，耗时: {processing_time:.2f}秒")
        logger.info(f"差异数量: {len(detailed_differences)}")
        logger.info(f"差异类型统计: {dict(type_counts)}")
        
        return result
    
    def compare_files_hybrid(self, file1: str, file2: str, key_columns: List[str]) -> Dict:
        """
        混合对比两个CSV文件
        
        Args:
            file1: 第一个CSV文件路径
            file2: 第二个CSV文件路径
            key_columns: 用于唯一标识行的关键列
            
        Returns:
            混合对比结果字典
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
        
        # 验证关键列是否在公共列中
        missing_key_columns = [col for col in key_columns if col not in common_columns]
        if missing_key_columns:
            return {
                'is_identical': False,
                'reason': f'关键列 {missing_key_columns} 不在公共列中',
                'processing_time': 0,
                'details': {}
            }
        
        logger.info(f"使用关键列: {key_columns}")
        
        # 第一步：快速hash对比
        logger.info("=" * 60)
        logger.info("第一步：快速Hash对比")
        logger.info("=" * 60)
        
        hash_result = self._fast_hash_compare(file1, file2, common_columns)
        
        if hash_result['is_identical']:
            logger.info("文件完全相同，无需进行详细对比")
            return {
                'is_identical': True,
                'processing_time': time.time() - start_time,
                'hash_result': hash_result,
                'detailed_result': None
            }
        
        # 第二步：对差异行进行详细对比
        logger.info("=" * 60)
        logger.info("第二步：差异行详细对比")
        logger.info("=" * 60)
        
        detailed_result = self._detailed_compare_differences(file1, file2, hash_result, key_columns)
        print("=======================****************")
        print(detailed_result)
        print("=======================****************")
        total_time = time.time() - start_time
        
        result = {
            'is_identical': detailed_result['is_identical'],
            'total_processing_time': total_time,
            'hash_processing_time': hash_result['processing_time'],
            'detailed_processing_time': detailed_result['processing_time'],
            'hash_result': hash_result,
            'detailed_result': detailed_result,
            'key_columns': key_columns
        }
        
        logger.info(f"混合对比完成，总耗时: {total_time:.2f}秒")
        logger.info(f"Hash对比耗时: {hash_result['processing_time']:.2f}秒")
        logger.info(f"详细对比耗时: {detailed_result['processing_time']:.2f}秒")
        
        return result
    
    def generate_hybrid_report(self, result: Dict, output_file: str = None) -> str:
        """生成混合对比报告"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CSV文件混合对比报告 (Hash对比 + 差异行全量对比)")
        report_lines.append("=" * 80)
        report_lines.append(f"对比时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"总耗时: {result['total_processing_time']:.2f}秒")
        report_lines.append(f"Hash对比耗时: {result['hash_processing_time']:.2f}秒")
        report_lines.append(f"详细对比耗时: {result['detailed_processing_time']:.2f}秒")
        report_lines.append(f"关键列: {', '.join(result['key_columns'])}")
        report_lines.append("")
        
        # Hash对比结果摘要
        hash_result = result['hash_result']
        report_lines.append("Hash对比结果摘要:")
        report_lines.append("-" * 40)
        report_lines.append(f"文件1行数: {hash_result['file1_lines']:,}")
        report_lines.append(f"文件2行数: {hash_result['file2_lines']:,}")
        report_lines.append(f"公共列数: {len(hash_result['common_columns'])}")
        report_lines.append(f"Hash算法: {hash_result['hash_algorithm']}")
        report_lines.append(f"分块大小: {hash_result['chunk_size']:,}")
        report_lines.append(f"处理方式: {hash_result['processing_method']}")
        report_lines.append(f"差异数量: {hash_result['total_differences']:,}")
        report_lines.append("")
        
        # 详细对比结果
        detailed_result = result['detailed_result']
        if detailed_result:
            report_lines.append("详细对比结果:")
            report_lines.append("-" * 40)
            report_lines.append(f"文件是否相同: {'是' if detailed_result['is_identical'] else '否'}")
            report_lines.append(f"差异总数: {detailed_result['total_differences']:,}")
            report_lines.append(f"处理行数: {detailed_result['total_processed']:,}")
            report_lines.append("")
            
            # 差异类型统计
            if detailed_result['type_counts']:
                report_lines.append("差异类型统计:")
                for diff_type, count in detailed_result['type_counts'].items():
                    type_name = {
                        'data_mismatch': '数据不匹配',
                        'file1_only': '仅在文件1中',
                        'file2_only': '仅在文件2中'
                    }.get(diff_type, diff_type)
                    report_lines.append(f"  {type_name}: {count:,}")
                report_lines.append("")
            
            # 详细差异信息（限制显示前50个）
            if detailed_result['differences']:
                report_lines.append("详细差异信息 (前200个):")
                report_lines.append("=" * 80)
                
                for i, diff in enumerate(detailed_result['differences'][:200], 1):
                    report_lines.append(f"{i}. 第 {diff['line_number']} 行")
                    
                    if diff['type'] == 'data_mismatch':
                        report_lines.append(f"   类型: 数据不匹配")
                        report_lines.append(f"   关键列: {diff['key_columns']}")
                        report_lines.append(f"   列级差异:")
                        
                        for col, (val1, val2) in diff['column_differences'].items():
                            report_lines.append(f"     {col}:")
                            report_lines.append(f"       文件1: {val1}")
                            report_lines.append(f"       文件2: {val2}")
                        
                    elif diff['type'] == 'file1_only':
                        report_lines.append(f"   类型: 仅在文件1中存在")
                        report_lines.append(f"   关键列: {diff['key_columns']}")
                        report_lines.append(f"   文件1数据:")
                        for col, val in diff['file1_data'].items():
                            if col in result['key_columns']:
                                report_lines.append(f"     {col}: {val} (关键列)")
                            else:
                                report_lines.append(f"     {col}: {val}")
                                
                    elif diff['type'] == 'file2_only':
                        report_lines.append(f"   类型: 仅在文件2中存在")
                        report_lines.append(f"   关键列: {diff['key_columns']}")
                        report_lines.append(f"   文件2数据:")
                        for col, val in diff['file2_data'].items():
                            if col in result['key_columns']:
                                report_lines.append(f"     {col}: {val} (关键列)")
                            else:
                                report_lines.append(f"     {col}: {val}")
                    
                    report_lines.append("")
                
                if len(detailed_result['differences']) > 200:
                    report_lines.append(f"... 还有 {len(detailed_result['differences']) - 200} 个差异未显示")
                    report_lines.append("")
        
        report = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                logger.info(f"混合对比报告已保存到: {output_file}")
            except Exception as e:
                logger.error(f"保存报告时出错: {e}")
        
        return report


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='CSV文件混合对比工具 (Hash对比 + 差异行全量对比)')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--key-columns', nargs='+', required=True, 
                       help='用于唯一标识行的关键列名')
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
        comparator = HybridComparator(
            chunk_size=args.chunk_size,
            hash_algorithm=args.hash_algorithm,
            use_dask=args.use_dask,
            memory_limit=args.memory_limit
        )
        
        # 执行混合对比
        result = comparator.compare_files_hybrid(args.file1, args.file2, args.key_columns)
        
        # 生成报告
        report = comparator.generate_hybrid_report(result, args.output_report)
        
        # 输出结果
        print(report)
        
        # 返回退出码
        exit(0 if result['is_identical'] else 1)
        
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        exit(1)


if __name__ == "__main__":
    main()
