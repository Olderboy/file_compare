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
                 use_dask: bool = False, memory_limit: str = '2GB', max_compare_rows: int = None):
        """
        初始化混合对比器
        
        Args:
            chunk_size: 分块大小，用于内存优化
            hash_algorithm: 哈希算法，支持md5, sha1, sha256
            use_dask: 是否使用dask进行分布式处理
            memory_limit: 内存限制
            max_compare_rows: 最大对比行数，None表示对比全部数据，指定数值表示只对比前N行
        """
        self.chunk_size = chunk_size
        self.hash_algorithm = hash_algorithm
        self.hash_func = getattr(hashlib, hash_algorithm)
        self.use_dask = use_dask
        self.memory_limit = memory_limit
        self.max_compare_rows = max_compare_rows
        
        # 尝试导入dask（无论use_dask设置如何，都尝试导入以便后续使用）
        try:
            import dask.dataframe as dd
            self.dd = dd
            logger.info("dask已导入，可用于大文件处理")
        except ImportError:
            logger.warning("dask未安装，大文件处理将回退到pandas")
            self.dd = None
    
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
    
    def _load_and_prepare_dataframes(self, file1: str, file2: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        一次性加载两个文件并返回DataFrame对象
        
        Args:
            file1: 第一个CSV文件路径
            file2: 第二个CSV文件路径
            
        Returns:
            包含两个DataFrame的元组
        """
        try:
            logger.info("开始加载文件...")
            start_time = time.time()
            
            # 加载文件1
            logger.info(f"加载文件1: {file1}")
            df1 = pd.read_csv(file1, dtype=str)
            df1['_file_line_number'] = range(1, len(df1) + 1)  # 添加行号信息
            
            # 加载文件2
            logger.info(f"加载文件2: {file2}")
            df2 = pd.read_csv(file2, dtype=str)
            df2['_file_line_number'] = range(1, len(df2) + 1)  # 添加行号信息
            
            loading_time = time.time() - start_time
            logger.info(f"文件加载完成，耗时: {loading_time:.2f}秒")
            logger.info(f"文件1行数: {len(df1):,}, 文件2行数: {len(df2):,}")
            
            return df1, df2
            
        except Exception as e:
            logger.error(f"加载文件时出错: {e}")
            raise
    
    def _sort_dataframes_by_key_columns(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                       key_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        根据关键列对两个DataFrame进行排序，确保数据顺序一致
        
        Args:
            df1: 第一个DataFrame
            df2: 第二个DataFrame
            key_columns: 关键列列表
            
        Returns:
            排序后的两个DataFrame
        """
        try:
            logger.info("开始根据关键列排序...")
            start_time = time.time()
            
            # 验证关键列是否存在
            missing_cols1 = [col for col in key_columns if col not in df1.columns]
            missing_cols2 = [col for col in key_columns if col not in df2.columns]
            
            if missing_cols1:
                raise ValueError(f"文件1中缺少关键列: {missing_cols1}")
            if missing_cols2:
                raise ValueError(f"文件2中缺少关键列: {missing_cols2}")
            
            # 创建排序键，处理空值
            def create_sort_key(row, cols):
                """创建排序键，空值用空字符串替代"""
                key_parts = []
                for col in cols:
                    val = row.get(col, '')
                    if pd.isna(val) or val == '':
                        key_parts.append('')
                    else:
                        key_parts.append(str(val))
                return tuple(key_parts)
            
            # 为DataFrame1创建排序键
            logger.info("为文件1创建排序键...")
            df1['_sort_key'] = df1.apply(lambda row: create_sort_key(row, key_columns), axis=1)
            
            # 为DataFrame2创建排序键
            logger.info("为文件2创建排序键...")
            df2['_sort_key'] = df2.apply(lambda row: create_sort_key(row, key_columns), axis=1)
            
            # 排序DataFrame1
            logger.info("排序文件1...")
            df1_sorted = df1.sort_values('_sort_key', kind='mergesort').reset_index(drop=True)
            df1_sorted['_sorted_index'] = range(len(df1_sorted))
            
            # 排序DataFrame2
            logger.info("排序文件2...")
            df2_sorted = df2.sort_values('_sort_key', kind='mergesort').reset_index(drop=True)
            df2_sorted['_sorted_index'] = range(len(df2_sorted))
            
            # 清理临时列
            df1_sorted = df1_sorted.drop('_sort_key', axis=1)
            df2_sorted = df2_sorted.drop('_sort_key', axis=1)
            
            sorting_time = time.time() - start_time
            logger.info(f"排序完成，耗时: {sorting_time:.2f}秒")
            logger.info(f"排序后文件1行数: {len(df1_sorted):,}, 文件2行数: {len(df2_sorted):,}")
            
            return df1_sorted, df2_sorted
            
        except Exception as e:
            logger.error(f"排序DataFrame时出错: {e}")
            raise
    
    def _get_common_columns(self, df1: pd.DataFrame, df2: pd.DataFrame) -> List[str]:
        """获取两个DataFrame的公共列名"""
        try:
            df1_cols = df1.columns.tolist()
            df2_cols = df2.columns.tolist()
            
            # 排除内部使用的列
            internal_cols = ['_file_line_number', '_sorted_index']
            df1_cols = [col for col in df1_cols if col not in internal_cols]
            df2_cols = [col for col in df2_cols if col not in internal_cols]
            
            common_columns = list(set(df1_cols).intersection(set(df2_cols)))
            logger.info(f"文件1列数: {len(df1_cols)}, 文件2列数: {len(df2_cols)}")
            logger.info(f"公共列数: {len(common_columns)}")
            
            return common_columns
                
        except Exception as e:
            logger.error(f"获取公共列时出错: {e}")
            raise
    
    def _get_dataframe_line_count(self, df: pd.DataFrame) -> int:
        """获取DataFrame的行数（不包括标题行）"""
        return len(df)
    
    def _process_dataframe_chunk_pandas(self, df: pd.DataFrame, columns: List[str], 
                                       start_line: int, end_line: int) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """使用pandas处理DataFrame的一个分块"""
        hash_counts = {}
        hash_line_numbers = {}
        
        try:
            # 选择指定范围的行
            df_chunk = df.iloc[start_line:end_line].copy()
            
            # 计算每行的哈希值
            for idx, row in df_chunk.iterrows():
                row_hash = self._hash_row(row, columns)
                hash_counts[row_hash] = hash_counts.get(row_hash, 0) + 1
                
                # 记录排序后的索引和原始行号
                sorted_index = row['_sorted_index']
                original_line_number = row['_file_line_number']
                hash_line_numbers[row_hash] = hash_line_numbers.get(row_hash, [])
                hash_line_numbers[row_hash].append({
                    'sorted_index': sorted_index,
                    'original_line': original_line_number
                })
            
            # 清理内存
            del df_chunk
            gc.collect()
            
        except Exception as e:
            logger.error(f"处理DataFrame分块时出错: {e}")
            raise
            
        return hash_counts, hash_line_numbers
    
    def _process_dataframe_chunk_dask(self, df: pd.DataFrame, columns: List[str]) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
        """使用dask处理DataFrame（转换为dask DataFrame）"""
        try:
            # 将pandas DataFrame转换为dask DataFrame
            ddf = self.dd.from_pandas(df[columns + ['_sorted_index', '_file_line_number']], npartitions=4)
            
            # 计算哈希值并统计
            def hash_partition(df_partition):
                hash_counts = {}
                hash_line_numbers = {}
                for idx, row in df_partition.iterrows():
                    row_hash = self._hash_row(row, columns)
                    hash_counts[row_hash] = hash_counts.get(row_hash, 0) + 1
                    
                    # 记录排序后的索引和原始行号
                    sorted_index = row['_sorted_index']
                    original_line_number = row['_file_line_number']
                    if row_hash not in hash_line_numbers:
                        hash_line_numbers[row_hash] = []
                    hash_line_numbers[row_hash].append({
                        'sorted_index': sorted_index,
                        'original_line': original_line_number
                    })
                
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
                            final_hash_line_numbers[hash_val].extend(hash_line_numbers.get(hash_val, []))
            
            return final_hash_counts, final_hash_line_numbers
            
        except Exception as e:
            logger.error(f"使用dask处理DataFrame时出错: {e}")
            raise
    
    def _fast_hash_compare(self, df1: pd.DataFrame, df2: pd.DataFrame, common_columns: List[str]) -> Dict:
        """快速hash对比，定位差异行（基于排序后的数据，使用Dask优化）"""
        start_time = time.time()
        
        # 获取DataFrame行数
        lines1 = self._get_dataframe_line_count(df1)
        lines2 = self._get_dataframe_line_count(df2)
        
        logger.info(f"文件1行数: {lines1:,}")
        logger.info(f"文件2行数: {lines2:,}")
        logger.info(f"使用列: {common_columns}")
        
        # 选择处理方式：大文件使用Dask，小文件使用pandas
        if (lines1 > 1500000 or lines2 > 1500000) and self.dd is not None:  # 150万行以上且Dask可用时使用Dask
            logger.info("使用Dask进行并行hash对比...")
            return self._fast_hash_compare_dask(df1, df2, common_columns, start_time)
        else:
            if lines1 > 1500000 or lines2 > 1500000:
                logger.info("文件较大但Dask不可用，使用pandas进行hash对比...")
            else:
                logger.info("使用pandas进行hash对比...")
            return self._fast_hash_compare_pandas(df1, df2, common_columns, start_time)
    
    def _fast_hash_compare_dask(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                common_columns: List[str], start_time: float) -> Dict:
        """使用Dask进行快速hash对比"""
        try:
            # 获取DataFrame行数
            lines1 = len(df1)
            lines2 = len(df2)
            
            # 将pandas DataFrame转换为dask DataFrame
            logger.info("转换DataFrame为Dask DataFrame...")
            ddf1 = self.dd.from_pandas(df1[common_columns + ['_sorted_index', '_file_line_number']], npartitions=max(4, lines1//50000))
            ddf2 = self.dd.from_pandas(df2[common_columns + ['_sorted_index', '_file_line_number']], npartitions=max(4, lines2//50000))
            
            # 定义hash计算函数
            def compute_hash_partition(df_partition):
                """计算分区的hash值"""
                hash_data = []
                for idx, row in df_partition.iterrows():
                    row_hash = self._hash_row(row, common_columns)
                    hash_data.append({
                        'hash': row_hash,
                        'sorted_index': row['_sorted_index'],
                        'file_line_number': row['_file_line_number']
                    })
                return pd.DataFrame(hash_data)
            
            # 并行计算两个文件的hash值
            logger.info("并行计算文件1的hash值...")
            hash_df1 = ddf1.map_partitions(compute_hash_partition).compute()
            logger.info("并行计算文件2的hash值...")
            hash_df2 = ddf2.map_partitions(compute_hash_partition).compute()
            
            # 重置索引以便后续处理
            hash_df1 = hash_df1.reset_index(drop=True)
            hash_df2 = hash_df2.reset_index(drop=True)
            
            logger.info(f"文件1 hash计算完成，共{len(hash_df1)}行")
            logger.info(f"文件2 hash计算完成，共{len(hash_df2)}行")
            
            # 基于排序后的数据进行逐行对比
            differences = []
            is_identical = True
            
            # 确定要对比的行数（考虑max_compare_rows限制）
            max_compare_lines = min(len(hash_df1), len(hash_df2))
            if self.max_compare_rows is not None:
                max_compare_lines = min(max_compare_lines, self.max_compare_rows)
                logger.info(f"限制对比行数: {max_compare_lines:,} (设置的最大对比行数: {self.max_compare_rows:,})")
            
            logger.info(f"开始对比{max_compare_lines:,}行数据...")
            
            # 批量对比（每批10000行以提高效率）
            batch_size = 10000
            for batch_start in range(0, max_compare_lines, batch_size):
                batch_end = min(batch_start + batch_size, max_compare_lines)
                
                # 获取当前批次的数据
                batch_df1 = hash_df1.iloc[batch_start:batch_end]
                batch_df2 = hash_df2.iloc[batch_start:batch_end]
                
                # 对比当前批次
                for i in range(len(batch_df1)):
                    row1 = batch_df1.iloc[i]
                    row2 = batch_df2.iloc[i]
                    
                    if row1['hash'] != row2['hash']:
                        is_identical = False
                        differences.append({
                            'hash': f"{row1['hash']} vs {row2['hash']}",
                            'file1_count': 1,
                            'file2_count': 1,
                            'type': 'row_mismatch',
                            'line_index': batch_start + i,
                            'file1_line': row1['file_line_number'],
                            'file2_line': row2['file_line_number'],
                            'file1_hash': row1['hash'],
                            'file2_hash': row2['hash']
                        })
                
                if batch_start % 100000 == 0:
                    logger.info(f"已处理 {batch_start:,}/{max_compare_lines:,} 行")
            
            # 处理文件长度不同的情况
            if len(hash_df1) > len(hash_df2):
                # 文件1有多余的行
                for i in range(len(hash_df2), len(hash_df1)):
                    row1 = hash_df1.iloc[i]
                    differences.append({
                        'hash': row1['hash'],
                        'file1_count': 1,
                        'file2_count': 0,
                        'type': 'file1_only',
                        'line_index': i,
                        'file1_line': row1['file_line_number'],
                        'file2_line': None,
                        'file1_hash': row1['hash'],
                        'file2_hash': None
                    })
                    is_identical = False
            elif len(hash_df2) > len(hash_df1):
                # 文件2有多余的行
                for i in range(len(hash_df1), len(hash_df2)):
                    row2 = hash_df2.iloc[i]
                    differences.append({
                        'hash': row2['hash'],
                        'file1_count': 0,
                        'file2_count': 1,
                        'type': 'file2_only',
                        'line_index': i,
                        'file1_line': None,
                        'file2_line': row2['file_line_number'],
                        'file1_hash': None,
                        'file2_hash': row2['hash']
                    })
                    is_identical = False
            
            processing_time = time.time() - start_time
            
            result = {
                'is_identical': is_identical,
                'processing_time': processing_time,
                'file1_lines': len(hash_df1),
                'file2_lines': len(hash_df2),
                'total_compare_lines': max_compare_lines,
                'common_columns': common_columns,
                'total_differences': len(differences),
                'differences': differences,
                'hash_algorithm': self.hash_algorithm,
                'processing_method': 'dask_parallel'
            }
            
            logger.info(f"Dask并行Hash对比完成，耗时: {processing_time:.2f}秒")
            logger.info(f"文件是否相同: {is_identical}")
            logger.info(f"对比行数: {max_compare_lines:,}")
            logger.info(f"差异数量: {len(differences)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Dask处理出错，回退到pandas: {e}")
            return self._fast_hash_compare_pandas(df1, df2, common_columns, start_time)
    
    def _fast_hash_compare_pandas(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                  common_columns: List[str], start_time: float) -> Dict:
        """使用pandas进行快速hash对比（原有逻辑）"""
        # 由于数据已排序，我们可以直接逐行对比
        logger.info("基于排序数据进行逐行hash对比...")
        
        differences = []
        is_identical = True
        
        # 确定要对比的行数（取两个文件的较小值，并考虑max_compare_rows限制）
        max_compare_lines = min(len(df1), len(df2))
        if self.max_compare_rows is not None:
            max_compare_lines = min(max_compare_lines, self.max_compare_rows)
            logger.info(f"限制对比行数: {max_compare_lines:,} (设置的最大对比行数: {self.max_compare_rows:,})")
        
        # 逐行对比
        for i in range(max_compare_lines):
            # 获取两个文件对应行的数据
            row1 = df1.iloc[i]
            row2 = df2.iloc[i]
            
            # 计算哈希值
            hash1 = self._hash_row(row1, common_columns)
            hash2 = self._hash_row(row2, common_columns)
            
            # 如果哈希值不同，记录差异
            if hash1 != hash2:
                is_identical = False
                differences.append({
                    'hash': f"{hash1} vs {hash2}",
                    'file1_count': 1,
                    'file2_count': 1,
                    'type': 'row_mismatch',
                    'line_index': i,
                    'file1_line': row1['_file_line_number'],
                    'file2_line': row2['_file_line_number'],
                    'file1_hash': hash1,
                    'file2_hash': hash2
                })
        
        # 处理文件长度不同的情况
        if len(df1) > len(df2):
            # 文件1有多余的行
            for i in range(len(df2), len(df1)):
                row1 = df1.iloc[i]
                hash1 = self._hash_row(row1, common_columns)
                differences.append({
                    'hash': hash1,
                    'file1_count': 1,
                    'file2_count': 0,
                    'type': 'file1_only',
                    'line_index': i,
                    'file1_line': row1['_file_line_number'],
                    'file2_line': None,
                    'file1_hash': hash1,
                    'file2_hash': None
                })
                is_identical = False
        elif len(df2) > len(df1):
            # 文件2有多余的行
            for i in range(len(df1), len(df2)):
                row2 = df2.iloc[i]
                hash2 = self._hash_row(row2, common_columns)
                differences.append({
                    'hash': hash2,
                    'file1_count': 0,
                    'file2_count': 1,
                    'type': 'file2_only',
                    'line_index': i,
                    'file1_line': None,
                    'file2_line': row2['_file_line_number'],
                    'file1_hash': None,
                    'file2_hash': hash2
                })
                is_identical = False
        
        processing_time = time.time() - start_time
        
        result = {
            'is_identical': is_identical,
            'processing_time': processing_time,
            'file1_lines': len(df1),
            'file2_lines': len(df2),
            'total_compare_lines': max_compare_lines,
            'common_columns': common_columns,
            'total_differences': len(differences),
            'differences': differences,
            'hash_algorithm': self.hash_algorithm,
            'processing_method': 'pandas_row_by_row'
        }
        
        logger.info(f"Pandas逐行Hash对比完成，耗时: {processing_time:.2f}秒")
        logger.info(f"文件是否相同: {is_identical}")
        logger.info(f"对比行数: {max_compare_lines:,}")
        logger.info(f"差异数量: {len(differences)}")
        
        return result
    
    def _get_row_by_key_columns(self, df: pd.DataFrame, key_columns: List[str], 
                                key_values: Dict[str, str]) -> Optional[Dict[str, str]]:
        """根据关键列值从DataFrame中获取行数据"""
        try:
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
    
    def _detailed_compare_differences(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                     hash_result: Dict, key_columns: List[str]) -> Dict:
        """对差异行进行详细对比（基于排序后的数据）"""
        start_time = time.time()
        
        logger.info("开始对差异行进行详细对比...")
        
        detailed_differences = []
        total_processed = 0
        
        # 直接处理hash对比结果中的差异
        logger.info(f"Hash对比识别出的差异类型:")
        for i, diff in enumerate(hash_result['differences']):
            logger.info(f"  差异{i+1}: 类型={diff['type']}, 行索引={diff.get('line_index', 'N/A')}")
        
        # 根据差异类型直接提取对应的行进行详细对比
        for diff in hash_result['differences']:
            if diff['type'] == 'row_mismatch':
                # 行不匹配，需要详细对比
                line_index = diff['line_index']
                row1_data = df1.iloc[line_index].to_dict()
                row2_data = df2.iloc[line_index].to_dict()
                
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
                        'line_number': row1_data['_file_line_number'],
                        'sorted_index': line_index,
                        'type': 'data_mismatch',
                        'file1_data': {k: v for k, v in row1_data.items() if k not in ['_file_line_number', '_sorted_index']},
                        'file2_data': {k: v for k, v in row2_data.items() if k not in ['_file_line_number', '_sorted_index']},
                        'column_differences': column_diffs,
                        'key_columns': {col: row1_data[col] for col in key_columns if col in row1_data}
                    })
                    total_processed += 1
                
            elif diff['type'] == 'file1_only':
                # 仅在文件1中存在
                line_index = diff['line_index']
                row1_data = df1.iloc[line_index].to_dict()
                
                detailed_differences.append({
                    'line_number': row1_data['_file_line_number'],
                    'sorted_index': line_index,
                    'type': 'file1_only',
                    'file1_data': {k: v for k, v in row1_data.items() if k not in ['_file_line_number', '_sorted_index']},
                    'file2_data': None,
                    'key_columns': {col: row1_data[col] for col in key_columns if col in row1_data}
                })
                total_processed += 1
                
            elif diff['type'] == 'file2_only':
                # 仅在文件2中存在
                line_index = diff['line_index']
                row2_data = df2.iloc[line_index].to_dict()
                
                detailed_differences.append({
                    'line_number': row2_data['_file_line_number'],
                    'sorted_index': line_index,
                    'type': 'file2_only',
                    'file1_data': None,
                    'file2_data': {k: v for k, v in row2_data.items() if k not in ['_file_line_number', '_sorted_index']},
                    'key_columns': {col: row2_data[col] for col in key_columns if col in row2_data}
                })
                total_processed += 1
        
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
        
        # 一次性加载两个文件
        df1, df2 = self._load_and_prepare_dataframes(file1, file2)
        
        try:
            # 根据关键列对两个DataFrame进行排序
            df1_sorted, df2_sorted = self._sort_dataframes_by_key_columns(df1, df2, key_columns)
            
            # 获取公共列
            common_columns = self._get_common_columns(df1_sorted, df2_sorted)
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
            
            hash_result = self._fast_hash_compare(df1_sorted, df2_sorted, common_columns)
            
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
            
            detailed_result = self._detailed_compare_differences(df1_sorted, df2_sorted, hash_result, key_columns)
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
            
        finally:
            # 清理内存
            del df1, df2
            if 'df1_sorted' in locals():
                del df1_sorted
            if 'df2_sorted' in locals():
                del df2_sorted
            gc.collect()
            logger.info("已清理DataFrame内存")
    
    def generate_hybrid_report(self, result: Dict, output_file: str = None) -> str:
        """生成混合对比报告"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CSV文件混合对比报告 (Hash对比 + 差异行全量对比)")
        report_lines.append("=" * 80)
        report_lines.append(f"对比时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 安全获取基本字段，提供默认值
        total_time = result.get('total_processing_time', 0)
        hash_time = result.get('hash_processing_time', 0)
        detailed_time = result.get('detailed_processing_time', 0)
        key_columns = result.get('key_columns', [])
        
        report_lines.append(f"总耗时: {total_time:.2f}秒")
        report_lines.append(f"Hash对比耗时: {hash_time:.2f}秒")
        report_lines.append(f"详细对比耗时: {detailed_time:.2f}秒")
        report_lines.append(f"关键列: {', '.join(key_columns) if key_columns else '未指定'}")
        if self.max_compare_rows is not None:
            report_lines.append(f"对比限制: 仅对比前 {self.max_compare_rows:,} 行数据")
        report_lines.append("")
        
        # Hash对比结果摘要 - 添加健壮性检查
        hash_result = result.get('hash_result', {})
        if hash_result:
            report_lines.append("Hash对比结果摘要:")
            report_lines.append("-" * 40)
            
            # 安全获取hash_result中的字段
            file1_lines = hash_result.get('file1_lines', 0)
            file2_lines = hash_result.get('file2_lines', 0)
            common_columns = hash_result.get('common_columns', [])
            hash_algorithm = hash_result.get('hash_algorithm', '未知')
            processing_method = hash_result.get('processing_method', '未知')
            total_differences = hash_result.get('total_differences', 0)
            
            report_lines.append(f"文件1行数: {file1_lines:,}")
            report_lines.append(f"文件2行数: {file2_lines:,}")
            report_lines.append(f"公共列数: {len(common_columns)}")
            report_lines.append(f"Hash算法: {hash_algorithm}")
            report_lines.append(f"处理方式: {processing_method}")
            report_lines.append(f"差异数量: {total_differences:,}")
            report_lines.append("")
        else:
            report_lines.append("Hash对比结果摘要: 数据不可用")
            report_lines.append("-" * 40)
            report_lines.append("")
        
        # 详细对比结果 - 添加健壮性检查
        detailed_result = result.get('detailed_result', {})
        if detailed_result:
            report_lines.append("详细对比结果:")
            report_lines.append("-" * 40)
            
            # 安全获取detailed_result中的字段
            is_identical = detailed_result.get('is_identical', False)
            total_differences = detailed_result.get('total_differences', 0)
            total_processed = detailed_result.get('total_processed', 0)
            
            report_lines.append(f"文件是否相同: {'是' if is_identical else '否'}")
            report_lines.append(f"差异总数: {total_differences:,}")
            report_lines.append(f"处理行数: {total_processed:,}")
            report_lines.append("")
            
            # 差异类型统计 - 添加健壮性检查
            type_counts = detailed_result.get('type_counts', {})
            if type_counts:
                report_lines.append("差异类型统计:")
                for diff_type, count in type_counts.items():
                    type_name = {
                        'data_mismatch': '数据不匹配',
                        'file1_only': '仅在文件1中',
                        'file2_only': '仅在文件2中'
                    }.get(diff_type, diff_type)
                    report_lines.append(f"  {type_name}: {count:,}")
                report_lines.append("")
            
            # 详细差异信息 - 添加健壮性检查
            differences = detailed_result.get('differences', [])
            if differences:
                report_lines.append("详细差异信息 (前200个):")
                report_lines.append("=" * 80)
                
                for i, diff in enumerate(differences[:200], 1):
                    # 安全获取diff中的字段
                    line_number = diff.get('line_number', '未知')
                    diff_type = diff.get('type', '未知')
                    key_columns_diff = diff.get('key_columns', {})
                    
                    report_lines.append(f"{i}. 第 {line_number} 行")
                    
                    if diff_type == 'data_mismatch':
                        report_lines.append(f"   类型: 数据不匹配")
                        report_lines.append(f"   关键列: {key_columns_diff}")
                        report_lines.append(f"   列级差异:")
                        
                        column_differences = diff.get('column_differences', {})
                        for col, values in column_differences.items():
                            if isinstance(values, (list, tuple)) and len(values) == 2:
                                val1, val2 = values
                                report_lines.append(f"     {col}:")
                                report_lines.append(f"       文件1: {val1}")
                                report_lines.append(f"       文件2: {val2}")
                            else:
                                report_lines.append(f"     {col}: {values}")
                        
                    elif diff_type == 'file1_only':
                        report_lines.append(f"   类型: 仅在文件1中存在")
                        report_lines.append(f"   关键列: {key_columns_diff}")
                        report_lines.append(f"   文件1数据:")
                        
                        file1_data = diff.get('file1_data', {})
                        if file1_data:
                            for col, val in file1_data.items():
                                if col in key_columns:
                                    report_lines.append(f"     {col}: {val} (关键列)")
                                else:
                                    report_lines.append(f"     {col}: {val}")
                        else:
                            report_lines.append("     数据不可用")
                                
                    elif diff_type == 'file2_only':
                        report_lines.append(f"   类型: 仅在文件2中存在")
                        report_lines.append(f"   关键列: {key_columns_diff}")
                        report_lines.append(f"   文件2数据:")
                        
                        file2_data = diff.get('file2_data', {})
                        if file2_data:
                            for col, val in file2_data.items():
                                if col in key_columns:
                                    report_lines.append(f"     {col}: {val} (关键列)")
                                else:
                                    report_lines.append(f"     {col}: {val}")
                        else:
                            report_lines.append("     数据不可用")
                    else:
                        report_lines.append(f"   类型: {diff_type}")
                        report_lines.append(f"   数据: {diff}")
                    
                    report_lines.append("")
                
                if len(differences) > 200:
                    report_lines.append(f"... 还有 {len(differences) - 200} 个差异未显示")
                    report_lines.append("")
        else:
            report_lines.append("详细对比结果: 数据不可用")
            report_lines.append("-" * 40)
            report_lines.append("")
        
        # 添加结果摘要
        report_lines.append("结果摘要:")
        report_lines.append("-" * 40)
        is_identical = result.get('is_identical', False)
        report_lines.append(f"文件是否相同: {'是' if is_identical else '否'}")
        report_lines.append(f"总差异数: {total_differences:,}")
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
    parser.add_argument('--max-compare-rows', type=int, 
                       help='最大对比行数，指定数值表示只对比前N行 (默认: 对比全部数据)')
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
            memory_limit=args.memory_limit,
            max_compare_rows=args.max_compare_rows
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
