#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版CSV对比报告生成器
可以显示具体哪些行存在差异，并提供更详细的对比信息
"""

import csv
import pandas as pd
from typing import Dict, List, Set
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedReportGenerator:
    """增强版报告生成器"""
    
    def __init__(self, file1: str, file2: str, common_columns: List[str]):
        """
        初始化报告生成器
        
        Args:
            file1: 第一个CSV文件路径
            file2: 第二个CSV文件路径
            common_columns: 公共列名列表
        """
        self.file1 = file1
        self.file2 = file2
        self.common_columns = common_columns
        
    def get_row_content(self, file_path: str, line_number: int) -> Dict[str, str]:
        """
        获取指定行的内容
        
        Args:
            file_path: 文件路径
            line_number: 行号（从1开始，包含标题行）
            
        Returns:
            行内容字典
        """
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as file:
                reader = csv.DictReader(file)
                
                # 跳过到指定行
                for i, row in enumerate(reader):
                    if i + 1 == line_number:  # +1因为行号从1开始
                        return {col: row.get(col, '') for col in self.common_columns}
                
                return {}
                
        except Exception as e:
            logger.error(f"读取文件 {file_path} 第 {line_number} 行时出错: {e}")
            return {}
    
    def generate_detailed_report(self, differences: List[Dict], output_file: str = None) -> str:
        """
        生成详细的差异报告
        
        Args:
            differences: 差异列表
            output_file: 输出文件路径
            
        Returns:
            报告内容
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CSV文件详细差异报告")
        report_lines.append("=" * 80)
        report_lines.append(f"文件1: {self.file1}")
        report_lines.append(f"文件2: {self.file2}")
        report_lines.append(f"对比列: {', '.join(self.common_columns)}")
        report_lines.append(f"差异总数: {len(differences)}")
        report_lines.append("")
        
        # 按类型分组差异
        count_mismatches = [d for d in differences if d['type'] == 'count_mismatch']
        file2_only = [d for d in differences if d['type'] == 'file2_only']
        
        # 统计信息
        report_lines.append("差异统计:")
        report_lines.append(f"  数量不匹配: {len(count_mismatches)}")
        report_lines.append(f"  仅在文件2中: {len(file2_only)}")
        report_lines.append("")
        
        # 详细差异信息
        if count_mismatches:
            report_lines.append("数量不匹配的差异:")
            report_lines.append("-" * 50)
            
            for i, diff in enumerate(count_mismatches, 1):
                report_lines.append(f"{i}. 哈希值: {diff['hash'][:16]}...")
                report_lines.append(f"   文件1出现次数: {diff['file1_count']}")
                report_lines.append(f"   文件2出现次数: {diff['file2_count']}")
                
                # 显示文件1中的行内容
                if 'file1_lines' in diff and diff['file1_lines']:
                    report_lines.append(f"   文件1中的行:")
                    for line_num in sorted(diff['file1_lines']):
                        row_content = self.get_row_content(self.file1, line_num)
                        if row_content:
                            content_str = ' | '.join([f"{col}: {val}" for col, val in row_content.items()])
                            report_lines.append(f"     第{line_num}行: {content_str}")
                
                # 显示文件2中的行内容
                if 'file2_lines' in diff and diff['file2_lines']:
                    report_lines.append(f"   文件2中的行:")
                    for line_num in sorted(diff['file2_lines']):
                        row_content = self.get_row_content(self.file2, line_num)
                        if row_content:
                            content_str = ' | '.join([f"{col}: {val}" for col, val in row_content.items()])
                            report_lines.append(f"     第{line_num}行: {content_str}")
                
                report_lines.append("")
        
        if file2_only:
            report_lines.append("仅在文件2中出现的差异:")
            report_lines.append("-" * 50)
            
            for i, diff in enumerate(file2_only, 1):
                report_lines.append(f"{i}. 哈希值: {diff['hash'][:16]}...")
                report_lines.append(f"   文件2出现次数: {diff['file2_count']}")
                
                # 显示文件2中的行内容
                if 'file2_lines' in diff and diff['file2_lines']:
                    report_lines.append(f"   文件2中的行:")
                    for line_num in sorted(diff['file2_lines']):
                        row_content = self.get_row_content(self.file2, line_num)
                        if row_content:
                            content_str = ' | '.join([f"{col}: {val}" for col, val in row_content.items()])
                            report_lines.append(f"     第{line_num}行: {content_str}")
                
                report_lines.append("")
        
        # 生成行号汇总
        report_lines.append("行号汇总:")
        report_lines.append("-" * 30)
        
        all_file1_lines = set()
        all_file2_lines = set()
        
        for diff in differences:
            if 'file1_lines' in diff:
                all_file1_lines.update(diff['file1_lines'])
            if 'file2_lines' in diff:
                all_file2_lines.update(diff['file2_lines'])
        
        if all_file1_lines:
            report_lines.append(f"文件1中有差异的行号: {', '.join(map(str, sorted(all_file1_lines)))}")
        if all_file2_lines:
            report_lines.append(f"文件2中有差异的行号: {', '.join(map(str, sorted(all_file2_lines)))}")
        
        report = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                logger.info(f"详细报告已保存到: {output_file}")
            except Exception as e:
                logger.error(f"保存详细报告时出错: {e}")
        
        return report
    
    def generate_summary_report(self, differences: List[Dict], output_file: str = None) -> str:
        """
        生成摘要报告
        
        Args:
            differences: 差异列表
            output_file: 输出文件路径
            
        Returns:
            报告内容
        """
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("CSV文件差异摘要报告")
        report_lines.append("=" * 60)
        report_lines.append(f"文件1: {self.file1}")
        report_lines.append(f"文件2: {self.file2}")
        report_lines.append(f"对比列: {', '.join(self.common_columns)}")
        report_lines.append("")
        
        # 按类型分组差异
        count_mismatches = [d for d in differences if d['type'] == 'count_mismatch']
        file2_only = [d for d in differences if d['type'] == 'file2_only']
        
        report_lines.append("差异摘要:")
        report_lines.append(f"  数量不匹配: {len(count_mismatches)}")
        report_lines.append(f"  仅在文件2中: {len(file2_only)}")
        report_lines.append("")
        
        # 显示前10个差异的行号
        if count_mismatches:
            report_lines.append("数量不匹配的行号 (前10个):")
            for i, diff in enumerate(count_mismatches[:10], 1):
                if 'file1_lines' in diff and diff['file1_lines']:
                    line_nums = ', '.join(map(str, sorted(diff['file1_lines'])))
                    report_lines.append(f"  {i}. 文件1行号: {line_nums}")
            if len(count_mismatches) > 10:
                report_lines.append(f"  ... 还有 {len(count_mismatches) - 10} 个差异")
            report_lines.append("")
        
        if file2_only:
            report_lines.append("仅在文件2中的行号 (前10个):")
            for i, diff in enumerate(file2_only[:10], 1):
                if 'file2_lines' in diff and diff['file2_lines']:
                    line_nums = ', '.join(map(str, sorted(diff['file2_lines'])))
                    report_lines.append(f"  {i}. 文件2行号: {line_nums}")
            if len(file2_only) > 10:
                report_lines.append(f"  ... 还有 {len(file2_only) - 10} 个差异")
            report_lines.append("")
        
        report = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                logger.info(f"摘要报告已保存到: {output_file}")
            except Exception as e:
                logger.error(f"保存摘要报告时出错: {e}")
        
        return report


def main():
    """主函数 - 示例用法"""
    import argparse
    
    parser = argparse.ArgumentParser(description='增强版CSV对比报告生成器')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--common-columns', nargs='+', help='要对比的公共列名')
    parser.add_argument('--differences', help='差异数据JSON文件路径')
    parser.add_argument('--output-dir', default='.', help='输出目录')
    parser.add_argument('--detailed', action='store_true', help='生成详细报告')
    
    args = parser.parse_args()
    
    try:
        # 如果没有指定公共列，尝试自动检测
        if not args.common_columns:
            # 读取两个文件的列名
            df1_cols = pd.read_csv(args.file1, nrows=0).columns.tolist()
            df2_cols = pd.read_csv(args.file2, nrows=0).columns.tolist()
            common_columns = list(set(df1_cols).intersection(set(df2_cols)))
            logger.info(f"自动检测到公共列: {common_columns}")
        else:
            common_columns = args.common_columns
        
        if not common_columns:
            logger.error("没有找到公共列，无法进行对比")
            return
        
        # 创建报告生成器
        generator = EnhancedReportGenerator(args.file1, args.file2, common_columns)
        
        # 如果没有提供差异数据，生成示例报告
        if not args.differences:
            logger.warning("未提供差异数据，生成示例报告")
            # 创建示例差异数据
            differences = [
                {
                    'hash': 'example_hash_1234567890abcdef',
                    'file1_count': 2,
                    'file2_count': 1,
                    'type': 'count_mismatch',
                    'file1_lines': [5, 10],
                    'file2_lines': [5]
                }
            ]
        else:
            # 从文件读取差异数据
            import json
            with open(args.differences, 'r', encoding='utf-8') as f:
                differences = json.load(f)
        
        # 生成报告
        output_dir = Path(args.output_dir)
        output_dir.mkdir(exist_ok=True)
        
        if args.detailed:
            # 生成详细报告
            detailed_report = generator.generate_detailed_report(
                differences, 
                str(output_dir / "detailed_differences_report.txt")
            )
            print("详细报告已生成")
        else:
            # 生成摘要报告
            summary_report = generator.generate_summary_report(
                differences, 
                str(output_dir / "summary_differences_report.txt")
            )
            print("摘要报告已生成")
            
    except Exception as e:
        logger.error(f"生成报告时出错: {e}")
        return


if __name__ == "__main__":
    main()
