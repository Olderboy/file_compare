"""
测试历史记录功能
"""
import json
import os

history_file = '.merge_history.json'

# 创建示例历史记录
sample_history = {
    'records': [
        {
            'timestamp': '2025-01-18 10:30:15',
            'left_files': ['test1.xlsx', 'test2.xlsx'],
            'left_paths': [r'E:\Work\数据文件2\test1.xlsx', r'E:\Work\数据文件2\test2.xlsx'],
            'right_files': ['test3.xlsx'],
            'right_paths': [r'E:\Work\数据文件2\test3.xlsx'],
            'output_file': r'D:\output\result.csv',
            'config': {
                'left_suffix': 'gauss',
                'right_suffix': 'sr',
                'numeric_columns': 'TotalTimeCount,DownloadFileTime',
                'original_only_columns': 'FileCount',
                'custom_separator_columns': 'EndTimeRecord:||'
            }
        },
        {
            'timestamp': '2025-01-17 15:45:20',
            'left_files': ['data1.csv'],
            'left_paths': [r'D:\data\data1.csv'],
            'right_files': ['data2.csv'],
            'right_paths': [r'D:\data\data2.csv'],
            'output_file': r'D:\output\comparison.csv',
            'config': {
                'left_suffix': 'old',
                'right_suffix': 'new',
                'numeric_columns': 'TotalTimeCount',
                'original_only_columns': 'FileCount',
                'custom_separator_columns': ''
            }
        }
    ]
}

# 保存示例历史记录
with open(history_file, 'w', encoding='utf-8') as f:
    json.dump(sample_history, f, ensure_ascii=False, indent=2)

print(f"✓ 已创建示例历史记录文件: {history_file}")
print(f"✓ 包含 {len(sample_history['records'])} 条记录")
print("\n历史记录内容:")
print(json.dumps(sample_history, ensure_ascii=False, indent=2))
