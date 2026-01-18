"""
测试历史记录功能完整性
"""
import json
import os

history_file = '.merge_history.json'

# 创建完整的历史记录测试数据
test_records = {
    'records': [
        {
            'timestamp': '2026-01-18 12:00:00',
            'left_files': ['test1.xlsx', 'test2.xlsx', 'test3.xlsx'],
            'left_paths': [r'E:\Work\数据文件2\test1.xlsx',
                          r'E:\Work\数据文件2\test2.xlsx',
                          r'E:\Work\数据文件2\test3.xlsx'],
            'right_files': [],
            'right_paths': [],
            'output_file': r'D:\XpathLib\merge_test_result\file_compare\comparison_result.csv',
            'config': {
                'left_suffix': 'gauss1',
                'right_suffix': 'gauss2',
                'numeric_columns': 'TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime,StartMemory,EndMemory,MaxMemory,MaxCpu,CreateTableTime',
                'original_only_columns': 'FileCount',
                'custom_separator_columns': 'EndTimeRecord:||'
            }
        },
        {
            'timestamp': '2026-01-17 14:30:00',
            'left_files': ['data1.csv'],
            'left_paths': [r'D:\data\data1.csv'],
            'right_files': ['data2.csv'],
            'right_paths': [r'D:\data\data2.csv'],
            'output_file': r'D:\output\result.csv',
            'config': {
                'left_suffix': 'old',
                'right_suffix': 'new',
                'numeric_columns': 'TotalTimeCount,DownloadFileTime',
                'original_only_columns': 'FileCount,TestCount',
                'custom_separator_columns': 'EndTimeRecord:||,ErrorStack:##'
            }
        },
        {
            'timestamp': '2026-01-16 09:15:00',
            'left_files': ['batch1.xlsx', 'batch2.xlsx', 'batch3.xlsx', 'batch4.xlsx', 'batch5.xlsx'],
            'left_paths': [r'D:\test\batch1.xlsx',
                          r'D:\test\batch2.xlsx',
                          r'D:\test\batch3.xlsx',
                          r'D:\test\batch4.xlsx',
                          r'D:\test\batch5.xlsx'],
            'right_files': ['ref1.xlsx', 'ref2.xlsx'],
            'right_paths': [r'D:\ref\ref1.xlsx', r'D:\ref\ref2.xlsx'],
            'output_file': r'D:\results\batch_comparison.csv',
            'config': {
                'left_suffix': 'test',
                'right_suffix': 'ref',
                'numeric_columns': 'TotalTimeCount,DownloadFileTime,InterfaceRequestTime,WriteLocalTime',
                'original_only_columns': 'FileCount',
                'custom_separator_columns': ''
            }
        }
    ]
}

# 保存测试历史记录
with open(history_file, 'w', encoding='utf-8') as f:
    json.dump(test_records, f, ensure_ascii=False, indent=2)

print("Created test history records successfully!")
print(f"Total records: {len(test_records['records'])}")
print(f"File location: {os.path.abspath(history_file)}")
print("\nHistory content:")
print("=" * 60)
for i, record in enumerate(test_records['records'], 1):
    print(f"\nRecord {i}:")
    print(f"  Time: {record['timestamp']}")
    print(f"  Left files: {len(record['left_files'])}")
    print(f"  Right files: {len(record['right_files'])}")
    config = record['config']
    print(f"  Suffix: {config['left_suffix']} / {config['right_suffix']}")
    print(f"  Numeric columns: {len(config['numeric_columns'].split(','))}")
print("=" * 60)
