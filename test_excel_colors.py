"""
测试Excel颜色输出功能
"""
import pandas as pd
from merge_compare_results import save_excel_with_colors

# 创建测试数据
data = {
    'OriginFileName': ['test1.xlsx', 'test2.xlsx', 'test3.xlsx'],
    'Type': ['A', 'B', 'C'],
    'TotalTimeCount_差值': ['+10.5', '-5.2', '+2.3'],
    'TotalTimeCount_状态': ['✓ 提升', '✗ 劣化', '- 持平'],
    'InterfaceRequestTime_差值': ['-2.1', '+3.5', '-0.5'],
    'InterfaceRequestTime_状态': ['✗ 劣化', '✓ 提升', '- 持平']
}

df = pd.DataFrame(data)

# 状态列颜色配置
status_columns = {
    'TotalTimeCount_状态': {
        '✓ 提升': '90EE90',  # 淡绿色
        '✗ 劣化': 'FFB6C1',  # 淡红色
        '- 持平': 'FFFFFF'   # 白色
    },
    'InterfaceRequestTime_状态': {
        '✓ 提升': '90EE90',
        '✗ 劣化': 'FFB6C1',
        '- 持平': 'FFFFFF'
    }
}

# 保存Excel文件（带颜色）
output_file = 'test_colors_output.xlsx'
save_excel_with_colors(df, output_file, status_columns)

print(f"✓ 测试完成！")
print(f"  输出文件: {output_file}")
print(f"  总行数: {len(df)}")
print(f"  总列数: {len(df.columns)}")
print(f"\n状态列颜色:")
for col_name, colors in status_columns.items():
    print(f"  - {col_name}:")
    for status, color in colors.items():
        print(f"    • {status}: #{color}")

print(f"\n请打开 {output_file} 查看颜色效果！")
