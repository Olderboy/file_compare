import pandas as pd
import re
import sys
import argparse

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='汇总多组测试数据到总表')
    parser.add_argument('files', nargs='+', help='要合并的测试CSV文件（至少2个）')
    parser.add_argument('-o', '--output', default='test_summary.csv', help='输出文件名（默认：test_summary.csv）')
    return parser.parse_args()

def extract_number_with_unit(value):
    """从字符串中提取数值和单位"""
    if pd.isna(value) or value == '' or value == 'TraceBack':
        return None, None
    value_str = str(value)
    # 提取数字
    match = re.search(r'([\d.]+)([MG])?', value_str)
    if match:
        num = float(match.group(1))
        unit = match.group(2) if match.group(2) else ''
        return num, unit
    return None, None

def extract_number(value):
    """从字符串中提取数值（去除单位）- 向后兼容"""
    num, _ = extract_number_with_unit(value)
    return num

def format_with_unit(original_values):
    """保留原始格式带单位"""
    return '/'.join(str(v) for v in original_values if not pd.isna(v) and v != '')

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    input_files = args.files
    output_file = args.output

    print(f"正在读取 {len(input_files)} 个测试文件...")

    # 读取所有测试文件
    dfs = []
    for file_path in input_files:
        try:
            df = pd.read_csv(file_path, encoding='gbk')
            dfs.append(df)
            print(f"  [OK] {file_path}")
        except Exception as e:
            print(f"  [ERROR] {file_path}: {e}")
            return

    if len(dfs) < 2:
        print("错误：至少需要2个测试文件进行合并")
        return

    # 获取行数（以第一个文件为准）
    num_rows = len(dfs[0])

    # 基础列（不参与统计的列）
    base_columns = ['OriginFileName', 'PreferenceStdFileName', 'Type', 'Level', 'Env&Ver', 'Row&Column']

    # 需要统计的数值列
    numeric_columns = ['TotalTimeCount', 'DownloadFileTime', 'InterfaceRequestTime', 'WriteLocalTime',
                       'StartMemory', 'EndMemory', 'MaxMemory', 'MaxCpu']

    # 只合并原始值不计算平均值的列
    original_only_columns = ['FileCount']
    # 使用自定义分隔符的列
    custom_separator_columns = {'EndTimeRecord': '||'}

    # 创建汇总结果DataFrame
    result_df = dfs[0][base_columns].copy()

    # 为每个数值列进行处理
    for col in numeric_columns:
        # 原始值列（用/分隔）
        original_values = []
        for i in range(num_rows):
            vals = []
            for df in dfs:
                v = df[col].values[i]
                if pd.notna(v) and v != '' and v != 'TraceBack':
                    vals.append(str(v))
            original_values.append('/'.join(vals))

        result_df[f'{col}_Original'] = original_values

        # 计算平均值
        averages = []
        for i in range(num_rows):
            # 提取数值和单位
            nums_with_units = []
            for df in dfs:
                val = df[col].values[i]
                if pd.notna(val) and val != '' and val != 'TraceBack':
                    num, unit = extract_number_with_unit(val)
                    if num is not None:
                        # 统一转换为基准单位（内存转换为M，时间保持s）
                        if 'Memory' in col:
                            if unit == 'G':
                                nums_with_units.append(num * 1024)  # G转M
                            else:
                                nums_with_units.append(num)
                        else:
                            nums_with_units.append(num)

            if nums_with_units:
                avg = sum(nums_with_units) / len(nums_with_units)
                # 根据列名确定显示单位
                if 'Memory' in col:
                    # 转换回最合适的单位
                    if avg >= 1024:
                        averages.append(f"{avg/1024:.1f}G")
                    else:
                        averages.append(f"{avg:.0f}M")
                elif 'Time' in col and 'TimeCount' not in col:
                    averages.append(f"{avg:.3f}s")
                elif 'Cpu' in col:
                    averages.append(f"{avg:.0f}%")
                else:
                    averages.append(f"{avg:.1f}s")
            else:
                averages.append('')

        result_df[f'{col}_Average'] = averages

    # 处理只合并原始值的列（不计算平均值）
    for col in original_only_columns:
        # 原始值列（用/分隔）
        original_values = []
        for i in range(num_rows):
            vals = []
            for df in dfs:
                v = df[col].values[i]
                if pd.notna(v) and v != '' and v != 'TraceBack':
                    vals.append(str(v))
            original_values.append('/'.join(vals))

        result_df[f'{col}_Original'] = original_values

    # 处理使用自定义分隔符的列
    for col, separator in custom_separator_columns.items():
        # 原始值列（用自定义分隔符）
        original_values = []
        for i in range(num_rows):
            vals = []
            for df in dfs:
                v = df[col].values[i]
                if pd.notna(v) and v != '' and v != 'TraceBack':
                    vals.append(str(v))
            original_values.append(separator.join(vals))

        result_df[f'{col}_Original'] = original_values

    # 计算TotalTime浮动（最大值-最小值）
    print("计算TotalTime浮动...")
    time_float_values = []
    for i in range(num_rows):
        times = []
        for df in dfs:
            t = extract_number(df['TotalTimeCount'].values[i])
            if t is not None:
                times.append(t)

        if len(times) >= 2:
            time_diff = max(times) - min(times)
            time_float_values.append(f"{time_diff:.1f}s")
        else:
            time_float_values.append('')

    result_df['TotalTime_Float'] = time_float_values

    # 计算Memory增长（MaxMemory - StartMemory）的平均值
    print("计算Memory增长...")
    memory_growth_values = []
    for i in range(num_rows):
        # 对每组数据计算MaxMemory - StartMemory
        growth_values = []

        for df in dfs:
            max_mem_val = df['MaxMemory'].values[i]
            start_mem_val = df['StartMemory'].values[i]

            if pd.notna(max_mem_val) and pd.notna(start_mem_val) and max_mem_val != 'TraceBack' and start_mem_val != 'TraceBack':
                max_mem_num, max_mem_unit = extract_number_with_unit(max_mem_val)
                start_mem_num, start_mem_unit = extract_number_with_unit(start_mem_val)

                if max_mem_num is not None and start_mem_num is not None:
                    # 统一转换为M
                    max_mem_mb = max_mem_num * 1024 if max_mem_unit == 'G' else max_mem_num
                    start_mem_mb = start_mem_num * 1024 if start_mem_unit == 'G' else start_mem_num

                    growth = max_mem_mb - start_mem_mb
                    growth_values.append(growth)

        if growth_values:
            avg_growth = sum(growth_values) / len(growth_values)
            # 转换为最合适的单位
            if avg_growth >= 1024:
                memory_growth_values.append(f"{avg_growth/1024:.1f}G")
            else:
                memory_growth_values.append(f"{avg_growth:.0f}M")
        else:
            memory_growth_values.append('')

    result_df['Memory_Growth_Avg'] = memory_growth_values

    # 保存结果
    print(f"正在保存结果到 {output_file}...")
    try:
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"[OK] 汇总完成！结果已保存到 {output_file}")
    except PermissionError:
        backup_file = output_file.replace('.csv', '_new.csv')
        result_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"[OK] 汇总完成！结果已保存到 {backup_file}")

    print(f"共处理 {num_rows} 行数据，合并了 {len(dfs)} 个测试文件")
    print("\n生成的列包括：")
    print("- 原始值列（xxx_Original）：多组数据用/分隔（EndTimeRecord用||分隔）")
    print("- 平均值列（xxx_Average）：多组数据的平均值（FileCount和EndTimeRecord除外）")
    print("- TotalTime_Float：最大TotalTime-最小TotalTime")
    print("- Memory_Growth_Avg：MaxMemory-StartMemory的平均值")

if __name__ == '__main__':
    main()
