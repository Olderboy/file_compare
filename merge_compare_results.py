import pandas as pd
import argparse
import os

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='合并对比两组测试结果数据')
    parser.add_argument('left_file', help='左表文件（高斯分布结果）')
    parser.add_argument('right_file', help='右表文件（SR结果）')
    parser.add_argument('-o', '--output', default='comparison_result.csv', help='输出文件名（默认：comparison_result.csv，支持.csv和.xlsx）')
    return parser.parse_args()

def read_file(file_path):
    """根据文件扩展名自动选择读取方法"""
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.xlsx' or ext == '.xls':
            return pd.read_excel(file_path)
        elif ext == '.csv':
            # 尝试多种编码
            try:
                return pd.read_csv(file_path, encoding='gbk')
            except UnicodeDecodeError:
                return pd.read_csv(file_path, encoding='utf-8-sig')
        else:
            raise ValueError(f"不支持的文件格式: {ext}。仅支持.csv、.xlsx和.xls文件")
    except Exception as e:
        raise Exception(f"读取文件失败: {e}")

def save_file(df, file_path):
    """根据文件扩展名自动选择保存方法"""
    output_ext = os.path.splitext(file_path)[1].lower()
    try:
        if output_ext == '.xlsx' or output_ext == '.xls':
            df.to_excel(file_path, index=False, engine='openpyxl')
        else:
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        return False

def main():
    """主函数"""
    args = parse_arguments()
    left_file = args.left_file
    right_file = args.right_file
    output_file = args.output

    print(f"正在读取左表文件: {left_file}")
    try:
        left_df = read_file(left_file)
        print(f"  [OK] 左表包含 {len(left_df)} 行, {len(left_df.columns)} 列")
    except Exception as e:
        print(f"  [ERROR] 读取左表失败: {e}")
        return

    print(f"正在读取右表文件: {right_file}")
    try:
        right_df = read_file(right_file)
        print(f"  [OK] 右表包含 {len(right_df)} 行, {len(right_df.columns)} 列")
    except Exception as e:
        print(f"  [ERROR] 读取右表失败: {e}")
        return

    # 匹配列（用于merge的键）
    match_columns = ['OriginFileName', 'Type', 'Level']

    # 基础列（不参与对比的列）
    base_columns = ['OriginFileName', 'PreferenceStdFileName', 'Type', 'Level', 'Env&Ver']

    # 检查匹配列是否存在
    missing_left = [col for col in match_columns if col not in left_df.columns]
    missing_right = [col for col in match_columns if col not in right_df.columns]

    if missing_left:
        print(f"[ERROR] 左表缺少以下匹配列: {', '.join(missing_left)}")
        return
    if missing_right:
        print(f"[ERROR] 右表缺少以下匹配列: {', '.join(missing_right)}")
        return

    print(f"\n使用匹配列: {', '.join(match_columns)}")

    # 重命名左表的列（除了基础列）
    left_columns = left_df.columns.tolist()
    left_new_columns = {}
    for col in left_columns:
        if col not in base_columns:
            left_new_columns[col] = f"{col}_gauss"

    left_df_renamed = left_df.rename(columns=left_new_columns)

    # 重命名右表的列（除了基础列）
    right_columns = right_df.columns.tolist()
    right_new_columns = {}
    for col in right_columns:
        if col not in base_columns:
            right_new_columns[col] = f"{col}_sr"

    right_df_renamed = right_df.rename(columns=right_new_columns)

    # 执行merge操作
    print("\n正在执行merge操作...")
    merged_df = pd.merge(
        left_df_renamed,
        right_df_renamed,
        on=match_columns,
        how='outer',
        suffixes=('', '_duplicate')  # 避免重复列名
    )

    # 处理可能的重复列（如果有）
    duplicate_cols = [col for col in merged_df.columns if col.endswith('_duplicate')]
    if duplicate_cols:
        print(f"[WARNING] 发现重复列: {', '.join(duplicate_cols)}")
        for col in duplicate_cols:
            merged_df = merged_df.drop(columns=[col])

    # 重新排列列顺序：保持左表的列顺序，然后添加右表独有的列
    print("\n正在调整列顺序...")

    # 获取基础列
    ordered_columns = []

    # 先添加基础列
    for col in base_columns:
        if col in merged_df.columns:
            ordered_columns.append(col)

    # 按照左表原始顺序添加左表的重命名列
    for col in left_columns:
        if col not in base_columns:
            renamed_col = f"{col}_gauss"
            if renamed_col in merged_df.columns:
                ordered_columns.append(renamed_col)

    # 添加右表独有的列（不在左表中的）
    right_only_columns = []
    for col in right_columns:
        if col not in base_columns and col not in left_columns:
            renamed_col = f"{col}_sr"
            if renamed_col in merged_df.columns and renamed_col not in ordered_columns:
                right_only_columns.append(renamed_col)

    ordered_columns.extend(right_only_columns)

    # 应用新的列顺序
    merged_df = merged_df[ordered_columns]

    # 保存结果
    print(f"\n正在保存结果到 {output_file}...")
    try:
        save_file(merged_df, output_file)
        print(f"[OK] 合并对比完成！结果已保存到 {output_file}")
    except PermissionError:
        # 生成备用文件名
        base, ext = os.path.splitext(output_file)
        backup_file = base + '_new' + ext
        save_file(merged_df, backup_file)
        print(f"[OK] 合并对比完成！结果已保存到 {backup_file}")

    print(f"\n共生成 {len(merged_df)} 行数据，{len(merged_df.columns)} 列")
    print(f"\n列命名规则:")
    print(f"- 基础列: {', '.join(base_columns)}")
    print(f"- 左表列: 添加 _gauss 后缀")
    print(f"- 右表列: 添加 _sr 后缀")
    print(f"\n匹配列: {', '.join(match_columns)}")

if __name__ == '__main__':
    main()
