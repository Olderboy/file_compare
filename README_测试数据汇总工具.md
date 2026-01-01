# 测试数据汇总工具使用说明

## 功能描述
该工具用于合并多组测试数据到一个汇总表，自动计算平均值和浮动值。

## 使用方法

### 基本用法
```bash
python merge_test_results.py test1.csv test2.csv test3.csv
```

### 指定输出文件名
```bash
python merge_test_results.py test1.csv test2.csv -o my_summary.csv
```

### 合并2个文件
```bash
python merge_test_results.py test1.csv test2.csv
```

### 合并4个或更多文件
```bash
python merge_test_results.py test1.csv test2.csv test3.csv test4.csv test5.csv
```

## 输出说明

生成的汇总表包含以下列：

1. **原始值列（xxx_Original）**：多组测试数据用 `/` 分隔
   - 特殊：`EndTimeRecord_Original` 使用 `||` 分隔（避免与日期斜杠混淆）

2. **平均值列（xxx_Average）**：多组测试数据的平均值
   - 例外：`FileCount` 和 `EndTimeRecord` 不计算平均值

3. **TotalTime_Float**：最大TotalTime - 最小TotalTime
   - 用于观察同一测试在不同运行时的浮动情况

4. **Memory_Growth_Avg**：MaxMemory - StartMemory 的平均值
   - 显示测试过程中的内存增长情况

## 支持的文件格式
- CSV格式
- 编码：GBK（脚本自动处理）
- 必须包含相同的列结构

## 示例

输入文件：
```
test1.csv, test2.csv, test3.csv
```

输出文件：
```
test_summary.csv (默认)
```

输出列示例：
```
TotalTimeCount_Original: 80.0s/89.0s/90.0s
TotalTimeCount_Average: 86.3s
TotalTime_Float: 10.0s
Memory_Growth_Avg: 2.7G
```
