import pandas as pd
import openpyxl
#将excel表格转化为csv文件，表格结构为：word explanation detail belong incapable，执行过本程序再直接执行CSV_Progress.py文件即可将csv文件写入。

def process_excel_to_csv(excel_path, output_csv_path):
    """
    处理 Excel 文件，将其转换为符合要求的 CSV 文件格式。
    其中 'word' 和 'explanation' 必须存在，其他字段可以为空。

    Args:
    excel_path: Excel 文件路径
    output_csv_path: 输出的 CSV 文件路径
    """
    # 读取 Excel 文件
    df = pd.read_excel(excel_path)

    # 打印读取的数据，检查列名和数据
    print("读取的 Excel 数据：")
    print(df.head())

    # 确保列名是正确的，假设列名已经是标准的，不需要修改
    # 如果列名不一致，进行映射，例如：
    # df.rename(columns={'原列名': 'word', '原列名': 'explanation', ...}, inplace=True)

    # 添加 'detail', 'belong', 'incapable' 列，并确保它们允许为空
    # 如果这些列不存在，首先创建它们并填充为 None 或 NaN
    if 'detail' not in df.columns:
        df['detail'] = None
    if 'belong' not in df.columns:
        df['belong'] = None
    if 'incapable' not in df.columns:
        df['incapable'] = None

    # 检查并确保 'word' 和 'explanation' 列非空
    if 'word' not in df.columns or 'explanation' not in df.columns:
        raise ValueError("Excel 文件中缺少必要的 'word' 或 'explanation' 列。")

    # 保留需要的列：'word', 'explanation', 'detail', 'belong', 'incapable'
    df = df[['word', 'explanation', 'detail', 'belong', 'incapable']]

    # 输出为 CSV 文件，确保支持中文字符编码，并使用分号分隔
    df.to_csv(output_csv_path, index=False, sep=';', encoding='utf-8')

    print(f"CSV 文件已保存到 {output_csv_path}")

# 示例用法
excel_path = 'word.xlsx'  # Excel 文件路径
output_csv_path = 'word.csv'  # 输出的 CSV 文件路径

# 调用函数处理 Excel 文件并转换为 CSV 文件
process_excel_to_csv(excel_path, output_csv_path)
