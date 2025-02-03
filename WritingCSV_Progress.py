import pandas as pd
import mysql.connector

def read_csv_to_mysql(csv_file="writingword.csv",
                       table_name="writing",
                       name_column="content",
                       host="localhost",
                       user="root",
                       password="password",
                       database="exam",
                       chunksize=1000,
                       sep=';',
                       encoding='utf-8',
                       header=None):  # header=None, 表示CSV没有表头

    """
    读取CSV文件，写入MySQL数据库，并进行去重

    Args:
    csv_file: CSV文件路径
    table_name: MySQL表名
    name_column: 用于去重的列名
    host: MySQL主机
    user: MySQL用户名
    password: MySQL密码
    database: MySQL数据库名
    chunksize: 批量插入的数据块大小
    sep: CSV文件分隔符
    encoding: CSV文件编码
    header: 是否包含表头，默认为None，表示CSV文件没有表头
    """

    try:
        # 连接MySQL数据库
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = mydb.cursor()

        # 获取所有已存在的名称
        sql = f"SELECT {name_column} FROM {table_name}"
        mycursor.execute(sql)
        existing_names = set(row[0] for row in mycursor.fetchall())

        # 手动指定列名
        columns = ['form', 'content', 'translation', 'domain', 'register']

        # 读取CSV文件，逐块插入
        for chunk in pd.read_csv(csv_file, sep=sep, encoding=encoding, header=None, names=columns, chunksize=chunksize):
            # 重置索引，避免重复索引问题
            chunk.reset_index(drop=True, inplace=True)

            # 检查CSV是否包含'content'这一列
            if name_column not in chunk.columns:
                raise ValueError(f"CSV文件中缺少 '{name_column}' 列")

            # 过滤掉已存在的数据
            existing_chunk = chunk[chunk[name_column].isin(existing_names)]
            if not existing_chunk.empty:
                print(f"以下内容由于与数据库中已有数据重复，未被录入：")
                print(existing_chunk[name_column].to_list())

            chunk = chunk[~chunk[name_column].isin(existing_names)]  # 过滤掉已存在的单词

            # 处理 NaN 为 None，确保插入数据时空值为 None 而不是 'nan'
            chunk = chunk.where(pd.notna(chunk), None)

            # 填充默认值：为 form, domain 和 register 列为空的值填充默认值
            chunk['form'] = chunk['form'].fillna('sentence')
            chunk['domain'] = chunk['domain'].fillna('daily')
            chunk['register'] = chunk['register'].fillna('neutral')

            # 构造插入语句，指定插入的字段
            values = chunk[columns].values.tolist()

            # 参数化插入，防止SQL注入
            sql = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
            mycursor.executemany(sql, values)

            # 更新已存在的名称集合，避免重复插入
            existing_names.update(chunk[name_column])

            # 提交事务
            mydb.commit()

    except Exception as e:
        print("Error:", e)
        print("CSV data:", chunk.head())  # 打印出错时的部分数据，方便调试

    finally:
        if 'mycursor' in locals():
            mycursor.close()
        if 'mydb' in locals():
            mydb.close()

# 示例用法
read_csv_to_mysql(csv_file="writingword.csv", sep=';', encoding='utf-8', header=None)
