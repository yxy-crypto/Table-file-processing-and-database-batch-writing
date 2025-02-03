import pandas as pd
import mysql.connector

def read_csv_to_mysql_optimized(csv_file, table_name, name_column, host, user, password, database, chunksize=1000, sep=';', encoding='utf-8', header=None):
    try:
        # 连接 MySQL 数据库
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = mydb.cursor()

        # 获取现有的名称，放到集合中（一次性读取）
        sql = f"SELECT {name_column} FROM {table_name}"
        mycursor.execute(sql)
        existing_names = set(row[0] for row in mycursor.fetchall())

        # 手动指定列名
        columns = ['word', 'explanation', 'detail', 'belong', 'incapable']

        # 分块读取 CSV 文件并插入到数据库
        for chunk in pd.read_csv(csv_file, sep=sep, encoding=encoding, header=header, names=columns, chunksize=chunksize):
            # 重置索引，避免索引问题
            chunk.reset_index(drop=True, inplace=True)

            # 过滤已存在的数据
            chunk = chunk[~chunk[name_column].isin(existing_names)]

            # 如果没有需要插入的数据，跳过当前块
            if chunk.empty:
                continue

            # 填充默认值
            chunk['belong'] = chunk.get('belong', pd.Series(dtype='object')).fillna('N0')
            chunk['incapable'] = chunk.get('incapable', pd.Series(dtype='object')).fillna('未分类')

            # 替换 NaN 和 'nan' 为 None
            chunk = chunk.applymap(lambda x: None if pd.isna(x) or x == 'nan' else x)

            # 调试：打印数据块的前几行
            print("当前处理的数据块：")
            print(chunk.head())

            # 准备插入数据
            values = chunk[columns].values.tolist()
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
            mycursor.executemany(sql, values)

            # 提交事务
            mydb.commit()

            # 更新已存在的名称集合
            existing_names.update(chunk[name_column])

        print("数据写入完成！")

    except Exception as e:
        print("发生错误:", e)

    finally:
        if 'mycursor' in locals():
            mycursor.close()
        if 'mydb' in locals():
            mydb.close()

# 示例用法
read_csv_to_mysql_optimized(
    csv_file="word.csv",
    table_name="study",
    name_column="word",
    host="localhost",
    user="root",
    password="password",
    database="exam",
    chunksize=1000,
    sep=';',
    encoding='utf-8',
    header=None
)
