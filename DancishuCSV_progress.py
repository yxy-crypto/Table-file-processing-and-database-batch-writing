import pandas as pd
import mysql.connector


def read_csv_to_mysql(csv_file="dancishu.csv",
                      table_name="dancishu",
                      name_column="word",
                      belong_column="belong",
                      host="localhost",
                      user="root",
                      password="password",
                      database="exam",
                      chunksize=1000,
                      sep=';',
                      encoding='utf-8',
                      header=None):

    try:
        # 连接MySQL数据库
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = mydb.cursor()

        # 获取所有已存在的名称和它们的 belong 值
        sql = f"SELECT {name_column}, {belong_column} FROM {table_name}"
        mycursor.execute(sql)
        existing_data = {row[0]: row[1] for row in mycursor.fetchall()}

        # 手动指定列名
        columns = ['word', 'meaning', 'collocation', 'belong']

        # 读取CSV文件，逐块插入
        for chunk in pd.read_csv(csv_file, sep=sep, encoding=encoding, header=None, names=columns, chunksize=chunksize):
            chunk.reset_index(drop=True, inplace=True)

            # 检查 CSV 是否包含 'word' 列
            if name_column not in chunk.columns:
                raise ValueError(f"CSV文件中缺少 '{name_column}' 列")

            # 过滤出重复数据
            duplicate_chunk = chunk[chunk[name_column].isin(existing_data.keys())]
            if not duplicate_chunk.empty:
                print(f"以下内容由于与数据库中已有数据重复，未被录入：")
                print(duplicate_chunk[name_column].to_list())

                # 更新重复单词的 `belong` 属性,0=未分类 1=外刊词汇 2=考研词汇 3=四级词汇 4=六级词汇 5=专四词汇 6=专八词汇 7=雅思词汇 8=托福词汇 9=GRE词汇
                for _, row in duplicate_chunk.iterrows():
                    word = row[name_column]
                    new_belong = str(row[belong_column])
                    current_belong = existing_data[word]
                    if new_belong not in current_belong.split(','):
                        updated_belong = current_belong + ',' + new_belong
                        update_sql = f"UPDATE {table_name} SET {belong_column} = %s WHERE {name_column} = %s"
                        mycursor.execute(update_sql, (updated_belong, word))
                        mydb.commit()
                        print(f"单词 {word} 的 belong 属性已更新为: {updated_belong}")

            # 插入未重复的数据
            chunk = chunk[~chunk[name_column].isin(existing_data.keys())]

            # NaN 替换为 None
            chunk = chunk.where(pd.notna(chunk), None)

            # 默认值填充
            chunk['belong'] = chunk['belong'].fillna('0')

            # 构造插入语句
            values = chunk[columns].values.tolist()
            sql = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
            mycursor.executemany(sql, values)
            mydb.commit()

    except Exception as e:
        print("Error:", e)

    finally:
        if 'mycursor' in locals():
            mycursor.close()
        if 'mydb' in locals():
            mydb.close()


# 示例用法
read_csv_to_mysql(csv_file="dancishu.csv", sep=';', encoding='utf-8', header=None)
