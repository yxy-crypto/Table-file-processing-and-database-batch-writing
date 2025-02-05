import pandas as pd
import mysql.connector
import nltk
from nltk.stem import WordNetLemmatizer
import swifter

nltk.download('wordnet')


def lemmatize_word(word):
    lemmatizer = WordNetLemmatizer()
    return lemmatizer.lemmatize(word.lower())


def read_csv_to_mysql_optimized(csv_file, awl_file, table_name, name_column, host, user, password, database,
                                chunksize=1000, sep=';', encoding='utf-8', header=None):
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = mydb.cursor()

        # 查询数据库中的所有单词（适用于 10 万条以下）
        mycursor.execute(f"SELECT {name_column} FROM {table_name}")
        existing_names = {row[0].lower() for row in mycursor.fetchall()}

        # 读取 AWL 词表
        awl_words = set()
        awl_df = pd.read_excel(awl_file)
        if 'word' in awl_df.columns:
            awl_words = {lemmatize_word(word) for word in awl_df['word'].dropna().astype(str)}

        duplicate_words = []
        columns = ['word', 'meaning', 'collocation', 'belong']

        for chunk in pd.read_csv(csv_file, sep=sep, encoding=encoding, header=header, names=columns,
                                 chunksize=chunksize):
            chunk.reset_index(drop=True, inplace=True)

            # 词形还原处理
            chunk['lemma_word'] = chunk['word'].astype(str).swifter.apply(lemmatize_word)

            # 标记 AWL 词汇
            chunk['belong'] = chunk['lemma_word'].apply(lambda x: 'AWL' if x in awl_words else 'neutral')

            # 找到重复的单词
            duplicates = chunk[chunk['lemma_word'].isin(existing_names)]
            duplicate_words.append(duplicates)

            # 过滤掉已存在的单词
            chunk = chunk[~chunk['lemma_word'].isin(existing_names)]
            if chunk.empty:
                continue

            chunk.drop(columns=['lemma_word'], inplace=True)
            chunk = chunk.applymap(lambda x: None if pd.isna(x) or x == 'nan' else x)

            # 使用 INSERT IGNORE 避免重复
            sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON DUPLICATE KEY UPDATE word=word
            """
            mydb.start_transaction()
            mycursor.executemany(sql, chunk[columns].values.tolist())
            mydb.commit()

            # 更新已存在的单词集合
            existing_names.update(chunk['word'].str.lower())

        print("数据写入完成！")

        if duplicate_words:
            duplicate_df = pd.concat(duplicate_words, ignore_index=True)
            duplicate_df.drop(columns=['lemma_word'], inplace=True)
            duplicate_df.to_excel("重复单词.xlsx", index=False)
            print("重复单词已保存到 '重复单词.xlsx'")

    except Exception as e:
        print("发生错误:", e)

    finally:
        if 'mycursor' in locals():
            mycursor.close()
        if 'mydb' in locals():
            mydb.close()


# 示例调用
read_csv_to_mysql_optimized(
    csv_file="dancishu.csv",
    awl_file="AWL词表.xlsx",
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
