import requests
import pandas as pd
from tqdm import tqdm  # 添加进度条

API_KEY = "029e05a1-8be8-4594-85bd-59d73cfcf271"
URL = "https://www.dictionaryapi.com/api/v3/references/collegiate/json/{}?key={}"

# 读取 Excel 文件
df = pd.read_excel('test.xlsx')  # 假设 Excel 文件的列中包含单词
words = df['word'].tolist()

# 存储每个单词的 JSON 数据
word_data = []

# 遍历单词并添加进度条
for word in tqdm(words, desc="查询进度", unit="word"):
    try:
        response = requests.get(URL.format(word, API_KEY), proxies={"http": None, "https": None})
        if response.status_code == 200:
            data = response.json()  # 获取 JSON 数据

            if not data:
                # 如果返回的是空列表，说明找不到单词
                word_data.append({
                    'Word': word,
                    'Part of Speech': 'Not found',
                    'Pronunciation': 'Not found',
                    'Etymology': 'Not found',
                    'Derived Words': 'Not found',
                    'Short Definitions': 'Not found',
                    'Definitions': 'Not found'
                })
                continue

            if isinstance(data, list) and isinstance(data[0], str):
                # 如果 data 是字符串列表，而不是 JSON 结构，则跳过
                word_data.append({
                    'Word': word,
                    'Part of Speech': 'Error: No JSON data',
                    'Pronunciation': 'Error: No JSON data',
                    'Etymology': 'Error: No JSON data',
                    'Derived Words': 'Error: No JSON data',
                    'Short Definitions': 'Error: No JSON data',
                    'Definitions': 'Error: No JSON data'
                })
                continue

            for entry in data:
                if not isinstance(entry, dict):
                    # 如果 entry 不是字典，跳过
                    continue

                part_of_speech = entry.get('fl', 'No part of speech found')
                pronunciation = entry.get('hwi', {}).get('hw', 'No pronunciation found')
                etymology = [str(item) for item in entry.get('et', ['No etymology found'])]
                derived_words = [uro.get('ure', 'No derived word found') for uro in entry.get('uros', [])]
                short_definitions = entry.get('shortdef', ['No short definition found'])

                # 获取详细定义
                definitions = []
                for sense in entry.get('def', []):
                    for seq in sense.get('sseq', []):
                        for sense_data in seq:
                            if isinstance(sense_data, dict) and 'sense' in sense_data:
                                definition = sense_data['sense'].get('dt', [])
                                for item in definition:
                                    if isinstance(item, list) and item[0] == 'text':
                                        definitions.append(item[1])

                word_data.append({
                    'Word': word,
                    'Part of Speech': part_of_speech,
                    'Pronunciation': pronunciation,
                    'Etymology': ', '.join(etymology),
                    'Derived Words': ', '.join(derived_words),
                    'Short Definitions': ', '.join(short_definitions),
                    'Definitions': '\n'.join(definitions) if definitions else 'No detailed definitions found'
                })
        else:
            word_data.append({
                'Word': word,
                'Part of Speech': 'Error',
                'Pronunciation': 'Error',
                'Etymology': 'Error',
                'Derived Words': 'Error',
                'Short Definitions': 'Error',
                'Definitions': 'Error'
            })
    except Exception as e:
        print(f"Error processing word '{word}': {str(e)}")
        word_data.append({
            'Word': word,
            'Part of Speech': 'Exception',
            'Pronunciation': 'Exception',
            'Etymology': 'Exception',
            'Derived Words': 'Exception',
            'Short Definitions': 'Exception',
            'Definitions': 'Exception'
        })

# 创建一个 DataFrame 并将其保存为 Excel 文件
df_output = pd.DataFrame(word_data)
df_output.to_excel('outputDictionary.xlsx', index=False)

print("✅ Data saved to outputDictionary.xlsx")
