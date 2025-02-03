from bs4 import BeautifulSoup
import pandas as pd
import re


def parse_mdx(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    entries = []

    # 遍历每个词性部分
    for part in soup.find_all(class_="cixing_part"):
        # 基础信息提取
        h_tag = part.find('h')
        lemma = h_tag.text.split('<')[0].strip('· ') if h_tag else ""
        pos_tag = part.find('pos')
        pos = pos_tag.text if pos_tag else ""

        # 遍历每个义项
        for sn_blk in part.find_all(class_=["sn-blk", "sn-blk-nolist"]):
            entry = {
                "lemma": lemma,
                "POS": pos,
                "grammarTag": "",
                "explanation": "",
                "collocation": "",
                "registerTag": "",
                "collocationTag": "",
                "SYN": "",
                "Ant": ""
            }

            # 语法标记
            gram = sn_blk.find('gram-g')
            if gram:
                entry["grammarTag"] = gram.get_text(strip=True).replace('[', '').replace(']', '')

            # 语域标记
            label = sn_blk.find(['subj', 'reg'])
            if label:
                entry["registerTag"] = label.get_text(strip=True)

            # 搭配用法
            cf = sn_blk.find('cf')
            if cf:
                coll_text = cf.get_text(" ", strip=True).replace('~', lemma)
                entry["collocation"] = re.sub(r'\(.*?\)', '', coll_text)

                # 判断搭配类型
                parent_text = cf.find_parent().text.lower()
                if 'phrasal verb' in parent_text:
                    entry["collocationTag"] = "phrasal verb"

            # 汉语释义
            chn = sn_blk.find('chn')
            if chn:
                entry["explanation"] = chn.get_text(strip=True).split('>')[-1]

            # 近义词处理
            syn_box = sn_blk.find('unbox', type="synonyms")
            if syn_box:
                synonyms = [li.text for li in syn_box.find_all('und')]
                entry["SYN"] = "; ".join(synonyms)

            # 习语标记
            if sn_blk.find('idm-g'):
                entry["collocationTag"] = "idiom"

            entries.append(entry)

    return pd.DataFrame(entries)


# 使用示例
with open("新建文本文档.txt", "r", encoding="utf-8") as f:
    html_content = f.read()

df = parse_mdx(html_content)

# 后处理
df['POS'] = df['POS'].str.extract(r'(noun|adj|verb)', flags=re.I)
df['grammarTag'] = df['grammarTag'].str.replace(r'[\[\]]', '', regex=True)

# 导出Excel
df.to_excel("dictionary_output.xlsx", index=False,
            columns=["lemma", "POS", "grammarTag", "explanation",
                     "collocation", "registerTag", "collocationTag", "SYN", "Ant"])