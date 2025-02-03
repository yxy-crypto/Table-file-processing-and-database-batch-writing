from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import reuters
from nltk.corpus import wordnet
# 获取单词 "bank" 的所有同义词
synsets = wordnet.synsets('bank')
print(synsets)

input_text = "what's the best way to split a sentence into words?"
words = word_tokenize(input_text)
files=reuters.fileids(

)
words16097=reuters.words(['test/16097'])
print(words16097)
# 词性标注
tagged_words = pos_tag(words)

print(tagged_words)

