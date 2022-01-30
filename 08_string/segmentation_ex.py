from razdel import sentenize, tokenize
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk import pos_tag
import pandas as pd
import json
from numpy import unique
import re

reviews = pd.read_csv('data/reviews_sample.csv', index_col=0, parse_dates=['date'])


def words(data):
    with open('data/words.json', 'w') as f:
        data = [[word_tokenize(j) for j in sent_tokenize(i)] for i in data['review'].fillna('')]
        json.dump(data, f)


def find_unique():
    result = []
    r = re.compile(r'[a-zа-яё]{3,}')
    with open('data/words.json') as f:
        data = json.load(f)
        for i in data:
            for j in i:
                result.extend(filter(lambda s: r.search(s) is not None, [e.lower() for e in j]))
    return len(unique(result))


def find_longer(data):
    r = re.compile(r'[a-zA-Zа-яёА-ЯЁ]+')
    data = data['review'].fillna('').apply(lambda s: r.findall(s)).explode().groupby(level=0).count().sort_values(
        ascending=False)[:5]
    return data


def tag(text):
    text = pos_tag(word_tokenize(text))
    for i in text:
        print(f'{i[1]:^10}', end=' ')
    print()
    for i in text:
        print(f'{i[0]:^10}', end=' ')
    print()

"""
перед началом работы раскомментируйте вызов функции words, а после исполнения закомментируйте,
функция создаст json файл с необходимыми данными, что позволит ускорить процесс чтения.
"""
# words(reviews)
# print(find_unique())
# print(find_longer(reviews))
# tag('I omitted the raspberries and added strawberries instead')
# tag(reviews['review'][0:1].to_string(index=False))
# print(len(f'{"h":^10}'), f'{"h":^10}')
