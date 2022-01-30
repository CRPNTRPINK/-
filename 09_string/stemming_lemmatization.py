from editing_distance import unique_words
from nltk.stem import SnowballStemmer, WordNetLemmatizer
from nltk import pos_tag
from nltk import word_tokenize
from nltk.corpus import wordnet, stopwords
import pandas as pd
import json
import nltk
from collections import Counter
snb_stemmer_en = SnowballStemmer('english')
lemmatizer = WordNetLemmatizer()
stop_words_en = stopwords.words('english')


# запишу данные 1.1 в json, чтобы ускорить выполнение кода
def write_unique_words_to_json_file():
    with open('data/unique_words.json', 'w') as f:
        json.dump(unique_words().tolist(), f)


# чтение данных из json файла
def u_words():
    with open('data/unique_words.json', 'r') as f:
        data = json.load(f)
    return data


def wordnet_get(word):
    tag = pos_tag(u_words())[0][1][0]
    tags = {
        'J': wordnet.ADJ,
        'N': wordnet.NOUN,
        'V': wordnet.VERB,
        'R': wordnet.ADV
    }
    return tags.get(tag, wordnet.NOUN)


# 1
def df_stem_lemm():
    stem = [snb_stemmer_en.stem(i) for i in u_words()]
    lemm_output = [lemmatizer.lemmatize(i) for i in u_words()]
    df = pd.DataFrame(data=zip(stem, lemm_output), columns=['stemmed_word', 'normalized_word'], index=u_words())
    return df


# 2
def delete_stop_word_recipe_description():
    recipes = pd.read_csv('data/recipes_sample.csv')
    recipe_description = recipes['description'].apply(lambda w: ''.join(
        f' {word}' if word not in ['.', ',', '?', '!'] else word for word in word_tokenize(str(w)) if
        word not in stop_words_en))
    return recipe_description


# 3
def get_top_10_words(recipes_description):
    top_10_words = Counter()
    for i in recipes_description.str.replace(f'[.,?"!()\d``:\']+', '', regex=True):
        top_10_words.update(word_tokenize(i))
    items = list(top_10_words.items())
    items.sort(key=lambda w: w[1], reverse=True)
    return items[:10]


if __name__ == '__main__':
    print(df_stem_lemm())
    # print(delete_stop_word_recipe_description())
    # print(get_top_10_words(delete_stop_word_recipe_description()))
