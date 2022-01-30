from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
from scipy.spatial.distance import cosine

recipes = pd.read_csv('data/recipes_sample.csv')
recipes_random_ten_name = recipes.sample(5)
recipes_random_ten = recipes_random_ten_name['description'].dropna().tolist()
pd.set_option('display.max_columns', None)


# 1
def text_vector():
    tv = TfidfVectorizer(stop_words='english')
    corpus_tv = tv.fit_transform(recipes_random_ten)
    tv_arr = corpus_tv.toarray()
    return tv_arr


# 2
def find_cosine_distance():
    result = []
    columns_index = []
    for i in range(len(text_vector())):
        result.append([])
        columns_index.append(recipes_random_ten_name.iloc[i, 0])
        for j in range(len(text_vector())):
            result[i].append(cosine(text_vector()[i], text_vector()[j]))
    df_cosine_distance = pd.DataFrame(data=result, columns=columns_index, index=columns_index)
    return df_cosine_distance


# 3
def get_min():
    min_value = find_cosine_distance()[~find_cosine_distance().isin([0])]
    min_value = find_cosine_distance()[find_cosine_distance().isin(min_value.min(axis=0))]
    # min_value.reset_index(inplace=True, drop=True)
    return min_value


if __name__ == '__main__':
    print(text_vector())
    print(find_cosine_distance())
    print(get_min())
    print('Наиболее схожие слова те, чье значение ближе к 0')
