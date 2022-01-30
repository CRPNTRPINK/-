from nltk import word_tokenize, edit_distance
import pandas as pd
import numpy as np


# 1
def unique_words():
    preprocessed = pd.read_csv('data/preprocessed_descriptions.csv')
    words = preprocessed['preprocessed_descriptions'].fillna('').apply(
        lambda w: word_tokenize(w)).explode().str.replace(f'[\d+\s+ ]', '', regex=True).dropna().unique()
    return words


# 2
def random_words_distance():
    five_random_words = np.random.choice(unique_words(), 5)
    five_random_words_distance = [[f'Расстояние между {i} и {j} = {edit_distance(i, j)}' for j in five_random_words] for
                                  i
                                  in five_random_words]
    return five_random_words_distance


# 3
def find_min_distance_words(word, stop):
    distance = [(edit_distance(word, i), i) for i in unique_words()]
    distance.sort(key=lambda x: x[0])
    return distance[:stop]


if __name__ == '__main__':
    print(unique_words())
    print(random_words_distance())
    print(find_min_distance_words('neil', 7))
