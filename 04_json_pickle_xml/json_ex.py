import json
from collections import Counter
import pandas as pd

with open('data/contributors_sample.json', 'r') as f:
    data = json.load(f)


def ex1(data):
    three_persons = data[0:3]
    print('Задание 1')
    for person in three_persons:
        for detail in person:
            print(f'{detail}: {person[detail]}')
        print("-" * 30)


def ex2(data):
    print('Задание 2')
    mails = set()
    for mail in data:
        mails.add(mail['mail'])
    print(mails)


def ex3(data, name):
    print('Задание 3')
    flag = False
    for person in data:
        if person['username'].lower() == name.lower():
            flag = True
            for details in person:
                print(f'{details}: {person[details]}')
        if flag:
            break
    if flag is False:
        raise ValueError


def ex4(data):
    print('Задание 4')
    count = Counter()
    for sex in data:
        count[sex['sex']] += 1
    print(count)


def ex5(data):
    print('Задание 5')
    data_pd = pd.DataFrame(data, columns=['id', 'username', 'sex'])
    print(data_pd)


def ex6():
    print('Задание 6')
    recipes = pd.read_csv('data/recipes_sample.csv', delimiter=',')
    persons = pd.DataFrame(data, columns=['id', 'username', 'sex'])
    recipes_contributors = pd.merge(recipes, persons, how='left', on='id')
    print(f'нет для: {persons.shape[0] - recipes_contributors.dropna(subset=["username"]).shape[0]}')


ex1(data)
ex2(data)
ex3(data, 'sheilaadams')
ex4(data)
ex5(data)
ex6()
