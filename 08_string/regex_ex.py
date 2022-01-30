import re
import pandas as pd
from ex8.format_line import step_teg

reviews = pd.read_csv('data/reviews_sample.csv', index_col=0)
recipes = pd.read_csv('data/recipes_sample_with_tags_ingredients.csv', index_col=0)
reviews = reviews.fillna({'review': ''})
pd.set_option('display.max_colwidth', None)


def count_d_review(data):
    data = data['review']
    return data.apply(lambda c: re.compile(r'\d').search(c) is not None).sum()


print(count_d_review(reviews))


def find_smile(data):
    data = data['review']
    r = re.compile(r'([=:]-[)(])|([)(]-[=:])')
    smile = data.apply(lambda s: r.search(s))
    return smile[~smile.isna()].apply(lambda s: s.group())


print(find_smile(reviews))


def check_date(data, pd=True):
    r = re.compile(r'[12][09][0-9][0-9]-(([0][0-9])|([1][0-2]))-(([0-2][0-9])|([3][01]))')
    if pd:
        data = data['date']
        checked = data.apply(lambda d: r.search(d))
        return f'Кол-во не совпадений: {checked.isna().sum()}\nсовпадения:\n{checked[~checked.isna()].apply(lambda s: s.group())}'
    result = r.search(data)
    return result.group() if result is not None else 'дата не прошла проверку'


print(check_date(reviews))
print(check_date('21-12-2021', pd=False))


def find_first_str(data):
    r = re.compile(r'[\d.]+ \w+ ')
    print(step_teg(recipes, 155430))
    tag_step = step_teg(data, 155430)
    return r.findall(tag_step)


print(find_first_str(recipes))


def preprocessing(data):
    r = re.compile(r'[a-zA-Z\d ]+')
    name = data.loc[:, ['name']]
    name['preprocessed_descriptions'] = data['description'].fillna('').apply(lambda s: ''.join(r.findall(s)))
    name.to_csv('reviewers_full/preprocessed_descriptions.csv', index=False)
    return name


print(preprocessing(recipes))