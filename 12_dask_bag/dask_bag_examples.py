import dask.bag as db
import json
import re

path = 'data'


def get_reviews_1():
    return db.read_text(f'{path}/*.json').map(json.loads)


def update_dict(obj: dict, key: str, value):
    obj.update({key: value})
    return obj


def get_reviews_rating_2() -> db:
    return db.read_text(f'{path}/*.json', include_path=True) \
        .map(lambda v: update_dict(json.loads(v[0]), 'rating', int(re.search(r'\d\.json', v[1]).group().split('.')[0])))


def get_reviews_count_3() -> db:
    return get_reviews_rating_2().count().compute()


def filter_by_date_4() -> db:
    return get_reviews_rating_2() \
        .filter(lambda v: v['date']
                .split('-')[0] in ('2014', '2015'))


def preprocessing(obj):
    obj['review'] = ''.join(re.findall(r'[a-zA-Z ]+', obj['review'].lower().strip()))
    return obj


def preprocessing_reviews_5() -> db:
    return filter_by_date_4().map(lambda v: preprocessing(v))


def count_preprocessing_reviews_6() -> db:
    return preprocessing_reviews_5().count()


def frequencies_rating() -> db:
    return preprocessing_reviews_5().map(lambda v: v['rating']).frequencies()


def mean_rating() -> db:
    return preprocessing_reviews_5().map(lambda v: v['rating']).mean()


def get_max_len(v, k):
    return max((v, k), key=lambda x: x[1])


def max_len_by_rating() -> db:
    return preprocessing_reviews_5(). \
        map(lambda x: (x['rating'], len(x['review']))). \
        foldby(lambda k: k[0], get_max_len)


if __name__ == '__main__':
    print(f'{"первое задание" :-^100}\n', get_reviews_1().take(5))
    print(f'{"второе задание" :-^100}\n', get_reviews_rating_2().take(5))
    print(f'{"третье задание" :-^100}\n', get_reviews_count_3())
    print(f'{"четвертое задание" :-^100}\n', filter_by_date_4().take(5))
    print(f'{"пятое задание" :-^100}\n', preprocessing_reviews_5().take(5))
    print(f'{"шестое задание" :-^100}\n', count_preprocessing_reviews_6().compute())
    print(f'{"седьмое задание" :-^100}\n', frequencies_rating().compute())
    print(f'{"восьмое задание" :-^100}\n', mean_rating().compute())
    print(f'{"девятое задание" :-^100}\n', max_len_by_rating().compute())
