import dask.dataframe as dd
import dask.bag as db
import json
import re
from datetime import datetime

bag_path = '../12_dask_bag/data'
df_path = 'data'


def read_csv_1() -> dd.DataFrame:
    return dd.read_csv(f'{df_path}/recipes_full_*.csv', parse_dates=['submitted'], assume_missing=True)


def metadata_2():
    return f'Партиции: {read_csv_1().npartitions}\nтипы данных:\n{read_csv_1().dtypes}'


def first_five_lst_five_3():
    # возникла ошибка из-за неопределенности типа данных, исправить можно было, указав типы данных,
    # либо assume_missing=True - дефолтная типизация float
    return f'первые 5:\n{read_csv_1().head(5)}\nпоследние 5:\n{read_csv_1().tail(5)}'


def partitions_size_4():
    return read_csv_1().map_partitions(len).compute()


def n_steps_max_5():
    # Логика работы dask
    # 1. чтение каждого csv
    # 2. извлечение значений
    # 3. поиск максимума в каждой партиции
    # 4. агрегация максимумов каждой партиции и поиск максимума среди них
    n_steps = read_csv_1()['n_steps'].max(axis=0)
    n_steps.visualize()
    return n_steps.compute()


def group_by_month_count_6():
    return read_csv_1().groupby(by=read_csv_1()['submitted'].dt.month).agg({'id': 'count'}).rename(
        columns={'id': 'recipes_count'})


def update_dict(obj: dict, key: str, value):
    obj.update({key: value})
    return obj


def get_reviews_rating() -> db:
    return db.read_text(f'{bag_path}/*.json', include_path=True) \
        .map(lambda v: update_dict(json.loads(v[0]), 'rating', int(re.search(r'\d\.json', v[1]).group().split('.')[0])))


def mean_rating_7():
    def count(x, v):
        return x[0], x[1] + v[1], x[2] + v[2]

    return get_reviews_rating() \
        .map(lambda v: (datetime.strptime(v['date'], '%Y-%m-%d').date().month, v['rating'], 1)) \
        .foldby(lambda x: x[0], count).map(lambda x: {'date': x[0], 'mean_rating': x[1][1] / x[1][2]})


def agg_8():
    mean = mean_rating_7().to_dataframe().set_index(
        ['date'], sorted=True)
    return group_by_month_count_6().merge(mean, left_index=True, right_index=True)


if __name__ == '__main__':
    print(f'{"первое задание" :-^100}\n', read_csv_1().head())
    print(f'{"второе задание" :-^100}\n', metadata_2())
    print(f'{"третье задание" :-^100}\n', first_five_lst_five_3())
    print(f'{"четвертое задание" :-^100}\n', partitions_size_4())
    print(f'{"пятое задание" :-^100}\n', n_steps_max_5())
    print(f'{"шестое задание" :-^100}\n', group_by_month_count_6().compute())
    print(f'{"седьмое задание" :-^100}\n', mean_rating_7().compute())
    print(f'{"восьмое задание" :-^100}\n', agg_8().compute())
