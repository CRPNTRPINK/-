import time
import xml.etree.ElementTree as ET
from datetime import datetime
import dask
import dask.bag as db
import dask.dataframe as dd

xml_files_path = 'data/'
json_files_path = '../12_dask_bag/data/'


# first example
def read_xml(number: int):  # number - номер файла
    reviewers = ET.parse(f'{xml_files_path}reviewers_full_{number}.xml').getroot()
    return [parse_xml(user) for user in reviewers]


def parse_xml(user):
    user_id = user.find('id').text
    username = getattr(user.find('username'), 'text', None)
    name = getattr(user.find('name'), 'text', None)
    sex = getattr(user.find('sex'), 'text', None)
    country = getattr(user.find('country'), 'text', None)
    mail = getattr(user.find('mail'), 'text', None)
    registered = getattr(user.find('registered'), 'text', None)
    birthdate = getattr(user.find('birthdate'), 'text', None)
    birthdate = datetime.strptime(birthdate, '%Y-%m-%d').date() if birthdate else birthdate
    name_prefix = user.attrib.get('prefix')
    country_code = user.find('country').attrib.get('code') if country else None
    return {'id': user_id, 'username': username, 'name': name, 'sex': sex, 'country': country,
            'mail': mail, 'registered': registered, 'birthdate': birthdate,
            'name_prefix': name_prefix, 'country_code': country_code}


# second example
def compare_simple(repeat):
    read_values_simple = []
    for _ in range(repeat):
        for i in range(5):
            read_values_simple.extend(read_xml(i))


def compare_delayed(repeat):
    read_values_delayed = []
    for _ in range(repeat):
        for i in range(5):
            read_values_delayed.append(dask.delayed(read_xml)(i))
    dask.compute(read_values_delayed, scheduler='processes')


def compare_time(repeat: int = 1):
    now_simple_time = time.time()
    compare_simple(repeat)
    simple_time = f'Время выполнения simple: {time.time() - now_simple_time}'

    now_delayed_time = time.time()
    compare_delayed(repeat)
    delayed_time = f'Время выполнения delayed: {time.time() - now_delayed_time}'
    return f'{simple_time}\n{delayed_time}'


# third example
def add_param(value: dict):
    date = value['birthdate']
    value['birth_year'] = date.year if date else 1981
    value['id'] = int(value['id'])
    return value


def filter_by_birth_year() -> db.Bag:
    users = [dask.delayed(read_xml)(i) for i in range(5)]
    users = db.from_delayed(users) \
        .map(add_param) \
        .filter(lambda u: u['birth_year'] > 1980)
    return users


# fourth example
def bag_to_dataframe() -> dd.DataFrame:
    reviewers_df = filter_by_birth_year().to_dataframe().set_index('id')
    return reviewers_df


# fifth example
def negative_comments():
    # 1533
    reviews = dd.read_json([f'{json_files_path}reviews_{i}.json' for i in range(3)]) \
        .groupby('user_id')['recipe_id'] \
        .count() \
        .rename('negative_comments_count').to_frame()
    reviewers = bag_to_dataframe().merge(reviews, left_index=True, right_index=True)
    return reviewers.head()


if __name__ == '__main__':
    print(f'{"первое задание" :-^100}\n', read_xml(1))
    print(f'{"второе задание" :-^100}\n', compare_time(5))
    # print(f'{"третье задание" :-^100}\n', filter_by_birth_year().take(10))
    # print(f'{"четвертое задание" :-^100}\n', bag_to_dataframe().head())
    # print(f'{"пятое задание" :-^100}\n', negative_comments())
