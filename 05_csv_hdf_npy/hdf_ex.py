import h5py
import os, sys


def one(path) -> dict:
    with h5py.File(path, 'r') as f:
        data = {}
        for i in f:
            dataset = f[i]
            shape = dataset.shape
            metadata = dict(dataset.attrs)
            print(f'Dataset name={str(dataset).split()[2]}, dataset size={shape[0]}, metadata={metadata["col_1"]}')
            data.setdefault(i, {'dataset': dataset[:], 'shape': shape, 'metadata': metadata})

    return data


# 3.2 Разбейте каждый из имеющихся датасетов на две части: 1 часть содержит только те строки,
# где PDV (Percent Daily Value) превышает 100%; 2 часть содержит те строки, где PDV не составляет не более 100%.
# Создайте 2 группы в файле и разместите в них соответствующие части датасета c сохранением метаданных исходных датасетов.
# Итого должно получиться 2 группы, содержащие несколько датасетов. Сохраните результаты в файл nutrition_grouped.h5


def two(one) -> dict:
    filtered_data = {'i[1] > 100': {}, 'i[1] <= 100': {}}
    for checker in filtered_data.keys():
        for data in one:
            for i in one[data]['dataset']:
                if 'PDV' in one[data]['metadata']['col_1'] and eval(checker):
                    filtered_data[checker].setdefault(data, one[data])

    return filtered_data


def four(filtered_data) -> None:
    with h5py.File('reviewers_full/archive.h5', 'w') as f:
        for k in filtered_data:
            h5 = f.create_group(k)
            for kk in filtered_data[k]:
                h5.create_dataset(name=kk, data=filtered_data[k][kk]['dataset']).attrs.update(
                    filtered_data[k][kk]['metadata'])


# one('reviewers_full/nutrition_sample.h5')
# one('reviewers_full/archive.h5')

four(two(one('data/nutrition_sample.h5')))
size_nutrition_sample = os.path.getsize('data/nutrition_sample.h5')
size_archive = os.path.getsize('data/archive.h5')
print(size_nutrition_sample, size_archive)
