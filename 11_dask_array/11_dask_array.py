import dask.array as da
import h5py
import numpy as np
import time
from functools import reduce


def println(*args):
    for console in args:
        print(f'{console.__name__:-^100}')
        print(console(), '\n')


def read_recipe_1(chunk=True):
    return da.from_array(f.get('recipe'), chunks=(100_000, 3) if chunk else 'auto', name=False)


def find_mean_2():
    return read_recipe_1()[:, 1:].mean(axis=0).compute()


def check_chunk_time_3():
    with_chunk_time = time.time()
    read_recipe_1().mean(axis=0)
    with_chunk = f'With chunk: {time.time() - with_chunk_time}'
    without_chunk_time = time.time()
    read_recipe_1(False).mean(axis=0)
    without_chunk = f'Without chunk: {time.time() - without_chunk_time}'
    return with_chunk, without_chunk  # с chunks дольше


def less_then_median_4():
    median = np.quantile(read_recipe_1()[:, 1:2].compute(), 0.5)
    return read_recipe_1()[read_recipe_1()[:, 1:2].flatten() < median].compute()


def n_ingredient_sum_5():
    return read_recipe_1().to_dask_dataframe().groupby(2)[0].count().compute()


def max_duration_6():
    quantile = read_recipe_1().to_dask_dataframe().iloc[:, 1].quantile(0.75).compute()
    return read_recipe_1()[read_recipe_1()[:, 1] < quantile, 1].max().compute()


def most_similar_7():
    preferences = da.array([33, 9]).compute()
    distance = read_recipe_1().to_dask_dataframe().iloc[:, 1:].sub(preferences)
    return read_recipe_1().to_dask_dataframe().loc[
        distance.apply(np.linalg.norm, axis=1, meta='int').idxmin().compute()].compute()


def hdf5_block_mean_8():
    chunks_sum_and_len = ((f_chunks['recipe'][chunk][:, 1].sum(), f_chunks['recipe'][chunk][:, 1].size) for chunk in
                          f_chunks['recipe'].iter_chunks())
    recipe_minutes_sum = reduce(lambda s, l: (s[0] + l[0], s[1] + l[1]), chunks_sum_and_len)
    return recipe_minutes_sum[0] / recipe_minutes_sum[1]


if __name__ == '__main__':
    f = h5py.File('reviewers_full/minutes_n_ingredients_full.hdf5', 'r')
    create_blocks_recipe = h5py.File('reviewers_full/recipe_chunks.hdf5', 'w')
    create_blocks_recipe.create_dataset('recipe', data=f['recipe'], chunks=(100_000, 3))
    f_chunks = h5py.File('reviewers_full/recipe_chunks.hdf5', 'r')
    println(read_recipe_1, find_mean_2, check_chunk_time_3, less_then_median_4, n_ingredient_sum_5, max_duration_6,
            most_similar_7,
            hdf5_block_mean_8)
    f.close()
    f_chunks.close()
