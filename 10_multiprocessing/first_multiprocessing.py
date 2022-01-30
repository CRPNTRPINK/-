import time
import numpy as np
import pandas as pd
import multiprocessing
from tqdm import tqdm


def split_df_into_8() -> None:
    start_time = time.time()
    recipes_full = pd.read_csv('data/recipes_full.csv')
    dfs_array = np.array_split(recipes_full, 8)
    for i, df in enumerate(dfs_array):
        df[['id', 'tags', 'n_steps']].to_csv(f'data13/id_tag_nsteps_{i + 1}.csv', sep=';')
    print('completed', time.time() - start_time)


if __name__ == '__main__':
    split_df_into_8()
