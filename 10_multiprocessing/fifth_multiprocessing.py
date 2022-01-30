import multiprocessing
import os
import time
from ast import literal_eval
from functools import reduce
from typing import Mapping
from typing import Optional
import numpy as np
import pandas as pd


def filenames(queue: multiprocessing.Queue, stop: int = 4):
    for name in list(filter(lambda x: x.startswith('id_tag_nsteps'), os.listdir('data')))[:stop]:
        queue.put(name)


def steps_mean_by_tag(name: Optional[str] = None, sep: str = ';', mean_array=None):
    if mean_array:
        return np.divide(reduce(lambda x, y: x.add(y), mean_array), len(mean_array)).to_dict()
    recipes = pd.read_csv(f'data/{name}', sep=sep, converters={'tags': literal_eval}, index_col=0)
    return recipes.explode('tags').groupby('tags')['n_steps'].mean()


def steps_mean_by_tag_all(queue: multiprocessing.Queue) -> Mapping[str, int]:
    pool = multiprocessing.Pool(processes=4)
    processes = pool.map(steps_mean_by_tag, [queue.get() for _ in range(8) if queue.empty() is False])
    return steps_mean_by_tag(mean_array=processes)


if __name__ == '__main__':
    queue_filenames = multiprocessing.Queue()
    start_time = time.time()
    filenames(queue_filenames, stop=8)
    print(steps_mean_by_tag_all(queue_filenames))
    print('Completed', time.time() - start_time)
