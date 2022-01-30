import multiprocessing
import os
import time
from ast import literal_eval
from functools import reduce
from typing import Optional
import numpy as np
import pandas as pd


def steps_mean_by_tag(name: Optional[str] = None, queue=None, sep: str = ';', mean_array=None):
    if mean_array:
        return np.divide(reduce(lambda x, y: x.add(y), mean_array), len(mean_array)).to_dict()
    recipes = pd.read_csv(f'data/{name}', sep=sep, converters={'tags': literal_eval}, index_col=0)
    queue.put(recipes.explode('tags').groupby('tags')['n_steps'].mean())


def steps_mean_by_tag_all() -> steps_mean_by_tag:
    listdir = list(filter(lambda x: x.startswith('id_tag_nsteps'), os.listdir('data')))
    queue = multiprocessing.Queue()
    pool = [multiprocessing.Process(target=steps_mean_by_tag, args=(name, queue)) for name in listdir]
    for i in pool:
        i.start()
    mean_result = steps_mean_by_tag(mean_array=[queue.get() for _ in range(len(pool))])
    return mean_result


if __name__ == '__main__':
    time_start = time.time()
    print(steps_mean_by_tag_all())
    print('Completed', time.time() - time_start)
