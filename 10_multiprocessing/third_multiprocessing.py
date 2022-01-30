from ast import literal_eval
import pandas as pd
from typing import Mapping
import numpy as np
from functools import reduce
import os
import time


def steps_mean_by_tag(name: str, sep: str = ';') -> Mapping[str, int]:
    recipes = pd.read_csv(f'data/{name}', sep=sep, converters={'tags': literal_eval}, index_col=0)
    group_by_tag = recipes.explode('tags').groupby('tags')['n_steps'].mean()
    return group_by_tag


def steps_mean_by_tag_all() -> Mapping[str, int]:
    start = time.time()
    listdir = list(filter(lambda x: x.startswith('id_tag_nsteps'), os.listdir('data')))
    mean_array = [steps_mean_by_tag(name) for name in listdir]
    print('completed', time.time() - start)
    return np.divide(reduce(lambda x, y: x.add(y), mean_array), len(mean_array)).to_dict()


if __name__ == '__main__':
    print(steps_mean_by_tag_all())
