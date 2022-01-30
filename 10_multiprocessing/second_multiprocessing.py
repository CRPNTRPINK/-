from typing import Mapping
from ast import literal_eval
import pandas as pd


def steps_mean_by_tag(name: str) -> Mapping[str, int]:
    recipes = pd.read_csv(f'data/{name}', sep=';', index_col=0, converters={'tags': literal_eval})
    group_by_tag = recipes.explode('tags').groupby('tags')['n_steps'].mean()
    return group_by_tag.to_dict()


if __name__ == '__main__':
    print(steps_mean_by_tag('id_tag_nsteps_1.csv'))
