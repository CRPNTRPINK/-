import numpy as np
import pandas as pd

# 1
recipes_with_tags_ingredients = pd.read_csv('data/recipes_sample_with_tags_ingredients.csv', delimiter=',',
                                            parse_dates=['submitted'], index_col=0)
recipes_lt_2010 = recipes_with_tags_ingredients[
    recipes_with_tags_ingredients['submitted'] < '2010-01-01'].dropna().to_numpy()
recipes_gte_2010 = recipes_with_tags_ingredients[
    recipes_with_tags_ingredients['submitted'] > '2010-01-01'].dropna().to_numpy()

# 2
np.savez('data/array_recipes_sample_with_tags_ingredients.npz', recipes_array_lt_2010=recipes_lt_2010,
         recipes_array_gte_2010=recipes_gte_2010)

# 3
with open('data/array_recipes_sample_with_tags_ingredients.npz', 'rb') as f:
    data = np.load(f, allow_pickle=True)
    print(data['recipes_array_lt_2010'])
    print(data['recipes_array_gte_2010'])
