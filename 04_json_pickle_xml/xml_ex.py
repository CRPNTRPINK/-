from bs4 import BeautifulSoup
import json
import pandas as pd

pd.options.mode.chained_assignment = None

with open('data/steps_sample.xml', 'r') as f:
    data = f.read()

recipes = pd.read_csv('data/recipes_sample.csv')

soup = BeautifulSoup(data, 'lxml')


def ex1(data: BeautifulSoup):
    result = {}
    for recipe in data.find_all('recipe'):
        recipe_id = recipe.find('id').text
        result.setdefault(recipe_id, [])
        for step in recipe.find_all('step'):
            result[recipe_id].append(step.text)
    with open('data/steps_sample.json', 'w') as f:
        json.dump(result, f)


def ex2(data: BeautifulSoup):
    result = {}
    for recipe in data.find_all('recipe'):
        step_len = []
        recipe_id = recipe.find('id').text
        for step in recipe.find_all('step'):
            step_len.append(step.text)
        step_len = len(step_len)
        result.setdefault(step_len, [])
        result[step_len].append(recipe_id)
    print(result)


def ex3(data: BeautifulSoup):
    result = []
    for recipe in data.find_all('recipe'):
        for step in recipe.find_all('step'):
            if step.get('has_minutes') == '1' or step.get('has_hours') == '1':
                result.append(recipe)
                break
    print(result)


def find_n_steps(data: BeautifulSoup):
    result = {'id': [], 'n_steps': []}
    for recipe in data.find_all('recipe'):
        steps = []
        recipe_id = recipe.find('id').text
        result['id'].append(int(recipe_id))
        for step in recipe.find_all('step'):
            steps.append(step.text)
        result['n_steps'].append(len(steps))
    print(result)
    return result


def ex4():
    steps = pd.DataFrame(find_n_steps(soup))
    recipes_steps = pd.merge(recipes, steps, how='left', on='id')
    recipes_steps.loc[recipes_steps['n_steps_x'].isna(), 'n_steps_x'] = \
        recipes_steps[recipes_steps['n_steps_x'].isna()]['n_steps_y']
    print(recipes_steps.drop(columns=['n_steps_y']))


def ex5(data: pd.DataFrame):
    data = data[~data['n_steps'].isna()]
    data['n_steps'] = data['n_steps'].astype(int)
    data.to_csv('reviewers_full/recipes_sample_with_filled_nsteps.csv', sep=',')


# ex1(soup)
# ex2(soup)
# ex3(soup)
# 04_json_pickle_xml()
# 05_csv_hdf_npy(recipes)

def check():
    steps_dict = {'id': [], 'n_steps': []}
    for recipe in soup.find_all('recipe'):
        recipe_id = recipe.find("id").text
        steps_dict['id'].append(int(recipe_id))
        steps_count = len(recipe.find_all("step"))  # счет количества шагов
        steps_dict['n_steps'].append(steps_count)  # запись в словарь id:кол-во шагов
    recipes = pd.read_csv('./data/recipes_sample.csv', sep=',', parse_dates=['submitted'])
    steps_df = pd.DataFrame(steps_dict)
    steps_df_and_recipes = pd.merge(recipes, steps_df, how='left', on='id')
    steps_df_and_recipes.loc['n_steps_x'] = steps_df_and_recipes['n_steps_x'].fillna(steps_df_and_recipes['n_steps_y'])
    steps_df_and_recipes.drop(columns=['n_steps_y'])
    return steps_df_and_recipes

print(check())
