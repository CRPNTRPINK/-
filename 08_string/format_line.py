import pandas as pd

recipes = pd.read_csv('data/recipes_sample_with_tags_ingredients.csv', index_col=0)
pd.set_option('display.max_colwidth', None)


def random_lines(data: pd.DataFrame, lines: int = 5) -> str:
    data = data.sample(n=lines)
    result = f'|{"id":^15}|{"n_in":^10}|\n'
    result += '|' + '-' * (len(result) - 3) + '|'
    for index, row in data[['id', 'n_ingredients']].iterrows():
        result += f'\n|{row["id"]:^15}|{row["n_ingredients"]:^10}|'
    return result


def step_teg(data: pd.DataFrame, id_: int) -> str:
    data = data[recipes['id'] == id_]
    if data.empty:
        return "данных с таким id не существует"
    result = f'"{data["name"].to_string(index=False).capitalize()}"\n'
    for i, step in enumerate(data['ingredients'].to_string(index=False).split('*')):
        result += f'\n{i + 1}. {step.capitalize()}.'
    result += '\n' + '-' * 30
    result += f'\n#{data["tags"].to_string(index=False).replace(";", " #")}'

    return result


if __name__ == '__main__':
    print(random_lines(recipes))
    print()
    print(step_teg(recipes, 155430))
