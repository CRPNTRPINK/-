import csv
import json


# 1


def one() -> dict:
    result_one = {}
    with open('data/tags_sample.csv', 'r') as f:
        read_csv = csv.DictReader(f, delimiter=',')
        for i in read_csv:
            result_one.setdefault(i['id'], [])
            result_one[i['id']].append(i['tag'])

    # with open('reviewers_full/tags_sample.json', 'w') as f:
    #     json.dump(result_one, f)  # запись
    return result_one


# 2
def two() -> list:
    result_one = one()
    result_two = []
    with open('../04_json_pickle_xml/data/recipes_sample_with_filled_nsteps.csv', 'r') as f:
        read_csv = csv.DictReader(f, delimiter=',')
        for i in read_csv:
            if result_one.get(i['']) is not None:
                i['n_tags'] = len(result_one[i['']])
                i['tags'] = ';'.join(result_one[i['']])
                result_two.append(i)
            else:
                result_two.append(i)
    return result_two


# 3
def three() -> dict:
    result_three = {}
    with open('./data/ingredients_sample.csv', 'r') as f:
        read_csv = csv.DictReader(f, delimiter=',')

        for i in read_csv:
            result_three.setdefault(i['recipe_id'], [])
            result_three[i['recipe_id']].append(i['ingredient'])
    return result_three


# 4
def four() -> list:
    result_two = two()
    result_three = three()
    result_four = []

    for i in result_two:
        if result_three.get(i['']) is not None:
            i['ingredients'] = '*'.join(result_three[i['']])
            if i['n_ingredients'] == '':
                i['n_ingredients'] = str(len(result_three[i['']]))
            result_four.append(i)
        else:
            result_four.append(i)
    return result_four


def five(result_four, save_as_file=False):
    def do_example(result_four):
        before_dot = result_four['n_ingredients'][:result_four['n_ingredients'].find('.')]
        if before_dot.isdigit():
            result_four['n_ingredients'] = int(before_dot)
        return result_four

    result_five = list(map(do_example, result_four))
    print(result_five)
    fieldnames = ['', 'name', 'id', 'minutes', 'contributor_id', 'submitted', 'n_steps', 'description', 'n_ingredients',
                  'n_tags', 'tags', 'ingredients']

    if save_as_file:
        with open('./data/recipes_sample_with_tags_ingredients.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()
            for i in result_five:
                writer.writerow(i)
    return result_four


print(five(four()))
