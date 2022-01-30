import pandas as pd
import xlwings as xw
from ex7.xlwings_functions import find_last_for_new, find_last, color

# task 1
recipes_sample_df = pd.read_csv('data/recipes_sample_with_tags_ingredients.csv', index_col=0, parse_dates=['submitted'])
reviews_sample_df = pd.read_csv('data/reviews_sample.csv', index_col=0, parse_dates=['date'])


# task 2
def excel_writer():
    with pd.ExcelWriter('data/recipes.xlsx') as w:
        reviews_sample_df.sample(frac=0.05).to_excel(w, sheet_name='reviews', index=False)
        recipes_sample_df.sample(frac=0.05).to_excel(w, sheet_name='recipes', index=False)


# excel reader
excel_writer()
book = xw.Book('reviewers_full/recipes.xlsx')
reviews_sheet = book.sheets['reviews']
recipes_sheet = book.sheets['recipes']


# task 3
def task3():
    seconds_assign = (recipes_sheet.range('C1').options(pd.DataFrame, expand='down', index=False).value * 60).rename(
        columns={'minutes': 'seconds_assign'})
    recipes_last_column = find_last_for_new(recipes_sheet, 'column')
    recipes_sheet.range((1, recipes_last_column)).options(index=False).value = seconds_assign


# task 4
def task4():
    recipes_sheet.range((1, find_last_for_new(recipes_sheet, 'column'))).value = 'seconds_assign_formula'
    recipes_sheet.range((2, find_last(recipes_sheet, 'column')), (
        find_last(recipes_sheet, 'row', expand='down'), find_last(recipes_sheet, 'column'))).formula = '=C2*60'


# task 5
def task5():
    recipes_sheet.range((1, find_last_for_new(recipes_sheet, 'column'))).value = 'n_reviews'
    recipes_sheet.range((2, find_last(recipes_sheet, 'column')), (
        find_last(recipes_sheet, 'row', expand='down'),
        find_last(recipes_sheet, 'column'))).formula = '=COUNTIF(D:D,D2)'


# task 6
def task6():
    recipes_sheet.range('A1').expand('right').api.font_object.font_style.set('bold')


# task 7
def task7():
    color(recipes_sheet, 'C2')


# task 8
def validate():
    start = 0
    recipe_id = recipes_sheet.range('D2').expand('down').value
    reviews_recipe_id = reviews_sheet.range('B2').expand('down').value
    rating_reviews = reviews_sheet.range('D2').expand('down').value
    rows_len = len(reviews_recipe_id)
    for i in range(rows_len):
        id, rating = reviews_recipe_id[i], rating_reviews[i]
        i += 1
        if ((id not in recipe_id) or ((0 <= rating <= 5) is not True)) and start == 0:
            start = i
        elif (id in recipe_id) and (0 <= rating <= 5):
            reviews_sheet.range(f'A{start}:A{i - 1}').expand('right').color = (255, 0, 0)
            start = 0
        elif (rows_len == i) and start != 0:
            reviews_sheet.range(f'A{start}:A{i}').expand('right').color = (255, 0, 0)


if __name__ == '__main__':
    # task3()
    # task4()
    # task5()
    # task6()
    # task7()
    validate()
