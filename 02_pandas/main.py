import pandas as pd

# 1.1
recipes = pd.read_csv('recipes_sample.csv', delimiter=',', parse_dates=['submitted'])
reviews = pd.read_csv('reviews_sample.csv', index_col=0, delimiter=',')
data = pd.merge(recipes, reviews, left_on='id', right_on='recipe_id')
# 1.2

print(f'recipes\nкол-во строк: {recipes.shape[0]}\nкол-во столбцов: {recipes.shape[1]}\nтип данных каждого столбца:\n{recipes.dtypes}')
print(f'\nreviews\nкол-во строк: {reviews.shape[0]}\nкол-во столбцов: {reviews.shape[1]}\nтип данных каждого столбца:\n{reviews.dtypes}')

# 1.3
print(f'\nСумма пустых столбцов:\n{data.isnull().sum(axis=0)}, \nкол-во пустых строк на общую сумму: {len(data.dropna(how="any"))/len(data)}')

# 1.4
print(data[['id', 'minutes', 'contributor_id', 'n_steps', 'n_ingredients']].mean())

# 1.5
print(f'10 случайных названий рецептов:\n{data["name"].sample(n=10)}')

# 1.6
reviews['Unnamed: 0'] = reviews.index

# 1.7

print(recipes[(recipes['minutes'] <= 20) & (recipes['n_ingredients'] <= 5)])

# 2.1
print(pd.to_datetime(recipes['submitted']))

# 2.2
print(recipes[recipes['submitted'] <= pd.to_datetime('2010-12-31')]['submitted'])

# 3.1
recipes['description_length'] = recipes.description.str.len()

# 3.2
recipes['name'] = recipes['name'].str.capitalize()
print(recipes['name'])

# 3.3
recipes['name_word_count'] = recipes['name'].str.split(' ').str.len()
print(recipes['name_word_count'])

# 4.1
print(recipes.groupby('contributor_id').count()['id'])
print(recipes[recipes['contributor_id'] == recipes['contributor_id'].max()])

# 4.2
print(data.groupby('id').mean()['rating'])
print(data.groupby('id').count()[data.groupby('id').count()['description'] == 0])

# 4.3
print(recipes.groupby('submitted').count())


# 5.1
reviews_with_description = reviews[~reviews['review'].isna()]
data_without_na = pd.merge(recipes, reviews_with_description, left_on='id', right_on='recipe_id')[['id', 'name', 'user_id', 'rating']]
print(data_without_na)

# 5.2
data_with_group = pd.merge(recipes, reviews, how='left', left_on='id', right_on='recipe_id')
data_with_review_count = data_with_group.groupby(['recipe_id', 'id']).agg(review_count=pd.NamedAgg(column='review', aggfunc='count'))
print(data_with_review_count)

# 5.3
print(data.groupby('submitted').mean()[data.groupby('submitted').mean()['rating'] == data.groupby('submitted').mean()['rating'].min()][['id', 'rating']])

# 6.1
recipes_by_name_word_count = recipes.sort_values(['name_word_count'], ascending=False)
recipes_by_name_word_count.to_csv('recipes_by_name_word_count.csv')

# 6.2

with pd.ExcelWriter("data_excel.xlsx") as f:
    data_without_na.to_excel(f, sheet_name='file_with_rating')
    data_with_review_count.to_excel(f, sheet_name='file_with_review_count')