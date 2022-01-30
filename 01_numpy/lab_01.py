import numpy as np

# 1
print('Задание 1')
data = np.array(np.loadtxt('minutes_n_ingredients.csv', delimiter=',', skiprows=1, dtype=np.int32))
print(f'Вывести первые 5 строк: \n{data[:5]}')

# 2
print('\nЗадание 2')
data_skip_one = data[:, 1:]
data_min = np.min(data_skip_one, axis=0)
data_mean = np.mean(data_skip_one, axis=0)
data_max = np.max(data_skip_one, axis=0)
data_median = np.median(data_skip_one, axis=0)
print(f'Минимум: \n{data_min},\nмаксимум: \n{data_max},\nмедиана: {data_median},\nсреднее: {data_mean}')

# 3
print('\nЗадание 3')
x = data[data[:, 1] <= np.quantile(data[:, 1], 0.75)]
print(x)

# 4
print('\nЗадание 4')
data_zero_len = data[data[:, 1] == 0]
print(f'Кол-во: {len(data_zero_len)}')
data_zero_len[:, 1] = 1
print(f'Измененный список:\n{data_zero_len}')

# 5
print('\nЗадание 5')
data_unique = np.unique(data[:, 0])
print(len(data_unique))

# 6
print('\nЗадание 6')
data_ingredients_unique = np.unique(data[:, 2])
print(data_ingredients_unique.shape[0])
print(data_ingredients_unique)

# 7
print('\nЗадание 7')
data_max_five_ingredients = data[data[:, 2] <= 5]
print(data_max_five_ingredients)

# 8
print('\nЗадание 8')
data_without_time_null = data[data[:, 1] != 0]
data_mean_ingredients = data_without_time_null[:, 2]
data_time_mean_ingredients = data_mean_ingredients / data_without_time_null[:, 1]
data_time_mean_ingredients_max = data_time_mean_ingredients.max()
print(data_mean_ingredients)
print(data_time_mean_ingredients)
print(data_time_mean_ingredients_max)

# 9
print('\nЗадание 9')
data_top_100_time_ingredients = data[np.isin(data[:, 1], np.sort(data[:, 1])[-100:])][-100:, 2].mean()
print(data_top_100_time_ingredients)

# 10
print('\nЗадание 10')
data_10_random_choice = data[np.random.randint(0, len(data), 10)]
print(data_10_random_choice)

# 11
print('\nЗадание 11')
data_percent_mean = (len(data[data[:, 2] < data[:, 2].mean()]) / (len(data)) * 100)
print(f'{round(data_percent_mean, 1)}')

# 12
print('\nЗадание 12')
data_boolean = (data[:, 1] <= 20) * (data[:, 2] <= 5)
easy = np.insert(data[data_boolean], 3, 1, axis=1)
hard = np.insert(data[~data_boolean], 3, 0, axis=1)
data_new = np.concatenate((easy, hard), axis=0)
print(easy)
print(hard)
print(data_new)

# 13
print('\nЗадание 13')
print((len(easy) / len(data_new)) * 100)

# 14
print('\nЗадание 14')
data_short = data[data[:, 1] < 10]
data_standard = data[(data[:, 1] > 10) & (data[:, 1] < 20)]
data_long = data[data[:, 1] > 20]
data_len = np.array([data_short.shape[0], data_standard.shape[0], data_long.shape[0]]).min()
data_3d = np.array([data_short[:data_len], data_standard[:data_len], data_long[:data_len]])
print(data_3d)