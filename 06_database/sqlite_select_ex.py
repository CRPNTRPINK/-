import sqlite3
from ex6.sqlite_commands import select, close

connect = sqlite3.connect("recipes.db")
cursor = connect.cursor()

# task 5
execute = """
    SELECT *
    FROM recipe
    WHERE CAST(n_ingredients AS INTEGER) = ?
    LIMIT 10
"""

# print(select(cursor, execute, (10,)))

# task 6

execute = """
    SELECT name
    FROM recipe
    WHERE minutes = (SELECT MAX(minutes)
                    FROM recipe)
"""

# print(select(cursor, execute))

# task 7

execute = """
    SELECT rec.id, name, minutes, submitted, description, n_ingredients
    FROM recipe rec INNER JOIN review rev ON rec.id = rev.recipe_id
    WHERE rev.user_id = ?
"""

# print(select(cursor, execute, (21752,), "Нет данных"))

# task 8

execute = """
    SELECT COUNT(*)
    FROM review
    WHERE CAST(rating AS INTEGER) = ?
"""

# print(select(cursor, execute, (5,)))

# task 9
execute = """
    SELECT COUNT(*)
    FROM (SELECT rec.id
        FROM recipe rec INNER JOIN review rev ON rec.id = rev.recipe_id
        GROUP BY rec.id HAVING min(rating) >= ?) e
"""

print(select(cursor, execute, (4,)))

# task 10

execute = """
    SELECT COUNT (*)
    FROM recipe
    WHERE strftime('%Y', submitted) = ? AND CAST(recipe.minutes AS INTEGER) >= ?
"""

# print(select(cursor, execute, ('2010', '15')))

# task 11

execute = """
    SELECT rec.id, rec.name, rev.user_id, rev.date, rev.rating
    FROM recipe rec INNER JOIN review rev ON rec.id = rev.recipe_id
    WHERE CAST(rec.n_ingredients AS INTEGER) >= ?
    ORDER BY rev.recipe_id
"""

# print(select(cursor, execute, (3,)))

print(close(connect))
