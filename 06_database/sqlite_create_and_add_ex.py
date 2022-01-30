import sqlite3
import csv
from ex6.sqlite_commands import create_table, insert
import datetime

connect = sqlite3.connect("recipes.db")
cursor = connect.cursor()

second_task = """
    CREATE TABLE recipe (
        id integer primary key,
        name varchar,
        minutes varchar (5),
        submitted date,
        description text,
        n_ingredients integer,
        foreign key (id) references review (user_id)
    )
    """

third_task = """
    CREATE TABLE review (
        id integer primary key,
        user_id integer,
        recipe_id integer,
        date date,
        rating integer,
        review text
    )
"""

fourth_task_review = """
    INSERT INTO review(user_id, recipe_id, date, rating, review) VALUES (
            ?, ?, ?, ?, ?
    )
"""

fourth_task_recipe = """
    INSERT INTO recipe(id, name, minutes, submitted, description, n_ingredients) VALUES (
            ?, ?, ?, ?, ?, ?
    )
"""

create_table(cursor, connect, third_task)
create_table(cursor, connect, second_task)

with open("06_database_data/reviews_sample.csv", 'r') as f:
    read_file = csv.DictReader(f)
    for line in read_file:
        insert(cursor, connect, fourth_task_review, (line["user_id"],
                                                     line["recipe_id"],
                                                     datetime.datetime.strptime(line["date"], "%Y-%m-%d"),
                                                     line["rating"],
                                                     line["review"]))

with open("06_database_data/recipes_sample_with_tags_ingredients.csv", 'r') as f:
    read_file = csv.DictReader(f)
    for line in read_file:
        insert(cursor, connect, fourth_task_recipe, (line["id"],
                                                     line["name"],
                                                     line["minutes"],
                                                     datetime.datetime.strptime(line["submitted"], "%Y-%m-%d"),
                                                     line["description"],
                                                     line["n_ingredients"]))
