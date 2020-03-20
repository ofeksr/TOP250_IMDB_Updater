"""
Tool for converting IMDB TOP250 Updater json database to MySQL database.
"""
import json

from mysql.connector import (connection)
from database.mysql_config import *

my_db = connection.MySQLConnection(
    user=DB_USER, password=DB_PASSWORD,
    host=DB_HOST)

my_cursor = my_db.cursor()

my_cursor.execute("CREATE DATABASE imdb")
my_cursor.close()

my_db = connection.MySQLConnection(
    user=DB_USER, password=DB_PASSWORD,
    host=DB_HOST, database=DB_NAME)

my_cursor = my_db.cursor()

with open('imdb_db.json') as file:
    root_dict = json.load(file)
    top250 = root_dict['top250']
    removed_movies = root_dict['removed_movies']
    # print(json.dumps(root_dict, indent=4))

my_cursor.execute("CREATE TABLE `top250` (`place` int, `title` text, `year` int, `rating` float, `reviewers` text, "
                  "`seen_status` bool, `link` text)")
my_cursor.execute("CREATE TABLE `removed_movies` (`title` text)")

for place in top250:
    my_cursor.execute(
        f"INSERT INTO top250 (place, title, year, rating, reviewers, seen_status, link) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s)", (
            int(place),
            top250[place]['Movie'],
            int(top250[place]['Year']),
            float(top250[place]['Rating'].split()[0]),
            top250[place]['Rating'].split()[3],
            top250[place]['Seen'],
            top250[place]['Link'],
        )
    )

for movie in removed_movies:
    my_cursor.execute(
        f"INSERT INTO removed_movies (title) VALUES (%s)", (movie,)
    )

my_db.commit()

my_db.close()
