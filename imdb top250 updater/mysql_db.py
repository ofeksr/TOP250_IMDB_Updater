import atexit
import logging
import subprocess
import sys

from mysql.connector import (connection)

# load DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
from data.mysql_config import *

LOG = logging.getLogger('MySQL.DB.Logger')
handler = logging.StreamHandler(sys.stdout)
LOG.addHandler(handler)


class DBConnection:

    def __init__(self, host: str = DB_HOST, user: str = DB_USER, password: str = DB_PASSWORD, db_name: str = DB_NAME):
        self.host = host
        self.user = user
        self.__password = password

        if db_name is None:
            self.db_name = DB_NAME
        else:
            self.db_name = db_name

        self.my_connection = None
        self.my_cursor = None

        try:  # check if DB exists, if not - create one.
            self.get_connection()
        except:
            LOG.error('DB not exists, creating DB now...')
            self.create_database()

        # close connection on force exit
        atexit.register(self.close_cursor)
        atexit.register(self.close_connection)

        LOG.info('DBConnection object created successfully')

    def get_connection(self):
        if self.my_connection is None:
            LOG.debug('Trying to create connection')
            self.my_connection = connection.MySQLConnection(host=self.host,
                                                            user=self.user,
                                                            passwd=self.__password,
                                                            database=self.db_name,
                                                            charset='utf8'
                                                            )
            LOG.info('Connection created successfully')
        return self.my_connection

    def get_cursor(self):
        if self.my_connection:
            self.my_cursor = self.my_connection.cursor(buffered=True)

        return self.my_cursor

    def create_database(self):
        temp_connection = connection.MySQLConnection(host=self.host,
                                                     user=self.user,
                                                     passwd=self.__password,
                                                     )
        temp_cursor = temp_connection.cursor()
        temp_cursor.execute(
            f"CREATE DATABASE {self.db_name}",
        )

    def drop_database(self):
        if self.my_connection and self.my_cursor:
            self.my_cursor.execute(
                f"DROP DATABASE {self.db_name}",
            )
            LOG.info(f'database {self.db_name} dropped')

    def commit(self):
        if self.my_cursor:
            self.my_connection.commit()

    def close_cursor(self):
        if self.my_cursor:
            self.my_cursor.close()

    def close_connection(self):
        if self.my_connection:
            self.my_connection.close()

    def backup_database(self):
        subprocess.call(['mysqldump', '-h', self.host, '-u', self.user,
                         f'-p{self.__password}', self.db_name, '-r', f'database/{self.db_name}_backup.sql'])

        LOG.info('Database backup completed')
        return True


class RemovedMoviesTable:
    """
    should be manged directly through Top250Table object
    """
    def __init__(self, parent):
        self.db_connection = parent
        self.my_cursor = parent.my_cursor

        try:  # check if table exists, if not - create one.
            self.select_titles()
        except:
            LOG.error('table not exists, creating removed movies table now...')
            self.create_table()

        LOG.info('RemovedMoviesTable object created successfully')

    def create_table(self):
        self.my_cursor.execute(
            "CREATE TABLE `removed_movies` (`title` text)"
        )

    def insert_title(self, title: str):
        self.my_cursor.execute(
            "INSERT INTO removed_movies (title) VALUES (%s)", (title,)
        )
        self.db_connection.commit()

    def select_titles(self):
        self.my_cursor.execute(
            "SELECT * FROM removed_movies"
        )
        return self.my_cursor.fetchall()

    def delete_movie(self, title: str):
        self.my_cursor.execute(
            f"DELETE FROM removed_movies WHERE (title = '{title}')"
        )
        self.db_connection.commit()


class TOP250Table:

    def __init__(self, parent):
        self.db_connection = parent
        self.removed_movies_db = RemovedMoviesTable(parent)

        self.my_cursor = parent.my_cursor

        try:  # check if table exists, if not - create one.
            self.select_all()
        except:
            LOG.error('table not exists, creating top250 table now...')
            self.create_table(table_name='top250')

        LOG.info('TOP250Table object created successfully')

    def create_table(self, table_name: str = False):
        self.my_cursor.execute(
            f"CREATE TABLE `{table_name}` (`place` int, `title` text, `year` int, `rating` float, `reviewers` text, "
            f"`seen_status` bool, `link` text)"
        )

    def update_movies_table(self):
        # build new table
        self.my_cursor.execute(
            "CREATE TABLE top250_new AS (SELECT u.place, u.title, u.year, u.rating, u.reviewers, t.seen_status, u.link"
            " FROM top250_update u, top250 t"
            " WHERE t.title = u.title and t.year = u.year and t.link = u.link)"
        )

        # get and insert new movies that added
        self.my_cursor.execute(
            "INSERT INTO top250_new"
            " ("
            " SELECT u.*"
            " FROM top250_update u"
            " WHERE u.title not in (select t.title from top250 t) and"
            " u.title not in (select r.title from removed_movies r)"
            " )"
        )

        # order table by place
        self.my_cursor.execute(
            "ALTER TABLE top250_new ORDER BY place"
        )

        # get new movies
        self.my_cursor.execute(
            " SELECT u.*"
            " FROM top250_update u"
            " WHERE u.title not in (select t.title from top250 t) and"
            " u.title not in (select r.title from removed_movies r)"
        )
        new_movies = self.my_cursor.fetchall()

        # log new movies
        if len(new_movies) > 0:
            for movie in new_movies:
                place, title, year, rating, reviewers, seen_status, link = movie
                LOG.info(f'inserted new movie to db: #{place}/ {title} / {year} / {rating} / {link}')

        # turn old top250 table to new top250_new table
        self.drop_table(table_name='top250')
        self.my_cursor.execute(
            "ALTER TABLE top250_new RENAME TO top250"
        )
        LOG.debug('table top250_new renamed to top250')
        self.drop_table(table_name='top250_update')
        self.db_connection.commit()

        LOG.info('top250 table updated')
        return new_movies

    def drop_table(self, table_name: str):
        self.my_cursor.execute(
            f"DROP TABLE {table_name}"
        )
        LOG.info(f'table `{table_name}` dropped')

    def insert_movie(self, values: list, table_name: str = None):
        place, title, year, rating, reviewers, seen_status, link = values

        self.my_cursor.execute(
            f"INSERT INTO {table_name} (place, title, year, rating, reviewers, seen_status, link)"
            f" VALUES (%s, %s, %s, %s, %s, %s, %s)", (place, title, year, rating, reviewers, seen_status, link,)
        )
        self.db_connection.commit()

    def update_seen_status(self, place: int = None, title: str = None, seen_status: bool = None):
        if title:
            self.my_cursor.execute(
                "UPDATE top250 SET seen_status = %s WHERE title = %s", (seen_status, title)
            )
            self.db_connection.commit()
            LOG.info(f'{title} seen status has been updated')

        else:
            self.my_cursor.execute(
                "UPDATE top250 SET seen_status = %s WHERE place = %s", (seen_status, place)
            )
            self.db_connection.commit()
            LOG.info(f'movie seen status in place {place} has been updated')

    def select_by_place(self, place: int = None):
        self.my_cursor.execute(
            "SELECT * FROM top250 WHERE place = %s", (place,)
        )
        return self.my_cursor.fetchone()

    def select_by_title(self, title: str = None):
        self.my_cursor.execute(
            "SELECT * FROM top250 WHERE title = %s", (title,)
        )
        return self.my_cursor.fetchone()

    def select_by_cols(self, columns: list = None):
        columns_as_str = ', '.join(columns)
        self.my_cursor.execute(
            f"SELECT {columns_as_str} FROM top250"
        )
        return self.my_cursor.fetchall()

    def select_all(self):
        self.my_cursor.execute(
            "SELECT * FROM top250"
        )
        return self.my_cursor.fetchall()

    def select_all_non_seen_status(self):
        self.my_cursor.execute(
            "SELECT title, seen_status FROM top250 WHERE seen_status is null"
        )
        return self.my_cursor.fetchall()

    def select_unseen_titles(self):
        self.my_cursor.execute(
            "SELECT * FROM top250 t WHERE seen_status is null or seen_status is False"
        )
        return self.my_cursor.fetchall()

    def delete_movie(self, place: int = None, title: str = None):
        if place or title:
            if place and self.select_by_place(place=place):
                title = self.select_by_place(place=place)[1]
                self.my_cursor.execute(
                    "DELETE FROM top250 WHERE (place = %s)", (place,)
                )
                self.removed_movies_db.insert_title(title=title)

            else:
                self.my_cursor.execute(
                    "DELETE FROM top250 WHERE (title = %s)", (title,)
                )
                self.removed_movies_db.insert_title(title=title)

            self.db_connection.commit()
