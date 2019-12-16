import unittest

import os

from imdb_updater import IMDBTOP250Updater
from tests.data.tests_config import *


class TestIMDBTop250Updater(unittest.TestCase):

    def setUp(self) -> None:
        for path in ['database', 'logs']:
            if not os.path.isdir(path):
                os.mkdir(path)

        self.updater = IMDBTOP250Updater(db_name='imdb_unittest')
        self.updater.create_list()

    def tearDown(self) -> None:
        self.updater.root_db.drop_database()
        self.updater.root_db.close_cursor()
        self.updater.root_db.close_connection()

        self.updater = None

    def test_create_list(self):
        self.assertTrue(self.updater.create_list(check_seen=False),
                        msg='Failed to create new top 250 movies list')

    def test_print_movies(self):
        self.assertTrue(self.updater.print_movies(),
                        msg='Failed to prints movies')

    def test_remove_movie(self):
        for i in range(2):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertTrue(self.updater.remove_movie(title='The Dark Knight'),
                                    msg='Failed to remove movie by title of str name')
                else:
                    try:
                        for n in range(10):
                            self.assertTrue(self.updater.remove_movie(place=n),
                                            msg='Failed to remove movie by title of int number')
                            break
                    except:
                        pass

    def test_update_list(self):
        self.updater.remove_movie(title='The Dark Knight')

        self.assertTrue(self.updater.update_top250(),
                        msg='Failed to updating list')

    def test_check_seen(self):
        for i in range(4):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertTrue(self.updater.change_seen_status(title='The Dark Knight', seen_status=True),
                                    msg='Failed to change movie seen status by title')
                elif i == 1:
                    self.assertTrue(self.updater.change_seen_status(place=1, seen_status=True),
                                    msg='Failed to change movie seen status by place')

                elif i == 2:
                    self.assertFalse(self.updater.change_seen_status(place=300, seen_status=True),
                                     msg='Failed to get false for place not exists in db')

                # else:
                #     self.assertTrue(self.updater.change_seen_status(),
                #                     msg='Failed to check seen status for all movies')

    def test_unseen_movies(self):
        for i in range(2):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertIsInstance(self.updater.unseen_movies(),
                                          list,
                                          msg='Failed to create unseen movies list')
                else:
                    self.assertIsNotNone(self.updater.unseen_movies(df_email=True),
                                         msg='Failed to create unseen movies list')

    def test_new_movie_details(self):
        self.test_update_list()
        self.assertIsInstance(self.updater.new_movie_details(),
                              list,
                              msg='Failed to get new movies details')

    def test_send_email(self):
        self.updater.remove_movie(title='The Dark Knight')
        self.updater.update_top250()

        self.assertTrue(self.updater.send_email(receiver_email, sender_mail, sender_password),
                        msg='Failed to send email report')

    def test_check_email_replies(self):
        for i in range(2):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertTrue(self.updater.check_email_replies(),
                                    msg='Failed to change movie status from email reply')
                else:
                    self.assertTrue(self.updater.check_email_replies(),
                                    msg='Failed to delete movie from email reply')
