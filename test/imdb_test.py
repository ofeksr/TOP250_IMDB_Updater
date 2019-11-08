import unittest
from os import environ

import keyring
import os

from __init__ import IMDBTOP250Updater
from imdb_schedule import run_script


class TestIMDBTop250Updater(unittest.TestCase):

    def setUp(self) -> None:
        for path in ['database', 'logs']:
            if not os.path.isdir(path):
                os.mkdir(path)
        self.updater = IMDBTOP250Updater()
        self.updater.create_list()

    def tearDown(self) -> None:
        self.updater = None

    def test_create_list(self):
        self.assertTrue(self.updater.create_list(),
                        msg='Failed to create new top 250 movies list')

    def test_import_db(self):
        self.assertIsNotNone(self.updater.import_database(),
                             msg='Failed to import database')

    def test_export_db(self):
        self.assertTrue(self.updater.save_database(),
                        msg='Failed to export database')

    def test_print_movies(self):
        self.assertTrue(self.updater.print_movies(),
                        msg='Failed to prints movies')

    def test_remove_movie(self):
        for i in range(3):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertTrue(self.updater.remove_movie(title='Joker'),
                                    msg='Failed to remove movie by title of str name')
                elif i == 1:
                    self.assertTrue(self.updater.remove_movie(title=1),
                                    msg='Failed to remove movie by title of str number')
                else:
                    self.assertTrue(self.updater.remove_movie(title='1'),
                                    msg='Failed to remove movie by title of int number')

    def test_update_list(self):
        self.assertTrue(self.updater.update_list(),
                        msg='Failed to updating list')

    def test_check_seen(self):
        for i in range(2):
            with self.subTest(msg=i):
                if i == 0:
                    self.assertTrue(self.updater.change_seen_status(title='Joker', status=True),
                                    msg='Failed to change movie status by title')
                else:
                    self.assertTrue(self.updater.change_seen_status(place=1, status=True),
                                    msg='Failed to change movie status by place')

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
        self.assertIsInstance(self.updater.new_movie_details(),
                              list,
                              msg='Failed to get new movies details')

    def test_send_email(self):
        receiver_email = environ.get('my_email')
        sender_mail = environ.get('imdb_email')
        sender_password = keyring.get_password('imdb-pass', 'ofeksofeks')

        self.assertTrue(self.updater.send_email(receiver_email, sender_mail, sender_password),
                        msg='Failed to change movie status')

    def test_delete_seen_email(self):
        email_address = environ.get('imdb_email')
        password = keyring.get_password('imdb-pass', 'ofeksofeks')

        self.assertTrue(self.updater.delete_seen_from_email(email_address, password),
                        msg='Failed to change movie status')

    def test_schedule(self):
        self.assertTrue(run_script(),
                        msg='Failed to run schedule script')
