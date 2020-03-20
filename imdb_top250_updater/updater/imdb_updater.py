"""
Manage your personal IMDB TOP 250 rated movies database.
Track what you seen and what not.
Get email_tools report and take actions like delete or seen from email_tools message replies.
"""

import re

import pandas as pd
import requests
import yagmail
from bs4 import BeautifulSoup
from tabulate import tabulate

from logs.exceptions import create_logger, datetime
from database import mysql_db
from email_tools import gmail_vars
from email_tools.google_agents import GmailAgent

LOG = create_logger()


class IMDBTOP250Updater:

    def __init__(self, db_name: str = None):
        self.LOG = LOG
        self.new_movies: list = []
        self.new_movie_flag: bool = False  # to check if new movie added to database so script need to send email_tools

        # set up mysql database connection
        self.root_db = mysql_db.DBConnection(db_name=db_name)
        self.root_db.get_connection()
        self.root_db.get_cursor()
        self.top250_db = mysql_db.TOP250Table(self.root_db)

        # self.gmail_agent = GmailAgent()
        self.gmail_agent = None

        self.LOG.info('IMDBTOP250Updater object created successfully')

    def get_imdb_website_response(self, url):
        try:
            self.LOG.debug('Trying to web scrap imdb website')
            response = requests.get(url)
            return response

        except Exception:
            self.LOG.exception('Failed to get respond from imdb website')
            raise ConnectionError

    def create_update_table(self, table_name):
        try:
            self.top250_db.create_table(table_name=table_name)
        except mysql_db.connection.errors.ProgrammingError:
            self.top250_db.drop_table(table_name=table_name)
            self.top250_db.create_table(table_name=table_name)

    @staticmethod
    def get_scraped_items(response):
        soup = BeautifulSoup(response.text, 'html.parser')

        movies = soup.select('td.titleColumn')
        links = ['https://www.imdb.com' + a.attrs.get('href') for a in soup.select('td.titleColumn a')]

        soup.select('td.ratingColumn')
        rating = [b.attrs.get('title') for b in soup.select('td.ratingColumn strong')]

        return movies, links, rating

    def insert_movie_without_checking_seen(self, index, movie_title, year, rating_value, reviewers, links, table_name):
        seen = None
        self.top250_db.insert_movie(
            values=[
                index + 1, movie_title, year, rating_value, reviewers, seen, links[index]
            ],
            table_name=table_name
        )

    def insert_movie_with_checking_seen(self, movie_title):
        seen_status = input(f'Did you seen {movie_title}? [y/n]')
        seen_status = True if seen_status == 'y' else False
        self.change_seen_status(title=movie_title, seen_status=seen_status)

    def insert_valid_movies_only_to_movies_table(self, movies, links, rating, check_seen, table_name):
        for index in range(0, len(movies)):
            movie_string = movies[index].get_text()
            movie = (' '.join(movie_string.split()).replace('.', ''))
            movie_title = movie[len(str(index)) + 1:-7].strip()
            year = re.search('\\((.*?)\\)', movie_string).group(1)

            if int(year) >= 1990:
                rating_value = float(rating[index].split()[0])
                reviewers = rating[index].split()[3]

                if not check_seen:
                    self.insert_movie_without_checking_seen(index, movie_title, year, rating_value, reviewers, links,
                                                            table_name)
                else:
                    self.insert_movie_with_checking_seen(movie_title)

    def create_list(self, check_seen: bool = False, update_table: bool = False):
        """
        Web Scrapping top 250 list and checking new movies that not in database and asking user for seen or not.
        :param update_table: for comparing to current table.
        :param check_seen: True to ask user for seen / not seen movies.
        :return: bool or None
        """

        url = 'https://www.imdb.com/chart/top'
        response = self.get_imdb_website_response(url)

        if update_table:
            table_name = 'top250_update'
            self.create_update_table(table_name)
        else:
            table_name = 'top250'
            self.top250_db.create_table(table_name=table_name)

        movies, links, rating = self.get_scraped_items(response)
        self.insert_valid_movies_only_to_movies_table(movies, links, rating, check_seen, table_name)

        self.LOG.info('Finished creating new movies list')
        return True

    def print_movies(self) -> bool:
        """
        Print all movies in top 250 user database.
        :return: True
        """
        records = self.top250_db.select_by_cols(columns=['place', 'title', 'year', 'rating', 'reviewers'])
        for record in records:
            print(*record, sep=' / ')
        return True

    def remove_movie(self, title: str = None, place: int = None) -> bool:
        """
        Remove movies from database.
        :param title: str, example: 'Joker'
        :param place: int, example: 126
        :return: True
        """
        if title and self.top250_db.select_by_title(title=title):
            self.top250_db.delete_movie(title=title)
            self.LOG.info(f'{title} has been removed from db')

        elif place and self.top250_db.select_by_place(place=place):
            self.top250_db.delete_movie(place=place)
            self.LOG.info(f'movie in place {place} has been removed from db')

        return True

    def update_top250(self) -> bool:
        """
        Update top250 db with added new movies to original top 250 from imdb website.
        :return: True
        """

        self.create_list(update_table=True)
        self.new_movies = self.top250_db.update_movies_table()
        if len(self.new_movies) > 0:
            self.new_movie_flag = True

        self.LOG.info('Finish updating movies list')
        return True

    @staticmethod
    def get_user_input_seen_status(title):
        while 1:
            try:
                user_input = int(input(f'Have you seen {title} ?\nType 1 / 0 ....'))
                print()
                break
            except ValueError:
                print('Try Again Please !')
        return user_input

    def check_seen_status_for_all_movies(self):
        none_seen_status_movies = self.top250_db.select_all_non_seen_status()

        for movie in none_seen_status_movies:
            title = movie[0]
            user_input = self.get_user_input_seen_status(title)

            if user_input == 1:
                self.top250_db.update_seen_status(title=title, seen_status=True)
            elif user_input == 0:
                self.top250_db.update_seen_status(title=title, seen_status=False)

    def change_seen_status(self, title: str = None, place: int or str = None, seen_status: bool = None) -> bool:
        """
        Check movies seen status if current status is None.
        Note: Use parameters if you want to change specific movie seen status.
        :param title: str, example: 'The Dark Knight'
        :param place: int or str, example: 2
        :param seen_status: bool, True or False.
        :return: True
        """
        # if movie in db
        if place and place <= 250 and len(self.top250_db.select_by_place(place=place)) > 1:
            self.top250_db.update_seen_status(place=place, seen_status=seen_status)
            return True

        elif title and len(self.top250_db.select_by_title(title=title)) > 0:
            self.top250_db.update_seen_status(title=title, seen_status=seen_status)
            return True

        elif not title and not place and not seen_status:
            self.check_seen_status_for_all_movies()
            return True

        else:
            self.LOG.info(f'Movie did not found in db, cannot update seen status')
            return False

    @staticmethod
    def get_tabulate_unseen_movies_chart(unseen_titles):
        df = pd.DataFrame(data=unseen_titles,
                          columns=['Place', 'Title', 'Year', 'Rating', 'Reviewers', 'Seen Status', 'Link'])
        df = df.drop(['Seen Status'], axis=1)
        tabulate.PRESERVE_WHITESPACE = True
        return tabulate(df, headers='keys', tablefmt='html', numalign='center', stralign='center', showindex=False)

    def unseen_movies(self, df_email: bool = False):
        """
        Exporting unseen movies as list of tuples (seen status is None).
        :param df_email: True for returning html data frame for email_tools usage.
        :return: list[tuple] or tabulate html
        """
        unseen = self.top250_db.select_unseen_titles()

        if not df_email:
            return unseen
        else:
            return self.get_tabulate_unseen_movies_chart(unseen)

    @staticmethod
    def get_trailer_link_from_soup(soup_new_movies):
        trailer_tag = soup_new_movies.findAll('div', {'class': 'slate'})
        trailer_link = "https://www.imdb.com" + str(trailer_tag).split('href="')[1].split('"> <img')[0]
        return trailer_link

    @staticmethod
    def get_poster_link_from_soup(soup_new_movies):
        poster_tag = soup_new_movies.findAll('div', {'class': 'poster'})
        poster_link = str(poster_tag).split('src=')[1].split(' title')[0]
        return poster_link

    def new_movie_details_for_email_contents(self) -> list:
        """
        Getting links of imdb movie url, poster image and trailer for new movie add to top 250.
        :return: list
        """
        contents = []
        for i, tup in enumerate(self.new_movies, start=1):
            place = tup[0]
            url = tup[-1]

            response = self.get_imdb_website_response(url)
            soup_new_movies = BeautifulSoup(response.text, 'html.parser')

            poster_link = self.get_poster_link_from_soup(soup_new_movies)
            # trailer_link = self.get_poster_link_from_soup(soup_new_movies)

            contents.append(
                (place, url, poster_link)
            )

        self.LOG.info('Finished scrapping for new movies details')
        return contents

    def send_email_with_yag(self, sender_mail, sender_password, receiver_email, subject, contents):
        self.LOG.debug('Trying to send email')
        yag = yagmail.SMTP(sender_mail, sender_password)
        yag.send(receiver_email, subject, contents)
        self.LOG.info(f'Email sent successfully to {receiver_email}')

    def add_new_movies_to_contents(self, contents):
        for item in self.new_movie_details_for_email_contents():
            place, link, poster = item[0], item[1], item[2]
            movie = self.top250_db.select_by_place(place=place)

            contents.append('<br>'
                            '<center>'
                            '<body>'
                            '<p>'
                            f'<h3>{" / ".join([str(x) for x in movie[:5]])}</h3>'
                            '</p>'
                            '<br>'
                            f'<img src={poster} alt="Poster" align="middle"/>'
                            f'<a href="{link}"><img src={IMDB_LOGO} width="80" height= "80" '
                            'align="middle" alt="IMDB Link"/></a>'
                            '<br>'
                            '<hr>'
                            '<br>'
                            '</body>'
                            '</center>')

    def add_unseen_movies_to_contents(self, contents):
        contents.append('<br>'
                        '<center>'
                        f'<h3><u>Unseen Movies List ({len(self.unseen_movies())})</u></h3>'
                        f'{self.unseen_movies(df_email=True)}'
                        '</center>')

    def create_contents_with_top_body_msg(self):
        top = '<br><br>' \
              '<center><body>' \
              '<p>' \
              '<h2><b>IMDB 250 Top Rated Update Notice</b></h2>' \
              '</p>' \
              f'<p>' \
              f'<h3><b>{len(self.new_movies)} Movies Added</b></h3>' \
              f'</p>' \
              '</body></center>'
        return [top]

    @staticmethod
    def add_notice_end_to_contents(contents):
        bottom = 'To delete movie from list reply with: " delete: ### "' \
                 '<br>' \
                 'To check seen status for movie in list reply with: " seen: ### "' \
                 '<br><br>' \
                 '<big>End of notice.</big>' \
                 '<br>' \
                 '<small>Sent with TOP250Updater.</small>'
        contents.append(bottom)

    def build_contents(self):
        contents = self.create_contents_with_top_body_msg()
        self.add_new_movies_to_contents(contents)
        self.add_unseen_movies_to_contents(contents)
        self.add_notice_end_to_contents(contents)
        return contents

    def send_email(self, receiver_email: str, sender_mail: str, sender_password: str) -> bool:
        """
        Sending email report msg with new movies that added and all unseen movies by user.
        :return: True
        """
        global IMDB_LOGO
        subject = f'IMDB TOP 250 Updater {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        IMDB_LOGO = 'https://ia.media-imdb.com/images/M/MV5BMTczNjM0NDY0Ml5BMl5BcG5nXkFtZTgwMTk1MzQ2OTE@._V1_.png'
        contents = self.build_contents()

        try:
            self.send_email_with_yag(sender_mail, sender_password, receiver_email, subject, contents)
            return True

        except Exception:
            self.LOG.exception(f'Failed to send email message')
            raise

    @staticmethod
    def get_movies_places_for_actions(message_body):
        movies_places_to_delete = re.search('(delete([:\-])\s*)([0-9]+)(\s+[0-9]+)*', message_body.lower())
        if movies_places_to_delete:
            split_by_spaces = movies_places_to_delete.group().split()
            movies_places_only_to_delete = [int(item) for item in split_by_spaces if item.isdecimal()]
        else:
            movies_places_only_to_delete = []

        movies_places_to_seen = re.search('(seen([:\-])\s*)([0-9]+)(\s+[0-9]+)*', message_body.lower())
        if movies_places_to_seen:
            split_by_spaces = movies_places_to_seen.group().split()
            movies_places_only_to_seen = [int(item) for item in split_by_spaces if item.isdecimal()]
        else:
            movies_places_only_to_seen = []

        return movies_places_only_to_delete, movies_places_only_to_seen

    def send_reply_request_email(self, body):
        msg = self.gmail_agent.create_message(sender=gmail_vars.sender, to=gmail_vars.to,
                                              subject='IMDB Updater auto-reply', message_text=body)
        self.gmail_agent.send_message(message=msg)

    def setup_gmail_agent(self):
        self.gmail_agent = GmailAgent()
        self.gmail_agent.login()

    def check_email_replies(self) -> bool:
        """
        Checking email mailbox for replying emails for taking actions - updating seen status or deleting from user db.
        example: email content - 'delete: 2 10 55' or 'seen- 150'
        :return: True
        """
        self.LOG.debug('Start check email_tools replays for actions')
        self.setup_gmail_agent()

        messages = self.gmail_agent.list_messages_from_inbox()
        for message in messages:
            msg_id = message['id']
            msg_body = self.gmail_agent.get_message(message_id=msg_id)
            movies_places_to_delete, movies_places_to_seen = self.get_movies_places_for_actions(msg_body)
            if movies_places_to_delete:
                titles_by_places = {}
                for p in movies_places_to_delete:
                    title = self.top250_db.delete_movie(place=p)
                    titles_by_places[p] = title

                body = f'The movies {titles_by_places} has been deleted from database'
                self.send_reply_request_email(body)
                self.LOG.info(f'The movies {titles_by_places} has been deleted by reply request')

            elif movies_places_to_seen:
                titles_by_places = {}
                for p in movies_places_to_seen:
                    title = self.top250_db.update_seen_status(place=p, seen_status=True)
                    titles_by_places[p] = title

                body = f'The Movies {titles_by_places} checked as seen in database'
                self.send_reply_request_email(body)
                self.LOG.info(f'The movies {titles_by_places} checked as seen by reply request')

            self.gmail_agent.delete_message(message_id=msg_id)

        return True
