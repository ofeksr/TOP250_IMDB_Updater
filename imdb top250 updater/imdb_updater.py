"""
Manage your personal IMDB TOP 250 rated movies database.
Track what you seen and what not.
Get email report and take actions like delete or seen from email message replies.
"""

import re
from datetime import datetime

import pandas as pd
import requests
import yagmail
from bs4 import BeautifulSoup
from tabulate import tabulate

import exceptions
import mysql_db
from data.gmail_vars import *
from google_agents import GmailAgent

LOG = exceptions.create_logger()


class IMDBTOP250Updater:

    def __init__(self, db_name: str = None):
        self.LOG = LOG
        self.new_movies: list = []
        self.new_movie_flag: bool = False  # to check if new movie added to database so script need to send email

        # set up mysql database connection
        self.root_db = mysql_db.DBConnection(db_name=db_name)
        self.root_db.get_connection()
        self.root_db.get_cursor()
        self.top250_db = mysql_db.TOP250Table(self.root_db)

        # self.gmail_agent = GmailAgent()
        self.gmail_agent = None

        self.LOG.info('IMDBTOP250Updater object created successfully')

    def create_list(self, check_seen: bool = False, update_table: bool = False):
        """
        Web Scrapping top 250 list and checking new movies that not in database and asking user for seen or not.
        :param update_table: for comparing to current table.
        :param check_seen: True to ask user for seen / not seen movies.
        :return: bool or None
        """

        try:
            self.LOG.debug('Trying to web scrap imdb website')
            url = 'https://www.imdb.com/chart/top'
            response = requests.get(url)

        except Exception:
            self.LOG.exception('Failed to get respond from imdb website')
            raise ConnectionError

        if update_table:
            table_name = 'top250_update'
            self.top250_db.create_table(table_name=table_name)
        else:
            table_name = 'top250'

        soup = BeautifulSoup(response.text, 'html.parser')

        movies = soup.select('td.titleColumn')
        links = ['https://www.imdb.com' + a.attrs.get('href') for a in soup.select('td.titleColumn a')]

        soup.select('td.ratingColumn')
        rating = [b.attrs.get('title') for b in soup.select('td.ratingColumn strong')]

        for index in range(0, len(movies)):
            movie_string = movies[index].get_text()
            movie = (' '.join(movie_string.split()).replace('.', ''))
            movie_title = movie[len(str(index)) + 1:-7].strip()
            year = re.search('\\((.*?)\\)', movie_string).group(1)

            if int(year) >= 1990:
                rating_value = float(rating[index].split()[0])
                reviewers = rating[index].split()[3]

                if not check_seen:
                    seen = None
                    self.top250_db.insert_movie(
                        values=[
                            index + 1, movie_title, year, rating_value, reviewers, seen, links[index]
                        ],
                        table_name=table_name
                    )

                else:
                    self.change_seen_status()

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

        elif title and len(self.top250_db.select_by_title(title=title)) > 1:
            self.top250_db.update_seen_status(title=title, seen_status=seen_status)
            return True

        elif not title and not place and not seen_status:
            none_seen_status_movies = self.top250_db.select_all_non_seen_status()
            for movie in none_seen_status_movies:
                title = movie[0]

                while 1:
                    try:
                        user_input = int(input(f'Have you seen {title} ?\nType 1 / 0 ....'))
                        print()
                        break
                    except ValueError:
                        print('Try Again Please !')

                if user_input == 1:
                    self.top250_db.update_seen_status(title=title, seen_status=True)

                elif user_input == 0:
                    self.top250_db.update_seen_status(title=title, seen_status=False)

            return True

        else:
            self.LOG.info(f'Movie did not found in db, cannot update seen status')
            return False

    def unseen_movies(self, df_email: bool = False):
        """
        Exporting unseen movies as list of tuples (seen status is None).
        :param df_email: True for returning html data frame for email usage.
        :return: list[tuple] or tabulate html
        """
        unseen = self.top250_db.select_unseen_titles()

        if not df_email:
            return unseen

        else:
            df = pd.DataFrame(data=unseen,
                              columns=['Place', 'Title', 'Year', 'Rating', 'Reviewers', 'Seen Status', 'Link'])
            df = df.drop(['Seen Status'], axis=1)
            tabulate.PRESERVE_WHITESPACE = True
            return tabulate(df, headers='keys', tablefmt='html', numalign='center', stralign='center', showindex=False)

    def new_movie_details(self) -> list:
        """
        Getting links of imdb movie url, poster image and trailer for new movie add to top 250.
        :return: list
        """
        contents = []
        for i, tup in enumerate(self.new_movies, start=1):
            place = tup[0]
            url = tup[-1]

            try:
                self.LOG.debug(f'Trying to web scrap {url}')
                response = requests.get(url)
                soup_new_movies = BeautifulSoup(response.text, 'html.parser')

            except Exception as e:
                self.LOG.exception(f'Failed to web scrap {url}')
                raise exceptions.WebScrapEvents(f'Failed to web scrap {url}', e)

            poster_tag = soup_new_movies.findAll('div', {'class': 'poster'})
            poster_link = str(poster_tag).split('src=')[1].split(' title')[0]

            # trailer_tag = soup_new_movies.findAll('div', {'class': 'slate'})
            # trailer_link = "https://www.imdb.com" + str(trailer_tag).split('href="')[1].split('"> <img')[0]

            contents.append((place, url, poster_link))

        self.LOG.info('Finished scrapping for new movies details')
        return contents

    def send_email(self, receiver_email: str, sender_mail: str, sender_password: str) -> bool:
        """
        Sending email report with new movies that added and all unseen movies by user.
        :return: True
        """
        subject = f'IMDB TOP 250 Updater {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        IMDB_LOGO = 'https://ia.media-imdb.com/images/M/MV5BMTczNjM0NDY0Ml5BMl5BcG5nXkFtZTgwMTk1MzQ2OTE@._V1_.png'

        top = '<br><br>' \
              '<center><body>' \
              '<p>' \
              '<h2><b>IMDB 250 Top Rated Update Notice</b></h2>' \
              '</p>' \
              f'<p>' \
              f'<h3><b>{len(self.new_movies)} Movies Added</b></h3>' \
              f'</p>' \
              '</body></center>'

        contents = [top]

        for item in self.new_movie_details():
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

        contents.append('<br>'
                        '<center>'
                        f'<h3><u>Unseen Movies List ({len(self.unseen_movies())})</u></h3>'
                        f'{self.unseen_movies(df_email=True)}'
                        '</center>')

        bottom = 'To delete movie from list reply with: " delete: ### "' \
                 '<br>' \
                 'To check seen status for movie in list reply with: " seen: ### "' \
                 '<br><br>' \
                 '<big>End of notice.</big>' \
                 '<br>' \
                 '<small>Sent with TOP250Updater.</small>'

        contents.append(bottom)

        try:
            self.LOG.debug('Trying to send email.')
            yag = yagmail.SMTP(sender_mail, sender_password)
            yag.send(receiver_email, subject, contents)
            self.LOG.info(f'Email sent successfully to {receiver_email}')
            return True

        except Exception:
            self.LOG.exception(f'Failed to send email message')
            raise

    def check_email_replies(self) -> bool:
        """
        Checking email mailbox for replying emails for taking actions - updating seen status or deleting from user db.
        example: email content - 'delete: 2' or 'seen: 150'
        :return: True
        """
        # setup gmail agent object
        self.LOG.debug('Start check email replays for actions')
        self.gmail_agent = GmailAgent()
        self.gmail_agent.login()

        # check for replies
        messages = self.gmail_agent.list_messages_from_inbox()
        for message in messages:
            msg_id = message['id']
            msg_body = self.gmail_agent.get_message(message_id=msg_id)

            # check for 'delete' or 'seen' in message body for actions on db
            movie_place_to_delete = re.search('delete:\s*[1-9][0-9]*[0-5]*', msg_body.lower())
            movie_place_to_seen = re.search('(seen):\s*[1-9][0-9]*[0-5]*', msg_body.lower())

            if movie_place_to_delete:
                temp = movie_place_to_delete.group().split(':')
                place = int(temp[1].strip())
                self.top250_db.delete_movie(place=place)
                self.gmail_agent.delete_message(message_id=msg_id)

                body = f'Movie in place {place} deleted from database'
                msg = self.gmail_agent.create_message(sender=sender, to=to,
                                                      subject='IMDB Updater auto-reply', message_text=body)
                self.gmail_agent.send_message(message=msg)

                self.LOG.info(f'movie in place {place} deleted by reply request')

            if movie_place_to_seen:
                temp = movie_place_to_seen.group().split(':')
                place = int(temp[1].strip())
                self.top250_db.update_seen_status(place=place, seen_status=True)
                self.gmail_agent.delete_message(message_id=msg_id)

                body = f'Movie in place {place} checked as seen in database'
                msg = self.gmail_agent.create_message(sender=sender, to=to,
                                                      subject='IMDB Updater auto-reply', message_text=body)
                self.gmail_agent.send_message(message=msg)

                self.LOG.info(f'movie in place {place} seen checked by reply request')

        return True
