"""
Manage your personal IMDB TOP 250 rated movies database.
Track what you seen and what not.
Get email report and take actions like delete or seen from email message replies.
"""

import copy
import imaplib
import json
import os
import re

import pandas as pd
import requests
import yagmail
from bs4 import BeautifulSoup
from tabulate import tabulate

import imdb_exceptions

LOG = imdb_exceptions.create_logger()


class IMDBTOP250Updater:

    def __init__(self, top250=None, removed_movies=None):
        LOG.debug('Initialising IMDBTOP250Updater object')
        if top250 is None:
            top250 = {}
        if removed_movies is None:
            removed_movies = []
        self.top250 = top250
        self.removed_movies = removed_movies
        self.new_movies: list = []
        self.new_movie_flag: bool = False  # to check if new movie added to database so script need to send email report
        LOG.info('IMDBTOP250Updater object created successfully')

    @classmethod
    def import_database(cls):
        """
        Import top 250 user database json file.
        Loading two variables self.top250 and self.removed_movies.
        :return: class object
        """

        try:
            LOG.debug('Trying to read json file, import user top 250 database')
            with open('database/imdb_db.json') as file:
                data = json.load(file)
                LOG.info(f'Import {file.name} finished')
                return cls(data['top250'], data['removed_movies'])

        except Exception as e:
            LOG.exception('Failed to import database')
            raise IOError(f'Error while importing database. {e}')

    def save_database(self) -> bool:
        """
        Save top 250 user database json file.
        :return: True
        """

        try:
            LOG.debug('Trying to write json file, save user top 250 database')
            if not os.path.isdir('database'):
                os.mkdir('database')
            with open('database/imdb_db.json', 'w') as file:
                json.dump({'top250': self.top250, 'removed_movies': self.removed_movies}, file,
                          indent=4, separators=(',', ': '))
                LOG.info(f'Save {file.name} finished')
                return True

        except Exception as e:
            LOG.exception('Failed to save database')
            raise IOError(f'Error while saving database, {e}')

    def create_list(self, check_seen: bool = False, export_dict: bool = False) -> bool or dict:
        """
        Web Scrapping top 250 list and checking new movies that not in database and asking user for seen or not.
        :param export_dict: Exporting dict if True
        :param check_seen: True to ask user for seen / not seen movies.
        :return: True or dict
        """

        try:
            LOG.debug('Trying to web scrap imdb website')
            url = 'https://www.imdb.com/chart/top'
            response = requests.get(url)

        except Exception as e:
            LOG.exception('Failed to get respond from imdb website')
            raise ConnectionError(f'Failed to get respond from imdb website, {e}')

        soup = BeautifulSoup(response.text, 'lxml')

        movies = soup.select('td.titleColumn')
        links = ['https://www.imdb.com' + a.attrs.get('href') for a in soup.select('td.titleColumn a')]

        soup.select('td.ratingColumn')
        rating = [b.attrs.get('title') for b in soup.select('td.ratingColumn strong')]

        top250 = {}

        for index in range(0, len(movies)):
            movie_string = movies[index].get_text()
            movie = (' '.join(movie_string.split()).replace('.', ''))
            movie_title = movie[len(str(index)) + 1:-7].strip()
            year = re.search('\\((.*?)\\)', movie_string).group(1)

            if int(year) >= 1990:

                if not check_seen:
                    seen = None
                    top250[str(index + 1)] = {'Movie': movie_title, 'Year': year, 'Rating': rating[index], 'Seen': seen,
                                              'Link': links[index]}
                else:
                    while 1:
                        try:
                            user_input = int(input(f'Have you seen {movie_title} ?\nType 1 / 0 ....'))
                            print()
                            break
                        except ValueError:
                            print('Try Again Please !')

                    if user_input == 1:
                        seen = True
                        top250[str(index + 1)] = {'Movie': movie_title, 'Year': year, 'Rating': rating[index],
                                                  'Seen': seen,
                                                  'Link': links[index]}

                    elif user_input == 0:
                        seen = False
                        top250[str(index + 1)] = {'Movie': movie_title, 'Year': year, 'Rating': rating[index],
                                                  'Seen': seen,
                                                  'Link': links[index]}

        if export_dict:
            return top250

        else:
            self.top250 = top250
            LOG.info('Finished creating new movies list')
            return True

    def print_movies(self) -> bool:
        """
        Print all movies in top 250 user database.
        :return: True
        """
        for i, movie in self.top250.items():
            print(f'{i}. {list(movie.values())[:4]}')
        return True

    def remove_movie(self, title: str = None, place: int or str = None) -> bool:
        """
        Remove movies from self.top250.
        :param title: str, example: 'Joker'
        :param place: int, example: 126
        :return: True
        """
        if title:
            for i, movie in self.top250.items():
                if movie['Movie'] == title:
                    removed_movie = self.top250.pop(i)
                    self.removed_movies.append(removed_movie['Movie'])
                    LOG.info(f'{title} has been removed from list')
                    break

        elif place:
            place = str(place)
            for i, movie in self.top250.items():
                if int(i) == int(place):
                    removed_movie = self.top250.pop(i)
                    self.removed_movies.append(removed_movie['Movie'])
                    LOG.info(f'{removed_movie["Movie"]} has been removed from list')
                    break

        LOG.info(f'Movie {title}, {place} removed from list')
        return True

    def update_list(self) -> bool:
        """
        Update self.top250 with added new movies to original top 250 from imdb website.
        :return: True
        """
        updated_list = self.create_list(check_seen=False, export_dict=True)
        current_movies = [val['Movie'] for val in self.top250.values()]
        new_top250 = {}

        for i, movie in updated_list.items():
            if movie['Movie'] in current_movies:
                new_top250[i] = movie

                current_key = [key for key, val in self.top250.items() if val['Movie'] == movie['Movie']][0]
                new_top250[i]['Seen'] = self.top250[current_key]['Seen']

            elif movie['Movie'] in self.removed_movies:
                continue

            else:
                LOG.info(f'NEW MOVIE ADDED !!!!\n'
                         f'{i}. {movie["Movie"]} / ({movie["Year"]}) / {movie["Rating"]} / {movie["Seen"]}')
                new_top250[i] = movie
                self.new_movies.append(movie['Link'])
                self.new_movie_flag = True

        self.top250 = new_top250
        LOG.info('Finish updating movies list')
        return True

    def change_seen_status(self, title: str = '', place: int or str = '', status: bool = None) -> bool:
        """
        Check movies seen status if current status is None.
        Note: Use parameters if you want to change specific movie seen status.
        :param title: str, example: 'The Dark Knight'
        :param place: int or str, example: 2
        :param status: bool, True or False.
        :return: True
        """
        if title:
            i = [key for key, val in self.top250.items() if val['Movie'] == title][0]
            self.top250[i]['Seen'] = status
            LOG.info(f'{title} status has been updated')
            return True

        elif place:
            place = str(place)
            if place in self.top250:
                self.top250[place]['Seen'] = status
                LOG.info(f'{self.top250[place]["Movie"]} status has been updated')
                return True

            else:
                LOG.info(f'Movie in place {place} did not found in list')
                return True

        else:
            for i, movie in self.top250.items():
                if movie['Seen'] is None:
                    while 1:
                        try:
                            user_input = int(input(f'Have you seen {movie["Movie"]} ?\nType 1 / 0 ....'))
                            print()
                            break
                        except ValueError:
                            print('Try Again Please !')

                    if user_input == 1:
                        movie['Seen'] = True

                    elif user_input == 0:
                        movie['Seen'] = False

            return True

    def unseen_movies(self, df_email: bool = False):
        """
        Exporting unseen movies (seen status is None).
        :param df_email: True for returning html data frame for email usage.
        :return: list or tabulate html
        """
        top = copy.deepcopy(self.top250)
        unseen, place = [], []

        for i, movie in top.items():
            if movie['Seen'] in [None, False]:
                link = movie['Link']
                title = movie['Movie']
                movie['Movie'] = f'<a href={link}>{title}</a>'
                place.append(i)
                unseen.append(movie)

        if df_email:
            df = pd.DataFrame(data=unseen)
            df.insert(0, 'Place', place)
            df = df.drop(['Seen', 'Link'], axis=1)
            tabulate.PRESERVE_WHITESPACE = True
            return tabulate(df, headers='keys', tablefmt='html', numalign='center', stralign='center', showindex=False)

        else:
            return unseen

    def new_movie_details(self) -> list:
        """
        Getting links of imdb movie url, poster image and trailer for new movie add to top 250.
        :return: list
        """
        contents = []
        for i, url in enumerate(self.new_movies, start=1):

            try:
                LOG.debug(f'Trying to web scrap {url}')
                response = requests.get(url)
                soup_new_movies = BeautifulSoup(response.text, 'lxml')

            except Exception as e:
                LOG.exception(f'Failed to web scrap {url}')
                raise imdb_exceptions.WebScrapEvents(f'Failed to web scrap {url}', e)

            poster_tag = soup_new_movies.findAll('div', {'class': 'poster'})
            poster_link = str(poster_tag).split('src=')[1].split(' title')[0]

            trailer_tag = soup_new_movies.findAll('div', {'class': 'slate'})
            trailer_link = "https://www.imdb.com" + str(trailer_tag).split('href="')[1].split('"> <img')[0]

            contents.append((url, poster_link, trailer_link))

        LOG.info('Finish web scrapping for new movies')
        return contents

    def send_email(self, receiver_email: str, sender_mail: str, sender_password: str) -> bool:
        """
        Sending email report with new movies that added and all unseen movies by user.
        :return: True
        """
        if self.new_movie_flag:
            subject = 'IMDB TOP 250 Updater'
            imdb = 'https://ia.media-imdb.com/images/M/MV5BMTczNjM0NDY0Ml5BMl5BcG5nXkFtZTgwMTk1MzQ2OTE@._V1_.png'

            top = '<br><br><center><body>' \
                  '<p><h2><b>IMDB 250 Top Rated Update Notice</b></h2></p>' \
                  f'<p><h3><b>{len(self.new_movies)} Movies Added</b></h3></p>' \
                  '</body></center>'

            def movie_item(link, poster, trailer):

                place = [key for key, val in self.top250.items() if val['Link'] == link]
                movie = [movie for movie in self.top250.values() if movie['Link'] == link]

                body = '<br><center><body>' \
                       f'<p><h3>{place[0]} / {" / ".join((list(movie[0].values())[:3]))}</h3></p><br>' \
                       f'<img src={poster} alt="Poster" align="middle"/>' \
                       f'<a href="{link}"><img src={imdb} width="80" height= "80" align="middle" alt="IMDB Link"/></a><br>' \
                       '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -<br>' \
                       '</body></center>'

                return body

            contents = [top]

            for item in self.new_movie_details():
                contents.append(movie_item(item[0], item[1], item[2]))

            contents.append(f'<br><center><h3><u>Unseen Movies List ({len(self.unseen_movies())})</u></h3>'
                            f'{self.unseen_movies(df_email=True)}</center>')

            bottom = f'<br><br><big>End of notice.</big>' \
                     f'<br><small>Sent with TOP250Updater.</small>'

            contents.append(bottom)

            try:
                LOG.debug('Trying to send email.')
                yag = yagmail.SMTP(sender_mail, sender_password)
                yag.send(receiver_email, subject, contents)
                LOG.info(f'Email sent successfully to {receiver_email}')
                return True

            except Exception as e:
                LOG.exception(f'Failed to send email message')
                raise ConnectionError(f'Error in sending email message, {e}')

        else:
            LOG.info('No need to send email message')
            return True

    def delete_seen_from_email(self, email_address: str, password: str) -> bool:
        """
        Checking email inbox for replying emails for taking actions - updating seen status or deleting from user DB.
        example: email content - 'delete: 2' or 'seen: 150'
        :return: True
        """
        LOG.debug('Start check email replays for actions')

        global need_to_delete
        need_to_delete = False

        def read_email_from_gmail(delete_emails=False):
            LOG.debug('Logging to email account to read messages')
            smtp_server = "imap.gmail.com"

            mail = imaplib.IMAP4_SSL(smtp_server)
            mail.login(email_address, password)
            mail.select('inbox')

            mail_type, data = mail.search(None, 'ALL')

            if delete_emails:
                emails_list = data[0].split()
                for num in emails_list:
                    mail.store(num, '+FLAGS', r'(\Deleted)')
                mail.expunge()
                mail.close()
                mail.logout()
                LOG.info(f'{len(emails_list)} Emails deleted')
                return

            mail_ids = data[0]
            id_list = mail_ids.split()

            if len(id_list) == 0:
                LOG.info('Inbox is empty')
                return
            else:
                global need_to_delete
                need_to_delete = True

            first_email_id = int(id_list[0])
            latest_email_id = int(id_list[-1])

            delete, seen = [], []

            for i in range(latest_email_id, first_email_id - 1, -1):
                typ, data = mail.fetch(str(i), '(RFC822)')

                raw_email = data[0][1]

                for string in str(raw_email).lower().split('\\n'):
                    if 'seen:' in string and string.startswith('seen:'):
                        seen.append(string.split('\\r')[0].strip('seen: '))

                    if 'delete:' in string and string.startswith('delete:'):
                        delete.append(string.split('\\r')[0].strip('delete: '))

            LOG.info('Finish reading email messages for actions')
            return list(set(seen)), list(set(delete))

        lists = read_email_from_gmail()

        if lists is not None:
            to_seen, to_delete = lists

            if to_delete:
                for place in to_delete:
                    self.remove_movie(place=place)

            if to_seen:
                for place in to_seen:
                    self.change_seen_status(place=place, status=True)

        if need_to_delete:
            read_email_from_gmail(delete_emails=True)

        LOG.info('Finished check email replays and changing user database')
        return True
