"""
Agents for using Google Services: Gmail, Calendar, Keep
Note:
    Must login before doing anything else with agents.
"""

__version__ = '1.3'
# v1.0 - first stable release
# v1.1 - added google calendar class
# v1.2 - added order_list_items_by_date function
# v1.3 - added gmail class

import base64
import logging
import os
import pickle
import re
import sys
from datetime import datetime
from email.mime.text import MIMEText

import keyring
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

LOG = logging.getLogger('Google.Agents.Logger')
handler = logging.StreamHandler(sys.stdout)
LOG.addHandler(handler)


class GmailAgent:
    """
    https://developers.google.com/gmail/api/quickstart/python
    """

    def __init__(self):
        LOG.debug('Initialising GmailAgent object')
        self.__credentials = None
        self.__service = None
        LOG.info('GmailAgent object created successfully')

    def login(self):
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('database/gmail_token.pickle'):
            with open('database/gmail_token.pickle', 'rb') as token:
                self.__credentials = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not self.__credentials or not self.__credentials.valid:
            if self.__credentials and self.__credentials.expired and self.__credentials.refresh_token:
                self.__credentials.refresh(Request())
            else:
                SCOPES = ['https://mail.google.com/', 'https://www.googleapis.com/auth/gmail.modify',
                          'https://www.googleapis.com/auth/gmail.compose', 'https://www.googleapis.com/auth/gmail.send']
                flow = InstalledAppFlow.from_client_secrets_file(
                    'database/credentials.json', SCOPES)
                self.__credentials = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open('database/gmail_token.pickle', 'wb') as token:
                pickle.dump(self.__credentials, token)
                LOG.info('Gmail token accepted')

        self.__service = build('gmail', 'v1', credentials=self.__credentials)
        return True

    def list_messages_from_inbox(self) -> list:
        response = self.__service.users().messages().list(userId='me', q='label:inbox ').execute()
        messages = []
        if 'messages' in response:
            messages.extend(response['messages'])

        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = self.__service.users().messages().list(userId='me',
                                                              q='label:inbox',
                                                              pageToken=page_token).execute()
            messages.extend(response['messages'])
        return messages

    def get_message(self, message_id: str) -> str:
        message = self.__service.users().messages().get(userId='me', id=message_id, format='full').execute()
        msg_str = base64.urlsafe_b64decode(message['payload']['parts'][0]['body']['data']).decode('utf-8')
        return msg_str

    @staticmethod
    def create_message(sender: str, to: str, subject: str, message_text: str):
        message = MIMEText(message_text)
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject
        return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

    def send_message(self, message: str) -> bool:
        self.__service.users().messages().send(userId='me', body=message).execute()
        LOG.info('message sent')
        return True

    def delete_message(self, message_id: str) -> bool:
        """make sure there is a message to delete in inbox"""
        self.__service.users().messages().delete(userId='me', id=message_id).execute()
        LOG.info(f'message {message_id} moved to trash')
        return True


class GoogleCalendarAgent:
    """
    https://developers.google.com/calendar/quickstart/python
    """

    def __init__(self):
        LOG.debug('Initialising GoogleCalendarAgent object')
        self.__credentials = None
        self.__service = None
        LOG.info('GoogleCalendarAgent object created successfully')

    def login(self) -> bool:
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('database/calendar_token.pickle'):
            with open('database/calendar_token.pickle', 'rb') as token:
                self.__credentials = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not self.__credentials or not self.__credentials.valid:
            if self.__credentials and self.__credentials.expired and self.__credentials.refresh_token:
                self.__credentials.refresh(Request())
            else:
                SCOPES = ['https://www.googleapis.com/auth/calendar']
                flow = InstalledAppFlow.from_client_secrets_file(
                    'database/credentials.json', SCOPES)
                self.__credentials = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open('database/calendar_token.pickle', 'wb') as token:
                pickle.dump(self.__credentials, token)
                LOG.info('Google Calendar token accepted')

        self.__service = build('calendar', 'v3', credentials=self.__credentials)
        return True

    def add_events(self, date: str, summary: str, description: str, delete: bool = False):

        event = {
            'summary': summary,
            'description': description,
            'start': {
                'date': date,
            },
            'end': {
                'date': date,
            },
            'reminders': {
                'useDefault': True,
            },
        }

        inserted_event = self.__service.events().insert(calendarId='primary', body=event).execute()
        LOG.info(f'Event created in calendar: {inserted_event.get("htmlLink")}')

        if delete:
            self.__service.events().delete(calendarId='primary', eventId=inserted_event['id']).execute()

        return True


class GoogleKeepAgent:
    """
    https://gkeepapi.readthedocs.io/en/latest/
    """

    def __init__(self):
        LOG.debug('Initialising GoogleKeepAgent object')
        self.keep = gkeepapi.Keep()
        LOG.info('GoogleKeepAgent object created successfully')

    def login(self, email_address: str, password: str, token: str = None) -> bool:
        """
        Logging in to Google Account, new login if saved token not exists or accepted.
        :return: True
        """
        LOG.debug('Trying to login to Google keep')

        try:  # in case that token not accepted
            if token:
                LOG.debug('Trying to resume with saved token')
                self.keep.resume(email_address, token)

        except:
            try:
                LOG.exception('Failed to get Google Keep token')
                # generate app password if you have Two Factor enabled! otherwise, use account password.
                self.keep.login(email_address, password)

                token = self.keep.getMasterToken()
                keyring.set_password('google-keep-token', username, token)
                LOG.debug('New token saved in keyring')

            except Exception:
                LOG.exception('Failed to log in to Google Keep')
                raise

        LOG.info('Logged in to Google keep')
        return True

    def add_events_to_list(self, list_id: str, events: list, top: bool = False, bottom: bool = False) -> bool:
        """
        Adding new events to list.
        :param bottom: True means item will be inserted to bottom of list.
        :param top: True means item will be inserted to top of list.
        :param list_id: str taken from Google Keep list URL.
        :param events: list of events.
        :return: True
        """
        LOG.debug('Trying to get specific list from Google Keep')
        g_list = self.keep.get(list_id)

        for event in events:

            if top:
                g_list.add(event, False, gkeepapi.node.NewListItemPlacementValue.Top)

            elif bottom:
                g_list.add(event, False, gkeepapi.node.NewListItemPlacementValue.Bottom)

        self.keep.sync()
        LOG.info(f'{len(events)} events added to "{g_list.title}" list')
        return True

    def order_list_items_by_date(self, list_id: str):
        """To reposition an item with `item.sort = int`.
        larger number is closer to the top"""
        # get items of list
        g_list = self.keep.get(list_id)
        g_list_items = g_list.items

        # create list of dates from items text
        dates_of_items = []
        for item in g_list_items:
            pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{4}')
            summary = re.findall(pattern, item.text)
            dates_of_items.append(summary[0])

        # sort dates by date
        dates_to_sort = sorted([datetime.strptime(date, '%d/%m/%Y') for date in dates_of_items])
        sorted_dates = [date.strftime('%#d/%m/%Y') for date in dates_to_sort]

        # reposition items in list by sorted dates list
        for item in g_list_items:
            pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{4}')
            summary = re.findall(pattern, item.text)
            item.sort = - int(sorted_dates.index(summary[0]))  # put minus (-) for desc order

        # sync for saving changes
        self.keep.sync()
        return True
