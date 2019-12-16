import logging
import os
import shutil
import sys
from datetime import datetime

TODAY = datetime.strftime(datetime.today(), '%d.%m.%Y')
NOW = datetime.now().strftime('%d/%m/%Y %H:%M:%S')


class IMDBError(Exception):
    """
    Base class for exceptions in this module.
    """
    pass


class WebScrapEvents(IMDBError):
    """
    Exceptions raised for errors in web scrapping data from IMDB website.
    """
    def __init__(self, message: str, exception: Exception):
        self.message = message
        self.exception = exception

    def __str__(self) -> str:
        return f'{self.message}, {self.exception}'


def create_logger():
    if not os.path.isdir('logs'):
        os.mkdir('logs')
    LOG = logging.getLogger('IMDB.Logger')
    handler = logging.StreamHandler(sys.stdout)
    LOG.addHandler(handler)
    logging.basicConfig(filename='logs/IMDB_Logger.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S',
                        level=logging.INFO)
    return LOG


def log_error_to_desktop():
    """for creating copy of log file on desktop when schedule script failed to complete"""
    shutil.copy2('logs/IMDB_Logger.log', f'logs/IMDB_Logger_{TODAY}.log')
    try:
        shutil.move(f'logs/IMDB_Logger_{TODAY}.log', f'C:/Users/{os.getlogin()}/Desktop')
    except shutil.Error:
        os.remove(f'logs/IMDB_Logger_{TODAY}.log')
