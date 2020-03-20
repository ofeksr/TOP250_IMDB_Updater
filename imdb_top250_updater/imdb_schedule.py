import os
import shutil
import time
from data import config
from logs.exceptions import log_error_to_desktop, datetime
from updater.imdb_updater import IMDBTOP250Updater, LOG


def run_script():
    try:
        updater = IMDBTOP250Updater()

        # check for delete or check seen status actions from last replies
        updater.check_email_replies()

        # enable for testing email_tools
        # updater.top250_db.delete_movie(title='Joker')
        # updater.top250_db.removed_movies_db.delete_movie(title='Joker')

        # update top250 list
        updater.update_top250()

        # send email_tools report with new movies and unseen movies
        if updater.new_movie_flag:
            updater.send_email(receiver_email=config.receiver_email,
                               sender_mail=config.sender_mail, sender_password=config.sender_password)
        else:
            updater.LOG.info('No need to send email_tools message')

        # close connections
        updater.root_db.close_cursor()
        updater.root_db.close_connection()

        updater.root_db.backup_database()

        backup_path = f'C:/Users/{os.getlogin()}/PycharmProjects/Backup Databases/IMDB/MySQL'
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)

        TODAY = datetime.strftime(datetime.today(), '%d.%m.%Y')
        shutil.copy2(f'database/{updater.root_db.db_name}_backup.sql',
                     f'{backup_path}/{updater.root_db.db_name}_backup_{TODAY}.sql')

        time.sleep(3.5)
        return True

    except Exception:
        LOG.exception('Script not fully finished, error file created')
        log_error_to_desktop()


if __name__ == '__main__':
    run_script()
