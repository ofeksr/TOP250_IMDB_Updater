import os
import shutil

from data.config import *
from exceptions import log_error_to_desktop
from imdb_updater import IMDBTOP250Updater, LOG


def run_script():

    try:
        updater = IMDBTOP250Updater()

        # check for delete or check seen status actions from last replies
        updater.check_email_replies()

        # # enable for testing email
        # updater.top250_db.delete_movie(title='Joker')
        # updater.top250_db.removed_movies_db.delete_movie(title='Joker')

        # update top250 list
        updater.update_top250()

        # send email report with new movies and unseen movies
        if updater.new_movie_flag:
            updater.send_email(receiver_email=receiver_email, sender_mail=sender_mail, sender_password=sender_password)
        else:
            updater.LOG.info('No need to send email message')

        # close connections
        updater.root_db.close_cursor()
        updater.root_db.close_connection()

        updater.root_db.backup_database()

        backup_path = f'C:/Users/{os.getlogin()}/PycharmProjects/Backup Databases'
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)

        shutil.copy(f'database/{updater.root_db.db_name}_backup.sql', backup_path)

        return True

    except Exception:
        LOG.exception('Script not fully finished, error file created')
        log_error_to_desktop()


if __name__ == '__main__':
    run_script()
