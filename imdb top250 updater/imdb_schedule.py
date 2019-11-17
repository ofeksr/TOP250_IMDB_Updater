import os
import shutil

import keyring

from imdb_updater import IMDBTOP250Updater
from exceptions import log_error_to_desktop

updater = None


def run_script():
    global updater
    email_address = os.environ.get('imdb_email')
    password = keyring.get_password('imdb-pass', 'ofeksofeks')

    receiver_email = os.environ.get('my_email')
    sender_mail = os.environ.get('imdb_email')
    sender_password = keyring.get_password('imdb-pass', 'ofeksofeks')

    try:

        updater = IMDBTOP250Updater.import_database()

        updater.delete_seen_from_email(email_address=email_address, password=password)

        updater.update_list()
        updater.save_database()

        backup_path = f'C:/Users/{os.getlogin()}/PycharmProjects/Backup Databases'
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)

        shutil.copy('database/imdb_db.json',
                    f'C:/Users/{os.getlogin()}/PycharmProjects/Backup Databases')

        updater.send_email(receiver_email=receiver_email, sender_mail=sender_mail, sender_password=sender_password)

        return True

    except Exception as e:
        IMDBTOP250Updater.LOG.exception('Script not fully finished, error file created')
        log_error_to_desktop(e)


if __name__ == '__main__':
    run_script()
