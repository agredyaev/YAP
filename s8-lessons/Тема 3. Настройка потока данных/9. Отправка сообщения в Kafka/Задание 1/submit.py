import os
import sys
import importlib
import time

import requests

EXITCODE = '-=#de_exit#=-'

class TerminalColors:
        HEADER = '\033[95m'
        OKBLUE = '\033[94m'
        OKCYAN = '\033[96m'
        OKGREEN = '\033[92m'
        WARNING = '\033[93m'
        FAIL = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'

def submit(t_code, rlz_file=''):
    user_code = ''
    if rlz_file:
        full_lesson_path = os.path.dirname(os.path.abspath(__file__))
        user_file = f'{full_lesson_path}/{rlz_file}'

        with open(user_file, 'r') as u_file:
            user_code = u_file.read()

    settings_path = os.path.dirname(os.path.abspath(__file__)).split('Тема')[0]
    settings_file = f'{settings_path}/settings.py'
    with open(settings_file) as settings:
        user_settings = settings.read()

    sys.path.append(settings_path)
    u_settings = importlib.import_module('settings')
    if u_settings.USER_HOST == 'xx.xx.xx.xx':
        print('\nУкажите в settings.py свой хост\n')
        return
    USER_HOST = u_settings.USER_HOST

    print(f'HOST: {USER_HOST}')

    try:
        while True:
            r = requests.post(
                f'http://{USER_HOST}:3002',
                json={
                    "code": user_code,
                    "test": t_code,
                    "conn": user_settings
                    },
                # timeout=3
            )
            if user_code:
                user_settings = f'{user_settings}\nDE_91_RUN =True\n'
            user_code = ''


            if EXITCODE in r.json()['stdout']:
                print(r.json()['stderr'].replace('__test',rlz_file[:-3]).replace(EXITCODE,''))
                print(r.json()['stdout'].replace('__test',rlz_file[:-3]).replace(EXITCODE,''))
                break

            if len(r.json()['stderr']) > 1:
                print(r.json()['stderr'].replace('__test',rlz_file[:-3]))
            if len(r.json()['stdout']) > 1:
                print(r.json()['stdout'].replace('__test',rlz_file[:-3]))

    except requests.exceptions.Timeout as e: 
        print(e)
        return

if __name__ == '__main__':
    submit(
        'de08030901',
        'realization.py'
    )

