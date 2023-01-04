import os
import sys
import importlib

import requests

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

    # print(f'{TerminalColors.OKGREEN}Для создания подключения потребуется некоторое время...{TerminalColors.ENDC}')

    try:
        r = requests.post(
            f'http://{USER_HOST}:3002',
            json={
                "code": user_code,
                "test": t_code,
                "conn": user_settings
                },
            timeout=20
        )
    except requests.exceptions.Timeout as e: 
        print(e)
        return

    print(r.json()['stderr'].replace('__test',rlz_file[:-3]))
    print(r.json()['stdout'].replace('__test',rlz_file[:-3]))

if __name__ == '__main__':
    submit(
        'de07050306'
    )


