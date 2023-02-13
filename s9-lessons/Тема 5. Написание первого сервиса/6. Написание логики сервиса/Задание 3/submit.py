import importlib
import os
import sys

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


def submit(test_name):

    settings_path = os.path.dirname(os.path.abspath(__file__)).split('Тема')[0]
    sys.path.append(settings_path)
    u_settings = importlib.import_module('settings')

    path_s9_srv = u_settings.path_s9_srv

    rlz_file = 'src/stg_loader/stg_message_processor_job.py'

    user_file = f'{path_s9_srv}/{rlz_file}'

    try:
        with open(user_file, 'r', encoding="utf8") as u_file:
            user_code = u_file.read()
    except FileNotFoundError:
        print(f'{TerminalColors.WARNING}Не найден файл `{user_file}{TerminalColors.ENDC}`\n\nВыполните условие задачи.\nПроверьте правильность пути переменной path_s9_srv в {settings_path}settings.py')  # noqa
        sys.exit()

    TESTS_HOST_2 = u_settings.TESTS_HOST_2
    student = u_settings.student
    # pg_settings = u_settings.pg_settings

    print(TESTS_HOST_2)

    try:
        r = requests.post(
            f'{TESTS_HOST_2}/{test_name}',
            json={
                "student": student,
                "user_stg_message_processor_job": user_code
            },
            timeout=300
        )

    except Exception as e:
        print(e)
        return

    if r.status_code == 200:
        if 'result_key' in r.json()['response']['payload']:
            print(
                f"\n{TerminalColors.OKGREEN}{r.json()['response']['payload']['message']}{TerminalColors.ENDC}\n")
            print(
                f"Ключ: {TerminalColors.HEADER}{r.json()['response']['payload']['result_key']}{TerminalColors.ENDC}\n")
        else:
            print(
                f"\n{TerminalColors.FAIL}{r.json()['response']['payload']['message']}{TerminalColors.ENDC}\n")
    else:
        print(
            f'Что-то пошло не так, сервер вернул ошибку {r.status_code}\n{test_name}')


if __name__ == '__main__':
    submit('de09050603_StgMessageProcessor')
