TESTS_HOST = '62.84.117.31'
TESTS_HOST_2 = 'https://postgres-check-service.sprint9.tgcloudenv.ru'

student = '*',  # ваш_логин
pg_settings = {
    'host': '*',  # хост_вашего_postgresql
    'port': 6432,  # порт_вашего_postgresql
    'dbname': '*',  # название_бд
    'username': '*',  # имя_пользователя_для_подключения
    'password': '*'  # пароль_для_подключения
}

# укажите полный путь до папки с sprint-9-sample-service
# например '/home/user/sp9/sprint-9-sample-service' - для linux или WSL
# 'C:/Users/username/s9/sprint-9-sample-service' - для Windows, используйте в пути / вместо \ иначе получите ошибку
path_s9_srv = '.../service_stg'
