import os

dbuser = os.environ["YAP_POSTGRES_LOGIN"]
password = os.environ["YAP_POSTGRES_PASS"]
host = os.environ["YAP_POSTGRES_HOST"]


TESTS_HOST = "51.250.8.141"
# "62.84.117.31"
TESTS_HOST_2 = "https://postgres-check-service.sprint9.tgcloudenv.ru"

student = "avgredyaev"  # ваш_логин
pg_settings = {
    "host": host,  # хост_вашего_postgresql
    "port": 6432,  # порт_вашего_postgresql
    "dbname": "sprint9dwh",  # название_бд
    "username": dbuser,  # имя_пользователя_для_подключения
    "password": password,  # пароль_для_подключения
}

# укажите полный путь до папки с sprint-9-sample-service
# например '/home/user/sp9/sprint-9-sample-service' - для linux или WSL
# 'C:/Users/username/s9/sprint-9-sample-service' - для Windows, используйте в пути / вместо \ иначе получите ошибку
path_s9_srv = "/home/bakhee/Documents/YAP/s9-service/service_stg"
