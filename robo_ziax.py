# connection
from sqlalchemy import create_engine
from funcs import get_conn
# data analysis 
import pandas as pd
import requests
import json
from pprint import pprint
import datetime as dt
from datetime import timedelta
import logging
# for DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook


def extract_transform_load(**kwargs):
    logging.info(["################# Проверка соединения с сервером ziax.ru ################### ",dt.datetime.now().strftime("%H:%M:%S")])
    print(requests.get('https://mog.ziax.ru'))

    logging.info(["################# Авторизация и получение списка диалогов Агента ################### ",dt.datetime.now().strftime("%H:%M:%S")])
    # адрес сервера
    url = 'https://mog.ziax.ru/userapi/v1/history/search'
    # параметры авторизации
    header_params = {
        'Authorization': 'Bearer 9153faf79cd70178b72e9bb5c210db94685d76d8d1056667a13eb84d3623a285',
        'Content-Type': 'application/json'
        }
    # получение списка диалогов Агента МОГ Жалобы начиная со дня старта и на текущий момент
    response = requests.post(url, data=json.dumps({"agent_id":14, "from":"2023-10-30 00:00:00", "to": dt.datetime.now().isoformat()}), headers=header_params)
    result = response.json()
    # из полученного списка диалогов (словарей) извлекаем значение chat_id и по нему извлекаем сами диалоги
    data = result['data']
    url_dialog = 'https://mog.ziax.ru/userapi/v1/history/show'
    # создаем пустой список и пишем в него диалоги
    rows = []
    for x in data:
        response = requests.post(url_dialog, data=json.dumps({"chat_id": x['chat_id']}), headers=header_params)
        result = response.json()
        rows.append(result)
    # сохраняем в датафрейм
    df = pd.DataFrame(rows)
    print(df.head())
    # разворачиваем диалоги
    df = df.data.apply(pd.Series)
    df = df.drop('history', axis=1) # временно удаляем столбец
    print(df.head())
    # пишем таблицу в БД
    mssql = MsSqlHook(mssql_conn_id='SERVICES')
    rpt = get_conn(mssql,"mssql+pymssql")
    logging.info(["################# Запись - старт ################### ",dt.datetime.now().strftime("%H:%M:%S")])
    df.to_sql("robo_ziax_agent_id_14", con=rpt, index=False, if_exists='replace')
    logging.info(["################# Запись - финиш ################### ",dt.datetime.now().strftime("%H:%M:%S")])
    

with DAG(dag_id='ROBO_ZIAX_agent_id_14',
         default_args = {
                'owner': 'glazole',
                'start_date':dt.datetime(2023,8,4,19,0,0),
                'email': ['pbi@mosoblgaz.ru'], 
                'email_on_failure': True},
         schedule_interval='0 3 * * *',
         start_date=dt.datetime(2023,8,4,19,0,0),
         tags=['телефон', 'робот', 'урс'],
         catchup=False,

    ) as dag:

    agent_id_14 = PythonOperator(
        task_id = 'robo_ziax_agent_id_14',
        python_callable = extract_transform_load,
        dag=dag,
        trigger_rule = 'all_done'
    )


agent_id_14