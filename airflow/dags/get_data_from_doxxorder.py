from datetime import timedelta, datetime
import requests
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import configparser
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.mongo_hook import MongoHook

config = configparser.ConfigParser()
main_path = os.path.dirname(__file__)
config.read('{}/config.txt'.format(main_path))
db_host = config.get('config', 'db_host')
db_user = config.get('config', 'db_user')
db_pass = config.get('config', 'db_pass')
db_port = config.get('config', 'db_port')
db_name = config.get('config', 'db_name')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 3, 30),
    "email": ["airflow@airflow.com"],
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "get_mongodb",
    default_args=default_args,
    description="Get data covid from api",
    schedule_interval="0/1 * * * *",
    catchup=False,
)

# def get_data_from_doxxorder(**kwargs):
#     db_host = kwargs.get('db_host')
#     db_user = kwargs.get('db_user')
#     db_pass = kwargs.get('db_pass')
#     db_port = kwargs.get('db_port')
#     db_name = kwargs.get('db_name')
#     main_path = kwargs.get('main_path')
#     api_data = requests.get('https://api.covid19india.org/data.json')
#     json_data = json.loads(api_data.text)
#     json_data = json_data['cases_time_series']
#     result = pd.DataFrame(json_data)
#     result.to_csv('{}/test.csv'.format(main_path), index=False)
#     engin_path = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
#     engine = create_engine(engin_path)
#     read_data = pd.read_csv('{}/test.csv'.format(main_path), chunksize=1000)
#     for df in read_data:
#         df.to_sql(
#             'covid_19', 
#             engine,
#             index=False,
#             if_exists='append'
#         )
#     print('Success!!!')

# t1_get_data = PythonOperator(
#     task_id="get_data_from_doxxorder", 
#     python_callable=get_data_from_doxxorder,
#     op_kwargs={
#         'db_host': db_host,
#         'db_user': db_user, 
#         'db_pass': db_pass, 
#         'db_port': db_port, 
#         'db_name': db_name, 
#         'main_path': main_path
#         },
#     dag=dag,
# )

# t2_save_data = PythonOperator(
#     task_id="get_data_from_doxxorder", 
#     python_callable=get_data_from_doxxorder,
#     op_kwargs={
#         'db_host': db_host,
#         'db_user': db_user, 
#         'db_pass': db_pass, 
#         'db_port': db_port, 
#         'db_name': db_name, 
#         'main_path': main_path
#         },
#     dag=dag,
# )

def get_mongodb():
    mongodb_test = MongoHook(conn_id='mongodb_id')
    data = mongodb_test.find('order_item', {})
    data = list(map(map_data, data))
    mongodb_test.insert_many('order_item', data)
    data = mongodb_test.find('order_item', {})
    data = list(map(map_data, data))
    print(data)
    
def map_data(data):
    if data:
        data_dic = {
            'title': data['title'],
            'first': data['first'],
            'age': data['age']+1
        }
        return data_dic
    
t1 = PythonOperator(
    task_id="get_mongodb", 
    python_callable=get_mongodb,
    dag=dag,
)

t1

# {
#     'searchCriteria[filter_groups][0][filters][0][field]': 'created_at', 
#     'searchCriteria[filter_groups][0][filters][0][value]': datetime.datetime(2020, 6, 24, 3, 6, 52), 
#     'searchCriteria[filter_groups][0][filters][0][condition_type]': 'gt', 
#     'searchCriteria[page_size]': 10000, 
#     'searchCriteria[current_page]': 1
# }
