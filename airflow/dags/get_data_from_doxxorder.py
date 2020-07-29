# from datetime import timedelta, datetime
# import requests
# import json
# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine
# import configparser
# import os

# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator

# config = configparser.ConfigParser()
# main_path = os.path.dirname(__file__)
# config.read('{}/config.txt'.format(main_path))
# db_host = config.get('config', 'db_host')
# db_user = config.get('config', 'db_user')
# db_pass = config.get('config', 'db_pass')
# db_port = config.get('config', 'db_port')
# db_name = config.get('config', 'db_name')

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2019, 3, 30),
#     "email": ["airflow@airflow.com"],
#     "retries": 0,
#     "retry_delay": timedelta(minutes=2),
# }

# dag = DAG(
#     "get_data_from_odoo",
#     default_args=default_args,
#     description="Get data covid from api",
#     schedule_interval=None,
#     catchup=False,
# )

# def get_data_from_doxxorder(**kwargs):
#     db_host = kwargs.get('db_host')
#     db_user = kwargs.get('db_user')
#     db_pass = kwargs.get('db_pass')
#     db_port = kwargs.get('db_port')
#     db_name = kwargs.get('db_name')
#     main_path = kwargs.get('main_path')
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
# #     print('Success!!!')

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
#     dag=dag,
# )

# t1_get_data



# t1

# {
#     'searchCriteria[filter_groups][0][filters][0][field]': 'created_at', 
#     'searchCriteria[filter_groups][0][filters][0][value]': datetime.datetime(2020, 6, 24, 3, 6, 52), 
#     'searchCriteria[filter_groups][0][filters][0][condition_type]': 'gt', 
#     'searchCriteria[page_size]': 10000, 
#     'searchCriteria[current_page]': 1
# }
