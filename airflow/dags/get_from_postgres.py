# import  pandas as pd

# data = pd.read_csv('./test.csv')
# data.reset_index(inplace=False)
# data_dic = data.to_dict('record')
# print(data_dic)


# from datetime import timedelta, datetime
# import requests
# import psycopg2
# import pandas as pd
# import os

# import airflow
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2019, 3, 30),
#     "email": ["airflow@airflow.com"],
#     "retries": 0,
#     "retry_delay": timedelta(minutes=2),
# }

# dag = DAG(
#     "get_data_postgres",
#     default_args=default_args,
#     description="Get data from postgres",
#     schedule_interval="0/2 * * * *",
#     catchup=False,
#     # schedule_interval=timedelta(days=1),
# )

# def get_data_source():
#     # postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
#     # src_conn = postgres_hook.get_conn()
#     print('from OS: ', os.path.dirname(__file__))
#     # records = postgres_hook.get_records(sql="select so.name, so.amount_total from sale_order as so limit 10")
#     # result = pd.DataFrame(records)
#     # print(src_conn)
        
# t1 = PythonOperator(
#     task_id="get_data_source", 
#     python_callable=get_data_source,
#     # op_args=['TEST PRINT PYTHON'],
#     # provide_context=True,
#     dag=dag,
# )

# t1

# import requests

# def authenticate_odoo(url):
#     # url = kwargs.get('authentication_url', '')
#     print(url)
#     params = {
#         "params":{
#             "login":"nong.aukrit@rdbox.co",
#             "password":"rd@1234",
#             "db":"22_07_2020"
#         }
#     }
#     print(params)
#     respones = requests.post(url, json=params)
#     print(respones.text)

# authentication_url = 'http://localhost:8069/web/session/authenticate'
# authenticate_odoo(authentication_url)
