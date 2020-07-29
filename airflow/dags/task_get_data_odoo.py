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

authentication_url = 'http://localhost:8069/web/session/authenticate'
url = 'http://localhost:8069/get/order_item'

config = configparser.ConfigParser()
main_path = os.path.dirname(__file__)
config.read('{}/config.txt'.format(main_path))
db_host = config.get('config_postgres', 'db_host')
db_user = config.get('config_postgres', 'db_user')
db_pass = config.get('config_postgres', 'db_pass')
db_port = config.get('config_postgres', 'db_port')
db_name = config.get('config_postgres', 'db_name')

username = config.get('config_odoo', 'username')
password = config.get('config_odoo', 'password')
url = config.get('config_odoo', 'url')
odoo_db_name = config.get('config_odoo', 'db_name')

# authentication_url = 'http://localhost:8069/web/session/authenticate'
# url = 'http://localhost:8069/get/order_item'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 3, 30),
    "email": ["airflow@airflow.com"],
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "get_data_from_odoo",
    default_args=default_args,
    description="Get data from postgres",
    schedule_interval=None,
    catchup=False,
)

def authenticate_odoo(url, username, password, db_name):
    url = url + '/web/session/authenticate'
    params = {
        "params":{
            "login":"{}".format(username),
            "password":"{}".format(password),
            "db":"{}".format(db_name)
        }
    }
    respones = requests.post(url, json=params)
    json_data = json.loads(respones.text)
    session_id = json_data['result']['session_id']
    return session_id
    
def get_data(url, session_id):
    url = url + '/get/order_item'
    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'session_id={}; frontend_lang=en_US'.format(session_id)
        }
    params = {
        "params":{
            "date": "2020-07-21"
        }
    }
    respones = requests.post(url, headers=headers, data=json.dumps(params))
    data = json.loads(respones.text)
    return data['result']
    
def get_data_odoo(**kwargs):
    username = kwargs.get('username')
    password = kwargs.get('password')
    url = kwargs.get('url')
    db_name = kwargs.get('db_name')
    main_path = kwargs.get('main_path')
    session_id = authenticate_odoo(url, username, password, db_name)
    data = get_data(url, session_id)
    data = list(map(remove_items, data))
    if data:
        result = pd.DataFrame(data)
        result.to_csv('{}/sale_order.csv'.format(main_path), index=False)

def remove_items(data):
    del data['items']
    return data
    
def save_data_to_postgres(**kwargs):
    db_host = kwargs.get('db_host')
    db_user = kwargs.get('db_user')
    db_pass = kwargs.get('db_pass')
    db_port = kwargs.get('db_port')
    db_name = kwargs.get('db_name')
    main_path = kwargs.get('main_path')
    engin_path = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
    engine = create_engine(engin_path)
    read_data = pd.read_csv('{}/sale_order.csv'.format(main_path), chunksize=1000)
    for df in read_data:
        test = df.to_sql(
            'sale_order', 
            engine,
            index=False,
            if_exists='append'
        )
        print(test)
    
t1_get_data = PythonOperator(
    task_id="get_data_odoo", 
    python_callable=get_data_odoo,
    op_kwargs={
        'username': username,
        'password': password, 
        'url': url, 
        'db_name': odoo_db_name,
        'main_path': main_path,
    },
    dag=dag,
)

t2_save_data = PythonOperator(
    task_id="save_data_to_postgres", 
    python_callable=save_data_to_postgres,
    op_kwargs={
        'db_host': db_host,
        'db_user': db_user, 
        'db_pass': db_pass, 
        'db_port': db_port, 
        'db_name': db_name, 
        'main_path': main_path
        },
    dag=dag,
)

t1_get_data >> t2_save_data


# SELECT
#   $__timeGroupAlias(sol.create_date,$__interval),
#   sum(sol.product_uom) AS "product_uom",
#   pt.name as "product_name"
# FROM sale_order_line as sol
# left join product_product as pp on sol.product_id = pp.id
# left join product_template as pt on pp.product_tmpl_id = pt.id
# WHERE
#   $__timeFilter(sol.create_date)
# GROUP BY 1, pt.name
# ORDER BY 1

# SELECT
#   $__timeGroupAlias(sol.create_date::date,$__interval),
#   sum(sol.product_uom) AS "product_uom",
#   pt.name as "product_name"
# FROM sale_order_line as sol
# left join product_product as pp on sol.product_id = pp.id
# left join product_template as pt on pp.product_tmpl_id = pt.id
# WHERE
#   $__timeFilter(sol.create_date::date) and pt.type = 'product'
# GROUP BY 1, pt.name
# ORDER BY product_uom desc