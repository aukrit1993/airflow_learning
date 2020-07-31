from datetime import timedelta, datetime
import requests
import json
import psycopg2
from sqlalchemy import create_engine
import configparser
import os
import re

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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
            "date": "2020-01-01"
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
    
    if data:
        order_items = list(map(get_order_item, data))
        sale_order = list(map(remove_items, data))
        with open('{}/order_items.json'.format(main_path), 'w') as outfile:
            json.dump(order_items, outfile)
        with open('{}/sale_order.json'.format(main_path), 'w') as outfile:
            json.dump(sale_order, outfile)
        
def remove_items(data):
    del data['items']
    return data

def get_order_item(data):
    return list(data['items'])
    
def save_data_to_postgres(**kwargs):
    db_host = kwargs.get('db_host')
    db_user = kwargs.get('db_user')
    db_pass = kwargs.get('db_pass')
    db_port = kwargs.get('db_port')
    db_name = kwargs.get('db_name')
    main_path = kwargs.get('main_path')
    engin_path = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
    engine = create_engine(engin_path)
    with open('{}/sale_order.json'.format(main_path)) as json_file:
        sale_order = json.load(json_file)
    with open('{}/order_items.json'.format(main_path)) as json_file:
        order_items = json.load(json_file)
    order_items = json.loads(json.dumps(order_items).encode('utf-8'))
    sale_orders = json.loads(json.dumps(sale_order).encode('utf-8'))
    index = 0
    connection = engine.connect()
    for data in sale_orders:
        insert_order_psql = """INSERT INTO sale_order(customer_name, address, amount, total_qty, 
                                                        sale_date, sale_channel, dealer_name)
	                        VALUES ('{}', '{}', {}, {}, '{}', '{}', '{}');""".format(
                                data.get('customer_name', ''),
                                data.get('address', ''),
                                data.get('amount', ''),
                                data.get('total_qty', ''),
                                data.get('sale_date', ''),
                                data.get('sale_channel', ''),
                                data.get('dealer_name', '')
                                )
        connection.execute(insert_order_psql)
        get_id_psql = """select id from sale_order order by id DESC limit 1;"""
        order_id = connection.execute(get_id_psql)
        order_id = list(order_id)
        order_id = order_id[0][0]
        item_index = 0
        for item in range(0, len(order_items[index]) - 1):
            order_items[index][item].update({
                'order_id': int(order_id)
            })
            item_dic = order_items[index][item]
            insert_item_psql = """INSERT INTO order_item (product_name, qty, unit_price, amount, 
                                                            category, color, size, order_id, brand, shirt_type)
                        VALUES('{}', {}, {}, {}, '{}', '{}', '{}', {}, '{}', '{}') """.format(
                                re.sub(r'\-.*', "", item_dic.get('product_name', '')),
                                item_dic.get('qty', ''),
                                item_dic.get('unit_price', ''),
                                item_dic.get('amount', ''),
                                item_dic.get('category', ''),
                                item_dic.get('color', ''),
                                item_dic.get('size', ''),
                                item_dic.get('order_id', ''),
                                item_dic.get('brand', ''),
                                item_dic.get('shirt_type', '')
                                )
            connection.execute(insert_item_psql)
        index += 1
        
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
