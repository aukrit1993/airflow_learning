from datetime import timedelta, datetime
import time
import requests
import json
import psycopg2
from sqlalchemy import create_engine
import configparser
import os
import re

import check_lat_long

from airflow import DAG
from airflow import AirflowException
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
    start_date=datetime(2018, 1, 1),
    schedule_interval='0 3 * * *',
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
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    url = url + '/get/order_item'
    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'session_id={}; frontend_lang=en_US'.format(session_id)
        }
    params = {
        "params":{
            "date_start": "{} 00:00:00".format(yesterday),
		    "date_end": "{} 23:59:59".format(yesterday)
        }
    }
    respones = requests.post(url, headers=headers, data=json.dumps(params))
    data = json.loads(respones.text)
    try:
        if data['result']:
            return data['result']
    except:
        return False
    
def get_data_odoo(**kwargs):
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    username = kwargs.get('username')
    password = kwargs.get('password')
    url = kwargs.get('url')
    db_name = kwargs.get('db_name')
    session_id = authenticate_odoo(url, username, password, db_name)
    data = get_data(url, session_id)
    if data:
        order_items = list(map(get_order_item, data))
        sale_order = list(map(remove_items, data))
        with open('sale_order.json', 'w', encoding='utf-8') as outfile:
            json.dump(sale_order, outfile, ensure_ascii=False)
        with open('order_items.json', 'w', encoding='utf-8') as outfile:
            json.dump(order_items, outfile, ensure_ascii=False)
        print('Path file: ', os.path.isfile('./sale_order.json'))
        print('Finish create json file')
    else:
        print('No Data for {}'.format(yesterday))
        
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
    engin_path = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
    engine = create_engine(engin_path)
    if os.path.isfile('./sale_order.json') and os.path.isfile('./order_items.json'):
        with open('./sale_order.json', 'r') as sale_file:
            sale_order = json.load(sale_file)
        with open('./order_items.json', 'r') as item_file:
            order_items = json.load(item_file)
        order_items = json.loads(json.dumps(order_items).encode('utf-8'))
        sale_orders = json.loads(json.dumps(sale_order).encode('utf-8'))
        index = 0
        connection = engine.connect()
        for data in sale_orders:
            if data.get('province'):
                lat_long = check_lat_long.get_lat_long(data.get('province'))
            insert_order_psql = """INSERT INTO sale_order(customer_name, address, tambon, amphur, province, zip, status,
                                                        amount, total_qty, sale_date, sale_channel, is_booking, latitude, longitude, create_date)
                                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', {}, '{}', '{}', '{}');""".format(
                                    data.get('customer_name', ''),
                                    data.get('address', ''),
                                    data.get('tambon', ''),
                                    data.get('amphur', ''),
                                    data.get('province', ''),
                                    data.get('zip', ''),
                                    data.get('status', ''),
                                    data.get('amount', ''),
                                    data.get('total_qty', ''),
                                    data.get('sale_date', ''),
                                    data.get('sale_channel', ''),
                                    data.get('is_booking'),
                                    str(lat_long[0]),
                                    str(lat_long[1]),
                                    datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                                    )
            connection.execute(insert_order_psql)
            get_id_psql = """select id, sale_date from sale_order order by id DESC limit 1;"""
            sale_order = connection.execute(get_id_psql)
            sale_order = list(sale_order)
            if sale_order:
                sale_order_id = sale_order[0][0]
                sale_order_date = sale_order[0][1].strftime("%Y-%m-%d, %H:%M:%S")
                for item in range(0, len(order_items[index]) - 1):
                    if order_items[index][item]:
                        order_items[index][item].update({
                            'order_id': int(sale_order_id),
                            'sale_date': sale_order_date
                        })
                        item_dic = order_items[index][item]
                        insert_item_psql = """INSERT INTO order_item (product_name, qty, unit_price, amount, 
                                                                        category, color, size, order_id, brand, shirt_type, create_date)
                                    VALUES('{}', {}, {}, {}, '{}', '{}', '{}', {}, '{}', '{}', '{}') """.format(
                                            item_dic.get('product_name', ''),
                                            item_dic.get('qty', ''),
                                            item_dic.get('unit_price', ''),
                                            item_dic.get('amount', ''),
                                            item_dic.get('category', ''),
                                            item_dic.get('color', ''),
                                            item_dic.get('size', ''),
                                            item_dic.get('order_id', ''),
                                            item_dic.get('brand', ''),
                                            item_dic.get('shirt_type', ''),
                                            datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                                            )
                        connection.execute(insert_item_psql)
                index += 1
        print('Finish save data to DW')
    else:
        ValueError('No data to save')
            
def remove_file():
    try:
        os.remove('sale_order.json')
        os.remove('order_items.json')
        print('Removed!!!!')
    except:
        print('Do not have file!!!')
        pass
        
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

# delay_python_task = PythonOperator(
#     task_id="delay_python_task",
#     dag=dag,
#     python_callable=lambda: time.sleep(300)
#     )

remove_file_task = PythonOperator(
    task_id="remove_file_task",
    dag=dag,
    python_callable=remove_file
    )

remove_file_task >> t1_get_data >> t2_save_data
