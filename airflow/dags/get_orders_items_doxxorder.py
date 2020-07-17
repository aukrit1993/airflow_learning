from datetime import timedelta, datetime
import requests
import json
import pandas as pd

url = 'http://35.247.175.241'

condition_search = {
    'searchCriteria[filter_groups][0][filters][0][field]':'created_at',
    'searchCriteria[filter_groups][0][filters][0][value]':'2020-06-24 03:06:52',
    'searchCriteria[filter_groups][0][filters][0][condition_type]':'gt',
    'searchCriteria[page_size]':10000,
    'searchCriteria[current_page]':1,
}

print((datetime.today() + timedelta(days=-1) ).strftime("%Y-%m-%d %H:%M:%S"))

def authentication(url):
    if url:
        url = '{}/index.php/rest/TH/V1/integration/admin/token'.format(url)
        data = {
            "username": "admin",
            "password": "admin123"
        }
        response = requests.post(url=url, json=data)
        
        return response.text.replace('"', '')

def get_data_doxxorder(url, condition_search):
    token = authentication(url)
    enpoint = '{}/index.php/rest/TH/V1/orders/items'.format(url)
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token),
    }
    test_get = requests.get(url=enpoint, headers=header, params=condition_search , verify=False)
    if test_get.status_code == 200:
        jason_data = json.loads(test_get.text)
        jason_data = jason_data['items']
        # print(type(jason_data))
        data = list(map(map_data, jason_data))
        for i in range(0, len(data)-1): 
            if data[i] == None:
                data.pop(i)
            
        return data
        
def map_data(data):
    if data['product_type'] == 'configurable':
        list_name = data['name'].split('-')
        color = data['product_option']['extension_attributes']['configurable_item_options'][0]['option_label']
        size = data['product_option']['extension_attributes']['configurable_item_options'][1]['option_label']
        data_dic = {
            'order_name': 'doxxorder {}'.format(data['order_id']),
            'customer_name': 'test',
            'product_name': list_name[0],
            'unit_price': '{0:.2f}'.format(float(data['price'])),
            'qty': data['qty_ordered'],
            'amount': '{0:.2f}'.format(float(data['row_total'])),
            'sale_date': datetime.strptime(data['created_at'], '%Y-%m-%d %H:%M:%S'),
            'channel_sale': 'doxxorder',
            'category': list_name[1],
            'brand': 'RD',
            'color': color,
            'size': size,
            'shirt_type': 'M',
        }
        return data_dic
    
def create_csv():
    
get_data_doxxorder(url, condition_search)


# create table order_item(id serial primary key, order_name char(255), customer_name char(255), product_name char(255), unit_price numeric, qty integer, amount numeric, sale_date timestamp, channel_sale char(255), category char(255), brand char(255), color char(255), size char(10), shirt_type char(2))