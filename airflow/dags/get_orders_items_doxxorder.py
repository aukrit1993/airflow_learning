from datetime import timedelta, datetime
import requests
import json
import pandas as pd
import re

url = 'http://35.247.175.241'

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

def get_data_doxxorder(url):
    token = authentication(url)
    # today = (datetime.today() + timedelta(days=-1) ).strftime("%Y-%m-%d")
    enpoint = '{}/index.php/rest/TH/V1/getOrders?date={}'.format(url, '2020-06-05')
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token),
    }
    test_get = requests.get(url=enpoint, headers=header, verify=False)
    if test_get.status_code == 200:
        jason_data = json.loads(test_get.text)
        data = list(map(map_data, jason_data))
        return data[0]
        
def map_data(data):
    if data:
        return list(map(map_data_items, data['items']))
    
def map_data_items(data):
    if data:
        data_dic = {
            'customer_name': '',
            'age': '',
            'product_name': re.sub(r'\-.*', "", data['name']),
            'unit_price': round(float(data['price']), 2),
            'qty': round(float(data['qty']),0),
            'amount': round(float(data['row_total']), 2),
            'sale_date': '',
            'channel_sale': 'doxxorder',
            'category': data['category'],
            'brand': re.sub(r'\®.*', '', data['brand']),
            'color': data['color'],
            'size': data['size'],
            'shirt_type': '',
            'seller': '',
        }
        if 'G' not in str(data_dic['size']).upper():
            data_dic.update({
                'shirt_type': 'M'
            })
        else:
            data_dic.update({
                'shirt_type': 'F'
            })
        return data_dic
    
def create_csv(url):
    data = get_data_doxxorder(url)
    
    if data:
        csv_file = pd.DataFrame(data)
        csv_file.to_csv('./test_data.csv', index=False)
    
create_csv(url)