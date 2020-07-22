from datetime import timedelta, datetime
import requests
import json
import pandas as pd
import re

authentication_url = 'http://localhost:8069/web/session/authenticate'
url = 'http://localhost:8069/get/order_item'
today = (datetime.today() + timedelta(days=-1) ).strftime("%Y-%m-%d")

def authenticate_odoo(url):
    params = {
        "params":{
            "login":"nong.aukrit@rdbox.co",
            "password":"rd@1234",
            "db":"22_07_2020"
        }
    }
    respones = requests.post(url, json=params)
    json_data = json.loads(respones.text)
    session_id = json_data['result']['session_id']
    return session_id
    
def get_data(url, authentication_url):
    session_id = authenticate_odoo(authentication_url)
    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'session_id={}; frontend_lang=en_US'.format(session_id)
        }
    params = {
        "params":{
            "date": "2020-07-20"
        }
    }
    respones = requests.post(url, headers=headers, data=json.dumps(params))
    data = json.loads(respones.text)
    return data['result']
    
def clean_data(url, authentication_url):
    data = get_data(url, authentication_url)
    if data:
        data = list(map(map_variants, data))
        return data
    
def map_variants(data):
    if data['variants']:
        variants = data['variants']
        for rec in range(0, len(variants)):
            try:
                key_data = list(variants[rec].keys())[0]
                value_data = list(variants[rec].values())[0]
                if key_data == 'MODEL':
                    data.update({
                        'product_name': value_data,
                    })
                elif key_data == 'COLOR':
                    data.update({
                        'color': value_data
                    })
                elif key_data == 'SIZE':
                    data.update({
                        'size': value_data
                    })
                elif key_data == 'FG TYPE':
                    data.update({
                        'category': value_data
                    })
            except:
                pass
        del data['variants']
    else:
        del data['variants']
    return data

data = clean_data(url, authentication_url)

df = pd.DataFrame(data)
df.sort_values(by=['product_name'], inplace=True)
df.to_csv('test.csv', index=False)
