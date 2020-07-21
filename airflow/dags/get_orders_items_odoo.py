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
            "db":"20_07_2020"
        }
    }
    respones = requests.post(url, json=params)
    json_data = json.loads(respones.text)
    session_id = json_data['result']['session_id']
    return session_id
    
# authenticate_odoo(authentication_url)
    
def get_data(url):
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
    print(data['result'])
    

get_data(url)
