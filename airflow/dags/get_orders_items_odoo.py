# from datetime import timedelta, datetime
# import requests
# import json
# import pandas as pd
# import re

# authentication_url = 'http://localhost:8069/web/session/authenticate'
# url = 'http://localhost:8069/get/order_item'
# today = (datetime.today() + timedelta(days=-1) ).strftime("%Y-%m-%d")

# def authenticate_odoo(url):
#     params = {
#         "params":{
#             "login":"nong.aukrit@rdbox.co",
#             "password":"rd@1234",
#             "db":"22_07_2020"
#         }
#     }
#     respones = requests.post(url, json=params)
#     json_data = json.loads(respones.text)
#     session_id = json_data['result']['session_id']
#     return session_id
    
# def get_data(url, authentication_url):
#     session_id = authenticate_odoo(authentication_url)
#     headers = {
#         'Content-Type': 'application/json',
#         'Cookie': 'session_id={}; frontend_lang=en_US'.format(session_id)
#         }
#     params = {
#         "params":{
#             "date": "2020-07-21"
#         }
#     }
#     respones = requests.post(url, headers=headers, data=json.dumps(params))
#     data = json.loads(respones.text)
#     return data['result']
    
# def clean_data(url, authentication_url):
#     data = get_data(url, authentication_url)
#     if data:
#         print(data)

# clean_data(url, authentication_url)

# # df = pd.DataFrame(data)
# # # df.to_csv('test.csv', index=False, encoding='utf-8')
# # # df.to_excel('test_excel.xls', index=False, encoding='utf-8')
# # df = pd.read_csv('test.csv')
# # print(df)