import requests
import json

url = 'http://10.29.4.242:7474/db/data_treat/transaction/commit'

headers={
         'Accept':'application/json; charset=UTF-8',
         'Content-Type':'application/json',
         'Authorization':'Basic bmVvNGo6d3NxeHk4MjE5MzM4',
         }

payload = {"statements": [{"statement":'MATCH n=(m:小区{name:"北苑家园茉藜园"})-[*..4]-(x:小区) where x.developers = m.developers or x.property_company=m.property_company return x.name,x.developers,x.property_company,x.type_built'}]}


data = requests.post(url=url, headers =headers, data=json.dumps(payload))

print(data.text)