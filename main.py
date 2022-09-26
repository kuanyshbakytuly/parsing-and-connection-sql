import pandas as pd
import pymssql
import requests
import sqlalchemy as sql
import urllib3
import warnings
from time import sleep

warnings.filterwarnings('ignore')
urllib3.disable_warnings()
pool_timeout = 30

def pd_show():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


headers = '{...}' #add your headers 

user = 'SA'
password = 'reallyStrongPwd123'
server = '127.0.0.1'
port = 1433
database = 'AZURE'
conn = sql.create_engine(url=f"mssql+pymssql://{user}:{password}@{server}:{port}/{database}", encoding='utf-8') #connection with sql server

register = input()
meta_url = f'https://data.egov.kz/meta/{register}/data?pretty' 
sql_file = f"{register}" #the name of file in sql server

#creating dtype for each column
def getting_meta_data(url): 
    r = requests.get(url, headers=headers, verify=False)
    d = {}
    pattern_dtype = {
        'Int': sql.types.INTEGER,
        'String': sql.types.NVARCHAR,
        'Datetime': sql.types.DATETIME,
        'Boolean': sql.types.NVARCHAR,
    }
    for k, v in r.json()['fields'].items():
        columns_type = v['type']
        if k == 'modified':
            d[k] = pattern_dtype['Datetime']
        elif k == 'actual':
            d[k] = pattern_dtype['Boolean']
        else:
            d[k] = pattern_dtype[columns_type]
    return d


meta_data = getting_meta_data(url=meta_url)
dtype = meta_data

#quering from sql server to get buildings those have not been installed yet
def recieveing_data_fromsql():
    quore = conn.execute(
        "select b.id from s_buildings_copy as b left join s_pb as p on b.id = p.s_building_id where p.s_building_id is NULL order by 1;")
    id_areas = []
    for row in quore:
        id_areas.append(str(row)[1:-2])
    return id_areas

#quering from sql server to delete buildings those have been already installed
def deleting_data_fromsql(which):
    quore = conn.execute(
        f"DELETE FROM s_buildings_copy WHERE id ={which};")

elements = recieveing_data_fromsql()

c = 0
data = pd.DataFrame(columns=list(meta_data.keys())) #creating empty dataframe with columns

#parsing
for i in elements:
    c += 1
    count = 0
    length = 1000 #because the site can provide max = 1000

    if c % 1000: #some buildings dont have apartments
        data.to_sql(sql_file, dtype=dtype, con=conn, if_exists='append') #sending dataframe to sql server
        data = pd.DataFrame(columns=list(meta_data.keys())) 
    while length == 1000:
        #query
        url = f'https://data.egov.kz/api/v4/{register}/data?apiKey=9b5655c01992497bb7e4f3d3c5cefd3d&source=' \
              + '{"from": ' + str(count * 1000) + ',"size": 1000,' \
                                                  '"query":{' \
                                                  '"bool":{' \
                                                  '"must": [' \
                                                  '{"match": {"s_building_id":' + i + '}}' \
                                                                                      ']' \
                                                                                      '}' \
                                                                                      '}' \
                                                                                      '}'
        count += 1
        #request with headers -
        r = requests.get(url, headers=headers, verify=False) 
        try:
            if r.status_code != 204:
                rjs = r.json()
                l = len(rjs)
                if l == 0:
                    deleting_data_fromsql(which=i)
                    length = 0
                else:
               
                    data.append(pd.DataFrame(rjs), ignore_index=True)
                    length = l
        except ValueError:
            count -= 1
            sleep(5) #necessary part because of avoiding blocks by the site
        print(f'{i}: {c} --- {count}')
