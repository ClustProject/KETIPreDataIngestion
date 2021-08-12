# set modules path
import sys
import os

from requests.api import head
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
#import influxdb
from influxdb_client import InfluxDBClient
import pandas as pd


class DataHeadTail():
    def __init__(self, db_parameter, dbname, measuremetename):
        self.db_parameter = db_parameter
        self.dbname = dbname
        self.measurementname = measuremetename
        self.influxdb = InfluxDBClient(url=self.db_parameter.url, token=self.db_parameter.token, org=self.db_parameter.org)

    def data_head(self):
        #self.influxdb.switch_database(self.dbname)
        query_string = "SELECT * FROM " + self.measurementname +" LIMIT 10"
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())

        return df

    def data_tail(self):
        self.influxdb.switch_database(self.dbname)
        query_string = "SELECT * FROM " + self.measurementname +" ORDER BY DESC LIMIT 10"
        #SELECT * FROM <SERIES> ORDER BY ASC LIMIT 1
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        
        return df.sort_index(ascending=False)



sys.path.append("..")


# query = 'from(bucket: "Energy_Solar\/autogen")\
#   |> filter(fn: (r) => r["_measurement"] == "Jeju")\
#   |> filter(fn: (r) => r["_field"] == "Total solar power")'


# result = test.query_api().query(org="test", query=query)
# print(result)
#DBheadtail = DataHeadTail(lds, 'Energy_Solar', 'Jeju')
#tt = DBheadtail.data_head()

#print(DBheadtail.data_head())
# print(DBheadtail.data_tail())

import requests
from influxdb_client import InfluxDBClient
test = InfluxDBClient(url='http://52.231.185.8:8086', token='pB1ZFiugRNTP8ukSPcmNcouT-JJTbujsPt10ARTj_uSAcKMTBQbbvJaVcCh9dB0TG5X8Z5B1e2xBnB-EkPhXmw==', org='test')
URL = 'http://52.231.185.8:8086/query?db=test' 
headers = {'Authorization': 'Token pB1ZFiugRNTP8ukSPcmNcouT-JJTbujsPt10ARTj_uSAcKMTBQbbvJaVcCh9dB0TG5X8Z5B1e2xBnB-EkPhXmw==',
'Accept': 'application/csv','Content-Type': 'application/json;'}

data = {
  'q': 'SELECT * FROM test.infinite.mem LIMIT 3'
}
response = requests.get(URL,headers=headers, params=data)
print(response.text)
print(response)
print(response.status_code)
