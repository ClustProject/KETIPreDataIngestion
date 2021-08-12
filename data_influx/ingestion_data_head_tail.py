# set modules path
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
import influxdb
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd


class DataHeadTail():
    def __init__(self, db_parameter, dbname, measuremetename):
        self.db_parameter = db_parameter
        self.dbname = dbname
        self.measurementname = measuremetename
        self.influxdb = InfluxDBClient(host=self.db_parameter.host_, port=self.db_parameter.port_, token=self.db_parameter.token, org=self.db_parameter.org)

    def data_head(self):
        self.influxdb.switch_database(self.dbname)
        query_string = "SELECT * FROM " + self.measurementname +" LIMIT 10"
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())

        return df

    def data_tail(self):
        self.influxdb.switch_database(self.dbname)
        query_string = "SELECT * FROM " + self.measurementname +" ORDER BY DESC LIMIT 10"
        #SELECT * FROM <SERIES> ORDER BY ASC LIMIT 1
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        
        return df.sort_index(ascending=False)




import KETIAppDataServer.db_model.influx_setting_KETI as lds
DBheadtail = DataHeadTail(lds, 'Energy_Solar', 'Jeju')
print(DBheadtail.data_head())
print(DBheadtail.data_tail())
