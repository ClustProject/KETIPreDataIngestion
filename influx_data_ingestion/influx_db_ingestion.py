import numpy as np
from sklearn.preprocessing import MinMaxScaler
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import time
from datetime import datetime
from datetime import date, timedelta
from influxdb import InfluxDBClient

class Influx_management:
    def __init__(self, host_, port_, user_, pass_, dbname, protocol, table=""):
        self.client = DataFrameClient(host_, port_, user_, pass_, dbname)
        self.protocol = protocol
        self.table = dbname if(table == "") else table
    
    def get_all_db_list(self):
        return self.client.get_list_database()
        
    def get_df_by_time(self, time_start, time_end, table):
        
        query_string = "select * from "+ table +" where time>='" + time_start+"' and time<='"+time_end+"' " 
        result = self.client.query(query_string)[table]
        #print("Data Length:", len(result))
        result = result.groupby(result.index).first()
        result.index = pd.to_datetime(result.index)
        result = result.drop_duplicates(keep='first')
        #print("After removing duplicates:", len(result))
        result = result.sort_index(ascending=True)
        return result
    
    def get_df_all(self, dbname):
        
        query_string = "select * from "+ dbname 
        result = self.client.query(query_string)[dbname]
        print("Data Length:", len(result))
        result = result.groupby(result.index).first()
        result.index = pd.to_datetime(result.index)
        #result = result.drop_duplicates(keep='first')
        print("After removing duplicates:", len(result))
        result = result.sort_index(ascending=True)
        return result

    def get_raw_data(self,start,end):
        self.data = self.get_df_by_time(
            start, end, self.table).sort_index(axis=1)
        self.data.replace("", np.nan, inplace=True)
        return self.data
    
    def get_start_end_day(self, result):
        time_end = pd.to_datetime(result.last('1s').index.values[-1])
        time_end = datetime.strftime(time_end, "%Y-%m-%d")
        time_start = pd.to_datetime(result.first('1s').index.values[0])
        time_start = datetime.strftime(time_start, "%Y-%m-%d")
        return time_start, time_end

    def drop_and_recreate_db(self, dbname, result):
        self.client.drop_database(dbname)
        self.client.create_database(dbname)
        if len(result) >0:
             self.client.write_points(result, dbname, protocol=self.protocol )
        return result
    

if __name__ =='__main__':
    import influx_setting as ins
    dbnames=['INNER_AIR','OUTDOOR_AIR','OUTDOOR_WEATHER']
    tables=['KDS1','KDS2','HS1','HS2','sangju']
    
    time_start='2020-09-10'
    time_end='2020-09-25'
    
    dbname=dbnames[0]
    table = tables[3]
    
    print(dbname)
    influx_c = Influx_management(ins.host_, ins.port_, ins.user_, ins.pass_, dbname, ins.protocol)
    result = influx_c.get_df_by_time(time_start,time_end,table)
    print(influx_c.get_start_end_day(result))

        
