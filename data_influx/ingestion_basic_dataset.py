# set modules path
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from requests.api import head
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

class BasicDatasetRead():
    def __init__(self, influx_setting, db_name, measurement_name):
        self.influx_setting = influx_setting
        self.db_name = db_name
        self.ms_name = measurement_name
        self.influxdb = InfluxDBClient(host=self.influx_setting.host_, port=self.influx_setting.port_, username=self.influx_setting.user_, password = self.influx_setting.pass_)
        self.influxdb.switch_database(self.db_name)
    
    def get_first_data(self):
        query_string = "select * from "+self.ms_name+""+" ORDER BY ASC LIMIT 1"
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_last_data(self):
        query_string = "select * from "+self.ms_name+""+" ORDER BY DESC LIMIT 1"
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)

        return df
    def get_data(self):
        query_string = "select * from "+self.ms_name+""
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_time(self, bind_params):
        """
        bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
        'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        """
        print(self.ms_name)
        query_string = "select * from "+self.ms_name+" where time >= $start_time and time < $end_time"
        df = pd.DataFrame(self.influxdb.query(query_string, bind_params = bind_params).get_points())
        df = self.cleanup_df(df)
        return df

    def get_datafront_by_duration(self, start_time, end_time):
        query_string = "select * from "+self.ms_name+" where time >= '"+start_time+"' and time <= '"+end_time+"'" 
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_datafront_by_num(self, number):
        query_string = "SELECT * FROM " + self.ms_name +" LIMIT "+ number +""
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_dataend_by_num(self, number):
        query_string = "SELECT * FROM " + self.ms_name +" ORDER BY DESC LIMIT "+ number +""
        df = pd.DataFrame(self.influxdb.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    

    def cleanup_df(self, df):
        import numpy as np
        if 'time' in df.columns:
            df = df.set_index('time')
        elif 'datetime' in df.columns:
            df = df.set_index('datetime')
        df = df.groupby(df.index).first()
        df.index = pd.to_datetime(df.index)#).astype('int64'))
        df = df.drop_duplicates(keep='first')
        df = df.sort_index(ascending=True)
        df.replace("", np.nan, inplace=True)

        return df

if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = BasicDatasetRead(ins, 'air_outdoor', 'V02Q1941015')

    #print(test.get_data_by_time('2020-09-10T00:36:00Z', '2020-09-10T01:36:00Z'))
    #print(test.get_datafront_by_num('5'))
    #print(test.get_dataend_by_num('5'))

    df = test.get_data()




