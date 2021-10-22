import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from influxdb import InfluxDBClient
import pandas as pd

class influxClient():
    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(host=self.influx_setting.host_, port=self.influx_setting.port_, username=self.influx_setting.user_, password = self.influx_setting.pass_)

    def get_DBList(self):
        db_list = []
        ori_db_list = self.DBClient.get_list_database()
        for num in range(1,len(ori_db_list)):
            db_list.append(ori_db_list[num]['name'])
        return db_list

class influxDB():
    def __init__(self, influxClient, db_name):
        self.DBClient = influxClient
        self.db_name = db_name 
        self.DBClient.switch_database(self.db_name)
    
    def measurement_list(self):
        measurement_list = []
        ori_ms_list=self.DBClient.get_list_measurements()
        for num in range(len(ori_ms_list)):
            measurement_list.append(ori_ms_list[num]['name'])
        return measurement_list

    def measurement_list_only_start_end(self):
        measurement_list = []
        ori_ms_list=self.DBClient.get_list_measurements()
        ori_len = len(ori_ms_list)
        if(ori_len==1):
            measurement_list.append(ori_ms_list[0]['name'])
        elif(ori_len==2):
            measurement_list.append(ori_ms_list[0]['name'])
            measurement_list.append(ori_ms_list[len(ori_ms_list)-1]['name'])
        elif(ori_len>2):
            measurement_list.append(ori_ms_list[0]['name'])
            measurement_list.append("...(+"+str(ori_len-2)+")")
            measurement_list.append(ori_ms_list[len(ori_ms_list)-1]['name'])
        return measurement_list

class influxMeasurementSet():
    def __init__(self, DBClient, intDataInfo):
        self.DBClient = DBClient
        self.intDataInfo = intDataInfo

    def get_MeasurementDataSet(self):
        MSdataSet ={}
        for i, dbinfo in enumerate(self.intDataInfo['db_info']):
            db_name = dbinfo['db_name']
            ms_name = dbinfo['measurement']
            infMS= influxMeasurement(self.DBClient, db_name, ms_name)
            bind_params = {'end_time': dbinfo['end'], 'start_time': dbinfo['start']}
            MSdataSet[i] =infMS.get_data_by_time(bind_params)
            MSdataSet[i].index.name ='datetime'

        return MSdataSet

class influxMeasurement():
    def __init__(self, influxClient, db_name, ms_name):
        self.DBClient = influxClient
        self.db_name = db_name 
        self.ms_name = ms_name
        self.DBClient.switch_database(self.db_name)

    def get_fieldList(self):
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(self.DBClient.query(query_string).get_points(measurement=self.ms_name))
        fieldList = list(x['fieldKey'] for x in fieldkeys)

        return fieldList
    
    def get_first_data(self):
        query_string = "select * from "+self.ms_name+""+" ORDER BY ASC LIMIT 1"
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_last_data(self):
        query_string = "select * from "+self.ms_name+""+" ORDER BY DESC LIMIT 1"
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data(self):
        query_string = "select * from "+self.ms_name+""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_time(self, bind_params):
        """
        bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
        'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        """
        print(self.ms_name)
        query_string = "select * from "+self.ms_name+" where time >= $start_time and time < $end_time"
        df = pd.DataFrame(self.DBClient.query(query_string, bind_params = bind_params).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_days(self, bind_params):
        """
        bind_params = {'end_time': 1615991400000, 
        'days': '7d"}
        """
        query_string = "select * from "+self.ms_name+' where time >= '+bind_params["end_time"]+' - '+bind_params["days"]
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_datafront_by_duration(self, start_time, end_time):
        query_string = "select * from "+self.ms_name+" where time >= '"+start_time+"' and time <= '"+end_time+"'" 
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_datafront_by_num(self, number):
        query_string = "SELECT * FROM " + self.ms_name +" LIMIT "+ number +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_dataend_by_num(self, number):
        query_string = "SELECT * FROM " + self.ms_name +" ORDER BY DESC LIMIT "+ number +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
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