import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from influxdb import InfluxDBClient
import pandas as pd

class influxClient():
    """
    basic influx DB connection
    docstring::

        code box test

    
    ``코드 박스`` 테스트중
    

    *italic체*

    **bold체**

    """
    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(host=self.influx_setting.host_, port=self.influx_setting.port_, username=self.influx_setting.user_, password = self.influx_setting.pass_)

    def get_DBList(self):
        """
        get all db List according to the influx setting
        remove the 1st useless name (defalut information)
        """
        db_list = []
        ori_db_list = self.DBClient.get_list_database()
        for num in range(1,len(ori_db_list)):
            db_list.append(ori_db_list[num]['name'])
        return db_list

##### DB Function

    def switch_DB(self, db_name):
        """
        Before explore the specific DB, Switch DB.

        """
        self.db_name = db_name 
        self.DBClient.switch_database(self.db_name)        
    
    def measurement_list(self, db_name):
        """
        get all measurement list related to the db
        """
        self.switch_DB(db_name)
        measurement_list = []
        ori_ms_list=self.DBClient.get_list_measurements()
        for num in range(len(ori_ms_list)):
            measurement_list.append(ori_ms_list[num]['name'])
        return measurement_list

    def measurement_list_only_start_end(self, db_name):
        """
        Get the only start and end measurement name
        Use this function to reduce the DB load time.
        """
        self.switch_DB(db_name)
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
##### MS Set Function

    def get_MeasurementDataSet(self, intDataInfo):
        """
        Get measurement Data Set according to the dbinfo
        Each function makes dataframe output with "timedate" index.

        """
        MSdataSet ={}
        for i, dbinfo in enumerate(intDataInfo['db_info']):
            db_name = dbinfo['db_name']
            ms_name = dbinfo['measurement']
            self.switch_MS(db_name, ms_name)
            bind_params = {'end_time': dbinfo['end'], 'start_time': dbinfo['start']}
            MSdataSet[i] =self.get_data_by_time(bind_params, db_name, ms_name)
            MSdataSet[i].index.name ='datetime'

        return MSdataSet

    ##### MS Function
    def switch_MS(self, db_name, ms_name):
        """
        Before getting the specific measurement data and information, switch MS
        """
        self.db_name = db_name 
        self.ms_name = ms_name
        self.DBClient.switch_database(db_name)
        
    def get_fieldList(self, db_name, ms_name):
        """
        Get all feature(field) list of the specific measurement.
        """
        self.switch_MS(db_name, ms_name)
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(self.DBClient.query(query_string).get_points(measurement=ms_name))
        fieldList = list(x['fieldKey'] for x in fieldkeys)

        return fieldList

    def get_first_time(self, db_name, ms_name):
        """
        Get the first data of the specific mearuement
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+''+'" LIMIT 1'
        first = pd.DataFrame(self.DBClient.query(query_string).get_points()).set_index('time')
        print(first)
        first_time = first.index[0]
        #df = self.cleanup_df(df)
        return first_time

    def get_last_time(self, db_name, ms_name):
        """
        Get the last data of the specific mearuement
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+'" ORDER BY DESC LIMIT 1'
        last = pd.DataFrame(self.DBClient.query(query_string).get_points()).set_index('time')
        print(last)
        #df = self.cleanup_df(df)
        last_time = last.index[0]
        return last_time

    def get_data(self,db_name, ms_name):
        """
        Get all data of the specific mearuement
        """
        """
        지정한  measurement의 ``모든 data`` 가져오기
        docstring::

            from influxdb import InfluxDBClient

            self.switch_MS(db_name, ms_name)
            query_string = "select * from "+'"'+ms_name+'"'+""
            df = pd.DataFrame(self.DBClient.query(query_string).get_points())
            df = self.cleanup_df(df)
            
            return df
        


        :param db_name: ``database name``
        :type db_name: string
        :param ms_name: ``measurement name``
        :type ms_name: string

               
        :returns: dataframe으로 가져온 data저장
        
        :rtype: tuple
        
        :raises ValueError: When ``a`` is not an integer.


        """
        self.switch_MS(db_name, ms_name)
        query_string = "select * from "+'"'+ms_name+'"'+""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_time(self, bind_params, db_name, ms_name):
        """
        Get data of the specific measurement based on start-end duration
        # get_datafront_by_duration(self, start_time, end_time)
        ex> bind_params example
        bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
        'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+'" where time >= $start_time and time < $end_time'
        df = pd.DataFrame(self.DBClient.query(query_string, bind_params = bind_params).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_days(self, bind_params, db_name, ms_name):
        """
        Get data of the specific mearuement based on time duration (days)
        
        ex> bind_param example
        bind_params = {'end_time': 1615991400000, 'days': '7d"}
        """
        self.switch_MS(db_name, ms_name)
        #query_string = 'select * from "'+ms_name+'" where time >= '+bind_params["end_time"]+" - "+bind_params["days"]
        query_string = 'select * from "'+ms_name+'" where time >= '+"'"+bind_params["end_time"]+"'"+" - "+bind_params["days"]
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df


    def get_datafront_by_num(self, number, db_name, ms_name):
        """
        Get the first N number data from the specific measurement
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_dataend_by_num(self, number, db_name, ms_name):
        """
        Get the last N number data from the specific measurement
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" ORDER BY DESC LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def cleanup_df(self, df):
        """
        Clean data, remove duplication, Sort, Set index (datetime)
        """
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

    def get_freq(self, db_name, ms_name):
        data = self.get_datafront_by_num(10,db_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import FrequencyRefine
        return {"freq" : str(FrequencyRefine(data).get_inferred_freq())}
