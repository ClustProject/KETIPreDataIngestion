import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from influxdb import InfluxDBClient
import pandas as pd

class influxClient():
    """
    basic influx DB connection

    """
    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(host=self.influx_setting.host_, port=self.influx_setting.port_, username=self.influx_setting.user_, password = self.influx_setting.pass_)

    def get_DBList(self):
        """
        **get all db List according to the influx setting** \n
        **remove the 1st useless name (defalut information)**

        모든 ``database`` 를 가져온다.

        """
        db_list = []
        ori_db_list = self.DBClient.get_list_database()
        for num in range(1,len(ori_db_list)):
            db_list.append(ori_db_list[num]['name'])
        return db_list

##### DB Function

    def switch_DB(self, db_name):
        """
        **Before explore the specific DB, Switch DB.**

        """
        self.db_name = db_name 
        self.DBClient.switch_database(self.db_name)        
    
    def measurement_list(self, db_name):
        """
        **get all measurement list related to the db**

        선택한 database의 모든 ``measurement`` 를 가져온다


        """
        self.switch_DB(db_name)
        measurement_list = []
        ori_ms_list=self.DBClient.get_list_measurements()
        for num in range(len(ori_ms_list)):
            measurement_list.append(ori_ms_list[num]['name'])
        return measurement_list

    def measurement_list_only_start_end(self, db_name):
        """
        **Get the only start and end measurement name** \n
        **Use this function to reduce the DB load time.**


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
        **Get measurement Data Set according to the dbinfo** \n
        **Each function makes dataframe output with "timedate" index.**

        ?????

        :param intDataInfo: don`t know
        :type intDataInfo: don`t know

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
        **Before getting the specific measurement data and information, switch MS** \n
        변수 사용을 위한 ``초기화``

        :param db_name: database name
        :type db_name: string
        :param ms_name: measurement name
        :type ms_name: string
        """
        self.db_name = db_name 
        self.ms_name = ms_name
        self.DBClient.switch_database(db_name)
        
    def get_fieldList(self, db_name, ms_name):
        """
        **Get all feature(field) list of the specific measurement.** \n
        지정한 measurement의 ``모든 field key`` 를 조회

        **Query**::

            show field keys on {ms_name}

        :param db_name: database name
        :type db_name: string
        :param ms_name: measurement name
        :type ms_name: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(self.DBClient.query(query_string).get_points(measurement=ms_name))
        fieldList = list(x['fieldKey'] for x in fieldkeys)

        return fieldList

    def get_first_time(self, db_name, ms_name):
        """
        **Get the first data of the specific mearuement** \n
        선택한 data의 ``첫 시간 data`` 를 조회

        **Query**::

            select * from {ms_name} LIMIT 1

        :param db_name: database name
        :type db_name: string
        :param ms_name: measurement name
        :type ms_name: string
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
        **Get the last data of the specific mearuement** \n
        선택한 data의 ``마지막 시간 data`` 를 조회

        **Query**::

            select * from {ms_name} ORDER BY DESC LIMIT 1

        :param db_name: database name
        :type db_name: string
        :param ms_name: measurement name
        :type ms_name: string

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
        선택한 measurement의 ``모든 data`` 가져오기

        **Query**::

            select * from {ms_name}
        
        :param db_name: database name
        :type db_name: string
        :param ms_name: measurement name
        :type ms_name: string

        """
        self.switch_MS(db_name, ms_name)
        query_string = "select * from "+'"'+ms_name+'"'+""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_time(self, bind_params, db_name, ms_name):
        """
        **Get data of the specific measurement based on start-end duration**
        *get_datafront_by_duration(self, start_time, end_time)* \n
        ``지정한 기간 사이`` 의 모든 data 조회

        **Example**::

            ex> bind_params example
            bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                            'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        

        **Query**::

            select * from {ms_name} where time >= {start_time} and time < {end_time}

        :param bind_params: end time & start time
        :type bind_params: dictionary

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+'" where time >= $start_time and time < $end_time'
        df = pd.DataFrame(self.DBClient.query(query_string, bind_params = bind_params).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_days(self, bind_params, db_name, ms_name):
        """
        **Get data of the specific mearuement based on time duration (days)**
        
        특정 ``기간``  안의 data 조회

        **Example**::

            ex> bind_param example
            bind_params = {'end_time': 1615991400000, 'days': '7d'}

        **Query**::

            select * from {ms_name} where time >= bind_params["end_time"] - bind_params["days"]

        :param bind_params: end time & duration days
        :type bind_params: dictionary

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string

        """
        self.switch_MS(db_name, ms_name)
        #query_string = 'select * from "'+ms_name+'" where time >= '+bind_params["end_time"]+" - "+bind_params["days"]
        query_string = 'select * from "'+ms_name+'" where time >= '+"'"+bind_params["end_time"]+"'"+" - "+bind_params["days"]
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df


    def get_datafront_by_num(self, number, db_name, ms_name):
        """
        **Get the first N number data from the specific measurement**
        
        data의 ``첫 1행`` 을 조회한다.

        **Query**::

            select * from {ms_name} limit {number}

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_dataend_by_num(self, number, db_name, ms_name):
        """
        **Get the last N number data from the specific measurement**

        data의 ``마지막 1행`` 을 조회한다.

        **Query**::

            select * from {ms_name} order by desc limit {number}

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" ORDER BY DESC LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def cleanup_df(self, df):
        """
        **Clean data, remove duplication, Sort, Set index (datetime)**

        - datetime을 ``index`` 로 설정 
        - ``중복데이터`` 제거
        - ``오름차순`` 으로 정렬
        - 빈 칸을 ``Nan`` 값으로 대체 

        :param df: dataFrame
        :type df: dataFrame
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
        """
        ...??
        """
        data = self.get_datafront_by_num(10,db_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import FrequencyRefine
        return {"freq" : str(FrequencyRefine().get_frequencyWith3DataPoints(data))}

    def get_tagList(self, db_name, ms_name):
        """
        **Get all tag keys list of the specific measurement.** \n
        특정 measurement가 가지고 있는 모든 ``tag key`` 를 출력한다.
       
        **Query**::

            show tag keys on {ms_name}

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = "SHOW tag KEYS"
        tagkeys = list(self.DBClient.query(query_string).get_points(measurement=ms_name))
        tagList = list(x['tagKey'] for x in tagkeys)

        return tagList

    def get_TagGroupData(self, db_name, ms_name, tag_key, tag_value):
        """
        **Get tagvalue set by tag key**

        선택한 ``tag key`` 의 특정 ``data`` 가 가지고 있는 모든 정보를 출력한다.

        **Query**::

            select * from ms_name WHERE {tag_key} = {tag_value}


        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string

        :param tag_key: tag key
        :type tag_key: string

        :param tag_value: select tag key data
        :type tag_value: string
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+'" WHERE "'+tag_key+'"=\''+tag_value+'\''
        print(query_string)
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        print(df)
        return df

    def get_TagValue(self, db_name, ms_name, tag_key):
        """
        **Get tag value distinct**

        선택한 ``tag key`` 가 가지고 있는 값들을 불러온다 ``(중복X)``

        **Query**::

            show tag values with key = {tag_key}
        
        Args:
            db_name: database name
            ms_name: measurement name
            tag_key: tag key
        """

        self.switch_MS(db_name, ms_name)
        query_string = 'show tag values with key = ' + tag_key
        print(query_string)
        tag_value = list(self.DBClient.query(query_string).get_points())
        value_list = list(x['value'] for x in tag_value)

        return value_list

        
