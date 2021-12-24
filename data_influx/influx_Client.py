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
        get all db List according to the influx setting
        *remove the 1st useless name (defalut information)*

        :return: db_list
        :rtype: List

        Note는 추가 사항 적기 가능 \n
        see also는 특정 함수를 지정해서 링크처럼 이동할 수 있음

        Note
        ----
        note test

        see also
        ------------
        :attr:`switch_MS`
        :attr:`measurement_list`
        :attr:`get_data`
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
        get :guilabel:`all measurement` list related to the db

        :param db_name: database
        :type db_name: string

        :return: measurement list
        :rtype: List
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

        :return: measurement list
        :rtype: List
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

        :param intDataInfo: intDataInfo
        :type intDataInfo: dic

        :return: MSdataset
        :rtype: Dict

        """
        MSdataSet ={}
        print(intDataInfo)
        for i, dbinfo in enumerate(intDataInfo['db_info']):
            print(i)
            print(dbinfo)
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

        :param db_name: database 
        :type db_name: string
        :param ms_name: measurement
        :type ms_name: string
        """
        self.db_name = db_name 
        self.ms_name = ms_name
        self.DBClient.switch_database(db_name)

        
    def get_fieldList(self, db_name, ms_name):
        """
        Get :guilabel:`all feature(field)` list of the specific measurement.

        **Influx Query**::

            show field keys on {ms_name}

        :param db_name: database 
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: fieldList in measurement
        :rtype: List
        """
        self.switch_MS(db_name, ms_name)
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(self.DBClient.query(query_string).get_points(measurement=ms_name))
        fieldList = list(x['fieldKey'] for x in fieldkeys)

        return fieldList


    def get_first_time(self, db_name, ms_name):
        """
        Get the :guilabel:`first data` of the specific mearuement

        **Influx Query**::

            select * from {ms_name} LIMIT 1

        :param db_name: database
        :type db_name: string
        :param ms_name: measurement
        :type ms_name: string

        :return: first time in data
        :return: String
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
        Get the :guilabel:`last data` of the specific mearuement

        **Influx Query**::

            select * from {ms_name} ORDER BY DESC LIMIT 1

        :param db_name: database
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: last time in data
        :rtype: String
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
        Get :guilabel:`all data` of the specific mearuement

        **Influx Query**::

            select * from {ms_name}
        
        :param db_name: database
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: df, measurement data
        :rtype: DataFrame
        """
        self.switch_MS(db_name, ms_name)
        query_string = "select * from "+'"'+ms_name+'"'+""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def get_data_by_time(self, bind_params, db_name, ms_name):
        """
        Get data of the specific measurement based on :guilabel:`start-end duration`
        *get_datafront_by_duration(self, start_time, end_time)*

        Example
            >>> ex> bind_params example
            >>> bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                            'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        
        **Influx Query**::

            select * from {ms_name} where time >= {start_time} and time < {end_time}

        :param bind_params: end time & start time
        :type bind_params: dictionary

        :param db_name: database 
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'select * from "'+ms_name+'" where time >= $start_time and time < $end_time'
        df = pd.DataFrame(self.DBClient.query(query_string, bind_params = bind_params).get_points())
        df = self.cleanup_df(df)
        return df


    def get_data_by_days(self, bind_params, db_name, ms_name):
        """
        Get data of the specific mearuement based on :guilabel:`time duration` (days)

        **Example**::

            ex> bind_param example
            bind_params = {'end_time': 1615991400000, 'days': '7d'}


        **Influx Query**::

            select * from {ms_name} where time >= bind_params["end_time"] - bind_params["days"]


        :param bind_params: end time & duration days
        :type bind_params: dictionary

        :param db_name: database
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame

        """
        self.switch_MS(db_name, ms_name)
        #query_string = 'select * from "'+ms_name+'" where time >= '+bind_params["end_time"]+" - "+bind_params["days"]
        query_string = 'select * from "'+ms_name+'" where time >= '+"'"+bind_params["end_time"]+"'"+" - "+bind_params["days"]
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df


    def get_datafront_by_num(self, number, db_name, ms_name):
        """
        Get the :guilabel:`first N number` data from the specific measurement
        

        **Influx Query**::

            select * from {ms_name} limit {number}


        :param db_name: database 
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, first N(number) row data in measurement
        :rtype: DataFrame
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df


    def get_dataend_by_num(self, number, db_name, ms_name):
        """
        Get the :guilabel:`last N number` data from the specific measurement

        **Influx Query**::

            select * from {ms_name} order by desc limit {number}

        :param db_name: database 
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, last N(number) row data in measurement
        :rtype: DataFrame
        """
        self.switch_MS(db_name, ms_name)
        query_string = 'SELECT * FROM "' + ms_name +'" ORDER BY DESC LIMIT '+ str(number) +""
        df = pd.DataFrame(self.DBClient.query(query_string).get_points())
        df = self.cleanup_df(df)
        return df

    def cleanup_df(self, df):
        """
        Clean data, remove duplication, Sort, Set index (datetime)

        - Set index to datetime
        - Remove duplication
        - Sort ascending
        - Replace blank to Nan

        :param df: dataFrame
        :type df: dataFrame

        :return: df, data setting
        :rtype: DataFrame
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
        :param db_name: database
        :type db_name: string
        :param ms_name: database
        :type ms_name: string

        :return: freq
        :rtype: Dict
        """
        data = self.get_datafront_by_num(10,db_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import FrequencyRefine
        return {"freq" : str(FrequencyRefine().get_frequencyWith3DataPoints(data))}


    def get_tagList(self, db_name, ms_name):
        """
        Get :guilabel:`all tag keys` list of the specific measurement. \n
       
        **Influx Query**::

            show tag keys on {ms_name}

        :param db_name: database
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :return: tagList, measurement tag keys
        :rtpye: List
        
        """
        self.switch_MS(db_name, ms_name)
        query_string = "SHOW tag KEYS"
        tagkeys = list(self.DBClient.query(query_string).get_points(measurement=ms_name))
        tagList = list(x['tagKey'] for x in tagkeys)

        return tagList


    def get_TagGroupData(self, db_name, ms_name, tag_key, tag_value):
        """
        Get :guilabel:`tag value` set by tag key

        **Influx Query**::

            select * from ms_name WHERE {tag_key} = {tag_value}


        :param db_name: database
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :param tag_key: tag key
        :type tag_key: string

        :param tag_value: selected tag key data
        :type tag_value: string

        :return: new dataframe
        :rtype: DataFrame
        
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
        Get :guilabel:`unique value` of selected tag key


        **Influx Query**::

            show tag values with key = {tag_key}


        :param db_name: database
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :param tag_key: select tag key data
        :type tag_key: string

        :return: unique tag value list
        :rtype: List
        """

        self.switch_MS(db_name, ms_name)
        query_string = 'show tag values with key = ' + tag_key
        print(query_string)
        tag_value = list(self.DBClient.query(query_string).get_points())
        value_list = list(x['value'] for x in tag_value)

        return value_list



# MSdataSet ={}
#         for i, dbinfo in enumerate(intDataInfo['db_info']):
#             print(i)
#             print(dbinfo)
#             db_name = dbinfo['db_name']
#             ms_name = dbinfo['measurement']
#             self.switch_MS(db_name, ms_name)
#             bind_params = {'end_time': dbinfo['end'], 'start_time': dbinfo['start']}
#             MSdataSet[i] =self.get_data_by_time(bind_params, db_name, ms_name)
#             MSdataSet[i].index.name ='datetime'

#         return MSdataSet

# if __name__ == "__main__":
#     from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

#     intDataInfo = {"db_info":
#                 [
#                 {"db_name": "air_indoor_어린이집",
#                 "measurement":"ICW0W2100153", 
#                 "end": "2021-08-20T21:54:00Z", 
#                 "start":"2021-02-09T16:17:00Z"}, 
#                 {"db_name": "air_indoor_어린이집",
#                 "measurement":"ICW0W2100152", 
#                 "end": "2021-09-01T00:01:00Z", 
#                 "start":"2021-02-09T16:17:00Z"}
#                 ]
#             }

#     test = influxClient(ins)
#     ms_dataset = test.get_MeasurementDataSet(intDataInfo)
#     print(ms_dataset)
#     print(type(ms_dataset))
