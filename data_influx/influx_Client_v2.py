from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket
import sys
import os
import pandas as pd
import datetime
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

UTC_Style = '%Y-%m-%dT%H:%M:%SZ'
class influxClient():
    """
    Influx DB 2.0 Connection

        **Standard Influx Query**::

            from(bucket:"bucket_name")
            |> range(start: start_time, stop: end_time)
            |> filter(fn: (r) => r._measurement == "measurement_name")

        **change result of Influx 2.0 to Influx 1.8**::

            |> drop(columns: ["_start", "_stop", "_measurement"])
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(url=self.influx_setting["url"], token=self.influx_setting["token"], org=self.influx_setting["org"])


    def get_DBList(self):
        """
        get all bucket(Database) list

        :return: db_list
        :rtype: List
        """
        buckets_api = self.DBClient.buckets_api()
        buckets = buckets_api.find_buckets(limit=100).buckets  # bucket list 보여주기 최대 100까지만 가능

        bk_list = []
        for bucket in buckets:
            bk_list.append(bucket.name)

        bk_list.remove('_monitoring')
        bk_list.remove('_tasks')

        return bk_list

    def measurement_list(self, bk_name):
        """
        get all measurement list of specific Bucket

        :param bk_name: bucket(database) 
        :type bk_name: string

        :return: measurement list
        :rtype: List
        """
        query = f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bk_name}")'
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        # print(query_result)
        ms_list = list(query_result["_value"])

        return ms_list

    def measurement_list_only_start_end(self, bk_name):
        """
        Get the only start and end measurement name
        Use this function to reduce the DB load time.

        :param db_name: bucket(database) 
        :type db_name: string
        :return: measurement list
        :rtype: List
        """
        ms_list = []
        ori_ms_list = self.measurement_list(bk_name)
        ori_len = len(ori_ms_list)

        if(ori_len == 1):
            ms_list.append(ori_ms_list[0])
        elif(ori_len == 2):
            ms_list.append(ori_ms_list[0])
            ms_list.append(ori_ms_list[len(ori_ms_list)-1])
        elif(ori_len > 2):
            ms_list.append(ori_ms_list[0])
            ms_list.append("...(+"+str(ori_len-2)+")")
            ms_list.append(ori_ms_list[len(ori_ms_list)-1])

        return ms_list

    def get_fieldList(self, bk_name, ms_name):
        """
        get all field list of specific measurements

        :param db_name: bucket(database) 
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: fieldList in measurement
        :rtype: List
        """
        query = f'''
        import "experimental/query"

        query.fromRange(bucket: "{bk_name}", start:0)
        |> query.filterMeasurement(
            measurement: "{ms_name}")
        |> keys()
        |> distinct(column: "_field")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        field_list = list(query_result["_field"])

        return field_list

    def get_data(self, bk_name, ms_name):
        """
        Get :guilabel:`all data` of the specific mearuement, change dataframe
        
        :param db_name: bucket(database) 
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: df, measurement data
        :rtype: DataFrame
        """
        query = f'''
        from(bucket:"{bk_name}")
        |> range(start: 0, stop: now())
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame

        # first() - 테이블에서 첫번째 레코드 반환
    def get_first_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`first data` of the specific mearuement

        :param db_name: bucket(database) 
        :type db_name: string
        :param ms_name: measurement
        :type ms_name: string

        :return: first time in data
        :return: pandas._libs.tslibs.timestamps.Timestamp
        """
        query = f'''from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> first()
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        first_time = query_result["_time"][0]
        

        return first_time

        # last() - 테이블에서 마지막 레코드 반환
    def get_last_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`last data` of the specific mearuement

        :param db_name: bucket(database) 
        :type db_name: string
        :param ms_name: measurement 
        :type ms_name: string

        :return: last time in data
        :rtype: pandas._libs.tslibs.timestamps.Timestamp
        """
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> last()
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        last_time = query_result["_time"][0]#.strftime('%Y-%m-%dT%H:%M:%S')

        return last_time

    def get_data_by_time(self, start_time, end_time, bk_name, ms_name):
        """
        Get data of the specific measurement based on :guilabel:`start-end duration`
        *get_datafront_by_duration(self, start_time, end_time)*

        :param start_time: start time
        :type start_time: pandas._libs.tslibs.timestamps.Timestamp or string

        :param end_time: end time
        :type end_time: pandas._libs.tslibs.timestamps.Timestamp or string

        :param db_name: database name
        :type db_name: string

        :param ms_name: measurement name
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame
        """
        if isinstance(start_time, str):
            pass
        else: #Not String:
            start_time= start_time.strftime(UTC_Style)
            end_time = end_time.strftime(UTC_Style)

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: {start_time}, stop: {end_time}) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame

    def get_data_by_days(self, end_time, days, bk_name, ms_name):
        """
        Get data of the specific mearuement based on :guilabel:`time duration` (days)

        :param end_time: end time 
        :type end_time: pandas._libs.tslibs.timestamps.Timestamp

        :param days: duration days
        :type days: string ex>'7d'

        :param db_name: database
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame

        """
        end_time = end_time.strftime(UTC_Style)
        query = f'''
        import "experimental"
        from(bucket: "{bk_name}") 
        |> range(start: experimental.subDuration(d: {days}, from: {end_time}), stop: now())
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame

    def get_datafront_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`first N number` data from the specific measurement
        
        :param db_name: number(limit) 
        :type db_name: integer

        :param db_name: bucket(database)   
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, first N(number) row data in measurement
        :rtype: DataFrame
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> limit(n:{number})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame

    def get_dataend_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`last N number` data from the specific measurement

        :param db_name: number(limit) 
        :type db_name: integer

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, last N(number) row data in measurement
        :rtype: DataFrame
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> tail(n:{number})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame


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
        if 'result' in df.columns:
            df = df.drop(['result', 'table'], axis=1)
            if '_time' in df.columns:
                df = df.set_index('_time')
            elif 'time' in df.columns:
                df = df.set_index('time')
            elif 'datetime' in df.columns:
                df = df.set_index('datetime')
            df.index.name ='time'
            
            df = df.groupby(df.index).first()
            df.index = pd.to_datetime(df.index)
            # index의 중복된 행 중 첫째행을 제외한 나머지 행 삭제
            df = df.sort_index(ascending=True)
            df.replace("", np.nan, inplace=True)
        else:
            pass
        return df

    def get_freq(self, bk_name, ms_name): 
        """
        :param db_name: bucket(database)  
        :type db_name: string
        :param ms_name: measurement
        :type ms_name: string

        :return: freq
        :rtype: Dict
        """

        data = self.get_datafront_by_num(10,bk_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import RefineFrequency
        result = str(RefineFrequency().get_frequencyWith3DataPoints(data))
        return result


    """
    def get_df_by_timestamp(self, bk_name, ms_name, time_start, time_end):
        
        # It returns a table that has data on a measurement(table) in the database from time_start to time_end.
        

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: {time_start}, stop: {time_end}) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame
    """

    def get_tagList(self, bk_name, ms_name):
        """
        Get :guilabel:`all tag keys` list of the specific measurement. \n

        :param db_name: bucket(database) 
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :return: tagList, measurement tag keys
        :rtpye: List
        """

        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementTagKeys(bucket: "{bk_name}", measurement: "{ms_name}")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        tag_list_value = list(query_result["_value"])
        tag_list = tag_list_value[4:]

        return tag_list


    def get_TagValue(self, bk_name, ms_name, tag_key):
        """
        Get :guilabel:`unique value` of selected tag key

        :param db_name: bucket(database) 
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :param tag_key: select tag key data
        :type tag_key: string

        :return: unique tag value list
        :rtype: List
        """

        query = f'''
        import "influxdata/influxdb/schema"

        schema.measurementTagValues(
            bucket: "{bk_name}",
            tag: "{tag_key}",
            measurement: "{ms_name}" )
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        tag_value = list(query_result["_value"])

        return tag_value


    def get_TagGroupData(self, bk_name, ms_name, tag_key, tag_value):
        """
        Get :guilabel:`tag value` set by tag key

        :param db_name: bucket(database) 
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
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> filter(fn: (r) => r.{tag_key} == "{tag_value}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)

        return query_result


    # TODO Define Guard code for ms without tags
    def get_MeasurementDataSet(self, intDataInfo):
        """
        Get measurement Data Set according to the dbinfo
        Each function makes dataframe output with "timedate" index.

        :param intDataInfo: intDataInfo
        :type intDataInfo: dic

        :return: MSdataset
        :rtype: Dict
        """
        MSdataSet = {}
        for i, dbinfo in enumerate(intDataInfo['db_info']):
            bk_name = dbinfo['db_name']
            ms_name = dbinfo['measurement']

            MSdataSet[i] = self.get_data_by_time(dbinfo['start'], dbinfo['end'], bk_name, ms_name)
            MSdataSet[i].index.name = 'datetime'

        return MSdataSet


    def get_data_limit_by_time(self, start_time, end_time, number, bk_name, ms_name):
        """
        Get the :guilabel:`limit data` of the specific mearuement based on :guilabel:`time duration` (days)
        
        
        :param start_time: start time
        :type start_time: pandas._libs.tslibs.timestamps.Timestamp

        :param end_time: end time 
        :type end_time: pandas._libs.tslibs.timestamps.Timestamp

        :param db_name: number(limit) 
        :type db_name: integer

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string


        :return: df, time duration
        :rtype: DataFrame
        """
        start_time= start_time.strftime(UTC_Style)
        end_time = end_time.strftime(UTC_Style)
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: {start_time}, stop: {end_time}) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> limit(n:{number})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame


    def get_data_count(self, bk_name, ms_name):
        """
        Get the :guilabel:`data count` from the specific measurement

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: data count
        :rtype: integer
        """
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> count()
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query)
        data_count = int(data_frame["_value"][0])

        return data_count


    def write_db(self, bk_name, ms_name, data_frame):
        """
        Write data to the influxdb
        """
        write_client = self.DBClient.write_api(write_options=ASYNCHRONOUS)
        self.create_bucket(bk_name)
        write_client.write(bucket=bk_name, record=data_frame,
                           data_frame_measurement_name=ms_name)
        print("========== write success ==========")
        self.DBClient.close()

    def create_bucket(self, bk_name):  # write_db 수행 시, bucket 생성 필요
        """
        Create bucket to the influxdb
        """
        buckets_api = self.DBClient.buckets_api()
        buckets_api.create_bucket(bucket_name=bk_name)
        print("========== create bucket ==========")
























if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = influxClient(ins.CLUSTDataServer2)
#     bk_name="air_indoor_경로당"
#     ms_name="ICL1L2000235"
    # bk_name="bio_covid_infected_world"
    # ms_name="england"
    # bk_name = "finance_korean_stock"
    # ms_name = "stock"

    bucket_list = test.get_DBList()
    print(bucket_list)

    # measurement_list = test.measurement_list(bk_name)
    # print(measurement_list)

    # filed_list = test.get_fieldList(bk_name, ms_name)
    # print(filed_list)

    # data_get = test.get_data(bk_name, ms_name)
    # print(data_get)

    # first_time = test.get_first_time(bk_name, ms_name)
    # print(first_time)

    # last_time = test.get_last_time(bk_name, ms_name)
    # print(last_time)

    # days = 7
    # time_data = test.get_data_by_time(start_time, end_time, bk_name, ms_name)

    # datafront = test.get_datafront_by_num("20000",bk_name, ms_name)
    # print(datafront)

    # number = 10
    # bind_params = {'start_time': '2020-02-22T00:00:00Z', 'end_time': '2020-03-22T00:00:00Z'}
    # data_limit_time = test.get_data_limit_by_time(start_time, end_time, number, db_name, ms_name)
    # print(data_limit_time)

    # datafreq = test.get_freq(bk_name, ms_name)
    # print(datafreq)

    # datadays = test.get_data_by_days(end_time,days,  bk_name, ms_name)
    # print(datadays)

    # tag_list = test.get_tagList(bk_name, ms_name)
    # print(tag_list)

    # tag_key = 'company'
    # tag_value = test.get_TagValue(bk_name, ms_name, tag_key)
    # print(tag_value)

    # tag_value = '컴캐스트'
    # tag_data = test.get_TagGroupData(bk_name, ms_name, tag_key, tag_value)
    # print(tag_data)

    # ms_lse = test.measurement_list_only_start_end(bk_name)
    # print(ms_lse)

