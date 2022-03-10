from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))


class influxClient():
    """
    Influx DB 2.0 Connection
    """

    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(
            url=self.influx_setting["url"], token=self.influx_setting["token"], org=self.influx_setting["org"])

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
        :return: String
        """
        query = f'''from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> first()
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        first_time = query_result["_time"][0].strftime('%Y-%m-%dT%H:%M:%SZ')

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
        :rtype: String
        """
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> last()
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        last_time = query_result["_time"][0].strftime('%Y-%m-%dT%H:%M:%SZ')

        return last_time

    def get_data_by_time(self, bind_params, bk_name, ms_name):
        """
        Get data of the specific measurement based on :guilabel:`start-end duration`
        *get_datafront_by_duration(self, start_time, end_time)*

        Example
            >>> ex> bind_params example
            >>> bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                            'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        

        :param bind_params: end time & start time
        :type bind_params: dictionary

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame
        """
        start_time = bind_params['start_time']
        end_time = bind_params['end_time']

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

    def get_data_by_days(self, bind_params, bk_name, ms_name):
        """
        Get data of the specific mearuement based on :guilabel:`time duration` (days)
        
        **Example**::

            ex> bind_param example
            bind_params = {'end_time': 1615991400000, 'days': '7d'}

        
        :param bind_params: end time & duration days
        :type bind_params: dictionary

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement
        :type ms_name: string

        :return: df, time duration
        :rtype: DataFrame
        """
        end_time = bind_params['end_time']
        days = bind_params['days']

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
        :type db_name: string

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
        :type db_name: string

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
            df = df.set_index('_time')
            df = df.groupby(df.index).first()
            # ).astype('int64')) # strftime('%Y-%m-%dT%H:%M:%SZ')
            df.index = pd.to_datetime(df.index)
            # index의 중복된 행 중 첫째행을 제외한 나머지 행 삭제
            df = df[~df.index.duplicated(keep='first')]
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

        data = self.get_datafront_by_num(10, bk_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import RefineFrequency
        return {"freq": str(RefineFrequency().get_frequencyWith3DataPoints(data))}


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
            start_time = dbinfo['start'].replace(" ", "T") + "Z"
            end_time = dbinfo['end'].replace(" ", "T") + "Z"
            bind_params = {'start_time': start_time, 'end_time': end_time}
            MSdataSet[i] = self.get_data_by_time(bind_params, bk_name, ms_name)
            MSdataSet[i].index.name = 'datetime'

        return MSdataSet


    def get_data_limit_by_time(self, bind_params, number, bk_name, ms_name):
        """
        Get the :guilabel:`limit data` of the specific mearuement based on :guilabel:`time duration` (days)
        
        Example
            >>> ex> bind_params example
            >>> bind_params = {'end_time': query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                            'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        
        :param bind_params: end time & start time
        :type bind_params: dictionary

        :param db_name: number(limit) 
        :type db_name: string

        :param db_name: bucket(database)  
        :type db_name: string

        :param ms_name: measurement 
        :type ms_name: string


        :return: df, time duration
        :rtype: DataFrame
        """
        start_time = bind_params['start_time']
        end_time = bind_params['end_time']

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
        :rtype: string
        """
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> count()
        '''
        data_frame = self.DBClient.query_api().query_data_frame(query)
        data_count = data_frame["_value"][0]

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













# if __name__ == "__main__":
#     from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
#     test = influxClient(ins.CLUSTDataServer2)
#     bk_name="air_indoor_경로당"
#     ms_name="ICL1L2000235"
    # bk_name="air_indoor_초등학교"
    # ms_name="ICW0W2000025"
    # bk_name="bio_covid_infected_world"
    # ms_name="england"
    # bk_name="writetest"
    # ms_name="wt1"
    # bk_name = "finance_korean_stock"
    # ms_name = "stock"

    # bucket_list = test.get_DBList()
    # print("\n-----bucket list-----")
    # print(bucket_list)

    # measurement_list = test.measurement_list(bk_name)
    # print("\n-----measurement list-----")
    # print(measurement_list)

    # filed_list = test.get_fieldList(bk_name, ms_name)
    # print("\n-----field list-----")
    # print(filed_list)

    # data_get = test.get_data(bk_name, ms_name)
    # print("\n-----get_data-----")
    # print(data_get)

    # first_time = test.get_first_time(bk_name, ms_name)
    # print("\n-----first_time-----")
    # print(type(first_time))
    # print(first_time)
    # print("\n")

    # last_time = test.get_last_time(bk_name, ms_name)
    # print("\n-----last_time-----")
    # print(last_time)

    # days = 7
    # bind_params = {'start_time': first_time, 'end_time': last_time, "days":str(days)+"d"}
    # time_data = test.get_data_by_time(bind_params, bk_name, ms_name)
    # print(time_data.head())
    # print(time_data.tail())

    # datafront = test.get_datafront_by_num("20000",bk_name, ms_name)
    # print("===== datafront =====")
    # print(datafront)

    # dataend = test.get_dataend_by_num(20000, bk_name, ms_name)
    # print("===== dataend =====")
    # print(dataend)

    # number = 10
    # bind_params = {'start_time': '2020-02-22T00:00:00Z', 'end_time': '2020-03-22T00:00:00Z'}
    # data_limit_time = test.get_data_limit_by_time(bk_name, ms_name, bind_params, number)
    # print("========== data limit time==========")
    # print(data_limit_time)

    # datafreq = test.get_freq(bk_name, ms_name)
    # print("===== datafreq =====")
    # print(datafreq)

    # datadays = test.get_data_by_days(bind_params, bk_name, ms_name)
    # print(datadays)

    # tag_list = test.get_tagList(bk_name, ms_name)
    # print("===== tag list =====")
    # print(tag_list)

    # tag_key = 'company'
    # tag_value = test.get_TagValue(bk_name, ms_name, tag_key)
    # print("===== tag key value =====")
    # print(tag_value)

    # tag_value = '컴캐스트'
    # tag_data = test.get_TagGroupData(bk_name, ms_name, tag_key, tag_value)
    # print("=======tag data========")
    # print(tag_data)

    # print("====================================")
    # ms_lse = test.measurement_list_only_start_end(bk_name)
    # print(ms_lse)

    # BASE_DIR = os.getcwd()
    # df_file = "/home/leezy/CLUST_KETI/KETIPreDataIngestion/day_wise.csv"
    # input_file = os.path.join(BASE_DIR, df_file)
    # df = pd.read_csv(df_file, parse_dates=True, index_col ='Date')
    # cr_bk = "test1234567"
    # cr_ms = "test1"
    # test.write_db(cr_bk, cr_ms, df)

    # time_start = "2020-09-01T00:00:00Z"
    # time_end = "2021-09-30T23:00:00Z"
    # by_timestamp = test.get_df_by_timestamp(bk_name, ms_name, time_start, time_end)
    # print("============== get_df_by_timestamp ==================")
    # print(by_timestamp)


    # intDataInfo ={ "db_info":[
    #             {"db_name": "air_indoor_경로당",
    #             "measurement":"ICL1L2000235",
    #             "end": "2021-08-31 23:58:00",
    #             "start":"2021-02-04 09:14:00"},
    #             {"db_name": "air_indoor_경로당",
    #             "measurement":"ICL1L2000236",
    #             "end": "2021-08-31 23:58:00",
    #             "start":"2021-02-03 17:18:00"}
    #             ]
    #         }

    # msdataset = test.get_MeasurementDataSet(intDataInfo)
    # print(msdataset)
