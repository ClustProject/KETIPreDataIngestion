import sys
import os
from turtle import bk
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket, PostBucketRequest, PatchBucketRequest, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS


class influxClient():
    """
    Influx DB 2.0 Connection
    """

    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(url=self.influx_setting["url"], token=self.influx_setting["token"], org=self.influx_setting["org"])



    def get_DBList(self):
        """
        get all bucket(Database) list
        """

        buckets_api = self.DBClient.buckets_api()
        buckets = buckets_api.find_buckets(limit=100).buckets
        # bucket list 보여주기 최대 100까지만 가능

        bk_list = []
        for bucket in buckets:
            bk_list.append(bucket.name)

        return bk_list    



    def measurement_list(self, bk_name):
        """
        get all measurement list of specific Bucket
        """

        query =f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bk_name}")'

        query_result = self.DBClient.query_api().query_data_frame(query=query)
        ms_list = list(query_result["_value"])

        return ms_list



    def measurement_list_only_start_end(self, bk_name):
        """
        Get the only start and end measurement name
        Use this function to reduce the DB load time.
        """

        ms_list =[]
        ori_ms_list = self.measurement_list(bk_name)
        ori_len = len(ori_ms_list)

        if(ori_len==1):
            ms_list.append(ori_ms_list[0])
        elif(ori_len==2):
            ms_list.append(ori_ms_list[0])
            ms_list.append(ori_ms_list[len(ori_ms_list)-1])
        elif(ori_len>2):
            ms_list.append(ori_ms_list[0])
            ms_list.append("...(+"+str(ori_len-2)+")")
            ms_list.append(ori_ms_list[len(ori_ms_list)-1])

        return ms_list



    def get_fieldList(self, bk_name, ms_name):
        """
        get all field list of specific measurements
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        '''

        query_result = self.DBClient.query_api().query_data_frame(query=query)
        field_result = set(query_result["_field"])
        field_list = list(field_result)
        
        return field_list




    def get_data(self, bk_name, ms_name):
        """
        Get :guilabel:`all data` of the specific mearuement
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        '''

        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query,data_frame_index=["_time"])

        return data_frame



    def get_data2(self, bk_name, ms_name):
        """
        Get :guilabel:`all data` of the specific mearuement, change dataframe
        """
        # 데이터 조회시, result, table을 제외시킬 수가 없음 -> 안보이게 하는 방법이 없나..?

        query = f'''
        from(bucket:"{bk_name}")
        |> range(start: 0, stop: now())
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        '''

        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query)

        return data_frame



    def get_first_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`first data` of the specific mearuement
        """

        query = f'''from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> limit(n:1)
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        first_time = query_result["_time"][0].strftime('%Y-%m-%dT%H:%M:%SZ')

        return first_time



    def get_last_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`last data` of the specific mearuement
        """
    
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> sort(desc:true) 
        |> limit(n:1)
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        last_time = query_result["_time"][0].strftime('%Y-%m-%dT%H:%M:%SZ')

        return last_time



    def get_data_by_time(self, bind_params, bk_name, ms_name):
        """
        Get data of the specific measurement based on :guilabel:`start-end duration`
        *get_datafront_by_duration(self, start_time, end_time)*
        """
        # 불러오는 start_time end_time 시간이 TZ 형식이 아니라서 오류 발생..?
        # ex. TZ형식 -> 2020-02-28T10:00:000Z
        #       현재 -> 2020-02-28 10:00:00+00:00

        start_time = bind_params['start_time']
        end_time = bind_params['end_time']
        
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: {start_time}, stop: {end_time}) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        #query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        #ex> params = {'end_time':end_time, 'start_time': start_time}
        
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)
    
        return data_frame



    def get_data_by_days(self, bind_params, bk_name, ms_name):
        """
        Get data of the specific mearuement based on :guilabel:`time duration` (days)
        """
        end_time = bind_params['end_time']
        days = bind_params['days']

        query = f'''
        import "experimental"
        from(bucket: "{bk_name}") 
        |> range(start: experimental.subDuration(d: {days}, from: {end_time}), stop: now())
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        #query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        #ex> params = {'end_time':end_time, 'start_time': start_time}
        
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)
    
        return data_frame



    def get_datafront_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`first N number` data from the specific measurement
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> limit(n:{number})
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)

        return data_frame



    def get_dataend_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`last N number` data from the specific measurement
        """

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> sort(desc:true)
        |> limit(n:{number})
        |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)

        return data_frame



    def get_freq(self, bk_name, ms_name):
        """
        """
        data = self.get_datafront_by_num(10, bk_name, ms_name)
        from KETIPrePartialDataPreprocessing.data_refine.frequency import FrequencyRefine
        return {"freq" : str(FrequencyRefine().get_frequencyWith3DataPoints(data))}



    def get_tagList(self, bk_name, ms_name):
        """
        Get :guilabel:`all tag keys` list of the specific measurement. \n
        """

        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementTagKeys(bucket: "{bk_name}", measurement: "{ms_name}")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        tag_list =list(query_result["_value"])

        return tag_list



    def get_TagValue(self, bk_name, ms_name, tag_key):
        """
        Get :guilabel:`unique value` of selected tag key
        """
        # 값이 안 나오는 이유를 못 찾고 있음

        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementTagValues(bucket: "{bk_name}", measurement: "{ms_name}", tag: "{tag_key}")
        '''
        tag_list = self.DBClient.query_api().query(query=query)

        return tag_list



    def get_TagGroupData(self, bk_name, ms_name, tag_key, tag_value):
        """
        Get :guilabel:`tag value` set by tag key
        """



    def get_MeasurementDataSet(self, intDataInfo):
        """
        Get measurement Data Set according to the dbinfo
        Each function makes dataframe output with "timedate" index.
        """
        # intDataInfo가 Dict로 들어오는데 2.0에서 어떻게 처리해야할지 모르겠음



    def cleanup_df(self, df):
        """
        Clean data, remove duplication, Sort, Set index (datetime)
        """



    def get_df_by_timestamp(self, ms_name, time_start, time_end):
        """
        It returns a table that has data on a measurement(table) in the database from time_start to time_end.
        """



    def write_db(self, bk_name, ms_name, df):
        """Write data to the influxdb
        """
        # .....?
        write_client = self.DBClient.write_api(write_options= ASYNCHRONOUS)
        self.create_bucket(bk_name)
        write_client.write(bucket=bk_name, record=df, data_frame_measurement_name=ms_name)
        print("========== write success ==========")
        self.DBClient.close()


    
    def create_bucket(self, bk_name):
        buckets_api = self.DBClient.buckets_api()
        buckets_api.create_bucket(bucket_name=bk_name)
        print("========== create bucket ==========")










if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = influxClient(ins.CLUSTLocalInflux)
    bk_name="bio_covid_infected_world"
    ms_name="england"
    # bk_name="writetest"
    # ms_name="wt1"

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

    # data_get2 = test.get_data2(bk_name, ms_name)
    # print("\n-----get_data2-----")
    # print(data_get2)

    # first_time = test.get_first_time(bk_name, ms_name)
    # print("\n-----first_time-----")
    # print(first_time)

    # last_time = test.get_last_time(bk_name, ms_name)
    # print("\n-----last_time-----")
    # print(last_time)

    # days = 7
    # bind_params = {'start_time': first_time, 'end_time': last_time, "days":str(days)+"d"}

    # time_data = test.get_data_by_time(bind_params, bk_name, ms_name)
    # print(time_data.head())
    # print(time_data.tail())

    # datafront = test.get_datafront_by_num(10,bk_name, ms_name)
    # print(datafront)

    # datafreq = test.get_freq(bk_name, ms_name)
    # print(datafreq)

    # datadays = test.get_data_by_days(bind_params, bk_name, ms_name)
    # print(datadays)

    # dataend = test.get_dataend_by_num(10, bk_name, ms_name)
    # print(dataend)

    # tag_keys = test.get_tagList(bk_name, ms_name)
    # print(tag_keys)

    # tag_key = '_start'
    # tag_value = test.get_TagValue(bk_name, ms_name, tag_key)
    # print(tag_value)

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