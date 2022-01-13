import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket, PostBucketRequest, PatchBucketRequest
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS



class influxClient2():
    """
    Influx DB 2.0 Connection
    """

    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.DBClient = InfluxDBClient(url=self.influx_setting.url_, token=self.influx_setting.token_, org=self.influx_setting.org_)


    def get_BucketList(self):
        """
        get all bucket list
        """

        buckets_api = self.DBClient.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        bk_list = []
        for bucket in buckets:
            bk_list.append(bucket.name)

        return bk_list    


    def measurement_List(self, bk_name):
        """
        get all measurement list of specific Bucket
        """

        query =f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bk_name}")'

        query_result = self.DBClient.query_api().query(query=query)
        ms_list = []
        for table in query_result:
            for record in table.records:
                ms_list.append(record.values["_value"])

        return ms_list


    def get_FieldList(self, bk_name, ms_name):
        """
        get all field list of specific measurements
        """

        query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}")'
        query_result = self.DBClient.query_api().query(query=query)
        results = []
        for table in query_result:
            for record in table.records:
                results.append(record.values["_value"])

        result_set = set(results)
        ms_list = list(result_set)

        return ms_list


    def get_Data(self, bk_name, ms_name):
        """
        Get :guilabel:`all data` of the specific mearuement
        """

        query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}")'
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query)

        return data_frame

    
    def measurement_list_only_start_end(self, bk_name):
        """
        Get the only start and end measurement name
        Use this function to reduce the DB load time.
        """

        # 1.8과 다르게 2.0부터는 get_list_measurements() 존재X
        # measurement 검색 쿼리 -> list[Fluxtable] -> measurement list -> 리스트 맨 앞,뒤 값 가져오기..?
        # list 길이가 1이면 하나만 저장, 2 이상이면 0, -1 위치 저장..?
        ms_list =[]


    def get_MeasurementDataSet(self, intDataInfo):
        """
        Get measurement Data Set according to the dbinfo
        Each function makes dataframe output with "timedate" index.
        """

        # intDataInfo가 Dict로 들어오는데 2.0에서 어떻게 처리해야할지 모르겠음


    def get_first_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`first data` of the specific mearuement
        """

        query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}") |> limit(n:1)'
        query_result = self.DBClient.query_api().query(query=query)
        results = []
        for table in query_result:
            for record in table.records:
                results.append(record.get_time())

        return results[0]




    def get_last_time(self, bk_name, ms_name):
        """
        Get the :guilabel:`last data` of the specific mearuement
        """


    def get_data_by_time(self, bind_params, bk_name, ms_name):
        """
        Get data of the specific measurement based on :guilabel:`start-end duration`
        *get_datafront_by_duration(self, start_time, end_time)*
        """


    def get_datafront_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`first N number` data from the specific measurement
        """


    def get_dataend_by_num(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`last N number` data from the specific measurement
        """


    def cleanup_df(self, df):
        """
        Clean data, remove duplication, Sort, Set index (datetime)
        """


    def get_tagList(self, bk_name, ms_name):
        """
        Get :guilabel:`all tag keys` list of the specific measurement. \n
        """


    def get_TagGroupData(self, bk_name, ms_name, tag_key, tag_value):
        """
        Get :guilabel:`tag value` set by tag key
        """


    def get_TagValue(self, bk_name, ms_name, tag_key):
        """
        Get :guilabel:`unique value` of selected tag key
        """


    def get_df_by_timestamp(self, ms_name, time_start, time_end):
        """
        It returns a table that has data on a measurement(table) in the database from time_start to time_end.
        """


    def write_db(self, df, table):
        """Write data to the influxdb
        """




