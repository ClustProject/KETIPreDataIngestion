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

    
    def switch_BK(self, bk_name):

        self.bk_name = bk_name
        self.DBClient




    




