import sys
import os
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
        write_api = self.DBClient.write_api()
        query_api = self.DBClient.query_api()

    def get_BucketList(self):
        '''
        get all bucket list
        '''
        
        bk_list =[]


        return bk_list    


    def measurement_List(self, bk_name):
        '''
        get all measurement list of specific Bucket
        '''

        ms_list =[]

        return ms_list


    def get_fieldList(self, ms_name):
        '''
        get all field list of specific measurements
        '''

        fieldList = []

        return fieldList




    




