from datetime import datetime
import re
import sys
import os
import pandas as pd 
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))



#if __name__ == "__main__":
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket, PostBucketRequest, PatchBucketRequest, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client.client.util.date_utils import get_date_helper
#client = InfluxDBClient.from_config_file("config.ini") #?????

bk_name = "writetest"
url = "http://localhost:8086"
token = "pM4K5ajbmvA4Tirzuzr3F1y0OF1KTNc0gsw5LGrZ-aT93y9KmoOgbqyX88aSZ73K27-nDVPwntfgd_W_cM5QJw=="
org = "testorg"
client = InfluxDBClient(url= url, token=token, org= org)
ms_name = "wt1"








### create bucket ------------------------------------
# buckets_api = client.buckets_api()
# new_bucket = buckets_api.create_bucket(bucket_name=bucket)







### Write ------------------------------------------------------------
# BASE_DIR = os.getcwd()
# df_file = "/home/leezy/CLUST_KETI/KETIPreDataIngestion/day_wise.csv"
# input_file = os.path.join(BASE_DIR, df_file)
# df = pd.read_csv(df_file, parse_dates=True, index_col ='Date')


# # https://docs.influxdata.com/influxdb/v2.1/query-data/get-started/query-influxdb/
# # https://influxdb-client.readthedocs.io/en/v1.2.0/usage.html#the-data-could-be-written-as
# # https://github.com/influxdata/influxdb-client-python
# ### Write
# write_client = client.write_api(write_options= ASYNCHRONOUS)
# write_client.write(bucket, record=df, data_frame_measurement_name=measurement_name)
# write_client.__del__()








### Read InfluxDB Data(Flux query) -----------------------------------------------
# 쿼리문 수정해야함
query_client = client.query_api()

# query =f'''
# from(bucket: "{bk_name}")
#   |> range(start: 2020-01-14T13:31:59Z, stop: 2022-01-14T14:31:59Z)
#   |> filter(fn: (r) => r._measurement == "{ms_name}")
#   |> drop(columns: ["_start", "_stop"])
# '''

# print(query)

# query = f'''
# from(bucket:"{bk_name}")
# |> range(start: 0, stop: now())
# |> filter(fn: (r) => r._measurement == "{ms_name}")
# |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
# |> drop(columns: ["_start", "_stop"])
# '''

# query = 'from(bucket:"'+bk_name+'")' \
#         '|> range(start: 0, stop: now()) ' \
#         '|> filter(fn: (r) => r._measurement == "'+ms_name+'")' \
#         '|> filter(fn: (r) => r._field == "pm10")' \
#         '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' \
#         '|> drop(columns: ["_start", "_stop"])'
#         '|> limit(n:10, offset: 0)' 


# print(type(query))
# data_frame = query_client.query_data_frame(query)
# print(type(data_frame))
# print(data_frame)









# ## Read Bucket list -------------------------------------------
# buckets_api = client.buckets_api()
# buckets = buckets_api.find_buckets().buckets
# # print("\n".join([f" ---------\n ID: {bucket.id}\n Name: {bucket.name}\n Retention: {bucket.retention_rules}"
# #                   for bucket in buckets]))

# bk_list = []

# for bucket in buckets:
#   bk_list.append(bucket.name)

# print(bk_list)







### Read meaasurements list --------------------------------------------
# query_result =f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bk_name}")'

# result = client.query_api().query(org=org_name,query=query_result)
# results = []
# for table in result:
#   print("table")
#   print(table)
#   for record in table.records:
#     print("record")
#     print(record)
#     results.append(record.values["_value"])


# print(results)






# ###Read field, time, start, stop of specific measurement ---------------------------------
# query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}")'
# query_result = client.query_api().query(query=query)
# results = []
# for table in query_result:
#   for record in table.records:
#     #results.append((record.get_measurement(), record.get_field(), record.get_time(), record.get_start(), record.get_stop()))
#     #results.append((record.get_field(), record.get_time()))
#     results.append(record.get_field())
#     #result_df = pd.DataFrame((record.get_field(),record.get_time()))

# result_set = set(results)
# field_list = list(result_set)
# print(type(field_list))
# print(field_list)







# ## Read field, time, start, stop of specific measurement ---------------------------------
# query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}")'
# query_result = client.query_api().query(query=query)
# results = []
# for table in query_result:
#   for record in table.records:
#     #results.append((record.get_measurement(), record.get_field(), record.get_time(), record.get_start(), record.get_stop()))
#     #results.append((record.get_field(), record.get_time()))
#     results.append((record.get_field(), record.get_time()))
#     #result_df = pd.DataFrame((record.get_field(),record.get_time()))

# print(results[0])


# result_set = set(results)
# field_list = list(result_set)
# print(type(field_list))
# print(field_list)





# ### Read first time of specific measurement ---------------------------------
# query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}") |> limit(n:1)'
# query_result = client.query_api().query(query=query)
# results = []
# for table in query_result:
#   for record in table.records:
#     results.append(record.get_time())

# first_time= results[0]

# print(first_time)
# print(type(first_time))




# ### first_time ver.2 -> 똑같은 값
# query = f'''
# from(bucket: "{bk_name}") 
# |> range(start: 0, stop: now()) 
# |> filter(fn: (r) => r._measurement == "{ms_name}")
# |> limit(n:1)
# '''
# query_result = client.query_api().query(query)
# # data_frame = query_client.query_data_frame(query=query,data_frame_index=["_time"])

# # print(data_frame)
# # print(type(data_frame))

# date_helper = get_date_helper()
# results = []
# for table in query_result:
#   for record in table.records:
#     print(record)
#     tt = record.get_time()
#     tata = date_helper.tt
#     # tz = datetime(tt, tzinfo=TZ()).isoformat()
#     print(tata)
#     results.append(tata)


# first_time= results[0]

# print(type(first_time))
# print(first_time)





# print(query_result)
# query_result = self.DBClient.query_api().query_data_frame(query=query,data_frame_index=["_time"])
        
# print("query_result.index[0].value")
# print(query_result.index[0].value)
# print(type(query_result.index[0].value))

# print("\nquery_result.index.values")
# print(query_result.index.values)
# print(type(query_result.index.values))





# ### Read first time of specific measurement ---------------------------------
# query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}") |> limit(n:1)'
# query_result = client.query_api().query(query=query)
# results = []
# for table in query_result:
#   for record in table.records:
#     results.append(record.get_time())

# first_time = results[0]

# print(first_time)





# ### Read last time of specific measurement ---------------------------------
# query = f'from(bucket: "{bk_name}") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "{ms_name}") |> sort(desc:true) |> limit(n:1)'
# query_result = client.query_api().query(query=query)
# results = []
# for table in query_result:
#   for record in table.records:
#     results.append(record.get_time())

# last_time = results[0]

# print(last_time)








### Read tagkeys ------------------------------------ 실행X

# query_result = f'import "influxdata/influxdb/schema" schema.measurementFieldKeys(bucket: "{bk_name}", measurement: "{ms_name}")'
# result = client.query_api().query(org=org_name,query=query_result)
# print(type(result))
# print(result)
# print("-----------")
# results = []
# for table in result:
#     print(table)
#     for record in table.columns:
#         results.append(record)

# print(results)




### Read fieldkeys ----------------------------------




query_client.__del__()

client.__del__()














### =============================== influx_Client_v2 수정 전 코드 ==============================




    # def measurement_list(self, bk_name):
    #     """
    #     get all measurement list of specific Bucket
    #     """

    #     query =f'import "influxdata/influxdb/schema" schema.measurements(bucket: "{bk_name}")'

    #     query_result = self.DBClient.query_api().query(query=query)
    #     ms_list = []
    #     for table in query_result:
    #         for record in table.records:
    #             ms_list.append(record.values["_value"])

    #     return ms_list





    # def get_fieldList(self, bk_name, ms_name):
    #     """
    #     get all field list of specific measurements
    #     """

    #     query = f'''
    #     from(bucket: "{bk_name}") 
    #     |> range(start: 0, stop: now()) 
    #     |> filter(fn: (r) => r._measurement == "{ms_name}")
    #     '''
    #     query_result = self.DBClient.query_api().query(query=query)
    #     results = []
    #     for table in query_result:
    #         for record in table.records:
    #             results.append(record.get_field())

    #     result_set = set(results)
    #     field_list = list(result_set)

    #     return field_list






        # def get_first_time(self, bk_name, ms_name):
    #     """
    #     Get the :guilabel:`first data` of the specific mearuement
    #     """

    #     query = f'''from(bucket: "{bk_name}") 
    #     |> range(start: 0, stop: now()) 
    #     |> filter(fn: (r) => r._measurement == "{ms_name}")
    #     |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
    #     |> limit(n:1)
    #     '''
    #     query_result = self.DBClient.query_api().query(query=query)

    #     results =[]
    #     for table in query_result:
    #         for record in table.records:
    #             results.append(record.get_time().strftime('%Y-%m-%dT%H:%M:%S'))

    #     first_time = results[0]

    #     return first_time






        # def get_last_time(self, bk_name, ms_name):
    #     """
    #     Get the :guilabel:`last data` of the specific mearuement
    #     """

    #     query = f'''
    #     from(bucket: "{bk_name}") 
    #     |> range(start: 0, stop: now()) 
    #     |> filter(fn: (r) => r._measurement == "{ms_name}")
    #     |> drop(columns: ["_start", "_stop", "_measurement", "result", "table"])
    #     |> sort(desc:true) 
    #     |> limit(n:1)
    #     '''

    #     query_result = self.DBClient.query_api().query(query=query)
    #     results = []
    #     for table in query_result:
    #         for record in table.records:
    #             results.append(record.get_time().strftime('%Y-%m-%dT%H:%M:%S'))

    #     last_time = results[1]

    #     return last_time


    


        # def get_data(self, bk_name, ms_name):
    #     """
    #     Get :guilabel:`all data` of the specific mearuement
    #     """

    #     query = f'''
    #     from(bucket: "{bk_name}") 
    #     |> range(start: 0, stop: now()) 
    #     |> filter(fn: (r) => r._measurement == "{ms_name}")
    #     '''

    #     query_client = self.DBClient.query_api()
    #     data_frame = query_client.query_data_frame(query=query,data_frame_index=["_time"])

    #     return data_frame





        # def cleanup_df(self, df):
    #     """
    #     Clean data, remove duplication, Sort, Set index (datetime)
    #     """
    #     import numpy as np
    #     df = df.drop(['result','table'], axis=1)
    #     print("===== drop result table =====")
    #     print(df)
    #     df = df.set_index('_time')
    #     print("===== set index time =====")
    #     print(df)
    #     df = df.groupby(df.index).first()
    #     print("===== groupby index first =====")
    #     print(df)
    #     df.index = pd.to_datetime(df.index).strftime('%Y-%m-%dT%H:%M:%S')#).astype('int64'))
    #     print("===== to datetime =====")
    #     print(df)
    #     # df = df.drop_duplicates(keep='first') # value값에 같은 값이 있는 행을 제거하는 현상 발생
    #     # print("===== duplicates =====")
    #     # print(df)
    #     df = df.sort_index(ascending=True)
    #     print("===== sort index =====")
    #     print(df)
    #     df.replace("", np.nan, inplace=True)
    #     print("===== nan inplace =====")
    #     print(df)

    #     return df



if __name__ == "__main__":
    import numpy as np
    from datetime import datetime
    # startdate = datetime.datetime(2001, 1, 1, 0, 0)
    # enddate = datetime.datetime(2001, 1, 1, 5, 0)
    # index = pd.DatetimeIndex(start=startdate, end=enddate, freq='H')
    # data1 = {'A' : range(6), 'B' : range(6)}
    # data2 = {'A' : [20, -30, 40], 'B' : [-50, 60, -70]}
    # df1 = pd.DataFrame(data=data1, index=index)
    # df2 = pd.DataFrame(data=data2, index=index[:3])
    # df3 = df2.append(df1)

    date_str = ["2018, 1, 1","2018, 1, 1","2018, 1, 2", "2018, 1, 4", "2018, 1, 5", "2018, 1, 6", "2018, 1, 6" ,"2018, 1, 7"]
    idx = pd.to_datetime(date_str)
    print(idx)
    np.random.seed(0)
    df = pd.DataFrame(np.random.randn(8), index=idx)
    print(df)
    print("==============================")
    print(df.index)
    print(type(df.index))
    print("==============================")
    df = df[~df.index.duplicated(keep='first')]
    print(df)


    def get_fieldList(self, bk_name, ms_name):
        """
        get all field list of specific measurements
        """
        # import time
        # start = time.time()
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        # query = f'import "influxdata/influxdb/schema" schema.measurementFieldKeys(bucket: "{bk_name}", measurement: "{ms_name}")'
        query_result = self.DBClient.query_api().query_data_frame(query=query)

        # results=[]
        # for table in query_result:
        #     print(table.columns)
        #     for record in table.records:
        #         print(record)
        #         results.append(record.field())
        # field_list = list(query_result["_value"])

        query_result = self.cleanup_df(query_result)
        field_list = list(query_result.columns)

        # end = time.time()
        # print("============time=================")
        # print(end - start)
        field_list = list(set(field_list))
        return field_list




    def get_fieldList2(self, bk_name, ms_name):
        """
        get all field list of specific measurements
        """
        import time
        start = time.time()

        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        query_result = self.DBClient.query_api().query_data_frame(query=query)
        query_result = self.cleanup_df(query_result)
        field_list = list(query_result.columns)

        end = time.time()
        print("=======first======")
        print(end - start)

        return field_list




    def get_datafront_by_num2(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`first N number` data from the specific measurement
        """
        import time
        start = time.time()


        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> limit(n:{number})
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)


        end = time.time()
        print("====== time =======")
        print(end - start)
        print("\n")

        return data_frame





    def get_dataend_by_num2(self, number, bk_name, ms_name):
        """
        Get the :guilabel:`last N number` data from the specific measurement
        """
        query = f'''
        from(bucket: "{bk_name}") 
        |> range(start: 0, stop: now()) 
        |> filter(fn: (r) => r._measurement == "{ms_name}")
        |> drop(columns: ["_start", "_stop", "_measurement"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc:true)
        |> limit(n:{number})
        '''
        query_client = self.DBClient.query_api()
        data_frame = query_client.query_data_frame(query=query)
        data_frame = self.cleanup_df(data_frame)

        return data_frame
