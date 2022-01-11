import re
import sys
import os

from rx.core.observable.observable import B
from urllib3 import request
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))



#if __name__ == "__main__":
from influxdb_client import InfluxDBClient, Point, BucketsService, Bucket, PostBucketRequest, PatchBucketRequest
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

#client = InfluxDBClient.from_config_file("config.ini") #?????

bucket = "example"
url = "http://localhost:8086"
token = "pM4K5ajbmvA4Tirzuzr3F1y0OF1KTNc0gsw5LGrZ-aT93y9KmoOgbqyX88aSZ73K27-nDVPwntfgd_W_cM5QJw=="
org_name = "testorg"
client = InfluxDBClient(url= url, token=token, org= org_name)
measurement_name = "test1"


### ------------------------------------------------------------------------------
### Write

# import pandas as pd 
# BASE_DIR = os.getcwd()
# df_file = "/home/leezy/CLUST_KETI/KETIPreDataIngestion/11월_환경데이터.csv"
# input_file = os.path.join(BASE_DIR, df_file)
# df = pd.read_csv(df_file, parse_dates=True, index_col ='time')


# # https://docs.influxdata.com/influxdb/v2.1/query-data/get-started/query-influxdb/
# # https://influxdb-client.readthedocs.io/en/v1.2.0/usage.html#the-data-could-be-written-as
# # https://github.com/influxdata/influxdb-client-python
# ### Write
# write_client = client.write_api(write_options= ASYNCHRONOUS)
# write_client.write(bucket, record=df, data_frame_measurement_name=measurement_name)
# write_client.__del__()
### ------------------------------------------------------------------------------





### ------------------------------------------------------------------------------------
### Read
query_client = client.query_api()

"""
from(bucket: "example")
  |> range(start: 2021-01-29T00:00:00Z, stop: 2021-06-01T00:00:00Z)
  ---> _start, _stop 사이의 _time 값을 가져올 방법은?

  |> filter(fn: (r) => r["_measurement"] == "test1")
  |> filter(fn: (r) => r["_field"] == "co2")
  ---> RDB의 where 조건문 같은 역할
  
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
  ---> every:시간 주기 / fn: value를 나타낼 값 계산

  |> yield(name: "mean")
"""

# query = 'from(bucket: "'+bucket+'") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "'+measurement_name+'") |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'

# query ='''
# from(bucket: "example")
#   |> range(start: 2021-01-29T00:00:00Z, stop: 2021-06-01T00:00:00Z)
#   |> filter(fn: (r) => r._measurement == "test1")
#   |> filter(fn: (r) => r._field == "co2")
#   |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
#   |> yield(name: "mean")
# '''

# print(query)
# data_frame = query_client.query_data_frame(query)
# print(data_frame)



### ---------------- Read meaasurements, field, time, start, stop ----------------

# result = client.query_api().query(org=org_name,query=query)
# print(type(result))
# results = []
# for table in result:
#   for record in table.records:
#     results.append((record.get_measurement(), record.get_field(), record.get_time(), record.get_start(), record.get_stop()))

# for i in results:
#   print(i)
### ------------------------------------------------------------------------------




### ---------------- Read Bucket list ----------------
buckets_api = client.buckets_api()
print("------------list test-------")
buckets = buckets_api.find_buckets().buckets
print("\n".join([f" ---------\n ID: {bucket.id}\n Name: {bucket.name}\n Retention: {bucket.retention_rules}"
                  for bucket in buckets]))
print("------------------")

### ------------------------------------------------------------------------------










query_client.__del__()


client.__del__()
