import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))



#if __name__ == "__main__":
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

#client = InfluxDBClient.from_config_file("config.ini") #?????

bucket = "example"
url = "http://localhost:8086"
token = "pM4K5ajbmvA4Tirzuzr3F1y0OF1KTNc0gsw5LGrZ-aT93y9KmoOgbqyX88aSZ73K27-nDVPwntfgd_W_cM5QJw=="
org_name = "testorg"
client = InfluxDBClient(url= url, token=token, org= org_name)
measurement_name = "test1"


import pandas as pd 
BASE_DIR = os.getcwd()
df_file = "/home/leezy/CLUST_KETI/KETIPreDataIngestion/data_miss_original.csv"
input_file = os.path.join(BASE_DIR, df_file)
df = pd.read_csv(df_file, parse_dates=True, index_col ='timedate')


# https://docs.influxdata.com/influxdb/v2.1/query-data/get-started/query-influxdb/
# https://influxdb-client.readthedocs.io/en/v1.2.0/usage.html#the-data-could-be-written-as
# https://github.com/influxdata/influxdb-client-python
### Write
write_client = client.write_api(write_options= ASYNCHRONOUS)
write_client.write(bucket, record=df, data_frame_measurement_name=measurement_name)
write_client.__del__()



### Read

query_client = client.query_api()

# query = 'from(bucket:"'+bucket +'") |> range(start: -1000d) |> filter(fn: (r) => r["_measurement"] == "'+measurement_name+'") '
# query = 'from(bucket:"'+bucket +'") '
# query = 'from(bucket: "'+bucket+'") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r["_measurement"] == "'+measurement_name+'" |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false) |> yield(name: "mean")'
# query = 'from(bucket: "'+bucket+'") ' \
# ' |> range(start: 2021-01-29T00:00:00Z, stop: 2021-06-01T00:00:00Z)' \
# ' |> filter(fn: (r) => r["_measurement"] == ""'+measurement_name+'")' \
# ' |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'



# 밑의 두 query 정상 작동
query = 'from(bucket: "'+bucket+'") |> range(start: 0, stop: now()) |> filter(fn: (r) => r["_measurement"] == "'+measurement_name+'") |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'

# query ='''
# from(bucket: "example")
#   |> range(start: 2021-01-29T00:00:00Z, stop: 2021-06-01T00:00:00Z)
#   |> filter(fn: (r) => r["_measurement"] == "test1")
#   |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
# '''


print(query)
data_frame = query_client.query_data_frame(query)

print(data_frame)
query_client.__del__()


client.__del__()
