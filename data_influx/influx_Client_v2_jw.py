import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))


#if __name__ == "__main__":
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

#client = InfluxDBClient.from_config_file("config.ini") #?????

bucket = "clust"
url = "http://localhost:8086"
token = "ikypdB_8I2wtlzNHPPjoy87UUbHasvPkBKNzeKUFb4d8GMfVE4PV_IPZiH3haRLz1_RTHB4u7OGgEXQ3ipJ1yQ=="
org_name = "KETI"
client = InfluxDBClient(url= url, token=token, org= org_name)
measurement_name = "test"


import pandas as pd 
BASE_DIR = os.getcwd()
df_file = "data_miss_original.csv"
input_file = os.path.join(BASE_DIR, df_file)
df = pd.read_csv(df_file, parse_dates=True, index_col ='timedate')


# https://docs.influxdata.com/influxdb/v2.1/query-data/get-started/query-influxdb/
# https://influxdb-client.readthedocs.io/en/v1.2.0/usage.html#the-data-could-be-written-as
# https://github.com/influxdata/influxdb-client-python
### Write
write_client = client.write_api(write_options= ASYNCHRONOUS)
write_client.write(bucket, record=df, data_frame_measurement_name=measurement_name)
write_client.__del__()

"""

### Read

query_client = client.query_api()


#query = 'from(bucket:"'+bucket +'") |> range(start: -9000d) => r._measurement == "'+measurement_name+'") '
query = 'from(bucket:"'+bucket +'") '
print(query)
data_frame = query_client.query_data_frame(query)

print(data_frame)
query_client.__del__()


client.__del__()
"""