import sys
sys.path.append("../")
sys.path.append("../..")
import pandas as pd
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import ingestion_basic_dataset 

db_name = 'air_indoor_경로당'
mm_name='ICL1L2000236'
db = ingestion_basic_dataset.BasicDatasetRead(ins, db_name, mm_name)

query_string = "select * from "+ mm_name +" ORDER BY ASC LIMIT 1"
df = pd.DataFrame(db.influxdb.query(query_string).get_points())['time']
print(df)
"""
query_string = "select * from "+ mm_name +" ORDER BY DESC LIMIT 1"

df = db.influxdb.query(query_string)

print(df)
"""