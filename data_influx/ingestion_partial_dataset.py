import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from requests.api import head
from KETIPreDataIngestion.data_influx import ingestion_basic_dataset as ibd
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

def partial_dataSet_ingestion(intDataInfo, influx_parameter):
    
    result={}
    for i, dbinfo in enumerate(intDataInfo['db_info']):
        db_name = dbinfo['db_name']
        measurement = dbinfo['measurement']
        ing_start= dbinfo['start']
        ing_end = dbinfo['end']
        influx_c = ibd.BasicDatasetRead(influx_parameter, db_name, measurement)
        result[i] = influx_c.get_data_by_time(ing_start, ing_end)        

        result[i].index.name ='datetime'
        
    return result

intdatainfo = { "db_info":[ { "db_name":"INNER_AIR", "measurement":"HS1", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" }, 
{ "db_name":"OUTDOOR_AIR", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" }, 
{ "db_name":"OUTDOOR_WEATHER", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" } ] }

print(partial_dataSet_ingestion(intdatainfo, ins))
