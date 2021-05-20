# set modules path
#import sys
#import os
#sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
import influxdb
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

#from KETI_pre_dataIngestion.data_influx import ingestion_measurement as ing
#from KETI_pre_dataIngestion.KETI_setting import influx_setting_KETI as ins

exploration_df = pd.DataFrame()

def exploration_data_list(influx_parameter):
    global exploration_df
    data_list = InfluxDBClient(host=influx_parameter.host_, port=influx_parameter.port_, username=influx_parameter.user_, password=influx_parameter.pass_)
    for num in range(1,len(data_list.get_list_database())):
        db_name = data_list.get_list_database()[num]['name']
        data_list.switch_database(db_name)
        if len(data_list.get_list_measurements()) == 0:
            meauserment_name = "-"
            query_string = "SELECT * FROM "+ db_name +""
            df = pd.DataFrame(data_list.query(query_string).get_points())
            data_length = len(df)
            start_time = "-"
            end_time = "-"
            exploration_df = exploration_df.append([[db_name, meauserment_name, data_length, start_time, end_time]])
        else:
            for num2 in range(len(data_list.get_list_measurements())):
                meauserment_name = data_list.get_list_measurements()[num2]['name']
                query_string = "SELECT * FROM "+ meauserment_name +""
                df = pd.DataFrame(data_list.query(query_string).get_points())
                data_length = len(df)
                start_time = df.sort_values(by=["time"]).iloc[0][0][:10]
                end_time = df.sort_values(by=["time"],ascending=False).iloc[0][0][:10]
                exploration_df = exploration_df.append([[db_name, meauserment_name, data_length, start_time, end_time]])
    exploration_df.columns=["DataBase_Name", "Measurement_Name", "Data_Length", "Start_time", "End_time"]
    exploration_df.index = [list(range(len(exploration_df)))]
    exploration_js = exploration_df.to_json(orient = 'records')

    return exploration_js