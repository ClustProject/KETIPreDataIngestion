# set modules path
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from requests.api import head
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from datetime import datetime

exploration_df = pd.DataFrame()
count=0
def exploration_data_list(influx_parameter):
    global exploration_df
    global count
    data = InfluxDBClient(host=influx_parameter.host_, port=influx_parameter.port_, username=influx_parameter.user_, password=influx_parameter.pass_)
    for num in range(1,len(data.get_list_database())):
        db_name = data.get_list_database()[num]['name']
        data.switch_database(db_name)
        if len(data.get_list_measurements()) == 0:
            measurement_name = "-"
            start_time = "-"
            end_time = "-"
            frequency = '-'
            number_of_columns = '-'
            exploration_df = exploration_df.append([[db_name, measurement_name, start_time, end_time, frequency, number_of_columns]])
        else:
            for num2 in range(len(data.get_list_measurements())):
                measurement_name = data.get_list_measurements()[num2]['name']
                
                query_string = "SHOW FIELD KEYS"
                fieldkeys = list(data.query(query_string).get_points(measurement=measurement_name))
                number_of_columns = len(fieldkeys)
                fieldkey = fieldkeys[0]['fieldKey']
                
                query_string = 'SELECT FIRST("'+fieldkey+'") FROM "'+measurement_name+'"'
                start_time = list(data.query(query_string).get_points())[0]['time']

                query_string = 'SELECT LAST("'+fieldkey+'") FROM "'+measurement_name+'"'
                end_time = list(data.query(query_string).get_points())[0]['time']

                query_string = 'SELECT * from "'+measurement_name+'" LIMIT 2' #frequency -> visitor test용으로 influxdb에 넣어두고 월별, 일별(covid) 비교하며 코드 구성 => 시간 빼기로 해보기
                df = pd.DataFrame(data.query(query_string).get_points())
                
                df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ")
                freq = df.time[1] - df.time[0]
                
                if freq.days == 0:
                    if freq.seconds == 60:
                        frequency = 'Minute'
                    elif freq.seconds == 3600:
                        frequency = 'Hour'
                    elif freq.seconds < 60:
                        frequency = 'Second'
                    else:
                        frequency = '{} Second'.format(str(freq.seconds))
                else:
                    if freq.days == 1:
                        frequency = '{} Day'.format(str(freq.days))
                    elif freq.days == 7:
                        frequency = 'Weekend'
                    elif freq.days == 31:
                        frequency = 'Month'
                    elif freq.days == 365:
                        frequency = 'Year'
                    else:
                        frequency = '{} Day'.format(str(freq.days))
                exploration_df = exploration_df.append([[db_name, measurement_name, start_time, end_time, frequency, number_of_columns]])
    
    exploration_df.columns = ['db_name', 'measurement_name', 'start_time', 'end_time', 'frequency', 'number_of_columns']
    exploration_df.reset_index(drop=True, inplace = True)
    return exploration_df


if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = exploration_data_list(ins)
    print(test)
    test.to_csv('test.csv')
