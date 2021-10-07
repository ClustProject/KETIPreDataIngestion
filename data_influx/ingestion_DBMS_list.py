# set modules path
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
import influxdb
from influxdb import InfluxDBClient, DataFrameClient

## InfluxDB Read - DataBase & Measurement List
class DatabaseMeasurementList():
    def __init__(self, influx_setting):
        self.influx_setting = influx_setting
        self.influxdb = InfluxDBClient(host=self.influx_setting.host_, port=self.influx_setting.port_, username=self.influx_setting.user_, password = self.influx_setting.pass_)

    def database_list(self):
        db_list = []
        ori_db_list = self.influxdb.get_list_database()
        for num in range(1,len(ori_db_list)):
            db_list.append(ori_db_list[num]['name'])
        return db_list
    
    def measurement_list(self, db_name):
        measurement_list = []
        self.influxdb.switch_database(db_name)
        ori_ms_list=self.influxdb.get_list_measurements()
        for num in range(len(ori_ms_list)):
            measurement_list.append(ori_ms_list[num]['name'])
        return measurement_list

    def measurement_list_only_start_end(self, db_name):
        measurement_list = []
        self.influxdb.switch_database(db_name)
        ori_ms_list=self.influxdb.get_list_measurements()
        if(len(ori_ms_list)==1):
            measurement_list.append(ori_ms_list[0]['name'])
        elif(len(ori_ms_list)>1):
            measurement_list.append(ori_ms_list[0]['name'])
            measurement_list.append(ori_ms_list[len(ori_ms_list)-1]['name'])
        return measurement_list

    def feature_list(self, db_name, ms_name):
        self.influxdb.switch_database(db_name)
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(self.influxdb.query(query_string).get_points(measurement=ms_name))
        fieldkey = list(x['fieldKey'] for x in fieldkeys)
        #print(fieldkey)
        return fieldkey


if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = DatabaseMeasurementList(ins)
    #print(test.database_list())
    #print(test.measurement_list('INNER_AIR'))
    print(test.feature_list('OUTDOOR_AIR', 'seoul'))