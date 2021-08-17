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
        for num in range(1,len(self.influxdb.get_list_database())):
            db_list.append(self.influxdb.get_list_database()[num]['name'])
        return db_list
    
    def measurement_list(self, db_name):
        measurement_list = []
        self.influxdb.switch_database(db_name)
        for num in range(len(self.influxdb.get_list_measurements())):
            measurement_list.append(self.influxdb.get_list_measurements()[num]['name'])
        return measurement_list 

if __name__ == "__main__":
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    test = DatabaseMeasurementList(ins)
    print(test.database_list())
    print(test.measurement_list('INNER_AIR'))