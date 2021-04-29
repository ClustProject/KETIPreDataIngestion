import numpy as np
from influxdb import InfluxDBClient

if __name__ =='__main__':
    import KETI_influx_setting as ins
    client = InfluxDBClient(host = ins.host_, port= ins.port_, username = ins.user_,  verify_ssl=True)
    db_list = client.get_list_database()

    d2 = [list(item.values())[0] for item in db_list]
    print (d2)
