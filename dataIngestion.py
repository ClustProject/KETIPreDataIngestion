import pandas as pd 
import sys
sys.path.append("../")
sys.path.append("../..")
import os
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client

class DataIngestionByInputType():
    def __init__(self):
        pass
    
    def getFileInput(self, filePath, time_index="timedate"):
        dataset = pd.read_csv(filePath, parse_dates=True, index_col=[time_index])
        return dataset

    def getInfluxInput(self, db_name, ms_name, num="2000"):
        
       
        DBClient = influx_Client.influxClient(ins.CLUSTDataServer)
        #db_list = DBClient.get_DBList() # 나중에 DB 이름 바꾸고 싶을때 여기 체크 가능
        #ms_list = DBClient.measurement_list(db_name) # MS 이름 바꾸고 싶을때 여기 체크 가능
        #feature_list = DBClient.get_fieldList(db_name, ms_name)

        #full_data = DBClient.get_data(db_name, ms_name)
        # 우선 full data 가져오도록 해놨음 db 이름과 ms 이름은 list에서 적당한것 바꿔볼 수 있음
       
        full_data = DBClient.get_datafront_by_num(num, db_name, ms_name)
        print("Data Length:" ,len(full_data))

        return full_data


def inputControl(inputType, db_name=None, ms_name=None, num="2000"):
    from KETIPreDataIngestion.dataIngestion import DataIngestionByInputType
    dataC = DataIngestionByInputType()
    if inputType=="file":
        BASE_DIR = os.getcwd()
        input_file = os.path.join(BASE_DIR, 'sampleData', 'data_miss_original.csv')
        input_data = dataC.getFileInput(input_file, 'timedate')
    elif inputType =="influx":
        input_data = dataC.getInfluxInput(db_name, ms_name, num)
    return input_data