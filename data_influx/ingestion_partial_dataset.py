from KETIPreDataIngestion.data_influx import ingestion_basic_dataset as ibd
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

def partial_dataSet_ingestion(intDataInfo, influx_parameter):
    
    
    result={}
    for i, dbinfo in enumerate(intDataInfo['db_info']):
        db_name = dbinfo['db_name']
        measurement = dbinfo['measurement']
        ing_start= dbinfo['start']
        ing_end = dbinfo['end']
        influx_c = ibd.BasicDatasetRead(ins, db_name, measurement)
        result[i] = influx_c.get_data_by_time(ing_start, ing_end)        

        result[i].index.name ='datetime'
        
    return result


