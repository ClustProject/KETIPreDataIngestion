from KETIPreDataIngestion.data_influx import ingestion_measurement as ing

def partial_dataSet_ingestion(intDataInfo, influx_parameter):
    
    
    result={}
    for i, dbinfo in enumerate(intDataInfo['db_info']):
        db_name = dbinfo['db_name']
        measurement = dbinfo['measurement']
        ing_start= dbinfo['start']
        ing_end = dbinfo['end']
        influx_c = ing.Influx_management(influx_parameter.host_, influx_parameter.port_, influx_parameter.user_, influx_parameter.pass_, db_name, influx_parameter.protocol)
        result[i] = influx_c.get_df_by_time(ing_start,ing_end,measurement)
        
    return result


