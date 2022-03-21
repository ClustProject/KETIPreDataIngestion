import sys
import os

def getAllMSDataOfDB(db_client, db_name, query_start_time, query_end_time):
    """
        It returns dataSet from all MS of a speicific DB.

        :param db_client: instance to get data from influx DB
        :type db_client: instance of influxClient class
        :param db_name: db_name
        :type db_name: str
        :param query_start_time: query_start_time
        :type query_start_time: pd.datatime
        :param query_end_time: query_end_time
        :type query_end_time: pd.datatime

        :returns: ms_list, dataSet
        
        :rtype: array of string (measurement list), list of dataframe (ms data)
    """
    ms_list = db_client.measurement_list(db_name)
    dataSet =[]
    for ms_name in ms_list:
        bind_params = {'end_time':query_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'), 'start_time': query_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        data = db_client.get_data_by_time(bind_params, db_name, ms_name)
        dataSet.append(data)
    return ms_list, dataSet