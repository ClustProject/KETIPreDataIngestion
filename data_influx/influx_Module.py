import pandas as pd
def getAllMSDataSetFromInfluxDB(db_client, db_name, bind_params):
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

        :returns: dataSet
        :rtype: list of dataframe (ms datasets)
    """
    ms_list = db_client.measurement_list(db_name)
    dataSet ={}
    for ms_name in ms_list:
        data = db_client.get_data_by_time(bind_params, db_name, ms_name)
        dataSet[ms_name] = data
    return dataSet
