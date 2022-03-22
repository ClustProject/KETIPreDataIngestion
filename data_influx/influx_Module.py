import pandas as pd
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

def cleanup_df(df):
    """
    Clean dataFrame, remove duplication, Set and sort by timeseries index (datetime type)

    - Set index to datetime
    - Remove duplication
    - Sort ascending
    - Replace blank to Nan

    :param df: dataFrame
    :type df: dataFrame

    :return: df, data setting
    :rtype: DataFrame
    """
    import numpy as np
    if 'time' in df.columns:
        df = df.set_index('time')
    elif 'datetime' in df.columns:
        df = df.set_index('datetime')
    """
    df = df.groupby(df.index).first()
    df.index = pd.to_datetime(df.index)#).astype('int64'))
    df = df.drop_duplicates(keep='first')
    """
    df.index = pd.to_datetime(df.index)#).astype('int64'))
    df = df[~df.index.duplicated(keep='first')]
    df = df.sort_index(ascending=True)
    df.replace("", np.nan, inplace=True)
    return df