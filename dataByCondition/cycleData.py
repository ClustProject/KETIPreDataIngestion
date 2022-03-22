from sqlite3 import Timestamp, TimestampFromTicks
import sys
import os
from datetime import datetime, timedelta
from numpy import true_divide
from sklearn import datasets
from pandas import errors
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIPreDataIngestion.data_influx.influx_Client_v2 import influxClient


class CycleData():
    """
    Prepare Data based on cycle parameter
    """
    def __init__(self):
        # self.start, self.end = self.getTimePointByDayUnit(data)
        # self.data = data[self.start: self.end] #(??)
        pass

    # def getTimePointByDayUnit(self, data):
    #     # data 를  정렬시킨 후 명확히 일자가 시작하는 위치 00:00:00에서~ 23:59:59 (맞나요?) 까지 끊고 그것에 대한  start, end time을 구한다.
    #     start =
    #     end =
    #     return start, end

    def getHourCycleSet(self, data):
        """
        Get data of hour cycle
        :param data: dataframe
        :type data: dataframe
        :param num: number
        :type num: int
        :return: dataFrameCollectionResult
        :rtype: dataframe
        """
        # 1시간 단위
        # 시간 00:00:00 ~ 23:59:59 일 경우에만 저장
        # ex) 시작시간이 01:00:00 or 끝 시간이 22:59:59 라면 해당 날짜 저장X


        return dataFrameCollectionResult






    def getDayCycleSet(self, data, num):
        # day 단위의 데이터 셋 리턴
        """
        Get data of day cycle
        :param data: dataframe
        :type data: dataframe
        :param num: number
        :type num: int
        :return: dataFrameCollectionResult
        :rtype: dataframe
        """
        # 첫 시간과 마지막 시간 구하기
        data_first = data.index.min()
        data_last = data.index.max()

        data_start = data_first
        data_stop = data_start + timedelta(days=num) - timedelta(seconds=1)

        if data.index.max().hour != 23:
            data_end = data_last - timedelta(hours=data_last.hour, minutes=data_last.minute, seconds=data_last.second+1)

        if data.index.min().hour != 0:
            data_start = data_first + timedelta(days=1) - timedelta(hours=data_first.hour, minutes=data_first.minute, seconds=data_first.second)
            data_stop = data_first + timedelta(days=num) - timedelta(seconds=1)


        print("\nstart",data_start)
        print("stop",data_stop)
        print("end",data_end,"\n")


        dataFrameCollectionResult = []

        while True:
            dataframe_num_day = data[data_start:data_stop]
            dataFrameCollectionResult.append(dataframe_num_day)
            print("=======dataFrameCollectionResult=======")
            print(data_start)
            print(data_stop)
            # print(dataFrameCollectionResult)

            data_start = data_stop + timedelta(seconds=1)
            print(((data_stop.date() - data_start.date()).days) / num)

            if ((data_stop.date() - data_start.date()).days + 1) / num == 0:
                print(((data_stop.date() - data_start.date()).days + 1) / num)
                data_stop = data_first + timedelta(days=num) - timedelta(seconds=1)
            else:
                data_stop = data_end

            print("============if else============")
            print(data_start)
            print(data_stop)

            if data_stop.date() == data_end.date():
                break

        # while True:
        #     dataframe_num_day = data[data_start:data_stop]
        #     dataFrameCollectionResult.append(dataframe_num_day)

        #     if data_first + timedelta(days=num) - timedelta(seconds=1) >= data_end:
        #         data_stop = data_end
        #     else:
        #         data_stop = data_first + timedelta(days=num) - timedelta(seconds=1)

        #     data_first = data_stop + timedelta(seconds=1)

        #     if data_stop.date() == data_end.date():
        #         break

        return dataFrameCollectionResult






    def getWeekCycleSet(self, data, num=1):
        # Week 단위의 데이터 셋 리턴
        # 월~일
        
        dataFrameCollectionResult = []
        return dataFrameCollectionResult

    def getMonthCycleSet(self, data, num=1):
        #  Month 단위의 데이터셋 리턴

        dataFrameCollectionResult = []
        return dataFrameCollectionResult

    def getYearCycleSet(self, data, num=1):
        # Year 단위의 데이터셋 리턴

        dataFrameCollectionResult = []
        return dataFrameCollectionResult

    def getDataFrameCollectionToSeriesDataType(self, datasetCollection):

        seriesDataset = []
        return seriesDataset




if __name__ == '__main__':
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

    db_setting = influxClient(ins.CLUSTDataServer2)
    # db_name="energy_wind_power"
    # ms_name="jeju"

    # db_name="farm_strawberry_jinan"
    # ms_name="environment"

    db_name="farm_outdoor_air"
    ms_name="seoul"

    import pandas as pd
    bind_params = {'start_time': '2020-07-01T01:00:00Z', 'end_time': '2020-07-10T18:00:00Z'}
    # bind_params = {'start_time': '2021-03-31T15:00:00.000Z', 'end_time': '2021-04-29T15:00:00.000Z'}
    data_get = db_setting.get_data_by_time(bind_params, db_name, ms_name)
    # print(data_get, "\n\n\n")

    hourCycle = CycleData().getDayCycleSet(data_get, 3)
    # 장기간 데이터 불옴
    # 위 클래스에 대한 각각의 펑션에 대한 결과가 제대로 나올 수 있도록