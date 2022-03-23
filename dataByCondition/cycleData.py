from sqlite3 import Timestamp
import sys
import os
from datetime import datetime, timedelta
from time import time
from numpy import true_divide
from sklearn import datasets
from sympy import timed
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
        """
        # 1시간 단위
        # 시간 00:00:00 ~ 23:59:59 일 경우에만 저장
        # ex) 시작시간이 01:00:00 or 끝 시간이 22:59:59 라면 해당 날짜 저장X

        dataFrameCollectionResult = []
        return dataFrameCollectionResult






    def getDayCycleSet(self, data, num):
        # day 단위의 데이터 셋 리턴
        """
        """
        # 첫 시간과 마지막 시간 구하기
        day_first = data.index.min()
        day_last = data.index.max()

        # 들어온 dataframe의 첫 시간이 00:00:00이 아니면 다음날 00:00:00으로 변경
        if day_first.time() != '00:00:00':
            day_start = day_first + timedelta(days=1) - timedelta(hours=day_first.hour, minutes=day_first.minute, seconds=day_first.second)
        else:
            day_start = day_first
        
        # start 시간에서 num만큼의 지난 날(범위를 지정할 때)
        day_stop = day_start + timedelta(days=num) - timedelta(seconds=1)

        # 들어온 dataframe의 마지막 시간이 23이 아닐경우 그 전날의 23:59:59로 변경
        if day_last.time() != '23:59:59':
            day_end = day_last - timedelta(hours=day_last.hour, minutes=day_last.minute, seconds=day_last.second+1)
        else:
            day_end = day_last

        dataFrameCollectionResult = []
        while True:
            # 지정한 범위의 데이터 저장
            dataframe_num_day = data[day_start:day_stop]
            dataFrameCollectionResult.append(dataframe_num_day)

            # dataframe의 마지막 데이터와 현재 저장중인 마지막 데이터가 같을 때, 무한루트 탈출
            if day_stop.date() == day_end.date():
                break
            
            # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
            day_start = day_stop + timedelta(seconds=1)

            # dataframe의 마지막 데이터와 새롭게 지정할 마지막 데이터 비교
            # dataframe의 마지막 시간을 넘으면, 지정할 끝 데이터 = dataframe 마지막 데이터
            if day_start + timedelta(days=num) <= day_end:
                day_stop = day_start + timedelta(days=num) - timedelta(seconds=1)
            else:
                day_stop = day_end

        return dataFrameCollectionResult






    def getWeekCycleSet(self, data, num):
        # Week 단위의 데이터 셋 리턴
        # 월~일
        week_first = data.index.min()
        week_last = data.index.max()
        
        # dataframe의 첫번째 데이터 처리
        if week_first.day_name() != 'Monday' and week_first.time() != '00:00:00':
            week_start =  week_first + timedelta(weeks=1) - timedelta(days=week_first.dayofweek, hours=week_first.hour, minutes=week_first.minute, seconds=week_first.second)
        elif week_first.day_name() != 'Monday' and week_first.time() == '00:00:00':
            week_start = week_first + timedelta(weeks=1) - timedelta(days=week_first.dayofweek)
        elif week_first.day_name() == 'Monday' and week_first.time() != '00:00:00':
            week_start = week_first + timedelta(weeks=1) - timedelta(hours=week_first.hour, minutes=week_first.minute, seconds=week_first.second)
        else:
            week_start = week_first

        week_stop = week_start + timedelta(weeks=num) - timedelta(seconds=1)


        # dataframe의 마지막 데이터 처리
        if week_last.day_name() != 'Sunday' and week_last.time() != '23:59:59':
            week_end = week_last - timedelta(days=week_last.dayofweek, hours=week_last.hour, minutes=week_last.minute, seconds=week_last.second+1)
        elif week_last.day_name() != 'Sunday' and week_last.time() == '23:59:59':
            week_end = week_last - timedelta(days=week_last.dayofweek, hours=week_last.hour, minutes=week_last.minute, seconds=week_last.second+1)
        elif week_last.day_name() == 'Sunday' and week_last.time() != '23:59:59':
            week_end = week_last - timedelta(days=6, hours=week_last.hour, minutes=week_last.minute, seconds=week_last.second+1)
        else:
            week_end = week_last
        

        dataFrameCollectionResult = []

        while True:
            dataframe_num_week = data[week_start:week_stop]
            dataFrameCollectionResult.append(dataframe_num_week)

            if week_stop.date() == week_end.date():
                break
            
            week_start = week_stop + timedelta(seconds=1)

            if week_start + timedelta(weeks=num) <= week_end:
                week_stop = week_start + timedelta(weeks=num) - timedelta(seconds=1)
            else:
                week_stop = week_end

        
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

    # db_name="farm_outdoor_air"
    # ms_name="seoul"

    db_name="farm_swine_vibes1"
    ms_name="CO"

    import pandas as pd
    # bind_params = {'start_time': '2021-10-22T00:00:22Z', 'end_time': '2022-03-13T23:59:22Z'}
    bind_params = {'start_time': '2021-10-25T00:00:00Z', 'end_time': '2021-11-01T23:10:22Z'}
    # bind_params = {'start_time': '2020-07-05T10:20:30Z', 'end_time': '2020-07-17T00:00:00Z'}
    # bind_params = {'start_time': '2021-03-31T15:00:00.000Z', 'end_time': '2021-04-29T15:00:00.000Z'}
    data_get = db_setting.get_data_by_time(bind_params, db_name, ms_name)
    # print(data_get, "\n\n\n")

    hourCycle = CycleData().getDayCycleSet(data_get,3)
    print(hourCycle)
    print(type(hourCycle))

    # weekCycle = CycleData().getWeekCycleSet(data_get, 7)
    # print(weekCycle)


    # 장기간 데이터 불옴
    # 위 클래스에 대한 각각의 펑션에 대한 결과가 제대로 나올 수 있도록