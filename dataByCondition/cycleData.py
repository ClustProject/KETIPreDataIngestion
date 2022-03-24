from queue import Empty
from sqlite3 import Timestamp
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import time
from sklearn import datasets
import math
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIPrePartialDataPreprocessing.data_refine.frequency import RefineFrequency
from KETIPreDataIngestion.data_influx.influx_Client_v2 import influxClient


class CycleData():
    """
    Prepare Data based on cycle parameter
    """
    def __init__(self):
        # self.start, self.end = self.getTimePointByDayUnit(data)
        # self.data = data[self.start: self.end] #(??)
        self.time_00 = datetime.strptime("00:00:00","%H:%M:%S").time()


    # def getTimePointByDayUnit(self, data):
    #     # data 를  정렬시킨 후 명확히 일자가 시작하는 위치 00:00:00에서~ 23:59:59 (맞나요?) 까지 끊고 그것에 대한  start, end time을 구한다.-
    #     start =
    #     end =
    #     return start, end

    def getHourCycleSet(self, data, num):
        """
        """
        hour_first = data.index[0]
        hour_last = data.index[-1]

        if hour_first.minute != 0 or hour_first.second != 0:
            hour_start = hour_first + timedelta(hours=1) - timedelta(minutes=hour_first.minute, seconds=hour_first.second)
        else:
            hour_start = hour_first

        hour_stop = hour_start + timedelta(hours=num) - timedelta(seconds=1)

        if hour_last.minute != 59 or hour_last.second != 59:
            hour_end = hour_last - timedelta(minutes=hour_last.minute, seconds=hour_last.second+1)
        elif hour_last.minute == 59 or hour_last.second != 59:
            hour_end = hour_last + timedelta(hours=1) - timedelta(minutes=hour_last.minute, seconds=hour_last.second+1)
        else:
            hour_end = hour_last

        hour_calcul = int((hour_end - hour_start).days*24 + ((hour_end - hour_start).seconds/3600) +1)
        hour_count = math.ceil(hour_calcul / num)

        dataFrameCollectionResult = []
        for i in range(hour_count):
            dataframe_num_hour = data[hour_start:hour_stop]
            if len(dataframe_num_hour) == 0:
                pass
            else:
                dataFrameCollectionResult.append(dataframe_num_hour)
                       
            hour_start = hour_stop + timedelta(seconds=1)
            hour_stop = hour_start + timedelta(hours=num) - timedelta(seconds=1)

            if hour_start + timedelta(hours=num) > hour_end:
                hour_stop = hour_end


        # dataFrameCollectionResult = []
        # while True:
        #     dataframe_num_hour = data[hour_start:hour_stop]
        #     dataFrameCollectionResult.append(dataframe_num_hour)

        #     if hour_stop == hour_end:
        #         break
            
        #     hour_start = hour_stop + timedelta(seconds=1)

        #     if hour_start + timedelta(hours=num) <= hour_end:
        #         hour_stop = hour_start + timedelta(hours=num) - timedelta(seconds=1)
        #     else:
        #         hour_stop = hour_end

        return dataFrameCollectionResult


    def getDayCycleSet(self, data, num):
        # day 단위의 데이터 셋 리턴
        """
        """
        # 첫 시간과 마지막 시간 구하기
        day_first = data.index[0]
        day_last = data.index[-1]

        # 들어온 dataframe의 첫 시간이 00:00:00이 아니면 다음날 00:00:00으로 변경
        if day_first.time() != self.time_00:
            day_start = day_first + timedelta(days=1) - timedelta(hours=day_first.hour, minutes=day_first.minute, seconds=day_first.second)
        else:
            day_start = day_first
        
        # start 시간에서 num만큼의 지난 날(범위를 지정할 때)
        day_stop = day_start + timedelta(days=num) - timedelta(seconds=1)

        # 들어온 dataframe의 마지막 시간이 23이 아닐경우 그 전날의 23:59:59로 변경
        if day_last.time() != self.time_23:
            day_end = day_last - timedelta(hours=day_last.hour, minutes=day_last.minute, seconds=day_last.second+1)
        else:
            day_end = day_last

        day_count = math.ceil(((day_end - day_start).days + 1) / num)

        dataFrameCollectionResult = []
        for i in range(day_count):
            dataframe_num_day = data[day_start:day_stop]
            if len(dataframe_num_day) == 0:
                pass
            else:
                dataFrameCollectionResult.append(dataframe_num_day)
                       
            # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
            day_start = day_stop + timedelta(seconds=1)
            day_stop = day_start + timedelta(days=num) - timedelta(seconds=1)

            if day_start + timedelta(days=num) > day_end:
                day_stop = day_end


        # dataFrameCollectionResult = []
        # while True:
        #     # 지정한 범위의 데이터 저장
        #     dataframe_num_day = data[day_start:day_stop]
        #     dataFrameCollectionResult.append(dataframe_num_day)

        #     # dataframe의 마지막 데이터와 현재 저장중인 마지막 데이터가 같을 때, 무한루트 탈출
        #     if day_stop.date() == day_end.date():
        #         break
            
        #     # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
        #     day_start = day_stop + timedelta(seconds=1)

        #     # dataframe의 마지막 데이터와 새롭게 지정할 마지막 데이터 비교
        #     # dataframe의 마지막 시간을 넘으면, 지정할 끝 데이터 = dataframe 마지막 데이터
        #     if day_start + timedelta(days=num) <= day_end:
        #         day_stop = day_start + timedelta(days=num) - timedelta(seconds=1)
        #     else:
        #         day_stop = day_end

        return dataFrameCollectionResult


    def getWeekCycleSet(self, data, num):
        # Week 단위의 데이터 셋 리턴
        # 월~일
        week_first = data.index[0]
        week_last = data.index[-1]
        
        # dataframe의 첫번째 데이터 처리
        if week_first.day_name() != 'Monday' and week_first.time() != self.time_00:
            week_start =  week_first + timedelta(weeks=1) - timedelta(days=week_first.dayofweek, hours=week_first.hour, minutes=week_first.minute, seconds=week_first.second)
        elif week_first.day_name() != 'Monday' and week_first.time() == self.time_00:
            week_start = week_first + timedelta(weeks=1) - timedelta(days=week_first.dayofweek)
        elif week_first.day_name() == 'Monday' and week_first.time() != self.time_00:
            week_start = week_first + timedelta(weeks=1) - timedelta(hours=week_first.hour, minutes=week_first.minute, seconds=week_first.second)
        else:
            week_start = week_first

        week_stop = week_start + timedelta(weeks=num) - timedelta(seconds=1)

        # dataframe의 마지막 데이터 처리
        if week_last.day_name() != 'Sunday' or week_last.time() != self.time_23:
            week_end = week_last - timedelta(days=week_last.dayofweek, hours=week_last.hour, minutes=week_last.minute, seconds=week_last.second+1)
        else:
            week_end = week_last

        week_count = math.ceil( ((week_end - week_start).days/7) / num)

        dataFrameCollectionResult = []
        for i in range(week_count):
            dataframe_num_week = data[week_start:week_stop]
            if len(dataframe_num_week) == 0:
                pass
            else:
                dataFrameCollectionResult.append(dataframe_num_week)
                       
            # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
            week_start = week_stop + timedelta(seconds=1)
            week_stop = week_start + timedelta(weeks=num) - timedelta(seconds=1)

            if week_start + timedelta(weeks=num) > week_end:
                week_stop = week_end


        # dataFrameCollectionResult = []
        # while True:
        #     dataframe_num_week = data[week_start:week_stop]
        #     dataFrameCollectionResult.append(dataframe_num_week)

        #     if week_stop.date() == week_end.date():
        #         break
            
        #     week_start = week_stop + timedelta(seconds=1)

        #     if week_start + timedelta(weeks=num) <= week_end:
        #         week_stop = week_start + timedelta(weeks=num) - timedelta(seconds=1)
        #     else:
        #         week_stop = week_end

        return dataFrameCollectionResult


    def getMonthCycleSet(self, data, num):
        #  Month 단위의 데이터셋 리턴
        month_first = data.index[0]
        month_last = data.index[-1]

        # 시작 월 설정
        if month_first.day != 1 and month_first.time() != self.time_00:
            month_start = month_first + relativedelta(months=1) - timedelta(days=month_first.day-1, hours=month_first.hour, minutes=month_first.minute, seconds=month_first.second)
        elif month_first.day != 1 and month_first.time() == self.time_00:
            month_start = month_first + relativedelta(months=1) - timedelta(days=month_first.day-1)
        elif month_first.day == 1 and month_first.time() != self.time_00:
            month_start = month_first + relativedelta(months=1) - timedelta(hours=month_first.hour, minutes=month_first.minute, seconds=month_first.second)
        else:
            month_start = month_first


        month_stop = month_start + relativedelta(months=num) - timedelta(seconds=1)

        # dataframe 마지막 데이터 설정
        if month_last.day != month_last.days_in_month or month_last.time() != self.time_23:
            month_end = month_last - timedelta(days=month_last.day-1, hours=month_last.hour, minutes=month_last.minute, seconds=month_last.second+1)
        else:
            month_end = month_last

        month_calcul = (month_end.year - month_start.year)*12 + month_end.month - month_start.month + 1
        month_count = math.ceil(month_calcul / num)

        dataFrameCollectionResult = []
        for i in range(month_count):
            dataframe_num_month = data[month_start:month_stop]
            if len(dataframe_num_month) == 0:
                pass
            else:
                dataFrameCollectionResult.append(dataframe_num_month)
                       
            # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
            month_start = month_stop + timedelta(seconds=1)
            month_stop = month_start + relativedelta(months=num) - timedelta(seconds=1)

            if month_start + relativedelta(months=num) > month_end:
                month_stop = month_end      

        return dataFrameCollectionResult




    def getYearCycleSet(self, data, num):
        # Year 단위의 데이터셋 리턴
        year_first = data.index[0]
        year_last = data.index[-1]

        if year_first.strftime("%m-%d") != '01-01' and year_first.time()  != self.time_00:
            year_start = year_first + relativedelta(years=1) - relativedelta(months=year_first.month-1, days=year_first.day-1, hours=year_first.hour, minutes=year_first.minute, seconds=year_first.second)
        elif year_first.strftime("%m-%d") != '01-01' and year_first.time()  == self.time_00:
            year_start = year_first + relativedelta(years=1) - relativedelta(months=year_first.month-1, days=year_first.day-1)
        elif year_first.strftime("%m-%d") == '01-01' and year_first.time() != self.time_00:
            year_start = year_first + relativedelta(years=1) - timedelta(hours=year_first.hour, minutes=year_first.minute, seconds=year_first.second)
        else:
            year_start = year_first

        year_stop = year_start + relativedelta(years=num) - timedelta(seconds=1)

        year_one_stop = year_start + relativedelta(years=1) - timedelta(seconds=1)
        year_last_front = year_last - relativedelta(months=year_last.month-1, days=year_last.day-1, hours=year_last.hour, minutes=year_last.minute, seconds=year_last.second)

        # 1년 주기 동안 데이터 개수
        year_freq_count = len(data[year_start:year_one_stop])
        # 마지막 연도 데이터 개수
        year_last_count = len(data[year_last_front:year_last])

        if year_freq_count != year_last_count:
            year_end = year_last - relativedelta(months=year_last.month-1) - timedelta(days=year_last.day-1, hours=year_last.hour, minutes=year_last.minute, seconds=year_last.second+1)
        else:
            year_end = year_last


        year_calcul = (year_end.year - year_start.year) +1
        year_count = math.ceil(year_calcul / num)

        dataFrameCollectionResult = []
        for i in range(year_count):
            dataframe_num_year = data[year_start:year_stop]
            dataFrameCollectionResult.append(dataframe_num_year)
                       
            # 저장한 마지막 데이터 범위(23:59:59)에서 1초 추가하여 다음날(00:00:00)로 변경
            year_start = year_stop + timedelta(seconds=1)
            year_stop = year_start + relativedelta(years=num) - timedelta(seconds=1)

            if year_start + relativedelta(years=num) > year_end:
                year_stop = year_end

        print(dataFrameCollectionResult)

  



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
    # bind_params = {'start_time': '2020-07-01T01:00:00Z', 'end_time': '2022-01-14T18:00:00Z'}
    bind_params = {'start_time': '2020-07-01T01:00:00Z', 'end_time': '2021-02-14T23:00:00Z'}

    # db_name="farm_swine_vibes1"
    # ms_name="CO"
    # bind_params = {'start_time': '2021-10-25T00:00:00Z', 'end_time': '2021-11-01T23:10:22Z'}

    # hour cycle test
    # db_name = "farm_swine_vibes1"
    # ms_name = "CO"
    # bind_params = {'start_time': '2021-10-22T00:00:22Z', 'end_time': '2021-10-27T23:10:22Z'}


    # month cycle test
    db_name ='energy_solar'
    ms_name ='busan'
    bind_params = {'start_time': '2015-05-02T10:00:00Z', 'end_time': '2020-12-31T23:30:00Z'}
    # bind_params = {'start_time': '2015-05-02T10:00:00Z', 'end_time': '2015-05-03T00:00:00Z'}


    # data_get = db_setting.get_data(db_name, ms_name)

    data_get = db_setting.get_data_by_time(bind_params, db_name, ms_name)
    # print(data_get, "\n\n\n")

    # hourCycle = CycleData().getHourCycleSet(data_get,10)
    # print(hourCycle)

    # dayCycle = CycleData().getDayCycleSet(data_get,3)
    # print(dayCycle)

    # weekCycle = CycleData().getWeekCycleSet(data_get, 3)
    # print(weekCycle)

    # monthCycle = CycleData().getMonthCycleSet(data_get, 3)
    # print(monthCycle)

    yearCycle = CycleData().getYearCycleSet(data_get, 3)
    # print(yearCycle)



    # 장기간 데이터 불옴
    # 위 클래스에 대한 각각의 펑션에 대한 결과가 제대로 나올 수 있도록