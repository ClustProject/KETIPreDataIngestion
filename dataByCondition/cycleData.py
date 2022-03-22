import sys
import os
from datetime import datetime, timedelta
from sklearn import datasets
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIPreDataIngestion.data_influx.influx_Client import influxClient


class CycleData():
    """
    Prepare Data based on cycle parameter
    """
    def __init__(self):
        # self.start, self.end = self.getTimePointByDayUnit(data)
        # self.data = data[self.start: self.end] #(??)
        global influxdb
        influxdb = influxClient(ins.CLUSTDataServer)
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
        print("=======hourcycle=======")
        print(data)

        print("=====timestamp year,month etc...=====")
        data_hour = data.index[0].hour
        print("hour: ",data_hour)

        print("=====first=====")
        data_index_first = data.index[0]
        print(data_index_first)

        print("======last=====")
        data_index_last = data.index.max()
        print(data_index_last)

        data_start = data.index.min()
        # data.loc[]
        print("======columns======")
        print(data.columns)

        for col in data.columns:
            print(col)

        print("======dict======")
        test_aa = data.to_dict('indeX')
        print(test_aa)

        dataFrameCollectionResult = []

        for df in data.index:
            dataframe_num_day = {}
            # print(df)
            if data_hour == 0 & data_hour == 23:
                aa = '정상'
            elif data_index_first != 0:
                aa = '앞 삭제'
            elif  data_index_last != 23:
                aa = '뒤 삭제'
            dataFrameCollectionResult.append(dataframe_num_day)


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

        # 시간 처리를 위한 시,분,초 계산
        # print("=====timestamp year,month etc...=====")
        data_hour = data.index[0].hour
        data_minute = data.index[0].minute
        data_second = data.index[0].second

        # print("data_hour: ",data_hour)
        # print("data_minute: ",data_minute)
        # print("data_second: ",data_second)

        # print("=================\n\n")


        # 첫 시간과 마지막 시간 구하기
        data_first = data.index.min()
        data_last = data.index.min()
        # print("data_first",data_first)
        # print("data_last",data_last)


        # data.loc[]
        # print("======columns======")
        # print(data.columns)
        # for col in data.columns:
        #     print(col)
        # print("======dict======")
        # test_aa = data.to_dict('index')
        # print(test_aa)


        dataFrameCollectionResult = []

        for i in data.index[0:10]:
            print("\n=========start===========")
            print(i)
            if data_hour == 0 & data_hour == 23:
                pass
            elif data_hour != 0:
                # 처음이 0이 아닌 첫 시간 -> 공식 : (다음날) - (초과된 시,분,초) 
                data_first = data_first + timedelta(days=num) - timedelta(hours=data_hour, minutes=data_minute, seconds=data_second)
                print("====update data first====")
                print(data_first)

                # 가져올 범위 지정 -> data_first에서 가져올 day만큼 설정
                data_last = data_first + timedelta(days=num) - timedelta(seconds=1)
                print("-----data last-------")
                print(data_last)

                print("================\n")
            elif  data_last != 23:
                break
            
            # print("=====dataframe_num_day=====")
            # dataframe_num_day = data[data_first:data_last]
            # print(dataframe_num_day)

            # # dataFrameCollectionResult.append(dataframe_num_day)

            # # 데이터 입력 후 data_first 조정
            # data_first = data_last + timedelta(seconds=1)
            # print("\n last to first")
            # print(data_first)


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

    db_setting = influxClient(ins.CLUSTDataServer)
    db_name="energy_wind_power"
    ms_name="jeju"

    # db_name="farm_strawberry_jinan"
    # ms_name="environment"

    # db_name="culture_subway"
    # ms_name="seoul"

    import pandas as pd
    bind_params = {'start_time': '2014-01-01T00:00:00Z', 'end_time': '2014-01-10T00:00:00Z'}
    # bind_params = {'start_time': '2021-03-31T15:00:00.000Z', 'end_time': '2021-04-29T15:00:00.000Z'}
    data_get = db_setting.get_data_by_time(bind_params, db_name, ms_name)
    print(data_get, "\n\n\n")

    hourCycle = CycleData().getDayCycleSet(data_get, 1)
    # 장기간 데이터 불옴
    # 위 클래스에 대한 각각의 펑션에 대한 결과가 제대로 나올 수 있도록