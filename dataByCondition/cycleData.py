import sys
import os

from sklearn import datasets
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))


class CycleData():
    """
    Prepare Data based on cycle parameter
    """
    def __init__(self, data):
        self.start, self.end = self.getTimePointByDayUnit(data)
        self.data = data[self.start: self.end] #(??)
       
    def getTimePointByDayUnit(self, data):
        # data 를  정렬시킨 후 명확히 일자가 시작하는 위치 00:00:00에서~ 23:59:59 (맞나요?) 까지 끊고 그것에 대한  start, end time을 구한다.
        start = 
        end = 
        return start, end

    def getHourCycleSet(self, data, num=1):
        return dataFrameCollectionResult

    def getDayCycleSet(self, data, num=1):
        # day 단위의 데이터 셋 리턴
        #
        # 
        return dataFrameCollectionResult

    def getWeekCycleSet(self, data, num=1):
        # Week 단위의 데이터 셋 리턴
        return dataFrameCollectionResult

    def getMonthCycleSet(self, data, num=1):
        #  Month 단위의 데이터셋 리턴
        return dataFrameCollectionResult

    def getYearCycleSet(self, data, num=1):
        # Year 단위의 데이터셋 리턴

        return dataFrameCollectionResult

    def getDataFrameCollectionToSeriesDataType(self, datasetCollection):
        return seriesDataset

    
if __name__ == '__main__':
    # 장기간 데이터 불옴
    # 위 클래스에 대한 각각의 펑션에 대한 결과가 제대로 나올 수 있도록