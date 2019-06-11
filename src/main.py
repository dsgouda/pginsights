#load the library and check its version, just to make sure we aren't using an older version
import numpy as np
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import sys
import itertools

class SimpleDataAnalyzer:
    __dbName=''
    __dbTableName=''
    __rawData = None
    __numericCols = None
    __timeSeriesCols = None

    def __init__(self, dbName, dbTableName):
        self.__dbName = dbName
        self.__dbTableName = dbTableName

    def _read_from_db(self, query):
        conn = None
        data = None
        try:
            #print('Connecting to the DB')
            conn = pg.connect(database = self.__dbName, user='postgres', host='localhost', port=5432, password='password123')
            data = pd.read_sql_query(query, conn)
        except pg.DatabaseError as e:
            print('Error', e)
            sys.exit(1)
        finally:   
            if conn:
                conn.close()
            #print('Exiting connect method')
        
        return data

    def _get_base_schema_query(self):
        return "SELECT column_name FROM information_schema.columns WHERE table_name = \'"+self.__dbTableName+"\' "

    def _get_numeric_cols(self):
        if self.__numericCols is None:
            numericColsQuery = self._get_base_schema_query() + "AND data_type in ('smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallserial', 'serial', 'bigserial', 'money')"
            self.__numericCols = self._read_from_db(numericColsQuery)['column_name']
        return self.__numericCols

    def _get_time_series_cols(self):
        if self.__timeSeriesCols is None:
            # fetch all the date column types
            dateColQuery = self._get_base_schema_query() + "AND data_type in ('time', 'timestamp', 'date')"
            self.__timeSeriesCols = self._read_from_db(dateColQuery)['column_name']
        return self.__timeSeriesCols

    def _get_trendline_slope(self, data):
        coeffs = np.polyfit(data.index.values, list(data), 1)
        slope = coeffs[-2]
        return float(slope)

    def _get_raw_data(self):
        if self.__rawData is None:
            self.__rawData = self._read_from_db("SELECT * FROM "+self.__dbTableName)
        return self.__rawData

    def detect_trendline(self):
        timeSeriesCols = self._get_time_series_cols()
        if(len(timeSeriesCols)==0):
            print('No Time-series columns found, cannot fit a trendline.')
            return

        dfTable = self._get_raw_data()
        numericCols = self._get_numeric_cols()
        for timeSeriesCol in timeSeriesCols:
            timeSeriesSortedData = dfTable.sort_values([timeSeriesCol])
            for numericCol in numericCols:
                z = self._get_trendline_slope(timeSeriesSortedData[numericCol])
                if(z>0):
                    print('Positive trend detected for '+numericCol+' with a slope of '+str(z))

    def detect_cross_correlation(self):
        data = self._get_raw_data()
        numericCols = self._get_numeric_cols()
        colPairs = list(itertools.combinations(numericCols, 2))
        for pair in colPairs:
            corr = np.corrcoef(data[pair[0]], data[pair[1]])[0,1]
            if corr<-0.80 or corr>0.80:
                print('found correlation between '+pair[0]+' and '+pair[1]+' with a correlation coefficient of '+str(corr))

    def detect_anomalies_in_col(self, colData):
        # Set upper and lower limit to 3 standard deviation
        colDataStd = np.std(colData)
        colDataMean = np.mean(colData)
        cutOff = colDataStd * 3
        
        lower_limit  = colDataMean - cutOff
        upper_limit = colDataMean + cutOff
        return list(filter(lambda x: x>upper_limit or x<lower_limit, colData))

    def detect_anamolies(self):
        data = self._get_raw_data()
        numericCols = self._get_numeric_cols()
        for col in numericCols:
            anomalies = self.detect_anomalies_in_col(data[col])
            if(len(anomalies)):
                print('Found anomalies in column '+col)
                print(anomalies)

def start_prog():
    # cross correlation example
    print('Cross correlation example with the happiness db')
    analyzer1 = SimpleDataAnalyzer('postgres', 'happiness')
    analyzer1.detect_cross_correlation()

    # trendline example
    print('Trendline example with the beer db')
    analyzer2 = SimpleDataAnalyzer('postgres', 'beer')
    analyzer2.detect_trendline()
    print('Corss correlation example with the beer db')
    analyzer2.detect_cross_correlation()
    print('Finding anomalies')
    analyzer2.detect_anamolies()

if __name__ == '__main__':
    start_prog()