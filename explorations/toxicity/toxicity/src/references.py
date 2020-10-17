import numpy as np
import pandas as pd


class References:

    def __init__(self):

        self.name = ''

    @staticmethod
    def gazetteer() -> pd.DataFrame:
        """
        Geographic reference data

        :return:
        """

        urlgazetteer = 'https://raw.githubusercontent.com/briefings/sars/develop/' \
                       'fundamentals/hopkins/warehouse/gazetteer.csv'

        try:
            data = pd.read_csv(filepath_or_buffer=urlgazetteer, header=0,
                               usecols=['STUSPS', 'STATE', 'COUNTYGEOID', 'COUNTY'],
                               dtype={'STUSPS': str, 'STATE': str, 'COUNTYGEOID': str, 'COUNTY': str},
                               encoding='utf-8')
        except OSError as err:
            raise err

        return data

    @staticmethod
    def accumulations() -> pd.DataFrame:
        """
        The latest per capita metrics

        :return:
        """

        urlaccumulations = 'https://raw.githubusercontent.com/briefings/sars/develop/' \
                           'fundamentals/hopkins/warehouse/accumulations.csv'

        fields = ['datetimeobject', 'STUSPS', 'COUNTYGEOID', 'deathRate', 'positiveRate']
        dtype = {'STUSPS': str, 'COUNTYGEOID': str, 'deathRate': np.float, 'positiveRate': np.float}
        parse_dates = ['datetimeobject']

        try:
            data = pd.read_csv(filepath_or_buffer=urlaccumulations, header=0,
                               usecols=fields, dtype=dtype, encoding='utf-8', parse_dates=parse_dates)
        except OSError as err:
            raise err

        return data

    @staticmethod
    def sources():
        """
        Reads-in the inventory of toxin data files that should be read

        :return:
        """

        urlpatterns = 'https://raw.githubusercontent.com/briefings/sars/develop/' \
                      'fundamentals/risks/warehouse/risk_.csv'

        try:
            data = pd.read_csv(filepath_or_buffer=urlpatterns, header=0,
                               usecols=['file', 'pattern'], dtype={'file': str, 'pattern': str}, encoding='utf-8')
        except OSError as err:
            raise err

        return data
