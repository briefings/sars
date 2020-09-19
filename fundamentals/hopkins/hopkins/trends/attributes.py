import numpy as np
import pandas as pd

import hopkins.base.directories


class Attributes:

    def __init__(self):
        self.name = ''

    @staticmethod
    def storage(listof: list):

        directories = hopkins.base.directories.Directories()
        directories.create(listof=listof)

    @staticmethod
    def variables():
        return ['positiveIncrease', 'deathIncrease', 'positiveCumulative', 'deathCumulative',
                'positiveIncreaseRate', 'deathIncreaseRate', 'positiveRate', 'deathRate']

    @staticmethod
    def fields():
        return ['datetimeobject', 'COUNTYGEOID',
                'positiveIncrease', 'deathIncrease', 'positiveCumulative', 'deathCumulative',
                'positiveIncreaseRate', 'deathIncreaseRate', 'positiveRate', 'deathRate', 'ndays']

    @staticmethod
    def dtype():
        return {'COUNTYGEOID': 'str', 'positiveIncrease': np.float64, 'deathIncrease': np.float64,
                'positiveCumulative': np.float64, 'deathCumulative': np.float64,
                'positiveIncreaseRate': np.float64, 'deathIncreaseRate': np.float64,
                'positiveRate': np.float64, 'deathRate': np.float64, 'ndays': np.int64}

    @staticmethod
    def parse_dates():
        return ['datetimeobject']

    def read(self, filestring: str) -> pd.DataFrame:
        """
        Reads the data of file 'filestring'

        :param filestring:
        :return:
        """

        try:
            data = pd.read_csv(filepath_or_buffer=filestring, header=0, encoding='utf-8',
                               usecols=self.fields(),
                               dtype=self.dtype(),
                               parse_dates=self.parse_dates())
        except OSError as err:
            raise err

        return data
