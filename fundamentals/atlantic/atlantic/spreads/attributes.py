import os

import numpy as np

import config


class Attributes:

    def __init__(self):
        configurations = config.Config()

        # Data Warehouse Path
        self.warehouse = configurations.warehouse

        # Data source
        self.sourcename = 'candles.csv'
        self.sourcestring = os.path.join(self.warehouse, self.sourcename)

        # Write calculations to directory ...
        self.path: str = os.path.join(self.warehouse, 'candles')

        # The candle, i.e., quantile, points of interest
        self.points = np.array((0.1, 0.25, 0.5, 0.75, 0.9))

    @staticmethod
    def categories():
        return ['positiveIncrease', 'testIncrease', 'deathIncrease', 'positiveCumulative', 'testCumulative',
                'deathCumulative', 'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                'positiveRate', 'testRate', 'deathRate']

    @staticmethod
    def fields():
        return ['datetimeobject', 'STUSPS', 'positiveIncrease', 'testIncrease', 'deathIncrease',
                'positiveCumulative', 'testCumulative', 'deathCumulative',
                'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                'positiveRate', 'testRate', 'deathRate', 'ndays']

    @staticmethod
    def dtype():
        return {'STUSPS': 'str', 'positiveIncrease': np.float64, 'testIncrease': np.float64,
                'deathIncrease': np.float64, 'positiveCumulative': np.float64, 'testCumulative': np.float64,
                'deathCumulative': np.float64, 'positiveIncreaseRate': np.float64, 'testIncreaseRate': np.float64,
                'deathIncreaseRate': np.float64, 'positiveRate': np.float64, 'testRate': np.float64,
                'deathRate': np.float64, 'ndays': np.int64}

    @staticmethod
    def parse_dates():
        return ['datetimeobject']
