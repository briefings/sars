import os

import numpy as np

import config


class Attributes:

    def __init__(self):
        configurations = config.Config()

        # Data Warehouse Path
        self.warehouse = configurations.warehouse

        # Data source
        self.sourcename = 'baselines.csv'
        self.sourcestring = os.path.join(self.warehouse, self.sourcename)

        # Write calculations to directory ...
        self.path: str = os.path.join(self.warehouse, 'candles')

        # The candle, i.e., quantile, points of interest
        self.points = np.array((0.1, 0.25, 0.5, 0.75, 0.9))

    @staticmethod
    def variables():
        return ['positiveIncrease', 'testIncrease', 'deathIncrease', 'hospitalizedIncrease', 'icuIncrease',
                'positiveCumulative', 'testCumulative', 'deathCumulative', 'hospitalizedCumulative', 'icuCumulative',
                'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                'hospitalizedIncreaseRate', 'icuIncreaseRate',
                'positiveRate', 'testRate', 'deathRate', 'hospitalizedRate', 'icuRate',
                'hospitalizedCurrently', 'icuCurrently']

    @staticmethod
    def fields():
        return ['datetimeobject', 'STUSPS',
                'positiveIncrease', 'testIncrease', 'deathIncrease', 'hospitalizedIncrease', 'icuIncrease',
                'positiveCumulative', 'testCumulative', 'deathCumulative', 'hospitalizedCumulative', 'icuCumulative',
                'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                'hospitalizedIncreaseRate', 'icuIncreaseRate',
                'positiveRate', 'testRate', 'deathRate', 'hospitalizedRate', 'icuRate',
                'hospitalizedCurrently', 'icuCurrently', 'ndays']

    @staticmethod
    def dtype():
        return {'STUSPS': 'str',
                'positiveIncrease': np.float64, 'testIncrease': np.float64,
                'deathIncrease': np.float64, 'hospitalizedIncrease': np.float, 'icuIncrease': np.float,
                'positiveCumulative': np.float64, 'testCumulative': np.float64,
                'deathCumulative': np.float64, 'hospitalizedCumulative': np.float, 'icuCumulative': np.float,
                'positiveIncreaseRate': np.float64, 'testIncreaseRate': np.float64,
                'deathIncreaseRate': np.float64, 'hospitalizedIncreaseRate': np.float, 'icuIncreaseRate': np.float,
                'positiveRate': np.float64, 'testRate': np.float64,
                'deathRate': np.float64, 'hospitalizedRate': np.float, 'icuRate': np.float,
                'hospitalizedCurrently': np.float64, 'icuCurrently': np.float64, 'ndays': np.int64}

    @staticmethod
    def parse_dates():
        return ['datetimeobject']
