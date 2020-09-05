import numpy as np


class Attributes:

    def __init__(self):
        # The candle, i.e., quantile, points of interest
        self.points = np.array((0.1, 0.25, 0.5, 0.75, 0.9))

    @staticmethod
    def variables():
        return ['positiveIncrease', 'deathIncrease', 'positiveCumulative',
                'deathCumulative', 'positiveIncreaseRate', 'deathIncreaseRate',
                'positiveRate', 'deathRate']

    @staticmethod
    def fields():
        return ['epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveIncrease', 'deathIncrease',
                'positiveCumulative', 'deathCumulative',
                'positiveIncreaseRate', 'deathIncreaseRate',
                'positiveRate', 'deathRate', 'ndays']

    @staticmethod
    def dtype():
        return {'epochmilli': np.longlong, 'STUSPS': 'str', 'COUNTYGEOID': 'str',
                'positiveIncrease': np.float64, 'deathIncrease': np.float64,
                'positiveCumulative': np.float64, 'deathCumulative': np.float64,
                'positiveIncreaseRate': np.float64, 'deathIncreaseRate': np.float64,
                'positiveRate': np.float64, 'deathRate': np.float64, 'ndays': np.int64}
