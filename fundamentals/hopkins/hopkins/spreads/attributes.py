import numpy as np


class Attributes:

    def __init__(self, level: str):

        # The candle, i.e., quantile, points of interest
        self.points = np.array((0.1, 0.25, 0.5, 0.75, 0.9))

        # county or state
        self.level = level

    @staticmethod
    def variables():
        return ['positiveIncrease', 'deathIncrease', 'positiveCumulative',
                'deathCumulative', 'positiveIncreaseRate', 'deathIncreaseRate',
                'positiveRate', 'deathRate']

    def fields(self):

        fv = ['epochmilli', 'STUSPS', 'positiveIncrease', 'deathIncrease',
              'positiveCumulative', 'deathCumulative',
              'positiveIncreaseRate', 'deathIncreaseRate',
              'positiveRate', 'deathRate', 'ndays']

        if self.level == 'county':
            fv.append('COUNTYGEOID')

        return fv

    def dtype(self):

        dv = {'epochmilli': np.longlong, 'STUSPS': 'str',
              'positiveIncrease': np.float64, 'deathIncrease': np.float64,
              'positiveCumulative': np.float64, 'deathCumulative': np.float64,
              'positiveIncreaseRate': np.float64, 'deathIncreaseRate': np.float64,
              'positiveRate': np.float64, 'deathRate': np.float64, 'ndays': np.int64}

        if self.level == 'county':
            dv['COUNTYGEOID'] = 'str'

        return dv
