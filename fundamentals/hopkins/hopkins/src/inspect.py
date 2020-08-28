import numpy as np


class Inspect:

    def __init__(self):

        self.name = ''

    @staticmethod
    def fips(data):

        data = data[data['FIPS'].notna()].copy()
        data.reset_index(drop=True, inplace=True)
        data.loc[:, 'FIPS'] = data['FIPS'].astype('int64')
        data.loc[:, 'FIPS'] = data['FIPS'].astype(str).str.zfill(5)

        return data

    @staticmethod
    def sequencing(points: np.ndarray):

        series = points
        indices: np.ndarray = (series[1:] < series[:-1])

        while indices.sum() > 0:
            series[:-1][indices] = series[1:][indices]
            indices = series[1:] < series[:-1]

        return series
