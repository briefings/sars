import dask
import numpy as np
import pandas as pd


class Difference:

    def __init__(self, data: pd.DataFrame, places: np.ndarray, placestype: str):
        self.data = data
        self.places = places
        self.placestype = placestype

    @staticmethod
    def gap(y: np.ndarray):
        """
        Pecentage difference calculator

        :param y: A data vector/array
        :return:
        """

        return y[-1] - y[0]

    @dask.delayed
    def algorithm(self, period: int):
        """
        For rolling difference calculations

        :param period: The number of days over which a difference is calculated
        :return:
        """

        values = self.data.rolling(window='{}d'.format(period), axis=0).apply(self.gap, raw=True)

        return values.iloc[(period - 1):, :]

    @dask.delayed
    def structure(self, blob: pd.DataFrame):
        """
        Structuring the rolling windows calculations

        :param blob:
        :return:
        """

        values = blob.reset_index(drop=False, inplace=False)

        return values.melt(id_vars='datetimeobject',
                           value_vars=self.places,
                           var_name=self.placestype,
                           value_name='delta')

    @dask.delayed
    def label(self, blob: pd.DataFrame, period: int):
        """

        :param blob:
        :param period: The number of days over which a percentage difference was calculated
        :return:
        """

        blob.loc[:, 'period'] = '{} days'.format(period)

        return blob

    def exc(self, periods: np.ndarray):
        """

        :param periods: An array of days numbers over which percentage differences will be calculated
        :return:
        """

        computations = []
        for period in periods:
            gaps = self.algorithm(period=period)
            values = self.structure(blob=gaps)
            values = self.label(blob=values, period=period)
            computations.append(values)

        dask.visualize(computations, filename='gap', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
