import dask
import numpy as np
import pandas as pd


class Differences:

    def __init__(self, data: pd.DataFrame, places: np.ndarray, placestype: str):
        """

        :param data:
        :param places: Calculations are conducted per unique place
        :param placestype: This will be the field name of the places data
        """
        self.data = data
        self.places = places
        self.placestype = placestype

    @staticmethod
    def difference(y: np.ndarray):
        """
        The difference calculator

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

        values = self.data.rolling(window='{}d'.format(period), axis=0).apply(self.difference, raw=True)

        return values.iloc[(period - 1):, :]

    @dask.delayed
    def structure(self, blob: pd.DataFrame, fieldname: str):
        """
        Structuring the rolling windows calculations

        :param blob:
        :param fieldname: A name for the new differences field
        :return:
        """

        values = blob.reset_index(drop=False, inplace=False)

        return values.melt(id_vars='datetimeobject',
                           value_vars=self.places,
                           var_name=self.placestype,
                           value_name=fieldname)

    @dask.delayed
    def label(self, blob: pd.DataFrame, period: int):
        """

        :param blob:
        :param period: The number of days over which a percentage difference was calculated
        :return:
        """

        blob.loc[:, 'period'] = '{} days'.format(period)

        return blob

    def exc(self, periods: np.ndarray, fieldname: str):
        """

        :param periods: An array of days numbers over which percentage differences will be calculated
        :param fieldname: A name for the new differences field
        :return:
        """

        computations = []
        for period in periods:
            subtractions = self.algorithm(period=period)
            values = self.structure(blob=subtractions, fieldname=fieldname)
            values = self.label(blob=values, period=period)
            computations.append(values)

        dask.visualize(computations, filename='gap', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
