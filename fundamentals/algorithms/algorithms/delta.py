import dask
import numpy as np
import pandas as pd

import logging


class Delta:

    def __init__(self, data: pd.DataFrame, places: np.ndarray, placestype: str):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.data = data
        self.places = places
        self.placestype = placestype

    @staticmethod
    def rate(y: np.ndarray):
        """
        Pecentage difference calculator

        :param y: A data vector/array
        :return:
        """

        return np.nan if y[0] == 0 else 100 * (y[-1] - y[0]) / y[0]

    @dask.delayed
    def algorithm(self, period: int):
        """
        For rolling percentage difference calculations

        :param period: The number of days over which a percentage difference is calculated
        :return:
        """

        values = self.data.rolling(window='{}d'.format(period), axis=0).apply(self.rate, raw=True)
        self.logger.info('\n{}\n'.format(values))

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
            rates = self.algorithm(period=period)
            values = self.structure(blob=rates)
            values = self.label(blob=values, period=period)
            computations.append(values)

        dask.visualize(computations, filename='delta', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
