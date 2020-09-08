import dask
import numpy as np
import pandas as pd


class Quantiles:

    def __init__(self, data: pd.DataFrame, places: np.ndarray, placestype: str):
        """
        The index of the DataFrame must be a 'datetime' object
        """

        self.data = data
        self.places = places
        self.placestype = placestype

    @dask.delayed
    def algorithm(self, period: int, quantile: float):
        """
        :params period: The calculation period (days)
        :params quantile: 0 < quantile < 1
        """

        values = self.data.rolling('{period}d'.format(period=period), axis=0).quantile(quantile=quantile)

        return values.iloc[(period - 1):, :]

    @dask.delayed
    def structure(self, blob, period: int, quantiletype: str):
        values = blob.reset_index(drop=False, inplace=False)

        return values.melt(id_vars='datetimeobject',
                           value_vars=self.places,
                           var_name=self.placestype,
                           value_name=quantiletype)

    @dask.delayed
    def label(self, blob: pd.DataFrame, period: int):

        blob.loc[:, 'period'] = '{} days'.format(period)
        blob.loc[:, 'weeks'] = float(period/7)

        return blob

    def exc(self, periods: np.ndarray, quantile: float, quantiletype: str):
        computations = []
        for period in periods:
            medians = self.algorithm(period=period, quantile=quantile)
            values = self.structure(blob=medians, period=period, quantiletype=quantiletype)
            values = self.label(blob=values, period=period)
            computations.append(values)

        dask.visualize(computations, filename='medians', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
