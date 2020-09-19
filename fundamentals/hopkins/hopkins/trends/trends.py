import os

import numpy as np
import pandas as pd

import algorithms.base.delta
import algorithms.base.quantiles
import hopkins.base.directories
import hopkins.trends.attributes


class Trends:

    def __init__(self, periods: np.ndarray, placestype: str, warehouse: str):
        """

        :param periods: Running periods, e.g., 9, 10, 11, etc., days
        :param placestype: The field of places; calculations are conducted per distinct place
        :param warehouse: The results parent path
        """

        self.periods = periods
        self.placestype = placestype

        self.warehouse = warehouse
        self.medianspath = os.path.join(self.warehouse, 'medians')
        self.deltapath = os.path.join(self.warehouse, 'delta')

        self.attributes = hopkins.trends.attributes.Attributes()
        self.attributes.storage(listof=[self.medianspath, self.deltapath])

    def delta_(self, data: pd.DataFrame, basestring: str, places: np.ndarray):
        """

        :param data:
        :param basestring:
        :param places:
        :return:
        """

        instances = pd.DataFrame()

        for event in ['deathRate', 'positiveRate']:

            # Focus on
            base = data[['datetimeobject', self.placestype, event]]

            # Pivot -> such that each field is a place, and each instance of a field is a date in time
            segment = base.pivot(index='datetimeobject', columns=self.placestype, values=event)

            # The percentage differences
            delta = algorithms.base.delta.Delta(data=segment, places=places, placestype=self.placestype)
            values = delta.exc(periods=self.periods, fieldname=(event + 'Delta'))

            # Include the variable the delta calculations are based on
            values = values.merge(base, how='left', on=['datetimeobject', self.placestype])

            # Structuring
            if instances.empty:
                instances = values
            else:
                instances = instances.merge(values, how='inner', on=['datetimeobject', self.placestype, 'period'])

        instances.to_csv(path_or_buf=os.path.join(self.deltapath, basestring),
                         index=False, encoding='utf-8', header=True)

    def medians_(self, data: pd.DataFrame, basestring: str, places: np.ndarray) -> None:
        """
        Running medians

        :param data:
        :param basestring: The name of the file wherein the calculations will be saved
        :param places: The distinct places inventoried by 'data'
        :return:
        """

        instances: pd.DataFrame = pd.DataFrame()

        for event in ['positiveIncreaseRate', 'deathIncreaseRate']:

            # Focus on
            base = data[['datetimeobject', self.placestype, event]].copy()

            # Pivot -> such that each field is a place, and each instance of a field is a date in time
            segment = base.pivot(index='datetimeobject', columns=self.placestype, values=event)

            # Quantiles
            quantiles = algorithms.base.quantiles.Quantiles(data=segment, places=places, placestype=self.placestype)
            values = quantiles.exc(periods=self.periods, quantile=0.5,
                                   fieldname=(event.replace('IncreaseRate', 'IRM')))

            # Structuring
            if instances.empty:
                instances = values
            else:
                instances = instances.merge(values, how='inner', on=['datetimeobject', self.placestype, 'period'])

        instances.to_csv(path_or_buf=os.path.join(self.medianspath, basestring),
                         index=False, encoding='utf-8', header=True)
