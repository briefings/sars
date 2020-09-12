import pandas as pd
import numpy as np

import algorithms.base.differences


class Doublet:

    def __init__(self, blob: pd.DataFrame, periods: np.ndarray, places: np.ndarray, placestype: str):
        """

        :param blob:
        :param periods: Difference calculations time periods, days.
        :param places: Calculations are conducted per unique place
        :param placestype: This will be the field name of the places data
        """

        self.blob = blob
        self.periods = periods
        self.places = places
        self.placestype = placestype

        self.suffix = 'Diff'

    def algorithm(self, event: str, fieldname: str):
        """
        Differences calculator

        :param event: The column/field the difference calculation will be applied to
        :param fieldname: The fieldname of the new field of differences
        :return:
        """

        # Focus on
        base = self.blob[['datetimeobject', 'STUSPS', event]].copy()

        # Pivot -> such that each field is a place, and each instance of a field is a date in time
        segment = base.pivot(index='datetimeobject', columns='STUSPS', values=event)

        # Determine ...
        # values.rename(columns={'delta': fieldname}, inplace=True)
        differences = algorithms.base.differences.\
            Differences(data=segment, places=self.places, placestype=self.placestype)
        values = differences.exc(periods=self.periods, fieldname=fieldname)

        return values

    def exc(self, numerator: str, denominator: str):
        """
        Calculates rates w.r.t. numerator & denominator

        :param numerator:
        :param denominator:
        :return:
        """

        values = pd.DataFrame()
        for event in [numerator, denominator]:

            calculations = self.algorithm(event=event, fieldname=(event + self.suffix))
            if values.empty:
                values = calculations
            else:
                values = values.merge(calculations, how='inner', on=['datetimeobject', 'STUSPS', 'period'])

        values.loc[:, 'rates'] = \
            (100 * values[numerator + self.suffix] / values[denominator + self.suffix]).fillna(value=0)

        return values.drop(columns=[numerator + self.suffix, denominator + self.suffix])
