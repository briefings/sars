import logging

import numpy as np
import pandas as pd

import config
import atlantic.algorithms.inspect


class Anomalies:
    """
    Addressing cumulative values discrepancies w.r.t. cumulative measures
    """

    def __init__(self, blob: pd.DataFrame):
        """

        :param blob: The latest, re-structured & checked, C.T.P. data.  It must include
        'STUSPS', 'datetimeobject', and the fields summarised in self.measures.
        """

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        configurations = config.Config()
        _, self.measures, _, _ = configurations.variables()

        # The data set;
        self.blob = blob

    @staticmethod
    def discrepancies(segment: pd.DataFrame, measure: str):
        """

        :param segment: A frame of STUSPS, date, and measure
        :param measure: The measure in focus
        :return:
        """

        inspect = atlantic.algorithms.inspect.Inspect()

        # Create a table matrix w.r.t. states & a single measure; subsequently ensure that
        # the columns, which are dates, are sequential
        matrix = segment.pivot(index='STUSPS', columns='datetimeobject', values=measure)
        matrix.sort_index(axis=1, inplace=True)

        # Per state, i.e., row, detect and address cases wherein c[i + 1] < c[i]; each
        # county's series should represent a cumulative series.
        anomalies = np.apply_along_axis(func1d=inspect.sequencing, axis=1, arr=matrix.values)

        # Assign the inspected values
        matrix.loc[:, matrix.columns.values] = anomalies

        return matrix

    @staticmethod
    def sequences(adjusted: pd.DataFrame, measure: str):
        """

        :param adjusted: Inspected ...
        :param measure: The measure in focus
        :return: Discrete values (discrete), and new calculations of cumulative values (continuous)
        """

        differences = adjusted.diff(periods=1, axis=1).fillna(value=0, inplace=False)
        accumulations = differences.cumsum(axis=1)

        differences.reset_index(drop=False, inplace=True)
        accumulations.reset_index(drop=False, inplace=True)

        discrete = differences.melt(id_vars='STUSPS',
                                    value_vars=differences.columns.drop(labels=['STUSPS']),
                                    var_name='datetimeobject',
                                    value_name=measure.replace('Cumulative', 'Increase'))

        continuous = accumulations.melt(id_vars='STUSPS',
                                        value_vars=differences.columns.drop(labels=['STUSPS']),
                                        var_name='datetimeobject',
                                        value_name=measure)

        return discrete, continuous

    def estimate(self):

        data = self.blob.copy()

        for measure in self.measures:
            # Calculations are conducted per measure, but w.r.t. distinct state & time combinations
            segment = data[['datetimeobject', 'STUSPS', measure]]

            # The function/method 'discrepancies' addresses cumulative values anomalies.  It returns a
            # matrix table wherein each row represents a state, and each column a date.
            adjusted = self.discrepancies(segment=segment, measure=measure)

            # Hence, determine ...
            discrete, continuous = self.sequences(adjusted=adjusted, measure=measure)

            # Append ... Increase (discrete)
            data = data.merge(discrete, how='left', on=['STUSPS', 'datetimeobject'])

            # Drop initial ... Cumulative
            data.drop(columns=measure, inplace=True)

            # Append re-calculated ...Cumulative (continuous)
            data = data.merge(continuous, how='left', on=['STUSPS', 'datetimeobject'])

        return data

    def exc(self):
        """

        :return:
        """

        estimate = self.estimate()
        self.logger.info('\n{}\n'.format(estimate.tail(n=33)))

        return estimate
