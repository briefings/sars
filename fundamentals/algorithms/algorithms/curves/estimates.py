import dask
import numpy as np
import pandas as pd


class Estimates:

    def __init__(self, date: pd.Timestamp, partitionby: str):
        """

        :param date: Latest data date.
        :param partitionby: The name of the field used for partitioning.
        """

        self.date = date
        self.partitionby = partitionby

    def structure(self, estimates: pd.DataFrame, partition: str):
        """
        Structures the statsmodel estimates

        :param estimates: The [statsmodel] model's gradient, parameters, confidence intervals, etc
        :param partition: The data partition in focus
        :return:
        """
        values = estimates.copy()

        gradient = values.gradient.median()
        lowerconfidenceinterval = values.lowerconfidenceinterval.min()
        upperconfidenceinterval = values.upperconfidenceinterval.max()
        pvalue = values.pvalue.median()
        rsquared = values.rsquared.median()
        intercept = values.intercept.median()

        array = np.array([[self.date, gradient, lowerconfidenceinterval,
                           upperconfidenceinterval, pvalue, rsquared, intercept, partition]])

        return pd.DataFrame(data=array,
                            columns=['datetimeobject', 'gradient', 'lowerconfidenceinterval',
                                     'upperconfidenceinterval', 'pvalue', 'rsquared', 'intercept', self.partitionby])

    @staticmethod
    def compute(estimates_) -> pd.DataFrame:
        """

        :param estimates_: delayed dask object
        :return:
        """

        frame = dask.compute(estimates_, scheduler='processes')[0]
        values = pd.concat(frame, axis=0, ignore_index=False)
        values.reset_index(drop=True, inplace=True)

        return values
