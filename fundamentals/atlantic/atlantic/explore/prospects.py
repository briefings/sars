import numpy as np
import pandas as pd
import dask

import algorithms.curves.secants
import algorithms.curves.estimates
import algorithms.curves.predictions


class Prospects:

    def __init__(self, data: pd.DataFrame, partitionby: str, periods: np.ndarray,
                 formula: str, regressor: str, step: int, num: int, latestdate: pd.Timestamp):
        """

        :param data: The data
        :param partitionby: The field via which the data is partitioned
        :param periods: Each value of this array indicates the number of sequential curve points that
                        is used to build a model.  Hence, the number of models built is equal to the
                        length of the array.
        :param formula: The Ordinary Least Square formula that is used to build a model of a regression
                        line, e.g., 'positives/100K [C] ~ tests/100K [C]'
        :param regressor: The independent variable, e.g., 'tests/100K [C]', w.r.t. the above formula
        :param step: The gap between the independent variables points that are used to predict new/future
                     dependent variables
        :param num: The number of points to predict
        :param latestdate: The latest date of the data; for inventory purposes.
        """

        self.data = data
        self.partitionby = partitionby
        self.regressor = regressor
        self.step = step
        self.num = num

        self.est = algorithms.curves.estimates.Estimates(date=latestdate, partitionby=partitionby)
        self.pre = algorithms.curves.predictions.Predictions(partitionby=partitionby)

        self.secants = algorithms.curves.secants.Secants(periods=periods, formula=formula, regressor=regressor)

    def get_points(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob: A data partition
        :return: Independent variable prediction points w.r.t. the data partition
        """

        step = self.step
        maximum = dask.compute(blob[self.regressor].max())
        start = step * np.ceil(maximum[0]/step)
        num = self.num
        array = np.linspace(start=start, stop=start+step*(num-1), num=num)

        return pd.DataFrame(data={self.regressor: array})

    @dask.delayed
    def get_partition(self, partition: str):

        values = self.data[self.data[self.partitionby] == partition]
        values.sort_values(by='datetimeobject', ascending=True, inplace=False, ignore_index=True)

        return values

    def exc(self, partitions: np.ndarray):
        """

        :param partitions:
        :return:
        """

        estimates_ = []
        predictions_ = []
        for part in partitions:

            partition = self.get_partition(partition=part)
            points = self.get_points(blob=partition)

            models = self.secants.get_models(blob=partition)
            estimates = self.secants.get_estimates(models=models)
            predictions = self.secants.get_predictions(models=models, points=points)

            estimates_.append(dask.delayed(self.est.structure)(estimates=estimates, partition=part))
            predictions_.append(dask.delayed(self.pre.structure)(predictions=predictions, partition=part))

        dask.visualize(estimates_, filename='estimates', format='pdf')
        dask.visualize(predictions_, filename='predictions', format='pdf')

        return self.est.compute(estimates_=estimates_), self.pre.compute(predictions_=predictions_)
