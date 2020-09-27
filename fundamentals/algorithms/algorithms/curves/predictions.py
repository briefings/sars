import dask
import pandas as pd


class Predictions:

    def __init__(self, partitionby: str):
        """

        :param partitionby: The name of the field used for partitioning.
        """

        self.partitionby = partitionby

    def structure(self, predictions: pd.DataFrame, partition: str):
        """

        :param predictions: The predictions w.r.t. a set of regressors
        :param partition: The data partition in focus
        :return:
        """

        values = predictions.copy()
        fields = values.columns

        values.loc[:, 'minimum'] = values[fields].min(axis=1)
        values.loc[:, 'median'] = values[fields].median(axis=1)
        values.loc[:, 'maximum'] = values[fields].max(axis=1)
        values.loc[:, self.partitionby] = partition
        values.drop(columns=fields, inplace=True)

        return values

    @staticmethod
    def compute(predictions_) -> pd.DataFrame:
        """

        :param predictions_: delayed dask object
        :return:
        """

        frame = dask.compute(predictions_, scheduler='processes')[0]
        values = pd.concat(frame, axis=0, ignore_index=False)
        values.reset_index(drop=False, inplace=True)

        return values
