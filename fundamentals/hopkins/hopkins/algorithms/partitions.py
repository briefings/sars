"""
Slices a DataFrame into partitions w.r.t. a field, e.g., the county code field, such
that each partition has a single county's data only, and each is written to a separate
file.
"""

import pandas as pd
import os
import multiprocessing as mp
import config

import hopkins.base.directories


class Partitions:

    def __init__(self, blob: pd.DataFrame, partitionby: str):
        """
        Details ...
        :param blob: A DataFrame of counties data
        :param partitionby: The field to partition by when writing to file; the unique values of this field are
                            used to create the data batches that are saved to distinct files.
        """

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        self.blob = blob

        self.partitionby = partitionby
        parts = self.blob[self.partitionby].unique()
        self.partitions = [{part} for part in parts]

    @staticmethod
    def paths(path):
        directories = hopkins.base.directories.Directories()
        directories.create(listof=[path])

    def write(self, path, partition: set):

        name = partition.pop()

        data = self.blob.copy()
        data = data[data[self.partitionby] == name]
        data.to_csv(path_or_buf=os.path.join(path, name + '.csv'), index=False, encoding='utf-8', header=True)
        return True

    def exc(self, segment: str):
        """

        :param segment: baselines, candles, increases, etc
        :return:
        """

        path = os.path.join(self.warehouse, segment)
        self.paths(path=path)

        partitions = self.partitions

        pool = mp.Pool(mp.cpu_count())
        pool.starmap(self.write, [(path, i) for i in partitions])
        pool.close()
