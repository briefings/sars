"""
Slices a DataFrame into partitions w.r.t. a field, e.g., the county code field, such
that each partition has a single county's data only, and each is written to a separate
file.
"""

import multiprocessing as mp
import os

import pandas as pd

import config
import hopkins.algorithms.gridlines
import hopkins.base.directories


class Partitions:

    def __init__(self, blob: pd.DataFrame, partitionby: str):
        """
        Details ...
        :param blob: A DataFrame of counties data
        """

        configurations = config.Config()

        # Directories
        self.warehouse = {directory: os.path.join(configurations.warehouse, directory)
                          for directory in ['baselines', 'capita']}

        # Data set-up
        self.blob = blob.drop(columns=[configurations.datestring, configurations.inhabitants, 'STATEFP'], inplace=False)
        self.excerpt = blob[['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveRate', 'deathRate']]

        # Creating an iterable for multiprocessing
        self.partitionby = partitionby
        self.partitions = [{part} for part in blob[self.partitionby].unique()]

        # Grid lines
        gridlines = hopkins.algorithms.gridlines.GridLines(death_rate_max=blob['deathRate'].max(),
                                                           positive_rate_max=blob['positiveRate'].max())
        self.dpr = gridlines.dpr()

    @staticmethod
    def paths(path: list):
        directories = hopkins.base.directories.Directories()
        directories.create(listof=path)

    def capita(self, partition: str):
        """

        :param partition: STUSPS code of a state
        :return:
        """

        data = self.excerpt.copy()
        data = data[data[self.partitionby] == partition]

        data = pd.concat([data, self.dpr], axis=0, ignore_index=True)

        data.to_csv(path_or_buf=os.path.join(self.warehouse['capita'], partition + '.csv'),
                    index=False, encoding='utf-8', header=True)

    def baselines(self, partition: str):
        """

        :param partition: STUSPS code of a state
        :return:
        """

        data = self.blob.copy()
        data = data[data[self.partitionby] == partition]
        data.to_csv(path_or_buf=os.path.join(self.warehouse['baselines'], partition + '.csv'),
                    index=False, encoding='utf-8', header=True)

    def exc(self):
        """

        :return:
        """

        self.paths(path=list(self.warehouse.values()))
        partitions = self.partitions

        pool = mp.Pool(mp.cpu_count())
        pool.starmap(self.baselines, [i for i in partitions])
        pool.close()

        pool = mp.Pool(mp.cpu_count())
        pool.starmap(self.capita, [i for i in partitions])
        pool.close()
