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
import hopkins.algorithms.gridlines


class Partitions:

    def __init__(self, blob: pd.DataFrame):
        """
        Details ...
        :param blob: A DataFrame of counties data
        """

        configurations = config.Config()
        self.warehouse = configurations.warehouse
        self.path = {directory: os.path.join(self.warehouse, directory) for directory in ['baselines', 'capita']}

        self.blob = blob.drop(columns=[configurations.datestring, configurations.inhabitants, 'STATEFP'], inplace=False)
        self.forcapita = blob[['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveRate', 'deathRate']]

        self.partitionby = 'STUSPS'
        self.partitions = [{part} for part in blob[self.partitionby].unique()]

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

        data = self.forcapita.copy()
        data = data[data[self.partitionby] == partition]

        data = pd.concat([data, self.dpr], axis=0, ignore_index=True)

        data.to_csv(path_or_buf=os.path.join(self.path['capita'], partition + '.csv'),
                    index=False, encoding='utf-8', header=True)

    def baselines(self, partition: str):
        """

        :param partition: STUSPS code of a state
        :return:
        """

        data = self.blob.copy()
        data = data[data[self.partitionby] == partition]
        data.to_csv(path_or_buf=os.path.join(self.path['baselines'], partition + '.csv'),
                    index=False, encoding='utf-8', header=True)

    def exc(self):
        """

        :return:
        """

        self.paths(path=list(self.path.values()))
        partitions = self.partitions

        pool = mp.Pool(mp.cpu_count())
        pool.starmap(self.baselines, [i for i in partitions])
        pool.close()

        pool = mp.Pool(mp.cpu_count())
        pool.starmap(self.capita, [i for i in partitions])
        pool.close()
