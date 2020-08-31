import os

import pandas as pd

import config
import hopkins.base.directories


class Segments:

    def __init__(self, blob: pd.DataFrame, category: str):
        """
        Details ...

        :param blob: The DataFrame of counties or states data
        :param category: county or state
        """

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        self.blob = blob
        self.category = category

        self.path = os.path.join(self.warehouse, self.category)
        self.paths(path=self.path)

    @staticmethod
    def paths(path):
        """
        Ensures that a directory exists.
        :param path: Directory string
        :return:
        """
        directories = hopkins.base.directories.Directories()
        directories.create(listof=[path])

    @staticmethod
    def write(data: pd.DataFrame, filestring: str):
        """

        :param data: The DataFrame that will be written to ...
        :param filestring: The path + file name + extension
        :return:
        """
        data.to_csv(path_or_buf=filestring, index=False, encoding='utf-8', header=True)
        return True

    def exc(self, select: list, segment: str):
        """

        :param select: The fields of interest
        :param segment: baseline, candles, capita, increases, etc ... this will be the file name
        :return:
        """

        data = self.blob.copy()
        data = data[select]

        self.write(data=data, filestring=os.path.join(self.path, segment + '.csv'))
