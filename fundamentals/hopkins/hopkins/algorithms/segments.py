import os

import pandas as pd

import config


class Segments:

    def __init__(self, blob: pd.DataFrame):
        """
        Details ...

        :param blob: The DataFrame of counties data
        :param category: county or state
        """

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        self.blob = blob

    @staticmethod
    def write(data: pd.DataFrame, filestring: str):
        """

        :param data: The DataFrame that will be written to ...
        :param filestring: The path + file name + extension
        :return:
        """

        data.to_csv(path_or_buf=filestring, index=False, encoding='utf-8', header=True)

    def exc(self, select: list, segment: str):
        """

        :param select: The fields of interest
        :param segment: baseline, candles, capita, increases, etc ... this will be the file name
        :return:
        """

        data = self.blob.copy()
        data = data[select]

        self.write(data=data, filestring=os.path.join(self.warehouse, segment + '.csv'))
