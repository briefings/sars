import os

import pandas as pd
import logging
import config
import hopkins.algorithms.gridlines


class Segments:

    def __init__(self, blob: pd.DataFrame):
        """
        Details ...

        :param blob: The DataFrame of counties data
        """

        configurations = config.Config()
        self.warehouse = configurations.warehouse
        self.blob = blob

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def capita(self):
        """

        :param data: The DataFrame that will be written to ...
        :param filestring: The path + file name + extension
        :return:
        """

        select = ['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveRate', 'deathRate']
        data = self.blob.copy()
        data = data[select]

        gridlines = hopkins.algorithms.gridlines.GridLines(death_rate_max=data['deathRate'].max(),
                                                           positive_rate_max=data['positiveRate'].max()).dpr()
        data = pd.concat([data, gridlines], axis=0, ignore_index=True)

        self.logger.info('Capita\n{}\n'.format(data.tail()))
        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'capita.csv'), index=False, encoding='utf-8', header=True)

    def exc(self):
        """

        :return:
        """

        self.capita()
