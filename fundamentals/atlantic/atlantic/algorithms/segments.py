import logging
import os

import pandas as pd

import atlantic.algorithms.gridlines
import atlantic.base.directories
import atlantic.src.gazetteer
import config


class Segments:

    def __init__(self, blob: pd.DataFrame):
        self.blob = blob

        self.warehouse = config.Config().warehouse

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def candles(self):
        data = self.blob.copy()
        data = data.drop(columns=['POPESTIMATE2019'])

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'candles.csv'),
                    header=True, index=False, encoding='utf-8')

    def increases(self):
        data = self.blob[['datetimeobject', 'STUSPS', 'positiveIncrease', 'testIncrease', 'deathIncrease']].copy()

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'increases.csv'),
                    header=True, index=False, encoding='utf-8')

    def baseline(self):
        data = self.blob.copy()
        data = data.drop(columns='POPESTIMATE2019', inplace=False)

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'baseline.csv'),
                    header=True, index=False, encoding='utf-8')

    def special(self):

        data = self.blob.copy()
        data = data.drop(columns='POPESTIMATE2019', inplace=False)

        gridlines = atlantic.algorithms.gridlines.GridLines(positive_rate_max=data['positiveRate'].max(),
                                                            test_rate_max=data['testRate'].max()).exc()

        data = pd.concat([data, gridlines], axis=0, ignore_index=True)
        self.logger.info(data.tail())

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'special.csv'),
                    header=True, index=False, encoding='utf-8')

    def exc(self):
        self.candles()
        self.increases()
        self.baseline()
        self.special()
