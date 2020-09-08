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
        self.blob.loc[:, 'datetimeobject'] = self.blob['datetimeobject'].astype(str)

        self.warehouse = config.Config().warehouse

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def baselines(self):
        data = self.blob.copy()
        data = data.drop(columns='POPESTIMATE2019', inplace=False)

        self.logger.info('\nBaselines:\n{}\n'.format(data.info()))
        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'baselines.csv'),
                    header=True, index=False, encoding='utf-8')

    def capita(self):

        data = self.blob.copy()
        data = data[['datetimeobject', 'STUSPS', 'deathRate', 'positiveRate', 'testRate',
                     'icuRate', 'hospitalizedRate', 'ndays']]

        gridlines = atlantic.algorithms.gridlines.GridLines(positive_rate_max=data['positiveRate'].max(),
                                                            test_rate_max=data['testRate'].max()).exc()

        data = pd.concat([data, gridlines], axis=0, ignore_index=True)

        self.logger.info('\nCapita:\n{}\n'.format(data.info()))
        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'capita.csv'),
                    header=True, index=False, encoding='utf-8')
        data.to_json(path_or_buf=os.path.join(self.warehouse, 'capita.json'), orient='values')

    def exc(self):

        # The baseline
        self.baselines()

        # In progress
        self.capita()
