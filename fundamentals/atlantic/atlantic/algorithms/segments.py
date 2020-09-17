import logging
import os

import pandas as pd

import atlantic.gridlines.ptg
import atlantic.gridlines.dtg
import atlantic.gridlines.dpg
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

    def dpc(self):
        """
        Appends the grid lines for the Deaths/100K v Positives/100K curves
        :return:
        """

        data = self.blob.copy()
        data = data[['datetimeobject', 'STUSPS', 'deathRate', 'positiveRate', 'ndays']]
        gridlines = atlantic.gridlines.dpg.DPG(death_rate_max=data['deathRate'].max(),
                                               positive_rate_max=data['positiveRate'].max()).exc()
        data = pd.concat([data, gridlines], axis=0, ignore_index=True)
        self.logger.info('\nDeaths Positives Curves:\n{}\n'.format(data.info()))

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesDeathsPositives.csv'),
                    header=True, index=False, encoding='utf-8')

    def dtc(self):
        """
        Appends the grid lines for the Deaths/100K v Tests/100K curves
        :return:
        """

        data = self.blob.copy()
        data = data[['datetimeobject', 'STUSPS', 'deathRate', 'testRate', 'ndays']]
        gridlines = atlantic.gridlines.dtg.DTG(death_rate_max=data['deathRate'].max(),
                                               test_rate_max=data['testRate'].max()).exc()
        data = pd.concat([data, gridlines], axis=0, ignore_index=True)
        self.logger.info('\nDeaths Tests Curves:\n{}\n'.format(data.info()))

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesDeathsTests.csv'),
                    header=True, index=False, encoding='utf-8')

    def ptc(self):
        """
        The Positives/100K v Tests/100K curves
        :return:
        """

        data = self.blob.copy()
        data = data[['datetimeobject', 'STUSPS', 'positiveRate', 'testRate', 'ndays']]
        gridlines = atlantic.gridlines.ptg.PTG(positive_rate_max=data['positiveRate'].max(),
                                               test_rate_max=data['testRate'].max()).exc()
        data = pd.concat([data, gridlines], axis=0, ignore_index=True)
        self.logger.info('\nPositives Tests Curves:\n{}\n'.format(data.info()))

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesPositivesTests.csv'),
                    header=True, index=False, encoding='utf-8')

    def capita(self):

        data = self.blob.copy()
        data = data[['datetimeobject', 'STUSPS', 'deathRate', 'positiveRate', 'testRate',
                     'icuRate', 'hospitalizedRate', 'ndays']]

        data.to_csv(path_or_buf=os.path.join(self.warehouse, 'capita.csv'),
                    header=True, index=False, encoding='utf-8')
        data.to_json(path_or_buf=os.path.join(self.warehouse, 'capita.json'), orient='values')

    def exc(self):

        # The baseline
        self.baselines()

        # Capita
        self.capita()

        # Death/100K v Test/100K Curves
        self.dpc()

        # Positive/100K v Test/100K Curves
        self.ptc()

        # Death/100K v Test/100K Curves
        self.dtc()
