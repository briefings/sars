import datetime
import logging
import os

import numpy as np
import pandas as pd

import atlantic.gridlines.hpg
import config


class Hospitalizations:

    def __init__(self, blob: pd.DataFrame):
        self.blob = blob

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        beginning = configurations.epochdays
        until = int(datetime.datetime.strptime(configurations.ending, '%Y-%m-%d').timestamp() / (60 * 60 * 24))
        self.epoch = until - beginning

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def places(self):
        tensor = self.blob.copy()
        tensor.loc[:, 'marker'] = np.where(tensor['hospitalizedIncrease'] > 0, 1, 0)

        usable = tensor[['STUSPS', 'marker']].groupby(by='STUSPS').sum()
        usable = usable[usable['marker'] > 0.05 * self.epoch]
        usable.reset_index(drop=False, inplace=True)

        self.logger.info('\nNumber of places:\n{}\n'.format(usable.shape[0]))

        return usable

    def set_deaths_hospitalized(self, data: pd.DataFrame):
        data[['datetimeobject', 'STUSPS', 'hospitalizedRate', 'deathRate', 'ndays'
              ]].to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesDeathsHospitalized.csv'),
                        header=True, index=False, encoding='utf-8')

    def set_hospitalized_positives(self, data: pd.DataFrame):
        frame = data[['datetimeobject', 'STUSPS', 'hospitalizedRate', 'positiveRate', 'ndays']]
        gridlines = atlantic.gridlines.hpg.HPG(hospitalized_rate_max=frame['hospitalizedRate'].max(),
                                               positive_rate_max=frame['positiveRate'].max()).exc()

        instances = pd.concat([frame, gridlines], axis=0, ignore_index=True)
        instances.to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesHospitalizedPositives.csv'),
                         header=True, index=False, encoding='utf-8')

    def set_hospitalized(self, data: pd.DataFrame):
        frame = data.copy()
        frame.to_csv(path_or_buf=os.path.join(self.warehouse, 'hospitalizations.csv'),
                     header=True, index=False, encoding='utf-8')

        self.logger.info('\nHospitalizations:\n')
        self.logger.info(frame.info())

    def exc(self):
        data = self.blob.copy()
        usable = self.places()
        data = data.merge(usable[['STUSPS']], how='right', on=['STUSPS'])

        self.set_deaths_hospitalized(data=data)
        self.set_hospitalized_positives(data=data)
        self.set_hospitalized(data=data)
