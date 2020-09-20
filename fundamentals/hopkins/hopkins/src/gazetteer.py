import logging
import os

import numpy as np
import pandas as pd

import config


class Gazetteer:

    def __init__(self, counties: pd.DataFrame, population: pd.DataFrame):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        configurations = config.Config()
        self.warehouse = configurations.warehouse
        self.inhabitants = configurations.inhabitants
        self.urn, self.urc = configurations.regions()

        self.counties = counties
        self.population = population

    def names(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.urn, header=0, encoding='utf-8',
                                 usecols=['REGION', 'DIVISION', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'REGION': str, 'DIVISION': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def codes(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.urc, header=0, encoding='utf-8',
                                 usecols=['STATEFP', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'STATEFP': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def county(self, region) -> pd.DataFrame:

        values = self.counties[['STATEFP', 'STUSPS', 'STATE', 'STATESQMETRES',
                                'COUNTYFP', 'COUNTYGEOID', 'COUNTY', 'ALAND']]

        gazetteer = values.merge(region, how='left', on='STATEFP')
        gazetteer = gazetteer.merge(self.population[['COUNTYGEOID', self.inhabitants]], how='right', on='COUNTYGEOID')
        gazetteer.to_csv(path_or_buf=os.path.join(self.warehouse, 'gazetteer.csv'),
                         header=True, index=False, encoding='utf-8')

        self.logger.info('Gazetteer\n{}\n'.format(gazetteer))

        return gazetteer

    def exc(self):
        """

        :return:
        """

        # Preview
        self.logger.info('Counties\n{}\n'.format(self.counties))

        region = self.codes().merge(self.names(), how='left', on=['REGIONFP', 'DIVISIONFP'])
        self.logger.info('\n{}\n'.format(region))

        return self.county(region=region)
