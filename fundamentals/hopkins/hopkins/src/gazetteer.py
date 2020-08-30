import logging
import os

import numpy as np
import pandas as pd

import config


class Gazetteer:

    def __init__(self, counties: pd.DataFrame, states: pd.DataFrame, population: pd.DataFrame, inhabitants: str):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        configurations = config.Config()
        self.warehouse = configurations.warehouse
        self.urn, self.urc = configurations.regions()

        self.counties = counties
        self.states = states
        self.population = population
        self.inhabitants = inhabitants

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

        values = self.counties[['STATEFP', 'STUSPS', 'STATE', 'COUNTYFP', 'COUNTYGEOID', 'COUNTY', 'ALAND']]

        gazetteer = values.merge(region, how='left', on='STATEFP')
        gazetteer = gazetteer.merge(self.population[['COUNTYGEOID', self.inhabitants]], how='right', on='COUNTYGEOID')
        gazetteer.to_csv(path_or_buf=os.path.join(self.warehouse, 'county', 'gazetteer.csv'),
                         header=True, index=False, encoding='utf-8')

        self.logger.info('\n{}\n'.format(gazetteer))

        return gazetteer

    def state(self, region: pd.DataFrame) -> pd.DataFrame:

        values = self.states[['STATEFP', 'STUSPS', 'STATE', 'ALAND']]

        population: pd.DataFrame = self.population[['STATEFP', self.inhabitants]].groupby(by='STATEFP').sum()
        population.reset_index(drop=False, inplace=True)

        gazetteer = values.merge(region, how='left', on='STATEFP')
        gazetteer = gazetteer.merge(population, how='right', on='STATEFP')
        gazetteer.to_csv(path_or_buf=os.path.join(self.warehouse, 'state', 'gazetteer.csv'),
                         header=True, index=False, encoding='utf-8')

        self.logger.info('\n{}\n'.format(gazetteer))

        return gazetteer

    def exc(self):
        """
        Option: region.drop(labels=['REGIONFP', 'DIVISIONFP'], inplace=True)
        :return:
        """

        region = self.codes().merge(self.names(), how='left', on=['REGIONFP', 'DIVISIONFP'])
        self.logger.info('\n{}\n'.format(region))

        return self.county(region=region), self.state(region=region)
