import os

import numpy as np
import pandas as pd

import config


class Gazetteer:

    def __init__(self):

        configurations = config.Config()
        self.warehouse = configurations.warehouse
        self.urn, self.urc = configurations.regions()

    def names(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.urn,
                                 header=0, encoding='utf-8', usecols=['REGION', 'DIVISION', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'REGION': str, 'DIVISION': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def codes(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.urc,
                                 header=0, encoding='utf-8', usecols=['STATEFP', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'STATEFP': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def exc(self, states: pd.DataFrame):

        regions = self.codes().merge(self.names(), how='left', on=['REGIONFP', 'DIVISIONFP'])
        gazetteer = states.merge(regions[['STATEFP', 'REGION', 'DIVISION']], how='left', on='STATEFP')

        gazetteer[['STATEFP', 'STATEGEOID', 'STUSPS', 'STATE', 'POPESTIMATE2019',
                   'ALAND', 'REGION', 'DIVISION']].to_csv(
            path_or_buf=os.path.join(self.warehouse, 'gazetteer.csv'), header=True, index=False, encoding='utf-8')
