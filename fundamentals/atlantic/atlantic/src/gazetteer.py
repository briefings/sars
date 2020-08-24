import os

import numpy as np
import pandas as pd

import config


class Gazetteer:

    def __init__(self, rates: pd.DataFrame):

        self.rates = rates

        self.warehouse = config.Config().warehouse

        self.url_names = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
                         'countries/us/geography/regions/names.csv'
        self.url_codes = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
                         'countries/us/geography/regions/fips.csv'

    def names(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.url_names,
                                 header=0, encoding='utf-8', usecols=['REGION', 'DIVISION', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'REGION': str, 'DIVISION': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def codes(self) -> pd.DataFrame:

        try:
            values = pd.read_csv(filepath_or_buffer=self.url_codes,
                                 header=0, encoding='utf-8', usecols=['STATEFP', 'REGIONFP', 'DIVISIONFP'],
                                 dtype={'STATEFP': str, 'REGIONFP': np.int, 'DIVISIONFP': np.int})
        except OSError as err:
            raise err

        return values

    def exc(self, states: pd.DataFrame):

        enhancements = self.codes().merge(self.names(), how='left', on=['REGIONFP', 'DIVISIONFP'])
        gazetteer = states.merge(self.rates[['STUSPS', 'positiveTestRate', 'datetimeobject']], how='left', on='STUSPS')
        gazetteer = gazetteer.merge(enhancements[['STATEFP', 'REGION', 'DIVISION']], how='left', on='STATEFP')

        gazetteer[['STATEFP', 'GEOID', 'STUSPS', 'NAME', 'POPESTIMATE2019', 'positiveTestRate',
                   'datetimeobject', 'REGION', 'DIVISION']].to_csv(
            path_or_buf=os.path.join(self.warehouse, 'gazetteer.csv'), header=True, index=False, encoding='utf-8')
