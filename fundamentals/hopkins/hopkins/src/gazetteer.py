import os

import numpy as np
import pandas as pd

import config
import hopkins.base.directories


class Gazetteer:

    def __init__(self):

        self.warehouse = config.Config().warehouse

        self.url_names = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
                         'countries/us/geography/regions/names.csv'
        self.url_codes = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
                         'countries/us/geography/regions/fips.csv'

    def paths(self):

        directories = hopkins.base.directories.Directories()

        j = [os.path.join(self.warehouse, i) for i in ['county', 'state']]
        directories.create(j)

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

    def exc(self, counties: pd.DataFrame, population: pd.DataFrame, inhabitants: str):

        self.paths()

        enhancements = self.codes().merge(self.names(), how='left', on=['REGIONFP', 'DIVISIONFP'])
        gazetteer = counties.merge(enhancements[['STATEFP', 'REGION', 'DIVISION']], how='left', on='STATEFP')

        county = gazetteer.merge(population, how='right', on='COUNTYGEOID')
        county.to_csv(path_or_buf=os.path.join(self.warehouse, 'county', 'gazetteer.csv'),
                      header=True, index=False, encoding='utf-8')

        state = county[['STATEFP', 'STUSPS', 'REGION', 'DIVISION', inhabitants
                           ]].groupby(by=['STATEFP', 'STUSPS', 'REGION', 'DIVISION']).sum()
        state.reset_index(drop=False, inplace=True)
        state.to_csv(path_or_buf=os.path.join(self.warehouse, 'state', 'gazetteer.csv'),
                     header=True, index=False, encoding='utf-8')

        return county, state
