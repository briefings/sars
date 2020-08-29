"""
Module readings

In relation to the U.S., at county level, J.H. updates two CSV files per day:
    * time_series_covid19_confirmed_US.csv
    * time_series_covid19_deaths_US.csv

This module reads-in these files, makes some corrections, re-structures, and
combines; it returns a single data frame.
"""

import pandas as pd

import logging

import hopkins.algorithms.inspect

import config


class Readings:

    def __init__(self, features: pd.DataFrame, reference: pd.DataFrame, days: pd.DataFrame):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        configurations = config.Config()
        self.datestring = configurations.datestring
        self.categories = configurations.categories
        self.measures = configurations.measures
        self.url = configurations.url

        self.inspect = hopkins.algorithms.inspect.Inspect()

        self.features = features
        self.reference = reference
        self.days = days

        self.datefields = self.days[self.datestring].values

    def attributes(self):

        fields = self.features.field.values
        types = self.features.set_index(keys='field',
                                        drop=False,
                                        inplace=False).to_dict(orient='dict')['type']
        return fields, types

    def read(self, category: str):

        fields, types = self.attributes()

        try:
            data = pd.read_csv(self.url.format(category=category), usecols=fields, dtype=types)
        except OSError as err:
            raise err

        # U.S.A. County Federal Information Processing Standards: Exclude non-county records
        data = self.inspect.fips(data=data)
        data.rename(columns={'FIPS': 'COUNTYGEOID'}, inplace=True)

        # Address NaN.  Noting that days[key].values are the data fields w.r.t. COVID-19 records
        data.loc[:, self.datefields] = data[self.datefields].fillna(value=0, inplace=False)

        return data

    def exc(self):

        data = self.reference.copy()

        for category in self.categories:

            # Read, melt, update
            readings = self.read(category=category)
            readings = readings.melt(id_vars='COUNTYGEOID', value_vars=self.datefields,
                                     var_name=self.datestring, value_name=category)
            data = data.merge(readings, how='left', on=[self.datestring, 'COUNTYGEOID'])

        data.rename(columns=self.measures, inplace=True)
        data.sort_values(by=['COUNTYGEOID', 'datetimeobject'], ascending=True, inplace=True, ignore_index=True)

        self.logger.info('\n{}\n'.format(data.tail()))

        return data
