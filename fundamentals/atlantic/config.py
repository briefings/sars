import os
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd

import atlantic.base.directories


class Config:

    def __init__(self):

        # Starting, ending, days thus far.  The name of the C.T.P. date field, and the pattern of the dates
        # https://covidtracking.com/data/api
        self.starting: str = '2020-01-22'
        self.epochdays: int = int(datetime.strptime(self.starting, '%Y-%m-%d').timestamp() / (60 * 60 * 24))
        self.ending: str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.datestring = 'date'
        self.datepattern = '%Y%m%d'

        # The data, the metadata, selection of metadata
        self.daily = 'https://covidtracking.com/api/v1/states/daily.csv'
        self.metadata = 'https://raw.githubusercontent.com/premodelling/dictionaries/' \
                        'develop/sars/covidTrackingProjectStates.json'
        self.metadatafilter = pd.DataFrame(
            {'name': [self.datestring, 'state', 'death', 'positive', 'negative', 'inIcuCumulative',
                      'hospitalizedCumulative', 'inIcuCurrently', 'hospitalizedCurrently']})

        # Results directory
        self.warehouse = os.path.join(os.getcwd(), 'warehouse')

    @staticmethod
    def variables():

        # For re-naming
        names = {'state': 'STUSPS', 'death': 'deathCumulative', 'positive': 'positiveCumulative',
                 'negative': 'negativeCumulative', 'inIcuCumulative': 'icuCumulative',
                 'inIcuCurrently': 'icuCurrently'}

        # For
        #   src.readings: Therein NaN values are replaced with zeros
        #   algorithms.anomalies: Cumulative values discrepancies are addressed therein
        measures = ['deathCumulative', 'positiveCumulative', 'negativeCumulative', 'icuCumulative',
                    'hospitalizedCumulative']

        # Marking cumulative measures
        cumulative = ['deathCumulative', 'positiveCumulative', 'testCumulative',
                      'icuCumulative', 'hospitalizedCumulative']

        # Marking discrete measures
        increase = ['deathIncrease', 'positiveIncrease', 'testIncrease',
                    'icuIncrease', 'hospitalizedIncrease']

        # Marking current counts
        currently = ['icuCurrently', 'hospitalizedCurrently']

        return names, measures, cumulative, increase, currently

    @staticmethod
    def regions():

        urn = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
              'countries/us/geography/regions/names.csv'
        urc = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
              'countries/us/geography/regions/fips.csv'

        return urn, urc

    def storage(self):

        directories = atlantic.base.directories.Directories()
        directories.cleanup(listof=[self.warehouse])
        directories.create(listof=[self.warehouse])

    def days(self):
        """
        values.datetimeobject.apply(lambda x: x.value / (10 ** 6)).astype(np.longlong)

        :return: A DataFrame of dates
        """

        values = pd.DataFrame(pd.date_range(start=self.starting, end=self.ending, freq='D'), columns=['datetimeobject'])
        values.loc[:, self.datestring] = values.datetimeobject.apply(lambda x: x.strftime(self.datepattern))
        values.loc[:, 'epochmilli'] = (values['datetimeobject'].astype(np.int64) / (10 ** 6)).astype(np.longlong)

        return values

    def attributes(self):
        """
        name, type, nullable, metadata
        :return:
        """

        try:
            values = pd.read_json(path_or_buf=self.metadata, orient='records', typ='series')
        except OSError as err:
            raise err

        metadata = pd.DataFrame(values.fields)

        # Focus on fields of interest
        meta = self.metadatafilter.merge(metadata, how='inner', on='name') \
            .drop(columns=['nullable', 'metadata'])

        # Set integer types as float types, this ensures that pandas can read nullable integer fields
        meta['type'] = meta['type'].apply(lambda x: 'float' if x == 'int' else x)

        fields: np.ndarray = meta.name.values
        types: dict = meta.set_index(keys='name', drop=True, inplace=False).to_dict(orient='dict')['type']

        return fields, types
