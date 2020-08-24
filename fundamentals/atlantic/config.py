import os
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd

import atlantic.base.directories


class Config:

    def __init__(self):
        # Records began ...
        self.starting: str = '2020-01-22'
        self.epochdays: int = int(datetime.strptime(self.starting, '%Y-%m-%d').timestamp() / (60 * 60 * 24))

        # End point
        limit: datetime = datetime.today() - timedelta(days=1)
        self.ending: str = limit.strftime('%Y-%m-%d')

        # Source: The name of the C.T.P. date field, and the pattern of the dates
        self.datestring = 'date'
        self.datepattern = '%Y%m%d'

        # Data
        self.daily = 'https://covidtracking.com/api/v1/states/daily.csv'

        # The metadata of 'dailystates'
        self.metadata = 'https://raw.githubusercontent.com/premodelling/dictionaries/' \
                        'develop/sars/covidTrackingProjectStates.json'

        # Metadata of the ...
        self.metadatafilter = pd.DataFrame({'name': [self.datestring, 'state', 'positiveIncrease',
                                                     'totalTestResultsIncrease', 'deathIncrease']})

        # Names & Measure
        self.names = {'state': 'STUSPS', 'totalTestResultsIncrease': 'testIncrease'}
        self.measures = ['positiveIncrease', 'testIncrease', 'deathIncrease']

        # Results directory
        self.warehouse = os.path.join(os.getcwd(), 'warehouse')

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
