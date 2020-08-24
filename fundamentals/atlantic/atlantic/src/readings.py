from datetime import datetime

import pandas as pd

import config


class Readings:

    def __init__(self, references: pd.DataFrame, states: pd.DataFrame):

        # Frame of reference: expected STUSPS & date combinations
        self.references = references

        # Enhanced state data
        self.states = states

        # Configurations
        configurations = config.Config()

        # ... Values
        self.daily = configurations.daily
        self.datestring = configurations.datestring
        self.datepattern = configurations.datepattern
        self.names = configurations.names
        self.measures = configurations.measures

        # ... Functions
        self.fields, self.types = configurations.attributes()

    def features(self, blob):

        data = blob.copy()

        data.loc[:, self.measures] = data[self.measures].fillna(value=0)

        return data.merge(self.states[['STUSPS', 'POPESTIMATE2019']], how='left', on='STUSPS')

    def structure(self, blob):

        data = blob.copy()
        data.loc[:, 'datetimeobject'] = data[self.datestring].apply(lambda x: datetime.strptime(x, self.datepattern))
        data.drop(columns=[self.datestring], inplace=True)

        return self.references[['datetimeobject', 'STUSPS']]. \
            merge(data, how='left', on=['datetimeobject', 'STUSPS'])

    def retrieve(self):

        try:
            values = pd.read_csv(filepath_or_buffer=self.daily, header=0, usecols=self.fields,
                                 dtype=self.types, encoding='utf-8')
        except OSError as err:
            raise err

        values.rename(columns=self.names, inplace=True)

        return values

    def exc(self):

        data = self.retrieve()
        data = self.structure(blob=data)
        data = self.features(blob=data)

        return data
