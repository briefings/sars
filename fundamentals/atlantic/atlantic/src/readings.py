import logging
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
        self.daily = configurations.daily
        self.datestring = configurations.datestring
        self.datepattern = configurations.datepattern
        self.names, self.measures, _, _ = configurations.variables()
        self.fields, self.types = configurations.attributes()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def features(self, blob):
        """

        :param blob:
        :return: Enhanced C.T.P. date
        """

        data = blob.copy()
        data.loc[:, self.measures] = data[self.measures].fillna(value=0)

        return data.merge(self.states[['STUSPS', 'POPESTIMATE2019']], how='left', on='STUSPS')

    def structure(self, blob) -> pd.DataFrame:
        """

        :param blob: The latest raw C.T.P. data
        :return: Restructured C.T.P. data
        """

        data = blob.copy()

        # Create a field of date objects via the raw datefield
        data.loc[:, 'datetimeobject'] = data[self.datestring].apply(lambda x: datetime.strptime(x, self.datepattern))

        # Drop the raw date field
        data.drop(columns=[self.datestring], inplace=True)

        # Ascertain that every combination of place & date exists
        return self.references[['datetimeobject', 'STUSPS']]. \
            merge(data, how='left', on=['datetimeobject', 'STUSPS'])

    def retrieve(self) -> pd.DataFrame:
        """

        :return: The DataFrame of the latest C.T.P. data
        """

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

        self.logger.info('\nReadings:\n{}\n'.format(data.info()))


        return data
