import logging
import os

import numpy as np
import pandas as pd

import config


class Features:

    def __init__(self, datestrings: np.ndarray, basefields: list):
        """

        :param datestrings: The latest array of dates that will constitute the date fields.
        :param basefields: The non-date fields of interest.  In addition to the date fields, the J.H. data
                           includes a variety of geographic, etc., fields.
        """

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        configurations = config.Config()
        self.urlfields = configurations.urlfields
        self.warehouse = configurations.warehouse

        self.datestrings = datestrings
        self.basefields = basefields

    def fields(self) -> pd.DataFrame:
        """

        :return: The required non-date fields
        """

        try:
            data = pd.read_json(path_or_buf=self.urlfields, orient='records', typ='frame',
                                dtype={'field': 'str', 'type': 'str'})
        except OSError as err:
            raise err

        return data[data.field.isin(self.basefields)]

    def dates(self):
        """

        :return: The latest date fields, and their types
        """

        return pd.DataFrame({'field': self.datestrings,
                             'type': np.repeat(['int'], repeats=self.datestrings.shape[0], axis=0)})

    def exc(self) -> pd.DataFrame:

        # Create the data dictionary
        values = pd.concat((self.fields(), self.dates()), ignore_index=True, axis=0)

        # Save a copy
        values.to_json(path_or_buf=os.path.join(self.warehouse, 'johnHopkinsCounties.json'), orient='records')

        self.logger.info('\n{}\n'.format(values.tail()))

        return values
