import logging
import os

import pandas as pd

import config


class Rates:

    def __init__(self, blob):
        """

        :param blob:
        """

        self.blob = blob

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def latest(self):
        """

        :return:
        """
        data = self.blob[['datetimeobject', 'STUSPS', 'positiveRate', 'testRate']].copy()

        values = data[data.datetimeobject == data.datetimeobject.max()]
        values.reset_index(drop=True, inplace=True)

        return values

    @staticmethod
    def estimates(latest) -> pd.DataFrame:
        """

        :param latest: Per state, the latest date's data
        :return:
        """

        data = latest.copy()
        values = pd.DataFrame(data={'STUSPS': data['STUSPS'].values,
                                    'positiveTestRate': (100 * data['positiveRate'] / data['testRate']).values
                                    })
        data = data.merge(values, how='left', on='STUSPS')

        return data

    def exc(self):
        """
        Creates a file of the latest ...

        :return:
        """

        latest = self.latest()
        estimates = self.estimates(latest=latest)
        self.logger.info('\n{}\n'.format(estimates.info()))
        estimates.to_csv(path_or_buf=os.path.join(self.warehouse, 'rates.csv'),
                         header=True, index=False, encoding='utf-8')
