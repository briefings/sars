import logging
import os

import pandas as pd

import config


class Accumulations:

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

        select = ['datetimeobject', 'STUSPS', 'deathCumulative', 'positiveCumulative', 'testCumulative',
                  'hospitalizedCumulative', 'icuCumulative', 'deathRate', 'positiveRate', 'testRate',
                  'hospitalizedRate', 'icuRate', 'ndays']
        data = self.blob[select].copy()

        values = data[data.datetimeobject == data.datetimeobject.max()]
        values.reset_index(drop=True, inplace=True)

        return values

    @staticmethod
    def ptr(latest) -> pd.DataFrame:
        """
        Calculates each record's positive test rate

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
        Creates a file of the latest cumulative values

        :return:
        """

        # The latest values by latest date
        latest = self.latest()

        # Append the latest cumulative positive test rate per record, i.e., state
        acc = self.ptr(latest=latest)

        # Preview & Write
        self.logger.info('\n{}\n'.format(acc.info()))
        acc.to_csv(path_or_buf=os.path.join(self.warehouse, 'accumulations.csv'),
                         header=True, index=False, encoding='utf-8')
