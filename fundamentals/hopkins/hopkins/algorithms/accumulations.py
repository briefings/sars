import logging
import os

import numpy as np
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

        select = ['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'deathCumulative', 'positiveCumulative',
                  'deathRate', 'positiveRate', 'ndays']
        data = self.blob[select].copy()

        values = data[data.datetimeobject == data.datetimeobject.max()]
        values.reset_index(drop=True, inplace=True)

        return values

    @staticmethod
    def dpr(latest) -> pd.DataFrame:
        """
        Calculates each record's death positive rate

        :param latest: Per county, the latest date's data
        :return:
        """

        data = latest.copy()
        values = pd.DataFrame(
            data={'STUSPS': data['STUSPS'].values,
                  'COUNTYGEOID': data['COUNTYGEOID'].values,
                  'deathPositiveRate': np.where(latest.positiveRate > 0, 100 * latest.deathRate / latest.positiveRate,
                                                0)}
        )
        data = data.merge(values, how='left', on=['STUSPS', 'COUNTYGEOID'])

        return data

    def exc(self):
        """
        Creates a file of the latest cumulative values

        :return:
        """

        # The latest values by latest date
        latest = self.latest()

        # Append the latest cumulative death positive rate per record, i.e., county
        acc = self.dpr(latest=latest)

        # Preview & Write
        self.logger.info('\n{}\n'.format(acc.info()))
        acc.to_csv(path_or_buf=os.path.join(self.warehouse, 'accumulations.csv'),
                   header=True, index=False, encoding='utf-8')
