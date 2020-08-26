import numpy as np
import pandas as pd

import populations.us.reference.settings


class County:

    def __init__(self):

        self.name = ''

    @staticmethod
    def urlstring():

        settings = populations.us.reference.settings.Settings()
        return settings.apicounty

    @staticmethod
    def read(urlstring: str, year: str):
        """

        :param urlstring: URL String
        :param year: Estimate of year ...
        :return:
        """

        try:
            data = pd.read_csv(filepath_or_buffer=urlstring, header=0, encoding='utf-8',
                               usecols=['STATEFP', 'COUNTYFP', 'POPESTIMATE{year}'.format(year=year)],
                               dtype={'STATEFP': str, 'COUNTYFP': str,
                                      'POPESTIMATE{year}'.format(year=year): np.int64})
        except OSError as err:
            raise err

        data.loc[:, 'COUNTYGEOID'] = data.STATEFP + data.COUNTYFP

        return data

    def exc(self, year: str):
        """

        :param year: Estimate of year ...
        :return:
        """

        urlstring: str = self.urlstring()
        return self.read(urlstring=urlstring, year=year)
