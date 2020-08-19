import dask
import numpy as np
import pandas as pd

import populations.us.api.settings


class County:

    def __init__(self, entry: str):
        """

        :param entry: A Census Bureau API Key string
        """

        self.entry = entry

    def urlstrings(self, year: str):
        """

        :return: The set of Census Bureau URL strings for reading populations per county
        """

        # For the skeleton API atring, and the set of U.S. State FIPS Codes
        settings = populations.us.api.settings.Settings()

        return [settings.apicounty.format(year=year, state=statefp, entry=self.entry)
                for statefp in settings.getstatefp()]

    @staticmethod
    def read(urlstring):
        """
            dask.dataframe: problematic due to in-file peculiarities

            {0: 'TOT_POP', 1: 'CTYNAME', 2: 'STATE', 3: 'COUNTY'}) =>
                {0: 'POPESTIMATE{YYYY}', 1: 'COUNTYNAME', 2: 'STATEFP', 3: 'COUNTYFP'}
        """

        try:
            raw = pd.read_json(path_or_buf=urlstring, dtype={0: 'str', 1: 'str', 2: 'str', 3: 'str'})
        except OSError as err:
            raise err

        streams = raw.loc[1:, :]

        return streams

    @staticmethod
    def estimates(readings):
        """

        :param readings: The dask.compute output ...
        :return:
        """

        values = pd.concat(readings, axis=0, ignore_index=True)
        values.rename(columns={0: 'POPESTIMATE', 1: 'COUNTYNAME', 2: 'STATEFP', 3: 'COUNTYFP'}, inplace=True)
        values.loc[:, 'POPESTIMATE'] = values['POPESTIMATE'].astype(np.int64)

        return values

    def exc(self, year: str):
        """
        Entry point
        :return:
        """

        urlstrings = self.urlstrings(year=year)
        computations = [dask.delayed(self.read)(urlstring) for urlstring in urlstrings]
        dask.visualize(computations, filename='computations', format='pdf')

        readings = dask.compute(computations, scheduler='processes')[0]

        return self.estimates(readings=readings)
