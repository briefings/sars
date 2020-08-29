import pandas as pd

import populations.us.reference.settings


class State:

    def __init__(self):
        self.name = ''

    @staticmethod
    def urlstring(segment: str, year: str) -> str:
        """

        :param segment: Pattern -> 'YYYY-YYYY', e.g., '2010-2019'.
        :param year: Pattern -> 'YYYY', e.g., '2019'.
        :return:
        """
        settings = populations.us.reference.settings.Settings()
        return settings.apistate.format(segment=segment, year=year)

    @staticmethod
    def read(urlstring: str, year: str):
        """
            pd.read_csv(filepath_or_buffer=urlstring, usecols=['STATE', 'NAME', 'POPESTIMATE2019'],
                        dtype={'STATE': 'str', 'NAME': 'str', 'POPESTIMATE{}'.format(year): 'int'},
                        names=['STATEFP', 'STATENAME', 'POPESTIMATE{}'.format(year)])
        """

        data = pd.read_csv(urlstring, header=0, encoding='utf-8')
        data = data[['STATE', 'NAME', 'POPESTIMATE{}'.format(year)]].copy()

        data.rename(columns={'STATE': 'STATEFP'}, inplace=True)
        data.loc[:, 'STATEFP'] = data['STATEFP'].astype(str).str.zfill(2)

        # Exclude the U.S.A. aggregated population value
        data = data[~data.STATEFP.isin(['00'])]

        return data

    def exc(self, segment: str, year: str):
        """

        :param segment: The required data segment of https://www2.census.gov/programs-surveys/popest/datasets/
        :param year: The segment's year
        :return:
        """

        return self.read(urlstring=self.urlstring(segment=segment, year=year),
                         year=year)
