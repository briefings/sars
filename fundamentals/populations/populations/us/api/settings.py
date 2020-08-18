import pandas as pd
import numpy as np


class Settings:

    def __init__(self):

        # The STATE FIP of each state/territory for which the API does not have population data
        self.inapplicable = np.array(['69', '66', '78', '60'])

        # County API URL
        self.apicounty = 'https://api.census.gov/data/{year}/pep/' \
                         'population?get=POP,NAME&for=county:*&in=state:{state}&key={entry}'

    def getstatefp(self):

        statefpframe = pd.read_csv(filepath_or_buffer='https://raw.githubusercontent.com/discourses/hub/develop/'
                                                 'data/countries/us/geography/states/states.csv',
                              header=0, encoding='utf-8', usecols=['STATEFP'], dtype={'STATEFP': 'str'})

        statefp = np.setdiff1d(statefpframe.STATEFP.values, self.inapplicable)

        return statefp
