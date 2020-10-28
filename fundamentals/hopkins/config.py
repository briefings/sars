import os
import platform
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd

import hopkins.base.directories


class Config:

    def __init__(self):

        # Starting, ending, days thus far.  The name of the J.H. date field, and the pattern of the dates
        self.starting: str = '2020-01-22'
        self.epochdays: int = int(datetime.strptime(self.starting, '%Y-%m-%d').timestamp() / (60 * 60 * 24))
        self.ending: str = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
        self.datestring = 'date'

        if platform.system() == 'Windows':
            self.datepattern = '%#m/%#d/%y'
        else:
            self.datepattern = '%-m/%-d/%y'

        # The starting date field
        self.startingfield: str = datetime.strptime(self.starting, '%Y-%m-%d').strftime(self.datepattern)

        # Data
        self.urlfields = 'https://raw.githubusercontent.com/premodelling/dictionaries/develop/' \
                         'sars/johnHopkinsCountiesReference.json'
        self.url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
                   'csse_covid_19_time_series/time_series_covid19_{category}_US.csv'
        self.categories = ['confirmed', 'deaths']
        self.measures = {'confirmed': 'positiveCumulative', 'deaths': 'deathCumulative'}

        # The name of the population field
        self.inhabitants = 'POPESTIMATE2019'

        # Outcomes directories
        self.warehouse = os.path.join(os.getcwd(), 'warehouse')

    @staticmethod
    def regions():

        urn = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
              'countries/us/geography/regions/names.csv'
        urc = 'https://raw.githubusercontent.com/discourses/hub/develop/data/' \
              'countries/us/geography/regions/fips.csv'

        return urn, urc

    def storage(self):

        directories = hopkins.base.directories.Directories()
        directories.cleanup(listof=[self.warehouse])
        directories.create(listof=[self.warehouse])

    def days(self):
        """
        values.datetimeobject.apply(lambda x: x.value / (10 ** 6)).astype(np.longlong)

        :return: A DataFrame of dates
        """

        values = pd.DataFrame(pd.date_range(start=self.starting, end=self.ending, freq='D'), columns=['datetimeobject'])
        values.loc[:, self.datestring] = values.datetimeobject.apply(lambda x: x.strftime(self.datepattern))
        values.loc[:, 'epochmilli'] = (values['datetimeobject'].astype(np.int64) / (10 ** 6)).astype(np.longlong)

        return values
