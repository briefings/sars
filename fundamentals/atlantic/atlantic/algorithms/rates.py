import pandas as pd

import os

import config

class Rates:

    def __init__(self, blob):

        configurations = config.Config()
        self.warehouse = configurations.warehouse

        self.blob = blob

    def latest(self):
        data = self.blob[['datetimeobject', 'STUSPS', 'positiveRate', 'testRate']].copy()

        values = data[data.datetimeobject == data.datetimeobject.max()]
        values.reset_index(drop=True, inplace=True)

        return values

    @staticmethod
    def estimates(latest) -> pd.DataFrame:
        data = latest.copy()
        values = pd.DataFrame(data={'STUSPS': data['STUSPS'].values,
                                    'positiveTestRate': (100 * data['positiveRate'] / data['testRate']).values
                                    })
        data = data.merge(values, how='left', on='STUSPS')

        return data

    def exc(self):
        # A DataFrame of the latest record, w.r.t. date, per State
        latest = self.latest()
        estimates = self.estimates(latest=latest)
        estimates.to_csv(path_or_buf=os.path.join(self.warehouse, 'rates.csv'),
                         header=True, index=False, encoding='utf-8')
