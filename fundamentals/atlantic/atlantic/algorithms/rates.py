import pandas as pd


class Rates:

    def __init__(self, blob):
        self.blob = blob

    def latest(self):
        data = self.blob[['datetimeobject', 'STUSPS', 'positiveRate', 'testRate']].copy()

        values = data[data.datetimeobject == data.datetimeobject.max()]
        values.reset_index(drop=True, inplace=True)

        return values

    @staticmethod
    def estimates(latest):
        data = latest.copy()
        values = pd.DataFrame(data={'STUSPS': data['STUSPS'].values,
                                    'positiveTestRate': (100 * data['positiveRate'] / data['testRate']).values
                                    })
        data = data.merge(values, how='left', on='STUSPS')

        return data

    def exc(self):
        # A DataFrame of the latest record, w.r.t. date, per State
        latest = self.latest()
        return self.estimates(latest=latest)
