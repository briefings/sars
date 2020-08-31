import os

import numpy as np
import pandas as pd

import atlantic.base.archive
import atlantic.base.directories
import atlantic.spreads.attributes
import candles.candlesticks


class Distributions:

    def __init__(self, states: pd.DataFrame):

        # The States
        self.states = states

        # Attributes for distributions calculations
        attributes = atlantic.spreads.attributes.Attributes()
        self.fields = attributes.fields()
        self.dtype = attributes.dtype()
        self.parse_dates = attributes.parse_dates()
        self.categories = attributes.categories()

        self.sourcestring = attributes.sourcestring
        self.sourcename = attributes.sourcename
        self.path = attributes.path
        self.points = attributes.points

    def data(self):

        try:
            values = pd.read_csv(filepath_or_buffer=self.sourcestring, header=0, usecols=self.fields,
                                 dtype=self.dtype, encoding='utf-8', parse_dates=self.parse_dates)
        except OSError as err:
            raise err

        values.loc[:, 'epochmilli'] = (values['datetimeobject'].astype(np.int64) / (10 ** 6)).astype(np.longlong)

        return values

    def candles(self, data, days, path):

        candlesticks = candles.candlesticks.CandleSticks(days=days, points=self.points)

        for category in self.categories:

            readings = data[['epochmilli', 'STUSPS', category]]
            pivoted = readings.pivot(index='STUSPS', columns='epochmilli', values=category)
            patterns = candlesticks.execute(data=pivoted, fields=days['epochmilli'].values)

            if category.endswith('Rate'):
                patterns.drop(columns=['tally'], inplace=True)

            if category.endswith('Increase'):
                patterns.loc[:, 'tallycumulative'] = patterns['tally'].cumsum(axis=0)

            patterns.to_json(path_or_buf=os.path.join(path, '{}.json'.format(category)), orient='values')

    def exc(self):

        directories = atlantic.base.directories.Directories()
        directories.create(listof=[self.path])

        data = self.data()
        days = data[['epochmilli']].drop_duplicates(inplace=False, ignore_index=True, keep='first')

        self.candles(data=data, days=days, path=self.path)
        atlantic.base.archive.Archive().exc(path=self.path)
