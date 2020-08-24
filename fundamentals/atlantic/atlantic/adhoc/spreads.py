import os

import numpy as np
import pandas as pd
import zipfile
import pathlib

import atlantic.base.directories
import candles.candlesticks
import config


class Spreads:

    def __init__(self, states: pd.DataFrame):

        self.poolprefix = 'candle'

        # The States
        self.states = states

        # Spread, i.e., quantile, points of interest
        self.points = np.array((0.1, 0.25, 0.5, 0.75, 0.9))

        # Data Warehouse Path
        self.warehouse = config.Config().warehouse

        # Spreads of interest
        self.events = ['positiveIncrease', 'testIncrease', 'deathIncrease', 'positiveCumulative', 'testCumulative',
                       'deathCumulative', 'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                       'positiveRate', 'testRate', 'deathRate']

        # Data Attributes
        self.fields = ['datetimeobject', 'STUSPS', 'positiveIncrease', 'testIncrease', 'deathIncrease',
                       'positiveCumulative', 'testCumulative', 'deathCumulative',
                       'positiveIncreaseRate', 'testIncreaseRate', 'deathIncreaseRate',
                       'positiveRate', 'testRate', 'deathRate', 'ndays']

        self.dtype = {'STUSPS': 'str', 'positiveIncrease': np.float64, 'testIncrease': np.float64,
                      'deathIncrease': np.float64, 'positiveCumulative': np.float64, 'testCumulative': np.float64,
                      'deathCumulative': np.float64, 'positiveIncreaseRate': np.float64, 'testIncreaseRate': np.float64,
                      'deathIncreaseRate': np.float64, 'positiveRate': np.float64, 'testRate': np.float64,
                      'deathRate': np.float64, 'ndays': np.int64}

        self.parse_dates = ['datetimeobject']

    def data(self, pool: str):

        try:
            values = pd.read_csv(filepath_or_buffer=os.path.join(self.warehouse, self.poolprefix + pool + '.csv'),
                                 header=0, usecols=self.fields, dtype=self.dtype, encoding='utf-8',
                                 parse_dates=self.parse_dates)
        except OSError as err:
            raise err

        values.loc[:, 'epochmilli'] = (values['datetimeobject'].astype(np.int64) / (10 ** 6)).astype(np.longlong)

        return values

    def sticks(self, data, days, path):

        candlesticks = candles.candlesticks.CandleSticks(days=days, key='datetimeobject', points=self.points)

        for event in self.events:

            readings = data[['datetimeobject', 'STUSPS', event]]
            pivoted = readings.pivot(index='STUSPS', columns='datetimeobject', values=event)
            sticks = candlesticks.execute(data=pivoted, fields=days['datetimeobject'].values)

            if event.endswith('Rate'):
                sticks.drop(columns=['tally'], inplace=True)

            if event.endswith('Increase'):
                sticks.loc[:, 'tallycumulative'] = sticks['tally'].cumsum(axis=0)

            sticks.to_json(path_or_buf=os.path.join(path, '{}.json'.format(event)), orient='values')

    @staticmethod
    def archive(path: str):

        items = pathlib.Path(path).glob('*.json')
        filestrings = [str(item) for item in items if item.is_file()]
        zipfileobject = zipfile.ZipFile(path + '.zip', 'w')

        with zipfileobject:
            for filestring in filestrings:
                zipfileobject.write(filename=filestring, arcname=os.path.basename(filestring))

        zipfileobject.close()

    def exc(self, pool: str):

        directories = atlantic.base.directories.Directories()
        path: str = os.path.join(self.warehouse, self.poolprefix, pool)
        directories.create(listof=[path])

        data = self.data(pool=pool)
        days = data[['datetimeobject', 'epochmilli']].drop_duplicates(inplace=False, ignore_index=True, keep='first')
        self.sticks(data=data, days=days, path=path)
        self.archive(path=path)
