import glob
import os

import dask
import pandas as pd

import candles.candlesticks
import config
import hopkins.base.directories
import hopkins.spreads.attributes


class Distributions:

    def __init__(self, level: str, via: str):
        """

        :param level: county or state
        :param via: Either COUNTYGEOID or STUSPS
        """

        self.level = level
        self.via = via

        configurations = config.Config()
        self.days = configurations.days()
        self.warehouse = configurations.warehouse
        self.path = os.path.join(self.warehouse, self.level, 'candles')

        attributes = hopkins.spreads.attributes.Attributes(level=self.level)
        self.variables = attributes.variables()
        self.fields = attributes.fields()
        self.dtype = attributes.dtype()
        self.points = attributes.points

        self.directories = hopkins.base.directories.Directories()
        self.candlesticks = candles.candlesticks.CandleSticks(days=self.days[['epochmilli']], points=self.points)

    def paths(self, path):
        self.directories.create(listof=[path])

    @dask.delayed
    def read(self, filestring: str):

        try:
            data = pd.read_csv(filepath_or_buffer=filestring, usecols=self.fields,
                               dtype=self.dtype, encoding='utf-8', header=0)
        except OSError as err:
            raise err

        return data

    @dask.delayed
    def candles(self, data, basename):
        """

        :param data: The data for calculating ...
        :param basename: Thus far, STUSPS codes only
        :return:
        """

        for variable in self.variables:

            readings = data[['epochmilli', self.via, variable]]
            pivoted = readings.pivot(index=self.via, columns='epochmilli', values=variable)
            patterns = self.candlesticks.execute(data=pivoted, fields=self.days['epochmilli'].values)

            if variable.endswith('Rate'):
                patterns.drop(columns=['tally'], inplace=True)

            if variable.endswith('Increase'):
                patterns.loc[:, 'tallycumulative'] = patterns['tally'].cumsum(axis=0)

            patterns.to_json(path_or_buf=os.path.join(self.path, basename, '{}.json'.format(variable)), orient='values')
            
        return 1

    def exc(self, path):
        """

        :param path: Either the directory to a set of files, or just a file
        :return:
        """

        filestrings = glob.glob(pathname=os.path.join(path, '*.csv'))
        basenames = [os.path.splitext(os.path.basename(filestring))[0] for filestring in filestrings]
        strings = [os.path.join(self.path, basename) for basename in basenames]

        directories = [dask.delayed(self.paths)(string) for string in strings]
        dask.visualize(directories, filename='directories', format='pdf')
        dask.compute(directories, scheduler='processes')

        computations = []
        for basename, filestring in zip(basenames, filestrings):
            data = self.read(filestring=filestring)
            status = self.candles(data=data, basename=basename)
            computations.append(status)
        dask.visualize(computations, filename='candles', format='pdf')
        numbers = dask.compute(computations, scheduler='processes')[0]

        return numbers
