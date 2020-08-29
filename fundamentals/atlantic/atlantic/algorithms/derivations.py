import dask
import numpy as np
import pandas as pd

import config


class Derivations:

    def __init__(self, data: pd.DataFrame):
        """

        :param data: The data set that accumulative & per capita calculations will be applied to
        """
        self.data = data

        # Configurations & Field Names
        configurations = config.Config()

        self.epochdays = configurations.epochdays

        self.measures = configurations.measures
        self.cumulative = [measure.replace('Increase', 'Cumulative') for measure in self.measures]
        self.rate = [measure.replace('Increase', 'Rate') for measure in self.measures]
        self.increase_rate = [measure + 'Rate' for measure in self.measures]

    @dask.delayed
    def accumulations(self, stusps: str):
        """
        Calculates cumulative values per state
        :param stusps: The STUSPS tring of a state
        :return:
        """

        pool = self.data.copy()
        sample = pool[pool['STUSPS'] == stusps]
        sample = sample.sort_values(by='datetimeobject', ascending=True, inplace=False, ignore_index=True)

        return pd.concat([sample,
                          pd.DataFrame(data=sample[self.measures].cumsum(axis=0).values, columns=self.cumulative)],
                         axis=1)

    @dask.delayed
    def capita_continuous(self, blob: pd.DataFrame):
        """
        :param blob:
        :return: Appends cumulative values per 100,000 people
        """
        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.cumulative].divide(blob['POPESTIMATE2019'], axis=0)).values,
                              columns=self.rate)],
                         axis=1)

    @dask.delayed
    def capita_discrete(self, blob: pd.DataFrame):
        """
        :param blob:
        :return: Appends discrete values per 100,000 people
        """
        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.measures].divide(blob['POPESTIMATE2019'], axis=0)).values,
                              columns=self.increase_rate)],
                         axis=1)

    def exc(self, places: pd.DataFrame):
        """

        :param places: A DataFrame of places that must include a field that denotes the FIPS
                       variable 'STUSPS' (ref: https://www.nist.gov/itl/publications-0/
                       federal-information-processing-standards-fips)
        :return:
        """
        computations = []

        for stusps in places.STUSPS.values:
            values = self.accumulations(stusps=stusps)
            values = self.capita_continuous(values)
            values = self.capita_discrete(values)
            computations.append(values)

        dask.visualize(computations, filename='derivations', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]
        data = pd.concat(calculations, axis=0, ignore_index=True)

        data.loc[:, 'ndays'] = (- self.epochdays) + (
                data['datetimeobject'].astype(np.int64) / (60 * 60 * 24 * (10 ** 9))).astype(int)

        return data
