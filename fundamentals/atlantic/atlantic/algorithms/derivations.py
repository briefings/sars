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
        _, _, self.cumulative, self.increase = configurations.variables()

        # Labels for continuous & discrete capita fields
        self.ccl = [measure.replace('Cumulative', 'Rate') for measure in self.cumulative]
        self.dcl = [measure + 'Rate' for measure in self.increase]

    @dask.delayed
    def ccv(self, blob: pd.DataFrame):
        """
        continuous capita values
        
        :param blob:
        :return: Appends cumulative values per 100,000 people
        """
        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.cumulative].divide(blob['POPESTIMATE2019'], axis=0)).values,
                              columns=self.ccl)],
                         axis=1)

    @dask.delayed
    def dcv(self, blob: pd.DataFrame):
        """
        discrete capita values
        
        :param blob:
        :return: Appends discrete values per 100,000 people
        """
        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.increase].divide(blob['POPESTIMATE2019'], axis=0)).values,
                              columns=self.dcl)],
                         axis=1)

    @dask.delayed
    def segment(self, stusps: str):
        """
        Gets all data w.r.t. STUSPS
        :param stusps:
        :return:
        """

        sample = self.data.copy()
        sample = sample[sample['STUSPS'] == stusps]
        sample = sample.sort_values(by='datetimeobject', ascending=True, inplace=False, ignore_index=True)

        return sample

    def exc(self, places: np.ndarray):
        """

        :param places: An array of STUSPS places; FIPS codes 'STUSPS' (ref: https://www.nist.gov/itl/publications-0/
                       federal-information-processing-standards-fips)
        :return:
        """
        computations = []

        for place in places:
            values = self.segment(stusps=place)
            values = self.ccv(values)
            values = self.dcv(values)
            computations.append(values)

        dask.visualize(computations, filename='derivations', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]
        data = pd.concat(calculations, axis=0, ignore_index=True)

        data.loc[:, 'ndays'] = (- self.epochdays) + (
                data['datetimeobject'].astype(np.int64) / (60 * 60 * 24 * (10 ** 9))).astype(int)

        return data
