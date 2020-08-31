import logging

import dask
import pandas as pd


class Reference:

    def __init__(self, dates: pd.DataFrame):

        self.dates = dates

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def baselines(self, stusps: str):

        values = self.dates.copy()
        values.loc[:, 'STUSPS'] = stusps

        return values

    def exc(self, states: pd.DataFrame):

        computations = [dask.delayed(self.baselines)(stusps) for stusps in states.STUSPS.values]
        dask.visualize(computations, filename='reference', format='pdf')
        calculations = dask.compute(computations, scheduler='processes')[0]
        values = pd.concat(calculations)

        self.logger.info('\n{}\n'.format(values.tail()))

        return values
