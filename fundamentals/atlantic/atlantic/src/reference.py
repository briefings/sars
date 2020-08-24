import dask
import pandas as pd


class Reference:

    def __init__(self, dates: pd.DataFrame):
        self.dates = dates

    def baselines(self, stusps: str):
        values = self.dates.copy()
        values.loc[:, 'STUSPS'] = stusps

        return values

    def exc(self, states: pd.DataFrame):
        computations = [dask.delayed(self.baselines)(stusps) for stusps in states.STUSPS.values]
        dask.visualize(computations, filename='reference', format='pdf')
        values = dask.compute(computations, scheduler='processes')[0]

        return pd.concat(values)
