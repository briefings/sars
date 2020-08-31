import os

import pandas as pd

import config
import hopkins.algorithms.anomalies
import hopkins.algorithms.derivations
import hopkins.algorithms.partitions
import hopkins.algorithms.segments
import hopkins.spreads.distributions


class Interface:

    def __init__(self, readings):
        self.readings = readings

        configurations = config.Config()
        self.inhabitants = configurations.inhabitants
        self.datestring = configurations.datestring
        self.warehouse = configurations.warehouse

    def ano(self):
        # Addressing Anomalies. Therefore, county & state readings after corrections ...
        avc = hopkins.algorithms.anomalies.Anomalies(blob=self.readings).exc()

        avs: pd.DataFrame = avc.drop(columns=['COUNTYGEOID']). \
            groupby(by=['datetimeobject', 'date', 'epochmilli', 'STATEFP', 'STUSPS']).sum()
        avs.reset_index(drop=False, inplace=True)

        return avc, avs

    @staticmethod
    def der(avc, avs):
        # Derivations
        derivations = hopkins.algorithms.derivations.Derivations()
        usc = derivations.exc(blob=avc)
        uss = derivations.exc(blob=avs)

        return usc, uss

    def sli(self, usc, uss):
        # Depositing
        partitions = hopkins.algorithms.partitions. \
            Partitions(blob=usc.drop(columns=[self.datestring, self.inhabitants]), partitionby='STUSPS')
        partitions.exc(category='county', segment='baselines')

        low = hopkins.algorithms.segments.Segments(blob=usc, category='county')
        low.exc(select=['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveRate', 'deathRate'],
                segment='capita')

        high = hopkins.algorithms.segments.Segments(blob=uss, category='state')
        high.exc(select=uss.columns.drop(labels=['date', 'STATEFP', 'POPESTIMATE2019']),
                 segment='baselines')
        high.exc(select=['datetimeobject', 'epochmilli', 'STUSPS', 'positiveRate', 'deathRate'],
                 segment='capita')

    def spr(self):
        # Spreads
        distc = hopkins.spreads.distributions.Distributions(level='county', via='COUNTYGEOID')
        distc.exc(path=os.path.join(self.warehouse, 'county', 'baselines', '*.csv'))

        dists = hopkins.spreads.distributions.Distributions(level='state', via='STUSPS')
        dists.exc(path=os.path.join(self.warehouse, 'state', 'baselines.csv'))

    def exc(self):
        avc, avs = self.ano()
        usc, uss = self.der(avc=avc, avs=avs)
        self.sli(usc=usc, uss=uss)
        self.spr()
