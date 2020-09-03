import os

import config
import hopkins.algorithms.anomalies
import hopkins.algorithms.derivations
import hopkins.algorithms.partitions
import hopkins.algorithms.segments
import hopkins.spreads.distributions


class Membrane:

    def __init__(self, data):
        self.data = data

        configurations = config.Config()
        self.inhabitants = configurations.inhabitants
        self.datestring = configurations.datestring
        self.warehouse = configurations.warehouse

    def spread(self):
        spreads = hopkins.spreads.distributions.Distributions(level='state', via='STUSPS')
        spreads.exc(path=os.path.join(self.warehouse, 'state', 'baselines.csv'))

    def exc(self):
        derivations = hopkins.algorithms.derivations.Derivations() \
            .exc(blob=self.data)

        segments = hopkins.algorithms.segments.Segments(blob=derivations, category='state')
        segments.exc(select=derivations.columns.drop(labels=['date', 'STATEFP', 'POPESTIMATE2019']),
                     segment='baselines')
        segments.exc(select=['datetimeobject', 'epochmilli', 'STUSPS', 'positiveRate', 'deathRate'],
                     segment='capita')

        self.spread()
