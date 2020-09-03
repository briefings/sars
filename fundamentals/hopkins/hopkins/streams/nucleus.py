import os

import config
import hopkins.algorithms.anomalies
import hopkins.algorithms.derivations
import hopkins.algorithms.partitions
import hopkins.algorithms.segments
import hopkins.spreads.distributions


class Nucleus:

    def __init__(self, data):
        self.data = data

        configurations = config.Config()
        self.inhabitants = configurations.inhabitants
        self.datestring = configurations.datestring
        self.warehouse = configurations.warehouse

        self.derivations = hopkins.algorithms.derivations.Derivations()

    def spread(self):
        spreads = hopkins.spreads.distributions.Distributions(level='county', via='COUNTYGEOID')
        spreads.exc(path=os.path.join(self.warehouse, 'county', 'baselines', '*.csv'))

    def exc(self):
        # Deriving variables
        derivations = self.derivations.exc(blob=self.data)

        # Row batches w.r.t. 'partitionby'
        partitions = hopkins.algorithms.partitions.Partitions(
            blob=derivations.drop(columns=[self.datestring, self.inhabitants], inplace=False),
            partitionby='STUSPS'
        )
        partitions.exc(category='county', segment='baselines')

        # Columnar batch
        segments = hopkins.algorithms.segments.Segments(blob=derivations, category='county')
        segments.exc(select=['datetimeobject', 'epochmilli', 'STUSPS', 'COUNTYGEOID', 'positiveRate', 'deathRate'],
                     segment='capita')

        self.spread()
