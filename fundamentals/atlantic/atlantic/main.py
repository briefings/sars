import logging
import os
import sys

import pandas as pd


def main():
    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # State Population
    population: pd.DataFrame = state.exc(segment='2010-2019', year='2019')

    # States
    states: pd.DataFrame = boundaries.states(year=settings.latest)
    states = population[['STATEFP', 'POPESTIMATE2019']].merge(states, how='left', on='STATEFP')
    states.rename(columns={'NAME': 'STATE', 'GEOID': 'STATEGEOID'}, inplace=True)

    # Days
    days = configurations.days()

    # Reference
    reference = atlantic.src.reference.Reference(dates=days[['datetimeobject']])
    references = reference.exc(states=states)

    # The C.T.P. data
    readings: pd.DataFrame = atlantic.src.readings.Readings(references=references, states=states).exc()

    # Addressing Anomalies
    anomalies = atlantic.algorithms.anomalies.Anomalies(blob=readings).exc()

    # Tests
    tests = atlantic.algorithms.tests.Tests(blob=anomalies).exc()

    # Features engineering; drop 'negativeIncrease' & 'negativeCumulative' beforehand
    derive = atlantic.algorithms.derivations.Derivations(
        data=tests.drop(columns=['negativeIncrease', 'negativeCumulative']))
    derivations = derive.exc(places=tests['STUSPS'].unique())
    logger.info('\nDerivations:\n{}\n'.format(derivations.info()))

    # The latest positive test rates: for graph labelling purposes
    atlantic.algorithms.rates.Rates(blob=derivations).exc()

    # Places
    gazetteer = atlantic.src.gazetteer.Gazetteer()
    gazetteer.exc(states=states)

    # Hence, save the data sets of interest
    atlantic.algorithms.segments.Segments(blob=derivations).exc()

    # Statistics
    spreads = atlantic.spreads.distributions.Distributions(states=states)
    spreads.exc()


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    import config

    import atlantic.src.reference
    import atlantic.algorithms.anomalies
    import atlantic.algorithms.tests
    import atlantic.algorithms.derivations
    import atlantic.algorithms.rates
    import atlantic.algorithms.segments

    import atlantic.src.gazetteer
    import atlantic.src.readings

    # Create a config instance and empty the results storage directories
    configurations = config.Config()
    configurations.storage()

    # Utilities
    import atlantic.base.utilities

    atlantic.base.utilities.Utilities().exc()

    # Utilities: Cartographs
    import cartographs.boundaries.us.boundaries
    import cartographs.boundaries.us.settings

    settings = cartographs.boundaries.us.settings.Settings()
    boundaries = cartographs.boundaries.us.boundaries.Boundaries(crs=settings.crs)

    # Utilities: Populations
    import populations.us.reference.state

    state = populations.us.reference.state.State()

    # Utilities: Candles
    import atlantic.spreads.distributions

    main()
