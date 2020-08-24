import logging
import os
import sys

import pandas as pd


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # State Population
    population: pd.DataFrame = state.exc(segment='2010-2019', year='2019')

    # States
    states: pd.DataFrame = boundaries.states(year=settings.latest)
    states = population[['STATEFP', 'POPESTIMATE2019']].merge(states, how='left', on='STATEFP')

    # Days
    days = configurations.days()

    # Reference
    reference = atlantic.src.reference.Reference(dates=days[['datetimeobject']])
    references = reference.exc(states=states)

    # The C.T.P. data
    read = atlantic.src.readings.Readings(references=references, states=states)
    readings: pd.DataFrame = read.exc()

    # Enhancements
    derive = atlantic.algorithms.derivations.Derivations(data=readings)
    derivations = derive.exc(places=states)
    logger.info(derivations.tail())

    # The latest positive test rates: for graph labelling purposes
    rates = atlantic.algorithms.rates.Rates(blob=derivations).exc()

    # Places
    gazetteer = atlantic.src.gazetteer.Gazetteer(rates=rates)
    gazetteer.exc(states=states)

    # Hence, save the data sets of interest
    atlantic.algorithms.segmentations.Segmentation(blob=derivations).exc()

    # Statistics
    spreads = atlantic.adhoc.spreads.Spreads(states=states)
    spreads.exc(pool='states')


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    import config

    import atlantic.src.reference
    import atlantic.algorithms.derivations
    import atlantic.algorithms.rates
    import atlantic.algorithms.segmentations

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
    import atlantic.adhoc.spreads

    main()
