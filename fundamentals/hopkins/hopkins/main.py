import logging
import os
import sys

import pandas as pd


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # County Population
    population: pd.DataFrame = county.exc(year='2019')
    population.drop_duplicates(inplace=True)
    logger.info('\n{}\n'.format(population.tail()))

    # States
    states: pd.DataFrame = boundaries.states(year=settings.latest)
    states.rename(columns={'GEOID': 'STATEGEOID', 'NAME': 'STATE'}, inplace=True)
    logger.info('\n{}\n'.format(states.tail()))

    # Counties
    counties = boundaries.counties(year=settings.latest)
    counties.rename(columns={'GEOID': 'COUNTYGEOID', 'NAME': 'COUNTY'}, inplace=True)
    counties = counties.merge(states[['STATEFP', 'STUSPS', 'STATE']], how='left', on='STATEFP')
    logger.info('\n{}\n'.format(counties.tail()))

    # Gazetteer
    gazetteer, _ = hopkins.src.gazetteer.Gazetteer(counties=counties, states=states, population=population,
                                                   inhabitants=inhabitants).exc()

    # The fields of the latest J.H. data set
    features = hopkins.src.features.Features(datestrings=days[configurations.datestring].values,
                                             basefields=['FIPS']).exc()

    # Reference
    reference = hopkins.src.reference.Reference(days=days,
                                                gazetter=gazetteer[['STATEFP', 'COUNTYGEOID', inhabitants]]
                                                ).exc()

    # Readings
    readings = hopkins.src.readings.Readings(features=features, reference=reference, days=days).exc()

    # Addressing Anomalies
    hencecounty = hopkins.algorithms.anomalies.Anomalies(blob=readings).exc()

    hencestate: pd.DataFrame = hencecounty.drop(columns=['COUNTYGEOID']).\
        groupby(by=['datetimeobject', 'date', 'epochmilli', 'STATEFP']).sum()
    hencestate.reset_index(drop=False, inplace=True)

    # Derivations
    derivations = hopkins.algorithms.derivations.Derivations()

    usc = derivations.exc(blob=hencecounty, inhabitants=inhabitants)
    logger.info('\n{}\n'.format(usc.info()))

    uss = derivations.exc(blob=hencestate, inhabitants=inhabitants)
    logger.info('\n{}\n'.format(uss.info()))


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    import config

    import hopkins.src.features
    import hopkins.src.gazetteer
    import hopkins.src.reference
    import hopkins.src.readings

    import hopkins.algorithms.anomalies
    import hopkins.algorithms.derivations

    # Create a config instance and empty the results storage directories
    configurations = config.Config()
    configurations.storage()
    days = configurations.days()
    inhabitants = configurations.inhabitants

    # Utilities
    import hopkins.base.utilities

    hopkins.base.utilities.Utilities().exc()

    # Utilities: Cartographs
    import cartographs.boundaries.us.boundaries
    import cartographs.boundaries.us.settings

    settings = cartographs.boundaries.us.settings.Settings()
    boundaries = cartographs.boundaries.us.boundaries.Boundaries(crs=settings.crs)

    # Utilities: Populations
    import populations.us.reference.county

    county = populations.us.reference.county.County()

    main()
