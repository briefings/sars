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
    logger.info('\n{}\n'.format(population.shape))

    # States
    states: pd.DataFrame = boundaries.states(year=settings.latest)
    states.rename(columns={'GEOID': 'STATEGEOID', 'NAME': 'STATE'}, inplace=True)
    logger.info('\n{}\n'.format(states.tail()))

    # Counties
    counties = boundaries.counties(year=settings.latest)
    counties.rename(columns={'GEOID': 'COUNTYGEOID', 'NAME': 'COUNTY'}, inplace=True)
    counties = counties.merge(states[['STATEFP', 'STUSPS']], how='left', on='STATEFP')
    logger.info('\n{}\n'.format(counties.tail()))

    # Gazetteer
    populationfield = 'POPESTIMATE2019'
    gazetteer, supplement = hopkins.src.gazetteer.Gazetteer(). \
        exc(counties=counties[['STATEFP', 'STUSPS', 'COUNTYFP', 'COUNTYGEOID', 'COUNTY']],
            population=population[['COUNTYGEOID', populationfield]],
            populationfield=populationfield)
    logger.info('\n{}\n'.format(gazetteer.tail()))
    logger.info('\n{}\n'.format(supplement))

    logger.info('\nCounty: {}\n'.format(gazetteer[populationfield].sum()))
    logger.info('\nState: {}\n'.format(supplement[populationfield].sum()))

    # Days
    days = configurations.days()
    logger.info('\n{}\n'.format(days.tail()))

    # The fields of the latest J.H. data set
    features = hopkins.src.features.Features(datestrings=days[configurations.datestring].values,
                                             basefields=['FIPS']).exc()
    logger.info('\n{}\n'.format(features.head()))

    # Reference
    reference = hopkins.src.reference.Reference(days=days,
                                                gazetter=gazetteer[['STATEFP', 'COUNTYGEOID', populationfield]]).exc()
    logger.info('\n{}\n'.format(reference.tail()))

    # Readings
    readings = hopkins.src.readings.Readings(features=features, reference=reference, days=days).exc()
    logger.info('\n{}\n'.format(readings.tail(n=13)))

    adjusted = hopkins.algorithms.anomalies.Anomalies(blob=readings).exc()
    logger.info('\n{}\n'.format(adjusted.tail(n=13)))

    derivations = hopkins.algorithms.derivations.Derivations().exc(blob=adjusted,
                                                                populationfield=populationfield)
    logger.info('\n{}\n'.format(derivations.tail(13)))


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
