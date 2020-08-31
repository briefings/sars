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

    # Reference: Creates all distinct county & dates combinations per county
    reference = hopkins.src.reference.Reference(days=days,
                                                gazetter=gazetteer[['STATEFP', 'STUSPS', 'COUNTYGEOID', inhabitants]]
                                                ).exc()

    # Readings: Reads and structures
    readings = hopkins.src.readings.Readings(features=features, reference=reference, days=days).exc()

    # Addressing Anomalies. Therefore, county & state readings after corrections ...
    rvc = hopkins.algorithms.anomalies.Anomalies(blob=readings).exc()
    logger.info('\n{}\n'.format(rvc.info()))

    rvs: pd.DataFrame = rvc.drop(columns=['COUNTYGEOID']). \
        groupby(by=['datetimeobject', 'date', 'epochmilli', 'STATEFP', 'STUSPS']).sum()
    rvs.reset_index(drop=False, inplace=True)
    logger.info('\n{}\n'.format(rvs.info()))

    # Derivations
    derivations = hopkins.algorithms.derivations.Derivations()

    usc = derivations.exc(blob=rvc, inhabitants=inhabitants)
    logger.info('\n{}\n'.format(usc.info()))

    uss = derivations.exc(blob=rvs, inhabitants=inhabitants)
    logger.info('\n{}\n'.format(uss.info()))

    # Depositing
    partitions = hopkins.algorithms.partitions. \
        Partitions(blob=usc.drop(columns=[datestring, inhabitants]), partitionby='STUSPS')
    partitions.exc(category='county', segment='baselines')

    low = hopkins.algorithms.segments.Segments(blob=usc, category='county')
    low.exc(select=['datetimeobject', 'epochmilli', 'STATEFP', 'COUNTYGEOID', 'positiveRate', 'deathRate'],
            segment='capita')

    high = hopkins.algorithms.segments.Segments(blob=uss, category='state')
    high.exc(select=uss.columns.drop(labels=['date', 'STUSPS', 'POPESTIMATE2019']),
             segment='baselines')
    high.exc(select=['datetimeobject', 'epochmilli', 'STATEFP', 'positiveRate', 'deathRate'],
             segment='capita')

    # Spreads
    logger.info(warehouse)
    logger.info(os.path.join(warehouse, 'county', 'baselines'))
    distributions = hopkins.spreads.distributions.Distributions(level='county', via='COUNTYGEOID')
    numbers = distributions.exc(path=os.path.join(warehouse, 'county', 'baselines'))
    logger.info('\n{}\n'.format(numbers))


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
    import hopkins.algorithms.partitions
    import hopkins.algorithms.segments



    # Create a config instance and empty the results storage directories
    configurations = config.Config()
    configurations.storage()
    days = configurations.days()
    inhabitants = configurations.inhabitants
    datestring = configurations.datestring
    warehouse = configurations.warehouse

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

    import hopkins.spreads.distributions

    main()
