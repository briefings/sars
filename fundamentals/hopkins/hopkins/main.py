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
    gazetteer, _ = hopkins.src.gazetteer.Gazetteer(counties=counties, states=states, population=population).exc()

    # The fields of the latest J.H. data set
    features = hopkins.src.features.Features(datestrings=days[datestring].values,
                                             basefields=['FIPS']).exc()

    # Reference: Creates all distinct county & dates combinations per county
    reference = hopkins.src.reference.Reference(
        days=days, gazetter=gazetteer[['STATEFP', 'STUSPS', 'COUNTYGEOID', inhabitants]]).exc()

    # Readings: Reads and structures
    readings = hopkins.src.readings.Readings(features=features, reference=reference, days=days).exc()

    # Addressing Anomalies. Therefore, county & state readings after corrections ...
    anomalies = hopkins.algorithms.anomalies.Anomalies(blob=readings)
    countylevel, statelevel = anomalies.exc()

    # Write
    hopkins.streams.nucleus.Nucleus(data=countylevel).exc()
    hopkins.streams.membrane.Membrane(data=statelevel).exc()


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    import config

    import hopkins.src.features
    import hopkins.src.gazetteer
    import hopkins.src.reference
    import hopkins.src.readings

    import hopkins.algorithms.anomalies

    # Create a config instance and empty the results storage directories
    configurations = config.Config()
    configurations.storage()
    days = configurations.days()
    inhabitants = configurations.inhabitants
    datestring = configurations.datestring

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

    # Calculations
    import hopkins.streams.nucleus
    import hopkins.streams.membrane

    main()
