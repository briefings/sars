import os
import sys

import pandas as pd


def main():

    # County Population
    population: pd.DataFrame = county.exc(year='2019')
    population.drop_duplicates(inplace=True)

    # States
    states: pd.DataFrame = boundaries.states(year=settings.latest)
    states.rename(columns={'GEOID': 'STATEGEOID', 'NAME': 'STATE', 'ALAND': 'STATESQMETRES'}, inplace=True)

    # Counties
    counties = boundaries.counties(year=settings.latest)
    counties.rename(columns={'GEOID': 'COUNTYGEOID', 'NAME': 'COUNTY'}, inplace=True)
    counties = counties.merge(states[['STATEFP', 'STUSPS', 'STATE', 'STATESQMETRES']], how='left', on='STATEFP')

    # Gazetteer
    gazetteer = hopkins.src.gazetteer.Gazetteer(counties=counties, population=population).exc()

    # The fields of the latest J.H. data set
    features = hopkins.src.features.Features(
        datestrings=days[configurations.datestring].values, basefields=['FIPS']).exc()

    # Reference: Creates all distinct county & dates combinations per county
    reference = hopkins.src.reference.Reference(
        days=days, gazetter=gazetteer[['STATEFP', 'STUSPS', 'COUNTYGEOID', configurations.inhabitants]]).exc()

    # Readings: Reads and structures
    readings = hopkins.src.readings.Readings(features=features, reference=reference, days=days).exc()

    # Addressing Anomalies. Returns county readings after corrections ...
    anomalies = hopkins.algorithms.anomalies.Anomalies(blob=readings).exc()

    # Deriving variables; these are appended to the initial variables
    derivations = hopkins.algorithms.derivations.Derivations().exc(blob=anomalies)

    # The latest cumulative values
    hopkins.algorithms.accumulations.Accumulations(blob=derivations).exc()

    # Row batches w.r.t. 'partitionby'
    partitions = hopkins.algorithms.partitions.Partitions(blob=derivations, partitionby='STUSPS')
    partitions.exc()

    # Columnar batch
    segments = hopkins.algorithms.segments.Segments(blob=derivations)
    segments.exc()

    # Candles
    spreads = hopkins.spreads.distributions.Distributions(via='COUNTYGEOID')
    spreads.exc(path=os.path.join(configurations.warehouse, 'baselines', '*.csv'))


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    # Utilities
    import hopkins.base.utilities
    hopkins.base.utilities.Utilities().exc()

    # Thenj
    import config

    import hopkins.src.features
    import hopkins.src.gazetteer
    import hopkins.src.reference
    import hopkins.src.readings

    import hopkins.algorithms.anomalies
    import hopkins.algorithms.accumulations
    import hopkins.algorithms.derivations
    import hopkins.algorithms.partitions
    import hopkins.algorithms.segments

    import hopkins.spreads.distributions

    # Create a config instance and empty the results storage directories
    configurations = config.Config()
    configurations.storage()
    days = configurations.days()

    # Utilities: Cartographs
    import cartographs.boundaries.us.boundaries
    import cartographs.boundaries.us.settings
    settings = cartographs.boundaries.us.settings.Settings()
    boundaries = cartographs.boundaries.us.boundaries.Boundaries(crs=settings.crs)

    # Utilities: Populations
    import populations.us.reference.county
    county = populations.us.reference.county.County()

    main()
