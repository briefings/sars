import logging
import os
import sys

import glob


def main():
    """
    Entry point

    :return: None
    """

    """
      Logging
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    """
      Preparing directories
    """
    directories.cleanup(listof=configurations.paths)
    directories.create(listof=configurations.paths)

    """
      Mapping geographic & EPA data with the latest county level
        deaths/100K [C]
        positives/100K [C]
      data.  The data is saved in the warehouse/modelling directory.
    """
    intersections.exc()

    """
      Subsequently, sets of JSON files are created for each file in 
      warehouse/modelling for web graphing purposes
    """
    # Geographic data
    gazetteer = references.gazetteer()

    # Each file of warehouse/modelling will be broken-down via Partitions
    partitions = toxicity.algorithms.partitions.Partitions(gazetteer=gazetteer)

    # The list of files in warehouse/modelling
    sources = glob.glob(os.path.join(configurations.warehouse, 'modelling', '*.csv'))

    # Hence
    for source in sources:

        # Initially, limited focus
        if not (source.__contains__('immunological') | source.__contains__('resp')):
            continue

        # Loop focus
        print('\nIn focus: {}\n'.format(os.path.basename(source)))

        # Risks and pollutants
        case, classification = configurations.names(source=source)
        logger.info('\nmedical case/risk type -> {}\nclassification, i.e., pollutant or pollution source group -> {}\n'
                    .format(case, classification))

        # Hence, create the directory path wherein the JSON files will be saved
        directory = os.path.join(configurations.warehouse, 'graphing', case, classification)
        directories.create(listof=[directory])
        logger.info('directory -> \n{}\n'.format(directory))

        # Proceed ...
        partitions.exc(source=source, directory=directory)


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    # Imports
    import config
    import toxicity.src.references
    import toxicity.base.directories
    import toxicity.algorithms.intersections
    import toxicity.algorithms.partitions

    # Instances
    configurations = config.Config()
    references = toxicity.src.references.References()
    directories = toxicity.base.directories.Directories()
    intersections = toxicity.algorithms.intersections.Intersections()

    main()
