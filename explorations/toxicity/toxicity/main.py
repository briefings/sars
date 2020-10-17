import logging
import os
import sys

import glob


def main():
    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Directories
    directories.cleanup(listof=configurations.paths)
    directories.create(listof=configurations.paths)

    # For ...
    intersections.exc()

    # Subsequently ...

    # Gazetter
    gazetteer = references.gazetteer()
    logger.info('\n{}\n'.format(gazetteer.tail()))

    # Partitions instance
    partitions = toxicity.algorithms.partitions.Partitions(gazetteer=gazetteer)
    sources = glob.glob(os.path.join(configurations.warehouse, 'modelling', '*.csv'))
    for source in sources:

        if not (source.__contains__('immunological') | source.__contains__('resp')):
            continue

        case, classification = configurations.names(source=source)

        directory = os.path.join(configurations.warehouse, 'graphing', case, classification)
        directories.create(listof=[directory])

        print(directory, '\n', source)

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
