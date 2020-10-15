import logging
import os
import sys


def main():
    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    gazetteer = references.gazetteer()
    logger.info('\n{}\n'.format(gazetteer.tail()))

    directories.cleanup(listof=configurations.paths)
    directories.create(listof=configurations.paths)

    intersections.exc()


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)

    # Imports
    import config
    import toxicity.src.references
    import toxicity.base.directories
    import toxicity.algorithms.intersections

    # Instances
    configurations = config.Config()
    references = toxicity.src.references.References()
    directories = toxicity.base.directories.Directories()
    intersections = toxicity.algorithms.intersections.Intersections()

    main()
