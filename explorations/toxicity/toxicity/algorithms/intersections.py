import dask
import os
import pandas as pd

import toxicity.src.references
import toxicity.src.risks

import config


class Intersections:

    def __init__(self):
        """
        The constructor

        """

        # References, A summary of the files to be read, The latest -SARS measures per 100,000 people- values
        references = toxicity.src.references.References()
        self.sources: pd.DataFrame = references.sources()
        self.accumulations: pd.DataFrame = references.accumulations()

        # Risks
        self.risks = toxicity.src.risks.Risks()

        # Configurations
        configurations = config.Config()
        self.warehouse = configurations.warehouse

    @dask.delayed
    def write(self, blob, file: str) -> int:
        """

        :param blob: The merged -risk- & -SARS measures per 100,000 people- data
        :param file: The name of the file that 'blob' will be written to
        :return:
        """

        blob.to_csv(path_or_buf=os.path.join(self.warehouse, 'modelling', file),
                    header=True, index=False, encoding='utf-8')

        return 0

    @dask.delayed
    def scores(self, file: str, pattern: str) -> pd.DataFrame:
        """
        Read a set of risk scores

        :param file: The name of the risk file that would be read
        :param pattern:
        :return:
        """

        return self.risks.exc(file=file, pattern=pattern)

    @dask.delayed
    def latest(self, scores_: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the latest -SARS measures per 100,000 people- & the EPA risk scores

        :param scores_: Risk data
        :return:
        """

        return self.accumulations.merge(scores_, how='left', on=['STUSPS', 'COUNTYGEOID'])

    def exc(self):
        """

        :return:
        """

        sources = self.sources

        intersections = []
        for file, pattern in zip(sources['file'].values, sources['pattern'].values):

            scores = self.scores(file=file, pattern=pattern)
            latest = self.latest(scores_=scores)
            intersections.append(self.write(blob=latest, file=file))

        dask.visualize(intersections, filename='intersections', format='pdf')
        dask.compute(intersections, scheduler='processes')
