import dask
import os

import toxicity.src.references
import toxicity.src.risks

import config


class Intersections:

    def __init__(self):
        """
        The constructor

        """

        # References
        references = toxicity.src.references.References()

        # The latest -SARS measures per 100,000 people- values
        self.accumulations = references.accumulations()

        # A summary of the files to be read
        self.sources = references.sources()

        # Risks
        self.risks = toxicity.src.risks.Risks()

        configurations = config.Config()
        self.warehouse = configurations.warehouse

    @dask.delayed
    def graphing(self):
        """
        In progress ...

            The COLAB Prototype has been finalised

            Merge self.graphing() & self.modelling ---> self.write()

        :return:
        """

        print('graphing')

    @dask.delayed
    def modelling(self, blob, file: str) -> int:
        """

        :param blob: The merged -risk- & -SARS measures per 100,000 people- data
        :param file: The name of the file that 'blob' will be written to
        :return:
        """

        blob.to_csv(path_or_buf=os.path.join(self.warehouse, 'modelling', file),
                    header=True, index=False, encoding='utf-8')

        return 0

    @dask.delayed
    def scores(self, file: str, pattern: str):
        """

        :param file: The name of the risk file that would be read
        :param pattern:
        :return:
        """

        return self.risks.exc(file=file, pattern=pattern)

    @dask.delayed
    def latest(self, scores_):
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

        computations = []
        for file, pattern in zip(sources['file'].values, sources['pattern'].values):

            scores = self.scores(file=file, pattern=pattern)
            latest = self.latest(scores_=scores)
            modelling = self.modelling(blob=latest, file=file)
            computations.append(modelling)

        dask.visualize(computations, filename='computations', format='pdf')
        dask.compute(computations, scheduler='processes')
