import numpy as np
import pandas as pd

import config


class Derivations:

    def __init__(self):
        """
        Constructor
        """
        configurations = config.Config()
        self.measures: dict = configurations.measures
        self.epochdays = configurations.epochdays

        self.cumulative = self.measures.values()
        self.rate = [measure.replace('Cumulative', 'Rate') for measure in self.cumulative]

        self.increase = [measure.replace('Cumulative', 'Increase') for measure in self.cumulative]
        self.increase_rate = [increase + 'Rate' for increase in self.increase]

    def capita_continuous(self, blob: pd, inhabitants: str):
        """
        Calculates the values per 100,000 people
        :param blob:
        :param inhabitants:
        :return:
        """
        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.cumulative].divide(blob[inhabitants], axis=0)).values,
                              columns=self.rate)],
                         axis=1)

    def capita_discrete(self, blob: pd, inhabitants: str):
        """

        :param blob:
        :param inhabitants:
        :return:
        """

        return pd.concat([blob,
                          pd.DataFrame(
                              data=(100000 * blob[self.increase].divide(blob[inhabitants], axis=0)).values,
                              columns=self.increase_rate)],
                         axis=1)

    def exc(self, blob: pd.DataFrame, inhabitants: str):
        """

        :param blob:
        :param inhabitants: The name of the population field in blob
        :return:
        """
        data = blob.copy()
        data = self.capita_continuous(blob=data, inhabitants=inhabitants)
        data = self.capita_discrete(blob=data, inhabitants=inhabitants)
        data.loc[:, 'ndays'] = (- self.epochdays) + \
                               (data['datetimeobject'].astype(np.int64) / (60 * 60 * 24 * (10 ** 9))).astype(int)

        return data
