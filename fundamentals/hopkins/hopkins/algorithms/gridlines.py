import math

import numpy as np
import pandas as pd


class GridLines:

    def __init__(self, death_rate_max: float, positive_rate_max: float, test_rate_max: float = 0):
        """

        :param death_rate_max:
        :param positive_rate_max:
        :param test_rate_max:
        """

        # The gap between the tick points of an axis
        self.xtick = 5000
        self.ytick = 1000
        self.ztick = 20

        # The maximum value per axis
        self.testlimit = self.xtick + self.xtick * math.ceil(test_rate_max / self.xtick)
        self.positivelimit = self.ytick + self.ytick * math.ceil(positive_rate_max / self.ytick)
        self.deathlimit = self.ztick + self.ztick * math.ceil(death_rate_max / self.ztick)

    def abscissae(self) -> np.ndarray:
        """

        :return: The set of x values of a grid
        """

        return np.arange(0, self.testlimit, self.xtick)

    def ordinates(self):
        """

        :return: A set of y values of a grid
        """

        return np.arange(0, self.positivelimit, self.ytick)

    @staticmethod
    def grid(vector: np.ndarray, gradients: np.ndarray) -> pd.DataFrame:
        """

        :param vector:
        :param gradients
        :return:
        """

        # Calculates the points of a line w.r.t. the set of x values 'abscissae', and a 'gradient' value
        lines = pd.DataFrame()
        for gradient in gradients:
            core = pd.DataFrame(data={'x': vector, 'y': gradient * vector})
            core.loc[:, 'label'] = str(100 * gradient)
            lines = pd.concat([lines, core], ignore_index=True, axis=0)

        return lines

    def ptr(self):
        """

        :return:
        """

        # The latest range of test/100K values
        abscissae = self.abscissae()

        # The gradients of a graph's grid lines: range [0 100]
        gradients = np.concatenate((np.arange(0, 6), np.arange(9, 21, 3), np.arange(20, 110, 20))) / 100
        lines = self.grid(vector=abscissae, gradients=gradients)

        # Renaming fields
        option = lines.rename(columns={'x': 'testRate', 'y': 'positiveRate'})

        # An inexistent state code -> a placeholder
        option.loc[:, 'STUSPS'] = 'ZZ'
        option.loc[:, 'COUNTYGEOID'] = '00000'
        option = option[option['positiveRate'] <= self.positivelimit]

        return option.drop_duplicates(inplace=False)

    def dpr(self):
        """

        :return:
        """

        # The latest range of positive/100K values
        ordinates = self.ordinates()

        # The gradients of a graph's grid lines
        gradients = np.concatenate((np.linspace(start=0, stop=0.3, num=3, endpoint=False),
                                    np.linspace(start=0.25, stop=1, num=3, endpoint=False),
                                    np.arange(1, 10), np.arange(10, 50, 10), np.arange(50, 100, 25)),
                                   axis=0) / 100
        lines = self.grid(vector=ordinates, gradients=gradients)

        # Renaming fields
        option = lines.rename(columns={'x': 'positiveRate', 'y': 'deathRate'})

        # An inexistent state code -> a placeholder
        option.loc[:, 'STUSPS'] = 'ZZ'
        option.loc[:, 'COUNTYGEOID'] = '00000'
        option = option[option['deathRate'] <= self.deathlimit]

        return option.drop_duplicates(inplace=False)
