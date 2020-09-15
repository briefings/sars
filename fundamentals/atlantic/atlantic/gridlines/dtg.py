import pandas as pd
import numpy as np
import math


class DTG:

    """
    The grid of Deaths/100K Tests/100K Graphs
    """

    def __init__(self, death_rate_max: float, test_rate_max: float):

        # The gap between the tick points of an axis
        self.xtick = 5000
        self.ytick = 20

        # The maximum value per axis
        self.ylimit = self.ytick + self.ytick * math.ceil(death_rate_max / self.ytick)
        self.xlimit = self.xtick + self.xtick * math.ceil(test_rate_max / self.xtick)

        # The gradients of a graph's grid lines
        self.gradients = np.concatenate((np.linspace(start=0, stop=0.3, num=3, endpoint=False),
                                         np.linspace(start=0.25, stop=1, num=3, endpoint=False),
                                         np.arange(1, 6), np.arange(7, 17, 4)),
                                        axis=0) / 100

    def abscissae(self) -> np.ndarray:
        """

        :return: The set of x values of a grid
        """

        return np.concatenate((np.arange(0, self.xtick, 1000),
                               np.arange(self.xtick, self.xlimit, self.xtick)), axis=0)

    def grid(self, abscissae: np.ndarray) -> pd.DataFrame:

        # Calculates the points of a line w.r.t. the set of x values 'abscissae', and a 'gradient' value
        lines = pd.DataFrame()
        for gradient in self.gradients:
            core = pd.DataFrame(data={'x': abscissae, 'y': gradient * abscissae})
            core.loc[:, 'label'] = str(100 * gradient)
            lines = pd.concat([lines, core], ignore_index=True, axis=0)

        # Renaming fields
        option = lines.rename(columns={'x': 'testRate', 'y': 'deathRate'})

        # An inexistent state code -> a placeholder
        option.loc[:, 'STUSPS'] = 'ZZ'
        option = option[option['deathRate'] <= self.ylimit]

        return option.drop_duplicates(inplace=False)

    def exc(self):
        abscissae = self.abscissae()
        grid = self.grid(abscissae=abscissae)

        return grid
