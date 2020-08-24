import math

import numpy as np
import pandas as pd


class GridLines:

    def __init__(self, blob):
        self.blob = blob

        # The gap between the tick points of an axis
        self.xtick = 5000
        self.ytick = 1000

        # The maximum value per axis
        self.xlimit = self.xtick + self.xtick * math.ceil(blob.testRate.max() / self.xtick)
        self.ylimit = self.ytick + self.ytick * math.ceil(blob.positiveRate.max() / self.ytick)

        # The gradients of a graph's grid lines
        self.gradients = np.concatenate((np.arange(0, 6), np.arange(9, 21, 3), np.arange(20, 110, 20))) / 100

    def abscissae(self):
        """

        :return: The set of x values of a grid
        """

        return np.arange(0, self.xlimit, self.xtick)

    def grid(self, abscissae):
        # Calculates the points of a line w.r.t. the set of x values 'abscissae', and a 'gradient' value
        lines = pd.DataFrame()
        for gradient in self.gradients:
            core = pd.DataFrame(data={'x': abscissae, 'y': gradient * abscissae})
            core.loc[:, 'label'] = int(100 * gradient)
            lines = pd.concat([lines, core], ignore_index=True, axis=0)

        # Renaming fields
        option = lines.rename(columns={'x': 'testRate', 'y': 'positiveRate'})

        # An inexistent state code -> a placeholder
        option.loc[:, 'STUSPS'] = 'ZZ'

        return option[option.positiveRate <= self.ylimit]

    def exc(self):
        abscissae = self.abscissae()
        grid = self.grid(abscissae=abscissae)

        return pd.concat([self.blob, grid], axis=0, ignore_index=True)
