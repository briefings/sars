import numpy as np


class Inspect:

    def __init__(self):

        self.name = ''

    @staticmethod
    def sequencing(points: np.ndarray):

        series = points
        indices: np.ndarray = (series[1:] < series[:-1])

        while indices.sum() > 0:
            series[:-1][indices] = series[1:][indices]
            indices = series[1:] < series[:-1]

        return series
