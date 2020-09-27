import os

import atlantic.base.dearchive
import atlantic.base.directories


class Utilities:

    def __init__(self):
        self.root = os.getcwd()

    def get(self):
        dearchive = atlantic.base.dearchive.Dearchive(into=self.root)
        dearchive.exc(urlstring='https://github.com/miscellane/cartographs/raw/develop/cartographs.zip')
        dearchive.exc(urlstring='https://github.com/miscellane/candles/raw/develop/candles.zip')
        dearchive.exc(
            urlstring='https://github.com/briefings/sars/raw/develop/fundamentals/algorithms/algorithms.zip')
        dearchive.exc(
            urlstring='https://github.com/briefings/sars/raw/develop/fundamentals/populations/populations.zip')

    def exc(self):
        listof = [os.path.join(self.root, directory)
                  for directory in ['cartographs', 'candles', 'populations', 'states']]
        atlantic.base.directories.Directories().cleanup(listof=listof)

        self.get()
