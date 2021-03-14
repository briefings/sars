import os

import hopkins.base.dearchive
import hopkins.base.directories

import config


class Utilities:

    def __init__(self):
        self.root = os.getcwd()

        self.configurations = config.Config()

    def get(self):

        dearchive = hopkins.base.dearchive.Dearchive(into=self.root)

        dearchive.unzip(
            urlstring='https://github.com/miscellane/cartographs/raw/develop/cartographs.zip')
        dearchive.unzip(
            urlstring='https://github.com/miscellane/candles/raw/develop/candles.zip')
        dearchive.unzip(
            urlstring='https://github.com/briefings/sars/raw/develop/fundamentals/algorithms/algorithms.zip')
        dearchive.unzip(
            urlstring='https://github.com/briefings/sars/raw/develop/fundamentals/populations/populations.zip')

    def exc(self):
        utilities_list = [os.path.join(self.root, directory)
                  for directory in self.configurations.utilities_list]
        hopkins.base.directories.Directories().cleanup(listof=utilities_list)

        self.get()
