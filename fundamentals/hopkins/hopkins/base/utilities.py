import os

import hopkins.base.dearchive
import hopkins.base.directories


class Utilities:

    def __init__(self):
        self.root = os.getcwd()

    def get(self):
        dearchive = hopkins.base.dearchive.Dearchive(into=self.root)
        dearchive.unzip(urlstring='https://github.com/miscellane/cartographs/raw/develop/cartographs.zip')
        dearchive.unzip(urlstring='https://github.com/miscellane/candles/raw/develop/candles.zip')
        dearchive.unzip(
            urlstring='https://github.com/briefings/sars/raw/develop/fundamentals/populations/populations.zip')

    def exc(self):
        listof = [os.path.join(self.root, directory)
                  for directory in ['cartographs', 'candles', 'populations', 'states', 'counties']]
        hopkins.base.directories.Directories().cleanup(listof=listof)

        self.get()
