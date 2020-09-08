import pandas as pd
import logging


class Tests:

    def __init__(self, blob: pd.DataFrame):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.blob = blob

    @staticmethod
    def addition(x, y):
        return x + y

    def exc(self):

        data = self.blob.copy()
        data.loc[:, 'testIncrease'] = self.addition(data['positiveIncrease'], data['negativeIncrease'])
        data.loc[:, 'testCumulative'] = self.addition(data['positiveCumulative'], data['negativeCumulative'])

        self.logger.info('\n{}\n'.format(data.head()))

        return data


