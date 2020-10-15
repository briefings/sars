import numpy as np
import pandas as pd


class Risks:

    def __init__(self):
        """
        Constructor
        """

        self.usecols = ['STUSPS', 'COUNTYGEOID', 'Population', 'riskType', 'risk']
        self.dtype = {'STUSPS': str, 'COUNTYGEOID': str, 'Population': np.int, 'riskType': str, 'risk': np.float}

        self.url = 'https://raw.githubusercontent.com/briefings/sars/develop/fundamentals/risks/warehouse/risk/'

    def read(self, file: str):
        """
        Reads-in the clean risk data

        :param file: The name of a file, including its extension.
        :return:
        """

        try:
            data = pd.read_csv(filepath_or_buffer=(self.url + file), header=0, usecols=self.usecols,
                               dtype=self.dtype, encoding='utf-8')
        except OSError as err:
            raise err

        return data

    @staticmethod
    def labels(blob: pd.DataFrame, pattern: str):
        """
        Prepares the label of each record via the text of the 'riskType' field

        :param blob: A risk data table
        :param pattern: The pattern that should be removed from a formatted 'riskType' text
        :return:
        """

        data = blob

        data.loc[:, 'class'] = data['riskType'].str.replace(
            pat=r'[^a-zA-Z0-9]', repl='', regex=True).str.upper()
        data.loc[:, 'label'] = data['class'].str.replace(
            pat=pattern, repl='', regex=True)

        data.drop(columns=['class', 'riskType'], inplace=True)

        return data

    def exc(self, file: str, pattern: str):
        """

        :param file: The name of a file, including its extension.
        :param pattern: The pattern that has to be removed from a formatted 'riskType' field. The
                        'riskType' field is one of the fields of the risk data read by self.read()
        :return:
        """

        data = self.read(file=file)
        data = self.labels(blob=data, pattern=pattern)

        return data
