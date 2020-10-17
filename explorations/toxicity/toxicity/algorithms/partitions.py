import dask
import os
import json

import pandas as pd

import config


class Partitions:

    def __init__(self, gazetteer: pd.DataFrame):
        
        self.gazetteer = gazetteer
        self.rename = {'deathRate': 'x', 'risk': 'y', 'COUNTY': 'name'}
        self.select = ['STATE', 'x', 'y', 'name', 'STUSPS']        

        # Configurations
        configurations = config.Config()
        self.warehouse = configurations.warehouse

    def read(self, source: str):

        try:
            data = pd.read_csv(
                filepath_or_buffer=source, header=0, encoding='utf-8',
                usecols=['STUSPS', 'COUNTYGEOID', 'deathRate', 'risk', 'label'],
                dtype={'STUSPS': str, 'COUNTYGEOID': str, 'deathRate': float, 'risk': float, 'label': str}
            )
        except OSError as err:
            raise err

        data = data.merge(self.gazetteer, how='left', on=['STUSPS', 'COUNTYGEOID'])
        data.rename(columns=self.rename, inplace=True)

        return data

    @dask.delayed
    def create(self, stusps: str, state: str, label: str, excerpt):

        segment = excerpt[excerpt['STUSPS'] == stusps]
        dictionary = segment.drop(columns=['STUSPS', 'STATE', 'label']).to_dict(orient='records')

        return {'hazard': label, 'name': state, 'data': dictionary}

    @staticmethod
    def write(target: str, data):
        """
        if os.path.isfile(target):
            os.remove(target)

        :param target: The name of the file that 'data' will be written into
        :param data: The data that would be written into a file
        :return:
        """

        with open(target, 'w') as disk:
            json.dump(data, disk)

    def exc(self, source: str, directory: str):

        frame = self.read(source=source)
        labels = frame.label.unique()

        for label in labels:

            excerpt = frame[frame.label == label]

            # Create the partitions that would exist within a JSON file
            partitions = []
            for stusps, state in excerpt[['STUSPS', 'STATE']].drop_duplicates().values:
                partitions.append(self.create(stusps=stusps, state=state, label=label, excerpt=excerpt))

            dask.visualize(partitions, filename='partitions', format='pdf')
            data = dask.compute(partitions, scheduler='processes')[0]

            # Write the partitions ...
            self.write(target=os.path.join(directory, label + '.json'), data=data)
