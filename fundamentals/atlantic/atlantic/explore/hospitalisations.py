import logging
import os

import numpy as np
import pandas as pd

import atlantic.gridlines.hpg
import config


class Hospitalisations:

    def __init__(self, blob: pd.DataFrame, warehouse: str):
        self.prose = '... the poor mechanic might have so accustomed his ear to good teaching, as to have ' \
                     'discerned between faithful teachers and false. But now, with a most inhuman cruelty, ' \
                     'they who have put out the people’s eyes, reproach them of their blindness ... ' \
                     'https://oll.libertyfund.org/titles/milton-the-prose-works-of-john-milton-vol-1'

        self.blob = blob

        self.warehouse = warehouse

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def places(self, limit: int):

        tensor = self.blob.copy()
        tensor.loc[:, 'marker'] = np.where(tensor['hospitalizedIncrease'] > 0, 1, 0)

        # tensor = pd.concat([tensor,
        #                     pd.DataFrame(data={'marker': np.where(tensor['hospitalizedIncrease'] > 0, 1, 0)})],
        #                    ignore_index=False, axis=1)

        usable = tensor[['STUSPS', 'marker']].groupby(by='STUSPS').sum()
        usable = usable[usable['marker'] > limit]
        usable.reset_index(drop=False, inplace=True)

        return usable

    def curves(self, data: pd.DataFrame):

        frame = data[['datetimeobject', 'STUSPS', 'hospitalizedRate', 'positiveRate', 'ndays']]
        gridlines = atlantic.gridlines.hpg.HPG(hospitalized_rate_max=frame['hospitalizedRate'].max(),
                                               positive_rate_max=frame['positiveRate'].max()).exc()

        instances = pd.concat([frame, gridlines], axis=0, ignore_index=True)
        instances.to_csv(path_or_buf=os.path.join(self.warehouse, 'curvesHospitalizedPositives.csv'),
                         header=True, index=False, encoding='utf-8')

        return frame

    def exc(self, limit: int):

        usable = self.places(limit=limit)

        data = self.blob.copy()
        data = data.merge(usable[['STUSPS']], how='right', on=['STUSPS'])

        frame = self.curves(data=data)
        frame = pd.concat([frame,
                   pd.DataFrame(data={'hospitalizedPositiveRate':
                                          np.where(frame['positiveRate'] > 0,
                                                   100 * frame['hospitalizedRate'] / frame['positiveRate'], 0)})],
                  ignore_index=False, axis=1)

        self.logger.info('\nHospitalized Positives Curves:\n{}\n'.format(frame.info()))

        return frame
