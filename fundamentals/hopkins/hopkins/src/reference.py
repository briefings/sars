import numpy as np
import pandas as pd

import config


class Reference:

    def __init__(self, days: pd.DataFrame, gazetter: pd.DataFrame):
        configurations = config.Config()
        self.datestring = configurations.datestring

        self.days = days
        self.gazetteer = gazetter

    def baseline(self):
        # Let each row represent a distinct county, whilst each column represents a distinct day
        matrix = np.zeros((self.gazetteer.shape[0], self.days.shape[0]))

        # Hence, in 'nils' each row represents a distinct county, whilst each column represents a distinct day
        nils = pd.DataFrame(data=matrix, columns=self.days[self.datestring].values)

        # A table matrix of gazetteer fields and days
        blueprint = pd.concat((self.gazetteer, nils), axis=1, ignore_index=False)

        # Melting ...
        reference = blueprint.melt(id_vars=self.gazetteer.columns,
                                   value_vars=self.days[self.datestring].values,
                                   var_name='date',
                                   value_name='zeros')

        return reference.drop(columns=['zeros'], inplace=False)

    def exc(self):
        # The latest expected combinations of a county & dates alongside
        #       county & state geographic codes,
        #       county population estimates
        baseline = self.baseline()

        # Prepending self.days, hence a few date format/object options are available
        baseline = self.days.merge(baseline, how='right', on=self.datestring)

        return baseline
