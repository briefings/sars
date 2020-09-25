import dask
import numpy as np
import pandas as pd
import statsmodels.regression.linear_model
from statsmodels.formula.api import ols


class Secants:

    def __init__(self, periods: np.ndarray, formula: str, regressor: str):

        self.periods = periods
        self.formula = formula
        self.regressor = regressor

    @dask.delayed
    def get_predictions(self, models, points: pd.DataFrame):

        data = pd.DataFrame()
        for key in np.arange(len(models)):

            model = models[key][0]
            period = models[key][1]

            values = model.predict(points)
            values.rename('{}d'.format(period), inplace=True)
            values = pd.concat([points, values], axis=1)
            values.set_index(keys=[self.regressor], drop=True, inplace=True)

            if data.empty:
                data = values
            else:
                data = pd.concat([data, values], axis=1, ignore_index=False)

        return data

    @dask.delayed
    def get_estimates(self, models):

        data = []
        for key in np.arange(len(models)):
            # A model and its corresponding period value
            model = models[key][0]
            period = models[key][1]

            # The confidence interval values of the independent variable
            interval = model.conf_int().loc[self.regressor].values

            # Hence
            data.append(['{}d'.format(period), model.params[self.regressor], interval[0], interval[1],
                         model.pvalues[self.regressor], model.rsquared, model.params.Intercept])

        estimates = pd.DataFrame(data=data,
                                 columns=['period', 'gradient', 'lowerconfidenceinterval', 'upperconfidenceinterval',
                                          'pvalue', 'rsquared', 'intercept'])

        return estimates

    @dask.delayed
    def get_models(self, blob: pd.DataFrame):

        models = []
        for period in self.periods:
            data = blob.iloc[-period:, :]
            model: statsmodels.regression.linear_model.RegressionResultsWrapper = ols(self.formula, data).fit()
            models.append([model, period])

        return models
