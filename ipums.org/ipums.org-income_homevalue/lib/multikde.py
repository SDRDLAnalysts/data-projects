
import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.nonparametric.kde import KDEUnivariate

class MultiKde(object):
    """Divide the data df in to groups on the colum group_col, then create multiple kdes
    of the values in value_col. These KDEs can be used to make synthetic distributions
    with a specified median """

    def __init__(self, df, group_col, value_col):

        self.value_col = value_col

        self.groups = df.groupby(df[group_col].astype(int))

        self.medians = [np.median(g) for n, g in self.groups[value_col] ]


    def get_group(self, target):
        """Return the index of the first income group that has a median larger than the parameter. """
        try:
            return next(i for i, v in enumerate(self.medians) if v > target)
        except StopIteration:
            return len(self.medians)-1


    def make_kde(self, median_income):

        group_n = self.get_group(median_income)
        
        g = self.groups.get_group(group_n)[self.value_col].astype(float)

        group_median = np.median(g)


        # Shift the supports to match the input median
        diff = group_median - median_income

        dens = KDEUnivariate(g - diff)
        dens.fit()

        zi = np.argmax(dens.support > 0)  # Zero index, removes part of icdf below 0
        s, d = dens.support[zi:], dens.density[zi:]

        gridsize = len(s)
        icdf = pd.Series(stats.mstats.mquantiles(g - diff, np.linspace(0, 1, gridsize)))

        return pd.Series(s), pd.Series(d), icdf, g


    def income_icdf(self, median_income):
        s, d, icdf, g = self.make_kde(median_income)

        return icdf


    def syn_dist(self, median_income, n, lower_threshold=0):
        s, d, icdf, g = self.make_kde(median_income)

        v =  pd.Series(np.random.choice(icdf, n))

        # Remove values below the lower threshold by randomly selecting another value that is larger than
        # the threshold
        gtz = v[v>lower_threshold]

        return v.where(v>lower_threshold, np.random.choice(gtz,1))
