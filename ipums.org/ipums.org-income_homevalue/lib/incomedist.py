#
# Functions for processing income and home value distributions.

import numpy as np
import pandas as pd
import seaborn as sns;

sns.set(color_codes=True)

from statsmodels.nonparametric.kde import KDEUnivariate
from scipy import stats
from itertools import chain

def sum_densities(s, d):
    """Cheap integration for the supports and densities. """
    sm = 0
    for i in range(0, len(s) - 1):
        w = s[i + 1] - s[i]
        sm += d[i] * w

    return float(sm.astype(float))


def integrate(s, d, l, u):
    """Cheap integration for the supports and densities. """
    sm = 0
    for i in range(l, u):
        w = s[i + 1] - s[i]
        sm += d[i] * w

    return float(sm.astype(float))


def integrate_until(s, d, l, t):
    """Integrate s and d from index l until the total is greater than t.
    Returns the final index, or None if the sum could not be reached"""

    sm = 0
    for i in range(l, len(s) - 1):
        w = s[i + 1] - s[i]
        sm += d[i] * w

        if sm > t:
            return i

    return None


def minmax_densities(s, d):
    last = None
    v = []
    for i, (si, di) in enumerate(zip(s, d)):
        if last is not None:
            w = si - last
            d = di * w

            if d != 0:
                v.append(d)

        last = si

    return min(v), max(v)


def resample(d, range25):
    """Interpolate a densities series to resize it to 25% of the total range"""
    ep = range25 / len(d)
    c = 0
    dp = []

    for i, dens in enumerate(d):
        c += ep
        if c > 1.0:
            c -= 1.0

            dp.append(dens)

            while c > 1.0:
                dp.append(np.nan)
                c -= 1.0
        else:
            pass

    return pd.Series(dp).interpolate()


def prepare_icdf(s, d):
    """Prepare an initial value of DP, which only has to be done once for particular values of s and d"""

    assert len(s) % 4 == 0

    range25 = int(len(s) / 4)  # width of each 25% of the final supports array

    b1 = integrate_until(s, d, 0, .25)
    b2 = integrate_until(s, d, b1, .25)
    b3 = integrate_until(s, d, b2, .25)

    # Breaks the supposrts and densities into quarters, and we'll reassign the support values at
    # at the break points and interpolate
    d0 = d[0:b1]
    d1 = d[b1:b2]
    d2 = d[b2:b3]
    d3 = d[b3:]
    assert (len(d0) + len(d1) + len(d2) + len(d3)) == len(s)

    # Resample all of the density segments
    dp0 = resample(d0, range25)
    dp1 = resample(d1, range25)
    dp2 = resample(d2, range25)
    dp3 = resample(d3, range25)

    dp = list(chain(dp0, dp1, dp2, dp3))

    # Resampling sometimes drops one or two samples
    dp += [dp[-1]] * (4 - (len(dp) % 4))  # get back to evenly divisable by 4

    return dp


def interpolate_curve(s, d, pctl25, pctl50, pctl75, idp=None):
    """ Stretch a prototype distribution to have given values for the 25 50 and 75th percentiles. """

    assert len(s) % 4 == 0

    range25 = int(len(s) / 4)  # width of each 25% of the final supports array


    if idp is None:
        idp = prepare_icdf(s, d)

    # The supports are interpolated linearly between the breakpoints

    sp = np.concatenate(
        (np.linspace(0, pctl25, range25),
         np.linspace(pctl25, pctl50, range25),
         np.linspace(pctl50, pctl75, range25),
         np.linspace(pctl75, max(s), range25))
    )
    sp = pd.Series(sp)
    dp = pd.Series(idp) / sum_densities(sp, idp)

    # Last ditch attempt to get the median exactly right.
    v = make_icdf(sp, dp)
    diff = pctl50 - np.median(v)

    return sp + diff, dp



def make_prototype(v, min_val=0, max_value=150000):
    """Make a prototype KDE and return the support and density

    Removes the parts of the suports below zero and above a maxium, and creates a linear segment
    in the curve from zero to a min value
    """

    # Using a set of values ( prices, incomes ) in v, produce supports and density
    # series using a KDE.

    # The min_val is the lowest price for which draw the distribution from PUMS. This smooths out the
    # distribution and prevents suprious large densities for dollar values where there really are no
    # homes or incomes.

    dens = KDEUnivariate(v)
    dens.fit()

    zi = np.argmax(dens.support > 0)  # Zero index, removes part of icdf below 0
    mv = np.argmax(dens.support > min_val)  # A minimum value, to smooth over left-side weirdness
    ub = np.argmax(dens.support >= max_value)  #

    # The span must be evenly divisible by 4, so we can neatly break it into 25% percentile ranges. We'll just take
    # that off the top end

    # Sum over the density
    width = (np.max(dens.support) - np.min(dens.support)) / len(dens.support)
    i1 = sum(dens.density * width)

    # Integrating over the density should also return 1
    from scipy import integrate
    i2, _ = integrate.quad(dens.evaluate, np.min(dens.support), np.max(dens.support))

    assert np.isclose(i1, i2, atol=1e-4)
    assert np.isclose(i1, 1.0, atol=1e-4)
    assert np.isclose(i2, 1.0, atol=1e-4)

    # Integrate the restricted range and
    # adjust the range back to 1

    d = dens.density
    d[zi:mv] = np.linspace(0, d[mv], mv - zi)

    ub -= (ub - zi) % 4

    s = pd.Series(dens.support[zi:ub])

    norm = True

    if norm:
        d = pd.Series(d[zi:ub] / sum(d[zi:ub] * width))
        assert np.isclose(sum_densities(s, d), 1.0, rtol=1e-4, atol=1e-4), sum_densities(s, d)
    else:
        d = pd.Series(d[zi:ub])

    return s, d


def make_icdf(s, d, n=10000):
    """Create a icdf from the supports and densities.  The routine makes multiple interations through
    all of the supports, randomly adding the support to the output with a probability proportional
    to the density at that support. """

    from random import shuffle

    # This factor increases the probability that a support will be selected, decreasing the
    # number of iterations. It is important to have multiple iterations.
    f = 800

    points = []
    last = None

    # Pre-calc the widths so we can shuffle later.
    for  si, di in zip(s, d):
        if last is not None:
            w = si - last
            try:
                a = di * w * f  # Area of the slice = relative number of values at this support
            except Exception as e:
                print('Erorr in make_icdf for: ', di, w)
                raise

            points.append(( si, di, a))

        last = si

    v = []
    e = 0

    i = 0

    while len(v) < n:
        #shuffle(points)

        for  si, di, a in points:
                e += a

                if e > 1.:
                    e -= 1.
                    v.append(si)

                    if len(v) >= n:
                        break

        i += 1

        if i > 20:
            raise Exception("Too Many Iterations len="+str(len(v)))

    v = sorted(v)
    return v


def syn_dist(s, d, n, lower_threshold=False):
    """Return a synthetic distribution of n samples"""

    icdf = make_icdf(s, d)

    v = pd.Series(np.random.choice(icdf, n))

    # Remove values below the lower threshold by randomly selecting another value that is larger than
    # the threshold
    if lower_threshold is not False:
        #gtz = np.random.choice(v[v > lower_threshold], len(v))

        return v.where(v > lower_threshold, lower_threshold)
    else:
        return v
