#
# Functions to replace values in a tract with the median of the value of the tracts neighbors


##
## Replace missing values in the mean home value column with the mean of the means of the four neirest neighbors
## The mean here should be weighted, but is not.
## A better way to calculate this would be to sum the aggregate values and number of homeowners of the
## neighbored, and then calculate the average.
##
import itertools
import numpy as np



from math import radians, cos, sin, asin, sqrt
# From https://stackoverflow.com/a/4913653
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).

    Returns distances in km.
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

# Vectorized for numpy
# From https://stackoverflow.com/a/29546836
def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c
    return km

def neighbors(geoid, positions, n=4):
    """Select n neighbors got the given geoids

    positions is a dict of geoid->(lat, lon)

    """
    try:
        tlat, tlon = positions[geoid]
    except KeyError: # Fails for 14000US06073990100
        return []

    dists = []

    for g, (lat, lon) in positions.items():
        if g == geoid:
            continue
        d = haversine(tlon, tlat, lon, lat)

        dists.append( (d, g) )

    neighbors = itertools.islice(( sorted(dists)), n)

    return [ e[1] for e in neighbors]


