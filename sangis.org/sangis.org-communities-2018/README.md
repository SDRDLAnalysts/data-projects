# Community, City and Tract Boundaries in San Diego County


This package combines three SANGIS datasets for communities and cities in San Diego county into a single file, in the ``communities`` resource. The source files are: 

* Municipal boundaries, of incorporated cities and the rest of the county
* Communities in unincorporated county areas
* Communities in San Diego. 

The ``communities`` dataset, has a ``type`` field to distinguish the types of area, which is one of: 

* city
* county_community
* sd_community
* community


The ``tracts_all_regions`` dataset joins tracts into regions by the internal
point. The dataset may have more than one row for each tract; the tract will
appear once for each of the four region types that it is in, but no tract is in
more than 2 regions. For instance, a tract in a community of San Diego will appear twice, once for the community, and once for the City. 

The ``tracts`` dataset also joins tracts to regions, but has two sets of
columns, for city and community. This dataset includes every tract in the
county, and each appears only once. If a tract is included in both a city and
community, then there is a name and code for both the city columns and the
community columns. Regions not in a city have a city value of "County" and a
city code of "CN".


