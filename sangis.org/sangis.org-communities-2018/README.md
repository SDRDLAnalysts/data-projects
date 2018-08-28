# Community and City Boundaries in San Diego County

This package combines three SANGIS datasets for communities and cities in San Diego county into a single file, in the ``communities`` resource. The source files are: 

* Municipal bopundaries, of incorporated cities and the rest of the county
* Communities in unincorporated county areas
* Communities in San Diego. 

The final resource, ``communities``, has a ``type`` field to distinguish the types of area, which is one of: 

* city
* county_community
* sd_community
