# San Diego Tract Estimates for Diabetes

This dataset provides estimates for the diabetes rate, per tract, for tracts in
San Diego county. The diabetes rates are calculated from CHIS microdata, for
the pooled years 2015, 2016 and 2017, for the whole state of California,
segmented by race, age group, . Then, the California rates are applied to the
population estimates from ACS table B17001, poverty status by sex by age.

 The dataset combines two upstream packages, both of which are primarily impemented in a single Jupyter notebook:
 
 * CHIS estimates: [healthpolicy.ucla.edu-chis-adult-1#rasp_diabetes](https://github.com/sandiegodata-projects/data-projects/blob/master/chis/healthpolicy.ucla.edu-chis/notebooks/DiabetesProbabilities.ipynb)
 * ACS Tract estimates: [sandiegodata.org-rasp-1#rasp_tracts_sd](https://github.com/sandiegodata-projects/data-projects/blob/master/sandiegodata.org/sandiegodata.org-rasp/notebooks/RaspTracts.ipynb)
 
 The final dataset is also [implemented in a single Jupyter notebook.](https://github.com/sandiegodata-projects/data-projects/blob/master/sandiegodata.org/sandiegodata.org-diabetes_sae/notebooks/Diabetes.ipynb) 