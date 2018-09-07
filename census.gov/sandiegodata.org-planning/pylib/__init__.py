


def extract_sandiego(resource, doc, env, *args, **kwargs):
    from geoid.acs import Tract
    
    r = doc.reference('census_planning_database') 

    for i, row in enumerate(r.iterdict):
        
        
        state = row['State']
        county = row['County']
        tract = row['Tract']
        
        del row['GIDTR']
        del row['State']
        del row['County']
        del row['State_name']
        del row['County_name']
        del row['Tract']
        del row['Num_BGs_in_Tract']
        
        if i == 0:
            yield ['geoid'] + list(row.keys())
        elif  state == '06' and  county == '073':
            tract = Tract(state, county, tract )
            yield [str(tract)] + list(row.values())
    
def make_tract_community(df, comm ):
    import geopandas
    # link a community record to each tract

    _1 = df[['geoid','geometry']].copy()
    _1['rep_p'] = _1.representative_point()
    df_rep = geopandas.GeoDataFrame(_1, geometry='rep_p')

    tract_community = geopandas.sjoin(comm, df_rep, op='contains')[['geoid','type','name','code']] # Probably will be slow
    tract_community.columns = ['geoid','region_type','region_name','region_code']

    # Check that the number of tracts in the City of San Diego equals the number
    # of tracts in communities in San Diego
    assert(tract_community.region_type.value_counts().loc['sd_community'] ==
           len(tract_community[tract_community.region_code == 'SD']) )

    # Add a code for the city, if the tract is in a city
    tract_community['city'] = tract_community.region_code.where(tract_community.region_type == 'city')

    # All of the sd_communities are in San Diego
    tract_community.loc[tract_community.region_type=='sd_community','city'] = 'SD'

    # Now we don't need the San Diego city tracts. 
    tract_community = tract_community[ ~(tract_community.region_code == 'SD')]

    # Get rid of the last of the dupes
    tract_community = tract_community[ ~(tract_community.city == 'CN')].sort_values('region_type') # # So duplicated() removes the county community, not a city

    tract_community = tract_community[ ~tract_community.duplicated('geoid',keep='first')]

    return tract_community.set_index('geoid')
    
def generate_geo(resource, doc, env, *args, **kwargs):
    from metapack.rowgenerator import PandasDataframeSource
    from metapack import get_cache

    r = doc.resource('sandiego_planning_db').dataframe().set_index('geoid')

    tracts =  doc.reference('tract_boundaries').geoframe()

    comm = doc.reference('communities').geoframe()
    

    sdg = tracts[['geometry']].join(r,how='right')

    # move geometry to the end
    new_cols = list(sdg.columns)[1:] + [list(sdg.columns)[0]]

    sdg = sdg[new_cols]

    tc = make_tract_community(sdg.reset_index(), comm)

    final = sdg.join(tc)
    
    yield from PandasDataframeSource('<df>',final , get_cache())

