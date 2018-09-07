


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

    _1 = df[['geoid','geometry']].copy()
    _1['rep_p'] = _1.representative_point()
    df_rep = gpd.GeoDataFrame(_1, geometry='rep_p')

    tract_community = gpd.sjoin(comm, df_rep, op='contains')[['geoid','type','name','code']] # Probably will be slow
    tract_community.columns = ['geoid','region_type','region_name','region_code']

    priority_map = {
        'sd_community' : 1,
        'city': 2,
        'county_community': 3
    }

    tract_community['priority'] = tract_community['region_type'].apply(lambda v: priority_map[v])

    tract_community.sort_values(['geoid','priority'], inplace=True)
    tract_community = tract_community[~tract_community.duplicated('geoid',keep='first')].set_index('geoid')

    tract_community['region_type'] = tract_community.region_type.where(tract_community.region_code != 'CN', 'county')

    return tract_community.drop('priority', axis=1)
    
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

