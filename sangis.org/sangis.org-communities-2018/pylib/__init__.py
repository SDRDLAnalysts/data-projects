

def generate_boundaries(resource, doc, env, *args, **kwargs):
    
    
    yield 'type name code geometry'.split()
    
    
    for row in doc.resource('cities').iterrows:  
        yield ['city', row.name, row.code, row.geometry]
    
    for row in doc.resource('sd_communities').iterrows:  
        yield ['sd_community', row.cpname, row.cpcode, row.geometry]
        
    for row in doc.resource('county_communities').iterrows:  
        yield ['county_community', row.cpasg_labe, row.code, row.geometry]
    
    