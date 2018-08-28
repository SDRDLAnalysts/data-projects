


def combine_references(resource, doc, env, *args, **kwargs):
    """ An example row generator function.

    Reference this function in a Metatab file as the value of a Datafile:

            Datafile: python:pylib#row_generator

    The function must yield rows, with the first being headers, and subsequenct rows being data.

    :param resource: The Datafile term being processed
    :param doc: The Metatab document that contains the term being processed
    :param args: Positional arguments passed to the generator
    :param kwargs: Keyword arguments passed to the generator
    :return:


    The env argument is a dict with these environmental keys:

    * CACHE_DIR
    * RESOURCE_NAME
    * RESOLVED_URL
    * WORKING_DIR
    * METATAB_DOC
    * METATAB_WORKING_DIR
    * METATAB_PACKAGE

    It also contains key/valu pairs for all of the properties of the resource.

    """


    for n, ref in enumerate(doc.references()):
        for i, row in enumerate(ref):
            if i==0:
                if n==0:
                    yield row
                else:
                    continue; # only yield the first header row
            else:
                yield row
                
                
def strip_junk(v):
    """ Strip junk from many of the columns
    """

    nv =  v.replace('=','').replace('"','').replace(',','').replace('$','')
    return nv
    
def parse_date(v):
    from datetime import date
    m,d,y = strip_junk(v).split('/')
    
    return date(int(y),int(m),int(d))
    
def clean_integer(v):
    from rowgenerators.valuetype.core import robust_int
    
    s = strip_junk(v)
    
    r = robust_int(s)

    return r
    