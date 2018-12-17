""" Example pylib functions"""

def combine_sources(resource, doc, env, *args, **kwargs):
    """Combine all of the sources. Conveniently, the first one has the header, 
    and none of the remaining do, so we can durectly yield everything """
    
    headers = None
    
    for ref in doc.references():
        

        for row in ref:
            
            if  any( e.strip() for e in row):
                row =  [ref.name]+row
                
                if not headers:
                    headers = row
                
                if len(row) < len(headers):
                    row += [None]*(len(headers)-len(row))
                 
                    
                yield row
                
        

def row_generator(resource, doc, env, *args, **kwargs):
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

    from collections import OrderedDict
    import hashlib
    from dateutil import tz
    from datetime import datetime
    import time
    import uuid

    s = doc.resource('source')
    
    sessions = {}
    
    def proto():
        return OrderedDict(
            [
                ('type', None),
                ('location', None), 
                ('time_in', None),
                ('time_out', None),
                ('duration', None),
                ('feeling_in', None),
                ('feeling_out', None),
                ('first_name', None),
                ('last_name', None),
                ('full_name', None),
                ('name_key', None),
                ('user_id', None),
                ('session_id', None),
                ('checkin_row_no', None),
                ('checkout_row_no', None),
            ]
        )
    
    def convt_date(utc_datetime):
    
        now_timestamp = time.time()
        offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
        return utc_datetime + offset
    

    yield proto().keys()

    for i, row in enumerate(s.iterdict,1):
           
        if not row['location']:
            row['location'] = 'NA'
            continue
            
        person = '{}-{}-{}'.format(row['first_name'], row['last_name'], str(row['birthdate'] or 'na'))
            
        user_id = hashlib.md5(person.encode('utf8')).hexdigest()
            
        t = None
        typ = '?'
        feel = None
            
        if row['checkin_timestamp']:
            t = convt_date(row['checkin_timestamp'])
            key = user_id+'-'+str(t.date())
            typ = 'i'
            feel = row['checkin_feeling']
            
        if row['checkout_timestamp']:
            t = convt_date(row['checkout_timestamp'])
            key = user_id+'-'+str(t.date())
            typ = 'o'
            feel = row['checkout_feeling']
            
        if not key in sessions:
            sessions[key] = []
            
        row.update({'time': t,'name_key': person, 'user_id': user_id, 'key': key, 
                    'type':typ , 'feel': feel, 'row':i})
        
        sessions[key].append(row)
     

    def first_in(entries):
        for e in entries:
            if e['type'] == 'i':
                return e

    def last_out(entries):
        for e in reversed(entries):
            if e['type'] == 'o':
                return e
                            
    for i, (k,v) in enumerate(sessions.items()):
        
        e_in = first_in(v)
        e_out = last_out(v)
        
        if e_in and e_out:
            typ = 'c' # full check in  / check out
        elif e_in:
            typ = 'i' # only the check in 
        else:
            typ = 'o' # only the check out
        
        
        p = proto()
        
        for k in p.keys():
            p[k] = v[0].get(k) # copy the first item's values
            
        if e_in:
            p['time_in'] = e_in['time']
            p['feeling_in'] = e_in['feel']
            p['checkin_row_no'] = e_in['row']
                        
        if e_out:
            p['time_out'] = e_out['time']
            p['feeling_out'] = e_out['feel']
            p['checkout_row_no'] = e_out['row']
            
        if typ == 'c':
            # Convert to Unix timestamp
            d1_ts = time.mktime(p['time_in'].timetuple())
            d2_ts = time.mktime(p['time_out'].timetuple())
            
            p['duration'] = round( (d2_ts - d1_ts)/60, 0)
        
        p['type'] = typ
        p['session_id'] = uuid.uuid4()
            
        yield list(p.values())
        

