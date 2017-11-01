
from appurl import parse_app_url
from itertools import islice

def extract(resource, doc, *args, **kwargs):
    """Combine all of the references into a single fine"""

    prefix = resource.prefix

    table = resource.row_processor_table()

    yield table.headers

    parser = table.make_fw_row_parser()

    print("!!!!", prefix)

    for r in doc.references():

        print("Processing ",r.name)

        t = parse_app_url(r.url).get_resource().get_target()

        with open(t.path, 'rU') as f:
            for line in f.readlines():
                
                if not line.startswith(prefix):
                    continue 
                
                yield parser(line)