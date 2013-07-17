'''

'''

from  databundles.bundle import BuildBundle
import os

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        self.array_cache = {}

    
    def get_array_by_type(self, aa, name):
        '''Cache lookups of analysis areaa arrays'''

        if name not in self.array_cache:
            a = aa.new_array()
            self.array_cache[name] = a
            return a 
        else:
            return self.array_cache[name]

    def get_incident_source(self):
        """Return the partition from the dataset that has the incidents"""
        import databundles.library as dl
        import databundles.geo as dg
        
        l = self.library
        aa = dg.get_analysis_area(l, geoid=self.config.build.aa_geoid)
        r =  l.find(dl.QueryCommand().identity(id='a2z2HM').partition(table='incidents',space=aa.geoid)).pop()
        return  aa, l.get(r.partition).partition

    def build(self):
        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area, draw_edges
        from osgeo.gdalconst import GDT_Float32
        import databundles.datasets.geo as ddg

        places = self.library.dep('places').partition
        incidents = self.library.dep('crime').partition
        raster = self.partitions.new_hdf_partition(table='incidentsr') 

        k = dg.GaussianKernel(27,9)

        lr = self.init_log_rate(25000)
        
        for place_row in places.query("SELECT * FROM places"): 
            
            if not place_row['aa']:
                continue
            
            if place_row['code'] != 'SndSAN':
                continue
            
            q = """SELECT * FROM incidents WHERE {} = ? """.format(place_row['type'])
            
            place = ddg.US(self.library).place(place_row['code'])
            aa = place.aa(scale=10)
            trans = aa.get_translator()
            a = aa.new_array()
            
            if a.size < 700:
                self.error("{} is too small ({},{})".format(place.name, a.shape[0], a.shape[1]))
                continue
            
            self.log("Loading place: {} ".format(place.name))
            
            for i, row in enumerate(incidents.query(q, place.code)):
           
                lr("Add raster point")
                try:
                    k.apply_add(a, trans(row['lon'], row['lat']))
                except Exception as e:
                    self.error("Failed for point: "+e)
    
            masked = place.mask(a,nodata=0, scale=10).filled(0)
            raster.database.put_geo(place.code, masked, aa)    

        return True

    def old_build(self):
        ''' '''
        
        import databundles.geo as dg
        import  random 
        
        rs = 5
    
        aa, source_partition = self.get_incident_source()

        #k = dg.GaussianKernel(33,11)
        k = dg.GaussianKernel(41,11)

        #
        # Create each of the arrays, one per type. 
        #
        for row in source_partition.query("select date, time, cellx, celly, type  from incidents"):

            p = dg.Point(row['cellx'], row['celly'])
  
            a = self.get_array_by_type(aa, row['type'])
            k.apply_add(a, p)
            
        #
        # Merge datasets. Create the higher-level merged sets, which are
        # composed of multiple smaller sets. 
        #
        for name, types in self.config.build.crime_merges.items():
            self.log("Creating merged array {} ".format(name))
            arrays = [ self.get_array_by_type(aa,type) for type in types ]
            
            out = self.get_array_by_type(aa, name)
            
            for a in arrays:
                out += a
            
        # Save the datasets to the HDF5 file, in a partition
        
        # Determine the length of the time period
        row = source_partition.query("""select julianday(min(date(date))), julianday(max(date(date)))
        from incidents""").first()
        days = row[1]-row[0]
        cell_area = aa.scale**2 # In m^2
        
        partition = self.partitions.all[0]# There is only one
        partition.hdf5file.open()
                  
        for name, a in self.array_cache.items():  
    
            self.log("Saving dataset: {}".format(name))
        
            # Convert numbers to crimes per year per km^2
            b = a * 1000000 * 365 / cell_area / days
        
            partition.hdf5file.put_geo(name, b, aa)
            
            print dg.statistics(b)

        partition.hdf5file.close()

        return True

    def  extract_sl_image(self, data):
        """ """
        
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        import numpy as np
        import numpy.ma as ma

        crimep = self.partitions.find(table='incidentsr')  
        crimea,aa = crimep.database.get_geo(data['type'])
                      
        lightsp = self.library.dep('streetlightsr').partition
        lightsa,aa = lightsp.database.get_geo(data['type'])
        
        file_name = self.filesystem.path('extracts','{}'.format(data['name']))
             
        self.log("Extracting {} to {} ".format(data['name'], file_name))


        crimea = crimea[...]
        crimea = np.clip(crimea, 0, crimea.mean()+4*crimea.std())
        crimea /= crimea.max() # Normalize to 1
        crimea = ma.masked_equal(crimea,0)

        print crimea.min(), crimea.mean(), crimea.max()

        lightsa = lightsa[...]
        lightsa = np.clip(lightsa, 0 , lightsa.mean()+ 2*lightsa.std())
        print lightsa.min(), lightsa.mean(), lightsa.max()
        
        lightsa /= lightsa.max()
        lightsa = ma.masked_equal(1-lightsa,0)

        print lightsa.min(), lightsa.mean(), lightsa.max()

        x = lightsa.filled(0) * crimea.filled(0)

        aa.write_geotiff(file_name, x, data_type=GDT_Float32)

        return file_name

  
    def extract_image(self, data):
        """Save an HDF5 Dataset directly as a geotiff"""
        
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma

        raster = self.partitions.find(table='incidentsr')        

        file_name = self.filesystem.path('extracts','{}'.format(data['name']))
             
        self.log("Extracting {} to {} ".format(data['name'], file_name))
             
        i,aa = raster.database.get_geo(data['type'])
             
        aa.write_geotiff(file_name, 
                         i[...], #std_norm(ma.masked_equal(i,0)),  
                         data_type=GDT_Float32)

     
        return file_name
        
    def extract_images(self):
        """Save an HDF5 Dataset directly as a geotiff"""
        
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma

        raster = self.partitions.find(grain='raster')        

        places = self.library.dep('places').partition

        lr = self.init_log_rate(25000)
        
        for place in places.query("SELECT * FROM places"): 

            if not place['aa']:
                continue

            file_name = self.filesystem.path('extracts','{}.tiff'.format(place['code']))
             
            self.log("Extracting {} to {} ".format(place['code'], file_name))
             
            try:
                i,aa = raster.database.get_geo(place['code'])
                 
                aa.write_geotiff(file_name, 
                                 i[...], #std_norm(ma.masked_equal(i,0)),  
                                 data_type=GDT_Float32)
            except Exception as e:
                self.error("Failed for {}: {}".format(place['code'], e))

     
        return file_name
         
    def make_contour_bounds_shapefile(self):
        import databundles.geo as dg
        from numpy import ma
        import yaml


        shape_file_dir = self.filesystem.path('extracts','contours')
        shape_file = os.path.join(shape_file_dir, 'contours.shp') # One of two. 
        
        if os.path.exists(shape_file):
            return shape_file_dir

        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()
        
        a1,_ = hdf.get_geo('property')
        a2,aa = hdf.get_geo('violent')
     
        a = dg.std_norm(ma.masked_equal(a1[...] + a2[...],0))   # ... Converts to a Numpy array. 

        # Creates the shapefile in the extracts/contour directory
        envelopes = dg.bound_clusters_in_raster( a, aa, shape_file_dir, 0.1,0.7, use_bb=True, use_distance=50)
  
        # Cache the envelopes for later. 
        env_file = self.filesystem.path('build','envelopes.yaml')
        with open(env_file,'w') as f:
            f.write(yaml.dump(envelopes, indent=4, default_flow_style=False))
  
        return  shape_file_dir
  
    def extract_contour_bounds(self, data):
        import ogr
        
        shape_file = self.make_contour_bounds_shapefile()

        format_map = {
            'kml': ('KML',[]),
            'geojson': ('GeoJSON',[]),
            'sqlite': ('SQLite',('SPATIALITE=YES', 
                                 'INIT_WITH_EPSG=YES','OGR_SQLITE_SYNCHRONOUS=OFF')),
            'shapefile': ('ESRI Shapefile',[])
        }
        
        source_ds = ogr.GetDriverByName('ESRI Shapefile').Open(shape_file)
        
        ogr_format, options = format_map[data['format']]
        
        file_name = self.filesystem.path('extracts',format(data['name']))
        
        if os.path.exists(file_name):
            if os.path.isdir(file_name):
                self.filesystem.rm_rf(file_name)
            else:
                os.remove(file_name)
        
        dest_ds = ogr.GetDriverByName( ogr_format ).CopyDataSource(source_ds,file_name, options=options)

        if os.path.isdir(file_name):
            from databundles.util import zip_dir
            fpath = file_name+'.zip'
            zip_dir(file_name, fpath)
            return fpath
        else:
            return file_name
            
    
    def _get_sub_aas(self):
        import yaml
        import databundles.geo as dg
        import databundles.library as dl
        aa = dg.get_analysis_area(dl.get_library(), geoid=self.config.build.aa_geoid)
        
        with open(self.filesystem.path('extracts','envelopes.yaml')) as f:
            envelopes = yaml.load(f)
            
        aas = []
        for r in envelopes:
            
            saa = aa.get_aa_from_envelope(r['env'], '', '')
            
            aas.append( (r['area'], saa))
  
        aas = sorted(aas, cmp=lambda x,y: cmp(x[0], y[0]), reverse=True)
        
        return [aa[1] for aa in aas]
  
    
    def build_aa_map(self):
        '''
        '''
        import databundles.library as dl
        import databundles.geo as dg
        import random 
        from numpy import  ma
        
        rs = 3
    
        # make a helper to store files in the extracts directory
        ed = lambda f: self.filesystem.path('extracts','subs',f+'.tiff')
    
        l = dl.get_library()
        aa = dg.get_analysis_area(l, geoid=self.config.build.aa_geoid)
        
        r =  l.find(dl.QueryCommand().identity(id='a2z2HM').partition(table='incidents',space=aa.geoid)).pop()
        source_partition = l.get(r.partition).partition

        k = dg.GaussianKernel(33,11)
        
        sub_aas = [self.get_sub_aas()[1]]
        
        top_a = aa.new_array()
        for i, sub_aa in enumerate(sub_aas):
            where = sub_aa.is_in_ll_query()
            sub_a = sub_aa.new_array()
            trans = sub_aa.get_translator()

            q = "select * from incidents WHERE {}".format(where)
        
            for row in source_partition.query(q):
                p = trans(row['lon'],row['lat'])
                k.apply_add(sub_a, p)
                print row
            
            sub_aa.write_geotiff(ed(str(i)), dg.std_norm(ma.masked_equal(sub_a,0)))
            sub_aa.write_geotiff(ed(str(i)), sub_a)

      
        return True
  

  
    def get_extract_name(self,data):
        import ogr 
        
        name = data['name']
        
        options = []
        if data['format'] == 'kml':
            drv = ogr.GetDriverByName( "KML" )
            fpath = path = self.filesystem.path('extracts',name)
            
        elif data['format'] == 'geojson':
            drv = ogr.GetDriverByName( "GeoJSON" )
            fpath = path = self.filesystem.path('extracts',name)
        elif data['format'] == 'sqlite':
            drv = ogr.GetDriverByName( "SQLite" )
            fpath = path = self.filesystem.path('extracts',name)
            options = ['SPATIALITE=YES', 'INIT_WITH_EPSG=YES','OGR_SQLITE_SYNCHRONOUS=OFF']
        elif data['format'] == 'shapefile':
            drv = ogr.GetDriverByName( "ESRI Shapefile" )
            path = self.filesystem.path('extracts',name,name)
            
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
                
            fpath = self.filesystem.path('extracts',name+'.zip')
            
        else: 
            self.error("Unknown extract format: {} ".format(data['format']))
            
        return path, fpath, drv, options        
  
    def extract_incident_shapefile(self, data):
        import ogr 
        import databundles.library as dl
        import databundles.geo as dg
        import random
        import dateutil.parser
        
        path, fpath, drv, options = self.get_extract_name(data)
        
        if os.path.exists(fpath):
            return fpath
        
        year = data['year']
            
        source_bundle  = self.library.dep('aacrime')
            
        l = self.library
        aa = dg.get_analysis_area(l, geoid=self.config.build.aa_geoid)
        r =  l.find(dl.QueryCommand().identity(id=source_bundle.identity.id_).partition(table='incidents',space=aa.geoid)).pop()
        
        source_partition = l.get(r.partition).partition
            
        ds = drv.CreateDataSource(path, options=options)
        srs = ogr.osr.SpatialReference()
        srs.ImportFromEPSG(4326) # Lat/Long in WGS84
        lyr = ds.CreateLayer( "incidents", srs, ogr.wkbPoint )

        lyr.CreateField(ogr.FieldDefn( "type", ogr.OFTString )) # 0 
        
        fn=ogr.FieldDefn( "time", ogr.OFTString ) # 1
        fn.SetWidth(8)
        lyr.CreateField(fn)
        
        lyr.CreateField(ogr.FieldDefn( "hour", ogr.OFTInteger )) # 2
        lyr.CreateField(ogr.FieldDefn( "day_time", ogr.OFTInteger )) # 3
        
        fn = ogr.FieldDefn( "date", ogr.OFTString ) # 4
        fn.SetWidth(10)
        lyr.CreateField(fn)
        
        fn = ogr.FieldDefn( "desc", ogr.OFTString ) # 5 
        fn.SetWidth(400)
        lyr.CreateField(fn)  
           
        fn = ogr.FieldDefn( "addr", ogr.OFTString ) # 6
        fn.SetWidth(100)
        lyr.CreateField(fn)           
        

        rnd = .00001 * 100 # Approx 100m
        for row in source_partition.query("""
        select date, time, lat, lon, type , description, address from incidents 
        where  CAST(strftime('%Y', date) AS INTEGER) = {year} """.format(year=year)):

            pt = ogr.Geometry(ogr.wkbPoint)
            
            px = row['lon']+random.uniform(-rnd, rnd)
            py = row['lat']+random.uniform(-rnd, rnd)

            
            pt.SetPoint_2D(0, px, py )

            feat = ogr.Feature(lyr.GetLayerDefn())
            
            try:  hour = dateutil.parser.parse(row['time']).time().hour
            except: hour = None
   
          
            feat.SetField(0, str(row['type']) )
            feat.SetField(1, str(row['time']) )  
            feat.SetField(2, hour )    
            feat.SetField(3, None )                 
            feat.SetField(4, str(row['date']) )     
            feat.SetField(5, str(row['description']) )  
            feat.SetField(6, str(row['address']) ) 
            feat.SetGeometry(pt)
            lyr.CreateFeature(feat)
            feat.Destroy()
        
        if data['format'] == 'shapefile':
            from databundles.util import zip_dir
            zip_dir(os.path.dirname(path), fpath)
            return fpath
        else:
            return path
  
    def extract_sum_image(self, data, file_=None):  
        """List extract_image, but will sum multiple atasets together
        to form the image. """
        
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma
        
        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()
        
        file_name = self.filesystem.path('extracts',format(data['name']))
        self.log("Extracting {} to {} ".format(data['name'], file_name))
        
        types = data['types']
        
        first_type = types.pop()
        
        i,aa = hdf.get_geo(first_type)
        
        i = i[...] # '...' converts the H5py Dataset to a numpy array
        
        for type in types:
            i2,aa = hdf.get_geo(type)
            
            i+=i2[...]
            
        aa.write_geotiff(file_name, 
                         i[...], # std_norm(ma.masked_equal(i,0)),  
                         type_=GDT_Float32)
        hdf.close()
        
        return file_name
    
    def extract_diff_image(self, data):
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma
        import numpy as np

        partition = self.partitions.all[0]# There is only one
        hdf = partition.database
        hdf.open()

        i1, aa = hdf.get_geo(data['type1'])
        i2, aa = hdf.get_geo(data['type2'])

        file_name = self.filesystem.path('extracts',format(data['name']))

        self.log("Extracting difference, {} - {} ".format(data['type1'], data['type2']))

        # After subtraction, 0 is a valid value, so we need to change it. 
        # [...] converts to a numpy array. 
        a1 = ma.masked_equal(i1[...],0)
        a2 = ma.masked_equal(i2[...],0)
                
        diff = a1 - a2
        
        o =  diff # std_norm(diff)
    
        o.set_fill_value(-1)

        self.log("Stats: \n{}".format(statistics(o)))

        aa.write_geotiff(file_name, ma.filled(o),  data_type=GDT_Float32, nodata = -1)

        self.log("Wrote Difference TIFF {}".format(file_name))
        
        hdf.close()
        
        return file_name


    

    def extract_colormaps(self, data):
        from  databundles.geo.colormap import get_colormap, write_colormap, expand_map
        import numpy as np

        raster = self.partitions.find(grain='raster')        
    
        a,_ = raster.database.get_geo('SndSAN')
     
        a1 = np.sort(a[...].ravel())
     
        cmap =  get_colormap(data['map_name'],9, reverse=bool(data['reversed']))
        
        cmap = expand_map(cmap,1)
        
        path = self.filesystem.path('extras',data['name'])

        write_colormap(path, a1, cmap,  min_val = 0, max_val = a1.std()*10, break_scheme = data['break'])
   
        return path

    def demo(self):
        '''A commented demonstration of how to create crime data extracts as GeoTIFF 
        images 
        
        Run with: python bundle.py run demo
        '''
        from databundles.geo.analysisarea import get_analysis_area,  draw_edges
        from databundles.geo.util import create_bb
        from databundles.geo import Point
        from databundles.geo.kernel import GaussianKernel
        from databundles.geo.array import statistics, unity_norm, std_norm
        from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16
        from numpy import ma
        import random
             
        # Get the San Diego analysis area from the GEOID ( Defined by the US Census)
        # you can look up geoids in clarinova.com-extents-2012-7ba4/meta/san-diego-places.csv,
        # or query the places table in clarinova.com-extents-2012-7ba4.db
        aa = get_analysis_area(self.library, geoid = 'CG0666000')    
      
        # Get a function to translate coodinates from the default lat/lon, WGS84, 
        # into the cordinate system of the AnalysisArea, which in this case
        # is 20m square cells in an area based on a California StatePlane Zone
        trans = aa.get_translator()

        
        print "\n---- Display Analysis Area ----"
        print aa
   
        # This should print a small value, something close to (0,0). 
        # It won't be exactly (0,0), since the analysis area envelope must be
        # larger than the envelop of the place to account for rotation from 
        # re-projection
        print "Origin", trans(aa.lonmin, aa.latmin)
         
        # At the Sandiego latitude, 1/5000 of a degree, .0002, is about 20 meters, 
        # So incrementing by that amount should advance our cell position by one
        print "\n---- Check translation function ----"
        import numpy as np
        for i,x in enumerate(np.arange(0,.002,.0002)):
            print i,x,trans(aa.lonmin+x, aa.latmin+x)
   
        # Now we can load in the crime incident data, translate the lat/lon points
        # to our array coordinates, and produce an image. 
        
        # Get a reference to the bundle named as "crime" in the bundle.yaml configuration
        # file.   crime = spotcrime.com-us_crime_incidents-orig-7ba4
        r = self.library.dep('crime')

        # Fill in the values for the extents of the analysis area into the
        # query template. 
        q = self.config.build.incident_query.format(**aa.__dict__)
        q += " AND type = 'Theft' "
        
        # A 'Kernel' is a matrix in a process called 'convolution'. We're doing something
        # somewhat different, but are re-using the name. This kernel is added
        # onto the output array for each crime incident, and represents a Normal
        # distribution, so it spreads out the influence over a larger area than
        # a single cell.
        
        # The matrix is square, 9 cells to a side. The function has 1/2 of its
        # maximun ( Full-Width-Half Maximum, FWHM) three cells from the center. 
        kernel =  GaussianKernel(33,11)
        
        # We're going to need an output array. This creates a numpy array that 
        # has the correct size
        a = aa.new_array() # Main array
        ar = aa.new_array() # Array with random perturbation 
        rs = 4
        print "Array shape: ",a.shape
        
        for i,row in enumerate(r.bundle.database.connection.execute(q)):
            
            if i > 0 and i%1000 == 0:
                print "Processed {} rows".format(i)
           
            if i > 5000:
                break
            
            point = trans(row['longitude'], row['latitude'])

            kernel.apply_add(a,point)
            
            # The source data is coded to the 'hundred block' address, 
            # such as: 12XX Main Street. This make the points quantized, so
            # add a little randomness for a smoother map. 
            rpoint = Point(point.x+random.randint(-rs, rs),
                           point.y+random.randint(-rs, rs))
            
            kernel.apply_add(ar,rpoint)
            
        # make a helper to store files in the extracts directory
        ed = lambda f: self.filesystem.path('extracts','demo',f+'.tiff')
            
        print "\n--- Statistics, Before Normalizing ---"
        print statistics(a)
        
        aa.write_geotiff(ed('orig'),  a,  type_=GDT_Float32)
  
        print "\n--- Statistics, After Masking Normalizing ---"
        #
        # Masking marks some values as invalid, so they don't get used in statistics. 
        # I this case, we are making 0 invalid, which will keep it from being
        # considered in the std deviation later in std_norm. 
        a = ma.masked_equal(a,0)  
        print statistics(a)
        
        aa.write_geotiff(ed('masked'),  a,  type_=GDT_Float32)
        
        print "\n--- Statistics, After StdDev Normalizing ---"
        o = std_norm(a)
        print statistics(o)
        
        aa.write_geotiff(ed('stddev'),  o,  type_=GDT_Float32)

        print "\n--- Statistics, After Unity Normalizing ---"
        o = unity_norm(a)
        print statistics(o)
        
        aa.write_geotiff(ed('unity'),  o,  type_=GDT_Float32)
        
        # Write the array with randomness
        ar = ma.masked_equal(ar,0)  
        aa.write_geotiff('/tmp/random.tiff', std_norm(ar),  type_=GDT_Float32)
            
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    