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

    def prepare(self):
        from databundles.identity import PartitionIdentity
        
        if not self.database.exists():
            self.database.create()

        pid =  PartitionIdentity(self.identity, grain='arrays')
        partition = self.partitions.new_partition(pid)

        return True
      
    
    def get_array_by_type(self, aa, name):
        '''Cache lookups of analysis areaa arrays'''

        if name not in self.array_cache:
            a = aa.new_array()
            self.array_cache[name] = a
            return a 
        else:
            return self.array_cache[name]
 


    def merge_datasets(self, aa):
        """Create combined datasets from the sets of types defined in the
        build.crime_merges config group"""
        
        for name, types in self.config.build.crime_merges.items():
            self.log("Creating merged array {} ".format(name))
            arrays = [ self.get_array_by_type(aa,type) for type in types ]
            
            out = self.get_array_by_type(aa, name)
            
            for a in arrays:
                out += a


    def save_datasets(self, aa):
        '''Save cached numpy arrays into the HDF5 file'''
        partition = self.partitions.all[0]# There is only one
        partition.hdf5file.open()
                  
        for name, a in self.array_cache.items():  
    
            self.log("Saving dataset: {}".format(name))
        
            partition.hdf5file.put_geo(name, a, aa)

        partition.hdf5file.close()
            
    def build(self):
        '''
        '''
        import databundles.library as dl
        import databundles.geo as dg
        import  random 
        
        rs = 3
    
        l = dl.get_library()
        aa = dg.get_analysis_area(l, geoid=self.config.build.aa_geoid)
        r =  l.find(dl.QueryCommand().identity(id='a2z2HM').partition(table='incidents',space=aa.geoid)).pop()
        source_partition = l.get(r.partition).partition

        k = dg.GaussianKernel(33,11)
        
        for row in source_partition.query("select date, time, cellx, celly, type  from incidents"):
            p = dg.Point(row['cellx']+random.randint(-rs, rs),
                         row['celly']+random.randint(-rs, rs))
            a = self.get_array_by_type(aa, row['type'])
            k.apply_add(a, p)
            
        self.merge_datasets(aa)
        self.save_datasets(aa)

        return True
  
    def custom_extract(self, data):
        self.log("Custom Extract")
        pass
        return False
  
    def extract_image(self, data):
        """Save an HDF5 Dataset directly as a geotiff"""
        
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma

        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()
   
        file_name = self.filesystem.path('extracts','{}'.format(data['name']))
             
        self.log("Extracting {} to {} ".format(data['name'], file_name))
             
             
        i,aa = hdf.get_geo(data['type'])
             
        aa.write_geotiff(file_name, 
                         std_norm(ma.masked_equal(i,0)),  
                         type_=GDT_Float32)

        hdf.close()
        
        return file_name
   
    def contour_bounds(self):
        import databundles.geo as dg
        from numpy import ma
        import yaml

        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()
        
        a1,_ = hdf.get_geo('Property')
        a2,aa = hdf.get_geo('Violent')
        
        a = dg.std_norm(ma.masked_equal(a1[...] + a2[...],0))   # ... Converts to a Numpy array. 
        shape_file_dir = self.filesystem.path('extracts','contour')
                           
        # Creates the shapefile in the extracts/contour directory
        envelopes = dg.bound_clusters_in_raster( a, aa, shape_file_dir, 0.1,0.7, use_bb=True, use_distance=50)
  
        with open(self.filesystem.path('extracts','envelopes.yaml'),'w') as f:
            f.write(yaml.dump(envelopes, indent=4, default_flow_style=False))
  
    
    def get_sub_aas(self):
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
        
        sub_aas = self.get_sub_aas()[0:5]
        
        top_a = aa.new_array()
        for i, sub_aa in enumerate(sub_aas):
            where = sub_aa.is_in_ll_query()
            sub_a = sub_aa.new_array()
            trans = sub_aa.get_translator()

            q = "select date, time, cellx, celly, lat, lon, type from incidents WHERE {}".format(where)
        
            for row in source_partition.query(q):
                p = dg.Point(row['cellx']+random.randint(-rs, rs),
                             row['celly']+random.randint(-rs, rs))
                k.apply_add(top_a, p)
            
                p = trans(row['lon'],row['lat'])
                k.apply_add(sub_a, p)
            
            sub_aa.write_geotiff(ed(str(i)), dg.std_norm(ma.masked_equal(sub_a,0)))
            
            
        aa.write_geotiff(ed('substest'), dg.std_norm(ma.masked_equal(top_a,0)))            
      
        return True
  
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
                         std_norm(ma.masked_equal(i,0)),  
                         type_=GDT_Float32)

        hdf.close()
        
        return file_name
    
    def extract_diff_image(self, data):
        from databundles.geo.array import statistics, std_norm
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma
        import numpy as np

        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()

        i1, aa = hdf.get_geo(data['type1'])
        i2, aa = hdf.get_geo(data['type2'])

        file_name = self.filesystem.path('extracts',format(data['name']))

        self.log("Extracting difference, {} - {} ".format(data['type1'], data['type2']))

        # After subtraction, 0 is a valid value, so we need to change it. 
        a1 = ma.masked_equal(i1[...],0)
        a2 = ma.masked_equal(i2[...],0)
                
        diff = a1 - a2
        
        o =  std_norm(diff)
    
        o.set_fill_value(-1)

        self.log("Stats: \n{}".format(statistics(o)))

        aa.write_geotiff(file_name, ma.filled(o),  type_=GDT_Float32, nodata = -1)

        self.log("Wrote Difference TIFF {}".format(file_name))
        
        hdf.close()
        
        return file_name

        
        
    
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
        aa = get_analysis_area(self.library, geoid = '0666000')    
      
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
     
    
    