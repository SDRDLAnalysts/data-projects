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
 
    def load_datasets(self, aa):
        '''Load data from the database into cached numpy arrays'''
        
        from databundles.geo import Point     
        from databundles.geo.kernel import GaussianKernel   

        trans = aa.get_translator()
        
        r = self.library.dep('crime')

        kernel =  GaussianKernel(33,11)
                
        q = self.config.build.incident_query.format(**aa.__dict__)

        for i,row in enumerate(r.bundle.database.connection.execute(q)):
            
            if (i)%500 == 0 and i > 0:
                self.log("Loaded {} rows".format(i))
                if self.run_args.test:
                    self.log("IN TEST MODE, BREAKING FROM LOADING LOOP")
                    break
            
            point = trans(row['longitude'], row['latitude'])

            a = self.get_array_by_type(aa, row['type'])
            
            kernel.apply_add(a,point) 

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
        
        from databundles.geo.analysisarea import get_analysis_area
        aa = get_analysis_area(self.library, geoid = self.config.build.aa_geoid)    
        
        self.load_datasets(aa)
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

    def test_mask(self):
        import numpy as np
        from numpy import  ma
        
        a = np.arange(100).reshape(10,10)
        m = ma.masked_less(a, 50)
        
        print m
        
        m.set_fill_value(0)
        
        print ma.filled(m)

        
        
        
    
    def demo(self):
        '''A commented demonstration of how to create crime data extracts as GeoTIFF 
        images 
        
        Run with: python bundle.py run demo
        '''
        from databundles.geo.analysisarea import get_analysis_area, create_bb,  draw_edges
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
     
    
    