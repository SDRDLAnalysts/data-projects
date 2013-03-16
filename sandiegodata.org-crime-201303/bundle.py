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
                   
    def contours(self):
        import databundles.geo as dg
        from osgeo.gdalconst import GDT_Float32
        from numpy import ma
        import tempfile,os
        
        from osgeo import gdal
        import ogr
        
        partition = self.partitions.all[0]# There is only one
        hdf = partition.hdf5file
        hdf.open()
        
        a1,aa = hdf.get_geo('Property')
        a2,aa = hdf.get_geo('Violent')
        
        a = dg.std_norm(ma.masked_equal(a1[...] + a2[...],0))   # ... Converts to a Numpy array. 
        
        shaped = self.filesystem.path('extracts','contour')
        
        if os.path.exists(shaped):
            self.filesystem.rm_rf(shaped)
            os.makedirs(shaped)
        
        rasterf = self.filesystem.path('extracts','contour.tiff')
        
        print "!!!", shaped
        
        ogr_ds = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(shaped)
        
        ogr_lyr = ogr_ds.CreateLayer('contour', aa.srs)
        field_defn = ogr.FieldDefn('ID', ogr.OFTInteger)
        ogr_lyr.CreateField(field_defn)
        field_defn = ogr.FieldDefn('elev', ogr.OFTReal)
        ogr_lyr.CreateField(field_defn)
        
        ds = aa.get_geotiff(rasterf,  a, type_=GDT_Float32)
        ds.GetRasterBand(1).SetNoDataValue(0)
        ds.GetRasterBand(1).WriteArray(a)
        
        gdal.ContourGenerate(ds.GetRasterBand(1), 
                             0.1,  # contourInterval
                             0,   # contourBase
                             [],  # fixedLevelCount
                             0, # useNoData
                             0, # noDataValue
                             ogr_lyr, #destination layer
                             0,  #idField
                             1 # elevation field
                             )
        
        
        
        print "Shape  : ",shaped
        print "Raster : ",rasterf   
 
        # Get buffered bounding boxes around each of the hotspots, 
        # and put them into a new layer. 
 
        bound_lyr = ogr_ds.CreateLayer('bounds', aa.srs)
        for i in range(ogr_lyr.GetFeatureCount()):
            f1 = ogr_lyr.GetFeature(i)
            if f1.GetFieldAsDouble('elev') != 0.7:
                continue
            g1 = f1.GetGeometryRef()
            bb = dg.create_bb(g1.GetEnvelope(), g1.GetSpatialReference())
            f = ogr.Feature(bound_lyr.GetLayerDefn())
            f.SetGeometry(bb)
            bound_lyr.CreateFeature(f)
            
    
        # Doing a full loop instead of a list comprehension b/c the way that comprehensions
        # compose arrays results in segfaults, probably because a copied geometry
        # object is being released before being used. 
        geos = []
        for i in range(bound_lyr.GetFeatureCount()):
            f = bound_lyr.GetFeature(i)
            g = f.geometry()
            geos.append(g.Clone())
    

        geos = self.combine_envelopes(geos) 
     
        lyr = ogr_ds.CreateLayer('combined_bounds', aa.srs)
        for env in geos:
            f = ogr.Feature(lyr.GetLayerDefn())
            bb = dg.create_bb(env.GetEnvelope(), env.GetSpatialReference())
            f.SetGeometry(bb)
            lyr.CreateFeature(f)                   
          
    def combine_envelopes(self, geos):
        loops = 0   
        while True: 
            i, new_geos = self._combine_envelopes(geos)
            old = len(geos)
            geos = None
            geos = [g.Clone() for g in new_geos]
            loops += 1
            print "{}) {} reductions. {} old, {} new".format(loops, i, old, len(geos))
            if old == len(geos):
                break
          
        return geos
          
    def _combine_envelopes(self, geometries):
        import databundles.geo as dg
        reductions = 0
        new_geometries = []
        
        accum = None
        reduced = set()

        for i1 in range(len(geometries)):
            if i1 in reduced:
                continue
            g1 = geometries[i1]
            for i2 in range(i1+1, len(geometries)):
                if i2 in reduced:
                    continue

                g2 = geometries[i2]

                # Why we have to do the bounding box check is a mystery -- 
                # there are some geometries that look like they overlay, but Intersects() 
                # returns false. 
                bb1 =  dg.create_bb(g1.GetEnvelope(), g1.GetSpatialReference())
                bb2 =  dg.create_bb(g2.GetEnvelope(), g2.GetSpatialReference())
   
                if (g1.Intersects(g2) or  g1.Contains(g2) or g2.Contains(g1) or g1.Touches(g2) or
                   bb1.Intersects(bb2)):
                    reductions += 1
                    reduced.add(i2)
                    if not accum:
                        accum = g1.Union(g2)
                    else:
                        accum = accum.Union(g2)
            
            if accum is not None:
                new_geometries.append(accum.Clone())
                accum = None
            else:
                new_geometries.append(g1.Clone())

        return reductions, new_geometries
                  
    
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
     
    
    