#!/usr/bin/env python

from osgeo import gdal, ogr, osr
import sys
import logging
from optparse import OptionParser
import os
import json

gdal.UseExceptions()

def GetExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
        yarr.reverse()
    return ext


def get_raster_extent(source_file, options):
    try:
        src_ds = gdal.Open(source_file)
    except RuntimeError, e:
        print 'Unable to open ' + source_file
        print e
        return None

    gt = src_ds.GetGeoTransform()
    cols = src_ds.RasterXSize
    rows = src_ds.RasterYSize
    extent = GetExtent(gt,cols,rows)
    properties = {}
    properties['filename'] = source_file
    properties['size'] = "%dx%d" % (src_ds.RasterXSize, src_ds.RasterYSize)
    properties['driver'] =  src_ds.GetDriver().ShortName + '/' + src_ds.GetDriver().LongName
    properties['projection'] = src_ds.GetProjection()

    return extent, properties

if __name__=='__main__':
    usage = "usage: %prog foo.tiff bar.tiff"
    parser = OptionParser(usage=usage,
        description="Generate a GeoJSON file representing the extent of a set of rasters")
    parser.add_option("-d", "--debug", action="store_true", dest="debug")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet")
    parser.add_option("-c", "--centroids", action="store_true", dest="centroids",
     help="Add centroid markers")
    (options, args) = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if options.debug else 
        (logging.ERROR if options.quiet else logging.INFO))
 
    features = []
    for path in args:
        if not os.path.exists(path):
            print path + " not found"
            continue
        extent, properties = get_raster_extent(path, options)
        ul, ll, lr, ur = extent
        geometry = {"type":"LineString", "coordinates" : [ul, ll, lr, ur, ul]}
        feature = {"type":"Feature", "geometry":geometry, "properties":properties}
        features.append(feature)
        if options.centroids:
            centroid_point = ((ul[0]+lr[0])/2.0, (ul[1]+lr[1])/2.0)
            centroid_geometry = {"type":"Point", "coordinates":centroid_point}
            centroid_feature = {"type":"Feature", "geometry":centroid_geometry, 
                "properties":properties}
            features.append(centroid_feature)
    feature_collection = {"type":"FeatureCollection", "features":features}
    print json.dumps(feature_collection, sort_keys=True,
        indent=4, separators=(',', ': '))
