#!/usr/bin/env python
#-------------------------------------------------------
# Translates between lat/long and the slippy-map tile
# numbering scheme
# 
# http://wiki.openstreetmap.org/index.php/Slippy_map_tilenames
# 
# Written by Oliver White, 2007
# This file is public-domain
#-------------------------------------------------------
from math import *
from optparse import OptionParser
import json

def numTiles(z):
  return(pow(2,z))

def sec(x):
  return(1/cos(x))

def latlon2relativeXY(lat,lon):
  x = (lon + 180) / 360
  y = (1 - log(tan(radians(lat)) + sec(radians(lat))) / pi) / 2
  return(x,y)

def latlon2xy(lat,lon,z):
  n = numTiles(z)
  x,y = latlon2relativeXY(lat,lon)
  return(n*x, n*y)
  
def tileXY(lat, lon, z):
  x,y = latlon2xy(lat,lon,z)
  return(int(x),int(y))

def xy2latlon(x,y,z):
  n = numTiles(z)
  relY = y / n
  lat = mercatorToLat(pi * (1 - 2 * relY))
  lon = -180.0 + 360.0 * x / n
  return(lat,lon)
  
def latEdges(y,z):
  n = numTiles(z)
  unit = 1 / n
  relY1 = y * unit
  relY2 = relY1 + unit
  lat1 = mercatorToLat(pi * (1 - 2 * relY1))
  lat2 = mercatorToLat(pi * (1 - 2 * relY2))
  return(lat1,lat2)

def lonEdges(x,z):
  n = numTiles(z)
  unit = 360 / n
  lon1 = -180 + x * unit
  lon2 = lon1 + unit
  return(lon1,lon2)
  
def tileEdges(x,y,z):
  lat1,lat2 = latEdges(y,z)
  lon1,lon2 = lonEdges(x,z)
  return((lat2, lon1, lat1, lon2)) # S,W,N,E

def mercatorToLat(mercatorY):
  return(degrees(atan(sinh(mercatorY))))

def tileSizePixels():
  return(256)

def print_pyramid(lat, lon, flip_y=False):
  print "calculating tiles for point %f,%f" % (lat, lon)
  for z in range(0,21):
    x,y = tileXY(lat, lon, z)
    s,w,n,e = tileEdges(x,y,z)
    if options.flip_y:
        y = (2 ** z) - y - 1
    print "%d/%d/%d --> %1.5f,%1.5f,%1.5f,%1.5f - %1.5f*%1.5f" % (z,x,y, w,s,e,n, abs(w-e), abs(n-s))


def print_bbox_pyramid(w, s, e, n, flip_y=False):
  print "calculating tiles for bbox %f,%f,%f,%f" % (w, s, e, n)
  for z in range(0,21):
    x1, y1 = tileXY(s, w, z)
    x2, y2 = tileXY(n, e, z)

    if options.flip_y:
        y1 = (2 ** z) - y1 - 1
        y2 = (2 ** z) - y2 - 1
        y1, y2 = y2, y1

    print "z:%d x:%d-%d y:%d-%d  %d tiles" % (z, x1, x2, y2, y1, (x2 - x1 + 1) * (y1 - y2 + 1))


if __name__ == "__main__":
    usage = "usage: %prog "
    parser = OptionParser(usage=usage,
        description="")
    parser.add_option("-l", "--latlon", action="store", dest="latlon")
    parser.add_option("-t", "--tile", action="store", dest="tile")
    parser.add_option("-b", "--bbox", action="store", dest="bbox")
    parser.add_option("-y", "--flip-y", action="store_true", dest="flip_y", help="use TMS y origin, not OSM/google")
    parser.add_option("-g", "--geojson", action="store_true", dest="geojson", help="Only output geojson")

    (options, args) = parser.parse_args()

    if options.latlon:
        lat, lon = options.latlon.split(',')
        print_pyramid(float(lat), float(lon), flip_y=options.flip_y)
    elif options.bbox:
        w, s, e, n = [float(f) for f in options.bbox.split(",")]
        print_bbox_pyramid(w, s, e, n, flip_y=options.flip_y)
    elif options.tile:
        z, x, y = options.tile.split('/')
        z = int(z)
        x = int(x)
        y = int(y)
        if options.flip_y:
          y = (2 ** z) - y - 1

        s,w,n,e = tileEdges(x,y,z)
        geojson_feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[w,n], [e,n], [e,s], [w,s], [w, n]]]
            }
        }
        if options.geojson:
          print json.dumps(geojson_feature)
        else:
          print "%d/%d/%d --> sw:%1.5f,%1.5f, ne:%1.5f,%1.5f" % (z,x,y, s, w, n, e)
          print "BBOX:  (%1.5f,%1.5f,%1.5f,%1.5f)" % (w, s, e, n)
          print "Centroid: %1.5f, %1.5f" % ((w + e)/2.0, (n + s)/2.0)
          print "Geojson: " + json.dumps(geojson_feature)
