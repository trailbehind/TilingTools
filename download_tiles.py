#!/usr/bin/env python3

import logging
from optparse import OptionParser
import os
import mercantile
from urllib.parse import urlparse
import requests
from boto.s3.connection import S3Connection
import sys

def download_tiles(minzoom, maxzoom, bbox, url, path, tile_cover=False, skip_existing=False):
    if not os.path.exists(path):
        os.makedirs(path)

    if tile_cover:
        ul = mercantile.tile(bbox[0], bbox[3], minzoom)
        lr = mercantile.tile(bbox[2], bbox[1], minzoom)
        ul_bounds = mercantile.bounds(ul.x, ul.y, ul.z)
        lr_bounds = mercantile.bounds(lr.x, lr.y, lr.z)
        bbox = (ul_bounds.west, lr_bounds.south, lr_bounds.east, ul_bounds.north)

    for zoom in range(minzoom, maxzoom + 1):
        tile_url_z = url.replace("{z}", str(zoom))
        z_dir = os.path.join(path, str(zoom))
        if not os.path.exists(z_dir):
            os.makedirs(z_dir)
        ul = mercantile.tile(bbox[0], bbox[3], zoom)
        lr = mercantile.tile(bbox[2], bbox[1], zoom)
        if ul.x < 0:
            ul = mercantile.Tile(x=0, y=ul.y, z=ul.z)
        if ul.y < 0:
            ul = mercantile.Tile(x=ul.x, y=0, z=ul.z)
        max_tile = pow(2, zoom) - 1
        if lr.x > max_tile:
            lr = mercantile.Tile(x=max_tile, y=lr.y, z=lr.z)
        if lr.y > max_tile:
            lr = mercantile.Tile(x=lr.x, y=max_tile, z=lr.z)
        logging.info("Downloading tiles for zoom %d x:%d-%d y:%d-%d " % (zoom, ul.x, lr.x, ul.y, lr.y))
        for x in range(ul.x, lr.x + 1):
            x_dir = os.path.join(z_dir, str(x))
            if not os.path.exists(x_dir):
                os.makedirs(x_dir)
            tile_url_x = tile_url_z.replace("{x}", str(x))
            for y in range(ul.y, lr.y + 1):
                tile_url_y = tile_url_x.replace("{y}", str(y))
                file_path = os.path.join(x_dir, str(y) + ".png")
                try:
                    download_tile(tile_url_y, file_path, skip_existing=skip_existing)
                except Exception as e:
                    logging.debug(e)
                    logging.error("Failed to download tile: " + tile_url_y)


CHUNK_SIZE = 1024
def download_tile(url, path, skip_existing=False):
    if skip_existing and os.path.exists(path):
        logging.debug("%s exists, skipping" % path)
        return

    logging.debug("Downloading %s to %s" % (url, path))
    parsed_url = urlparse(url)

    if parsed_url.scheme == "s3":
        conn = S3Connection(calling_format='boto.s3.connection.OrdinaryCallingFormat')
        if conn is None:
            raise Exception("Error connecting to s3")
        bucket = conn.get_bucket(parsed_url.netloc, validate=False)
        if bucket is None:
            raise Exception("Error getting s3 bucket")
        key = bucket.get_key(parsed_url.path)
        if key is not None:
            with open(path, "wb") as f:
                key.get_contents_to_file(f)
    else:
        res = requests.get(url, stream=True, verify=False)

        if not res.ok:
            raise IOError

        with open(path, "wb") as f:
            for chunk in res.iter_content(CHUNK_SIZE):
                f.write(chunk)


def _main():
    usage = "usage: %prog"
    parser = OptionParser(usage=usage,
                          description="")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Turn on debug logging")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                      help="turn off all logging")
    parser.add_option("-f", "--force", action="store_true", dest="force",
                      help="Redownload existing tiles.")
    parser.add_option("-z", "--min-zoom", action="store", type="int", 
        dest="min_zoom", default=0)
    parser.add_option("-Z", "--max-zoom", action="store", type="int", 
        dest="max_zoom", default=15)
    parser.add_option("-b", "--bbox", action="store", dest="bbox", default="-180,-85.05113,180,85.05113")
    parser.add_option("-t", "--tile-cover", action="store_true", dest="tileCover", default=False,
        help="Download all tiles covered by extent at min zoom")

    (options, args) = parser.parse_args()
 
    logging.basicConfig(level=logging.DEBUG if options.debug else
    (logging.ERROR if options.quiet else logging.INFO))

    bounds = options.bbox.split(",")
    if len(bounds) != 4:
        logging.error("BBOX must have 4 components")
        sys.exit(-1)
    bounds = [float(f) for f in bounds]
    if bounds[0] > bounds[2] or bounds[1] > bounds[3] or \
        abs(bounds[0]) > 180 or abs(bounds[2]) > 180 or \
        abs(bounds[1]) > 90 or abs(bounds[3]) > 90:
        logging.error("BBOX is in incorrect order or contains values out of range, must be W,S,E,N.\n" +
        "If you are trying to specify a region that crosses the antimeridian," + 
        " it must be split into 2 regions, split at the antimeridian.")
        sys.exit(-1)

    url = args[0]
    path = args[1]
    download_tiles(options.min_zoom, options.max_zoom, bounds, url, path, 
        tile_cover=options.tileCover, skip_existing=(not options.force))

if __name__ == "__main__":
    _main()