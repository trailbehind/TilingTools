#!/usr/bin/env python3
###########################################################
# Plots a GeoTiff with a pixel count bounding box overlay.
# Use to determine pixel values for ground control points
# when geo-referencing or creating/testing cutlines.
#######################################################
import argparse
import logging
import os
import rasterio
from rasterio import windows
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger()


def run(args):
    src_file = args.src
    min_x = args.min_x
    max_x = args.max_x
    min_y = args.min_y
    max_y = args.max_y

    logging.info('Reading band 1 from ' + src_file)
    raster_src = rasterio.open(src_file)

    logging.info('Building overlay')
    slice_raster = (slice(min_y, max_y), slice(min_x, max_x))
    window_slice = windows.Window.from_slices(*slice_raster)

    plt.imshow(raster_src.read(1))
    plt.title(os.path.basename(src_file))
    ax = plt.gca()
    ax.add_patch(
        Rectangle(
            (window_slice.col_off, window_slice.row_off),
            width=window_slice.width,
            height=window_slice.height,
            fill=True,
            alpha=.5,
            color="red"
        )
    )

    plt.show()


def main():
    parser = argparse.ArgumentParser(description='Plots GeoTiff with a pixel value bbox overlay')
    parser.add_argument('-s', help='Filepath to raster', dest='src', required=True)
    parser.add_argument('-x', help='Min Horizontal value', dest='min_x', type=int, default=1, required=False)
    parser.add_argument('-X', help='Max Horizontal value', dest='max_x', type=int, default=100, required=False)
    parser.add_argument('-y', help='Min Vertical value', dest='min_y', type=int, default=1, required=False)
    parser.add_argument('-Y', help='Max Vertical value', dest='max_y', type=int, default=100, required=False)
    parser.set_defaults(func=run)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
