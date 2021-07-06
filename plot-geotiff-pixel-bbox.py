#!/usr/bin/env python3
################################################################
# Plots a GeoTiff with a pixel coordinate bounding box overlay.
# Use to determine pixel coordinates for ground control points
# when geo-referencing or creating/testing cutlines.
################################################################
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
    min_x = int(args.ll.split(',')[0])
    max_x = int(args.ur.split(',')[0])
    min_y = int(args.ur.split(',')[1])
    max_y = int(args.ll.split(',')[1])
    bbox_color = args.bbox_color
    output = args.output
    png_dpi = args.png_dpi

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
            color=bbox_color
        )
    )

    if output:
        logging.info('Exporting ' + output)
        if not png_dpi:
            png_dpi = 300
        plt.savefig(output, dpi=png_dpi)
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description='Plots GeoTiff with a pixel value bbox overlay')
    parser.add_argument('-s', help='Filepath to raster', dest='src', required=True)
    parser.add_argument('-lower_left', help='Lower Left corner pixel coordinates', dest='ll', default='1,100', required=False)
    parser.add_argument('-upper_right', help='Upper Right corner pixel coordinates', dest='ur', default='100,1', required=False)
    parser.add_argument('-bbox_color', help='Color of bounding box overlay', dest='bbox_color', default='red', required=False)
    parser.add_argument('-o', help='Export plot as a PNG', dest='output', required=False)
    parser.add_argument('-dpi', help='PNG export DPI', dest='png_dpi', type=int, required=False)
    parser.set_defaults(func=run)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
