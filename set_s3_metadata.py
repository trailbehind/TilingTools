#!/usr/bin/env python

import logging
from optparse import OptionParser
import os
from urlparse import urlparse

from boto.s3.connection import S3Connection

def _main():
    usage = "usage: %prog"
    parser = OptionParser(usage=usage,
                          description="")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Turn on debug logging")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                      help="turn off all logging")
    parser.add_option("-s", "--set", action="append", dest="set", default=[])
    (options, args) = parser.parse_args()
 
    logging.basicConfig(level=logging.DEBUG if options.debug else
    (logging.ERROR if options.quiet else logging.INFO))

    conn = S3Connection(
        calling_format='boto.s3.connection.OrdinaryCallingFormat'
    )
    if not conn:
        logging.error("error connecting to s3 for download")
        sys.exit(-1)

    values_to_set = {}
    for o in options.set:
        split_option = o.split("=")
        if len(split_option) != 2:
            logging.error("Invalid option: '%s'" % o)
            sys.exit(-1)
        values_to_set[split_option[0]] = split_option[1]
        logging.debug("will set '%s'='%s'" % (split_option[0], split_option[1]))

    for arg in args:
        url_parts = urlparse(arg)
        bucket_name = url_parts.netloc
        try:
            bucket = conn.get_bucket(bucket_name, validate=True)
        except Exception, e:
            logging.error("error connecting to source bucket: %s", e)

        s3_prefix = url_parts.path.strip("/")
        list_results = bucket.list(s3_prefix)
        if not list_results:
            logging.error("Failed to list bucket")
            return

        for key in list_results:
            logging.debug("Setting values on %s" % key)
            key.set_remote_metadata(values_to_set, {}, True)

if __name__ == "__main__":
    _main()
