#!/usr/bin/env python

import logging
from optparse import OptionParser
import os
from urllib.parse import urlparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import boto3 
from botocore.exceptions import BotoCoreError
from time import sleep


def replace_metadata(bucket, key, new_metadata):
    s3 = boto3.client('s3')
    try:
        existing = s3.head_object(
            Key=key, Bucket=bucket
        )
        existing_metadata = existing['Metadata']
        existing_etag = existing["ETag"]
        existing_metadata.update(new_metadata)
        s3.copy_object(
            Key=key, Bucket=bucket, 
            CopySource={"Bucket": bucket, "Key": key}, 
            Metadata=new_metadata, 
            MetadataDirective='REPLACE', 
            CopySourceIfMatch=existing_etag
        )
        logging.debug(f"copy successful ({key})")
    except BotoCoreError as e:
        logging.error(f"Error copying key: {key}: {e}")
    

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

    s3 = boto3.client('s3')

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

        s3_prefix = url_parts.path.strip("/")
        bucket_paginator = s3.get_paginator('list_objects')
        bucket_pages = bucket_paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

        if not bucket_pages:
            logging.error("Failed to list bucket")
            return

        logging.info("Queueing tasks...")
        pool = ThreadPoolExecutor()
        total_files = 0
        for page in bucket_pages:
            total_files += len(page['Contents'])
            [pool.submit(replace_metadata, bucket_name, obj['Key'], values_to_set) for obj in page['Contents']]
        
        logging.debug(f"total files: {total_files}")

        logging.info("Processing...")
        progress = tqdm(unit='files', total=total_files)
        while pool._work_queue.qsize() > 0:
            progress.n = total_files - pool._work_queue.qsize()
            progress.refresh()
            sleep(2) 
        progress.close()


if __name__ == "__main__":
    _main()
