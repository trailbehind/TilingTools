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

## AWS S3 has special metadata that's "system-defined"
## https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
## The S3 console shows these with hyphens, but Boto3 requires no hyphens. 
AWS_SYSTEM_METADATA_KEYS = [
    "CacheControl",
    "ContentDisposition",
    "ContentEncoding",
    "ContentLanguage",
    "ContentType",
]

def _get_existing_system_metadata(existing_object):
    """
    Extract system-defined metadata from s3.head_object response
    """
    existing_meta = {}
    for syskey in AWS_SYSTEM_METADATA_KEYS:
        if syskey in existing_object:
            existing_meta[syskey] = existing_object[syskey]
    return existing_meta

def _prep_metadata(metadata):
    """
    Separate user and system defined metadata keys from metadata dict
    passed from user input
    """
    sys_meta = {}
    user_meta = {_k.replace("-", "").strip().lower() : _v for _k, _v in metadata.items()}
    for syskey in AWS_SYSTEM_METADATA_KEYS: 
        if syskey.lower() in user_meta:
            sys_meta[syskey] = user_meta[syskey.lower()]
            del user_meta[syskey.lower()]

    return (sys_meta,user_meta)

def replace_metadata(bucket, key, new_metadata):
    s3 = boto3.client('s3')
    try:
        ## Get existing system and user defined metadata
        existing = s3.head_object(
            Key=key, Bucket=bucket
        )
        existing_system_metadata = _get_existing_system_metadata(existing)
        existing_user_metadata = existing['Metadata']
        existing_etag = existing["ETag"]

        ## get updates to user and system defined metadata
        new_sys_meta, new_user_meta = _prep_metadata(new_metadata)
        print(new_sys_meta, new_user_meta)

        existing_user_metadata.update(new_user_meta)
        existing_system_metadata.update(new_sys_meta)

        s3.copy_object(
            Key=key, Bucket=bucket, 
            CopySource={"Bucket": bucket, "Key": key}, 
            Metadata=new_metadata, 
            MetadataDirective='REPLACE', 
            CopySourceIfMatch=existing_etag, 
            **existing_system_metadata
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
    parser.add_option("-s", "--set", action="append", dest="set", default=[],
                      help="<KEY>=<VALUE>. AWS S3 System defined metadata keys should contain hyphens.")
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
