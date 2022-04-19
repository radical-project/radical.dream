"""
Script which uploads coverage data files to a S3 bucket.
"""

from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import time

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

ACCESS_KEY_ID     = os.environ.get('ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.environ.get('ACCESS_KEY_SECRET')
REGION            = "us-east-1"

if not ACCESS_KEY_ID:
    raise ValueError("AWS_ACCESS_KEY_ID env variable not set")

if not ACCESS_KEY_SECRET:
    raise ValueError("AWS_ACCESS_KEY_SECRET env variable not set")


def list_buckets():
    """
    This function list the avilabe buckets within
    a given account
    """
    cls = get_driver(Provider.S3)
    driver = cls(ACCESS_KEY_ID, ACCESS_KEY_SECRET, region=REGION)
    container_list = list(driver.iterate_containers())
    return sorted([c.name for c in container_list])


def create_s3_bucket(bucket_name: str):

    cls = get_driver(Provider.S3)
    driver = cls(ACCESS_KEY_ID, ACCESS_KEY_SECRET, region=REGION)
    bucket = driver.create_container(bucket_name)
    return bucket


def delete_s3_bucket(bucket):

    s3_buckets = list_buckets()
    if bucket:
        cls = get_driver(Provider.S3)
        for s3_bucket in s3_buckets:

            if s3_bucket.name == bucket.name:
                driver = cls(ACCESS_KEY_ID, ACCESS_KEY_SECRET, region=REGION)
                driver.delete_container(bucket)
                print('bucket {0} is deleted'.format(bucket.name))
                return True
            else:
                print('bucket {0} does not exist'.format(bucket.name))
                return False
    else:
        raise('Not a valid bucket')


def upload_file(file_path):
    """
    :param file_path: Path to the coverage file to upload.
    """
    if not os.path.isfile(file_path):
        raise ValueError("File %s doesn't exist" % (file_path))

    print("Uploading coverage file to S3")

    cls = get_driver(Provider.S3)
    driver = cls(ACCESS_KEY_ID,
                 ACCESS_KEY_SECRET,
                 region=REGION)

    file_name = os.path.basename(file_path)

    # We also attach timestamp to file name to avoid conflicts
    now = str(int(time.time()))

    object_name = "%s.%s" % (file_name, now)

    container = driver.get_container(container_name=BUCKET_NAME)
    obj = container.upload_object(file_path=file_path, object_name=object_name)

    print(("Object uploaded to: %s/%s" % (BUCKET_NAME, object_name)))
    print(obj)


def download_coverage_files(destination_path):
    if not os.path.isdir(destination_path):
        raise ValueError(
            "%s path doesn't exist or it's not a directory" % (destination_path)
        )

    print("Downloading files from S3")

    cls = get_driver(Provider.S3)
    driver = cls(ACCESS_KEY_ID,
                 ACCESS_KEY_SECRET,
                 region=REGION)
    container = driver.get_container(container_name=BUCKET_NAME)

    for index, obj in enumerate(container.list_objects()):
        print(("Downloading object %s" % (obj.name)))

        obj_time = obj.name.split("/")[-1].rsplit(".")[-1]
        obj_destination_path = os.path.join(
            destination_path, ".coverage.%s.%s" % (index, obj_time)
        )
        obj.download(destination_path=obj_destination_path, overwrite_existing=True)

        print(("Obj %s downloaded to %s" % (obj.name, destination_path)))


