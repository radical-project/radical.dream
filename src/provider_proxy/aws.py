"""
Script which uploads coverage data files to a S3 bucket.
"""

from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import time

from src.provider_proxy.proxy import proxy

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

class AWS(object):
    def __init__(self, proxy_mgr: proxy):

        if proxy:
            if isinstance(proxy_mgr, proxy):
                if 'aws' in proxy_mgr.loaded_providers:
                    cred = proxy_mgr._load_credentials('aws')
        else:
            raise ('Provider AWS not loaded, existing....')
        
        cls = get_driver(Provider.S3)
        self.driver = cls(cred['aws_access_key_id'], 
                          cred['aws_secret_access_key'],
                          cred['region_name'])

    def list_s3_buckets(self):
        """
        This function list the avilabe buckets within
        a given account
        """
        container_list = list(self.driver.iterate_containers())
        return sorted([c.name for c in container_list])


    def create_s3_bucket(self, bucket_name: str):

        bucket = self.driver.create_container(bucket_name)
        return bucket
    

    def get_bucket_obj(self, bucket_name: str):
        bucket = self.driver.get_container(bucket_name)
        return bucket


    def delete_s3_bucket(self, bucket_name):

        s3_buckets = self.list_s3_buckets()
        if bucket_name in s3_buckets:

            bucket = self.get_bucket_obj(bucket_name)
            self.driver.delete_container(bucket)
            print('bucket {0} is deleted'.format(bucket_name))
            return True
        else:
            print('bucket {0} does not exist'.format(bucket_name))
            return False


    def upload_to_s3_bucket(self, file_path, bucket_name):
        """
        :param file_path: Path to the coverage file to upload.
        """
        if not os.path.isfile(file_path):
            raise ValueError("File %s doesn't exist" % (file_path))

        print("Uploading {0} file to S3".format(file_path))

        file_name = os.path.basename(file_path)

        bucket = self.get_bucket_obj(bucket_name)
        obj    = bucket.upload_object(file_path=file_path, 
                                      object_name=file_name)

        print(("Object uploaded to: %s/%s" % (bucket_name, file_name)))
        return obj.get_cdn_url()


    def download_from_s3_bucket(self, bucket_name, file_name, destination_path):
        if not os.path.isdir(destination_path):
            raise ValueError(
                "%s path doesn't exist or it's not a directory" % (destination_path))

        print("Downloading files from S3")

        bucket = self.get_bucket_obj(bucket_name)

        for index, obj in enumerate(bucket.list_objects()):
            if obj.name == file_name:
                print(("Downloading object %s" % (obj.name)))

                obj.download(destination_path, overwrite_existing=True)

                print(("Obj %s downloaded to %s" % (obj.name, destination_path)))

                return True