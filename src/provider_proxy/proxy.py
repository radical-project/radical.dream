import os
import json
import boto3

from botocore.exceptions import NoCredentialsError
from botocore.exceptions import InvalidConfigError

AWS    = 'aws'
AZURE  = 'azure'
GCLOUD = 'google'

class proxy(object):
    """ 
        For now we assume that the user can login only with a permenant credentials.
        best practice is:
        1- not to embed the keys in the code
        2- to use IAM role (aws) or any alternative for another
           cloud provider to gain temproray access.
        3- Check: https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html
    """

    def __init__(self):

        self.loaded_providers    = []
        self._loaded_credentials  = {}
    
    def login(self, providers: list):

        for provider in providers:
            if provider in  [AWS, AZURE, GCLOUD]:
                self._verify_credentials(provider)
                self.loaded_providers.append(provider)
                print('login to {0} succeed'.format(provider))
            else:
                print('{0} provider not supported'.format(provider))

    
    def _verify_credentials(self, provider):
        '''
        check if the provided credentials are valid.
        '''
        print('verifying {0} loaded credentials'.format(provider))
        if provider == AWS:
            try:
                # get AWS credentials
                aws_creds = self._load_credentials(provider)
                aws_client = boto3.client('sts', **aws_creds)
                aws_client.get_caller_identity()

            except NoCredentialsError:
                print(f'{provider},--- no credentials --')
            except InvalidConfigError:
                print(f'{provider},--- invalid config --')
            except Exception as e:
                raise

        if provider == AZURE:
            raise NotImplementedError
        if provider == GCLOUD:
            raise NotImplementedError

    def _load_credentials(self, provider):
        """
        provider : AWS, Azure, Google Cloud
        fetch_tye: config_file, env_vars
        """
        if provider in self._loaded_credentials:
            return self._loaded_credentials[provider]

        print('loading {0} credentials'.format(provider))
        if provider == AWS:
            try:
                ACCESS_KEY_ID     = os.environ['ACCESS_KEY_ID']
                ACCESS_KEY_SECRET = os.environ['ACCESS_KEY_SECRET']
                REGION            = os.environ['AWS_REGION']
                
                aws_creds = {'aws_access_key_id'     : ACCESS_KEY_ID,
                             'aws_secret_access_key' : ACCESS_KEY_SECRET,
                             'region_name'           : REGION}
                self._loaded_credentials[provider] = aws_creds
                return aws_creds
            except KeyError:
                raise

        if provider == AZURE:
            try:
                AZ_TENANT_ID   = os.environ['az_tenant_id']
                AZ_SUB_ID      = os.environ['az_sub_id']
                AZ_APP_ID      = os.environ['az_app_id']
                AZ_APP_SEC_KEY = os.environ['az_app_sec']
                
                return (AZ_TENANT_ID, AZ_SUB_ID, AZ_APP_ID, AZ_APP_SEC_KEY)
            except KeyError:
                raise

        if provider == GCLOUD:
             raise NotImplementedError

    
        
        
