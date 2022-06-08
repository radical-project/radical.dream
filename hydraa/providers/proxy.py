import os
import json
import boto3

from botocore.exceptions import NoCredentialsError
from botocore.exceptions import InvalidConfigError

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources import ResourceManagementClient

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

        self.loaded_providers     = []
        self._loaded_credentials  = {}


    def login(self, providers: list):

        for provider in providers:
            if provider in  [AWS, AZURE, GCLOUD]:
                self._verify_credentials(provider)
                self.loaded_providers.append(provider)
                print('Login to {0} succeed'.format(provider))
            else:
                print('{0} provider not supported'.format(provider))

    
    def _verify_credentials(self, provider):
        '''
        check if the provided credentials are valid.
        '''
        print('Verifying {0} credentials'.format(provider))
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
            azu_creds  = self._load_credentials(provider)
            credential = DefaultAzureCredential()
            azu_client = ResourceManagementClient(credential=credential, 
                                                  subscription_id=azu_creds['az_sub_id'])
            for resource_group in azu_client.resource_groups.list():
                pass
            
        if provider == GCLOUD:
            raise NotImplementedError

    def _load_credentials(self, provider):
        """
        provider : AWS, Azure, Google Cloud
        fetch_tye: config_file, env_vars
        """
        if provider in self._loaded_credentials:
            return self._loaded_credentials[provider]

        if provider == AWS:
            try:
                aws_creds = {'aws_access_key_id'     : os.environ['ACCESS_KEY_ID'],
                             'aws_secret_access_key' : os.environ['ACCESS_KEY_SECRET'],
                             'region_name'           : os.environ['AWS_REGION']}
                self._loaded_credentials[provider] = aws_creds
                return aws_creds
            except KeyError:
                raise

        if provider == AZURE:
            try:
                azu_creds = {'az_sub_id'    : os.environ['AZURE_SUBSCRIPTION_ID'],
                             'region_name'  : os.environ['AZURE_REGION'],
                             'az_batch_name': os.environ.get('AZURE_BATCH_NAME', None),
                             'az_batch_url' : os.environ.get('AZURE_BATCH_URL',  None),
                             'az_batch_pkey': os.environ.get('AZURE_BATCH_PKEY', None),
                             'az_batch_skey': os.environ.get('AZURE_BATCH_SKEY', None)}
                return azu_creds
            except KeyError:
                raise

        if provider == GCLOUD:
             raise NotImplementedError

    
        
        
