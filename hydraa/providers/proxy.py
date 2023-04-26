import os
import boto3
import openstack

from botocore.exceptions import NoCredentialsError
from botocore.exceptions import InvalidConfigError

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources import ResourceManagementClient

AWS    = 'aws'
CHI    = 'chameleon'
JET2   = 'jetstream2'
AZURE  = 'azure'
GCLOUD = 'google'


class proxy:
    """ 
        For now we assume that the user can login only with a permenant credentials.
        best practice is:
        1- not to embed the keys in the code
        2- to use IAM role (aws) or any alternative for another
           cloud provider to gain temproray access.
        3- Check: https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html
    """

    def __init__(self, providers):

        if not isinstance(providers, list):
            providers = [providers]

        self._providers           = [pr.lower() for pr in providers]
        self._loaded_providers    = []
        self._loaded_credentials  = {}
        self._supported_providers = [AWS, AZURE, GCLOUD, JET2, CHI]
        self._login()


    def _login(self):
        for provider in self._providers:
            if provider in self._supported_providers:
                self._verify_credentials(provider)
                self._loaded_providers.append(provider)
            else:
                print('{0} provider not supported'.format(provider))
        
        print('login to: {0} succeed'.format(self._loaded_providers))


    def _verify_credentials(self, provider):
        '''
        check if the provided credentials are valid.
        '''
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
                raise Exception('failed to login to {0}: {1}'.format(provider, e))

        if provider == AZURE:
            azu_creds  = self._load_credentials(provider)
            credential = DefaultAzureCredential()
            azu_client = ResourceManagementClient(credential=credential, 
                                                  subscription_id=azu_creds['az_sub_id'])
            try:
                for _ in azu_client.resource_groups.list():
                    pass
            except Exception as e:
                raise Exception('failed to login to {0}: {1}'.format(provider, e))

        if provider == JET2:
            jet2_creds  = self._load_credentials(provider)
            jet2_client = openstack.connect(**jet2_creds)
            try:
                jet2_client.list_flavors()
            except Exception as e:
                raise Exception('failed to login to {0}: {1}'.format(provider, e))

        if provider == CHI:
            chi_creds  = self._load_credentials(provider)
            chi_client = openstack.connect(**chi_creds)
            try:
                chi_client.list_flavors()
            except Exception as e:
                raise Exception('failed to login to {0}: {1}'.format(provider, e))

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

        if provider == JET2:
            try:
                jet2_creds = {'auth_url'                     : os.environ['JET_OS_AUTH_URL'],
                              'auth_type'                    : os.environ['JET_OS_AUTH_TYPE'],
                              'compute_api_version': 2,
                              'identity_interface'           : os.environ['JET_OS_INTERFACE'],
                              'application_credential_secret': os.environ['JET_OS_APPLICATION_CREDENTIAL_SECRET'],
                              'application_credential_id'    : os.environ['JET_OS_APPLICATION_CREDENTIAL_ID']}

                return jet2_creds
            except KeyError:
                raise

        if provider == CHI:
            try:
                chi_creds = {'auth_url'           : os.environ['CHI_OS_AUTH_URL'],
                             'auth_type'          : os.environ['CHI_OS_AUTH_TYPE'],
                             'compute_api_version': 2,
                             'identity_interface' : os.environ['CHI_OS_INTERFACE'],
                             'application_credential_secret': os.environ['CHI_OS_APPLICATION_CREDENTIAL_SECRET'],
                             'application_credential_id'    : os.environ['CHI_OS_APPLICATION_CREDENTIAL_ID']}
                return chi_creds
            except KeyError:
                raise

        if provider == GCLOUD:
             raise NotImplementedError
                

    
        
        
