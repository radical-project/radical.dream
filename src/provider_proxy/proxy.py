import os
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

    def __init__(self, provider):

        if provider in AWS, AZURE, GCLOUD:
            self.provider = provider
        else:
            raise Exception('cloud provider not supported')
    
    def verify_credentials(self):
        '''
        check if the provided credentials are valid.
        '''

        if self.provider == AWS:
            try:
                k_id, k_sec, reg = load_credentials(self.provider)
                aws_client = boto3.client('sts')
                aws_client.get_caller_identity(k_id, k_sec, reg)
                return True

            except NoCredentialsError:
                print(f'{self.provider},--- no credentials --')
            except InvalidConfigError:
                print(f'{self.provider},--- invalid config --')
            except Exception as e:
                print(f'{self.provider}{e},--- exception --')
        
        if self.provider == AZURE:
            raise NotImplementedError
        if self.provider == GCLOUD:
            raise NotImplementedError

    def load_credentials(self, provider):
        """
        provider : AWS, Azure, Google Cloud
        fetch_tye: config_file, env_vars
        """
        if provider == AWS:
            try:
                ACCESS_KEY_ID     = os.environ['ACCESS_KEY_ID']
                ACCESS_KEY_SECRET = os.environ['ACCESS_KEY_SECRET']
                REGION            = os.environ['AWS_REGION']
                return ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION
            except KeyError:
                raise
        
        if provider == AZURE:
            try:
                AZ_TENANT_ID   = os.environ['az_tenant_id']
                AZ_SUB_ID      = os.environ['az_sub_id']
                AZ_APP_ID      = os.environ['az_app_id']
                AZ_APP_SEC_KEY = os.environ['az_app_sec']
                
                return AZ_TENANT_ID, AZ_SUB_ID, AZ_APP_ID, AZ_APP_SEC_KEY
            except KeyError:
                raise
        
        if provider == GCLOUD:
             raise NotImplementedError
        
        
