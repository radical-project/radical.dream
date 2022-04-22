import os
import boto3

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
                k_id, k_sec, reg = load_credentials(self.provider, 'env')
                client = boto3.client('sts')
                client.get_caller_identity(k_id, k_sec, reg)
                return True
            except:
                return False
                raise

    def load_credentials(self, provider, fetch_type):
        """
        provider : AWS, Azure, Google Cloud
        fetch_tye: config_file, env_vars
        """
        if provider == AWS:
            if fetch_type = 'env':
                ACCESS_KEY_ID     = os.environ.get('ACCESS_KEY_ID')
                ACCESS_KEY_SECRET = os.environ.get('ACCESS_KEY_SECRET')
                REGION            = "us-east-1"
            if fetch_type = 'config':
                
            return ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION

        if not ACCESS_KEY_ID:
            raise ValueError("AWS_ACCESS_KEY_ID env variable not set")

        if not ACCESS_KEY_SECRET:
            raise ValueError("AWS_ACCESS_KEY_SECRET env variable not set")
        
        
