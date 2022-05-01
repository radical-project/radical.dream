from src.provider_proxy import proxy
from src.service_proxy.caas_manager.aws_ecr import AWS_ECR

AWS    = 'aws'
AZURE  = 'azure'
GCLOUD = 'google'

class CaasManager(AWS_ECR):
    def __init__(self, proxy_mgr):
        if proxy:
            self._proxy = proxy_mgr

    def get_container_cost(self):
        """
        calculate the cost of executing a 
        container on a provider
        """
        raise NotImplementedError

    def get_container_status(self, ID):
        """
        check if the contianer is still executing or 
        Done/failed
        """
        raise NotImplementedError
    
    def async_execute_container(self, provider, container_path=None):
        """
        execute contianer and do not wait for it
        Ideally when container_path is provided it means 
        we need to upload it to aws and use it.
        """
        if provider == AWS:
            cred = self._proxy._load_credentials('aws')
            self._deploy_contianer_to_aws(cred, container_path)
        
        if provider == AZURE:
            raise NotImplementedError
        
        if provider == GCLOUD:
            raise NotImplementedError 

    def sync_execute_container(self, provider, container_path):
        """
        execute contianer and wait for it
        """
        raise NotImplementedError