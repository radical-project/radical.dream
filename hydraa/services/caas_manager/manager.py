import uuid
from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.aws_caas   import AwsCaas
from hydraa.services.caas_manager.azure_caas import AzureCaas

AWS    = 'aws'
AZURE  = 'azure'
GCLOUD = 'google'

# --------------------------------------------------------------------------
#
class CaasManager:
    """
    ctask: container task
    """

    # TODO: we might need to pass a TaskDescription
    #       class that contains:
    #       TaskDescription.AwsTaskDefination
    #       TaskDescription.AwsContainerDefination
    #       TaskDescription.AwsService


    # --------------------------------------------------------------------------
    #
    def __init__(self, proxy_mgr, asynchronous):
        
        _id = str(uuid.uuid4())
        self._registered_managers = []
        

        if proxy:
            self._proxy = proxy_mgr
        
        for provider in self._proxy.loaded_providers:
            if provider == AWS:
                cred = self._proxy._load_credentials(AWS)
                self.AwsCaas = AwsCaas(_id, cred, asynchronous)
            if provider == AZURE:
                cred = self._proxy._load_credentials(AZURE)
                self.AzureCaas = AzureCaas(_id, cred, asynchronous)
            if provider == GCLOUD:
                raise NotImplementedError
            
    # --------------------------------------------------------------------------
    #
    def get_ctask_cost(self, provider):
        """
        calculate the cost of executing a 
        container on a provider
        """
        if provider == AWS:
            pass
        if provider == AZURE:
            pass
        
        if provider == GCLOUD:
            raise NotImplementedError 


    # --------------------------------------------------------------------------
    #
    def get_ctask_status(self, ID):
        """
        check if the contianer is still executing or 
        Done/failed
        """
        raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def execute_ctask_batch(self, launch_type, batch_size, cpu, memory,
                                budget=0, time=0, container_path=None):
        """
        execute contianers and wait for it. Ideally when
        container_path is provided it means we need to
        upload it to aws and use it.
        """
        # TODO: pass a ctask description
        #       via the user
        if AWS in self._proxy.loaded_providers:
            run_id = self.AwsCaas.run(launch_type, batch_size, budget, cpu, memory,
                                                                              time)
            return run_id
        
        if AZURE in self._proxy.loaded_providers:
            raise NotImplementedError
        
        if GCLOUD in self._proxy.loaded_providers:
            raise NotImplementedError 


    # --------------------------------------------------------------------------
    #
    def execute_cjob_batch(self, launch_type, batch_size, cpu, memory,
                               budget=0, time=0, container_path=None):
        raise NotImplementedError
    
    # --------------------------------------------------------------------------
    #
    def shutdown(self, provider):
        """
        shudown the manager(s) by deleting all the 
        previously created components by the user
        """
        if provider == AWS:
            self.AwsCaas._shutdown()
        
        if provider == AZURE:
            raise NotImplementedError
        
        if provider == GCLOUD:
            raise NotImplementedError 




