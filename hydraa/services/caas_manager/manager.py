from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.aws_caas import AwsCaas

AWS    = 'aws'
AZURE  = 'azure'
GCLOUD = 'google'

# --------------------------------------------------------------------------
#
class CaasManager(AwsCaas):
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
    def __init__(self, proxy_mgr):
        
        self._registered_managers = []

        if proxy:
            self._proxy = proxy_mgr
        
        for provider in self._proxy.loaded_providers:
            if provider == AWS:
                cred = self._proxy._load_credentials(AWS)
                AwsCaas.__init__(self, cred)
            if provider == AZURE:
                raise NotImplementedError
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
            self._aws_container_cost(1, 1, 400, 'hour')
        if provider == AZURE:
            raise NotImplementedError
        
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
    def sync_execute_ctask(self, launch_type, batch_size, cpu, memory,
                               budget=0, time=0, container_path=None):
        """
        execute contianers and wait for it. Ideally when
        container_path is provided it means we need to
        upload it to aws and use it.
        """
        # TODO: pass a ctask description
        #       via the user
        if AWS in self._proxy.loaded_providers:
            self.run(launch_type, batch_size, budget, cpu, memory, time)
        
        if AZURE in self._proxy.loaded_providers:
            raise NotImplementedError
        
        if GCLOUD in self._proxy.loaded_providers:
            raise NotImplementedError 


    # --------------------------------------------------------------------------
    #
    def async_execute_ctask(self, launch_type, batch_size, cpu, memory,
                                budget=0, time=0, container_path=None):
        """
        execute contianers and do not wait for it. Ideally when
        container_path is provided it means we need to
        upload it to aws and use it.
        """
        # TODO: pass a ctask description
        #       via the user
        if AWS in self._proxy.loaded_providers:
            self.run(launch_type, batch_size, cpu, memory, time)
        
        if AZURE in self._proxy.loaded_providers:
            raise NotImplementedError
        
        if GCLOUD in self._proxy.loaded_providers:
            raise NotImplementedError 
    
    # --------------------------------------------------------------------------
    #
    def shutdown(self, provider):
        """
        shudown the manager(s) by deleting all the 
        previously created components by the user
        """
        if provider == AWS:
            self._shutdown()
        
        if provider == AZURE:
            raise NotImplementedError
        
        if provider == GCLOUD:
            raise NotImplementedError 




