import uuid
from typing import List
from hydraa.cloud_vm.vm import AwsVM
from hydraa.cloud_task.task import Task
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
        # TODO: add the created classes based on the loaded
        #       providers instead of only provider name. This
        #       will help for easier shutdown.
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
    def get_run_status(self, run_id, provider=None):
        """
        check if the entire run is still executing or 
        pending/done/failed
        """
        if not provider:
            for provider in self._proxy.loaded_providers:
                if provider == AWS:
                    return self.AwsCaas._get_run_status(run_id)
                if provider == AZURE:
                    return self.AzureCaas._get_run_status(run_id)
                if provider == GCLOUD:
                    raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def get_run_tree(self, run_id, provider=None):
        """
        get the run tree and structure
        """
        if not provider:
            for provider in self._proxy.loaded_providers:
                if provider == AWS:
                    self.AwsCaas._get_runs_tree(run_id)
                if provider == AZURE:
                    self.AzureCaas._get_runs_tree(run_id)
                if provider == GCLOUD:
                    raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def get_task_status(self, task_id):
        """
        check if the contianer is still executing or
        Done/failed
        """
        raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, VM: AwsVM, tasks: List[Task], launch_type = None,
                                           service=False, budget=0, time=0):
        """
        submit contianers and wait for them or not.
        """
        if AWS in self._proxy.loaded_providers:
            run_id = self.AwsCaas.run(VM, tasks, launch_type, service, budget, time)
            return run_id
        
        if AZURE in self._proxy.loaded_providers:
            run_id = self.AzureCaas.run(tasks, budget, time)
            return run_id
        
        if GCLOUD in self._proxy.loaded_providers:
            raise NotImplementedError 


    # --------------------------------------------------------------------------
    #
    def submit_jobs(self, jobs: List[Task], budget=0, time=0, container_path=None):
        raise NotImplementedError
    
    # --------------------------------------------------------------------------
    #
    def shutdown(self, provider):
        """
        shudown the manager(s) by deleting all the 
        previously created components by the user
        """
        if provider:
            if provider == AWS:
                self.AwsCaas._shutdown()
            
            if provider == AZURE:
                self.AzureCaas._shutdown()
            
            if provider == GCLOUD:
                raise NotImplementedError
            
