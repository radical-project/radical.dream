import uuid
from typing import List
from hydraa.cloud_vm.vm import AwsVM
from hydraa.cloud_task.task import Task
from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.aws_caas   import AwsCaas
from hydraa.services.caas_manager.jet2_caas import Jet2Caas
from hydraa.services.caas_manager.azure_caas import AzureCaas

AWS    = 'aws'
JET2   = 'jetstream2'
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
        for provider in self._proxy._loaded_providers:
            if provider == AWS:
                cred = self._proxy._load_credentials(AWS)
                self.AwsCaas = AwsCaas(_id, cred, asynchronous)
            if provider == AZURE:
                cred = self._proxy._load_credentials(AZURE)
                self.AzureCaas = AzureCaas(_id, cred, asynchronous)
            if provider == GCLOUD:
                raise NotImplementedError
            if provider == JET2:
                cred = self._proxy._load_credentials(JET2)
                self.Jet2Caas = Jet2Caas(_id, cred, asynchronous)
                
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
            for provider in self._proxy._loaded_providers:
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
            for provider in self._proxy._loaded_providers:
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
    def submit_tasks(self, VM, tasks: List[Task], service=False, budget=0, time=0):
        """
        submit contianers and wait for them or not.
        """
        if AWS in self._proxy._loaded_providers:
            run_id = self.AwsCaas.run(VM, tasks, service, budget, time)
            return run_id

        if AZURE in self._proxy._loaded_providers:
            run_id = self.AzureCaas.run(VM, tasks, budget, time)
            return run_id

        if GCLOUD in self._proxy._loaded_providers:
            raise NotImplementedError
        
        if JET2 in self._proxy._loaded_providers:
            run_id = self.Jet2Caas.run(VM, tasks, service, budget, time)
            return run_id


    # --------------------------------------------------------------------------
    #
    def submit_jobs(self, jobs: List[Task], budget=0, time=0, container_path=None):
        raise NotImplementedError
    
    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        """
        shudown the manager(s) by deleting all the 
        previously created components by the user
        """
        for provider in self._proxy._loaded_providers:
            if provider == AWS:
                self.AwsCaas._shutdown()
            
            if provider == AZURE:
                self.AzureCaas._shutdown()
            
            if provider == GCLOUD:
                raise NotImplementedError
            
            if provider == JET2:
                self.Jet2Caas._shutdown()
            
