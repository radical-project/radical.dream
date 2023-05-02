import uuid
import queue
import threading     as mt
import radical.utils as ru

from typing                 import List
from hydraa.cloud_vm        import vm
from hydraa.cloud_task.task import Task
from hydraa.providers.proxy import proxy

from hydraa.services.caas_manager.utils      import misc
from hydraa.services.caas_manager.chi_caas   import ChiCaas
from hydraa.services.caas_manager.aws_caas   import AwsCaas
from hydraa.services.caas_manager.jet2_caas  import Jet2Caas
from hydraa.services.caas_manager.azure_caas import AzureCaas


AWS    = 'aws'
CHI    = 'chameleon'
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
    def __init__(self, proxy_mgr, vms, asynchronous):
        
        _id = str(uuid.uuid4())
        sandbox = misc.create_sandbox(_id)

        self._registered_managers = {}
        self.prof = ru.Profiler
        self.log = misc.logger(path='{0}/{1}.log'.format(sandbox, 'caas_manager'))
        self._terminate  = mt.Event()

        if proxy:
            self._proxy = proxy_mgr

        # TODO: add the created classes based on the loaded
        #       providers instead of only provider name. This
        #       will help for less code and easier shutdown.

        for provider in self._proxy._loaded_providers:
            if provider == AZURE:
                cred = self._proxy._load_credentials(AZURE)
                vmx  = next(v for v in vms if isinstance(v, vm.AzureVM))
                self.AzureCaas = AzureCaas(sandbox, _id, cred, vmx, asynchronous, self.log, self.prof)
                self._registered_managers[AZURE] = {'class' : self.AzureCaas,
                                                    'run_id': self.AzureCaas.run_id,
                                                    'in_q'  : self.AzureCaas.incoming_q,
                                                    'out_q' : self.AzureCaas.outgoing_q}
            if provider == AWS:
                cred = self._proxy._load_credentials(AWS)
                vmx  = next(v for v in vms if isinstance(v, vm.AwsVM))
                self.AwsCaas = AwsCaas(sandbox, _id, cred, vmx, asynchronous, self.log, self.prof)
                self._registered_managers[AWS] = {'class' : self.AwsCaas,
                                                  'run_id': self.AwsCaas.run_id,
                                                  'in_q'  : self.AwsCaas.incoming_q,
                                                  'out_q' : self.AwsCaas.outgoing_q}

            # TODO: merge Jet2cass and ChiCaas in one class 
            if provider == JET2:
                cred = self._proxy._load_credentials(JET2)
                vmx  = next(v for v in vms if v.Provider == JET2)
                self.Jet2Caas = Jet2Caas(sandbox, _id, cred, vmx, asynchronous, self.log, self.prof)
                self._registered_managers[JET2] = {'class' : self.Jet2Caas,
                                                   'run_id': self.Jet2Caas.run_id,
                                                   'in_q'  : self.Jet2Caas.incoming_q,
                                                   'out_q' : self.Jet2Caas.outgoing_q}

            if provider == CHI:
                cred = self._proxy._load_credentials(CHI)
                vmx  = next(v for v in vms if v.Provider == CHI)
                self.ChiCaas = ChiCaas(sandbox, _id, cred, vmx, asynchronous, self.log, self.prof)
                self._registered_managers[CHI] = {'class' : self.ChiCaas,
                                                  'run_id': self.ChiCaas.run_id,
                                                  'in_q'  : self.ChiCaas.incoming_q,
                                                  'out_q' : self.ChiCaas.outgoing_q}

            
            
            self._get_result = mt.Thread(target=self._get_results, args=(self._registered_managers[provider],),
                                                                                     name="CaaSManagerResult")
            self._get_result.daemon = True

            self._get_result.start()

   
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
        if provider:
            if provider in self._proxy._loaded_providers:
                self._registered_managers[provider]['class']._get_run_status(run_id)
        else:
            for manager_k, manager_attrs in self._registered_managers.items():
                manager_attrs['class']._get_run_status(run_id)


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
    def _get_results(self, manager_attrs):
        """
        check if the contianer is still executing or
        Done/failed
        """
        manager_queue = manager_attrs['out_q']
        manager_name  = manager_attrs['class'].vm.Provider

        while not self._terminate.is_set():
            try:
                msg = manager_queue.get(block=True, timeout=0.1)
                if msg:
                    self.log.trace('manager {0} reported: {1}'.format(manager_name,
                                                                              msg))
            except queue.Empty:
                continue


    # --------------------------------------------------------------------------
    #
    def submit(self, tasks: List[Task]):
        """
        submit contianers and wait for them or not.
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        # NOTE: soon we will have an orchestrator to distribuite the tasks
        # based on:
        # 1- user provider preference (if user set the provider of the task)
        # 2- or based on user resousource requirement (orchestrator decision)
        tasks_counter = 0
        for manager_k, manager_attrs in self._registered_managers.items():
            for task in tasks:
                if task.provider == manager_k:
                    manager_attrs['in_q'].put(task)
                    print('submitting tasks: ', tasks_counter, end='\r')
                    tasks_counter +=1

                if task.provider not in self._registered_managers.keys():
                    self.log.trace('no manager ({0}) found for task {0}'.format(task.provider,
                                                                                        task))


    # --------------------------------------------------------------------------
    #
    def shutdown(self, provider=None):
        """
        shudown the manager(s) by deleting all the 
        previously created components by the user
        """
        self._terminate.set()

        if provider:
            if provider in self._proxy._loaded_providers:
                print('terminating manager {0}'.format(self._registered_managers[provider]))
                self._registered_managers[provider]['class']._shutdown()

        else:
            print('shutting down all managers and wait for resource termination')
            for manager_k, manager_attrs in self._registered_managers.items():
                print('terminating manager {0}'.format(manager_k))
                manager_attrs['class']._shutdown()
