""" A class to manage different providers' instances for the CaaS application."""
import uuid
import queue
import threading as mt
import radical.utils as ru

from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.utils import misc
from hydraa.services.caas_manager.chi_caas import ChiCaas
from hydraa.services.caas_manager.aws_caas import AwsCaas
from hydraa.services.caas_manager.jet2_caas import Jet2Caas
from hydraa.services.caas_manager.azure_caas import AzureCaas

AWS = 'aws'
AZURE = 'azure'
GCLOUD = 'google'
CHI = 'chameleon'
JET2 = 'jetstream2'

PROVIDER_TO_CLASS = {
    AWS: AwsCaas,
    CHI: ChiCaas,
    JET2: Jet2Caas,
    AZURE: AzureCaas}

# --------------------------------------------------------------------------
#
class CaasManager:
    """
    The `CaasManager` class is responsible for managing different providers'
    instances for the Cloud-as-a-Service (CaaS) application. It initializes
    instances of provider-specific classes and starts threads to handle results.

    Parameters
    ----------
    proxy_mgr : ProxyManager
        An instance of the proxy manager to load credentials and providers.

    vms : list
        A list of virtual machine instances.

    asynchronous : bool
        Indicates whether the processing should be asynchronous.

    Attributes
    ----------
    _registered_managers : dict
        A dictionary to store registered manager instances by provider name.

    prof : ru.Profiler
        A profiler instance for profiling.

    _terminate : mt.Event
        An event to signal termination.

    log : Logger
        A logger instance for logging.

    _proxy : ProxyManager
        An instance of the proxy manager (optional).
    """


    # --------------------------------------------------------------------------
    #
    def __init__(self, proxy_mgr: proxy, vms: list, asynchronous: bool,
                 auto_terminate: bool = True) -> None:
        """
        Initialize the CaasManager.

        Parameters
        ----------
        proxy_mgr : ProxyManager
            An instance of the proxy manager to load credentials and providers.

        vms : list
            A list of virtual machine instances.

        asynchronous : bool
            Indicates whether the processing should be asynchronous.

        auto_terminate : bool
            Indicates whether the manager should terminate automatically.
        """

        _id = str(uuid.uuid4())
        self.prof = ru.Profiler
        self._proxy = proxy_mgr
        self._terminate = mt.Event()
        self._registered_managers = {}
        self.sandbox = misc.create_sandbox(_id)
        self.log = misc.logger(path=f'{self.sandbox}/caas_manager.log')

        for provider in self._proxy.loaded_providers:
            if provider in PROVIDER_TO_CLASS:
                cred = self._proxy._load_credentials(provider)
                vmx = [v for v in vms if v.Provider == provider]
                caas_class = PROVIDER_TO_CLASS[provider]
                caas_instance = caas_class(self.sandbox, _id, cred, vmx,
                                           asynchronous, auto_terminate,
                                           self.log, self.prof)

                self._registered_managers[provider] = {'class' : caas_instance,
                                                       'run_id': caas_instance.run_id,
                                                       'in_q'  : caas_instance.incoming_q,
                                                       'out_q' : caas_instance.outgoing_q}
                setattr(self, caas_class.__name__, caas_instance)

            self._get_result = mt.Thread(target=self._get_results,
                                         name="CaaSManagerResult",
                                         args=(self._registered_managers[provider],))
            self._get_result.daemon = True
            self._get_result.start()


    # --------------------------------------------------------------------------
    #
    def _get_results(self, manager_attrs):
        """
        check if the contianer is still executing or
        Done/failed
        """
        manager_queue = manager_attrs['out_q']
        manager_name = manager_attrs['class'].__class__.__name__

        while not self._terminate.is_set():
            try:
                msg = manager_queue.get(block=True, timeout=0.1)
                if msg:
                    self.log.trace(f'{manager_name} reported: {msg}')
            except queue.Empty:
                continue


    # --------------------------------------------------------------------------
    #
    def submit(self, tasks):
        """
        submit contianers and wait for them or not.
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        if not self._registered_managers:
            raise RuntimeError('No CaaS managers found to submit to.')

        tasks_counter = 0
        for task in tasks:
            task_provider = task.provider.lower()
            if self._registered_managers.get(task_provider, None):
                manager = self._registered_managers.get(task_provider)
            else:
                manager = next(iter(self._registered_managers.values()))
                self.log.warning('no manager found for this task, submitting to a any manager')

            print('submitting tasks: ', tasks_counter, end='\r')
            manager['in_q'].put(task)
            tasks_counter += 1


    # --------------------------------------------------------------------------
    #
    def shutdown(self, provider=None):
        """
        shudown the manager(s) by deleting all the
        previously created components by the user
        """
        self._terminate.set()

        if provider:
            if provider in self._registered_managers:
                print(f'terminating manager {self._registered_managers[provider]}')
                self._registered_managers[provider]['class'].shutdown()

        else:
            print('shutting down all managers and wait for resource termination')
            for manager_k, manager_attrs in self._registered_managers.items():
                print(f'terminating manager {manager_k}')
                manager_attrs['class'].shutdown()
