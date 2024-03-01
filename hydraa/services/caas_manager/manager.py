""" A class to manage different providers' instances for the CaaS application."""
import uuid
import queue
import threading as mt
import radical.utils as ru

from hydraa import Task
from typing import Callable

from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.utils import misc
from hydraa.services.caas_manager.chi_caas import ChiCaas
from hydraa.services.caas_manager.aws_caas import AwsCaas
from hydraa.services.caas_manager.jet2_caas import Jet2Caas
from hydraa.services.caas_manager.azure_caas import AzureCaas
from hydraa.services.caas_manager.local_caas import LocalCaas

AWS = 'aws'
AZURE = 'azure'
LOCAL = 'local'
GCLOUD = 'google'
CHI = 'chameleon'
JET2 = 'jetstream2'

PROVIDER_TO_CLASS = {
    AWS: AwsCaas,
    CHI: ChiCaas,
    JET2: Jet2Caas,
    AZURE: AzureCaas,
    LOCAL: LocalCaas}

TERM_SIGNALS = {0: "Auto-terminate was set, terminating.",
                1: "No more tasks to process, terminating.",
                2: "User terminations requested, terminating.",
                3: "Internal failure is detected, terminating."}

TIMEOUT = 0.1

_id = str(uuid.uuid4())


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

        
        self.vms = vms
        self.sandbox = None
        self.prof = ru.Profiler
        self._proxy = proxy_mgr
        self._terminate = mt.Event()
        self._registered_managers = {}
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate


    # --------------------------------------------------------------------------
    #
    def start(self, sandbox, logger, profiler):

        self.porf = profiler
        self.logger = logger
        self.sandbox = sandbox

        providers = len(self._proxy.loaded_providers)

        print(f'session sandbox is created: {self.sandbox} with [{providers}] providers')

        for provider in self._proxy.loaded_providers:
            if provider in PROVIDER_TO_CLASS:
                cred = self._proxy._load_credentials(provider)
                vmx = [v for v in self.vms if v.Provider == provider]
                caas_class = PROVIDER_TO_CLASS[provider]
                caas_instance = caas_class(self.sandbox, _id, cred, vmx,
                                           self.asynchronous, self.auto_terminate,
                                           self.logger, self.prof)

                self._registered_managers[provider] = {'class' : caas_instance,
                                                       'run_id': caas_instance.run_id,
                                                       'in_q'  : caas_instance.incoming_q,
                                                       'out_q' : caas_instance.outgoing_q}
                setattr(self, caas_class.__name__, caas_instance)

            self._get_result = mt.Thread(target=self._get_results,
                                         name=f"{provider}-CaaSManagerResult",
                                         args=(self._registered_managers[provider],))
            self._get_result.daemon = True
            self._get_result.start()


    # --------------------------------------------------------------------------
    #
    def _get_results(self, manager_attrs):
        """
        Retrieve and process messages from a manager's output queue.

        This method continuously checks the manager's output queue for messages
        while the termination flag is not set. It handles termination signals
        and regular task report messages, logging appropriate information.

        Parameters:
        - manager_attrs (dict): A dictionary containing manager attributes,
        including the output queue ('out_q') and the manager class name.

        Returns:
        None

        Raises:
        - TypeError: If an unexpected message type is encountered.

        Notes:
        The method uses the provided manager attributes to access the manager's
        output queue and class name. It handles termination signals, logs task
        reports, and raises an exception for unexpected message types.

        Example:
        ```python
        manager_attributes = {'out_q': output_queue, 'class': MyManager}
        instance._get_results(manager_attributes)
        ```
        """
        manager_queue = manager_attrs['out_q']
        manager_name = manager_attrs['class'].__class__.__name__

        while not self._terminate.is_set():
            try:
                msg = manager_queue.get(block=True, timeout=TIMEOUT)
                # check if the provided msg is a termination signal
                # or a regular tasks report message from the manager
                if msg:
                    # Termination message
                    if isinstance(msg, tuple):
                        term_sig, prov = msg
                        term_msg = TERM_SIGNALS.get(term_sig)
                        print(term_msg)
                        self.shutdown(provider=prov)

                    # Report message
                    elif isinstance(msg, str):
                        self.logger.info(f'{manager_name} reported: {msg}')

                    # Unexpected message
                    else:
                        self.shutdown()
                        raise TypeError(f'Unexpected message type: {type(msg)}')

            except queue.Empty:
                continue


    # --------------------------------------------------------------------------
    #
    def __call__(self, func: Callable=None, provider='') -> Callable:
        """
        Decorator function to invoke the submit function of CaasManager with
        additional arguments.

        Parameters
        ----------
        provider : str
            The provider for the tasks.
        """

        if func is None:
            return lambda f: self.__call__(f, provider)

        def wrapper(*args, **kwargs):
            task = func(*args, **kwargs)

            if not isinstance(task, Task):
                raise ValueError(f'function must return object of type {Task}')

            if not task.provider:
                task.provider = provider

            self.submit(task)

            return task

        return wrapper


    # --------------------------------------------------------------------------
    #
    def submit(self, tasks):
        """
        This is our base submit method. This method allows the submission of tasks
        to a registered CaaS managers. If a single task is provided, it is converted
        to a list for consistency. The method iterates through the provided tasks,
        determines the associated manager based on the task's provider, and submits
        the task to the manager's input queue.

        Parameters:
        - tasks (list or object): A list of tasks or a single task to be submitted.

        Returns:
        None

        Raises:
        - RuntimeError: If no registered CaaS managers are found.

        Notes:
        The method checks the type of the 'tasks' parameter, ensuring it is a list.
        If no specific manager is found for a task, it defaults to submitting the
        task to any available manager, logging a warning.

        Example:
        ```python
        tasks_to_submit = [task1, task2, task3]
        instance.submit(tasks_to_submit)
        ```
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        if not self._registered_managers:
            raise RuntimeError('No CaaS managers found to submit to.')

        tasks_counter = 0

        for task in tasks:
            task._verify()
            task_provider = task.provider.lower()
            
            if self._registered_managers.get(task_provider, None):
                manager = self._registered_managers.get(task_provider)
            else:
                manager = next(iter(self._registered_managers.values()))
                self.logger.warning('no manager found for this task, submitting to a any manager')

            # now put the task in the corresponding sub-manager queue
            manager['in_q'].put(task)
            tasks_counter += 1

        print(f'{tasks_counter} task(s) has been submitted')


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
                print(f'terminating manager {provider}')
                self._registered_managers[provider]['class'].shutdown()

        else:
            print('shutting down all managers and wait for resource termination')
            for manager_k, manager_attrs in self._registered_managers.items():
                print(f'terminating manager {manager_k}')
                manager_attrs['class'].shutdown()
