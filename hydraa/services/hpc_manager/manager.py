import uuid
import threading as mt
import radical.entk as re

from parsl.config import Config


class HpcManager:

    # --------------------------------------------------------------------------
    #
    def __init__(self, runtime_backends: list):

        _id = str(uuid.uuid4())
        self._terminate = mt.Event()
        self._registered_backends = []
        self._supported_backends = {'entk': self._start_or_submit_entk,
                                    'parsl': self._start_or_submit_parsl}

        self._task_id = 0
        self.runtime_backends = runtime_backends

        for b in self.runtime_backends:
            backend = next(iter(b.keys())).lower()
            backend_entity = next(iter(b.values()))
            if self._supported_backends.get(backend):
                print('Starting backend: {0}'.format(backend))
                # start a new thread for the backend since this is the first time
                backend_thread = mt.Thread(target=self._supported_backends[backend],
                                           name=f"{backend}_Backend_Thread",
                                           args=(backend_entity,))
                backend_thread.daemon = True
                backend_thread.start()
                self._registered_backends.append(backend)
                setattr(self, backend, backend_thread) 
            else:
                raise ValueError("Unsupported backend: {0}".format(backend))


    def submit(self, backend, work):
        """
        Submit the workflow to the backend.
        """
        if backend not in self._registered_backends:
            raise ValueError("Backend is not registered.")
        else:
            self._supported_backends[backend](pipelines=work)

    def _start_or_submit_entk(self, resource_dict: dict={}, pipelines: list=[]):
        """
        Start the EnTK workflow.
        """
        if not isinstance(resource_dict, dict):
            raise TypeError("EnTK resource description must be a dictionary.")

        # startup mode
        if resource_dict and not pipelines:
            if not hasattr(self, 'entk'):
                self.appman = re.AppManager(autoterminate=False)
                self.appman.resource_desc = resource_dict
                # EnTK app manager has no status attribute
                # so we create a new one to track the status
                self.appman.status = re.states.RUNNING = True

        # submit mode
        else:
            if self.appman.status == re.states.RUNNING:
                self.appman.workflow = set(pipelines)
                self.appman.run()
            else:
                raise ValueError("EnTK AppManager is not running.")

    def _start_or_submit_parsl(self, config: dict, tasks: list):
        """
        Start the Parsl workflow.
        """
        if not isinstance(config, Config):
            raise TypeError("Parsl resource description must be a dictionary.")

        raise NotImplementedError("Parsl Backend is not yet supported.")
