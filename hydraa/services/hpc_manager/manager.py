import uuid
import queue
import parsl
import threading as mt
import radical.pilot as rp


class HpcManager:

    # --------------------------------------------------------------------------
    #
    def __init__(self, runtime_backend: list, entK_resource_descr=None, parsl_config=None):

        _id = str(uuid.uuid4())
        self._terminate  = mt.Event()
        self._registered_managers = {}

        if 'entk' in runtime_backend:
            if not entK_resource_descr:
                raise ValueError("entK_resource_descr must be provided for entk backend")
        if 'parsl' in runtime_backend:
            if not parsl_config:
                raise ValueError("parsl_config must be provided for parsl backend")
        
        self._runtime_backend = runtime_backend