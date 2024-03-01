import uuid
from hydraa.services.caas_manager.utils import misc

_id = str(uuid.uuid4())

class ServiceManager():
    def __init__(self, managers) -> None:

        if not isinstance(managers, list):
            managers = [managers]

        self.managers = managers
        self.sandbox  = misc.create_sandbox(_id)


    def start_services(self):
        for service_manager in self.managers:
            service_manager.start(self.sandbox)
    

    def shutdown_services(self):
        for manager in self.managers:
            manager.shutdown()
