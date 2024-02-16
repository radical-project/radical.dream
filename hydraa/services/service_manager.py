import uuid
from hydraa.services.caas_manager.utils import misc

_id = str(uuid.uuid4())

class service_manager():
    def __init__(self, managers) -> None:

        self.managers = managers
        self.sandbox  = misc.create_sandbox(_id)


    def start_services(self):
        for service_manager in self.managers:
            service_manager.start(self.sandbox, _id)
    

    def shutdown_services(self):
        for service_manager in self.managers:
            service_manager.shutdown()
