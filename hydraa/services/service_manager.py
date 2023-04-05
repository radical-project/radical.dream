import uuid
from hydraa.services.caas_manager.utils import misc

_id = str(uuid.uuid4())

class service_manager():
    def __init__(self, managers=[]) -> None:
        self.sandbox  = misc.create_sandbox(_id)
        self.managers = managers
    
    def start(self):
        for manager in self.managers:
            manager.start(self.sandbox, _id)
