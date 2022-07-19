import sys
from typing import OrderedDict

class Task(object):
    """
    Base class for a cloud Task instance.
    """
    def __init(self):
        self.id       = None
        self.name     = None
        self.run_id   = None
        self.provider = None
        self.memory   = 0.0
        self.vcpus    = 0.0

        self.cmd      = None
        self.image    = None

        self.state    = None


    def name(self):
        return self.name
    

    def id(self):
        return self.id


    def run_id(self):
        """represents the run id that the task belongs to"""
        return self.run_id

    
    @property
    def provider(self):
        """represents the ptovider of that task (AZURE, AWS, G-CLOUD)"""
        return self.__provider
    
    @provider.setter
    def provider(self, provider):
        providers = ['aws', 'azure', 'google']
        if provider not in providers:
            raise ValueError('task provider not supported') 
        self.__provider = provider
    

    @property
    def launch_type(self):
        """represents the launch_type of that task (EC2, B1)"""
        return self.__launch_type


    @launch_type.setter
    def launch_type(self, launch_type):
        launch_types = ['EC2', 'B1']
        if launch_type not in launch_types:
            raise ValueError('launch type not supported') 
        self.__launch_type = launch_type


    @property
    def arn(self) -> str:
        return self.__arn

    @arn.setter
    def arn(self, arn):        
        self.__arn = arn

    @property
    def ip(self) -> str:
        return self.ip


    @property
    def dns(self) -> str:
        pass


    @property
    def retry(self) -> bool:
        pass


    @property
    def retry_number(self) -> bool:
        pass


    @property
    def memory(self) -> float:
        return self.__memory


    @memory.setter
    def memory(self, memory) -> float:
        if memory <= 0.0:
            raise ValueError('memory per ctask must be > 0.0')
        self.__memory = memory


    @property
    def vcpus(self) -> float:
        return self.__vcpus

    @vcpus.setter
    def vcpus(self, vcpus) -> float:
        if vcpus <= 0.0:
            raise ValueError('vcpus per ctask must be > 0.0')
        self.__vcpus = vcpus


    @property
    def state(self) -> str:
        return self.__state


    @state.setter
    def state(self, state):
        self.__state = state


    
    @property
    def events(self) -> OrderedDict:
        return self.__events
    

    @events.setter
    def events(self, events):
        self.__events = events


    @property
    def cmd(self) -> list:
        return self.__cmd


    @cmd.setter
    def cmd(self, cmd) -> list:
        if not cmd:
            raise ValueError('cmd per ctask must be set')
        self.__cmd = cmd
    
    @property
    def image(self) -> str:
        return self.__image


    @image.setter
    def image(self, image) -> list:
        if not image:
            raise ValueError('image per ctask must be set')
        self.__image = image

    @property
    def result(self):
        pass

    
    def exit_code(self):
        pass


    def is_service(self):
        """True if task needs to run for every.
           if True then self.retry() must be True
        """
        pass
