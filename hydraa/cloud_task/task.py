import sys
import socket
from typing import OrderedDict

class Task(object):
    """
    Base class for a cloud Task instance.
    """
    def __init__(self):
        self.id       = int
        self.name     = str
        self.run_id   = None
        self.provider = None
        self.memory   = 0.1
        self.vcpus    = 0.1
        self.env_var  = None
        self.ip       = None
        self.port     = None
        self.arn      = None
        self.arch     = None

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
    
    def port(self):
        """represents the port of a task"""
        return self.port


    @property
    def ip(self):
        """represents the ip of a task"""
        return self.__ip
    
    @ip.setter
    def ip(self, ip):
        self.__ip = ip


    @property
    def provider(self):
        """represents the ptovider of that task (AZURE, AWS, G-CLOUD)"""
        return self.__provider
    
    @provider.setter
    def provider(self, provider):
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
    def env_var(self):
        return self.__env_var
    
    @env_var.setter
    def env_var(self, env_var):
        self.__env_var = env_var


    @property
    def arn(self) -> str:
        return self.__arn

    @arn.setter
    def arn(self, arn):        
        self.__arn = arn


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
        self.__memory = memory


    @property
    def vcpus(self) -> float:
        return self.__vcpus

    @vcpus.setter
    def vcpus(self, vcpus) -> float:
        self.__vcpus = vcpus


    @property
    def state(self) -> str:
        return self.__state

   
    @state.setter
    def state(self, state):
        self.__state = state


    @property
    def arch(self) -> str:
        return self.__arch
    

    @arch.setter
    def arch(self, arch):
        self.__arch = arch
    
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
        self.__cmd = cmd
    
    @property
    def image(self) -> str:
        return self.__image


    @image.setter
    def image(self, image) -> list:
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
