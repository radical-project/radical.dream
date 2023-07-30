import copy
from typing import OrderedDict, Optional, Union
from concurrent.futures import Future
from ..services.data.volumes import PersistentVolume, PersistentVolumeClaim


class Task(Future):
    """
    Base class for a cloud Task instance.
    """
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

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
        self.type     = None
        self.cmd      = None
        self.args     = list
        self.image    = None
        self.state    = None
        self.restart  = None
        self.inputs   = None
        self.outputs  = []
        self.volume   : Optional[Union[PersistentVolume,
                                       PersistentVolumeClaim]] = None
        self.depends_on = []


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
    def restart(self) -> str:
        return self.__restart
    
    @restart.setter
    def restart(self, restart):
        self.__restart = restart


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
    def args(self) -> list:
        return self.__args


    @args.setter
    def args(self, args) -> list:
        self.__args = args


    @property
    def image(self) -> str:
        return self.__image


    @image.setter
    def image(self, image) -> list:
        self.__image = image

    
    def exit_code(self):
        pass


    def is_service(self):
        """True if task needs to run for every.
           if True then self.retry() must be True
        """
        pass


    def reset_state(self):
        self.state = None
        self._state = 'PENDING'
        self._exception = None


    def pending(self):
        return self._state == 'PENDING'


class Pod:
    global containers
    containers = []
    
    def add_containers(tasks: list):
        for t in tasks:
            containers.append(t)

    def get_containers():
        return containers


class MPI_Pod(Pod):
    pass


class BatchTasks:
    """
    Represents a batch of replicated tasks.

    Args:
        replicas (int): The number of task replicas to create.
        task (Task): The original task to be replicated.

    Raises:
        Exception: If replicas argument is not provided or evaluates to False.

    Returns:
        list: A list containing the replicated tasks futures to the user.

    Example:
        replicas = 3
        task = Task(...)
        batch = BatchTasks(replicas, task)
        # batch.tasks will contain a list of three replicated task instances.
    """

    def __init__(self, replicas, task: Task) -> None:
        
        self.replicas = replicas
        if not self.replicas:
            raise Exception('BatchTasks type requires replicas to be set')
        
        tasks = []
        for _ in range(self.replicas):
            _task = copy.copy(task)
            tasks.append(_task)

        return self


    
