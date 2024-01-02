import copy

from typing import List
from typing import Optional
from typing import OrderedDict
from concurrent.futures import Future


# --------------------------------------------------------------------------
#
class Task(Future):
    """Base class for a cloud Task instance.
    Task class representing a computational task in a workload/workflow.
    The Task class encapsulates all information required to execute a single
    task within a larger workflow. It contains attributes like the task ID, name,
    computational resource requirements, runtime parameters, and dependencies.
    The class allows programmatically defining tasks that can be run on various
    cloud infrastructure providers.

    Parameters
    ----------
    id : int
        Unique identifier for the task.

    name : str 
        Name of the task.

    vcpus : float
        Number of virtual CPUs to allocate. 

    memory : float
        Amount of memory in GB to allocate.

    run_id : str
        Identifier for the current run.

    provider : str
        Name of cloud provider.
        
    args : List[str]
        List of command line arguments.

    ip : Optional[str]
        IP address of the instance running this task.

    cmd : Optional[str]
        Command to execute for the task.

    depends_on : List[str]
        List of task IDs this task depends on.
        
    arn : Optional[str]
        ARN (AWS only) identifier for the task instance.

    type : Optional[str]
        Type of task (e.g. paramiko, docker).
        
    port : Optional[int]
        Port number to listen on.

    state : Optional[str]
        Current state of the task. 

    image : Optional[str]
        Docker image name to use for the task.

    env_var : Optional[str]
        Environment variable associated with the task.

    restart : Optional[bool]
        Whether the task should restart on failure.
        
    outputs : Optional[List[str]]
        List of output keys generated by the task.

    inputs : Optional[List[str]]
        List of input keys required by the task.
        
    volume : Optional[Union[PersistentVolume, PersistentVolumeClaim]]
        Volume to mount for remote storage.

    """


    # --------------------------------------------------------------------------
    def __init__(self,
                 id: int = None,
                 tries: int = 0,
                 cmd: list = [],
                 args: list = [],
                 name: str = None,
                 image: str = None,
                 run_id: str = None,
                 vcpus: float = 0.0,
                 memory: float = 0.0,
                 provider: str = None,
                 ip: Optional[str] = None,
                 arn: Optional[str] = None,
                 type: Optional[str] = None,
                 port: Optional[int] = None,
                 depends_on: List[str] = [],
                 ecs_launch_type: str = None,
                 state: Optional[str] = None,
                 volume: Optional[List] = None,
                 env_var: Optional[str] = None,
                 restart: Optional[bool] = None,
                 container_group_name: str = None,
                 inputs: Optional[List[str]] = [],
                 outputs: Optional[List[str]] = [],
                 ecs_kwargs: dict = {'executionRoleArn': ''}):


        super().__init__()

        self.id: int = id
        self.cmd: str = cmd
        self.name: str = name
        self.tries: int = tries
        self.image: str = image
        self.run_id: str = run_id
        self.vcpus: float = vcpus
        self.memory: float = memory
        self.args: List[str] = args
        self.ip: Optional[str] = ip
        self.provider: str = provider
        self.arn: Optional[str] = arn
        self.type: Optional[str] = type
        self.port: Optional[int] = port
        self.state: Optional[str] = state
        self.ecs_kwargs: dict = ecs_kwargs
        self.volume: Optional[List] = volume
        self.env_var: Optional[str] = env_var
        self.restart: Optional[bool] = restart
        self.depends_on: List[str] = depends_on
        self.inputs: Optional[List[str]] = inputs
        self.outputs: Optional[List[str]] = outputs
        self.ecs_launch_type: str = ecs_launch_type
        self.container_group_name: str = container_group_name


    # --------------------------------------------------------------------------
    #
    def _verify(self):
        _required_args = ['cmd', 'vcpus', 'memory', 'image']
        if not all([self.cmd, self.vcpus, self.memory, self.image]):
            raise ValueError(f'Missing required arguments: {_required_args}')


    # --------------------------------------------------------------------------
    #
    def add_dependency(self, tasks):
        for t in tasks:
            self.depends_on.append(t)


    # --------------------------------------------------------------------------
    #
    def get_dependency(self):
        return [t for t in self.depends_on]


    # --------------------------------------------------------------------------
    #
    def name(self):
        return self.name


    # --------------------------------------------------------------------------
    #
    def id(self):
        return self.id


    # --------------------------------------------------------------------------
    #
    def run_id(self):
        """represents the run id that the task belongs to"""
        return self.run_id


    # --------------------------------------------------------------------------
    #
    def port(self):
        """represents the port of a task"""
        return self.port


    # --------------------------------------------------------------------------
    #
    @property
    def ip(self):
        """represents the ip of a task"""
        return self.__ip


    # --------------------------------------------------------------------------
    #
    @ip.setter
    def ip(self, ip):
        self.__ip = ip


    # --------------------------------------------------------------------------
    #
    @property
    def provider(self):
        """represents the ptovider of that task (AZURE, AWS, G-CLOUD)"""
        return self.__provider


    # --------------------------------------------------------------------------
    #
    @provider.setter
    def provider(self, provider):
        self.__provider = provider
    

    # --------------------------------------------------------------------------
    #
    @property
    def launch_type(self):
        """represents the launch_type of that task (EC2, B1)"""
        return self.__launch_type


    # --------------------------------------------------------------------------
    #
    @launch_type.setter
    def launch_type(self, launch_type):
        self.__launch_type = launch_type


    # --------------------------------------------------------------------------
    #
    @property
    def env_var(self):
        return self.__env_var


    # --------------------------------------------------------------------------
    #
    @env_var.setter
    def env_var(self, env_var):
        self.__env_var = env_var



    # --------------------------------------------------------------------------
    #
    @property
    def arn(self) -> str:
        return self.__arn


    # --------------------------------------------------------------------------
    #
    @arn.setter
    def arn(self, arn):        
        self.__arn = arn


    # --------------------------------------------------------------------------
    #
    @property
    def dns(self) -> str:
        pass


    # --------------------------------------------------------------------------
    #
    @property
    def restart(self) -> str:
        return self.__restart


    # --------------------------------------------------------------------------
    #
    @restart.setter
    def restart(self, restart):
        self.__restart = restart


    # --------------------------------------------------------------------------
    #
    @property
    def memory(self) -> float:
        return self.__memory


    # --------------------------------------------------------------------------
    #
    @memory.setter
    def memory(self, memory) -> float:
        self.__memory = memory


    # --------------------------------------------------------------------------
    #
    @property
    def vcpus(self) -> float:
        return self.__vcpus


    # --------------------------------------------------------------------------
    #
    @vcpus.setter
    def vcpus(self, vcpus) -> float:
        self.__vcpus = vcpus


    # --------------------------------------------------------------------------
    #
    @property
    def state(self) -> str:
        return self.__state


    # --------------------------------------------------------------------------
    #
    @state.setter
    def state(self, state):
        self.__state = state


    # --------------------------------------------------------------------------
    #
    @property
    def events(self) -> OrderedDict:
        return self.__events


    # --------------------------------------------------------------------------
    #
    @events.setter
    def events(self, events):
        self.__events = events


    # --------------------------------------------------------------------------
    #
    @property
    def cmd(self) -> list:
        return self.__cmd


    # --------------------------------------------------------------------------
    #
    @cmd.setter
    def cmd(self, cmd) -> list:
        self.__cmd = cmd


    # --------------------------------------------------------------------------
    #
    @property
    def args(self) -> list:
        return self.__args


    # --------------------------------------------------------------------------
    #
    @args.setter
    def args(self, args) -> list:
        self.__args = args


    # --------------------------------------------------------------------------
    #
    @property
    def image(self) -> str:
        return self.__image


    # --------------------------------------------------------------------------
    #
    @image.setter
    def image(self, image) -> list:
        self.__image = image


    # --------------------------------------------------------------------------
    #
    def exit_code(self):
        pass


    # --------------------------------------------------------------------------
    #
    def is_service(self):
        """True if task needs to run for every.
           if True then self.retry() must be True
        """
        pass


    # --------------------------------------------------------------------------
    #
    def reset_state(self):
        self.state = None
        self._state = 'PENDING'
        self._exception = None


    # --------------------------------------------------------------------------
    #
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


    
