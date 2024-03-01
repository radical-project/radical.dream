import os
import uuid
import radical.pilot as rp

from hydraa import Task
from typing import Callable
from collections import OrderedDict

RP = 'radical.pilot'


# --------------------------------------------------------------------------
#
class HPCManager:
    """
    A class to manage High-Performance Computing (HPC) resources using RADICAL-Pilot.

    Attributes:
        task_id (int): An integer representing the ID of the task.
        sandbox (str): A string representing the directory path for sandbox creation.
        pdesc (radical.pilot.PilotDescription): A PilotDescription object describing the pilot.
        run_id (str): A string representing the unique ID for the current run.
        tasks_book (collections.OrderedDict): An ordered dictionary to store tasks.
    """

    def __init__(self, pilot_description) -> None:
        """
        Initializes the HPCManager with the provided PilotDescription.

        Args:
            pilot_description (radical.pilot.PilotDescription): A description of the pilot.
        """

        self.task_id = 0
        self.sandbox  = None
        self.run_id = str(uuid.uuid4())
        self.tasks_book = OrderedDict()
        if not isinstance(pilot_description, dict):
            raise ValueError(f'Expected type dict, got type {type(pilot_description)}')

        self.pdesc = rp.PilotDescription(from_dict=pilot_description)


    # --------------------------------------------------------------------------
    #
    def task_state_cb(self, task, state):
        """
        Callback function to handle task state changes.

        Args:
            task (radical.pilot.Task): The task object.
            state (str): The state of the task.
        """

        task_fut = self.tasks_book[task.uid]

        if state == rp.AGENT_EXECUTING:
            task_fut.set_running_or_notify_cancel()

        if state == rp.DONE:
            task_fut.set_result(task.stdout)
        
        elif state == rp.CANCELED:
            task_fut.cancel()
        
        elif state == rp.FAILED:
            task_fut.set_exception(Exception(task.stderr))


    # --------------------------------------------------------------------------
    #
    def start(self, sandbox):
        """
        Starts the RADICAL-Pilot HPC Manager.

        Args:
            sandbox (str): The directory path for creating the sandbox.
        """

        print('starting RADICAL-Pilot HPC Mananger')

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, RP, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.session = rp.Session(cfg={'base':self.sandbox})
        self.pmgr = rp.PilotManager(session=self.session)
        self.tmgr = rp.TaskManager(session=self.session)

        self.pdesc.verify()

        # Register the pilot in a TaskManager object.
        self.tmgr.register_callback(self.task_state_cb)
        self.pilot = self.pmgr.submit_pilots(self.pdesc)
        self.tmgr.add_pilots(self.pilot)

        print('RADICAL-Pilot HPC Mananger is in Running state')


    # --------------------------------------------------------------------------
    #
    def __call__(self, func: Callable=None, provider='hpc') -> Callable:
        """
        Decorator function to invoke the submit function of HPCManager with
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
                task.provider = 'hpc'

            self.submit(task)

            return task

        return wrapper


    # --------------------------------------------------------------------------
    #
    def submit(self, tasks: Task):
        """
        Submits tasks to the HPC Manager.

        Args:
            tasks (Task or list[Task]): A single Task object or a list of Task objects to be submitted.
        """

        to_submit = []

        if not isinstance(tasks, list):
            tasks = [tasks]

        for task in tasks:
        
            if not isinstance(task, Task):
                raise Exception(f'task must be of type {Task}')

            task.image = 'void'
            task._verify()

            td = rp.TaskDescription()

            td.ranks = task.vcpus
            td.uid = task.id = self.task_id
            td.executable = task.cmd
            td.mem_per_rank = task.memory
            td.arguments = task.args
            td.name = task.name = 'task-{0}'.format(self.task_id)

            # make sure we set extra task args if we pass it via
            # hydraa task object
            for k, v in task.__dict__.items():
                if k in td.as_dict():
                    td[k] = v

            td.verify()

            to_submit.append(td)

            self.tasks_book[str(self.task_id)] = task

            self.task_id += 1

        self.tmgr.submit_tasks(to_submit)

        print(f'{self.task_id} task(s) has been submitted')


    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        """Shuts down the HPC Manager."""
        self.session.close(download=True)
