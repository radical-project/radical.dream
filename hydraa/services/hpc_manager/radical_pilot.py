import os
import uuid
import threading as mt
import radical.pilot as rp
import radical.utils as ru 

from hydraa import Task
from collections import OrderedDict

RP = 'radical.pilot'

class RadicalPilot:

    def __init__(self, pilot_description: rp.PilotDescription) -> None:

        self.task_id = 0
        self.sandbox  = None
        self.pdesc = pilot_description
        self.run_id = str(uuid.uuid4())
        self.tasks_book = OrderedDict()


    def callbacks(self, task, state):

        task_fut = self.tasks_book[task.uid]

        if state == rp.AGENT_EXECUTING:
            task_fut.set_running_or_notify_cancel()

        if state == rp.DONE:
            task_fut.set_result(task.stdout)
        
        elif state == rp.CANCELED:
            task_fut.cancel()
        
        elif state == rp.FAILED:
            task.set_exception(Exception(task.stderr))


    def start(self, sandbox):

        print('starting HighPerformance Mananger with Radical.Pilot backend')

        def start_rp():
            self.sandbox  = '{0}/{1}.{2}'.format(sandbox, RP, self.run_id)
            os.mkdir(self.sandbox, 0o777)

            self.session = rp.Session(cfg={'base':self.sandbox})
            self.pmgr = rp.PilotManager(session=self.session)
            self.tmgr = rp.TaskManager(session=self.session)
            self.pilot = self.pmgr.submit_pilots(self.pdesc)

            self.pilot.verify()

            # Register the pilot in a TaskManager object.
            self.register_callback(self.callbacks)
            self.tmgr.add_pilots(self.pilot)

            print('HighPerformance manager is in Ready state')

        rp_thread = mt.Thread(target=start_rp)
        rp_thread.start()


    def submit(self, task: Task):

        if not isinstance(task, Task):
            raise Exception(f'task must be of type {Task}')


        td = rp.TaskDescription()

        td.ranks = task.vcpus
        td.uid = self.task_id
        td.executable = task.cmd
        td.mem_per_rank = task.memory
        td.arguments = task.arguments
        td.name = 'task-{0}'.format(self.task_id)

        # make sure we set extra task args if we pass it via
        # hydraa task object
        for k, v in task.__dict__.items():
            if k in td.as_dict():
                td[k] = v

        td.verify()

        self.tmgr.submit_tasks(td)

        self.task_id += 1

        self.tasks_book[self.task_id] = task

        return task


    def shutdown(self):

        self.session.close(download=True)
