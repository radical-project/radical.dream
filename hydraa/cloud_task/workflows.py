import os
import copy

from .task import Task
from ..services.caas_manager.utils.misc import build_pod
from ..services.caas_manager.utils.misc import load_yaml, dump_multiple_yamls


# --------------------------------------------------------------------------
#
class Workflow:
    def __init__(self, name, volume=None) -> None:
        self._counter = 0
        self.tasks = []
        self.name = name
        self._workflows = []
        self.argo_object = None

        loc = os.path.join(os.path.dirname(__file__))
        loc += '/argo_template.yaml'
        self.argo_template = load_yaml(loc)

        #self._setup_volume(volume)


    # --------------------------------------------------------------------------
    #
    def _setup_volume(self, volume):
        if volume:
            self.volume = volume
            self.argo_template['volumes'] = [{'name': 'workdir',
                                              self.volume.kind: {'claimName': self.volume.name}}]
        else:
            self.argo_template['spec'].pop('volumes')


    # --------------------------------------------------------------------------
    #
    def add_task(self, task: Task):
        self.tasks.append(task)


    # --------------------------------------------------------------------------
    #
    def _create_step(self, task):

        template_name = 'template-{0}'.format(task.name)
        step_name = 'step-{0}'.format(task.name)
        step = [{'name' : step_name, 'template': template_name}]
        task.step = step

        container = build_pod([task], task.id)['spec']['containers'][0]
        task.template = {'name': template_name, 'container': container}


    # --------------------------------------------------------------------------
    #
    def create(self):

        self._tasks = copy.copy(self.tasks)

        # iterate on each task in the tasks list
        for task in self._tasks:
            task.id = str(self._counter)
            task.name = 'ctask-{0}'.format(self._counter)

            # move the output of this task to 
            # the volume if exists
            if task.outputs:
                self.move_to_volume(task)

            # if this task depends on other tasks
            # then move the output of the tasks that
            # we depend on to the shared volume so this
            # task can access it.
            if task.depends_on:
                self.move_from_volume(task)

            # create a step entry in the yaml file
            self._create_step(task)

            self._counter +=1

        self.argo_template['spec']['templates'] = []
        self.argo_template['spec']['templates'].append({'name': self.name, 'steps': None})

        self.argo_template['spec']['templates'][0]['steps'] = [t.step for t in self.tasks]
        
        for t in self.tasks:
            self.argo_template['spec']['templates'].append(t.template)
        
        self._workflows.append(self.argo_template)


    # --------------------------------------------------------------------------
    #
    def run(self):
        dump_multiple_yamls(self._workflows, '/home/aymenalsaadi/dump.yaml',
                            sort_keys=False)


    # --------------------------------------------------------------------------
    #
    def move_from_volume(self, task):
        if self.volume:
            v_path = '/data'#self.volume.host_path
            outputs = []
            for t in task.depends_on:
                outputs.extend(t.outputs)

            outputs = ",".join(outputs)
            move_data = f' ; mv {v_path}/{{{outputs}}} .'

            task.cmd[-1] += move_data
        
        else:
            raise Exception('exchanging outputs between workflows tasks'
                            'requires an exisiting volume to be specified')


    def move_to_volume(self, task):
        if self.volume:
            v_path = '/data'#self.volume.host_path
            outputs = " ".join(task.outputs)
            move_data = f' ; mv {outputs} {v_path}'

            task.cmd[-1] += move_data

        else:
            raise Exception('exchanging outputs between workflows tasks'
                            'requires an exisiting volume to be specified')
