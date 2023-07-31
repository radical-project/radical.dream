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

        loc = os.path.join(os.path.dirname(__file__))
        loc += '/argo_template.yaml'
        self.argo_template = load_yaml(loc)
        self.argo_template['spec']['entrypoint'] = self.name

        self._setup_volume(volume)


    # --------------------------------------------------------------------------
    #
    def _setup_volume(self, volume):
        if volume:
            # FIXME: support multiple volumes instead of one
            self.volume = volume
            self.argo_template['spec']['volumes'][0]['name'] = self.volume.name + '-workdir'
            self.argo_template['spec']['volumes'][0]['persistentVolumeClaim'] = \
                              {'claimName': self.volume.name}
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

        self.argo_object = copy.deepcopy(self.argo_template)

        # iterate on each task in the tasks list
        for task in self.tasks:
            task.id = str(self._counter)
            task.name = 'ctask-{0}'.format(self._counter)

            # mv task.ouputs >> /volume/data
            if task.outputs:
                self.move_to_volume(task)

            # mv /volume/data/outputs >> /image/local 
            if task.depends_on:
                self.move_to_local(task)

            # create a step entry in the yaml file
            self._create_step(task)

            self._counter +=1

        self.argo_object['spec']['templates'] = []
        self.argo_object['spec']['templates'].append({'name': self.name, 'steps': None})
        self.argo_object['spec']['templates'][0]['steps'] = [t.step for t in self.tasks]

        for t in self.tasks:
            self.argo_object['spec']['templates'].append(t.template)

        self._workflows.append(self.argo_object)
        self.tasks = []


    # --------------------------------------------------------------------------
    #
    def run(self):
        dump_multiple_yamls(self._workflows, '/home/aymenalsaadi/dump.yaml',
                            sort_keys=False)


    # --------------------------------------------------------------------------
    #
    def move_to_local(self, task):
        if self.volume:
            outputs = []
            for t in task.depends_on:
                outputs.extend(t.outputs)

            #outputs = ",".join(outputs)
            #move_data = f'mv {self.volume.host_path}/{{{outputs}}} $PWD ;'
            outputs = [self.volume.host_path + '/' + filename for filename in outputs]
            outputs = " ".join(outputs)
            move_data = f'mv {outputs} $PWD ;'

            # FIXME: we assume the user is doing sh -c python3
            # and we are inserting between sh-c and python3
            task.cmd[2] = move_data + ' ' + task.cmd[2]

        else:
            raise Exception('exchanging outputs between workflows tasks'
                            'requires an exisiting volume to be specified')


    def move_to_volume(self, task):
        if self.volume:
            outputs = " ".join(task.outputs)
            move_data = f' ; mv {outputs} {self.volume.host_path}'

            task.cmd[-1] += move_data

        else:
            raise Exception('exchanging outputs between workflows tasks'
                            'requires an exisiting volume to be specified')
