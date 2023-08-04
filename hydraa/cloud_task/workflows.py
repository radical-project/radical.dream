import os
import copy

from .task import Task
from ..services.caas_manager.utils.misc import build_pod
from ..services.caas_manager.utils.misc import load_yaml, dump_multiple_yamls


# --------------------------------------------------------------------------
#
class Workflow:
    def __init__(self, name, cluster, volume=None) -> None:
        self.tasks = []
        self.name = name
        self.workflows = []
        self.cluster = cluster
        self._tasks_counter = 0
        self._workflows_counter = 0

        self._setup_template()
        self._setup_volume(volume)
        self._setup_argo()


    # --------------------------------------------------------------------------
    #
    def _setup_template(self):
        loc = os.path.join(os.path.dirname(__file__))
        loc += '/argo_templates.yaml'
        self.argo_template = load_yaml(loc)
        self.argo_template['spec']['entrypoint'] = self.name


    # --------------------------------------------------------------------------
    #
    def _setup_volume(self, volume):
        if volume:
            # FIXME: support multiple volumes instead of one
            self.volume = volume
            self.argo_template['spec']['volumes'][0]['name'] = \
                              self.volume.name + '-workdir'
            self.argo_template['spec']['volumes'][0]['persistentVolumeClaim'] = \
                              {'claimName': self.volume.name}
        else:
            self.argo_template['spec'].pop('volumes')


    # --------------------------------------------------------------------------
    #
    def add_tasks(self, tasks: Task):
        for task in tasks:
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

        # set the workflow name
        wf_name = 'hydraa-' + self.name + '-' + str(self._workflows_counter)
        self.argo_object['metadata']['name'] = wf_name

        # iterate on each task in the tasks list
        for task in self.tasks:
            task.id = str(self._tasks_counter)
            task.name = 'ctask-{0}'.format(self._tasks_counter)

            # mv task.ouputs >> /volume/data
            if task.outputs:
                self.move_to_volume(task)

            # mv /volume/data/outputs >> /image/local 
            if task.get_dependency():
                self.move_to_local(task)

            # create a step entry in the yaml file
            self._create_step(task)

            self._tasks_counter +=1

        self.argo_object['spec']['templates'] = []
        self.argo_object['spec']['templates'].append({'name': self.name,
                                                      'steps': None})
        self.argo_object['spec']['templates'][0]['steps'] = \
                        [t.step for t in self.tasks]

        for t in self.tasks:
            self.argo_object['spec']['templates'].append(t.template)

        self.tasks = []
        self.workflows.append(self.argo_object)
        self._workflows_counter +=1


    # --------------------------------------------------------------------------
    #
    def run(self):

        print('submitting workflows x [{0}] to {1}'.format(self._workflows_counter,
                                                           self.cluster.name))
        file_path = self.cluster.sandbox + '/' + 'workflow.yaml'
        dump_multiple_yamls(self.workflows, file_path, sort_keys=False)
        self.cluster.submit(deployment_file=file_path)


    # --------------------------------------------------------------------------
    #
    def _setup_argo(self):

        cmd = "kubectl create namespace argo ;"
        cmd += "kubectl apply -n argo -f"
        cmd += "https://github.com/argoproj/argo-workflows/" \
               "releases/download/v3.4.9/install.yaml"
        
        res = self.cluster.remote.run('kubectl get crd', hide=True)
        if res.return_code:
            self.cluster.logger.error('checking for Argo CRD failed: {0}\
                                      '.format(res.stderr))

        elif not "argo-server" in res.stdout:
            res = self.cluster.remote.run(cmd, hide=True)


    # --------------------------------------------------------------------------
    #
    def move_to_local(self, task):
        if self.volume:
            outputs = []
            for t in task.get_dependency():
                outputs.extend(t.outputs)

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
