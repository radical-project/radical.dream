import os
import copy
import threading

from ..utils.misc import build_pod
from ..utils.misc import sh_callout
from ..utils.misc import load_multiple_yamls, dump_multiple_yamls

WORKFLOW_TYPE = ['steps', 'containerset']


# --------------------------------------------------------------------------
#
class Workflow:
    """
    This parent class and its subclasses extends the functionality of
    Argo workflows (or any workflow backend) such as Steps, DAGs,
    containerSets by:
    1- Parsing the pythonic API of the workflow and convert it into Yaml.
    2- Performs data movements in the background between local <==> volume.
    """

    def __init__(self, name, type, manager, volume=None) -> None:
        
        if not type in WORKFLOW_TYPE:
            raise TypeError('Workflow type must be one of {0}'.format(WORKFLOW_TYPE))

        self.type = type        
        self.tasks = []
        self.name = name
        self.workflows = []
        self.manager = manager
        self.cluster = manager.cluster
        self._workflows_counter = 0
        self.update_lock = threading.Lock()

        self._setup_template()
        self._setup_volume(volume)
        self._setup_argo()


    # --------------------------------------------------------------------------
    #
    def _setup_template(self) -> None:
        loc = os.path.join(os.path.dirname(__file__))
        loc += '/argo_templates.yaml'
        templates = load_multiple_yamls(loc)

        for t in templates:
            if t['metadata'].get('name') == self.type.lower():
                self.argo_template = t

        self.argo_template['spec']['entrypoint'] = self.name


    # --------------------------------------------------------------------------
    #
    def _setup_volume(self, volume) -> None:
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
    def add_tasks(self, tasks) -> None:
        for task in tasks:
            self.tasks.append(task)


    # --------------------------------------------------------------------------
    #
    def create(self) -> None:

        self.argo_object = copy.deepcopy(self.argo_template)

        # set the workflow name
        wf_name = 'hydraa-' + self.name + '-' + str(self._workflows_counter)
        self.argo_object['metadata']['name'] = wf_name

        # iterate on each task in the tasks list
        for task in self.tasks:
            task.id = str(self.manager._task_id)
            task.name = 'ctask-{0}'.format(self.manager._task_id)

            # mv task.ouputs >> /volume/data
            if task.outputs:
                self.move_to_volume(task)

            # mv /volume/data/outputs >> /image/local 
            if task.get_dependency():
                self.move_to_local(task)

            # make sure only one instance is updating 
            # the task_id at a time.
            with self.update_lock:
                self.manager._task_id +=1


    # --------------------------------------------------------------------------
    #
    def run(self) -> None:

        print('submitting workflows x [{0}] to {1}'.format(self._workflows_counter,
                                                           self.cluster.name))
        file_path = self.cluster.sandbox + '/' + 'workflow.yaml'
        dump_multiple_yamls(self.workflows, file_path, sort_keys=False)
        self.cluster.submit(deployment_file=file_path)


    # --------------------------------------------------------------------------
    #
    def _setup_argo(self) -> None:

        cmd = "kubectl create namespace argo ;"
        cmd += "kubectl apply -n argo -f"
        cmd += "https://github.com/argoproj/argo-workflows/" \
               "releases/download/v3.4.9/install.yaml"

        out, err, ret = sh_callout('kubectl get crd', shell=True,
                                   kube=self.cluster)
        if ret:
            self.cluster.logger.error('checking for Argo CRD failed: {0}\
                                      '.format(err))

        elif out and "argo-server" not in out:
            out, err, ret = sh_callout(cmd, shell=True, kube=self.cluster)


    # --------------------------------------------------------------------------
    #
    def move_to_local(self, task):
        """
        move data from PV or PVC to a local container
        storage to be used during execution time if 
        the container has no awarness of the volume
        path
        """
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
        """
        move data from local container storage to a
        shared node PV or PVC storage to be used
        during execution time by other containers or
        stored for other purposes even after the 
        pod/container is deleted
        """
        if self.volume:
            outputs = " ".join(task.outputs)
            move_data = f' ; mv {outputs} {self.volume.host_path}'

            task.cmd[-1] += move_data

        else:
            raise Exception('exchanging outputs between workflows tasks'
                            'requires an exisiting volume to be specified')



# --------------------------------------------------------------------------
#
class StepsWorkflow(Workflow):
    def __init__(self, name, manager, volume=None) -> None:

        type = WORKFLOW_TYPE[0]
        super().__init__(name, type, manager, volume)


    # --------------------------------------------------------------------------
    #
    def add_step(self, task) -> None:

        template_name = 'template-{0}'.format(task.name)
        step_name = 'step-{0}'.format(task.name)
        step = [{'name' : step_name, 'template': template_name}]
        task.step = step

        container = build_pod([task], task.id)['spec']['containers'][0]
        task.template = {'name': template_name, 'container': container}
    

    # --------------------------------------------------------------------------
    #
    def create(self) -> None:

        # FIXME: Argo has 2 modes of steps and we only support Mode1:
        # Mode-1:
        # --name: step-1
        # --name: step-2
        # step will run as: step1 >> step2 run sequentially 

        # Mode-2
        # --name: step-1
        #  -name: step2
        # steps will run as: step1 and step2 will run in paralle
        # https://argoproj.github.io/argo-workflows/walk-through/steps/#steps

        super().create()

        self.argo_object['spec']['templates'] = []
        self.argo_object['spec']['templates'].append({'name': self.name,
                                                      'steps': None})

        for task in self.tasks:
            # create a step entry in the yaml file
            self.add_step(task)
            self.argo_object['spec']['templates'].append(task.template)

        self.argo_object['spec']['templates'][0]['steps'] = \
                        [t.step for t in self.tasks]

        self.workflows.append(self.argo_object)
        self._workflows_counter +=1
        # reset the tasks for a new wf
        self.tasks.clear()


# --------------------------------------------------------------------------
#
class ContainerSetWorkflow(Workflow):
    """
    check Argo ContainerSet/Inputs and Outputs
    All container set templates that have artifacts
    must/should have a container named "main" when
    collecting outputs. Thus, since we do not depend
    on Argo artifacts input/ouput, Hydraa, extends 
    the capabilitey
    """
    def __init__(self, name, manager, volume=None) -> None:

        type = WORKFLOW_TYPE[1]
        super().__init__(name, type, manager, volume)
    

    def create(self) -> None:

        super().create()

        spec = self.argo_object['spec']['templates'][0]
        spec['name'] = self.name
        spec['containerSet']['volumeMounts'][0]['name'] = self.volume.name + '-workdir'
        spec['containerSet']['volumeMounts'][0]['mountPath'] = self.volume.host_path
        containers_set = spec['containerSet']['containers'] = []

        for task in (self.tasks):
            c = build_pod([task], task.id)['spec']['containers'][0]
            if task.get_dependency():
                deps = [dep.name for dep in task.get_dependency()]
                c['dependencies'] = deps

            containers_set.append(c)

        self.workflows.append(self.argo_object)
        self._workflows_counter +=1
        # reset the tasks for a new wf
        self.tasks.clear()
