import os
import copy
import threading

from ...utils.misc import build_pod
from ...utils.misc import sh_callout
from ...utils.misc import load_multiple_yamls, dump_multiple_yamls

WORKFLOW_TYPE = ['steps', 'containerset']


# --------------------------------------------------------------------------
#
class Workflow:
    """
    Workflow Class

    This parent class and its subclasses extend the functionality of
    Argo workflows (or any workflow backend) such as Steps, DAGs,
    containerSets by:
    1- Parsing the pythonic API of the workflow and converting it into Yaml.
    2- Performing data movements in the background between local <==> volume.

    Parameters
    ----------
    name : str
        The name of the workflow.
    wf_type : str
        The wf_type of the workflow, must be one of WORKFLOW_TYPE.
    manager : str
        The manager for this workflow.
    volume : Volume or None, optional
        An optional volume object to be used for mounting data in the workflow.

    Attributes
    ----------
    name : str
        The name of the workflow.
    wf_type : str
        The wf_type of the workflow.
    manager : str
        The manager for this workflow.
    cluster : Cluster
        The cluster associated with this workflow.
    tasks : list
        A list of tasks to be executed in the workflow.
    workflows : list
        A list to store the generated workflows.
    _workflows_counter : int
        A counter to keep track of the number of generated workflows.
    update_lock : threading.Lock
        A lock to ensure safe updating of task IDs.
    argo_template : dict
        The Argo workflow template.
    volume : Volume or None
        The volume object to be used for data mounting in the workflow.

    Methods
    -------
    add_tasks(tasks)
        Add tasks to the workflow.

    create()
        Create the workflow based on the provided configuration.

    run()
        Submit the generated workflows to the associated cluster.

    move_to_local(task)
        Move data from PV or PVC to a local container storage.

    move_to_volume(task)
        Move data from local container storage to a shared node PV or PVC storage.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, wf_type, manager, volume=None) -> None:
        """
        Initialize a Workflow instance.

        Parameters
        ----------
        name : str
            The name of the workflow.
        wf_type : str
            The wf_type of the workflow, must be one of WORKFLOW_TYPE.
        manager : str
            The manager for this workflow.
        volume : Volume or None, optional
            An optional volume object to be used for mounting data in the workflow.
        """
        if not wf_type in WORKFLOW_TYPE:
            raise TypeError('Workflow type must be one of {0}'.format(WORKFLOW_TYPE))

        self.wf_type = wf_type     
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
        """
        Set up the Argo workflow template.
        """
        loc = os.path.join(os.path.dirname(__file__))
        loc += '/argo_templates.yaml'
        templates = load_multiple_yamls(loc)

        for t in templates:
            if t['metadata'].get('name') == self.wf_type.lower():
                self.argo_template = t


    # --------------------------------------------------------------------------
    #
    def _setup_volume(self, volume) -> None:
        """
        Set up the volume for data mounting.
        """
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
        """
        Add tasks to the workflow.

        Parameters
        ----------
        tasks : list
            A list of Task objects to be added to the workflow.
        """
        for task in tasks:
            self.tasks.append(task)


    # --------------------------------------------------------------------------
    #
    def _create(self) -> str:
        """
        Create the workflow based on the provided configuration.
        """
        self.argo_object = copy.deepcopy(self.argo_template)

        # set the workflow name
        wf_name = 'hydraa-' + self.name + '-' + str(self._workflows_counter)
        self.argo_object['metadata']['name'] = wf_name
        self.argo_object['spec']['entrypoint'] = wf_name

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
                self.manager._tasks_book[str(ctask.name)] = task

        return wf_name


    # --------------------------------------------------------------------------
    #
    def run(self) -> None:
        """
        Submit the generated workflows to the associated cluster.
        """
        print('submitting workflows x [{0}] to {1}'.format(len(self.workflows),
                                                           self.cluster.name))
        file_path = self.cluster.sandbox + '/' + self.wf_type.lower() + \
                    '-workflow.yaml'
        dump_multiple_yamls(self.workflows, file_path, sort_keys=False)
        self.cluster.submit(deployment_file=file_path)
        self.workflows.clear()


    # --------------------------------------------------------------------------
    #
    def _setup_argo(self) -> None:
        """
        Set up Argo workflow manager.
        """
        cmd = "kubectl create namespace argo ;"
        cmd += "kubectl apply -n argo -f "
        cmd += "https://github.com/argoproj/argo-workflows/" \
               "releases/download/v3.4.9/install.yaml"

        out, err, ret = sh_callout('kubectl get svc -n argo', shell=True,
                                   kube=self.cluster)
        if ret:
            self.cluster.logger.error('checking for Argo CRD failed: {0}\
                                      '.format(err))
            return

        elif out and "argo-server" in out:
            self.cluster.logger.info('workflow backend [Argo] is already '\
                                     'installed')

        else:
            out, err, ret = sh_callout(cmd, shell=True, kube=self.cluster)
            if ret:
                self.cluster.logger.error('installing Argo failed: {0}\
                                          '.format(err))
                return
            else:
                self.cluster.logger.info('workflow backend [Argo] '\
                                         'installed: {0}'.format(out))


    # --------------------------------------------------------------------------
    #
    def move_to_local(self, task):
        """
        move data from PV or PVC to a local container
        storage to be used during execution time if 
        the container has no awarness of the volume
        path.

        Parameters
        ----------
        task : Task
            The task for which data needs to be moved.
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
        pod/container is deleted.

        Parameters
        ----------
        task : Task
            The task for which data needs to be moved to the volume.
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
    """
    StepsWorkflow Class

    A workflow class for defining steps in an Argo workflow.

    This class extends the capability of Argo workflows by providing support
    for creating steps within a workflow. Steps can be used to define the
    execution order of tasks within the workflow.

    Parameters
    ----------
    name : str
        The name of the workflow.
    manager : str
        The manager for this workflow.
    volume : Volume or None, optional
        An optional volume object to be used for mounting data in the workflow.

    Attributes
    ----------
    name : str
        The name of the workflow.
    manager : str
        The manager for this workflow.
    volume : Volume or None
        The volume object to be used for data mounting in the workflow.

    Methods
    -------
    add_step(task)
        Add a step to the workflow for a given task.

        Parameters
        ----------
        task : Task
            The task for which to add a step.

    create()
        Create the StepsWorkflow.

        This method creates the Argo workflow based on the provided
        configuration. It sets up the necessary steps and templates for
        running tasks within the workflow.
    """


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, manager, volume=None) -> None:
        """
        Initialize a StepsWorkflow instance.

        Parameters
        ----------
        name : str
            The name of the workflow.
        manager : str
            The manager for this workflow.
        volume : Volume or None, optional
            An optional volume object to be used for mounting
            data in the workflow.
        """
        wf_type = WORKFLOW_TYPE[0]
        super().__init__(name, wf_type, manager, volume)


    # --------------------------------------------------------------------------
    #
    def add_step(self, task) -> None:
        """
        Add a step to the workflow for a given task.

        Parameters
        ----------
        task : Task
            The task for which to add a step.
        """
        template_name = 'template-{0}'.format(task.name)
        step_name = 'step-{0}'.format(task.name)
        step = [{'name' : step_name, 'template': template_name}]
        task.type = 'pod'
        task.step = step
        task.pod_name = step_name

        container = build_pod([task], task.id)['spec']['containers'][0]
        task.template = {'name': template_name, 'container': container}
    

    # --------------------------------------------------------------------------
    #
    def create(self) -> None:
        """
        Create the StepsWorkflow.

        This method creates the Argo workflow based on the provided
        configuration. It sets up the necessary steps and templates for
        running tasks within the workflow.
        """
        # FIXME: Argo has 2 modes of steps and we only support Mode1:
        # Mode-1:
        # --name: step-1
        # --name: step-2
        # step will run as: step1 >> step2 run sequentially 

        # Mode-2
        # --name: step-1
        #  -name: step-2
        # steps will run as: step1 and step2 will run in paralle
        # https://argoproj.github.io/argo-workflows/walk-through/steps/#steps

        wf_name = super()._create()

        self.cluster.profiler.prof('create_wf_start', uid=wf_name)

        self.argo_object['spec']['templates'] = []
        self.argo_object['spec']['templates'].append({'name': wf_name,
                                                      'steps': None})

        for task in self.tasks:
            # create a step entry in the yaml file
            self.add_step(task)
            self.argo_object['spec']['templates'].append(task.template)

        self.argo_object['spec']['templates'][0]['steps'] = \
                        [t.step for t in self.tasks]

        self.workflows.append(self.argo_object)
        self._workflows_counter +=1

        self.cluster.profiler.prof('create_wf_stop', uid=wf_name)

        # reset the tasks for a new wf
        self.tasks.clear()


# --------------------------------------------------------------------------
#
class ContainerSetWorkflow(Workflow):
    """
    ContainerSetWorkflow Class

    A workflow class for checking Argo ContainerSet inputs and outputs.

    This class extends the capability of Argo workflows by providing support
    for container sets that do not rely on Argo artifacts for input/output
    handling. It allows for creating complex workflows with multiple tasks.
    
    The main intended use case for this class is to allow for running workflows
    that requires in-node data movement between containers only. 

    Parameters
    ----------
    name : str
        The name of the workflow.
    manager : str
        The manager for this workflow.
    volume : Volume or None, optional
        An optional volume object to be used for mounting data in the workflow.

    Attributes
    ----------
    name : str
        The name of the workflow.
    manager : str
        The manager for this workflow.
    volume : Volume or None
        The volume object to be used for data mounting in the workflow.

    Methods
    -------
    create()
        Create the ContainerSetWorkflow.

        This method creates the Argo workflow based on the provided
        configuration. It sets up the necessary specifications for
        running tasks within a container set.

    Examples
    --------
    >>> workflow = ContainerSetWorkflow(name="my_workflow", manager="argo_manager")
    >>> task1 = Task(name="task1")
    >>> task2 = Task(name="task2")
    >>> workflow.add_task(task1)
    >>> workflow.add_task(task2)
    >>> workflow.create()
    """


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, manager, volume=None) -> None:
        """
        Initialize a ContainerSetWorkflow instance.

        Parameters
        ----------
        name : str
            The name of the workflow.
        manager : str
            The manager for this workflow.
        volume : Volume or None, optional
            An optional volume object to be used for mounting
            data in the workflow.
        """
        wf_type = WORKFLOW_TYPE[1]
        super().__init__(name, wf_type, manager, volume)
    

    # --------------------------------------------------------------------------
    #
    def create(self) -> None:
        """
        Create the ContainerSetWorkflow.

        This method creates the Argo workflow based on the provided
        configuration. It sets up the necessary specifications for
        running tasks within a container set.
        """

        wf_name = super()._create()

        self.cluster.profiler.prof('create_wf_start', uid=wf_name)
    
        spec = self.argo_object['spec']
        # update the pod label with workflow name since
        # the entire workflow is a single pod we only set
        # the pod (containerset) label to the name of the workflow.
        spec['podMetadata']['labels']['task_label'] = wf_name
        template = spec['templates'][0]
        template['name'] = wf_name
        template['containerSet']['volumeMounts'][0]['name'] = self.volume.name + '-workdir'
        template['containerSet']['volumeMounts'][0]['mountPath'] = self.volume.host_path
        containers_set = template['containerSet']['containers'] = []

        for task in (self.tasks):
            c = build_pod([task], task.id)['spec']['containers'][0]
            if c.get('volumeMounts'):
                raise Exception('containerSet does not support volumeMounts '\
                                 'on the invidiual containers')

            if task.get_dependency():
                deps = [dep.name for dep in task.get_dependency()]
                c['dependencies'] = deps

            task.type = 'container'
            task.pod_name = wf_name
            containers_set.append(c)

        self.workflows.append(self.argo_object)
        self._workflows_counter +=1

        self.cluster.profiler.prof('create_wf_stop', uid=wf_name)

        # reset the tasks for a new wf
        self.tasks.clear()
