import os
import sys
import math
import uuid
import copy
import time
import queue
import atexit
import datetime
import threading

from collections import OrderedDict
from azure.batch import BatchServiceClient
from azure.identity import DefaultAzureCredential
from azure.batch.batch_auth import SharedKeyCredentials
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (ContainerGroup,
                                                 Container,
                                                 ContainerGroupNetworkProtocol,
                                                 ContainerGroupRestartPolicy,
                                                 ContainerPort,
                                                 EnvironmentVariable,
                                                 IpAddress,
                                                 Port,
                                                 ResourceRequests,
                                                 ResourceRequirements,
                                                 OperatingSystemTypes)

from hydraa.services.caas_manager.utils import kubernetes
from hydraa.services.caas_manager.utils.misc import generate_id


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


AKS       = ['AKS', 'aks']
AZURE     = 'azure'
WAIT_TIME = 2
AZURE_RGX = '[a-z0-9]([-a-z0-9]*[a-z0-9])?'


# NOTE: If we ask for 100 tasks for now it will fail.
#       free tier (current subscription) only supports
#       6 container groups per resource group (pay as you go is 60)

CPCG  = 6   # Number of containers per container group
CGPRG = 60  # Number of container groups per resource group


# --------------------------------------------------------------------------
#
class AzureCaas:
    def __init__(self, sandbox, manager_id, cred, VMS, asynchronous, log, prof):
        
        self.vms = VMS
        self._task_id = 0
        self.logger = log
        self.status = False
        self.cluster = None
        self.manager_id = manager_id
        self._tasks_book  = OrderedDict()

        self.resource_group = None
        self.resource_group_name = None
        self._container_group_names = OrderedDict()
        self.launch_type = VMS[0].LaunchType.lower()
        
        for vm in self.vms:
            vm.Region = cred['region_name']

        self.runs_tree = OrderedDict()
        self.asynchronous = asynchronous

        self.run_id = '{0}.{1}'.format(self.launch_type, str(uuid.uuid4()))
        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, AZURE, self.run_id)
        os.mkdir(self.sandbox, 0o777)
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.res_client  = self._create_resource_client(cred)
        self.con_client = self._create_container_client(cred)

        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start,
                                             name='AzureCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        # now set the manager as active
        self.status = True

        atexit.register(self._shutdown)


    # --------------------------------------------------------------------------
    #
    @property
    def is_active(self):
        return self.status


    # --------------------------------------------------------------------------
    #
    def start(self):

        if self.status:
            print('Manager already started')
            return self.run_id

        print("starting run {0}".format(self.run_id))

        self.profiler.prof('prep_start', uid=self.run_id)

        self.resource_group = self.create_resource_group()

        self.profiler.prof('prep_stop', uid=self.run_id)

        if self.launch_type in AKS:
            for vm in self.vms:
                vm.ResourceGroup = self.resource_group

            self.cluster = kubernetes.AKSCluster(self.run_id, self.vms,
                                                 self.sandbox, self.logger)
            self.cluster.bootstrap()

        # call get work to pull tasks
        self._get_work()

        self.runs_tree[self.run_id] = self._container_group_names


    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = 1000000
        max_bulk_time = 2 # seconds
        min_bulk_time = 0.1 # seconds

        self.wait_thread = threading.Thread(target=self._wait_tasks,
                                            name='AzureCaaSWatcher')
        self.wait_thread.daemon = True
        if not self.asynchronous and not self.wait_thread.is_alive():
            self.wait_thread.start()

        while not self._terminate.is_set():
            now = time.time()  # time of last submission
            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < max_bulk_time:
                try:
                    task = self.incoming_q.get(block=True,
                                               timeout=min_bulk_time)
                except queue.Empty:
                        task = None

                if task:
                        bulk.append(task)
                
                if len(bulk) >= max_bulk_size:
                        break

            if bulk:
                if self.launch_type  in AKS:
                    self.submit_to_aks(bulk)
                else:
                    self.submit(bulk)
                bulk = list()


    # --------------------------------------------------------------------------
    #
    @property
    def get_runs_tree(self):
        return self.runs_tree


    # --------------------------------------------------------------------------
    #
    def _get_run_status(self, run_id):

        run_groups_names = self.runs_tree[run_id]
        statuses = self._get_task_statuses(run_groups_names)

        pending = list(filter(lambda pending: pending == 'Waiting', statuses))
        running = list(filter(lambda pending: pending == 'Running', statuses))
        stopped = list(filter(lambda pending: pending == 'Terminated', statuses))

        msg = ('pending: {0}, running: {1}, stopped: {2}'.format(len(pending),
                                                                 len(running),
                                                                 len(stopped)))
        if running or pending:
            print('run: {0} is running'.format(run_id))

        if all([status == 'Terminated' for status in statuses]):
            print('run: {0} is finished'.format(run_id))

        print(msg)


    # --------------------------------------------------------------------------
    #
    def _create_container_client(self, cred):
        
        client = ContainerInstanceManagementClient(credential=DefaultAzureCredential(), 
                                                    subscription_id=cred['az_sub_id'])

        return client


    # --------------------------------------------------------------------------
    #
    def _create_resource_client(self, cred):
        
        client = ResourceManagementClient(credential=DefaultAzureCredential(), 
                                            subscription_id=cred['az_sub_id'])

        return client


    # --------------------------------------------------------------------------
    #
    def create_resource_group(self):

        # Create (and then get) a resource group into which the container groups
        # are to be created
        self.resource_group_name = generate_id(prefix='hydraa-rg', length=4)

        self.logger.trace("creating resource group {0}".format(
            self.resource_group_name))
        
        # Create the resource group
        self.res_client.resource_groups.create_or_update(
            self.resource_group_name, {'location': self.vms[0].Region})

        # Get the resource group object
        resource_group = self.res_client.resource_groups.get(
            self.resource_group_name)

        self.logger.trace("resource group {0} is created".format(
            self.resource_group_name))

        return resource_group


    # --------------------------------------------------------------------------
    #
    def create_container_group(self, resource_group, contianers):
        """Creates a container group with a single.multiple container(s).

        Arguments:

            resource_group {azure.mgmt.resource.resources.models.ResourceGroup}
                        -- The resource group in which to create the container group.
            container_group_name {str}
                        -- The name of the container group to create.
            container_image_name {str}
                        -- The container image name and tag, for example:
                        mcr.microsoft\aci-helloworld:latest
            command = ['/bin/sh', '-c', 'echo FOO BAR && tail -f /dev/null']
        """
        self._container_group_name = generate_id('hydraa-cgr', length=4)

        group = ContainerGroup(location=resource_group.location,
                               containers=contianers,
                               os_type=OperatingSystemTypes.linux,
                               restart_policy=ContainerGroupRestartPolicy.never)

        # Create the container group
        cg = self.con_client.container_groups.begin_create_or_update(resource_group.name,
                                                               self._container_group_name,
                                                                                    group)

        # Get the created container group
        container_group = self.con_client.container_groups.get(resource_group.name,
                                                         self._container_group_name)
        
        self._container_group_names[self._container_group_name] = OrderedDict()

        return self._container_group_name


    # --------------------------------------------------------------------------
    #
    def submit_to_aks(self, ctasks):
        """
        submit a single pod per batch of tasks
        """
        self.profiler.prof('submit_batch_start', uid=self.run_id)
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = AZURE
            ctask.launch_type = self.launch_type 

            self._tasks_book[str(ctask.name)] = ctask
            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.cluster.submit(ctasks)
        
        # create entry for the pod in the pods book
        '''
        for idx, pod_name in enumerate(pods_names):
            self._pods_book[pod_name] = OrderedDict()
            self._pods_book[pod_name]['manager_id']    = self.manager_id
            self._pods_book[pod_name]['task_list']     = batches[idx]
            self._pods_book[pod_name]['batch_size']    = len(batches[idx])
            self._pods_book[pod_name]['pod_file_path'] = depolyment_file
        '''
        self.logger.trace('batch of [{0}] tasks is submitted '.format(len(ctasks)))

        self.profiler.prof('submit_batch_stop', uid=self.run_id)


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        cpcg = self._schedule(ctasks)
        for batch in cpcg:
            containers = []
            for ctask in batch:
                ctask.run_id      = self.run_id
                ctask.id          = self._task_id
                ctask.name        = 'ctask-{0}'.format(self._task_id)
                ctask.provider    = AZURE
                ctask.launch_type = self.launch_type

                # the minimum memory for a container is 0.1 GB
                # and it should be an increment of 0.1
                memory = round(ctask.memory / 1000, 1)
                if memory < 0.1:
                    memory = 0.1
                    self.logger.trace('setting task memory to {0} GB'.format(memory))                    
    
                container_resource_requests = ResourceRequests(memory_in_gb=memory,
                                                                   cpu=ctask.vcpus)
                container_resource_requirements = ResourceRequirements(
                                            requests=container_resource_requests)

                # set env vars for each container
                az_vars =[]
                if ctask.env_var:
                    for var in ctask.env_var:
                        tmp_var = var.split('=')
                        tmp_var = EnvironmentVariable(name=tmp_var[0], value=tmp_var[1])
                        az_vars.append(tmp_var)

                container = Container(name=ctask.name,
                                      image=ctask.image,
                                      resources=container_resource_requirements,
                                      command=ctask.cmd,
                                      environment_variables=az_vars)
                containers.append(container)

                self._tasks_book[str(ctask.name)] = ctask
                self.logger.trace('submitting tasks {0}'.format(ctask.id))

                self._task_id +=1

            if not self.resource_group:
                raise TypeError('resource group can not be empty')

            try:
                # create container groups and submit
                contaier_group_name = self.create_container_group(self.resource_group, containers)
                self._container_group_names[contaier_group_name]['task_list'] = batch
                self._container_group_names[contaier_group_name]['batch_size'] = len(batch)
                self._container_group_names[contaier_group_name]['manager_id'] = self.manager_id
                self._container_group_names[contaier_group_name]['resource_name'] = self.resource_group_name
    
            except Exception as e:
                # upon failure mark the tasks
                # as failed
                for ctask in batch:
                    self.logger.error('failed to submit {0}, check task exception'.format(ctask.name))
                    ctask.set_exception(e)


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self, container_group_names):

        statuses = {}
        stopped  = []
        failed   = []
        running  = []

        groups = [g for g in container_group_names.keys()]
        for group in groups:
            container_group = self.con_client.container_groups.get(self.resource_group_name,
                                                                                        group)
            
            for container in container_group.as_dict()['containers']:
                name = container.get('name', '')
                container_task = container.get('instance_view', {})
                if name and container_task:
                    try:
                        if container_task['current_state']['state'] == 'Terminated':
                            if  container_task['current_state']['exit_code'] == 0:
                                stopped.append(name)
                            else:
                                failed.append(name)
                        
                        elif container_task['current_state']['state'] == 'Running':
                            running.append(name)
                        
                        elif container_task['current_state']['state'] == 'Waiting':
                            events = container.get('events', {})
                            if events:
                                for e in events:
                                    if e['name'] == 'Failed':
                                        failed.append(name)
                                    else:
                                        pass
                        else:
                            running.append(name)

                    except AttributeError:
                        self.logger.warning('no task statuses avilable yet')
                        time.sleep(1)

            statuses = {'stopped': stopped, 'failed': failed, 'running':running}

        return statuses


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        marked_tasks = set()

        while not self._terminate.is_set():

            if self.launch_type in AKS:
                statuses = self.cluster._get_task_statuses()
            else:
                statuses = self._get_task_statuses(self._container_group_names)

            if statuses:
                msg = '[failed: {0}, done {1}, running {2}]'.format(len(statuses['failed']),
                                                                    len(statuses['stopped']),
                                                                    len(statuses['running']))

                for task in self._tasks_book.values():
                    if task in marked_tasks:
                        if task.state == 'FAILED':
                            # state is changed so reset the task state to 'PENDING'
                            if task.name not in statuses['failed']:
                                task.reset_state()
                                marked_tasks.remove(task)
                        else:
                            continue

                    if task.name in statuses['stopped']:
                        if not task.done():
                            task.state = 'DONE'
                            task.set_result('Done')
                            marked_tasks.add(task)

                    elif task.name in statuses['failed']:
                        if task.state != 'FAILED':
                            task.state = 'FAILED'
                            task.set_exception(Exception('Failed'))
                            marked_tasks.add(task)

                    elif task.name in statuses['running']:
                        if not task.running():
                            task.state = 'RUNNING'
                            task.set_running_or_notify_cancel()

                self.outgoing_q.put(msg)
    
                time.sleep(5)
            else:
                time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def _get_tasks_stamps(self):

        task_stamps = OrderedDict()
        containers = []
        groups = [g for g in self._container_group_names.keys()]
        for group in groups:
            container_group = self.con_client.container_groups.get(self.resource_group_name,
                                                                                        group)
            containers.extend(c for c in container_group.containers)

        for container in containers:
            tname = container.name
            events = container.instance_view.events
            task_stamps[tname] = OrderedDict()

            for event in events:
                task_stamps[tname][event.name+'_start'] = datetime.datetime.timestamp(event.first_timestamp)
                task_stamps[tname][event.name+'_stop']  = datetime.datetime.timestamp(event.last_timestamp)

                state = container.instance_view.current_state
                task_stamps[tname][state.state+'_start'] = datetime.datetime.timestamp(state.start_time)
                task_stamps[tname][state.state+'_stop']  = datetime.datetime.timestamp(state.finish_time)
        
        return task_stamps


    # --------------------------------------------------------------------------
    #
    def profiles(self):

        fname = '{0}/{1}_{2}_ctasks.csv'.format(self.sandbox,
                                       len(self._tasks_book),
                                             self.manager_id)

        if os.path.isfile(fname):
            print('profiles already exist {0}'.format(fname))
            return fname

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')
        
        
        if self.launch_type  in AKS:
            pod_stamps  = self.cluster.get_pod_status()
            task_stamps = self.cluster.get_pod_events()
            fname = '{0}/{1}_{2}_ctasks.csv'.format(self.sandbox,
                                                    len(self._tasks_book),
                                                    self.cluster.size)
            df = (pd.merge(pod_stamps, task_stamps, on='Task_ID'))
        else:
        
            task_stamps = self._get_tasks_stamps()
            df = pd.DataFrame(task_stamps.values(), index =[t for t in task_stamps.keys()])

        df.to_csv(fname)
        print('Dataframe saved in {0}'.format(fname))

        return fname


    # --------------------------------------------------------------------------
    #
    def ttx(self):
        fcsv = self.profiles()
        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')
        
        df = pd.read_csv(fcsv)
        st = df['Pulling_start'].min()
        en = df['Terminated_stop'].max()
        ttx = en - st
        return '{0} seconds'.format(ttx)


    # --------------------------------------------------------------------------
    #
    def list_container_groups(self, resource_group):
        """Lists the container groups in the specified resource group.

        Arguments:
        aci_client {azure.mgmt.containerinstance.ContainerInstanceManagementClient}
                    -- An authenticated container instance management client.
        resource_group {azure.mgmt.resource.resources.models.ResourceGroup}
                    -- The resource group containing the container group(s).
        """
        self.logger.trace("Listing container groups in resource group {0}".format(
                                                             resource_group.name))

        container_groups = self.con_client.container_groups.list_by_resource_group(
                                                                resource_group.name)

        for container_group in container_groups:
            self.logger.trace("found  {0}".format(container_group.name))


    # --------------------------------------------------------------------------
    #
    def _schedule(self, tasks):

        task_batch = copy.copy(tasks)
        batch_size = len(task_batch)
        if not batch_size:
            raise Exception('Batch size can not be 0')


        tasks_per_container_grp = []

        container_grps = math.ceil(batch_size / CPCG)

        if container_grps > CGPRG:
            raise Exception('container groups per ({0}) resource group > ({1})'.format(container_grps,
                                                                                               CGPRG))

        # If we cannot split the
        # number into exactly 'container_grps of 10' parts
        if(batch_size < container_grps):
            print(-1)
    
        # If batch_size % container_grps == 0 then the minimum
        # difference is 0 and all
        # numbers are batch_size / container_grps
        elif (batch_size % container_grps == 0):
            for i in range(container_grps):
                tasks_per_container_grp.append(batch_size // container_grps)
        else:
            # upto container_grps-(batch_size % container_grps) the values
            # will be batch_size / container_grps
            # after that the values
            # will be batch_size / container_grps + 1
            zp = container_grps - (batch_size % container_grps)
            pp = batch_size // container_grps
            for i in range(container_grps):
                if(i>= zp):
                    tasks_per_container_grp.append(pp + 1)
                else:
                    tasks_per_container_grp.append(pp)
        
        batch_map = tasks_per_container_grp
        
        objs_batch = []
        for batch in batch_map:
           objs_batch.append(task_batch[:batch])
           task_batch[:batch]
           del task_batch[:batch]
        return(objs_batch)


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):

        if not (self.resource_group_name and self.status):
            return

        self.logger.trace("termination started")

        self._terminate.set()

        for key, val in self._container_group_names.items():
            self.logger.trace(("terminating container group {0}".format(key)))
            self.con_client.container_groups.begin_delete(self.resource_group_name, key)
        
        self.logger.trace(("terminating resource group {0}".format(self.resource_group_name)))
        self.res_client.resource_groups.begin_delete(self.resource_group_name)

        if self.cluster:
            self.cluster.shutdown()

        self.resource_group_name = None
        self.status = False
