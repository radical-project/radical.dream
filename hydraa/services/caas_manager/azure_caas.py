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

from hydraa.services.caas_manager.kubernetes import kubernetes
from hydraa.services.caas_manager.utils.misc import generate_id


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


AKS = ['AKS', 'aks']
ACI = ['ACI', 'aci']
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
    def __init__(self, sandbox, manager_id, cred, VMS, asynchronous, auto_terminate, log, prof):
        
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
        self.auto_terminate = auto_terminate

        self.run_id = '{0}.{1}'.format(self.launch_type, str(uuid.uuid4()))
        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, AZURE, self.run_id)
        os.mkdir(self.sandbox, 0o777)
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.res_client = self._create_resource_client(cred)
        self.con_client = self._create_container_client(cred)

        self.incoming_q = queue.Queue() # caas manager -> main manager
        self.outgoing_q = queue.Queue() # main manager -> caas manager
        self.internal_q = queue.Queue() # caas manager -> caas manager

        self._task_lock = threading.Lock()
        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start,
                                             name='AzureCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        # now set the manager as active
        self.status = True

        atexit.register(self.shutdown)


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
    def get_tasks(self):
        return list(self._tasks_book.values())


    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = os.environ.get('MAX_BULK_SIZE', 1024) # tasks
        max_bulk_time = os.environ.get('MAX_BULK_TIME', 2)    # seconds
        min_bulk_time = os.environ.get('MAX_BULK_TIME', 0.1)  # seconds

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
                with self._task_lock:
                    if self.launch_type in AKS:
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
    def create_and_submit_container_group(self, resource_group, contianers):
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

        self.logger.trace('batch of [{0}] tasks is submitted '.format(len(ctasks)))

        self.profiler.prof('submit_batch_stop', uid=self.run_id)


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        cpcg = self._schedule(ctasks)

        if not self.resource_group:
            raise TypeError('resource group can not be empty')

        for batch in cpcg:
            containers = []
            for ctask in batch:
                ctask.provider = AZURE
                ctask.id = self._task_id
                ctask.run_id = self.run_id
                ctask.launch_type = self.launch_type
                ctask.name = 'ctask-{0}'.format(self._task_id)

                # the minimum memory for a container is 0.1 GB
                # and it should be an increment of 0.1
                if ctask.memory < 0.1:
                    ctask.memory = 0.1                   

                container_resource_requests = ResourceRequests(cpu=ctask.vcpus,
                                                               memory_in_gb=ctask.memory)
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

            try:
                # create container groups (pod) and submit
                contaier_group_name = self.create_and_submit_container_group(self.resource_group,
                                                                             containers)
                for ctask in batch:
                    ctask.contaier_group_name = contaier_group_name

                self._container_group_names[contaier_group_name]['task_list'] = batch
                self._container_group_names[contaier_group_name]['batch_size'] = len(batch)
                self._container_group_names[contaier_group_name]['manager_id'] = self.manager_id
                self._container_group_names[contaier_group_name]['resource_name'] = self.resource_group_name

            except Exception as e:
                # upon failure mark the tasks as failed
                for ctask in batch:
                    ctask.state = 'Failed'
                    ctask.set_exception(e)


    # --------------------------------------------------------------------------
    #
    def aci_tasks_statuses_watcher(self, container_group_names=None):

        if not container_group_names:
            container_group_names = self._container_group_names

        def _watch():

            last_seen_msg = None
        
            while not self._terminate.is_set():
                msgs = []
                with self._task_lock:
                    tasks = self.get_tasks()

                # make sure all tasks have container group names and registered
                if not tasks or not all(t.contaier_group_name for t in tasks):
                    time.sleep(0.1)
                    continue

                # container_group (i.e. Pod)
                for group in container_group_names.keys():
                    container_group = self.con_client.container_groups.get(self.resource_group_name,
                                                                           group)
                    status = container_group.instance_view.state
                    if status and status in ['Pending', 'Running', 'Succeeded', 'Failed']:

                        containers = container_group.containers
                        for cont in containers:
                            msg = {}

                            # skip containers that has no status yet 
                            if not cont.as_dict().get('instance_view'):
                                continue

                            cont_status = cont.instance_view.current_state
                            if cont_status.state == 'Terminated':
                                if cont_status.exit_code == 0:
                                    msg = {'id': cont.name,
                                           'status': 'Completed'}
                                else:
                                    msg = {'id': cont.name,
                                           'status': 'Failed',
                                           'exception': cont_status.detail_status}

                            elif cont_status.state == 'Waiting':

                                # check if the container was terminated previously and stuck in restarting
                                prev_state = cont.instance_view.as_dict().get('previous_state')
                                if prev_state and cont.instance_view.previous_state.state == 'Terminated':
                                    msg = {'id': cont.name,
                                           'status': 'Failed',
                                           'exception': cont_status.detail_status}
                                else:
                                    msg = {'id': cont.name,
                                           'status': 'Pending',
                                           'reason': cont_status.detail_status}

                            elif cont_status.state == 'Running':
                                msg = {'id': cont.name,
                                       'status': 'Running'}

                            else:
                                msg = {'id': cont.name,
                                       'status': cont_status.state}
                            if msg:
                                msgs.append(msg)
                    else:
                        pass

                if msgs and msgs != last_seen_msg:
                    self.internal_q.put({'pod_id': container_group.name,
                                         'pod_status': status,
                                         'containers': msgs})
                    last_seen_msg = msgs

            time.sleep(0.5)

        watcher = threading.Thread(target=_watch, daemon=True, name='AciEventWatcher')
        watcher.start()

        self.logger.trace(f'watcher thread {watcher.ident} started on {self.resource_group_name}')


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        msg = None
        finshed = []
        failed, done, running = 0, 0, 0

        if self.launch_type in AKS:
            queue = self.cluster.return_queue

        else:
            # set the queue and start the ecs watcher thread
            queue = self.internal_q
            self.aci_tasks_statuses_watcher()

        while not self._terminate.is_set():

            try:
                # pull a message from the cluster queue
                if not queue.empty():
                    _msg = queue.get(block=True, timeout=1)

                    if _msg:
                        parent_pod = _msg.get('pod_id')
                        containers = _msg.get('containers')

                    else:
                        continue

                    for cont in containers:

                        tid = cont.get('id')
                        status = cont.get('status')
                        task = self._tasks_book.get(tid)

                        msg = f'Task: "{task.name}" from pod "{parent_pod}" is in state: "{status}"'

                        if not task:
                            raise RuntimeError(f'task {cont.name} does not exist, existing')

                        if task.name in finshed:
                            continue

                        if not status:
                            continue

                        if status == 'Completed':
                            if task.running():
                                running -= 1
                            task.set_result('Finished successfully')
                            finshed.append(task.name)
                            done += 1

                        elif status == 'Running':
                            if not task.running():
                                task.set_running_or_notify_cancel()
                                running += 1
                            else:
                                continue

                        elif status == 'Failed':
                            if task.tries:
                                task.tries -= 1
                                task.reset_state()
                            else:
                                if task.running():
                                    running -= 1
                                exception = cont.get('exception')
                                task.set_exception(Exception(exception))
                                finshed.append(task.name)
                                failed += 1

                        elif status == 'Pending':
                            reason = cont.get('reason')
                            message = cont.get('message')
                            msg += f': reason: {reason}, message: {message}'

                        # preserve the task state for future use
                        task.state = status

                    self.outgoing_q.put(msg)

                    if len(finshed) == len(self._tasks_book):
                        if self.auto_terminate:
                            termination_msg = (0, AZURE)
                            self.outgoing_q.put(termination_msg)

            except queue.Empty:
                time.sleep(0.1)
                continue



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
        
        
        if self.launch_type in AKS:
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
    def shutdown(self):

        if not (self.resource_group_name and self.status):
            return

        self.logger.trace("termination started")

        self._terminate.set()

        if self.cluster:
            self.cluster.shutdown()

        for key, val in self._container_group_names.items():
            self.logger.trace(("terminating container group {0}".format(key)))
            self.con_client.container_groups.begin_delete(self.resource_group_name, key)
        
        self.logger.trace(("terminating resource group {0}".format(self.resource_group_name)))
        self.res_client.resource_groups.begin_delete(self.resource_group_name)

        self.status = False
        self.resource_group_name = None
