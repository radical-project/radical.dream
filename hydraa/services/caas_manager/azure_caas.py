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

from hydraa.services.caas_manager.utils      import kubernetes


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


AKS       = ['AKS', 'aks']
AZURE     = 'azure'
ACTIVE    = True
WAIT_TIME = 2
AZURE_RGX = '[a-z0-9]([-a-z0-9]*[a-z0-9])?'


# NOTE: If we ask for 100 tasks for now it will fail.
#       free tier (current subscription) only supports
#       6 container groups per resource group (pay as you go is 60)

CPCG  = 6   # Number of containers per container group
CGPRG = 60  # Number of container groups per resource group




class AzureCaas():
    def __init__(self, sandbox, manager_id, cred, VM, asynchronous,
                                          log, prof, DryRun=False):
        

        self.manager_id = manager_id

        self.status = False

        # TODO: enable DryRun by the user to
        #       verify permissions before starting
        #       the actual run.
        self.DryRun = DryRun
        
        self.res_client  = self._create_resource_client(cred)
        self._con_client = self._create_container_client(cred)

        self.AKS_Cluster            = None
        self._resource_group        = None
        self._resource_group_name   = None
        self._container_group_names = OrderedDict()

        # tasks_book is a datastructure that keeps most of the 
        # cloud tasks info during the current run.
        self._task_id     = 0
        self._tasks_book  = OrderedDict()

        self.vm        = VM
        self.vm.Region = cred['region_name']

        self.run_id   = '{0}.{1}'.format(self.vm.LaunchType, str(uuid.uuid4()))
        self.run_cost = 0

        self.runs_tree = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, AZURE, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.logger   = log
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()

        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start, name='AzureCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

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

        self._resource_group = self.create_resource_group()
        
        self.runs_tree[self.run_id] =  self._container_group_names

        self.profiler.prof('prep_stop', uid=self.run_id)

        if self.vm.LaunchType  in AKS:
            self.vm.ResourceGroup = self._resource_group
            self.AKS_Cluster = kubernetes.AKS_Cluster(self.run_id, self.vm, self.sandbox)
            self.AKS_Cluster.bootstrap()

        else:  
            pass
        
        # call get work to pull tasks
        self._get_work()

        # now set the manager as active
        self.status = ACTIVE


    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = 100
        max_bulk_time = 2        # seconds
        min_bulk_time = 0.1      # seconds

        while not self._terminate.is_set():
            now = time.time()  # time of last submission
            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < max_bulk_time:
                try:
                    task = self.incoming_q.get(block=True, timeout=min_bulk_time)
                except queue.Empty:
                        task = None
                
                if task:
                        bulk.append(task)
                
                if len(bulk) >= max_bulk_size:
                        break

            if bulk:
                if self.vm.LaunchType  in AKS:
                    self.submit_to_aks(bulk)
                else:
                    self.submit(bulk)

                if not self.asynchronous:
                    self._wait_tasks()
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
        self._resource_group_name = 'hydraa-rg-{0}'.format(self.manager_id)

        self.logger.trace("creating resource group {0}".format(self._resource_group_name))
        self.res_client.resource_groups.create_or_update(self._resource_group_name,
                                                     {'location': self.vm.Region})

        resource_group = self.res_client.resource_groups.get(self._resource_group_name)

        self.logger.trace("resource group {0} is created".format(self._resource_group_name))

        return resource_group


    # --------------------------------------------------------------------------
    #
    def create_container_group(self, resource_group, contianers):
        """Creates a container group with a single.multiple container(s).
        https://docs.microsoft.com/en-us/azure/container-instances/container-instances-container-groups
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
        self._container_group_name = 'hydraa-contianer-group-{0}'.format(str(uuid.uuid4()))

        group = ContainerGroup(location=resource_group.location,
                               containers=contianers,
                               os_type=OperatingSystemTypes.linux,
                               restart_policy=ContainerGroupRestartPolicy.never)

        # Create the container group
        cg = self._con_client.container_groups.begin_create_or_update(resource_group.name,
                                                               self._container_group_name,
                                                                                    group)

        # Get the created container group
        container_group = self._con_client.container_groups.get(resource_group.name,
                                                         self._container_group_name)
        
        self._container_group_names[self._container_group_name] = OrderedDict()

        return self._container_group_name
    
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
            ctask.launch_type = self.vm.LaunchType 

            self._tasks_book[str(ctask.id)] = ctask.name
            self.logger.trace('submitting tasks {0}'.format(ctask.id))
            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.AKS_Cluster.submit(ctasks)
        
        # create entry for the pod in the pods book
        '''
        for idx, pod_name in enumerate(pods_names):
            self._pods_book[pod_name] = OrderedDict()
            self._pods_book[pod_name]['manager_id']    = self.manager_id
            self._pods_book[pod_name]['task_list']     = batches[idx]
            self._pods_book[pod_name]['batch_size']    = len(batches[idx])
            self._pods_book[pod_name]['pod_file_path'] = depolyment_file
        '''
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
                ctask.launch_type = self.vm.LaunchType 

                container_resource_requests = ResourceRequests(memory_in_gb=ctask.memory,
                                                                         cpu=ctask.vcpus)
                container_resource_requirements = ResourceRequirements(
                                            requests=container_resource_requests)

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

                self._tasks_book[str(ctask.id)] = ctask.name
                self.logger.trace('submitting tasks {0}'.format(ctask.id))

                self._task_id +=1

            if not self._resource_group:
                raise TypeError('resource group can not be empty')

            contaier_group_name = self.create_container_group(self._resource_group, containers)
            self._container_group_names[contaier_group_name]['manager_id']    = self.manager_id
            self._container_group_names[contaier_group_name]['resource_name'] = self._resource_group_name
            self._container_group_names[contaier_group_name]['task_list']     = batch
            self._container_group_names[contaier_group_name]['batch_size']    = len(batch)


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self, container_group_names):

        # FIXME: this function should performs a general task info pulling
        # (not only tasks statuses)
        statuses = []
        groups = [g for g in container_group_names.keys()]
        for group in groups:
            container_group = self._con_client.container_groups.get(self._resource_group_name,
                                                                                        group)

            ctasks = container_group_names[group]['task_list']

            try:
                for i in range(len(ctasks)):
                    ctasks[i].events = container_group.containers[i].instance_view.events
                    ctasks[i].state  = container_group.containers[i].instance_view.current_state.state
                    if ctasks[i].events:
                        # detect if the task is hanging in wait but already failed
                        for event in ctasks[i].events:
                            if event.name == "Failed" and ctasks[i].state == "Waiting":
                                ctasks[i].state = event.name
                                container_group.containers[i].instance_view.current_state.state = event.name
                            else:
                                pass
                    ctasks[i].exit_code = container_group.containers[0].instance_view.current_state.exit_code
                statuses.extend(c.instance_view.current_state.state for c in container_group.containers)

            except AttributeError:
                self.logger.warning('no task statuses avilable yet, sleeping')
                time.sleep(0.2)

        return statuses


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        if self.asynchronous:
            raise Exception('Task wait is not supported in asynchronous mode')
        
        if self.vm.LaunchType  in AKS:
            self.AKS_Cluster.wait()
            return

        while True:
            statuses = self._get_task_statuses(self._container_group_names)
            if statuses:
                pending = list(filter(lambda pending: pending == 'Waiting'   , statuses))
                running = list(filter(lambda running: running == 'Running'   , statuses))
                stopped = list(filter(lambda stopped: stopped == 'Terminated', statuses))

                if stopped:
                    for task in stopped:
                        self.outgoing_q.put(task)
                        self.logger.trace('task result sent to queue')

                if all([status == 'Terminated' or status == 'Failed' for status in statuses]):
                    break

                self.logger.trace("Pending: {0} Running: {1} Stopped: {2}".format(len(pending),
                                                                                  len(running),
                                                                                  len(stopped)))

            time.sleep(0.5)

        self.logger.trace('Finished, {0} tasks stopped with status: {1}'.format(len(statuses),
                                                                                    statuses))

    # --------------------------------------------------------------------------
    #
    def _get_tasks_stamps(self):

        task_stamps = OrderedDict()
        containers = []
        groups = [g for g in self._container_group_names.keys()]
        for group in groups:
            container_group = self._con_client.container_groups.get(self._resource_group_name,
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
        
        
        if self.vm.LaunchType  in AKS:
            pod_stamps  = self.AKS_Cluster.get_pod_status()
            task_stamps = self.AKS_Cluster.get_pod_events()
            fname       = '{0}/{1}_{2}_ctasks.csv'.format(self.sandbox,
                                                 len(self._tasks_book),
                                                 self.AKS_Cluster.size)
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

        container_groups = self._con_client.container_groups.list_by_resource_group(
                                                                resource_group.name)

        for container_group in container_groups:
            self.logger.trace("found  {0}".format(container_group.name))


    # --------------------------------------------------------------------------
    #
    def _schedule(self, tasks):

        task_batch = copy.deepcopy(tasks)
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
    def __cleanup(self):

        caller = sys._getframe().f_back.f_code.co_name
        self._container_group_names = OrderedDict()

        self._task_id = 0
        self.run_cost = 0
        self._tasks_book.clear()

        if caller == '_shutdown':
            self.manager_id  = None
            self.status      = False
            self.res_client  = None
            self._con_client = None

            self._resource_group_name   = None
            self._container_group_names.clear()

            self.runs_tree.clear()
            self.logger.trace('cleanup done')


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        if not self._resource_group_name and self.status == False:
            return
        
        self.logger.trace("termination started")

        for key, val in self._container_group_names.items():
            self.logger.trace(("terminating container group {0}".format(key)))
            del_op = self._con_client.container_groups.begin_delete(self._resource_group_name, key)
        
        self.logger.trace(("terminating resource group {0}".format(self._resource_group_name)))
        self.res_client.resource_groups.begin_delete(self._resource_group_name)
        
        self.__cleanup()
