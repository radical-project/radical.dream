import os
import sys
import math
import uuid
import copy
import time
import atexit
import datetime

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

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


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
    def __init__(self, manager_id, cred, asynchronous, DryRun=False):
        
        self.manager_id = manager_id

        self.status = False

        # TODO: enable DryRun by the user to
        #       verify permissions before starting
        #       the actual run.
        self.DryRun = DryRun
        
        self.res_client  = self._create_resource_client(cred)
        self._con_client = self._create_container_client(cred)

        self._resource_group_name   = None
        self._container_group_names = OrderedDict()

        self.run_id       = None 
        self._task_id     = 0
        self._task_ids    = OrderedDict()

        self._region_name = cred['region_name']
        self._run_cost    = 0

        self.runs_tree = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        atexit.register(self._shutdown)


    # --------------------------------------------------------------------------
    #
    @property
    def is_active(self):
        return self.status


    # --------------------------------------------------------------------------
    #
    def run(self, ctasks, budget=0, time=0, container_path=None):
        
        if self.status:
            self.__cleanup()

        self.status = ACTIVE

        self.run_id = str(uuid.uuid4())

        cpcg = self._schedule(ctasks)

        res_group = self.create_resource_group()

        for batch in cpcg:
            containers, tasks = self.submit_ctasks(batch)
            contaier_group_name = self.create_container_group(res_group, containers)
            self._container_group_names[contaier_group_name]['manager_id']    = self.manager_id
            self._container_group_names[contaier_group_name]['resource_name'] = self._resource_group_name
            self._container_group_names[contaier_group_name]['task_list']     = tasks
            self._container_group_names[contaier_group_name]['batch_size']    = len(batch)
        
        self.runs_tree[self.run_id] =  self._container_group_names

        if self.asynchronous:
            return self.run_id
        
        self._wait_tasks(ctasks)


    # --------------------------------------------------------------------------
    #
    @property
    def get_runs_tree(self):
        return self.runs_tree


    # --------------------------------------------------------------------------
    #
    def get_run_status(self, run_id):
        #TODO: implement this
        pass


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

        for resource_group in self.res_client.resource_groups.list():
            if 'hydraa-rg' in resource_group.name:
                print('hydraa resource group exist')
                return resource_group
        
        # Create (and then get) a resource group into which the container groups
        # are to be created
        self._resource_group_name = 'hydraa-resource-group-{0}'.format(self.manager_id)

        print("Creating resource group '{0}'...".format(self._resource_group_name))
        self.res_client.resource_groups.create_or_update(self._resource_group_name,
                                             {'location': self._region_name})
        
        resource_group = self.res_client.resource_groups.get(self._resource_group_name)
        
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
                        microsoft\aci-helloworld:latest
            command = ['/bin/sh', '-c', 'echo FOO BAR && tail -f /dev/null']
        """
        self._container_group_name = 'hydraa-contianer-group-{0}'.format(str(uuid.uuid4()))

        #print("Creating container group '{0}'...".format(self._container_group_name), end='\r')

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


    # --------------------------------------------------------------------------
    #
    def submit_ctasks(self, ctasks_batch):

        tasks          = []
        container_list = []

        for ctask in ctasks_batch:
            ctask.run_id   = self.run_id
            ctask.id       = self._task_id
            ctask.name     = 'ctask-{0}'.format(self._task_id)
            ctask.provider = AZURE

            container_resource_requests = ResourceRequests(memory_in_gb=ctask.memory,
                                                                     cpu=ctask.vcpus)
            container_resource_requirements = ResourceRequirements(
                                          requests=container_resource_requests)

            container = Container(name=ctask.name,
                                  image=ctask.image,
                                  resources=container_resource_requirements,
                                  command=ctask.cmd)

            tasks.append(ctask)
            container_list.append(container)
            self._task_ids[str(ctask.id)] = ctask.name
            print(('submitting tasks {0}/{1}').format(ctask.id, len(self._task_ids) - 1),
                                                                                end='\r')

            self._task_id +=1

        return container_list, tasks


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self):

        statuses = []
        groups = [g for g in self._container_group_names.keys()]
        for group in groups:
            container_group = self._con_client.container_groups.get(self._resource_group_name,
                                                                                        group)
            
            ctasks = self._container_group_names[group]['task_list']

            try:
                for i in range(len(ctasks)):
                    ctasks[i].events = container_group.containers[i].instance_view.events
                    ctasks[i].state  = container_group.containers[i].instance_view.current_state.state
                statuses.extend(c.instance_view.current_state.state for c in container_group.containers)
            
            except AttributeError:
                time.sleep(0.2)

        return statuses


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self, ctasks):

        if self.asynchronous:
            raise Exception('Task wait is not supported in asynchronous mode')
        
        UP = "\x1B[3A"
        CLR = "\x1B[0K"
        print("\n\n")

        while True:

            statuses = self._get_task_statuses()

            pending = list(filter(lambda pending: pending == 'Waiting', statuses))
            running = list(filter(lambda pending: pending == 'Running', statuses))
            stopped = list(filter(lambda pending: pending == 'Terminated', statuses))

            if all([status == 'Terminated' for status in statuses]):
                print('Finished, {0} tasks stopped with status: "Done"'.format(len(statuses)))
                break

            print("{0}Pending: {1}{2}\nRunning: {3}{4}\nStopped: {5}{6}".format(UP,
                        len(pending), CLR, len(running), CLR, len(stopped), CLR))
            time.sleep(0.2)


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

        fname = '{0}_{1}_ctasks_{2}.csv'.format(AZURE, len(self._task_ids), self.manager_id)

        if os.path.isfile(fname):
            print('profiles already exist {0}'.format(fname))
            return fname

        task_stamps = self._get_tasks_stamps()

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

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
    def list_container_groups(resource_group):
        """Lists the container groups in the specified resource group.

        Arguments:
        aci_client {azure.mgmt.containerinstance.ContainerInstanceManagementClient}
                    -- An authenticated container instance management client.
        resource_group {azure.mgmt.resource.resources.models.ResourceGroup}
                    -- The resource group containing the container group(s).
        """
        print("Listing container groups in resource group '{0}'...".format(
            resource_group.name))

        container_groups = self._con_client.container_groups.list_by_resource_group(
                                                                resource_group.name)

        for container_group in container_groups:
            print("  {0}".format(container_group.name))


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
            raise Exception('container groups per ({0}) resource group > ({1})'.format(container_grps, CGPRG))

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

        self._task_id     = 0
        self._task_ids.clear()
        self._run_cost    = 0

        if caller == '_shutdown':
            self.manager_id  = None
            self.status      = False
            self.res_client  = None
            self._con_client = None

            self._resource_group_name   = None
            self._container_group_names.clear()

            self.runs_tree.clear()
            print('done')


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        if not self._resource_group_name and self.status == False:
            return

        for key, val in self._container_group_names.items():
            print(("terminating container group {0}".format(key)))
            del_op = self._con_client.container_groups.begin_delete(self._resource_group_name, key)
        
        print(("terminating resource group {0}".format(self._resource_group_name)))
        self.res_client.resource_groups.begin_delete(self._resource_group_name)
        
        self.__cleanup()










    
        