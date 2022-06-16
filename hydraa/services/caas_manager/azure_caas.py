import os
import sys
import math
import uuid
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
    def run(self, launch_type, batch_size=1, budget=0, cpu=0, memory=0,
                                          time=0, container_path=None):
        
        if self.status:
            self.__cleanup()

        self.status = ACTIVE

        run_id = str(uuid.uuid4())

        # FIXME: this will be a user defined via class Cloud_Task
        container_image_app = "screwdrivercd/noop-container"

        cpcg = self._schedule(batch_size)

        res_group = self.create_resource_group()

        for batch in cpcg:
            containers, tasks = self.submit_ctasks(batch, memory, cpu, container_image_app)
            contaier_group_name = self.create_container_group(res_group, containers)
            self._container_group_names[contaier_group_name]['manager_id']    = self.manager_id
            self._container_group_names[contaier_group_name]['run_id']        = run_id
            self._container_group_names[contaier_group_name]['resource_name'] = self._resource_group_name
            self._container_group_names[contaier_group_name]['task_list']     = tasks
            self._container_group_names[contaier_group_name]['batch_size']    = batch
        
        self.runs_tree[run_id] =  self._container_group_names

        if self.asynchronous:
            return run_id
        
        self._wait_tasks(batch_size)


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
    def submit_ctasks(self, container_batch, memory, cpu, container_image_name,
                                                  start_command_line=None):

        tasks_names    = []
        container_list = []

        for container in range(container_batch):
            # Configure the container
            task_name = 'ctask-{0}'.format(self._task_id)
            container_resource_requests = ResourceRequests(memory_in_gb=memory,
                                                                       cpu=cpu)
            container_resource_requirements = ResourceRequirements(
                                          requests=container_resource_requests)

            container = Container(name=task_name,
                                  image=container_image_name,
                                  resources=container_resource_requirements,
                                  command=["/bin/echo", "noop"])

            tasks_names.append(task_name)
            container_list.append(container)
            self._task_ids[str(self._task_id)] = task_name
            print(('submitting tasks {0}/{1}').format(self._task_id, len(self._task_ids) - 1),
                                                                                     end='\r')

            self._task_id +=1

        return container_list, tasks_names


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self):
        
        statuses = []
        groups = [g for g in self._container_group_names.keys()]
        for group in groups:
            container_group = self._con_client.container_groups.get(self._resource_group_name,
                                                                                        group)
            try:
                statuses.extend(c.instance_view.current_state.state for c in container_group.containers)
            except AttributeError:
                time.sleep(1)

        return statuses


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self, batch_size):

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

            if len(statuses) == batch_size:
                if all([status == 'Terminated' for status in statuses]):
                    print('Finished, {0} tasks stopped with status: "Done"'.format(len(statuses)))
                    break

            print("{0}Pending: {1}{2}\nRunning: {3}{4}\nStopped: {5}{6}".format(UP,
                        len(pending), CLR, len(running), CLR, len(stopped), CLR))
            time.sleep(0.5)


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

            task_stamps[tname]['Pulling_start'] = datetime.datetime.timestamp(events[0].first_timestamp)
            task_stamps[tname]['Pulling_stop']  = datetime.datetime.timestamp(events[0].last_timestamp)

            task_stamps[tname]['Pulled_start']  = datetime.datetime.timestamp(events[1].first_timestamp)
            task_stamps[tname]['Pulled_stop']  = datetime.datetime.timestamp(events[1].last_timestamp)

            task_stamps[tname]['Created_start']  = datetime.datetime.timestamp(events[2].first_timestamp)
            task_stamps[tname]['Created_stop']   = datetime.datetime.timestamp(events[2].last_timestamp)

            task_stamps[tname]['Started_start']  = datetime.datetime.timestamp(events[3].first_timestamp)
            task_stamps[tname]['Started_stop']   = datetime.datetime.timestamp(events[3].last_timestamp)

            state = container.instance_view.current_state

            task_stamps[tname][state.state+'_start'] = datetime.datetime.timestamp(state.start_time)
            task_stamps[tname][state.state+'_stop']  = datetime.datetime.timestamp(state.finish_time)
        
        return task_stamps


    # --------------------------------------------------------------------------
    #
    def profiles(self):

        fname = 'azure_ctasks_df_{0}.csv'.format(self.manager_id)

        if os.path.isfile(fname):
            print('profiles already exist {0}'.format(fname))
            return fname

        task_stamps = self._get_tasks_stamps()

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

        df = pd.DataFrame(task_stamps.values(), index =[t for t in task_stamps.keys()])

        fname = 'azure_{0}_ctasks_{1}.csv'.format(len(self._task_ids), self.manager_id)
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
    def _schedule(self, batch_size):

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
        
        return tasks_per_container_grp


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










    
        