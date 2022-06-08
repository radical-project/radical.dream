import sys
import math
import uuid
import time
import atexit

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
        
        
        self.status = ACTIVE

        run_id = str(uuid.uuid4())

        container_image_app = "screwdrivercd/noop-container"

        cpcg = self._schedule(batch_size)

        res_group = self.create_resource_group()
        
        for batch in cpcg:
            containers, tasks = self.run_ctask(batch, memory, cpu, container_image_app)
            contaier_group_name = self.create_container_group(res_group, containers)
            self._container_group_names[contaier_group_name]['run_id']     = run_id
            self._container_group_names[contaier_group_name]['task_list']  = tasks
            self._container_group_names[contaier_group_name]['batch_size'] = batch

        if self.asynchronous:
            return run_id
        
        self._wait_tasks()

    
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
        self._resource_group_name = 'hydraa-rg-{0}'.format(self.manager_id)

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
        self._container_group_name = 'hydraa-contianer-gr-{0}'.format(str(uuid.uuid4()))

        print("Creating container group '{0}'...".format(self._container_group_name))

        group = ContainerGroup(location=resource_group.location,
                               containers=contianers,
                               os_type=OperatingSystemTypes.linux,
                               restart_policy=ContainerGroupRestartPolicy.never)

        # Create the container group
        cg = self._con_client.container_groups.begin_create_or_update(resource_group.name,
                                                              self._container_group_name,
                                                                                  group)
        #while cg.done() is False:
        #    sys.stdout.write('.')
        #    time.sleep(1)

        # Get the created container group
        container_group = self._con_client.container_groups.get(resource_group.name,
                                                         self._container_group_name)
        
        self._container_group_names[self._container_group_name] = OrderedDict()

        return self._container_group_name


    # --------------------------------------------------------------------------
    #
    def run_ctask(self, container_batch, memory, cpu, container_image_name,
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
            self._task_ids[str(self._task_id)]  = task_name
            print(('submitting tasks {0}/{1}').format(self._task_id, container_batch - 1))

            self._task_id +=1
        

        return container_list, tasks_names
    
    
    def _wait_tasks(self):

        if self.asynchronous:
            raise Exception('Task wait is not supported in asynchronous mode')
        
        UP = "\x1B[3A"
        CLR = "\x1B[0K"
        print("\n\n")

        groups = [g for g in self._container_group_names.keys()]

        while True:
            for group in groups:
                container_group = self._con_client.container_groups.get(self._resource_group_name,
                                                                                            group)
                for container in container_group.containers:
                    print(container.instance_view.current_state.state)


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
    

    def _shutdown(self):
        if not self._resource_group_name and self.status == False:
            return

        for key, val in self._container_group_names.items():
            print(print("terminating container group {0}".format(key)))
            del_op = self._con_client.container_groups.begin_delete(self._resource_group_name, key)
        
        print(print("terminating resource group {0}".format(self._resource_group_name)))
        self.res_client.resource_groups.begin_delete(self._resource_group_name)
        




    









    
        