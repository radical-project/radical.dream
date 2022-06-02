from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources import ResourceManagementClient

class AzureCaas():
    def __init__(self, manager_id, cred, asynchronous):
        
        self.manager_id = manager_id

        self.status = False
        
        credential  = DefaultAzureCredential()
        self.res_client = ResourceManagementClient(credential=credential, 
                                       subscription_id=cred['az_sub_id'])
        
        self._con_client = ContainerInstanceManagementClient(credential=credential, 
                                                 subscription_id=cred['az_sub_id'])
        
        self._region_name =  cred['region_name']
        self.asynchronous = asynchronous
    
    def run(self, launch_type, batch_size=1, budget=0, cpu=0, memory=0,
                                          time=0, container_path=None):


        multi_container_group_name = container_group_name + "-multi"
        task_container_group_name = container_group_name + "-task"

        res_group = self.create_res_group()

        self.create_container_group(res_group, container_image_app)

    

    def create_res_group(self, resource_group_name):


        if self.res_client.resource_groups.check_existence(resource_group_name):
            res_group = self.res_client.resource_groups.get(resource_group_name)
            return resource_group

        for resource_group in self.res_client.resource_groups.list():
            if 'hydraa' in resource_group.name:
                print('hydraa resource group exist')
                return resource_group
        
        # Create (and then get) a resource group into which the container groups
        # are to be created
        resource_group_name = 'hydraa-rg-{0}'.format(self.manager_id))

        print("Creating resource group '{0}'...".format(resource_group_name))
        resource_group = self.res_client.resource_groups.create_or_update(resource_group_name,
                                                               {'location': azure_region})
        
        return resource_group


    def create_container_group(self, resource_group, container_group_name, container_image_name):
        """Creates a container group with a single container.
        Arguments:

            resource_group {azure.mgmt.resource.resources.models.ResourceGroup}
                        -- The resource group in which to create the container group.
            container_group_name {str}
                        -- The name of the container group to create.
            container_image_name {str}
                        -- The container image name and tag, for example:
                        microsoft\aci-helloworld:latest
        """

        container_group_name = 'hydraa-{0}'.format(self.manager_id))

        print("Creating container group '{0}'...".format(container_group_name))

        # Configure the container
        container_resource_requests = ResourceRequests(memory_in_gb=1, cpu=1.0)
        container_resource_requirements = ResourceRequirements(
            requests=container_resource_requests)
        container = Container(name=container_group_name,
                            image=container_image_name,
                            resources=container_resource_requirements,
                            ports=[ContainerPort(port=80)])

        # Configure the container group
        ports = [Port(protocol=ContainerGroupNetworkProtocol.tcp, port=80)]
        group_ip_address = IpAddress(ports=ports,
                                    dns_name_label=container_group_name,
                                    type="Public")
        group = ContainerGroup(location=resource_group.location,
                            containers=[container],
                            os_type=OperatingSystemTypes.linux,
                            ip_address=group_ip_address)

        # Create the container group
        self._con_client.begin_create_or_update.create_or_update(resource_group.name,
                                                                container_group_name,
                                                                               group)

        # Get the created container group
        container_group = self._con_client.container_groups.get(resource_group.name,
                                                               container_group_name)

        print("Once DNS has propagated, container group '{0}' will be reachable at"
            " http://{1}".format(container_group_name,
                                container_group.ip_address.fqdn))
            



    
        