import uuid

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

LOCAL_LTYPE = ['join', 'create']
OPTYPE = ['chameleon', 'jetstream2', 'local']
AZURE_LTYPE = ['ACI', 'aci', 'AKS', 'aks']
AWS_LTYPE  = ['FARGATE', 'fargate', 'EC2', 'ec2', 'EKS', 'eks']


# --------------------------------------------------------------------
#
class AwsVM:
    def __init__(self, launch_type: str,
                 image_id: str=None, min_count: int=0,
                 max_count: int=0, instance_id: str=None,
                 zones: list=[], **input_kwargs):

        self.Zones = zones
        self.Provider = 'aws'
        self.VmName = f'AwsVM-{uuid.uuid4()}'

        if launch_type not in AWS_LTYPE:
            raise ValueError(f'launch_type must be: {AWS_LTYPE}')

        self.LaunchType = launch_type

        if self.LaunchType in AWS_LTYPE[2:]:
            ec2_eks_required = ['image_id', 'min_count', 'max_count', 'instance_id']

            if not all([image_id, min_count, max_count, instance_id]):
                raise ValueError(f'EC2/EKS VM requires {ec2_eks_required} values to be set')

        self.ImageId = image_id
        self.MinCount = min_count
        self.MaxCount = max_count
        self.InstanceID = instance_id
        self.KeyPair = input_kwargs.get('KeyPair', None)
        self.UserData = input_kwargs.get('UserData', '')
        self.IamInstanceProfile = input_kwargs.get('IamInstanceProfile', {})
        self.TagSpecifications = [{'ResourceType': 'instance',
                                   'Tags'        : [{'Key':'Name',
                                                     'Value': self.VmName}]}]

        self.input_kwargs = input_kwargs


    # --------------------------------------------------------------------------
    #
    @property
    def __name__(self):
        return self.__class__.__name__.lower()

    # --------------------------------------------------------------------------
    #
    def _user_data(self, cluster_name, user_cmds=None):
        start_cmd = ''
        start_cmd +='#!/bin/bash \n '
        start_cmd += f'echo ECS_CLUSTER={cluster_name} >> '
        start_cmd += '/etc/ecs/ecs.config'

        if user_cmds:
            cmd = f'{start_cmd}; {user_cmds}'
            return cmd
        return start_cmd


    # --------------------------------------------------------------------------
    #
    def __call__(self, cluster):
        self.required_kwargs = {}
        self.required_kwargs['ImageId'] = self.ImageId           
        self.required_kwargs['MinCount'] = self.MinCount          
        self.required_kwargs['MaxCount'] = self.MaxCount          
        self.required_kwargs['InstanceType'] = self.InstanceID

        user_data = self._user_data(cluster, self.UserData)
        self.required_kwargs['UserData'] = user_data
        self.required_kwargs['TagSpecifications'] = self.TagSpecifications
        self.required_kwargs['IamInstanceProfile'] = self.IamInstanceProfile

        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class AzureVM:

    def __init__(self, launch_type, instance_id,
                 min_count, max_count, **input_kwargs):

        self.Provider = 'azure'
        self.VmName = f'AzureVM-{uuid.uuid4()}'
        self.MinCount = min_count
        self.MaxCount = max_count

        if launch_type not in AZURE_LTYPE:
            raise ValueError(f'launch_type must be one of: {AZURE_LTYPE}')

        self.LaunchType = launch_type
        self.InstanceID = instance_id
        self.input_kwargs = input_kwargs

    def __call__(self):

        self.required_kwargs = {}
        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class OpenStackVM:
    def __init__(self, provider, launch_type,
                 flavor_id: str, image_id: str,
                 min_count=1, max_count=1, **input_kwargs):

        self.VmId = None
        self.ImageId = image_id
        self.MinCount = min_count
        self.MaxCount = max_count
        self.FlavorId = flavor_id
        self.LaunchType = launch_type
        self.VmName = f'OpenStackVM-{uuid.uuid4()}'

        if provider not in OPTYPE:
            raise ValueError(f'OpenStack VM provider must be one of {OPTYPE}')

        self.Provider       = provider
        self.SecurityGroups = input_kwargs.get('security_groups', '')
        self.Network        = input_kwargs.get('networks', '')
        self.KeyPair        = input_kwargs.get('keypair', [])
        self.Rules          = input_kwargs.get('rules', [])
        self.Subnet         = input_kwargs.get('subnet', '')
        self.Port           = input_kwargs.get('port', None)
        self.input_kwargs   = input_kwargs


    def __call__(self):

        self.required_kwargs = {}
        self.required_kwargs['image_id']     = self.ImageId
        self.required_kwargs['flavor_id']    = self.FlavorId
        self.required_kwargs['launch_type']  = self.LaunchType
        self.required_kwargs['min_count']    = self.MinCount
        self.required_kwargs['max_count']    = self.MaxCount 

        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class LocalVM(OpenStackVM):
    def __init__(self, launch_type, **input_kwargs):

        if launch_type not in LOCAL_LTYPE:
            raise ValueError(f'launch_type must be one of: {LOCAL_LTYPE}')

        super().__init__(provider='local', launch_type=launch_type,
                         flavor_id=None, image_id=None,
                         min_count=1, max_count=1, **input_kwargs)

        self.Servers = []
        self.VmName = f'LocalVM-{uuid.uuid4()}'
