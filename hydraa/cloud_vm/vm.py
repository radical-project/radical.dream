import uuid

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

LTYPE = ['FARGATE', 'fargate', 'EC2', 'ec2', 'EKS', 'eks']


# --------------------------------------------------------------------
#
class AwsVM:
    def __init__(self, launch_type: str, image_id: str, min_count: int,
                      max_count: int, instance_id: str, user_data: str, 
                                        profile: dict, **input_kwargs):

        self.VmName             = 'AWS_VM-{0}'.format(uuid.uuid4())
        self.ImageId            = image_id
        self.MinCount           = min_count
        self.MaxCount           = max_count
        self.InstanceID         = instance_id
        self.LaunchType         = launch_type
        self.UserData           = user_data
        self.IamInstanceProfile = profile
        self.KeyPair            = input_kwargs.get('keypair', None)
        self.TagSpecifications  = [{'ResourceType': 'instance',
                                    'Tags'        : [{'Key'  :'Name',
                                                      'Value': self.VmName}]}]

        if self.LaunchType not in LTYPE:
            raise Exception('LaunchType must be: {0}'.format(LTYPE))

        self.input_kwargs       = input_kwargs


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
        start_cmd += 'echo ECS_CLUSTER={0} >> '.format(cluster_name)
        start_cmd += '/etc/ecs/ecs.config'

        if user_cmds:
            cmd = '{0}; {1}'.format(start_cmd, user_cmds)
            return cmd
        return start_cmd


    # --------------------------------------------------------------------------
    #
    def __call__(self, cluster):
        self.required_kwargs = {}
        self.required_kwargs['ImageId']            = self.ImageId           
        self.required_kwargs['MinCount']           = self.MinCount          
        self.required_kwargs['MaxCount']           = self.MaxCount          
        self.required_kwargs['InstanceType']       = self.InstanceID

        user_data = self._user_data(cluster, self.UserData)
        self.required_kwargs['UserData']           = user_data
        self.required_kwargs['IamInstanceProfile'] = self.IamInstanceProfile
        self.required_kwargs['TagSpecifications']  = self.TagSpecifications

        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class AzureVM:
    def __init__(self, launch_type, instance_id, min_count, max_count,
                                                      **input_kwargs):

        self.VmName             = 'AZURE_VM-{0}'.format(uuid.uuid4())
        self.LaunchType         = launch_type
        self.InstanceID         = instance_id
        self.MinCount           = min_count
        self.MaxCount           = max_count
        self.input_kwargs       = input_kwargs

    def __call__(self):

        self.required_kwargs = {}
        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs


class OpenStackVM:
    def __init__(self, launch_type, flavor_id: str, image_id: str,  **input_kwargs):

        self.VmName         = 'OpenStackVM-{0}'.format(uuid.uuid4())
        self.VmId           = None
        self.LaunchType     = launch_type
        self.FlavorId       = flavor_id
        self.ImageId        = image_id
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

        kwargs = {**self.required_kwargs, **self.input_kwargs}
        return kwargs