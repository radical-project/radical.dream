import os
import json
import boto3
import pprint
import base64

from src.provider_proxy import proxy
"""
~~~~~~~~~~~~~~~~

A simple script that demonstrates how the docker and AWS Python clients
can be used to automate the process of: building a Docker image, as
defined by the Dockerfile in the project's root directory; pushing the
image to AWS's Elastic Container Registry (ECR); and, then forcing a
redeployment of a AWS Elastic Container Service (ECS) that uses the
image to host the service. 

For now, it is assumed that the AWS infrastructure is already in
existence and that Docker is running on the host machine.
"""

WAIT_TIME = 2

class AwsCaas(object):
    def __init__(self):
        """
        The components of AWS ECS form the following hierarchy:

        1-Cluster: A cluster is a logical grouping of tasks or services

        2-Task Definition: The task definition is a text file, in JSON 
                            format, that describes one or more containers,
                            up to a maximum of ten, that form your application.
                            It can be thought of as a blueprint for your application.

        3-Service: allows you to run and maintain a specified number of instances of
                   a task definition simultaneously in an AWS ECS cluster

        4-Task: is the instantiation of a task definition within a cluster
        """
        self._ecs_client = None
        self._ec2_client = None

        self._cluster_name = "BotoCluster"
        self._service_name = "service_hello_world"
        self._task_name    = None

        self._task_ids     = []

    def run_aws_container(self, cred, container_path):
        """
        Build Docker image, push to AWS and update ECS service.
        """
        # The user will provide the mem and cpu once they do that
        # a fucntion [here] should calculate the cost
        #
        #
        # cost = self._calculate_container_cost(time, cpu, mem)
        #
        # FIXME: ask the user if they want to continue to the 
        #        execution based on the cost

        # 1-Create Clients
        self._ecs_client = self._create_ecs_client(cred)
        self._ec2_client = self._create_ec2_client(cred)
        self._iam_client = self._create_iam_client(cred)

        # 2-Create a cluster (this should be done once)
        cluster = self.build_new_cluster(self._cluster_name)

        # 3-Create Container definition
        container_def = self.create_container_def()

        # 4-Create a task definition
        task_name, task_def_arn = self.create_task_def(container_def)
        
        # 5-create a service (this should be done once)
        _ = self.create_ecs_service()

        # 6-create ec2 instance
        self.create_ec2_instance(cluster)

        # 7-Start the task
        tasks = self.run_task(task_def_arn, cluster)

        # 8-Wait on task completion
        self._wait_tasks(tasks, cluster)


    
    def _calculate_container_cost(self, time, cpu, mem):
        raise NotImplementedError


    def _create_ec2_client(self, cred):
        ec2_client = boto3.client('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                           aws_secret_access_key = cred['aws_secret_access_key'],
                                           region_name           = cred['region_name'])
        
        print('ec2 client created')

        return ec2_client
    

    def _create_ecs_client(self, cred):
        ecs_client = boto3.client('ecs', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        print('ecs client created')
        return ecs_client
    

    def _create_iam_client(self, cred):
        iam_client = boto3.client('iam', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        
        print('iam client created')
        return iam_client
    

    def create_container_def(self, name ='hello_world_container',
                                                 cpu=1, memory=1,
                                                 image='ubuntu'):
        con_def = {'name'        : name,
                   'cpu'         : cpu,
                   'memory'      : memory,
                   'portMappings': [],
                   'essential'   : True,
                   'environment' : [],
                   'mountPoints' : [],
                   'volumesFrom' : [],
                   'image'       : image,
                   'command'     : ['/bin/echo', 'hello world']}

        return con_def
    

    def create_task_def(self, container_def):
        """
        create a container defination
        """
        # FIXME: This a registering with minimal definition,
        #        user should specifiy how much per task (cpu/mem) 
        task_def = {'family' :  'hello_world',
                    'volumes': [],
                    'cpu'    : '256',
                    'memory' : '512',
                    'containerDefinitions'   : [container_def],
                    'executionRoleArn'       : 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole',
                    'networkMode'            : 'awsvpc',
                    'requiresCompatibilities': ['FARGATE']}
        
        reg_task     = self._ecs_client.register_task_definition(**task_def)
        
        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        # FIXME: this should be a uniqe name that this class assigns
        #        with the unique uuid
        self._task_name = "hello_world"
        
        print('task {0} is registered'.format(self._task_name))

        return self._task_name, task_def_arn
        
    
    
    def create_ecs_service(self):
        """
        Create service with exactly 1 desired instance of the task
        Info: Amazon ECS allows you to run and maintain a specified number
        (the "desired count") of instances of a task definition
        simultaneously in an ECS cluster.
        """

        # Check if the service already exist and use it
        running_services = self._ecs_client.list_services(cluster = self._cluster_name)

        for service in running_services['serviceArns']:
            if self._service_name in service:
                print('service {0} already exist on cluster {1}'.format(self._service_name, self._cluster_name))

                return service
        
        print('no exisitng service found, creating.....')
        response = self._ecs_client.create_service(cluster    = self._cluster_name,
                                                   serviceName= self._service_name,
                                                   taskDefinition = self._task_name,
                                                   launchType     = 'FARGATE',
                                                   desiredCount   = 1,
                                                   networkConfiguration = {'awsvpcConfiguration': {
                                                                          'subnets': ['subnet-094da8d73899da51c',],
                                                                          'assignPublicIp': 'ENABLED',
                                                                          'securityGroups': ["sg-0702f37d21c55da64"]}},
                                                   #clientToken='request_identifier_string',
                                                   deploymentConfiguration={'maximumPercent': 200,
                                                                            'minimumHealthyPercent': 50})
        

        
        print('service {0} created'.format(self._service_name))
        return response



    def start_task(self, task_def, cluster_name):
        """
        Starts a new task from the specified task definition on the
        specified container instance or instances. StartTask uses/assumes
        that you have your own scheduler to place tasks manually on specific
        container instances.
        """

        containers   = self._ecs_client.list_container_instances(cluster=cluster_name)
        container_id = containers["containerInstanceArns"][0].split("/")[-1]

        response = self._ecs_client.start_task(taskDefinition=task_def,
                                               overrides={},
                                               containerInstances=[container_id],
                                               startedBy="foo",)
    

    def run_task(self, task_def, cluster_name):
        """
        Starts a new task using the specified task definition. In this
        mode AWS scheduler will handle the task placement.

        cluster: The short name or full Amazon Resource Name
        (ARN) of the cluster to run your task on. If you do
        not specify a cluster, the default cluster is assumed.
        """
        kwargs = {}
        kwargs['count']           = 1
        kwargs['cluster']         = cluster_name
        kwargs['launchType']      = 'FARGATE'
        kwargs['overrides']       = {}
        kwargs['taskDefinition']  = task_def
        kwargs['platformVersion'] = 'LATEST'
        kwargs['networkConfiguration'] = {'awsvpcConfiguration': {'subnets': [
                                                                  'subnet-094da8d73899da51c',],
                                           'assignPublicIp'    : 'ENABLED',
                                           'securityGroups'    : ["sg-0702f37d21c55da64"]}}

        response = self._ecs_client.run_task(**kwargs)
        #print(response)

        if response['failures']:
            raise Exception(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'], failure['reason'])
                                       for failure in response['failures']]))
        else:
            print('running task {0}'.format(task_def))
        
        self._task_ids.append(response)
        
        task_ids = [t['taskArn'] for t in response['tasks']]
        
        return task_ids

    def _get_task_statuses(self, task_ids, cluster):
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Retrieve task statuses from ECS API

        Returns list of {RUNNING|PENDING|STOPPED} for each id in task_ids
        """
        response = self._ecs_client.describe_tasks(tasks=task_ids, cluster=cluster)

        # Error checking
        if response['failures'] != []:
            raise Exception('There were some failures:\n{0}'.format(
                response['failures']))
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            msg = 'Task status request received status code {0}:\n{1}'
            raise Exception(msg.format(status_code, response))

        return [t['lastStatus'] for t in response['tasks']]
    

    def _wait_tasks(self, task_ids, cluster):
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Wait for task status until STOPPED
        """
        import time
        while True:
            statuses = self._get_task_statuses(task_ids, cluster)
            if all([status == 'STOPPED' for status in statuses]):
                print('ECS tasks {0} STOPPED'.format(','.join(task_ids)))
                break
            time.sleep(WAIT_TIME)
            print('ECS task status for tasks {0}: {1}'.format(task_ids, statuses))
        
        # shutdown and delete everything
        self._shutdown()


    def kill_task(self, cluster_name, task_id, reason):
        """
        we identify 3 reasons to stop task:
        1- USER_CANCELED : user requested to kill the task
        2- SYS_CANCELED  : the system requested to kill the task
        3- COST_CANCELED : Task must be kill due to exceeding cost limit (set by user)
        """
        
        response = self._ecs_client.stop_task(cluster = cluster_name,
                                              task    = task_id,
                                              reason  = reason)
        
        return response



    def list_tasks(self, task_name):
        response = self._ecs_client.list_task_definitions(familyPrefix=task_name,
                                                          status='ACTIVE')
        return response
    

    def build_new_cluster(self, cluster_name):

        clusters = self._ecs_client.list_clusters()

        if cluster_name in clusters['clusterArns'][0]:
            print('cluster {0} already exist'.format(cluster_name))
            return cluster_name
        
        print("no existing cluster found, creating....")
        self._ecs_client.create_cluster(clusterName=cluster_name)

        return cluster_name


    def create_ec2_instance(self, cluster_name):
        """
        By default, your container instance launches into your default cluster.
        If you want to launch into your own cluster instead of the default,
        choose the Advanced Details list and paste the following script
        into the User data field, replacing your_cluster_name with the name of your cluster.
        !/bin/bash
        echo ECS_CLUSTER=your_cluster_name >> /etc/ecs/ecs.config

        ImageId: An AMI ID is required to launch an instance and must be
                 specified here or in a launch template.

        Instancetype: The instance type. Default m1.small For more information 
                      (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html).
        
        MinCount: The minimum number of instances to launch.
        MaxCount: The maximum number of instances to launch.

        UserData: accept any linux commnad (acts as a bootstraper for the instance)
        """
        #cmd = ''
        instance_id = None
        # if cluster_name is provided then we run the new one else use the default
        #if cluster_name:
        
        
        # check if we have an already running instance
        reservations = self._ec2_client.describe_instances(Filters=[{"Name": "instance-state-name",
                                                                     "Values": ["running"]},]).get("Reservations")
        
        # if we have a running instance(s) return the first one 
        if reservations:
            print("found running instances:")
            for reservation in reservations:
                for instance in reservation["Instances"]:
                    instance_id   = instance["InstanceId"]
                    instance_type = instance["InstanceType"]
                    print(instance_id)
            
            return instance_id
        
        print("no existing instance found")

        command  = "#!/bin/bash \n echo ECS_CLUSTER={0} >> /etc/ecs/ecs.config".format(cluster_name)
        response = self._ec2_client.run_instances(ImageId="ami-8f7687e2",
                                                  MinCount=1, MaxCount=1,
                                                  InstanceType="t1.micro",
                                                  IamInstanceProfile={"Arn" : 'arn:aws:iam::626113121967:instance-profile/ecsInstanceRole'},
                                                  UserData= command)

        instance    = response["Instances"][0]
        instance_id = response["Instances"][0]["InstanceId"]

        while instance['State'] != 'running':
            import time
            print ('...instance is %s' % instance['State'])
            time.sleep(10)
            #instance.load()

        print("instance {0} created".format(instance_id))

        return instance_id

    
    def _shutdown(self):
        #Shut everything down and delete task/service/instance/cluster
        try:
            print("Shutting down.....")
            # Set desired service count to 0 (obligatory to delete)
            response = self._ecs_client.update_service(cluster=self._cluster_name,
                                                       service=self._service_name,
                                                       desiredCount=0)
            # Delete service
            response = self._ecs_client.delete_service(cluster=self._cluster_name,
                                                       service=self._service_name)
            #pprint.pprint(response)
        except:
            print("Service not found/not active")
        
        # List all task definitions and revisions
        tasks = self.list_tasks(self._task_name)

        # De-Register all task definitions
        for task_definition in tasks["taskDefinitionArns"]:
            # De-register task definition(s)
            print("deregistering task {0}".format(task_definition))
            deregister_response = self._ecs_client.deregister_task_definition(
                taskDefinition=task_definition)
            #pprint.pprint(deregister_response)
        
        # Terminate virtual machine(s)
        instances = self._ecs_client.list_container_instances(cluster=self._cluster_name)
        if instances["containerInstanceArns"]:
            container_instance_resp = self._ecs_client.describe_container_instances(
            cluster=self._cluster_name,
            containerInstances=instances["containerInstanceArns"])

            for ec2_instance in container_instance_resp["containerInstances"]:
                print("terminating instance {0}".format(ec2_instance))
                ec2_termination_resp = self._ec2_client.terminate_instances(
                    DryRun=False,
                    InstanceIds=[ec2_instance["ec2InstanceId"],])
            
            

        # Finally delete the cluster
        print("deleting cluster {0}".format(self._cluster_name))
        response = self._ecs_client.delete_cluster(cluster=self._cluster_name)
        #pprint.pprint(response)

