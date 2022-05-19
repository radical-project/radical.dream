import os
import json
import time
import uuid
import boto3
import pprint
import base64
import datetime
from dateutil.tz import tzlocal
from collections import OrderedDict
from src.service_proxy.cost_manager.aws_cost import AwsCost
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
__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

WAIT_TIME = 2

# --------------------------------------------------------------------------
#
class AwsCaas():

    # --------------------------------------------------------------------------
    #
    def __init__(self, cred):
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
        __aws_cost   = None
        __manager_id = str(uuid.uuid4())

        self._ecs_client    = self._create_ecs_client(cred)
        self._ec2_client    = self._create_ec2_client(cred)
        self._iam_client    = self._create_iam_client(cred)
        self._prc_client    = self._create_prc_client(cred)
        self._dydb_resource = self._create_dydb_resource(cred)

        self._cluster_name = "hydraa_cluster_{0}".format(__manager_id)
        self._service_name = "hydraa_service_{0}".format(__manager_id)
        self._task_name    = None
        self._task_ids     = OrderedDict()

        self._region_name  =  cred['region_name']

        # FIXME: If budget mode is enabled by the user then we
        #        can init the cost class otherwise we do not 
        #        need to do that.
        self.cost = AwsCost(self._prc_client, self._dydb_resource,
                            self._cluster_name, self._service_name, 
                                                 self._region_name)


    # --------------------------------------------------------------------------
    #
    def run(self, batch_size=1, container_path=None):
        """
        Build Docker image, push to AWS and update ECS service.
        """
        # The user will provide the task , mem and cpu once they do that
        # a fucntion [here] should calculate the cost
        #
        # 1-check the avilable budget
        #    avilabel_budget = self.get_user_budget()
        # 2- calculate the cost of executing this container
        #    cost = self._calculate_container_cost(time, cpu, mem)
        # 3- ask the user if he/she wants to continue or not:
        #    value = input("Executing container will cost {0} out of your budget {1} press enter to continue:\n")
        #    print(f'You entered {value}')
        #
        # FIXME: ask the user if they want to continue to the 
        #        execution based on the cost

        submit_start = time.time()
        # 2-Create a cluster (this should be done once)
        cluster = self.create_cluster(self._cluster_name)

        # 3-Create Container definition
        container_def = self.create_container_def()

        # 4-Create a task definition
        task_name, task_def_arn = self.create_task_def(container_def)
        
        # 5-create a service
        # this should not be done unless we need to resbumit the task forever
        #_ = self.create_ecs_service()

        # 6-create ec2 instance or
        # this requires to link the EC2 instance with ECS
        # so far it is failing, alternatively we are using
        # FARGATE
        #self.create_ec2_instance(cluster)
        submit_stop = time.time()

        print('Submit time: {0}'.format(submit_stop - submit_start))

        # 7-Start set of tasks
        tasks = self.run_task(batch_size, task_def_arn, cluster)

        # 8-Wait on task completion
        self._wait_tasks(tasks, cluster)

        done_stop = time.time()

        print('Done time: {0}'.format(done_stop - submit_start))


    # --------------------------------------------------------------------------
    #
    def _create_dydb_resource(self, cred):
        dydb_client = boto3.resource('dynamodb', aws_access_key_id     = cred['aws_access_key_id'],
                                                 aws_secret_access_key = cred['aws_secret_access_key'],
                                                 region_name           = cred['region_name'])
        print('dynamodb resource created')

        return dydb_client


    # --------------------------------------------------------------------------
    #
    def _create_prc_client(self, cred):
        prc_client = boto3.client('pricing', aws_access_key_id     = cred['aws_access_key_id'],
                                             aws_secret_access_key = cred['aws_secret_access_key'],
                                             region_name           = cred['region_name'])
        
        print('pricing client created')

        return prc_client


    # --------------------------------------------------------------------------
    #
    def _create_ec2_client(self, cred):
        ec2_client = boto3.client('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                           aws_secret_access_key = cred['aws_secret_access_key'],
                                           region_name           = cred['region_name'])
        
        print('ec2 client created')

        return ec2_client


    # --------------------------------------------------------------------------
    #
    def _create_ecs_client(self, cred):
        ecs_client = boto3.client('ecs', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        print('ecs client created')
        return ecs_client


    # --------------------------------------------------------------------------
    #
    def _create_iam_client(self, cred):
        iam_client = boto3.client('iam', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        
        print('iam client created')
        return iam_client


    # --------------------------------------------------------------------------
    #
    def create_container_def(self, name ='hello_world_container', image='ubuntu',
                                                                  cpu=1, memory=1024):
        con_def = {'name'        : name,
                   'cpu'         : cpu,
                   'memory'      : memory,
                   'portMappings': [],
                   'essential'   : True,
                   'environment' : [],
                   'mountPoints' : [],
                   'volumesFrom' : [],
                   "logConfiguration": {"logDriver": "awslogs",
                                        "options": { 
                                                    "awslogs-group" : "/ecs/first-run-task-definition",
                                                    "awslogs-region": "us-east-1",
                                                    "awslogs-stream-prefix": "ecs"}},
                   'image'       : 'screwdrivercd/noop-container',
                   "entryPoint"  : [],
                   'command'     : ["/bin/echo", "noop"]}

        return con_def


    # --------------------------------------------------------------------------
    #
    def create_task_def(self, container_def):
        """
        create a container defination
        """
        # FIXME: This a registering with minimal definition,
        #        user should specifiy how much per task (cpu/mem)
        task_def = {'family' :  'hello_world',
                    'volumes': [],
                    'cpu'    : '256',
                    'memory' : '1024', # this should be greate or equal to container def memory
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

    
    # --------------------------------------------------------------------------
    #
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
        response = self._ecs_client.create_service(cluster        = self._cluster_name,
                                                   serviceName    = self._service_name,
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


    # --------------------------------------------------------------------------
    #
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


    # --------------------------------------------------------------------------
    #
    def run_task(self, batch_size, task_def, cluster_name):
        """
        Starts a new task using the specified task definition. In this
        mode AWS scheduler will handle the task placement.

        cluster: The short name or full Amazon Resource Name
        (ARN) of the cluster to run your task on. If you do
        not specify a cluster, the default cluster is assumed.
        """
        task_id    = 0 
        kwargs     = {}
        tasks_arns = []

        kwargs['count']                = 1
        kwargs['cluster']              = cluster_name
        kwargs['launchType']           = 'FARGATE'
        kwargs['overrides']            = {}
        kwargs['taskDefinition']       = task_def
        kwargs['platformVersion']      = 'LATEST'
        kwargs['networkConfiguration'] = {'awsvpcConfiguration': {'subnets': [
                                                                  'subnet-094da8d73899da51c',],
                                           'assignPublicIp'    : 'ENABLED',
                                           'securityGroups'    : ["sg-0702f37d21c55da64"]}}
        for task in range(batch_size):
            response = self._ecs_client.run_task(**kwargs)
            task_arn = response['tasks'][0]['taskArn']
            tasks_arns.append(task_arn)

            if response['failures']:
                raise Exception(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'], failure['reason'])
                                        for failure in response['failures']]))
            else:
                print('submitting task {0}'.format(task_id))
                task_id +=1
                self._task_ids[str(task_arn)] = str(task_id)

        return tasks_arns


    # --------------------------------------------------------------------------
    #
    def _get_task_stamps(self, task_ids, cluster):
        
        task_stamps = OrderedDict()
        task_arns   = [arn for arn in task_ids.keys()]
        response    = self._ecs_client.describe_tasks(tasks=task_arns,
                                                      cluster=cluster)

        for task in response['tasks']:
            arn = task['taskArn']
            tid = task_ids[arn]

            task_stamps[tid] = OrderedDict()
            task_stamps[tid]['pullStartedAt'] = int(task['pullStartedAt'].strftime("%s"))
            task_stamps[tid]['pullStoppedAt'] = int(task['pullStoppedAt'].strftime("%s"))
            task_stamps[tid]['startedAt']     = int(task['startedAt'].strftime("%s"))
            task_stamps[tid]['stoppedAt']     = int(task['stoppedAt'].strftime("%s"))
            task_stamps[tid]['stoppingAt']    = int(task['stoppingAt'].strftime("%s"))
        

        return task_stamps


    # --------------------------------------------------------------------------
    #
    def task_run_time(self):

        task_stamps = self._get_task_stamps(self._task_ids, self._cluster_name)
        for key, val in task_stamps.items():
            print (key, val['stoppedAt'] - val['pullStartedAt'])

    # --------------------------------------------------------------------------
    #
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


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self, task_ids, cluster):
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Wait for task status until STOPPED
        """
        tasks = [t for t in self._task_ids.values()]
        while True:
            statuses = self._get_task_statuses(task_ids, cluster)
            if all([status == 'STOPPED' for status in statuses]):
                print('ECS tasks {0} STOPPED'.format(','.join(tasks)))
                break
            time.sleep(WAIT_TIME)
            print('ECS task status for tasks {0}: {1}'.format(tasks, statuses))


    # --------------------------------------------------------------------------
    #
    def stop_ctask(self, cluster_name, task_id, reason):
        """
        we identify 3 reasons to stop task:
        1- USER_CANCELED : user requested to kill the task
        2- SYS_CANCELED  : the system requested to kill the task
        3- COST_CANCELED : Task must be kill due to exceeding cost
                           limit (set by user)
        """
        task_arn = self.task_ids[task_id]

        response = self._ecs_client.stop_task(cluster = cluster_name,
                                              task    = task_arn,
                                              reason  = reason)

        return response


    # --------------------------------------------------------------------------
    #
    def list_tasks(self, task_name):
        response = self._ecs_client.list_task_definitions(familyPrefix=task_name,
                                                          status='ACTIVE')
        return response


    # --------------------------------------------------------------------------
    #
    def list_cluster(self):
        clusters = self._ecs_client.list_clusters()
        return clusters['clusterArns']


    # --------------------------------------------------------------------------
    #
    def create_cluster(self, cluster_name):

        clusters = self.list_cluster()
        # FIXME: check for existing clusters, if multiple clusters with "hydraa"
        #        froud, then ask the user which one to use.
        if clusters:
            print('checking for existing hydraa cluster')
            if 'hydraa' in clusters[0]:
                print('found: {0}'.format(cluster_name))
                # FIXME: cluster name should be generated in this func
                #        and not in the class __init__
                self._cluster_name = clusters[0]
                return clusters[0]
            else:
                print('not found: {0}')
    
        print('creating new cluster {0}'.format(cluster_name))
        self._ecs_client.create_cluster(clusterName=cluster_name)
        return cluster_name



    # --------------------------------------------------------------------------
    #
    def get_ecs_cluster_arn(self, cluster_name):
        """ Given the ECS cluster name, get the ECS ClusterARN.
        """
        response = self._ecs_client.describe_clusters(clusters=[cluster_name])

        print("ECS Cluster Details: %s", response)
        if len(response['clusters']) == 1:
            return (response['clusters'][0]['clusterArn'])
        else:
            return ''


    # --------------------------------------------------------------------------
    #
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
        cmd = ''
        instance_id = None
        
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

        print("instance {0} created".format(instance_id))

        return instance_id


    # --------------------------------------------------------------------------
    #
    def _wait_instances(self, cluster, instance_id):

        # check for any container instance within the given cluster
        instances = self._ecs_client.list_container_instances(cluster = cluster,
                                                              status  = 'ACTIVE' or 
                                                                        'DRAINING' or
                                                                        'REGISTERING'or
                                                                        'DEREGISTERING' or
                                                                        'REGISTRATION_FAILED')
        while True:
            if not instances['containerInstanceArns']:
                print('all container instances are drained/stopped')
                break
            time.sleep(WAIT_TIME)
            print('EC2 instances status for instance {0}: draining'.format(instance_id))


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        # shut everything down and delete task/service/instance/cluster
        try:
            print("Shutting down.....")
            # set desired service count to 0 (obligatory to delete)
            response = self._ecs_client.update_service(cluster=self._cluster_name,
                                                       service=self._service_name,
                                                       desiredCount=0)
            # delete service
            response = self._ecs_client.delete_service(cluster=self._cluster_name,
                                                       service=self._service_name)
        except:
            print("service not found/not active")
        
        tasks = None
        try:
            # list all task definitions and revisions
            tasks = self.list_tasks(self._task_name)
        except:
            print("tasks not found/not active")

        # de-Register all task definitions
        if tasks:
            for task_definition in tasks["taskDefinitionArns"]:
                # De-register task definition(s)
                print("deregistering task {0}".format(task_definition))
                deregister_response = self._ecs_client.deregister_task_definition(
                    taskDefinition=task_definition)

        # terminate virtual machine(s)
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

                # wait for every container instance to be inactive
                self._wait_instances(self._cluster_name, ec2_instance["ec2InstanceId"])

        # finally delete the cluster
        clusters = self.list_cluster()

        # check if we have running clusters
        if clusters:
            if not self._cluster_name in clusters[0]:
                print('cluster {0} does not exist'.format(self._cluster_name))
            elif 'hydraa' in clusters[0]:
                print('hydraa cluster {0} found'.format(self._cluster_name))
                response = self._ecs_client.delete_cluster(cluster=self._cluster_name)
                print("{0} deleted".format(self._cluster_name))
        else:
            print("no cluster(s) found/active")
        

