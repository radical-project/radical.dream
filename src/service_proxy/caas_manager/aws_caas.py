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

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

EC2               = 'EC2'
ACTIVE            = True
FARGATE           = 'FARGATE'
WAIT_TIME         = 2
TASKS_PER_CLUSTER = 1000

# --------------------------------------------------------------------------
#
class AwsCaas():
    """Represents a collection of clusters (resources) with a collection of
       services, tasks and instances.:
       :param cred: AWS credentials (access key, secert key, region)
    """

    def __init__(self, cred):

        self.manager_id = str(uuid.uuid4())

        self.status = False
        
        self._ecs_client    = self._create_ecs_client(cred)
        self._ec2_client    = self._create_ec2_client(cred)
        self._iam_client    = self._create_iam_client(cred)
        self._prc_client    = self._create_prc_client(cred)

        self._ec2_resource  = self._create_ec2_resource(cred)
        self._dydb_resource = self._create_dydb_resource(cred)

        self._cluster_name = None
        self._service_name = None
        self._task_name    = None
        self._task_ids     = OrderedDict()

        self._region_name  =  cred['region_name']


    # --------------------------------------------------------------------------
    #
    @property
    def is_active(self):
        return self.status


    # --------------------------------------------------------------------------
    #
    def __cost(self):
        # FIXME: If budget mode is enabled by the user then we
        #        can enable cost class otherwise we do
        #        not need to do that.
        self.cost = AwsCost(self._prc_client, self._dydb_resource,
                            self._cluster_name, self._service_name,
                                                 self._region_name)
        
        return self.cost

        
    # --------------------------------------------------------------------------
    #
    def run(self, launch_type, batch_size=1, container_path=None):
        """
        Create a cluster, container, task defination with user requirements.
        and run them via **run_task

        :param: batch_size: number of identical tasks that runs within the
                same cluster on with the same resource requirements.
        :param: container_path: if provided then upload that container to
                S3 storage for execution. 
        """
        # TODO: User should provide the task , mem and cpu once they do that.
        # TODO: CaasManager should operates within a budget.
        #
        # TODO: Ask the user if they want to continue to the 
        #       execution based on the cost.

        # TODO: if the user is asking for > 2000 tasks we should do:
        #       1- Create new cluster and submit to it
        #       2- Or break and ask the user to use AWSbatch
        #
        #       tasks_per_cluster = ceil(batch_size / TASKS_PER_CLUSTER)

        if batch_size > TASKS_PER_CLUSTER:
            raise Exception ('batch limit per cluster ({0}>{1})'.format(batch_size, TASKS_PER_CLUSTER))
        
        self.status = ACTIVE
        
        submit_start = time.time()
        
        cluster = self.create_cluster()

        self._wait_clusters(cluster)

        container_def = self.create_container_def()

        if launch_type == EC2:
            self.create_ec2_instance(self._cluster_name)
            task_name, task_def_arn = self.create_ec2_task_def(container_def)
        if launch_type == FARGATE:
            task_name, task_def_arn = self.create_fargate_task_def(container_def)

        # FIXME: Enabling the service should be specified by the user
        #
        # self.create_ecs_service()


        # FIXME: this requires to link the EC2 instance with ECS
        #        so far it is failing, alternatively we are using
        #        Fargate
        #
        # self.create_ec2_instance(cluster)

        while True:
            res = self._ecs_client.describe_clusters(clusters = [self._cluster_name])
            if res['clusters'][0]['registeredContainerInstancesCount'] >= 1:
                print('EC2 instance registered')
                break
            print('waiting for an EC2 instance to regiser')
            time.sleep(WAIT_TIME)

        submit_stop = time.time()

        print('Submit time: {0}'.format(submit_stop - submit_start))

        # NOTE: submitting more than 50 conccurent tasks might fail
        #       due to user ECS/Fargate quota
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-quotas.html

        tasks = self.run_ctask(launch_type, batch_size, task_def_arn, cluster)

        # wait on task completion
        self._wait_tasks(tasks, cluster)

        done_stop = time.time()

        print('Done time: {0}'.format(done_stop - submit_start))


    # --------------------------------------------------------------------------
    #
    def _create_ec2_resource(self, cred):
        """a wrapper around create dynamo db client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        dydb_client = boto3.resource('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                            aws_secret_access_key = cred['aws_secret_access_key'],
                                            region_name           = cred['region_name'])
        print('EC2 resource created')

        return dydb_client


    # --------------------------------------------------------------------------
    #
    def _create_dydb_resource(self, cred):
        """a wrapper around create dynamo db client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        dydb_client = boto3.resource('dynamodb', aws_access_key_id     = cred['aws_access_key_id'],
                                                 aws_secret_access_key = cred['aws_secret_access_key'],
                                                 region_name           = cred['region_name'])
        print('DynamoDB resource created')

        return dydb_client


    # --------------------------------------------------------------------------
    #
    def _create_prc_client(self, cred):
        """a wrapper around create price client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        prc_client = boto3.client('pricing', aws_access_key_id     = cred['aws_access_key_id'],
                                             aws_secret_access_key = cred['aws_secret_access_key'],
                                             region_name           = cred['region_name'])
        
        print('Pricing client created')

        return prc_client


    # --------------------------------------------------------------------------
    #
    def _create_ec2_client(self, cred):
        """a wrapper around create EC2 client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        ec2_client = boto3.client('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                           aws_secret_access_key = cred['aws_secret_access_key'],
                                           region_name           = cred['region_name'])
        
        print('EC2 client created')

        return ec2_client


    # --------------------------------------------------------------------------
    #
    def _create_ecs_client(self, cred):
        """a wrapper around create ECS client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        ecs_client = boto3.client('ecs', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        print('ECS client created')
        return ecs_client


    # --------------------------------------------------------------------------
    #
    def _create_iam_client(self, cred):
        """a wrapper around create IAM client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        iam_client = boto3.client('iam', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        
        print('IAM client created')
        return iam_client


    # --------------------------------------------------------------------------
    #
    def create_cluster(self):
        """Create a HYDRAA cluster or check for existing one
           
           :param : cluster_name: string name to create with or look for
           :return: the name of the created or found cluster
        """
        clusters = self.list_cluster()
        # FIXME: check for existing clusters, if multiple clusters with "hydraa"
        #        found, then ask the user which one to use.
        if clusters:
            print('checking for existing hydraa cluster')
            
            if 'hydraa' in clusters[0]:
                print('hydraa cluster found: {0}'.format(clusters[0]))
                self._cluster_name = clusters[0]
                return clusters[0]
            else:
                print('not hydraa cluster found: {0}')
        
        else:
            print('no cluster found in this account')

        cluster_name = "hydraa_cluster_{0}".format(self.manager_id)

        print('creating new cluster {0}'.format(cluster_name))

        self._ecs_client.create_cluster(clusterName=cluster_name)
        self._cluster_name = cluster_name

        return cluster_name

    
    # --------------------------------------------------------------------------
    #
    def _wait_clusters(self, cluster_name):

        clsuter = self.get_ecs_cluster_arn(cluster_name=cluster_name)
        while True:
            statuses = self._get_cluster_statuses(cluster_name)
            if all([status == 'ACTIVE' for status in statuses]):
                print('ECS cluster {0} is ACTIVE'.format(cluster_name))
                break
            time.sleep(WAIT_TIME)
            print('ECS status for cluster {0}: {1}'.format(cluster_name, statuses))


    # --------------------------------------------------------------------------
    #
    def _get_cluster_statuses(self, cluster_name):
        """
        """
        response = self._ecs_client.describe_clusters(clusters = [cluster_name])

        # Error checking
        if response['failures'] != []:
            raise Exception('There were some failures:\n{0}'.format(
                response['failures']))
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            msg = 'Task status request received status code {0}:\n{1}'
            raise Exception(msg.format(status_code, response))

        return [t['status'] for t in response['clusters']]


    # --------------------------------------------------------------------------
    #
    def create_container_def(self, name ='hello_world_container', image='ubuntu',
                                                                  cpu=1, memory=200):
        """ Build the internal structure of the container defination.
            
            :param: name   : container name
            :param: image  : image name to pull or upload
            :param: cpu    : number of cores to assign for this container
            :param: memory : amount of memory to assign for this container

            :return: container defination
        """
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
    def create_fargate_task_def(self, container_def):
        """Build the internal structure of the task defination.

           :param: container_def: a dictionary of a container specifications.

           :return: task defination name and task ARN (Amazon Resource Names)
        """
        # FIXME: This a registering with minimal definition,
        #        user should specifiy how much per task (cpu/mem)
        task_def = {}

        task_def['family']                  = 'hello_world'
        task_def['volumes']                 = []
        task_def['cpu']                     = '256'
        task_def['memory']                  = '2048' # this should be greate or equal to container def memory
        task_def['containerDefinitions']    = [container_def]
        task_def['executionRoleArn']        = 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'
        task_def['networkMode']             = 'awsvpc'
        task_def['requiresCompatibilities'] = ['FARGATE']
        
        reg_task = self._ecs_client.register_task_definition(**task_def)
        
        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        # FIXME: this should be a uniqe name that this class assigns
        #        with the unique uuid
        self._task_name = "hello_world"
        
        print('task {0} is registered'.format(self._task_name))

        return self._task_name, task_def_arn


    # --------------------------------------------------------------------------
    #
    def create_ec2_task_def(self, container_def):
        """Build the internal structure of the task defination.

           :param: container_def: a dictionary of a container specifications.

           :return: task defination name and task ARN (Amazon Resource Names)
        """
        # FIXME: This a registering with minimal definition,
        #        user should specifiy how much per task (cpu/mem)
        task_def = {}

        task_def['family']                  = 'hello_world'
        task_def['volumes']                 = []
        task_def['cpu']                     = '128'
        task_def['memory']                  = '900'
        task_def['containerDefinitions']    = [container_def]
        task_def['executionRoleArn']        = 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'
        task_def['requiresCompatibilities'] = ['EC2']
        
        reg_task = self._ecs_client.register_task_definition(**task_def)
        
        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        # FIXME: this should be a uniqe name that this class assigns
        #        with the unique uuid
        self._task_name = "hello_world"
        
        print('task {0} is registered'.format(self._task_name))

        return self._task_name, task_def_arn

    
    # --------------------------------------------------------------------------
    #
    def create_ecs_service(self):
        """Create service with exactly 1 desired instance of the task
           Info: Amazon ECS allows you to run and maintain a specified
           number (the "desired count") of instances of a task definition
           simultaneously in an ECS cluster.

           :param: None

           :return: response of created ECS service
        """
        # FIXME: check for exisitng hydraa services specifically.

        self._service_name = "hydraa_service_{0}".format(self.manager_id)
        
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
                                                   # clientToken='request_identifier_string',
                                                   deploymentConfiguration={'maximumPercent': 200,
                                                                            'minimumHealthyPercent': 50})
        

        
        print('service {0} created'.format(self._service_name))
        
        return response


    # --------------------------------------------------------------------------
    #
    def start_ctask(self, task_def, cluster_name):
        """Starts a new container task (ctask) from the specified task definition
           on the specified container instance or instances. StartTask uses/assumes
           that you have your own scheduler to place tasks manually on specific
           container instances.

           :param: task_def    : a dictionary of a task defination specifications.
           :param: cluster_name: cluster name to operate within

           :return: None
        """

        containers   = self._ecs_client.list_container_instances(cluster=cluster_name)
        container_id = containers["containerInstanceArns"][0].split("/")[-1]

        response = self._ecs_client.start_task(taskDefinition=task_def,
                                               overrides={},
                                               containerInstances=[container_id],
                                               startedBy="foo",)


    # --------------------------------------------------------------------------
    #
    def run_ctask(self, launch_type, batch_size, task_def, cluster_name):
        """Starts a new ctask using the specified task definition. In this
           mode AWS scheduler will handle the task placement.

           :param: batch_size  : number of tasks to submit to the cluster.
           :param: task_def    : a dictionary of a task defination specifications.
           :param: cluster_name: cluster name to operate within.

           :return: submited ctasks ARNs

        """
        task_id    = 0 
        kwargs     = {}
        tasks_arns = []

        kwargs['count']                = 1
        kwargs['cluster']              = cluster_name
        kwargs['launchType']           = launch_type
        kwargs['overrides']            = {}
        kwargs['taskDefinition']       = task_def

        # EC2 does not support Network config or platform version
        if launch_type == FARGATE:
            kwargs['platformVersion']      = 'LATEST'
            kwargs['networkConfiguration'] = {'awsvpcConfiguration': {'subnets': [
                                                                      'subnet-094da8d73899da51c',],
                                              'assignPublicIp'     : 'ENABLED',
                                              'securityGroups'     : ["sgr-035b80163ea840a9f"]}}
                
        for task in range(batch_size):
            response = self._ecs_client.run_task(**kwargs)
            print(response)
            task_arn = response['tasks'][0]['taskArn']
            tasks_arns.append(task_arn)

            if response['failures']:
                raise Exception(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'], failure['reason'])
                                        for failure in response['failures']]))
            else:
                self._task_ids[str(task_arn)] = 'ctask.{0}'.format(task_id)
                print('submitting task: {0}/{1}'.format(task_id, batch_size), end = "\r")
                task_id +=1

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
    def profiles(self):

        fname = 'ctasks_df_{0}.csv'.format(self.manager_id)

        if os.path.isfile(fname):
            print('profiles already exist {0}'.format(fname))
            return fname

        task_stamps = self._get_task_stamps(self._task_ids, self._cluster_name)

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

        df = pd.DataFrame(task_stamps.values(), index =[t for t in task_stamps.keys()])

        fname = 'ctasks_df_{0}.csv'.format(self.manager_id)
        df.to_csv(fname)
        print('Dataframe saved in {0}'.format(fname))

        return fname


    # --------------------------------------------------------------------------
    #
    @property
    def ttx(self):
        fcsv = self.profiles()
        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')
        
        df = pd.read_csv(fcsv)
        st = df['pullStartedAt'].min()
        en = df['stoppedAt'].max()
        ttx = en - st
        return '{0} seconds'.format(ttx)


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
    def get_ecs_cluster_arn(self, cluster_name):
        """ Given the ECS cluster name, get the ECS ClusterARN.
        """
        response = self._ecs_client.describe_clusters(clusters=[cluster_name])

        print("ECS cluster details: {0}".format(response))
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

            # FIXME: check for exisitng EC2 hydraa instances specifically.
            
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
            ECS_Optimized_AMI    = 'ami-061c10a2cb32f3491'
            command   = "#!/bin/bash \n echo ECS_CLUSTER={0} >> /etc/ecs/ecs.config".format(cluster_name)
            instances = self._ec2_resource.create_instances(ImageId=ECS_Optimized_AMI,
                                                            MinCount=1, 
                                                            MaxCount=1,
                                                            InstanceType="t2.micro",
                                                            #IamInstanceProfile={},
                                                            TagSpecifications=[
                                                                            {
                                                                                'ResourceType': 'instance',
                                                                                'Tags': [
                                                                                    {
                                                                                        'Key': 'Name',
                                                                                        'Value': 'my-ec2-instance'
                                                                                    },
                                                                                ]
                                                                            },
                                                                        ],
                                                            UserData= command)

            for instance in instances:
                print(f'EC2 instance "{instance.id}" has been launched')
                
                instance.wait_until_running()
                
                self._ec2_client.associate_iam_instance_profile(IamInstanceProfile={"Arn" : 'arn:aws:iam::626113121967:instance-profile/ecsInstanceRole',},
                                                                InstanceId=instance.id,)

                print(f'EC2 Instance Profile has been attached')

                print(f'EC2 instance "{instance.id}" has been started')

            return instance_id


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        """Shut everything down and delete task/service/instance/cluster"""

        if not self._cluster_name and self.status == False:
            print('can not call shutdown on a non-active manager')
            return

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
        
        self.status = False
        

