import os
import sys
import json
import time
import uuid
import boto3
import pprint
import atexit
import base64
import typing as t

from datetime import datetime
from dateutil.tz import tzlocal
from collections import OrderedDict
from hydraa.services.cost_manager.aws_cost import AwsCost
from hydraa.services.maas_manager.aws_maas import AwsMaas

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

EC2               = 'EC2'
BUDGET            = 0
ACTIVE            = True
FARGATE           = 'FARGATE'
WAIT_TIME         = 2

CPTD = 10    # The max number of containers defs within a task def.
TDPC = 500   # The max number of task defs per cluster.
TPFC = 1000  # The max number of tasks per FARGATE cluster.
CIPC = 5000  # Number of container instances per cluster.
CSPA = 10000 # Number of clusters per account.


# --------------------------------------------------------------------------
#
class AwsCaas():
    """Represents a collection of clusters (resources) with a collection of
       services, tasks and instances.:
       :param cred: AWS credentials (access key, secert key, region)

       :param DryRun: Do a dryrun first to verify permissions.
    """

    def __init__(self, manager_id, cred, asynchronous, DryRun=False):

        self.manager_id = manager_id

        self.status = False
        
        # TODO: enable DryRun by the user to
        #       verify permissions before starting
        #       the actual run.
        self.DryRun = DryRun
        
        self._ecs_client    = self._create_ecs_client(cred)
        self._ec2_client    = self._create_ec2_client(cred)
        self._iam_client    = self._create_iam_client(cred)
        self._prc_client    = self._create_prc_client(cred)
        self._clw_client    = self._create_clw_client(cred)

        self._ec2_resource  = self._create_ec2_resource(cred)
        self._dydb_resource = self._create_dydb_resource(cred)


        self._cluster_name = None
        self._service_name = None

        self._task_id      = 0
        
        # FIXME: rename task_ids to tasks_book
        #        as we save more things than just
        #        task ids
        self._task_ids     = OrderedDict()
        self._family_ids   = OrderedDict()
        self._tasks_arns   = []

        self.launch_type  =  None
        self._region_name =  cred['region_name']

        self._run_cost     = 0

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
    @property
    def run_budget(self):
        return BUDGET


    # --------------------------------------------------------------------------
    #
    @property
    def run_cost(self):
        return self._run_cost


    # --------------------------------------------------------------------------
    #
    def _budget(self) -> AwsCost:      
        return AwsCost(self._prc_client, self._dydb_resource, self._region_name)
    

    # --------------------------------------------------------------------------
    #
    def _moniter(self) -> AwsMaas:
        return AwsMaas(self._clw_client)

        
    # --------------------------------------------------------------------------
    #
    def run(self, launch_type, batch_size=1, budget=0, cpu=0, memory=0,
                                          time=0, container_path=None):
        """
        Create a cluster, container, task defination with user requirements.
        and run them via **run_task

        :param: batch_size: int 
                         number of identical tasks that runs within the
                         same cluster on with the same resource requirements.
        
        :param: budget: float
                         expected charging amount by AWS to operate within.
        
        :param: cpu: int
                     number of cpus per ctask
        
        :param: memory: int
                        memory size in MB
        
        :param: timeL int
                       expected execution time on AWS in minutes

        :param: container_path: str
                         if provided then upload that container to
                         S3 storage for execution. 
        """
        #
        # TODO: In our scheduling mechanism we need to consider:
        #       memory, cpu and number of instances besides tasks
        #       per task_def and task_defs per cluster
        if budget:
            budget_calc = self._budget()
            if not time:
                raise Exception('estimated runtime is required')

            run_cost =  budget_calc.get_cost(launch_type, batch_size, cpu, memory, time)

            if run_cost > budget:
                msg = '({0} USD > {1} USD)'.format(run_cost, budget)
                user_in = input('run cost is higher than budget {0}, continue? yes/no: \n'.format(msg))
                if user_in == 'no':
                    return
                if user_in == 'yes':
                    pass
                else:
                    print('invalid input, abort')
                    return
            
            print('Estimated run_cost is: {0} USD'.format(round(run_cost, 4)))
            
            BUDGET           = budget
            self.cost        = run_cost

        self.status      = ACTIVE
        self.launch_type = launch_type

        cluster = self.create_cluster()
        self._wait_clusters(cluster)

        container_def = self.create_container_def()
        tptd = self._schedule(batch_size, launch_type)

        run_id = str(uuid.uuid4())

        if launch_type == FARGATE:
            for batch in tptd:
                family_id, task_def_arn = self.create_fargate_task_def(container_def, 256, 1024)
                tasks = self.run_ctask(launch_type, batch, task_def_arn, cluster)
                self._family_ids[family_id]['batch_size'] = batch

        if launch_type == EC2:
            self.create_ec2_instance(self._cluster_name)
            for batch in tptd:
                family_id, task_def_arn = self.create_ec2_task_def(container_def)
                tasks = self.run_ctask(launch_type, batch, task_def_arn, cluster)
                self._family_ids[family_id]['run_id']     = run_id
                self._family_ids[family_id]['task_list']  = tasks
                self._family_ids[family_id]['batch_size'] = batch

        if self.asynchronous:
            return run_id

        # FIXME: Enabling the service should be specified by the user
        #
        # self.create_ecs_service()

        # wait on task completion
        self._wait_tasks(self._tasks_arns, cluster)


    # --------------------------------------------------------------------------
    #
    def get_run_status(self, run_id):
        
        run_status = [] 
        for key, val in self._family_ids.items():
            if val.get('run_id') == run_id:
                statuses = self._get_task_statuses(val.get('task_list'),
                                                     self._cluster_name)
                run_status.append(statuses)
        
        run_stat =  [item for sublist in run_status for item in sublist]
        pending = list(filter(lambda pending: pending == 'PENDING', run_stat))
        running = list(filter(lambda pending: pending == 'RUNNING', run_stat))
        stopped = list(filter(lambda pending: pending == 'STOPPED', run_stat))

        msg = ('pending: {0}, running: {1}, stopped: {2}'.format(len(pending),
                                                                 len(running),
                                                                 len(stopped)))
        if running or pending:
            print('run: {0} is running'.format(run_id))
        
        if all([status == 'STOPPED' for status in run_stat]):
            print('run: {0} is finished'.format(run_id))
        
        print(msg)
           

    # --------------------------------------------------------------------------
    #
    def _create_ec2_resource(self, cred):
        """a wrapper around create dynamo db client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        dydb_client = boto3.resource('ec2', aws_access_key_id     = cred['aws_access_key_id'],
                                            aws_secret_access_key = cred['aws_secret_access_key'],
                                            region_name           = cred['region_name'])

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

        return iam_client


    # --------------------------------------------------------------------------
    #
    def _create_clw_client(self, cred):
        """a wrapper around create CloudWatch client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        clw_client = boto3.client('cloudwatch', aws_access_key_id     = cred['aws_access_key_id'],
                                                aws_secret_access_key = cred['aws_secret_access_key'],
                                                region_name           = cred['region_name'])

        return clw_client


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
    def create_container_def(self, image='ubuntu', cpu=1, memory=7):
        """ Build the internal structure of the container defination.
            
            :param: name   : container name
            :param: image  : image name to pull or upload
            :param: cpu    : number of cores to assign for this container
            :param: memory : amount of memory to assign for this container

            :return: container defination
        """
        con_def = {'name'        : 'noop',
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
    def create_fargate_task_def(self, container_def, cpu, memory):
        """Build the internal structure of the task defination.

           :param: container_def: a dictionary of a container specifications.

           :return: task defination name and task ARN (Amazon Resource Names)
        """
        task_def = {}
        if not cpu or not memory:
            raise Exception('Fargate task must have a memory or cpu units specified')

        family_id = 'hydraa_family_{0}'.format(str(uuid.uuid4()))

        task_def['family']                  = family_id
        task_def['volumes']                 = []
        task_def['cpu']                     = str(cpu)    # required
        task_def['memory']                  = str(memory) # required
        task_def['containerDefinitions']    = [container_def]
        task_def['executionRoleArn']        = 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'
        task_def['networkMode']             = 'awsvpc'
        task_def['requiresCompatibilities'] = ['FARGATE']
        
        reg_task = self._ecs_client.register_task_definition(**task_def)
        
        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        print('task {0} is registered'.format(family_id))

        # save the family id and its ARN
        self._family_ids[family_id] = OrderedDict()
        self._family_ids[family_id]['ARN'] =  task_def_arn

        return family_id, task_def_arn


    # --------------------------------------------------------------------------
    #
    def create_ec2_task_def(self, container_def, cpu = 0, memory = 0):
        """Build the internal structure of the task defination.

           :param: container_def: a dictionary of a container specifications.

           :param: cpu: the number of CPU units used by the task.

           :param: memory: the amount of memory (in MiB) used by the task.

           :return: task defination name and task ARN (Amazon Resource Names)
        """
        task_def = {}
        if cpu:
            task_def['cpu'] = cpu # optional
        if memory:
            task_def['memory'] = memory    # optional
        
        family_id = 'hydraa_family_{0}'.format(str(uuid.uuid4()))

        task_def['family']                  = family_id
        task_def['volumes']                 = []
        task_def['containerDefinitions']    = [container_def]
        task_def['executionRoleArn']        = 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'
        task_def['requiresCompatibilities'] = ['EC2']
        
        reg_task = self._ecs_client.register_task_definition(**task_def)
        
        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']
        
        print('task {0} is registered'.format(family_id))

        # save the family id and its ARN
        self._family_ids[family_id] = OrderedDict()
        self._family_ids[family_id]['ARN'] =  task_def_arn

        return family_id, task_def_arn

    
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
           run_ctask is a wrapper around RUN_TASK: which suitable for tasks
           that runs for x amount of time and stops like batch jobs.

           run_ctask supports only default task placement.

           :param: batch_size  : number of tasks to submit to the cluster.
           :param: task_def    : a dictionary of a task defination specifications.
           :param: cluster_name: cluster name to operate within.

           :return: submited ctasks ARNs

        """
        kwargs     = {}

        kwargs['count']                = batch_size
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
                                              'securityGroups'     : ["sg-0702f37d21c55da64"]}}
                
        # submit tasks of size "batch_size"
        response = self._ecs_client.run_task(**kwargs)

        if response['failures']:
            raise Exception(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'],
                                       failure['reason']) for failure in response['failures']]))

        for task in response['tasks']:
            task_arn = task['taskArn']
            self._tasks_arns.append(task_arn)

            self._task_ids[str(task_arn)] = 'ctask.{0}'.format(self._task_id)
            print(('submitting tasks {0}/{1}').format(self._task_id, len(self._task_ids) - 1),
                                                                                     end='\r')
            self._task_id +=1

        return self._tasks_arns


    # --------------------------------------------------------------------------
    #
    def _get_task_stamps(self, task_ids, cluster):
        """Pull the timestamps for every task by its ARN and convert
           them to a human readable.
        """
        
        task_stamps = OrderedDict()
        task_arns   = [arn for arn in task_ids.keys()]
        response    = self._ecs_client.describe_tasks(tasks=task_arns,
                                                      cluster=cluster)

        for task in response['tasks']:
            arn = task['taskArn']
            tid = task_ids[arn]
            task_stamps[tid] = OrderedDict()
            try:
                task_stamps[tid]['pullStartedAt'] = datetime.timestamp(task.get('pullStartedAt', 0.0))
                task_stamps[tid]['pullStoppedAt'] = datetime.timestamp(task.get('pullStoppedAt', 0.0))
                task_stamps[tid]['startedAt']     = datetime.timestamp(task.get('startedAt', 0.0))
                task_stamps[tid]['stoppedAt']     = datetime.timestamp(task.get('stoppedAt', 0.0))
                task_stamps[tid]['stoppingAt']    = datetime.timestamp(task.get('stoppingAt', 0.0))
            
            except TypeError:
                pass

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

        fname = '{0}_{1}_ctasks_{2}.csv'.format(self.launch_type, len(self._task_ids),
                                                                      self.manager_id)
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

        #FIXME: if we have a failure then update the task_ids 
        #       with status/error code and print it.
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
        if not self.asynchronous:
            raise Exception('Task wait is not supported in synchronous mode')

        UP = "\x1B[3A"
        CLR = "\x1B[0K"
        print("\n\n")
        tasks = [t for t in self._task_ids.values()]
        while True:
            statuses = self._get_task_statuses(task_ids, cluster)
            pending = list(filter(lambda pending: pending == 'PENDING', statuses))
            running = list(filter(lambda pending: pending == 'RUNNING', statuses))
            stopped = list(filter(lambda pending: pending == 'STOPPED', statuses))
            
            if all([status == 'STOPPED' for status in statuses]):
                print('Finished, {0} tasks stopped with status: "Done"'.format(len(tasks)))
                break

            print("{0}Pending: {1}{2}\nRunning: {3}{4}\nStopped: {5}{6}".format(UP,
                           len(pending), CLR, len(running), CLR, len(stopped), CLR))
            time.sleep(0.5)


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

        if len(response['clusters']) == 1:
            return (response['clusters'][0]['clusterArn'])
        else:
            return ''


    # --------------------------------------------------------------------------
    #
    def create_ec2_instance(self, cluster_name, InstanceType = None):
            """
            ImageId: An AMI ID is required to launch an instance and must be
                    specified here or in a launch template.

            Instancetype: The instance type. Default m1.small For more information 
                        (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html).
            
            MinCount: The minimum number of instances to launch.
            MaxCount: The maximum number of instances to launch.

            UserData: accept any linux commnad (acts as a bootstraper for the instance)
            """
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
            
            print("No existing EC2 instance found")
            
            command        = "#!/bin/bash \n echo ECS_CLUSTER={0} >> /etc/ecs/ecs.config".format(cluster_name)
            ecs_opt_ami    = 'ami-061c10a2cb32f3491'
            instance_type  = "t2.micro"

            kwargs = {}
            kwargs['ImageId']           = ecs_opt_ami
            kwargs['MinCount']          = 1
            kwargs['MaxCount']          = 1
            kwargs['InstanceType']      = instance_type
            kwargs['UserData']          = command
            kwargs['TagSpecifications'] = [{'ResourceType'   : 'instance',
                                            'Tags': [{'Key'  : 'Name',
                                                      'Value': 'my-ec2-instance'}
                                                       ]
                                                       }]

            instances = self._ec2_resource.create_instances(**kwargs)

            for instance in instances:
                print(f'EC2 instance "{instance.id}" of type "{instance_type}" is being launched')

                instance.wait_until_running()
                
                self._ec2_client.associate_iam_instance_profile(IamInstanceProfile={"Arn" : 'arn:aws:iam::626113121967:instance-profile/ecsInstanceRole',},
                                                                InstanceId=instance.id,)

                print(f'EC2 instance "{instance.id}" has been started')

            # wait for the instance to connect to the targeted cluster
            while True:
                res = self._ecs_client.describe_clusters(clusters = [self._cluster_name])
                if res['clusters'][0]['registeredContainerInstancesCount'] >= 1:
                    print('EC2 instance registered')
                    break
                print('waiting for instance {0} to register'.format(instance.id), end = '\r')
                time.sleep(WAIT_TIME)

            return instance_id


    # --------------------------------------------------------------------------
    #
    def _schedule(self, batch_size, launch_type):

        import math

        if not batch_size:
            raise Exception('Batch size can not be 0')

        if launch_type == FARGATE:
            # NOTE: submitting more than 50 conccurent tasks might fail
            #       due to user ECS/Fargate quota
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-quotas.html
            if batch_size > TPFC:
                raise Exception('batch limit per cluster ({0}>{1})'.format(batch_size, TPFC))

        tasks_per_task_def = []

        task_defs = math.ceil(batch_size / CPTD)

        if task_defs > TDPC:
            raise Exception('scheduled task defination per ({0}) cluster > ({1})'.format(task_defs, TDPC))

        # If we cannot split the
        # number into exactly 'task_defs of 10' parts
        if(batch_size < task_defs):
            print(-1)
    
        # If batch_size % task_defs == 0 then the minimum
        # difference is 0 and all
        # numbers are batch_size / task_defs
        elif (batch_size % task_defs == 0):
            for i in range(task_defs):
                tasks_per_task_def.append(batch_size // task_defs)
        else:
            # upto task_defs-(batch_size % task_defs) the values
            # will be batch_size / task_defs
            # after that the values
            # will be batch_size / task_defs + 1
            zp = task_defs - (batch_size % task_defs)
            pp = batch_size//task_defs
            for i in range(task_defs):
                if(i>= zp):
                    tasks_per_task_def.append(pp + 1)
                else:
                    tasks_per_task_def.append(pp)
        
        return tasks_per_task_def


    # --------------------------------------------------------------------------
    #
    def __cleanup(self):

        caller = sys._getframe().f_back.f_code.co_name
        if caller != '_shutdown':
            raise Exception('can not perform cleanup')

        self.manager_id = None

        self.status = False

        self._ecs_client    = None
        self._ec2_client    = None
        self._iam_client    = None
        self._prc_client    = None

        self._ec2_resource  = None
        self._dydb_resource = None

        self._cluster_name = None
        self._service_name = None

        self._task_ids.clear()
        self._family_ids.clear()
        self._tasks_arns.clear()

        self.launch_type  =  None
        self._region_name =  None

        print('done')
        

    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        """Shut everything down and delete task/service/instance/cluster"""

        if not self._cluster_name and self.status == False:
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
            print("no active service found")

        # degister all task definitions
        if self._family_ids:
            for task_fam_key, task_fam_val in self._family_ids.items():
                # deregister task definition(s)
                print("deregistering task {0}".format(task_fam_val['ARN']))
                deregister_response = self._ecs_client.deregister_task_definition(
                    taskDefinition=task_fam_val['ARN'])

        # terminate virtual machine(s)
        instances = self._ecs_client.list_container_instances(cluster=self._cluster_name)
        if instances["containerInstanceArns"]:
            container_instance_resp = self._ecs_client.describe_container_instances(
            cluster=self._cluster_name,
            containerInstances=instances["containerInstanceArns"])

            for ec2_instance in container_instance_resp["containerInstances"]:
                istance_id = ec2_instance['ec2InstanceId']
                print("terminating instance {0}".format(istance_id))
                ec2_termination_resp = self._ec2_client.terminate_instances(
                    DryRun=False,
                    InstanceIds=[istance_id])
                
                waiter = self._ec2_client.get_waiter('instance_terminated')
                waiter.wait(InstanceIds=[istance_id])

        # finally delete the cluster
        clusters = self.list_cluster()

        # check if we have running clusters
        if clusters:
            if not self._cluster_name in clusters[0]:
                print('cluster {0} does not exist'.format(self._cluster_name))
            elif 'hydraa' in clusters[0]:
                response = self._ecs_client.delete_cluster(cluster=self._cluster_name)
                print("hydraa cluster {0} found and deleted".format(self._cluster_name))
        else:
            print("no cluster(s) found/active")
        
        self.__cleanup()
        

