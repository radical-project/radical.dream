import os
import sys
import copy
import math
import time
import uuid
import boto3
import queue
import atexit
import botocore
import threading
import itertools as iter

from queue import Empty
from datetime import datetime
from collections import OrderedDict

from hydraa.services.caas_manager.kubernetes import kubernetes

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


AWS = 'aws'
EKS = ['EKS', 'eks'] # Elastic kubernetes Service
EC2 = ['EC2', 'ec2'] # Elastic Cloud 
ECS = ['ECS', 'ecs'] # Elastic Container Service
FARGATE = ['FARGATE', 'fargate'] # Fargate container service

ECS_TASKS_OTHER_STATUSES = ['PROVISIONING', 'PENDING', 'ACTIVATING',
                            'DEACTIVATING', 'STOPPING', 'DEPROVISIONING',
                            'DELETED']

CPTD = 10    # The max number of containers defs within a task def.
TDPC = 500   # The max number of task defs per cluster.
TPFC = 1000  # The max number of tasks per FARGATE cluster.
CIPC = 5000  # Number of container instances per cluster.
CSPA = 10000 # Number of clusters per account.

WAIT_TIME = 5 # seconds


# --------------------------------------------------------------------------
#
class AwsCaas:
    """Represents a collection of clusters (resources) with a collection of
       services, tasks and instances.:
       :param cred: AWS credentials (access key, secert key, region)

       :pram asynchronous: wait for the tasks to finish or run in the
                           background.
       :param DryRun: Do a dryrun first to verify permissions.
    """

    def __init__(self, sandbox, manager_id, cred, VMS, asynchronous, auto_terminate, log, prof):

        self.vms = VMS
        self._task_id = 0
        self.logger = log
        self.status = False
        self.cluster = None
        self.cluster_name = None
        self.service_name = None
        self.manager_id = manager_id
        self._tasks_book = OrderedDict()
        self._family_ids = OrderedDict()

        self.launch_type = VMS[0].LaunchType.lower()

        for vm in self.vms:
            vm.Region = cred['region_name']

        self.runs_tree = OrderedDict()
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate

        self.run_id = '{0}.{1}'.format(self.launch_type, str(uuid.uuid4()))
        self.sandbox = '{0}/{1}.{2}'.format(sandbox, AWS, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.profiler = prof(name=__name__, path=self.sandbox)

        self.incoming_q = queue.Queue() # caas manager -> main manager
        self.outgoing_q = queue.Queue() # main manager -> caas manager
        self.internal_q = queue.Queue() # caas manager -> caas manager

        self._task_lock = threading.Lock()
        self._terminate = threading.Event()

        # FIXME: try to orgnize these clients into a uniform dict
        #        self.clients['EKS'], self.clients['EC2'], etc.
        self._ecs_client = self._create_ecs_client(cred)
        self._ec2_client = self._create_ec2_client(cred)
        self._iam_client = self._create_iam_client(cred)
        self._prc_client = self._create_prc_client(cred)
        self._clw_client = self._create_clw_client(cred)
        self._clf_client = self._create_clf_client(cred)
        self._eks_client = self._create_eks_client(cred)
        self._clf_resource = self._create_clf_resource(cred)
        self._ec2_resource = self._create_ec2_resource(cred)
        self._dydb_resource = self._create_dydb_resource(cred)

        self.start_thread = threading.Thread(target=self.start,
                                             name='AwsCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        # now set the manager as active
        self.status = True

        atexit.register(self.shutdown)


    # --------------------------------------------------------------------------
    #
    @property
    def is_active(self):
        return self.status

        
    # --------------------------------------------------------------------------
    #
    def start(self, service=False):
        """
        Create a cluster, container, task defination with user requirements.
        and run them via **run_task

        :param: tasks:   List of Task class object 
        """
        if self.status:
            print('Manager already started')
            return self.run_id

        print("starting run {0}".format(self.run_id))

        # check if this launch will be a service
        if service:
            if self.launch_type in EKS:
                raise Exception('EKS does not support services yet')
            self.create_ecs_service()

        if self.launch_type in FARGATE:
            self.ecs = self.create_cluster()
            self._wait_clusters(self.ecs)

        if self.launch_type in EC2:
            self.ecs = self.create_cluster()
            self._wait_clusters(self.ecs)
            for vm in self.vms:
                self.create_ec2_instance(vm)
        
        # check if this is an EKS service
        if self.launch_type in EKS:
            self.cluster = kubernetes.EKSCluster(self.run_id, self.sandbox,
                                                 self.vms, self._ec2_client,
                                                 self.logger)
            self.cluster.bootstrap()

        self.runs_tree[self.run_id] = self._family_ids

        # call get work to pull tasks
        self._get_work()


    # --------------------------------------------------------------------------
    #
    def get_tasks(self):
        return list(self._tasks_book.values())


    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = os.environ.get('MAX_BULK_SIZE', 1024) # tasks
        max_bulk_time = os.environ.get('MAX_BULK_TIME', 2)    # seconds
        min_bulk_time = os.environ.get('MAX_BULK_TIME', 0.1)  # seconds

        self.wait_thread = threading.Thread(target=self._wait_tasks,
                                            name='AwSCaaSWatcher')	
        self.wait_thread.daemon = True
        if not self.asynchronous and not self.wait_thread.is_alive():
            self.wait_thread.start()

        while not self._terminate.is_set():
            now = time.time()  # time of last submission
            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < max_bulk_time:
                try:
                    task = self.incoming_q.get(block=True, timeout=min_bulk_time)
                except queue.Empty:
                        task = None

                if task:
                        bulk.append(task)

                if len(bulk) >= max_bulk_size:
                        break

            if bulk:
                with self._task_lock:
                    if self.launch_type in EKS:
                        self.submit_to_eks(bulk)

                    else:
                        self.submit(bulk, self.ecs)

                bulk = list()


    # --------------------------------------------------------------------------
    #
    def _get_runs_tree(self, run_id):
        return self.runs_tree[run_id]


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
    def _create_clf_resource(self, cred):
        """a wrapper around create cloudformation resource client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        clf_client = boto3.resource('cloudformation', aws_access_key_id     = cred['aws_access_key_id'],
                                                      aws_secret_access_key = cred['aws_secret_access_key'],
                                                      region_name           = cred['region_name'])

        return clf_client


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
    def _create_clf_client(self, cred):
        """a wrapper around create cloudformation client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        clf_client = boto3.client('cloudformation', aws_access_key_id     = cred['aws_access_key_id'],
                                                    aws_secret_access_key = cred['aws_secret_access_key'],
                                                    region_name           = cred['region_name'])
        
        return clf_client


    # --------------------------------------------------------------------------
    #
    def _create_eks_client(self, cred):
        """a wrapper around create eks client

           :param: cred: AWS credentials (access key, secert key, region)
        """
        eks_client = boto3.client('eks', aws_access_key_id     = cred['aws_access_key_id'],
                                         aws_secret_access_key = cred['aws_secret_access_key'],
                                         region_name           = cred['region_name'])
        
        return eks_client


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
            self.logger.trace('checking for existing hydraa cluster')
            
            if 'hydraa' in clusters[0]:
                self.logger.trace('hydraa cluster found: {0}'.format(clusters[0]))
                self.cluster_name = clusters[0]
                return clusters[0]
            else:
                self.logger.trace('exisiting hydraa cluster not found')

        else:
            self.logger.trace('no cluster found in this account')

        cluster_name = "hydraa_cluster_{0}".format(self.manager_id)

        self.logger.trace('creating new cluster {0}'.format(cluster_name))

        self._ecs_client.create_cluster(clusterName=cluster_name)
        self.cluster_name = cluster_name

        return cluster_name


    # --------------------------------------------------------------------------
    #
    def _wait_clusters(self, cluster_name):

        clsuter = self.get_ecs_cluster_arn(cluster_name=cluster_name)
        while not self._terminate.is_set():
            statuses = self._get_cluster_statuses(cluster_name)
            if all([status == 'ACTIVE' for status in statuses]):
                self.logger.trace('cluster {0} is active'.format(cluster_name))
                break
            time.sleep(WAIT_TIME)
            self.logger.trace('ECS status for cluster {0}: {1}'.format(cluster_name, statuses))


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
    def create_container_def(self, ctask):
        """ Build the internal structure of the container defination.
            
            :param: name   : container name
            :param: image  : image name to pull or upload
            :param: cpu    : number of cores to assign for this container
            :param: memory : amount of memory to assign for this container

            :return: container defination
        """
        con_def = {'name'        : ctask.name,
                   'cpu'         : ctask.vcpus,
                   'memory'      : ctask.memory,
                   'portMappings': [],
                   'essential'   : True,
                   'environment' : [],
                   'mountPoints' : [],
                   'volumesFrom' : [],
                   'image'       : ctask.image,
                   "entryPoint"  : [],
                   'command'     : ctask.cmd}

        return con_def


    # --------------------------------------------------------------------------
    #
    def create_ecs_task(self, container_def, batch):
        """Build the internal structure of the task.

           :param: container_def: a dictionary of a container specifications.

           :param: batch: list of containers to be added to the task

           :return: ecs task object and the family_id of the task
        """
        cpus = 0
        memory = 0
        task_def = {}
        ecs_task = {}
        family_id = f'hydraa_family_{str(uuid.uuid4())}'

        # cpu and memory specifications are required when using ECS FARGATE
        # FIXME: find a way to round the sum to a value that FARGATE allows
        if self.launch_type in FARGATE:
            cpus = sum(t.vcpus for t in batch)
            memory = sum(t.memory for t in batch)

            # check min cpus and memory
            if cpus < 256 or memory < 512:
                self.logger.warning('setting FARGATE resource requeste to minimum allowed:'
                                    ' CPU/MEM: 256/512')
                cpus = 256
                memory = 512

            task_def['cpu']  = str(cpus)
            task_def['memory'] = str(memory)
            task_def['networkMode'] = 'awsvpc'

            if batch[0].ecs_kwargs.get('subnet'):
                subnet = batch[0].ecs_kwargs.get('subnet')

            # FIXME: this will only work when we invoke the VM()
            elif hasattr(self.vms[0], 'SubnetID'):
                subnet = self.vms[0].SubnetID

            else:
                raise ValueError('FARGATE networkConfiguration requires subnet via VM'
                                 ' (SubnetID) or task.ecs_kwargs')

            ecs_task['networkConfiguration'] = {'awsvpcConfiguration': {'subnets': [subnet],
                                                                        'assignPublicIp': 'ENABLED'}}

        if not all(t.ecs_kwargs.get('executionRoleArn') for t in batch):
            raise ValueError('ECS executionRoleArn is required to launch a task_def')

        task_def['volumes'] = []
        task_def['family'] = family_id
        task_def['containerDefinitions'] = container_def
        task_def['requiresCompatibilities'] = [self.launch_type.upper()]
        task_def['executionRoleArn'] = batch[0].ecs_kwargs['executionRoleArn']

        # FIXME: sperate ecs_kwargs into: ecs_def_kwargs and ecs_kwargs
        # update the task def with any user specific ECS kwargs

        # register the task defination
        reg_task = self._ecs_client.register_task_definition(**task_def)

        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        self.logger.trace(f'task {family_id} is registered')

        ecs_task['count'] = 1
        ecs_task['cluster'] = self.cluster_name
        ecs_task['launchType'] = self.launch_type.upper()
        ecs_task['taskDefinition'] = task_def_arn

        # save the family id and its ARN
        self._family_ids[family_id] = OrderedDict()
        self._family_ids[family_id]['ARN'] = task_def_arn

        return family_id, ecs_task


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

        self.service_name = "hydraa_service_{0}".format(self.manager_id)

        # Check if the service already exist and use it
        running_services = self._ecs_client.list_services(cluster = self.cluster_name)

        for service in running_services['serviceArns']:
            if self.service_name in service:
                self.logger.trace('service {0} already exist on cluster {1}'.format(self.service_name,
                                                                                    self.cluster_name))

                return service

        self.logger.trace('no exisitng service found, creating.....')
        response = self._ecs_client.create_service(desiredCount = 1,
                                                   launchType = self.launch_type.upper(),
                                                   cluster = self.cluster_name,
                                                   serviceName = self.service_name,
                                                   taskDefinition = 'Hydraa-Task-Def',
                                                   networkConfiguration = {'awsvpcConfiguration': {
                                                                          'subnets': ['subnet-094da8d73899da51c',],
                                                                          'assignPublicIp': 'ENABLED',
                                                                          'securityGroups': ["sg-0702f37d21c55da64"]}},
                                                   # clientToken='request_identifier_string',
                                                   deploymentConfiguration={'maximumPercent': 200,
                                                                            'minimumHealthyPercent': 50})



        self.logger.trace('service {0} created'.format(self.service_name))

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
    def submit_to_eks(self, ctasks):
        """
        submit a single pod per batch of tasks
        """
        self.profiler.prof('submit_batch_start', uid=self.run_id)
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = AWS
            ctask.launch_type = self.launch_type

            self._tasks_book[str(ctask.name)] = ctask
            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.cluster.submit(ctasks)

        self.logger.trace('batch of [{0}] tasks is submitted '.format(len(ctasks)))

        self.profiler.prof('submit_batch_stop', uid=self.run_id)  


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks, cluster_name):
        """Starts a new ctask using the specified task definition. In this
           mode AWS scheduler will handle the task placement.
           submit is a wrapper around RUN_TASK: which suitable for tasks
           that runs for x amount of time and stops like batch jobs.

           submit supports only default task placement.

           :param: batch_size  : number of tasks to submit to the cluster.
           :param: task_def    : a dictionary of a task defination specifications.
           :param: cluster_name: cluster name to operate within.

           :return: submited ctasks ARNs

        """
        tptd = self._schedule(ctasks)
        containers = []
        for batch in tptd:
            for ctask in batch:
                # build an aws container defination from the task object
                ctask.provider = AWS
                ctask.id = self._task_id
                ctask.run_id = self.run_id
                ctask.launch_type = self.launch_type
                ctask.name = f'ctask-{ctask.id}'

                self._tasks_book[str(ctask.name)] = ctask

                containers.append(self.create_container_def(ctask))

                self.logger.trace(f'submitting tasks {ctask.name}')

                self._task_id +=1

            try:
                if self.launch_type in FARGATE:
                    if not all(t.ecs_launch_type in FARGATE for t in batch):
                        raise ValueError(f'Got wrong ECS task launch type, expected: {FARGATE}')

                # EC2 does not support Network config or platform version
                elif self.launch_type in EC2:
                    if not all(t.ecs_launch_type in EC2 for t in batch):
                        raise ValueError(f'Got wrong ECS tasks launch type, expected: {EC2}')

                else:
                    raise RuntimeError(f'launch type {self.launch_type} is not supported')

                family_id, ecs_task = self.create_ecs_task(containers, batch)

                # submit tasks of size "batch_size"
                response = self._ecs_client.run_task(**ecs_task)
                if response['failures']:
                    failures = [(failure['arn'], failure['reason']) for failure in response['failures']]
                    raise Exception(failures)

                task_def_arn = response['tasks'][0]['taskArn']
                for ctask in batch:
                    ctask.arn = task_def_arn

                # attach the unique ARN (i.e. pod id) to batch
                self._family_ids[family_id]['task_list']  = batch
                self._family_ids[family_id]['run_id'] = self.run_id
                self._family_ids[family_id]['batch_size'] = len(batch)
                self._family_ids[family_id]['manager_id'] = self.manager_id
                self._family_ids[family_id]['task_arns'] = response['tasks'][0]['taskArn']

            except Exception as e:
                # upon failure mark the tasks as failed
                for ctask in batch:
                    ctask.state = 'Failed'
                    ctask.set_exception(e)


    # --------------------------------------------------------------------------
    #
    def _get_task_stamps(self, tasks, cluster=None):
        """Pull the timestamps for every task by its ARN and convert
           them to a human readable.
        """
        if not cluster:
            cluster = self.cluster_name

        task_stamps  = OrderedDict()
        task_arns    = [arn for arn in tasks.keys()]
        tasks_chunks = self._describe_tasks(tasks, cluster)

        for task_chunk in tasks_chunks:   
            for task in task_chunk:
                arn = task['taskArn']
                tid = tasks[arn]
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

        task_stamps = self._get_task_stamps(self._tasks_book)

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

        df = pd.DataFrame(task_stamps.values(), index =[t for t in task_stamps.keys()])

        fname = '{0}_{1}_ctasks_{2}.csv'.format(self.launch_type, len(self._tasks_book),
                                                                          self.manager_id)
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
        st = df['pullStartedAt'].min()
        en = df['stoppedAt'].max()
        ttx = en - st
        return '{0} seconds'.format(ttx)


    # --------------------------------------------------------------------------
    #
    def _describe_tasks(self, tasks, cluster):

        tasks_arns = [t.arn for t in tasks]

        if len(tasks_arns) <= 100:
            response = self._ecs_client.describe_tasks(tasks=tasks_arns,
                                                       cluster=cluster)
            return [response['tasks']]

        tasks_chuncks = []

        # break the task_arns into a chunks of 100
        chunks = [tasks_arns[x:x+100] for x in range(0, len(tasks_arns), 100)]

        for chunk in chunks:
            response = self._ecs_client.describe_tasks(tasks=chunk,
                                                       cluster=cluster)

            if response['failures'] != []:
                raise Exception(f"There were some failures:\n{response['failures']}")
            status_code = response['ResponseMetadata']['HTTPStatusCode']
            if status_code != 200:
                msg = 'Task status request received status code {0}:\n{1}'
                raise Exception(msg.format(status_code, response))

            tasks_chuncks.append(response['tasks'])

        return tasks_chuncks


    # --------------------------------------------------------------------------
    #
    def ecs_tasks_statuses_watcher(self, cluster=None):

        if not cluster:
            cluster = self.cluster_name

        def _watch():

            last_seen_msgs = None

            while not self._terminate.is_set():
                with self._task_lock:
                    tasks = self.get_tasks()

                # make sure all tasks have ARNs and registered
                if tasks and all(t.arn for t in tasks):
                    tasks_chunks = self._describe_tasks(tasks, cluster)
                else:
                    time.sleep(0.1)
                    continue

                for chunk in tasks_chunks:
                    # task_def (i.e. Pod)
                    for task_def in chunk:
                        msgs = []
                        status = task_def['lastStatus']
                        if status and status in ['PENDING', 'RUNNING', 'STOPPED']:

                            containers = task_def['containers']
                            for cont in containers:
                                msg = {}
                                if cont['lastStatus'] == 'STOPPED':

                                    if cont.get('exitCode') == 0:
                                        msg = {'id': cont.get('name'),
                                               'status': 'Completed'}
                                    else:
                                        msg = {'id': cont.get('name'),
                                               'status': 'Failed',
                                               'exception': cont.get('reason')}

                                elif cont['lastStatus'] == 'PENDING':
                                    msg = {'id': cont.get('name'),
                                           'status': 'Pending',
                                           'reason': cont.get('reason')}

                                elif cont['lastStatus'] == 'RUNNING':
                                    msg = {'id': cont.get('name'),
                                           'status': 'Running'}
                                else:
                                    msg = {'id': cont.get('name'),
                                           'status': cont['lastStatus']}

                                if msg:
                                    msgs.append(msg)

                            if msgs and msgs != last_seen_msgs:
                                self.internal_q.put({'pod_id': task_def['taskArn'],
                                                    'pod_status': status,
                                                    'containers': msgs})
                                last_seen_msgs = msgs

                time.sleep(0.5)    

        watcher = threading.Thread(target=_watch, daemon=True, name='EcsEventWatcher')
        watcher.start()

        self.logger.trace(f'watcher thread {watcher.ident} started on {self.cluster_name}')


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        msg = None
        finshed = []
        failed, done, running = 0, 0, 0

        if self.launch_type in EKS:
            queue = self.cluster.result_queue

        else:
            # set the queue and start the ecs watcher thread
            queue = self.internal_q
            self.ecs_tasks_statuses_watcher()

        while not self._terminate.is_set():

            try:
                # pull a message from the cluster queue
                if not queue.empty():
                    _msg = queue.get(block=True, timeout=10)

                    if _msg:
                        parent_pod = _msg.get('pod_id')
                        containers = _msg.get('containers')

                    else:
                        continue

                    for cont in containers:

                        tid = cont.get('id')
                        status = cont.get('status')
                        task = self._tasks_book.get(tid)

                        if not task:
                            continue

                        if task.name in finshed or not status:
                            continue

                        msg = f'Task: "{task.name}" from pod "{parent_pod}" is in state: "{status}"'

                        if status == 'Completed':
                            if task.running():
                                running -= 1
                            task.set_result('Finished successfully')
                            finshed.append(task.name)
                            done += 1

                        elif status == 'Running':
                            if not task.running():
                                task.set_running_or_notify_cancel()
                                running += 1
                            else:
                                continue

                        elif status == 'Failed':
                            if task.tries:
                                task.tries -= 1
                                task.reset_state()
                            else:
                                if task.running():
                                    running -= 1
                                exception = cont.get('exception')
                                task.set_exception(Exception(exception))
                                finshed.append(task.name)
                                failed += 1

                        elif status == 'Pending':
                            reason = cont.get('reason')
                            message = cont.get('message')
                            msg += f': reason: {reason}, message: {message}'

                        # preserve the task state for future use
                        task.state = status

                        self.outgoing_q.put(msg)

                    if len(finshed) == len(self._tasks_book):
                        if self.auto_terminate:
                            termination_msg = (0, AWS)
                            self.outgoing_q.put(termination_msg)

            except Empty:
                time.sleep(0.1)
                continue


    # --------------------------------------------------------------------------
    #
    def stop_ecs_tasks(self, cluster_name, reason):
        """
        we identify 3 reasons to stop task:
        1- USER_CANCELED : user requested to kill the task
        2- SYS_CANCELED  : the system requested to kill the task
        """
        tasks = self.get_tasks()

        # we elimnate any duplicates by using set 
        tasks_arns = set([t.arn for t in tasks])

        for task_def_arn in tasks_arns:
            response = self._ecs_client.stop_task(task = task_def_arn,
                                                  reason = reason,
                                                  cluster = cluster_name)

            self.logger.info(f'stopping task {task_def_arn} on cluster {cluster_name} due to {reason}')


    # --------------------------------------------------------------------------
    #
    def list_task_definitions(self, task_name, status=None):
        """
        List all the task definations that match the task name and status.
        """
        if not status:
            status = 'ACTIVE'

        response = self._ecs_client.list_task_definitions(familyPrefix=task_name,
                                                          status=status)
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
    def create_ec2_instance(self, VM, use_existing=False):
            """
            VM: VM class object of vm.AwsVM
            use_existing: if True, then we will use the first running instance
            """
            # return dict of VM attributes by invoking the vm class
            vm = VM(self.cluster_name)
            instances = []

            if use_existing:
                # check if we have an already running instance
                filters=[{"Name": "instance-state-name", "Values": ["running"]}]
                reservations = self._ec2_client.describe_instances(filters).get("Reservations")

                # if we have a running instance(s) return the first one 
                if reservations:
                    for reservation in reservations:
                        for instance in reservation["Instances"]:
                            # TODO: maybe we should ask the users if they
                            # want to use that instance or not.
                            if instance["InstanceType"] == VM.InstanceID:
                                self.logger.trace("found running instances of type {0}".format(instance["InstanceType"]))
                                instances.append(instance)
                                break

            if not instances or not use_existing:
                self.logger.trace("no existing EC2 instance found or use_existing is disabled. Creating new instance")
                instances = self._ec2_resource.create_instances(**vm)

            for instance in instances:
                self.logger.trace("waiting for EC2 instance {0} to be launched".format(instance.id))
                instance.wait_until_running()
                self.logger.trace("EC2 instance: {0} has been launched".format(instance.id))

            VM.InstanceID = instance.id

            # wait for the instance to connect to the targeted cluster
            while not self._terminate.is_set():
                res = self._ecs_client.describe_clusters(clusters = [self.cluster_name])
                if res['clusters'][0]['registeredContainerInstancesCount'] >= 1:
                    self.logger.trace("instance {0} has been registered".format(instance.id))
                    break
                self.logger.trace('waiting for instance {0} to register to {1}'.format(instance.id,
                                                                                       self.cluster_name))
                time.sleep(WAIT_TIME)

            return instance.id


    # --------------------------------------------------------------------------
    #
    def _schedule(self, tasks):

        task_batch = copy.copy(tasks)
        batch_size = len(task_batch)

        if not batch_size:
            raise Exception('Batch size can not be 0')

        if self.launch_type in FARGATE:
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
        
        batch_map = tasks_per_task_def
        objs_batch = []
        for batch in batch_map:
           objs_batch.append(task_batch[:batch])
           task_batch[:batch]
           del task_batch[:batch]

        return(objs_batch)


    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        """Shutdown and delete task/service/instance/cluster
        """

        if not (self.cluster or self.cluster_name and self.status):
            return

        self.logger.trace("termination started")

        self._terminate.set()

        # Delete all the resource and the associated tasks resouces
        if self.launch_type in EC2 or self.launch_type in FARGATE:
            try:
                if self.service_name:
                    # set desired service count to 0 (obligatory to delete)
                    self._ecs_client.update_service(cluster=self.cluster_name,
                                                    service=self.service_name,
                                                               desiredCount=0)
                    # delete service
                    self._ecs_client.delete_service(cluster=self.cluster_name,
                                                    service=self.service_name)
            except:
                pass

            # step-1 deregister all task definitions
            if self._family_ids:
                for task_fam_key, task_fam_val in self._family_ids.items():
                    self.logger.trace("deregistering task {0}".format(task_fam_val['ARN']))
                    self._ecs_client.deregister_task_definition(taskDefinition=task_fam_val['ARN'])

            # step-2 terminate virtual machine(s)
            instances = self._ecs_client.list_container_instances(cluster=self.cluster_name)
            if instances["containerInstanceArns"]:
                container_instance_resp = self._ecs_client.describe_container_instances(
                cluster=self.cluster_name,
                containerInstances=instances["containerInstanceArns"])

                for ec2_instance in container_instance_resp["containerInstances"]:
                    istance_id = ec2_instance['ec2InstanceId']
                    self.logger.trace("terminating instance {0}".format(istance_id))
                    self._ec2_client.terminate_instances(DryRun=False,
                                                         InstanceIds=[istance_id])

                    waiter = self._ec2_client.get_waiter('instance_terminated')
                    waiter.wait(InstanceIds=[istance_id])

            # step-3 delete the ECS cluster
            try:
                self._ecs_client.delete_cluster(cluster=self.cluster_name)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'ClusterContainsTasksException':
                    self.stop_ecs_tasks(self.cluster_name, reason='USER_CANCELED')
                    self._ecs_client.delete_cluster(cluster=self.cluster_name)
                else:
                    raise error

                self.logger.trace("hydraa cluster {0} found and deleted".format(self.cluster_name))

        elif self.launch_type in EKS:
            self.cluster.shutdown()
            self.cluster = None

        self.status = False
        self.cluster_name = None
