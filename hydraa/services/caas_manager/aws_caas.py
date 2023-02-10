import os
import sys
import copy
import math
import time
import uuid
import boto3
import queue
import atexit
import threading
import itertools  as iter
from datetime import datetime
from collections import OrderedDict


from hydraa.services.caas_manager.utils    import kubernetes
from hydraa.services.cost_manager.aws_cost import AwsCost
from hydraa.services.maas_manager.aws_maas import AwsMaas

__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


AWS       = 'aws'
BUDGET    = 0
ACTIVE    = True
EKS       = ['EKS', 'eks']         # Elastic kubernetes Service
EC2       = ['EC2', 'ec2']         # Elastic Cloud 
ECS       = ['ECS', 'ecs']         # Elastic Container Service
FARGATE   = ['FARGATE', 'fargate'] # Fargate container service

WAIT_TIME = 2

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

       :pram asynchronous: wait for the tasks to finish or run in the
                           background.
       :param DryRun: Do a dryrun first to verify permissions.
    """

    def __init__(self, sandbox, manager_id, cred, VM, asynchronous,
                                          log, prof, DryRun=False):

        self.manager_id = manager_id

        self.status = False
        
        # TODO: enable DryRun by the user to
        #       verify permissions before starting
        #       the actual run.
        self.DryRun = DryRun
        
        # FIXME: try to ognize these clients into a uniform dict
        #        self.clients['EKS'], self.clients['EC2'], etc.
        self._ecs_client    = self._create_ecs_client(cred)
        self._ec2_client    = self._create_ec2_client(cred)
        self._iam_client    = self._create_iam_client(cred)
        self._prc_client    = self._create_prc_client(cred)
        self._clw_client    = self._create_clw_client(cred)
        self._clf_client    = self._create_clf_client(cred)
        self._eks_client    = self._create_eks_client(cred)

        self._clf_resource  = self._create_clf_resource(cred)
        self._ec2_resource  = self._create_ec2_resource(cred)
        self._dydb_resource = self._create_dydb_resource(cred)


        self.cluster_name = None
        self.service_name = None

        self.vm        = VM
        self.vm.Region = cred['region_name']

        self.run_id   = '{0}.{1}'.format(self.vm.LaunchType, str(uuid.uuid4()))
        self._task_id = 0

        # tasks_book is a datastructure that keeps most of the 
        # cloud tasks info during the current run.
        self._tasks_book   = OrderedDict()
        self._family_ids   = OrderedDict()

        self._run_cost     = 0

        self.runs_tree = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, AWS, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.logger   = log
        self.profiler = prof(name=__name__, path=self.sandbox)
         

        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()

        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start, name='AwsCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

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
        return AwsCost(self._prc_client, self._dydb_resource, self.region)
    

    # --------------------------------------------------------------------------
    #
    def _moniter(self) -> AwsMaas:
        return AwsMaas(self._clw_client)

        
    # --------------------------------------------------------------------------
    #
    def start(self, service=False, budget=0, time=0):
        """
        Create a cluster, container, task defination with user requirements.
        and run them via **run_task

        :param: tasks:   List of Task class object 
                         
        
        :param: budget: float
                         expected charging amount by AWS to operate within.
        
        :param: time: int
                         expected execution time on AWS in minutes

        """
        if self.status:
            print('Manager already started')
            return self.run_id

        # TODO: In our scheduling mechanism we need to consider:
        #       memory, cpu and number of instances besides tasks
        #       per task_def and task_defs per cluster
        if budget:
            budget_calc = self._budget()
            if not time:
                raise Exception('estimated runtime is required')

            run_cost =  budget_calc.get_cost(self.vm.LaunchType, tasks, time)

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

        print("starting run {0}".format(self.run_id))

        # check if this launch will be a service
        if service:
            self.create_ecs_service()

        if self.vm.LaunchType in FARGATE:
            self.logger.trace('Fargate VM type')
            self.ECS_cluster = self.create_cluster()
            self._wait_clusters(self.ECS_cluster)

        if self.vm.LaunchType in EC2:
            self.logger.trace('EC2 VM type')
            self.ECS_cluster = self.create_cluster()
            self._wait_clusters(self.ECS_cluster)
            self.create_ec2_instance(self.vm)

        
        # check if this is an EKS service
        if self.vm.LaunchType in EKS:
            self.EKS_cluster = kubernetes.EKS_Cluster(self.run_id, self.sandbox,
                                   self.vm, self._iam_client,self._clf_resource,
                                           self._clf_client, self._ec2_resource,
                                                               self._eks_client)
            self.EKS_cluster.bootstrap()

        self.runs_tree[self.run_id] =  self._family_ids

        # call get work to pull tasks
        self._get_work()

        # now set the manager as active
        self.status = ACTIVE

    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = 100
        max_bulk_time = 2        # seconds
        min_bulk_time = 0.1      # seconds
        
        self.wait_thread = threading.Thread(target=self._wait_tasks, name='AwSCaaSWatcher')	
        self.wait_thread.daemon = True

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
                if self.vm.LaunchType in EKS:
                    self.submit_to_eks(bulk)

                else:
                    self.submit(bulk, self.ECS_cluster)

                if not self.asynchronous:
                    if not self.wait_thread.is_alive():
                        self.wait_thread.start()
                bulk = list()


    # --------------------------------------------------------------------------
    #
    def _get_runs_tree(self, run_id):
        return self.runs_tree[run_id]


    # --------------------------------------------------------------------------
    #
    def _get_run_status(self, run_id):
        
        run_status = [] 
        for key, val in self._family_ids.items():
            if val.get('run_id') == run_id:
                tasks = [t.arn for t in val.get('task_list')]
                statuses = self._get_task_statuses(tasks, self.cluster_name)
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
        while True:
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
                   "logConfiguration": {"logDriver": "awslogs",
                                        "options": { 
                                                    "awslogs-group" : "/ecs/first-run-task-definition",
                                                    "awslogs-region": "us-east-1",
                                                    "awslogs-stream-prefix": "ecs"}},
                   'image'       : ctask.image,
                   "entryPoint"  : [],
                   'command'     : ctask.cmd}

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
        task_def['containerDefinitions']    = container_def
        task_def['executionRoleArn']        = 'arn:aws:iam::626113121967:role/ecsTaskExecutionRole'
        task_def['networkMode']             = 'awsvpc'
        task_def['requiresCompatibilities'] = ['FARGATE']

        reg_task = self._ecs_client.register_task_definition(**task_def)

        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']

        self.logger.trace('task {0} is registered'.format(family_id))

        # save the family id and its ARN
        self._family_ids[family_id] = OrderedDict()
        self._family_ids[family_id]['ARN'] =  task_def_arn

        return family_id, task_def_arn


    # --------------------------------------------------------------------------
    #
    def create_ec2_task_def(self, container_defs, cpu = 0, memory = 0):
        """Build the internal structure of the task defination.

           :param: container_def: a dictionary of a container specifications.

           :param: cpu: the number of CPU units used by the task.

           :param: memory: the amount of memory (in MiB) used by the task.

           :return: task defination name and task ARN (Amazon Resource Names)
        """
        # represents a pod in kuberentes lingo
        task_def = {}
        if cpu:
            task_def['cpu'] = cpu # optional
        if memory:
            task_def['memory'] = memory    # optional

        family_id = 'hydraa_family_{0}'.format(str(uuid.uuid4()))

        reg_task = self._ecs_client.register_task_definition(volumes=[],
                                                             family=family_id,
                                                             requiresCompatibilities=['EC2'],
                                                             containerDefinitions=container_defs,
                                                             executionRoleArn='arn:aws:iam::626113121967:role/ecsTaskExecutionRole')

        task_def_arn = reg_task['taskDefinition']['taskDefinitionArn']      
        self.logger.trace('task {0} is registered'.format(family_id))

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

        self.service_name = "hydraa_service_{0}".format(self.manager_id)

        # Check if the service already exist and use it
        running_services = self._ecs_client.list_services(cluster = self.cluster_name)

        for service in running_services['serviceArns']:
            if self.service_name in service:
                self.logger.trace('service {0} already exist on cluster {1}'.format(self.service_name, self.cluster_name))

                return service

        self.logger.trace('no exisitng service found, creating.....')
        response = self._ecs_client.create_service(cluster        = self.cluster_name,
                                                   serviceName    = self.service_name,
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
            ctask.launch_type = self.vm.LaunchType

            self._tasks_book[str(ctask.id)] = ctask.name
            self.logger.trace('submitting tasks {0}').format(ctask.id)
            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.EKS_cluster.submit(ctasks)
        
        # create entry for the pod in the pods book
        '''
        for idx, pod_name in enumerate(pods_names):
            self._pods_book[pod_name] = OrderedDict()
            self._pods_book[pod_name]['manager_id']    = self.manager_id
            self._pods_book[pod_name]['task_list']     = batches[idx]
            self._pods_book[pod_name]['batch_size']    = len(batches[idx])
            self._pods_book[pod_name]['pod_file_path'] = depolyment_file
        '''
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
        tptd       = self._schedule(ctasks)
        containers = []
        for batch in tptd:
            for ctask in batch:
                # build an aws container defination from the task object
                ctask.run_id      = self.run_id
                ctask.id          = self._task_id
                ctask.name        = 'ctask-{0}'.format(ctask.id)
                ctask.provider    = AWS
                ctask.launch_type = self.vm.LaunchType
                
                containers.append(self.create_container_def(ctask))
                
                self._tasks_book[str(ctask.name)] = ctask

                self.logger.trace('submitting tasks {0}'.format(ctask.name))

                self._task_id +=1

            try:
                # FIXME: Pass the memory and cpu via a VM class
                if self.vm.LaunchType in FARGATE:
                    task_def_arn = self.create_fargate_task_def(containers, 256, 1024)
                    kwargs['platformVersion']      = 'LATEST'
                    kwargs['networkConfiguration'] = {'awsvpcConfiguration': {'subnets': [
                                                                            'subnet-094da8d73899da51c',],
                                                    'assignPublicIp'     : 'ENABLED',
                                                    'securityGroups'     : ["sg-0702f37d21c55da64"]}}

                # EC2 does not support Network config or platform version
                if self.vm.LaunchType in EC2:
                    family_id, task_def_arn = self.create_ec2_task_def(containers)

                kwargs = {}
                # count of tasks is count of pods
                # FIXME: find a way to let the user make set
                # the number of pods if they are identical
                kwargs['count']         = 1
                kwargs['cluster']       = cluster_name
                kwargs['launchType']    = self.vm.LaunchType
                kwargs['overrides']     = {}
                kwargs['taskDefinition'] = task_def_arn

                # submit tasks of size "batch_size"
                response = self._ecs_client.run_task(**kwargs)
                if response['failures']:
                    self.logger.error(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'],
                                                failure['reason']) for failure in response['failures']]))

                # attach the unique ARN (i.e. pod id) to batch
                self._family_ids[family_id]['task_arns'] = []

                for i, task in enumerate(response['tasks']):
                    self._family_ids[family_id]['task_arns'].append(task['taskArn'])

                self._family_ids[family_id]['manager_id'] = self.manager_id
                self._family_ids[family_id]['run_id']     = self.run_id
                self._family_ids[family_id]['task_list']  = batch
                self._family_ids[family_id]['batch_size'] = len(batch)
            
            except Exception as e:
                # upon failure mark the tasks
                # as failed
                for ctask in batch:
                    self.logger.error('failed to submit {0}, check task exception'.format(ctask.name))
                    ctask.set_exception(e)


    # --------------------------------------------------------------------------
    #
    def _get_task_stamps(self, tasks, cluster):
        """Pull the timestamps for every task by its ARN and convert
           them to a human readable.
        """

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

        task_stamps = self._get_task_stamps(self._tasks_book, self.cluster_name)

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

        df = pd.DataFrame(task_stamps.values(), index =[t for t in task_stamps.keys()])

        fname = '{0}_{1}_ctasks_{2}.csv'.format(self.vm.LaunchType, len(self._tasks_book),
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
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Retrieve task statuses from ECS API

        Returns list of {RUNNING|PENDING|STOPPED} for each id in tasks_book
        """
        # describe_tasks accepts only 100 arns per invokation
        # so we split the task arns into chunks of 100
        tasks_arns = list(iter.chain.from_iterable(tasks))
        if len(tasks_arns) <= 100:
            response = self._ecs_client.describe_tasks(tasks=tasks_arns,
                                                        cluster=cluster)
            return [response['tasks']]

        tasks = iter(tasks)
        tasks_chuncks = []

        chunks = [tasks_arns[x:x+1] for x in range(0, len(tasks_arns), 100)]

        for chunk in chunks:
            response = self._ecs_client.describe_tasks(tasks=chunk,
                                                   cluster=cluster)

            #FIXME: if we have a failure then update the tasks_book 
            #       with status/error code and print it.
            if response['failures'] != []:
                raise Exception('There were some failures:\n{0}'.format(
                    response['failures']))
            status_code = response['ResponseMetadata']['HTTPStatusCode']
            if status_code != 200:
                msg = 'Task status request received status code {0}:\n{1}'
                raise Exception(msg.format(status_code, response))

            tasks_chuncks.append(response['tasks'])

        return tasks_chuncks


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self, tasks, cluster):
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Retrieve task statuses from ECS API

        Returns list of {RUNNING|PENDING|STOPPED} for each id in tasks_book
        """
        stopped = []
        failed  = []
        running = []
        '''
        Lifecycle states: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-lifecycle.html
        '''

        tasks_chunks = self._describe_tasks(tasks, cluster)
        statuses = []
        for task_chunk in tasks_chunks:
            for task in task_chunk:
                # the task (i.e. pod) stopped
                if task['lastStatus'] == 'STOPPED':
                    # if the pod stopped then mark all of 
                    # its container as stopped
                    for container in task['containers']:
                        if container['exitCode'] == 0:
                            stopped.append(container['name'])
                        else:
                            failed.append(container['name'])

                # the task (i.e. pod) running
                if task['lastStatus'] == 'RUNNING':
                    for container in task['containers']:
                        if container['lastStatus'] == 'STOPPED':
                            if container['exitCode'] == 0:
                                stopped.append(container['name'])
                            else:
                                failed.append(container['name'])

                        elif container['lastStatus'] == 'RUNNING':
                            running.append(container['name'])

                # else it is transitioning 
                elif task['lastStatus'] in ['PROVISIONING', 'PENDING', 'ACTIVATING',
                                            'DEACTIVATING', 'STOPPING', 'DEPROVISIONING', 'DELETED']:
                                            for container in task['containers']:
                                                running.append(container['name'])

            statuses.append(stopped)
            statuses.append(failed)
            statuses.append(running)

        return statuses


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):
        """
        ref: https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ecs.html
        Wait for task status until STOPPED
        """
        if self.asynchronous:
            raise Exception('Task wait is not supported in asynchronous mode')

        if self.vm.LaunchType in EKS:
            self.EKS_cluster.wait()
            return

        while not self._terminate.is_set():

            # some other threads updates the task book so keep this
            # thread updated as well with the latest task book entries
            tasks = [t['task_arns'] for t in self._family_ids.values()]

            # request the statuses for all task_arns
            statuses = self._get_task_statuses(tasks, self.cluster_name)

            stopped = statuses[0]
            failed  = statuses[1]
            running = statuses[2]

            self.logger.trace('failed tasks " {0}'.format(failed))
            self.logger.trace('stopped tasks" {0}'.format(stopped))
            self.logger.trace('running tasks" {0}'.format(running))

            for task in self._tasks_book.values():
                if task.name in stopped:
                    if task.done():
                        continue
                    else:
                        task.set_result('Done')
                        self.logger.trace('sending done {0} to output queue'.format(task.name))
                        self.outgoing_q.put(task.name)

                # FIXME: better approach?
                elif task.name in failed:
                    try:
                        # check if the task marked failed
                        # or not and wait for 0.1s to return
                        exc = task.exception(0.1)
                        if exc:
                            # we already marked it
                            continue
                    except TimeoutError:
                        # never marked so mark it.
                        task.set_exception('Failed')
                        self.logger.trace('sending failed {0} to output queue'.format(task.name))
                        self.outgoing_q.put(task.name)

                elif task.name in running:
                    if task.running():
                        continue
                    else:
                        task.set_running_or_notify_cancel()

            time.sleep(1)


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
        task_arn = self.tasks_book[task_id]

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
    def create_ec2_instance(self, VM):
            """
            ImageId: An AMI ID is required to launch an instance and must be
                    specified here or in a launch template.

            Instancetype: The instance type. Default m1.small For more information 
                        (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html).
            
            MinCount: The minimum number of instances to launch.
            MaxCount: The maximum number of instances to launch.

            UserData: accept any linux commnad (acts as a bootstraper for the instance)
            """
            vm = VM(self.cluster_name)

            # FIXME: check for exisitng EC2 hydraa instances specifically.
            
            # check if we have an already running instance
            reservations = self._ec2_client.describe_instances(Filters=[{"Name": "instance-state-name",
                                                                         "Values": ["running"]},]).get("Reservations")
            
            # if we have a running instance(s) return the first one 
            if reservations:
                for reservation in reservations:
                    for instance in reservation["Instances"]:
                        # TODO: maybe we should ask the users if they
                        # want to use that instance or not.
                        if instance["InstanceType"] == VM.InstanceID :
                            self.logger.trace("found running instances of type {0}".format(instance["InstanceType"]))

                            return instance["InstanceId"]

            self.logger.trace("no existing EC2 instance found with the same resource requirements")

            instances = self._ec2_resource.create_instances(**vm)

            for instance in instances:
                self.logger.trace("waiting for EC2 instance {0} to be launched".format(instance.id))
                instance.wait_until_running()
                self.logger.trace("EC2 instance: {0} has been launched".format(instance.id))
            
            VM.InstanceID = instance.id

            # wait for the instance to connect to the targeted cluster
            while True:
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

        if self.vm.LaunchType in FARGATE:
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
        
        batch_map = tasks_per_task_def
        objs_batch = []
        for batch in batch_map:
           objs_batch.append(task_batch[:batch])
           task_batch[:batch]
           del task_batch[:batch]

        return(objs_batch)


    # --------------------------------------------------------------------------
    #
    def __cleanup(self):

        caller = sys._getframe().f_back.f_code.co_name
        self._task_id     = 0
        self._run_cost    = 0
        self._tasks_book.clear()

        if caller == '_shutdown':
            self.manager_id = None
            self.status = False

            self._ecs_client    = None
            self._ec2_client    = None
            self._iam_client    = None
            self._prc_client    = None

            self._ec2_resource  = None
            self._dydb_resource = None

            self.cluster_name = None
            self.service_name = None

            self._family_ids.clear()

            self.region =  None
            self.logger.trace('cleanup done')
        

    # --------------------------------------------------------------------------
    #
    def _shutdown(self):
        """Shut everything down and delete task/service/instance/cluster"""

        if not self.cluster_name and self.status == False:
            return
        
        self.logger.trace("termination started")

        self._terminate.set()

        # Delete all the resource and the associated tasks resouces
        if self.vm.LaunchType in EC2 or self.vm.LaunchType in FARGATE:
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
            self._ecs_client.delete_cluster(cluster=self.cluster_name)
            self.logger.trace("hydraa cluster {0} found and deleted".format(self.cluster_name))
        
        if self.vm.LaunchType in EKS:
            self.EKS_cluster.shutdown()

        self.__cleanup()
        

