import os
import sys
import time
import uuid
import errno
import queue
import atexit
import openstack
import threading

from openstack.cloud import exc

from collections import OrderedDict
from hydraa.services.caas_manager.utils import ssh
from hydraa.services.caas_manager.utils import misc
from hydraa.services.caas_manager.utils import kubernetes


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

JET2      = 'jetstream2'
ACTIVE    = True
WAIT_TIME = 2

# --------------------------------------------------------------------------
#
class Jet2Caas():
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

        self.vm     = None
        self.status = False
        self.DryRun = DryRun

        self.client   = self._create_client(cred)
        self.network  = None
        self.security = None
        self.server   = None
        self.ips      = []
        self.remote   = None
        self.cluster  = None

        self._task_id = 0      

        self.vm     = VM
        self.run_id = '{0}.{1}'.format(self.vm.LaunchType, str(uuid.uuid4()))
        # tasks_book is a datastructure that keeps most of the 
        # cloud tasks info during the current run.
        self._tasks_book  = OrderedDict()
        self._pods_book   = OrderedDict()

        self._run_cost    = 0
        self.runs_tree    = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, JET2, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.logger   = log
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()

        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start, name='Jet2CaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        atexit.register(self._shutdown)
    

    def start(self):

        if self.status:
            print('Manager already started')
            return self.run_id

        print("starting run {0}".format(self.run_id))

        self.profiler.prof('prep_start', uid=self.run_id)

        self.image    = self.create_or_find_image()
        self.flavor   = self.client.compute.find_flavor(self.vm.FlavorId)
        self.security = self.create_security_with_rule()
        self.keypair  = self.create_or_find_keypair()

        self.profiler.prof('prep_stop', uid=self.run_id)

        self.profiler.prof('server_create_start', uid=self.run_id)
        
        self.server = self._create_server(self.image, self.flavor, self.keypair,
                              self.security, self.vm.MinCount, self.vm.MaxCount)
        self.profiler.prof('server_create_stop', uid=self.run_id)

        self.profiler.prof('ip_create_start', uid=self.run_id)

        self.assign_ips()

        self.profiler.prof('ip_create_stop', uid=self.run_id)

        self.assign_ssh_security_groups()

        self.vm.Servers = self.list_servers()

        self.vm.Remotes = {}
        for server in self.vm.Servers:
            public_ip = server.access_ipv4

            #FIXME: VM should have a username instead of hard coded ubuntu
            self.vm.Remotes[server.name] = ssh.Remote(self.vm.KeyPair, 'ubuntu', public_ip,
                                                                               self.logger)

        # containers per pod
        cluster_size = self.server.flavor.vcpus - 1

        self.cluster = kubernetes.Cluster(self.run_id, self.vm, cluster_size,
                                                   self.sandbox, self.logger)

        self.cluster.bootstrap()

        # call get work to pull tasks
        self._get_work()

        self.status = ACTIVE

        self.runs_tree[self.run_id] =  self._pods_book


    # --------------------------------------------------------------------------
    #
    def _get_work(self):

        bulk = list()
        max_bulk_size = 100
        max_bulk_time = 2        # seconds
        min_bulk_time = 0.1      # seconds

        self.wait_thread = threading.Thread(target=self._wait_tasks, name='Jet2CaaSWatcher')
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
                self.submit(bulk)

            if not self.asynchronous:
                if not self.wait_thread.is_alive():
                    self.wait_thread.start()

            bulk = list()


    # --------------------------------------------------------------------------
    #
    def _create_client(self, cred):
        jet2_client = openstack.connect(**cred)
        
        return jet2_client


    # --------------------------------------------------------------------------
    #
    def _get_run_status(self, pod_id):
        self.cluster.get_pod_status(pod_id)
    

    # --------------------------------------------------------------------------
    #
    def create_security_with_rule(self):

        # we are using the default security group for now
        security_group_name = 'SSH and ICMP enabled'
        security_rule_exist = 'ConflictException: 409'

        security = self.client.get_security_group(security_group_name)

        # FIXME: check if these rules already exist, if so pass
        self.vm.Rules = []
        try:
            if security.id:
                return security

        except Exception as e:
            # FIXME: check rules exist by id not by exceptions type
            if security_rule_exist in e.args:
                self.logger.trace('security rules exist')
                pass
            else:
                raise Exception(e)

        return security


    # --------------------------------------------------------------------------
    #
    def create_or_find_keypair(self):

        keypair = None
        if self.vm.KeyPair:
            self.logger.trace('Checking user provided ssh keypair')
            keypair = self.client.compute.find_keypair(self.vm.KeyPair)

        if not keypair:
            self.logger.trace("creating ssh key Pair")
            key_name = 'id_rsa_{0}'.format(self.run_id.replace('.', '-'))
            keypair  = self.client.create_keypair(name=key_name)

            ssh_dir_path = '{0}/.ssh'.format(self.sandbox)

            os.mkdir(ssh_dir_path, 0o700)

            # download both private and public keys
            keypair_pri = '{0}/{1}'.format(ssh_dir_path, key_name)
            keypair_pub = '{0}/{1}.pub'.format(ssh_dir_path, key_name)

            # save pub/pri keys in .ssh
            with open(keypair_pri, 'w') as f:
                f.write("%s" % keypair.private_key)

            with open(keypair_pub, 'w') as f:
                f.write("%s" % keypair.public_key)

            self.vm.KeyPair = [keypair_pri, keypair_pub]

            # modify the permission
            os.chmod(keypair_pri, 0o600)
            os.chmod(keypair_pub, 0o644)

        if not keypair:
            raise Exception('keypair creation failed')

        return keypair


    # --------------------------------------------------------------------------
    #
    def create_or_find_image(self):
        
        image = self.client.compute.find_image(self.vm.ImageId)

        if not image.id:
            raise NotImplementedError

        return image


    # --------------------------------------------------------------------------
    #
    def create_or_find_network(self):

        network = None
        # user provided a newtwork name that we need to find
        if self.vm.Network:
            network = self.client.network.find_network(self.vm.Network)
            if network and network.id:
                self.logger.trace('network {0} found'.format(network.name))
                return network
        
        
        network = self.client.network.find_network('auto_allocated_network')
        return network

    # --------------------------------------------------------------------------
    #
    def list_servers(self):
        servers = self.client.list_servers()
        
        return servers


    # --------------------------------------------------------------------------
    #
    def assign_ips(self):

        servers = self.list_servers()
        
        for server in servers:
            # if the server has an ip assigned then skip it
            if not server.access_ipv4:
                self.client.add_auto_ip(server)
                self.logger.trace('auto assigned ip {0} to vm {1}'.format(server.name, server.access_ipv4))
            else:
                self.logger.trace('vm {0} already has an ip assigned'.format(server.name, server.access_ipv4))


    # --------------------------------------------------------------------------
    #
    def assign_ssh_security_groups(self):

        # we always assume that the SSH group is prepaired by user for us
        ssh_sec_group = None
        for sec in self.client.list_security_groups():
            if ('SSH' or 'ssh') in sec.name:
                ssh_sec_group = sec

        if ssh_sec_group:
            servers = self.list_servers()
            for server in servers:
                ssh_is_associated = any([('SSH' or 'ssh') in d['name'] for d in server.security_groups])

                # if ssh group already assigned then skip
                if ssh_is_associated:
                    self.logger.trace('vm {0} already has ssh security group {1}'.format(server.name,
                                                                                    sec.get('name')))
                    continue
    
                # if not ssh group associated then assign the ssh group
                else:
                    self.client.add_server_security_groups(server, ssh_sec_group)
                    self.logger.trace('vm {0} assigned ssh security group {1}'.format(server.name,
                                                                                 sec.get('name')))

        # we could not find any group or SSH group
        else:
            raise Exception('No valid SSH security group found')


    # --------------------------------------------------------------------------
    #
    def _create_server(self, image, flavor, key_pair, security, min_count, max_count):

        server_name = 'hydraa_Server-{0}'.format(self.run_id)

        self.logger.trace('creating {0}'.format(server_name))
        server = self.client.create_server(name=server_name,
                                           image=image.id,
                                           flavor=flavor.id,
                                           key_name=key_pair.name,
                                           min_count=min_count,
                                           max_count=max_count)
        
        # Wait for a server to reach ACTIVE status.
        self.client.wait_for_server(server)

        if not security.name == 'default':
            self.client.add_server_security_groups(server, [security.name])
        
        self.logger.trace('server is active')
        
        return server


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):
        """
        submit a single pod per batch of tasks
        """
        self.profiler.prof('submit_batch_start', uid=self.run_id)
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = JET2
            ctask.launch_type = self.vm.LaunchType

            self._tasks_book[str(ctask.id)] = ctask
            self.logger.trace('submitting tasks {0}'.format(ctask.id))

            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.cluster.submit(ctasks)
        
        # create entry for the pod in the pods book
        for idx, pod_name in enumerate(pods_names):
            self._pods_book[pod_name] = OrderedDict()
            self._pods_book[pod_name]['manager_id']    = self.manager_id
            self._pods_book[pod_name]['task_list']     = batches[idx]
            self._pods_book[pod_name]['batch_size']    = len(batches[idx])
            self._pods_book[pod_name]['pod_file_path'] = depolyment_file
        
        self.profiler.prof('submit_batch_start', uid=self.run_id)


        #self.profiles()


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        if self.asynchronous:
            raise Exception('Task wait is not supported in asynchronous mode')

        while not self._terminate.is_set():

            statuses = self.cluster._get_task_statuses()

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


            time.sleep(5)


    # --------------------------------------------------------------------------
    #
    def profiles(self):
        
        pod_stamps  = self.cluster.get_pod_status()
        task_stamps = self.cluster.get_pod_events()
        fname       = '{0}/{1}_{2}_ctasks.csv'.format(self.sandbox,
                                             len(self._tasks_book),
                                                 self.cluster.size)
        if os.path.isfile(fname):
            print('profiles already exist {0}'.format(fname))
            return fname

        try:
            import pandas as pd
        except ModuleNotFoundError:
            print('pandas module required to obtain profiles')

        df = (pd.merge(pod_stamps, task_stamps, on='Task_ID'))

        df.to_csv(fname)
        print('Dataframe saved in {0}'.format(fname))

        return fname


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

            self.client   = None
            self.network  = None
            self.security = None
            self.server   = None

            self._pods_book.clear()
            self.logger.trace('done')


    # --------------------------------------------------------------------------
    #
    def _shutdown(self):

        if not self.server:
            return

        self.logger.trace("termination started")

        self._terminate.set()

        if self.vm.KeyPair:
            self.logger.trace('deleting ssh keys')
            for k in self.vm.KeyPair:
                try:
                    os.remove(k)
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise e

            if self.keypair.id:
                self.logger.trace('deleting key-name from cloud storage')
                self.client.delete_keypair(self.keypair.id)

        # delete all subnet
        if self.network:
            if not self.network.name == "auto_allocated_network":
                self.logger.trace('deleting subnets')
                for subnet in self.network.subnet_ids:
                    self.client.network.delete_subnet(subnet, ignore_missing=False)

        # deleting the server
        if self.server:
            for server in self.list_servers():
                self.client.delete_server(server.name)
                self.logger.trace('server {0} is deleted'.format(server.name))

            if self.ips:
                for ip in self.ips:
                    self.logger.trace('deleting allocated ip')
                    self.client.delete_floating_ip(ip)

        self.__cleanup()

