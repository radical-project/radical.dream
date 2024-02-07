import os
import chi
import sys
import copy
import time
import uuid
import queue
import atexit
import threading
import openstack
import chi.lease
import chi.server
import keystoneauth1
import blazarclient

from chi import lease
from chi import server
from queue import Empty

from collections import OrderedDict
from hydraa.services.caas_manager.utils import ssh
from hydraa.services.caas_manager.kubernetes import kubernetes


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

CHI = 'chameleon'
ACTIVE = True
CHI_USER = 'cc'


# --------------------------------------------------------------------------
#
class ChiCaas:
    """Represents a collection of clusters (resources) with a collection of
       services, tasks and instances.:
       :param cred: CHI credentials (source ~/app-cred-remote_acess-openrc.sh)

       :pram asynchronous: wait for the tasks to finish or run in the
                           background.
       :param DryRun: Do a dryrun first to verify permissions.
    """

    def __init__(self, sandbox, manager_id, cred, VMS, asynchronous, auto_terminate, log, prof):
        
        self.manager_id = manager_id
        self.status = False

        self.client   = self.create_op_client(cred)
        self.lease    = None
        self.network  = None
        self.security = None
        self.servers  = None
        self.remote   = None
        self.cluster  = None
        self.keypair  = None
        self.launch_type = VMS[0].LaunchType

        self._task_id = 0
    
        self.vms = VMS
        self.run_id = '{0}.{1}'.format(self.launch_type.lower(),
                                       str(uuid.uuid4()))
    
        self._tasks_book  = OrderedDict()
        self._pods_book   = OrderedDict()

        self._run_cost    = 0
        self.runs_tree    = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, CHI, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.logger   = log
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.incoming_q = queue.Queue() # caas manager -> main manager
        self.outgoing_q = queue.Queue() # main manager -> caas manager
        self.internal_q = queue.Queue() # caas manager -> caas manager

        self._task_lock = threading.Lock()
        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start,
                                             name='ChiCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        atexit.register(self.shutdown)


    # --------------------------------------------------------------------------
    #
    def start(self):

        print("starting run {0}".format(self.run_id))

        self.keypair = self.create_or_find_keypair()

        if self.launch_type == 'KVM':
            chi.use_site('KVM@TACC')
            self.profiler.prof('server_create_start', uid=self.run_id)
            self.servers = self.create_servers()
            self.profiler.prof('server_create_stop', uid=self.run_id)

        elif self.launch_type == 'Baremetal':
            chi.use_site('CHI@TACC')
            msg = ("would you like to use an existing lease?" \
                   "if so, please provide it:")

            user_in = input (msg)

            if user_in ['no', 'No']:
                self.lease = self._lease_resources()
            else: 
                self.lease = self._lease_resources(lease_id=str(user_in))
                self.servers = self.create_servers(user_lease=self.lease)

        self.cluster = kubernetes.K8sCluster(self.run_id, self.vms,
                                             self.sandbox, self.logger)

        self.cluster.bootstrap()

        # call get work to pull tasks
        self._get_work()

        self.status = ACTIVE

        self.runs_tree[self.run_id] =  self._pods_book


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
                                            name='ChiCaaSWatcher')
        self.wait_thread.daemon = True
        if not self.asynchronous and not self.wait_thread.is_alive():
            self.wait_thread.start()

        while not self._terminate.is_set():
            now = time.time()  # time of last submission
            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < max_bulk_time:
                try:
                    task = self.incoming_q.get(block=True,
                                               timeout=min_bulk_time)
                except queue.Empty:
                        task = None
                
                if task:
                        bulk.append(task)
                
                if len(bulk) >= max_bulk_size:
                        break

            if bulk:
                with self._task_lock:
                    self.submit(bulk)

            bulk = list()


    # --------------------------------------------------------------------------
    #
    def create_op_client(self, cred):
        """
        1-user must create an application credentials for each site here:
        https://auth.chameleoncloud.org/auth/realms/chameleon/account
        
        2- The user must download the cred.sc file
        and source it: source ~/cred.sc
        """
        jet2_client = openstack.connect(**cred)
        
        return jet2_client


    # --------------------------------------------------------------------------
    #
    def _lease_resources(self, lease_id=None, reservations=None, lease_node_type=None,
                                                                 duration=0, nodes=0):
        
        res_lease = None
        if lease_id:
            self.logger.trace('using user provided lease')
            res_lease = lease.get_lease(lease_id)
            return res_lease

        if not lease_node_type:
            lease_node_type = "compute_skylake"
        
        if not reservations:
            reservations = []
        
        if not duration:
            duration = 1

        if not nodes:
            nodes = 1

        lease_name = 'hydraa-lease-{0}'.format(self.run_id)
        try:
            self.logger.trace("Creating lease...")

            # add floating ip reservation
            # FIXME: we might need more than one
            lease.add_fip_reservation(reservations, count=1)

            # add nodes to the resveration count and type
            lease.add_node_reservation(reservations, node_type=lease_node_type,
                                       count=nodes)

            # specify the duration of the lease default=1
            start_date, end_date = lease.lease_duration(hours=duration)

            # create the lease
            res_lease = lease.create_lease(lease_name, reservations,
                                           start_date=start_date, end_date=end_date)

            self.logger.trace('waiting for the resources to become ACTIVE')
            # wait for lease to become active
            lease.wait_for_active(res_lease['id'])
            self.logger.trace('resource {0} is active'.format(res_lease['id']))

        except keystoneauth1.exceptions.http.Unauthorized:
            raise Exception("Unauthorized.\nDid set your project name and site?")

        except blazarclient.exception.BlazarClientException as e:
            raise Exception(f"There is an issue making the reservation: {0}.".format(e))
        except Exception as e:
            raise Exception(e)

        if not res_lease:
            return

        return res_lease


    # --------------------------------------------------------------------------
    #
    def create_security_with_rule(self, vm):

            # we are using the default security group for now
            security_group_name = 'default'

            if vm.LaunchType == 'KVM':
                security = self.client.get_security_group('Allow SSH')
                vm.Rules = [security_group_name, security.name]
                return security
            
            if vm.LaunchType == 'Baremetal':
                security = self.client.get_security_group(security_group_name)
                vm.Rules = [security.name]
                return security


    # --------------------------------------------------------------------------
    #
    def assign_servers_vms(self):
        """
        exapnd on each vm.Min and vm.Max to get the actual
        number of servers and assign the servers to each VM.
        """
        while True:
            if all(s.access_ipv4 for s in self.list_servers()):
                # all vms has ips assigned to them
                servers = self.list_servers()
                break

        for vm in self.vms:
            vm.Servers = []
            for server in servers:
                if server.flavor.id == vm.FlavorId:
                    vm.Servers.append(server)
                    public_ip = server.access_ipv4
                    server.remote = ssh.Remote(vm.KeyPair, CHI_USER, public_ip,
                                               self.logger)

    # --------------------------------------------------------------------------
    #
    def create_servers(self, user_lease=None):

        server_name = 'hydraa-chi-{0}'.format(self.run_id)
        number_of_servers = sum([vm.MinCount for vm in self.vms])

        for vm in self.vms:
            image = self.create_or_find_image(vm)
            flavor = self.client.compute.find_flavor(vm.FlavorId)
            security = self.create_security_with_rule(vm)

            if self.launch_type == 'KVM':
                self.logger.trace('creating {0} x [{1}] [{2}]'.format(server_name,
                                                                      vm.MinCount,
                                                                      vm.FlavorId))
                instance = self.client.create_server(name=server_name,
                                                     image=image.id,
                                                     flavor=flavor.id,
                                                     key_name=self.keypair.name,
                                                     min_count=vm.MinCount,
                                                     max_count=vm.MaxCount)

                # Wait for a server to reach ACTIVE status.
                self.client.wait_for_server(instance)

            if self.launch_type == 'Baremetal':
                # Launch your compute node instances
                if not user_lease or not security:
                    raise Exception('baremetal requires both active lease and security group')

                lease_id = lease.get_node_reservation(user_lease["id"])
                instance = server.create_server(server_name, 
                                                reservation_id=lease_id,
                                                image_name=image, count=1,
                                                key_name=self.keypair.name,
                                                min_count=vm.MinCount,
                                                max_count=vm.MaxCount)

                # It will take approximately 10 minutes for the bare metal
                # node to be successfully provisioned.
                self.logger.trace('waiting for the instance to become ACTIVE')
                server.wait_for_active(instance.id)

            if not security == 'default':
                self.client.add_server_security_groups(instance, [security.name])

        self.assign_servers_ips()
        self.assign_servers_ssh()
        self.assign_servers_vms()

        self.logger.trace('all servers(s) are active x [{0}]'.format(number_of_servers))

        return self.list_servers()


    # --------------------------------------------------------------------------
    #
    def create_or_find_keypair(self):

        keys = None
        keypair = None
        for vm in self.vms:
            if vm.KeyPair:
                if isinstance(vm.KeyPair, list):
                    if len(vm.KeyPair) == 2:
                        self.logger.trace('using a user provided ssh keypair')
                        keys = vm.KeyPair
                        keypair = self.client.compute.find_keypair(vm.KeyPair[0].split('/')[-1:])
                        break

        if not keypair:
            key_name = 'id_rsa_{0}'.format(self.run_id.replace('.', '-'))
            keypair  = self.client.create_keypair(name=key_name)

        if not keypair:
            raise Exception('keypair creation failed')

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

        # modify the permission
        os.chmod(keypair_pri, 0o600)
        os.chmod(keypair_pub, 0o644)

        keys = [keypair_pri, keypair_pub]

        for vm in self.vms:
            vm.KeyPair = keys

        self.logger.trace("ssh keypair is created for all servers: [{0}]".format(key_name))

        return keypair


    # --------------------------------------------------------------------------
    #
    def create_or_find_image(self, vm):
        
        image = self.client.compute.find_image(vm.ImageId)

        if not image.id:
            raise NotImplementedError

        return image


    # --------------------------------------------------------------------------
    #
    def list_servers(self):
        servers = self.client.list_servers()

        hydraa_servers = []
        for server in servers:
            # make sure to get only vms from the current run
            if self.run_id in server.name:
                hydraa_servers.append(server)

        return hydraa_servers


    # --------------------------------------------------------------------------
    #
    def assign_servers_ips(self):
        for server in self.list_servers():
            # if the server has an ip assigned then skip it
            if not server.access_ipv4:
                self.client.add_auto_ip(server)
                self.logger.trace('auto assigned ip {0} to vm {1}'.format(server.access_ipv4, server.name))
            else:
                self.logger.trace('vm {0} already has an ip assigned'.format(server.name, server.access_ipv4))


    # --------------------------------------------------------------------------
    #
    def assign_servers_ssh(self):
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
    def submit(self, ctasks):
        """
        submit a single pod per batch of tasks
        """
        self.profiler.prof('submit_batch_start', uid=self.run_id)
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = CHI
            ctask.launch_type = self.launch_type

            self._tasks_book[str(ctask.name)] = ctask
            self._task_id +=1

        # submit to kubernets cluster
        self.cluster.submit(ctasks)

        self.logger.trace('batch of [{0}] tasks is submitted'.format(len(ctasks))) 

        self.profiler.prof('submit_batch_stop', uid=self.run_id)


    # --------------------------------------------------------------------------
    #
    def _wait_tasks(self):

        msg = None
        finshed = []
        failed, done, running = 0, 0, 0

        queue = self.cluster.result_queue

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

                        msg = f'Task: "{task.name}" from pod "{parent_pod}" is in state: "{status}"'

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
                            termination_msg = (0, JET2)
                            self.outgoing_q.put(termination_msg)

            except Empty:
                time.sleep(0.1)
                continue


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

        self.manager_id = None
        self.status = False

        self.client   = None
        self.network  = None
        self.security = None
        self.servers  = None

        self._pods_book.clear()
        self.logger.trace('internal cleanup is done')

    # --------------------------------------------------------------------------
    #
    def shutdown(self):

        if not self.servers:
            return
        
        self.logger.trace("termination started")

        self._terminate.set()

        if self.cluster:
            self.cluster.shutdown()

        if self.keypair:
            self.logger.trace('deleting ssh keys')
            if self.keypair.id:
                self.logger.trace('deleting key-name from cloud storage')
                self.client.delete_keypair(self.keypair.id)

        for server in self.servers:
            self.client.delete_server(server.name)
            self.logger.trace('server {0} is deleted'.format(server.name))
            self.client.delete_floating_ip(server.access_ipv4)
            self.logger.trace('floating ip [{0}] is deleted'.format(server.access_ipv4))

        if self.lease:
            msg = "would you like to delete the lease? yes/no:"
            user_in = input(msg)
            if user_in == 'Yes' or user_in == 'yes':
                lease.delete(self.lease['id'])
            else:
                pass

        self.__cleanup()
