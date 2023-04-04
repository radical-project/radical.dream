import os
import chi
import sys
import time
import uuid
import queue
import errno
import atexit
import threading
import openstack
import chi.lease
import chi.server
import keystoneauth1, blazarclient

from chi import lease
from chi import server

from collections import OrderedDict
from hydraa.services.caas_manager.utils import ssh
from hydraa.services.caas_manager.utils import misc
from hydraa.services.caas_manager.utils import kubernetes


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

CHI    = 'chameleon'
ACTIVE = True


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

    def __init__(self, sandbox, manager_id, cred, VM, asynchronous,
                                          log, prof, DryRun=False):
        
        self.manager_id = manager_id

        self.vm     = None
        self.status = False
        self.DryRun = DryRun

        self.client   = self._create_client(cred)
        self.lease    = None
        self.network  = None
        self.security = None
        self.server   = None
        self.ips      = [] 
        self.remote   = None
        self.cluster  = None
        self.keypair  = None

        self._task_id = 0
    
        self.vm     = VM
        self.run_id = '{0}.{1}'.format(self.vm.LaunchType, str(uuid.uuid4()))
    
        self._tasks_book  = OrderedDict()
        self._pods_book   = OrderedDict()

        self._run_cost    = 0
        self.runs_tree    = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        self.sandbox  = '{0}/{1}.{2}'.format(sandbox, CHI, self.run_id)
        os.mkdir(self.sandbox, 0o777)

        self.logger   = log
        self.profiler = prof(name=__name__, path=self.sandbox)

        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()

        self._terminate = threading.Event()

        self.start_thread = threading.Thread(target=self.start, name='ChiCaaS')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        atexit.register(self._shutdown)


    # --------------------------------------------------------------------------
    #
    def start(self):

        if not self.vm.LaunchType:
            raise Exception('CHI requires to specify KVM or Baremetal')

        self.status      = ACTIVE
        print("starting run {0}".format(self.run_id))

        self.profiler.prof('prep_start', uid=self.run_id)

        self.image    = self.create_or_find_image()
        self.flavor   = self.client.compute.find_flavor(self.vm.FlavorId)
        self.keypair  = self.create_or_find_keypair()
        self.security = self._create_security_with_rule()

        self.profiler.prof('prep_stop', uid=self.run_id)
        
        if self.vm.LaunchType == 'KVM':
            chi.use_site('KVM@TACC')
            self.profiler.prof('server_create_start', uid=self.run_id)
            self.server   = self._create_server(self.image, self.flavor, self.keypair,
                                    self.security, self.vm.MinCount, self.vm.MaxCount)
            self.profiler.prof('server_create_stop', uid=self.run_id)

        elif self.vm.LaunchType == 'Baremetal':
            chi.use_site('CHI@TACC')

            msg = "would you like to use an existing lease? if so, please provide it:"

            user_in = input (msg)

            if user_in == 'no' or user_in == 'No':
                self.lease  = self._lease_resources()
            else:
                self.lease  = self._lease_resources(lease_id=str(user_in))
            self.server   = self._create_server(self.image, self.flavor, self.keypair,
                        self.security, self.vm.MinCount, self.vm.MaxCount, self.lease)

        self.profiler.prof('ip_create_start', uid=self.run_id)

        self.assign_ips()

        self.profiler.prof('ip_create_stop', uid=self.run_id)

        self.assign_ssh_security_groups()

        self.vm.Servers = self.list_servers()

        self.vm.Remotes = {}
        for server in self.vm.Servers:
            public_ip = server.access_ipv4
            self.vm.Remotes[server.name] = ssh.Remote(self.vm.KeyPair, 'cc', public_ip,
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

        self.wait_thread = threading.Thread(target=self._wait_tasks, name='ChiCaaSWatcher')
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
            lease.add_node_reservation(reservations, node_type=lease_node_type, count=nodes)

            # specify the duration of the lease default=1
            start_date, end_date = lease.lease_duration(hours=duration)

            # create the lease
            res_lease = lease.create_lease(lease_name, reservations, start_date=start_date, end_date=end_date)

            self.logger.trace('waiting for the resources to become ACTIVE')
            # wait for lease to become active
            lease.wait_for_active(res_lease['id'])
            self.logger.trace('resource {0} is active'.format(res_lease['id']))

        except keystoneauth1.exceptions.http.Unauthorized as e:
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
    def _create_security_with_rule(self):

            # we are using the default security group for now
            security_group_name = 'default'

            if self.vm.LaunchType == 'KVM':
                security = self.client.get_security_group('Allow SSH')
                self.vm.Rules = [security_group_name, security.name]
                return security
            
            if self.vm.LaunchType == 'Baremetal':
                security = self.client.get_security_group(security_group_name)
                self.vm.Rules = [security.name]
                return security


    # --------------------------------------------------------------------------
    #
    def _create_server(self, image, flavor, keypair, security, min_count, max_count,
                                                                   user_lease=None):

        if self.vm.VmId:
            # we assume that the instance has a keypair
            self.logger.trace('using user provided instance')
            instance = server.get_server(self.vm.VmId)
            return instance

        server_name = 'hydraa-chi-{0}'.format(self.run_id)
        instance    = None
        self.logger.trace('creating {0}'.format(server_name))
        if self.vm.LaunchType == 'KVM':
            instance = self.client.create_server(name=server_name, 
                                                 image=image, 
                                                 flavor=flavor,
                                                 key_name=keypair.name,
                                                 min_count=min_count,
                                                 max_count=max_count)

            # Wait for a server to reach ACTIVE status.
            self.client.wait_for_server(instance, timeout=600)

        if self.vm.LaunchType == 'Baremetal':
            # Launch your compute node instances
            if not user_lease or not security:
                raise Exception('baremetal requires both active lease and security group')

            lease_id    = lease.get_node_reservation(user_lease["id"])
            instance    = server.create_server(server_name, 
                                               reservation_id=lease_id,
                                               image_name=image, count=1,
                                               key_name=keypair.name,
                                               min_count=min_count,
                                               max_count=max_count)


            # It will take approximately 10 minutes for the bare metal
            #  node to be successfully provisioned.
            self.logger.trace('waiting for the instance to become ACTIVE')
            server.wait_for_active(instance.id)

        if not security == 'default':
            self.client.add_server_security_groups(instance, [security.name])

        self.logger.trace('instance is ACTIVE')
        return instance


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

            ssh_dir_path     = '{0}/.ssh'.format(self.sandbox)

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
    def moniter_cpu_usage(self):
        if self.remote:
            cmd = "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}'"
            try:
                while True:
                    cpu_usage = self.remote.run(cmd, hide=True)
                    print(cpu_usage)
                    time.sleep(4)
            except KeyboardInterrupt:
                return


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
            ctask.launch_type = self.vm.LaunchType

            self._tasks_book[str(ctask.id)] = ctask
            self.logger.trace('submitting tasks {0}'.format(ctask.id))

            self._task_id +=1

        # submit to kubernets cluster
        depolyment_file, pods_names, batches = self.cluster.submit(ctasks)

        self.profiler.prof('submit_batch_stop', uid=self.run_id)

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

        try:
            if self.server:
                for server in self.list_servers():
                    self.client.delete_server(server.name)
                    self.logger.trace('server {0} is deleted'.format(server.name))

                if self.ips:
                    for ip in self.ips:
                        self.logger.trace('deleting allocated ip')
                        self.client.delete_floating_ip(self.ip)

                if self.lease:
                    msg = "would you like to delete the lease? yes/no:"
                    user_in = input(msg)
                    if user_in == 'Yes' or user_in == 'yes':
                        lease.delete(self.lease['id'])
                    else:
                        pass
        
            self.__cleanup()

        except Exception as e:
            raise Exception(e)

            
