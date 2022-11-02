import os
import chi
import json
import time
import uuid
import errno
import shutil
import socket
import atexit
import openstack
import chi.lease
import chi.server
import keystoneauth1, blazarclient

from chi import lease
from chi import server
from chi.ssh import Remote

from pathlib import Path
from openstack.cloud import exc

from collections import OrderedDict
from hydraa.services.caas_manager.utils import ssh
from hydraa.services.caas_manager.utils import kubernetes


HOME   = str(Path.home())
CHI    = 'chameleon'
ACTIVE = True

# --------------------------------------------------------------------------
#
class ChiCaas:
    """Represents a collection of clusters (resources) with a collection of
       services, tasks and instances.:
       :param cred: CHI credentials (source ~/chi_cred.sh)

       :pram asynchronous: wait for the tasks to finish or run in the
                           background.
       :param DryRun: Do a dryrun first to verify permissions.
    """

    def __init__(self, manager_id, cred, asynchronous, DryRun=False):
        
        self.manager_id = manager_id

        self.vm     = None
        self.status = False
        self.DryRun = DryRun

        self.client   = self._create_client(cred)
        self.lease    = None
        self.network  = None
        self.security = None
        self.server   = None
        self.ip       = None 
        self.remote   = None
        self.cluster  = None
        self.keypair  = None

        self.run_id   = None
        self._task_id = 0
    

        self._tasks_book  = OrderedDict()
        self._family_ids  = OrderedDict()
        self.launch_type  = None
        self._run_cost    = 0
        self.runs_tree    = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous
        atexit.register(self._shutdown)



    def run(self, VM, tasks, service=False, budget=0, time=0):
        self.vm          = VM
        self.launch_type = VM.LaunchType

        if not self.launch_type:
            raise Exception('CHI requires to specify KVM or Baremetal')

        self.run_id      = str(uuid.uuid4())
        self.status      = ACTIVE
        print("starting run {0}".format(self.run_id))
        
        self.image   = self.create_or_find_image()
        self.flavor  = self.client.compute.find_flavor(VM.FlavorId)
        self.keypair = self.create_or_find_keypair()
        self.security = self._create_security_with_rule()

        if self.launch_type == 'KVM':
            chi.use_site('KVM@TACC')
            self.server   = self._create_server(self.image, self.flavor, self.keypair,
                                                                        self.security)

        elif self.launch_type == 'Baremetal':
            chi.use_site('CHI@TACC')

            msg = "would you like to use an existing lease? if so, please provide it:"

            user_in = input (msg)

            if user_in == 'no' or user_in == 'No':
                self.lease  = self._lease_resources()
            else:
                self.lease  = self._lease_resources(lease_id=str(user_in))
            self.server   = self._create_server(self.image, self.flavor, self.keypair,
                                                            self.lease, self.security)

        self.ip      = self.create_and_assign_floating_ip()
        self.remote  = ssh.Remote(self.vm.KeyPair, 'cc', self.ip)
        self.cluster = kubernetes.Cluster(self.run_id, self.remote)
        
        self.cluster.bootstrap_local()

        self.submit(tasks)


    def _create_client(self, cred):
        """
        1-user must create an application credentials for each site here:
        https://auth.chameleoncloud.org/auth/realms/chameleon/account
        
        2- The user must download the cred.sc file
        and source it: source ~/cred.sc
        """
        jet2_client = openstack.connect(**cred)
        
        return jet2_client
    

    def _lease_resources(self, lease_id=None, reservations=None, lease_node_type=None,
                                                                 duration=0, nodes=0):
        
        res_lease = None
        if lease_id:
            print('using user provided lease')
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
            print("Creating lease...")

            # add floating ip reservation
            # FIXME: we might need more than one
            lease.add_fip_reservation(reservations, count=1)

            # add nodes to the resveration count and type
            lease.add_node_reservation(reservations, node_type=lease_node_type, count=nodes)

            # specify the duration of the lease default=1
            start_date, end_date = lease.lease_duration(hours=duration)

            # create the lease
            res_lease = lease.create_lease(lease_name, reservations, start_date=start_date, end_date=end_date)

            print('waiting for the resources to become ACTIVE')
            # wait for lease to become active
            lease.wait_for_active(res_lease['id'])
            print('resource {0} is active'.format(res_lease['id']))

        except keystoneauth1.exceptions.http.Unauthorized as e:
            raise Exception("Unauthorized.\nDid set your project name and site?")

        except blazarclient.exception.BlazarClientException as e:
            raise Exception(f"There is an issue making the reservation: {0}.".format(e))
        except Exception as e:
            raise Exception(e)

        if not res_lease:
            return

        return res_lease


    def _create_security_with_rule(self):

            # we are using the default security group for now
            security_group_name = 'default'

            if self.launch_type == 'KVM':
                security = self.client.get_security_group('Allow SSH')
                self.vm.Rules = [security_group_name, security.name]
                return security
            
            if self.launch_type == 'Baremetal':
                security = self.client.get_security_group(security_group_name)
                self.vm.Rules = [security.name]
                return security



    def _create_server(self, image, flavor, keypair, security, user_lease=None):

        if self.vm.VmId:
            # we assume that the instance has a keypair
            print('using user provided instance')
            instance = server.get_server(self.vm.VmId)
            return instance

        server_name = 'hydraa-chi-{0}'.format(self.run_id)
        instance    = None
        print('creating {0}'.format(server_name))
        if self.launch_type == 'KVM':
            import chi.server

            instance = self.client.create_server(name=server_name, 
                                                 image=image, 
                                                 flavor=flavor,
                                                 key_name=keypair.name)

            self.client.wait_for_server(instance)

        if self.launch_type == 'Baremetal':
            # Launch your compute node instances
            if not user_lease or not security:
                raise Exception('baremetal requires both active lease and security group')

            lease_id    = lease.get_node_reservation(user_lease["id"])
            instance    = server.create_server(server_name, 
                                               reservation_id=lease_id,
                                               image_name=image, count=1,
                                               key_name=keypair.name)


            # It will take approximately 10 minutes for the bare metal
            #  node to be successfully provisioned.
            print('waiting for the instance to become ACTIVE')
            server.wait_for_active(instance.id)

        if not security == 'default':
            self.client.add_server_security_groups(instance, [security.name])

        print('instance is ACTIVE')
        return instance



    def create_or_find_keypair(self):
        keypair = None
        if self.vm.KeyPair:
            print('Checking user provided ssh keypair')
            keypair = self.client.compute.find_keypair(self.vm.KeyPair)
        
        if not keypair: 
            print("creating ssh key Pair")
            key_name = 'id_rsa'
            keypair  = self.client.create_keypair(name=key_name)

            # FIXME: move this to utils
            work_dir_path    = '{0}/hydraa.sandbox.{1}'.format(HOME, self.run_id)
            ssh_dir_path     = '{0}/.ssh'.format(work_dir_path)

            os.mkdir(work_dir_path, 0o777)
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


    def create_or_find_image(self):
        
        image = self.client.compute.find_image(self.vm.ImageId)

        if not image.id:
            raise NotImplementedError

        return image


    def create_and_assign_floating_ip(self):

        try:
            # the user provided a running instance
            # so let's extract the IP from it
            if self.vm.VmId:
                addresses = self.server.addresses.get('sharednet1', None)
                if addresses:
                    for address in addresses:
                        ip_type = address.get('OS-EXT-IPS:type')
                        if ip_type == 'floating':
                            ip = address.get('addr')
                            return ip
            # let's create a public ip
            else:
                # FIXME: some error about an ip from the floating ip list
                # that can not be added.
                try:
                    ip = self.client.create_floating_ip()
                    self.client.add_ip_list(self.server, [ip.floating_ip_address])
                    return ip.floating_ip_address
                except exc.exceptions.ConflictException:
                    print('can not assign ip (machine already has a public ip)')
                    assigned_ips = self.client.list_floating_ips()
                    for assigned_ip in assigned_ips:
                        if assigned_ip.status == 'ACTIVE':
                            attached_ip = assigned_ip.name
                            return attached_ip

        except exc.exceptions.BadRequestException as e:
            raise Exception(e)


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


    def submit(self, ctasks):
        """
        submit a single pod per batch of tasks
        """
        
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = CHI
            ctask.launch_type = self.launch_type
            self._task_id +=1

        # generate a json file with the pod setup
        pod, pod_id = self.cluster.generate_pod(ctasks)

        # submit to kubernets cluster
        self.cluster.submit_pod(pod)

        # watch the pod in the cluster
        self.cluster.watch(pod_id)


    def _shutdown(self):

        if self.status == False:
            return

        if self.vm.KeyPair:
            print('deleting ssh keys')
            for k in self.vm.KeyPair:
                try:
                    os.remove(k)
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise e

            if self.keypair.id:
                print('deleting key-name from cloud storage')
                self.client.delete_keypair(self.keypair.id)

        try:
            if self.server:
                print('deleteing server {0}'.format(self.server.id))
                self.client.delete_server(self.server.id)

                if self.ip:
                    print('deleting allocated ip')
                    self.client.delete_floating_ip(self.ip)

                if self.lease:
                    msg = "would you like to delete the lease? yes/no:"
                    user_in = input(msg)
                    if user_in == 'Yes' or user_in == 'yes':
                        lease.delete(self.lease['id'])
                    else:
                        pass

        except Exception as e:
            raise Exception(e)

            
