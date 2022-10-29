import os
import sys
import copy
import math
import time
import uuid
import errno
import atexit
import openstack

from pathlib import Path
from openstack.cloud import exc

from collections import OrderedDict
 
HOME      = str(Path.home())
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

    def __init__(self, manager_id, cred, asynchronous, DryRun=False):

        self.manager_id = manager_id

        self.vm     = None
        self.status = False
        self.DryRun = DryRun

        self.client   = self._create_client(cred)
        self.network  = None
        self.security = None
        self.server   = None
        self.ip       = None 

        self.run_id   = None
        self._task_id = 0      

        # tasks_book is a datastructure that keeps most of the 
        # cloud tasks info during the current run.
        self._tasks_book   = OrderedDict()
        self._family_ids   = OrderedDict()
        self.launch_type  =  None
        self._run_cost    = 0
        self.runs_tree = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        atexit.register(self._shutdown)
    

    def run(self, VM, tasks, service=False, budget=0, time=0):
        if self.status:
            pass
            self.__cleanup()

        self.vm          = VM
        self.status      = ACTIVE
        self.run_id      = str(uuid.uuid4())
        self.launch_type = VM.LaunchType

        print("starting run {0}".format(self.run_id))

        self.image    = self.create_or_find_image()
        self.flavor   = self.client.compute.find_flavor(self.launch_type)
        self.security = self.create_security_with_rule()
        self.keypair  = self.create_or_find_keypair()

        self.server = self._start_server(self.image, self.flavor,
                                         self.keypair, self.security)

        self.ip = self.create_and_assign_floating_ip()

        print("ssh -i {0} root@{1}".format(self.vm.KeyPair[0], self.ip))

        print('{0} is started'.format(self.server.id))

    
    def _create_client(self, cred):
        jet2_client = openstack.connect(**cred)
        
        return jet2_client
    

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
                print('creating ssh and ping rules')
                ssh_rule = self.client.create_security_group_rule(security.id,
                                                                  port_range_min=22,
                                                                  port_range_max=22,
                                                                  protocol='tcp',
                                                                  direction='ingress',
                                                                  remote_ip_prefix='0.0.0.0/0')
                
                ping_rule = self.client.create_security_group_rule(security.id,
                                                                   port_range_max=None,
                                                                   port_range_min=None,
                                                                   protocol='icmp',
                                                                   direction='ingress',
                                                                   remote_ip_prefix='0.0.0.0/0',
                                                                   ethertype='IPv4')
                self.vm.Rules = [ssh_rule.id, ping_rule.id]

        except Exception as e:
            # FIXME: check rules exist by id not by exceptions type
            if security_rule_exist in e.args:
                print('security rules exist')
                pass
            else:
                raise Exception(e)

        return security



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
            work_dir_path    = '{0}/hydraa_sandbox_{1}'.format(HOME, self.run_id)
            ssh_dir_path     = '{0}/.ssh'.format(work_dir_path)
            #auth_k_dir_path  = '{0}/authorized_keys'.format(ssh_dir_path)

            os.mkdir(work_dir_path, 0o777)
            os.mkdir(ssh_dir_path, 0o700)
            #os.mkdir(auth_k_dir_path, 0o644)

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


    def create_or_find_network(self):

        network = None
        # user provided a newtwork name that we need to find
        if self.vm.Network:
            network = self.client.network.find_network(self.vm.Network)
            if network and network.id:
                print('network {0} found'.format(network.name))
                return network
        
        
        network = self.client.network.find_network('auto_allocated_network')
        return network
        
        # if we could not find it, then let's create a network with
        # subnet
        '''
        else:
            network_name = 'hydraa-newtwork-{0}'.format(self.run_id)
            subnet_name  = 'hydraa-subnet-{0}'.format(self.run_id)

            print('creating network {0}'.format(network_name))
            network = self.client.network.create_network(name=network_name)

            # add a subnet for the network
            self.client.network.create_subnet(name=subnet_name,
                                              network_id=network.id,
                                              ip_version='4',
                                              cidr='10.0.2.0/24',
                                              gateway_ip='10.0.2.1')

            return network
        '''
    

    def create_and_assign_floating_ip(self):
        
        ip = self.client.create_floating_ip()
        # FIXME: some error about an ip from the floating ip list
        # that can not be added.
        try:
            self.client.add_ip_list(self.server, [ip.floating_ip_address])
        except exc.exceptions.ConflictException:
            pass

        return ip.floating_ip_address



    def _start_server(self, image, flavor, key_pair, security):

        server_name = 'hydraa_Server-{0}'.format(self.run_id)

        #user_data = self._build_bootstrap()

        print('creating {0}'.format(server_name))
        server = self.client.create_server(name=server_name,
                                           image=image.id,
                                           flavor=flavor.id,
                                           key_name=key_pair.name)
        
        # Wait for a server to reach ACTIVE status.
        self.client.wait_for_server(server)

        if not security.name == 'default':
            self.client.add_server_security_groups(server, [security.name])
        
        print('server is ACTIVE')
        
        return server
    


    def _build_bootstrap(self):

        boostrap_path = 'user_data.txt'
        bootstrap =  """
        #cloud-config
        users:
        - default
        - name: exouser
            shell: /bin/bash
            groups: sudo, admin
            sudo: ['ALL=(ALL) NOPASSWD:ALL']{ssh-authorized-keys}
        password: mypasswd
        ssh_pwauth: true
        chpasswd: {expire: False }
        package_update: true
        package_upgrade: {install-os-updates}
        packages:
        - python3-virtualenv
        - git{write-files}        
        """
        with open(boostrap_path,"a+") as f:
            f.write(bootstrap)

        return boostrap_path


    # --------------------------------------------------------------------------
    #
    def __cleanup(self):

        caller = sys._getframe().f_back.f_code.co_name
        self._task_id     = 0
        self.launch_type  = None
        self._run_cost    = 0
        self._tasks_book.clear()

        if caller == '_shutdown':
            self.manager_id = None
            self.status = False

            self.client   = None
            self.network  = None
            self.security = None
            self.server   = None

            self._family_ids.clear()
            print('done')


    def _shutdown(self):

        if not self.server:
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

        # delete all subnet
        if self.network:
            if not self.network.name == "auto_allocated_network":
                print('deleting subnets')
                for subnet in self.network.subnet_ids:
                    self.client.network.delete_subnet(subnet, ignore_missing=False)
        
        # deleting the server
        if self.server:
            print('deleting server')
            self.client.delete_server(self.server)

        # delete the networks
        # FIXME: unassigne and delete the public IP
        if self.network:
            print('deleting networks')
            if not self.network.name == "auto_allocated_network":
                while True:
                    try:
                        self.client.network.delete_network(self.network, ignore_missing=False)
                        break
                    except exc.exceptions.ConflictException:
                        time.sleep(0.1)
                print('network is deleted')

        # FIXME: delete only security groups that we created
        if self.security:
            return
            print('deleting security groups')
            for sec in self.client.list_security_groups():
                if not self.security.name == 'default':
                    self.client.delete_security_group(self.security.id)
            
            if self.vm.Rules:
                return
                print('deleting security rules')
                for rule in self.vm.Rules:
                    self.client.delete_security_group_rule(rule)
        
        print('shutting down')
        self.__cleanup()
