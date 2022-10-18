import os
import sys
import copy
import math
import time
import uuid
import errno
import atexit
import openstack

from collections import OrderedDict


JET2      = 'jetstream2'
ACTIVE    = True
WAIT_TIME = 2
KEYPAIR_NAME = 'id_rsa'
PRIVATE_KEYPAIR_FILE = 'jet_ssh'

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


        self.run_id   = None
        self._task_id = 0      

        # tasks_book is a datastructure that keeps most of the 
        # cloud tasks info during the current run.
        self._tasks_book   = OrderedDict()
        self._family_ids   = OrderedDict()

        self.launch_type  =  None

        self._run_cost     = 0

        self.runs_tree = OrderedDict()

        # wait or do not wait for the tasks to finish 
        self.asynchronous = asynchronous

        atexit.register(self._shutdown)
    

    def run(self, VM, tasks, service=False, budget=0, time=0):
        if self.status:
            pass
            #self.__cleanup()

        self.vm          = VM
        self.launch_type = VM.LaunchType

        self.status      = ACTIVE
        self.run_id      = str(uuid.uuid4())

        print("starting run {0}".format(self.run_id))

        self.security = self.create_or_find_security_group()
        self.network  = self.create_or_find_network(self.security)
        self.key_pair = self.create_or_find_keypair()
        self.image    = self.create_or_find_image()

        # FIXME: we might need to wrap this with a try/except
        flavor   = self.client.compute.find_flavor(self.launch_type)

        self.server = self._start_server(self.image, flavor, self.key_pair,
                                               self.network, self.security)
        
        # wait for the server to become active
        self.client.compute.wait_for_server(self.server)

        print('{0} is started'.format(self.server.id))

    
    def _create_client(self, cred):
        jet2_client = openstack.connect(**cred)
        
        return jet2_client
    

    def create_or_find_security_group(self):

        security = self.client.network.find_security_group(self.vm.SecurityGroups)

        if not security.id:
            #security_id = 'hydraa-security-group={0}'.format(self.run_id)
            security    = self.client.network.create_security_group()
        
        return security


    def create_or_find_keypair(self):

        keypair = self.client.compute.find_keypair(self.vm.KeyPair)

        if not keypair:
            print("creating ssh key Pair")

            keypair = self.client.compute.create_keypair(name=self.vm.KeyPair)

            try:
                os.mkdir('.')
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise e

            with open(PRIVATE_KEYPAIR_FILE, 'w') as f:
                f.write("%s" % keypair.private_key)

            os.chmod(PRIVATE_KEYPAIR_FILE, 0o400)

        return keypair
    


    def create_or_find_image(self):
        
        image = self.client.compute.find_image(self.vm.ImageId)

        if not image.id:
            raise NotImplementedError

        return image


    def create_or_find_network(self, security):

        # user provided a newtwork name that we need to find
        if self.vm.Network:
            network = self.client.network.find_network(self.vm.Network)
            if network.id:
                return network
        
        # if we could not find it, then let's create a network with
        # subnet
        else:
            network_name = 'hydraa-newtwork-{0}'.format(self.run_id)
            subnet_name  = 'hydraa-subnet-{0}'.format(self.run_id)

            network = self.client.network.create_network(name=network_name)

            # add a subnet for the network
            self.client.network.create_subnet(name=subnet_name,
                                              network_id=network.id,
                                              ip_version='4',
                                              cidr='10.0.2.0/24',
                                              gateway_ip='10.0.2.1')    

            print(network)
            return network
    

    def _start_server(self, image, flavor, key_pair, networks, security):

        server_name = 'hydraa_Server-{0}'.format(self.run_id)
        server = self.client.compute.create_server(name=server_name,
                                                         image_id=image.id,
                                                         flavor_id=flavor.id,
                                                         key_name=key_pair.name,
                                                         networks=[{"uuid": networks.id}])
        
        print('creating {0}'.format(server_name))
        return server



    def _shutdown(self):

        if not self.network.id and not self.server.id:
            return

        # delete the keypair
        keypair = self.client.compute.find_keypair(KEYPAIR_NAME)

        try:
            os.remove(PRIVATE_KEYPAIR_FILE)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

        self.client.compute.delete_keypair(keypair)

        # deleting security groups
        # FIXME: without try/except it will raise ConflictException 
        # "Insufficient rights for removing default security group."
        # FIXME: delete only security groups that we created
        try:
            for sec in self.client.network.security_groups():
                self.client.network.delete_security_group(sec.id)
        except:
            pass
        
        # delete all subnet
        for subnet in self.network.subnet_ids:
            self.client.network.delete_subnet(subnet, ignore_missing=False)
        
        # delete the networks
        self.client.network.delete_network(self.network, ignore_missing=False)

        
        # deleting the server
        self.client.compute.delete_server(self.server)

        print('shutting down')

