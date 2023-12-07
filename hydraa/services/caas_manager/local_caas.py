from .jet2_caas import Jet2CaaS
from openstack.compute.v2 import op_server
from openstack.compute.v2 import op_flavor


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'


# --------------------------------------------------------------------------
#
# FIXME: Create a BaseCaaS class and inherit from it
class LocalCaas(Jet2CaaS):
    def __init__(self, sandbox, manager_id, cred,
                 VMS, asynchronous, auto_terminate, log, prof):
        
        super().__init__(sandbox, manager_id, cred,
                         VMS, asynchronous, auto_terminate, log, prof)


    # --------------------------------------------------------------------------
    #
    def create_or_find_keypair(self):
        pass


    # --------------------------------------------------------------------------
    #
    def create_server(self, vm):
        vcpus = os.cpu_count()
        memory = psutil.virtual_memory().total
        for vm in self.VMS:
            vm.Servers = []
            flavor = op_flavor(vcpus=vcpus, ram=memory, disk=10)
            server = op_server.Server(status='ACTIVE',
                                      flavor=flavor,
                                      name='local-hydraa',
                                      access_ipv4='127.0.0.1',
                                      addresses={'fixed_ip': [{'addr': '127.0.0.1'}]},)

            server.remote = ssh.Remote(user=None,
                                       vm_keys=[],
                                       fip='127.0.0.1',
                                       log=self.logger, local=True)
            vm.Servers.append(server)
    

    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        self.cluster.shutdown()

