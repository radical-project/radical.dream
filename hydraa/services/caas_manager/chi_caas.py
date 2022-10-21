import chi
import time
import socket
import chi.lease
import chi.server

from chi import ssh
from chi.ssh import Remote


WORK_DIR = '$HOME/HYDRAA'


class ChiCaas:
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

        self.run_id   = None

        chi.set('project_name', 'CHI-221047')
        chi.use_site('CHI@TACC')



    def run(self, VM, tasks, service=False, budget=0, time=0):
        self.vm          = VM
        self.status      = ACTIVE
        self.run_id      = str(uuid.uuid4())
        self.launch_type = VM.LaunchType

        self.lease  = self._lease_resources()
        self.server = self._create_server(self.lease)
        self.ips    = self._create_and_assign_floating_ip(self.server)
        self.remote = self.



    def _lease_resources(self,):
        # Reserve 1 compute nodes
        node_list = []

        # for now we only allow for 1 lease per time
        chi.lease.add_node_reservation(node_list, count=1)
        lease_name = 'hydraa-lease-{0}'.format(self.run_id)
        lease = chi.lease.create_lease(lease_name, reservations=node_list)
        
        print('waiting for the resources to become ACTIVE')
        # wait for lease to become active
        chi.lease.wait_for_active(lease['id'])  # Ensure lease has started

        print('resource {0} is active'.format(lease.id))

        return lease



    def _create_server(self, lease):
        # Launch your compute node instances
        lease_id    = chi.lease.get_node_reservation(lease["id"])
        server_name = 'hydraa-chi-{0}'.format(self.run_id)
        server      = chi.server.create_server(server_name, reservation_id=lease_id,
                                                   image_name=self.vm.ImageId, count=1)
        
        print('waiting for the instance to become ACTIVE')
        chi.server.wait_for_active(server['id'])
        print('instance {0} is active'.format(server.id))

        return server



    def _create_and_assign_floating_ip(self, server, ips=[]):

        if ips:
            ip_list = []
            for ip in ips:
                ip = chi.server.associate_floating_ip(server.id)
                ip_list.append(ip)
            
            return ip_list
        
        else:
            ip = chi.server.associate_floating_ip(server.id)
            return [ip]



    def open_remote_connection(self, ip):
        try:
            if self.ips:
                if len(ips) >=2:
                    raise ('can not create remote connection with more than one IP')
                else:
                    remote = Remote(ip[0])
                    return remote
        except:
            raise('please make sure you have a rule for SSH connection with: Egreee/IPv4/TCP/22')



    def moniter_cpu_usage(self):
        if self.remote:
            cmd = "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}'"
            try:
                while True:
                    cpu_usage = self.remote.run(cmd, hide=True)
                    print(cpu_usage)
                    sleep(4)
            except KeyboardInterrupt:
                return
    

    def check_ssh_connection(self):
        
        print(f"Waiting for SSH connectivity on {self.ip[0]} ...")
        timeout = 60*2

        # Repeatedly try to connect via SSH.
        while True:
            try:
                with socket.create_connection((floating_ip, 22), timeout=timeout):
                    print("Connection successful")
                    break
            except OSError as ex:
                time.sleep(10)
                if time.perf_counter() - start_time >= timeout:
                    print(f"After {timeout} seconds, could not connect via SSH. Please try again.")
    

    def __bootstrap(self):

        """
        deploy kubernetes cluster K8s on chi
        via Ansible.
        """
        with self.remote(self.ips[0]) as conn:
            # Upload the script
            conn.put("chi_deploy_kuberentes.sh")
            conn.put("chi_deploy_kuberentes.yaml")

            # Run the script
            conn.run("bash chi_deploy_kuberentes.sh")

        # verify that the bootstraping went well
        self.__verify()


    def __verify(self):
        
        # FIXME: for now we verify it by eyeballing it
        # we need to match the output of these commands
        # with whatever setting we have.
        with self.remote(self.ips[0]) as conn:
            # Checking the deployments in the kubernetes cluster in "ata-namespace" namespace
            check1 = conn.run("kubectl get deployments -n ata-namespace")
            # Checking the pods in the kubernetes cluster in "ata-namespace" namespace
            check2 = conn.run("kubectl get pod -n ata-namespace")
            # Checking the namespace "ata-namespace" in the kubernetes cluster using grep command
            check3 = conn.run("kubectl get namespace | grep ata")
        
        print('{0}\n {1}\n {2}\n'.format(check1, check2, check3))
    

    
    def _build_pods_file(self):
        
        for c in self.tasks:
            # build the pod file 
            # from each task info
            # then upload it somehow to the
            # remote machine
            # then feed it to the main.yaml




        
            
