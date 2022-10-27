import os
import chi
import json
import time
import uuid
import socket
import chi.lease
import chi.server
import keystoneauth1, blazarclient

from chi import ssh
from chi import lease
from chi import server
from chi.ssh import Remote
from openstack.cloud import exc
from kubernetes import client, config

CHI    = 'chameleon'
ACTIVE = True

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

        self.run_id   = None
        self._task_id = 0
        self.launch_type  = None

        chi.set('project_name', 'CHI-221047')
        chi.set('project_domain_id', 'e6de2391926f42c6be4ebaa3d9ef3974')
        chi.use_site('CHI@TACC')



    def run(self, VM, tasks, service=False, budget=0, time=0):
        self.vm          = VM
        self.status      = ACTIVE
        self.run_id      = str(uuid.uuid4())
        
        self.launch_type = VM.LaunchType

        print("starting run {0}".format(self.run_id))
        msg = "would you like to use an existing lease? if so, please provide it:"
        user_in = input (msg)
        if user_in == 'no' or user_in == 'No':
            self.lease  = self._lease_resources()
        else:
            self.lease  = self._lease_resources(lease_id=str(user_in))
            
        self.server = self._create_server(self.lease)
        self.ip     = self._create_and_assign_floating_ip(self.server)
        self.remote = self.open_remote_connection(self.ip)
        self.submit(tasks)



    def _lease_resources(self, lease_id=None, reservations=None, lease_node_type=None, duration=0, nodes=0):
        
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
            print("Unauthorized.\nDid set your project name and and site?")
        except blazarclient.exception.BlazarClientException as e:
            print(f"There is an issue making the reservation. Check the calendar to make sure a {lease_node_type} node is available.")
            print("https://chi.uc.chameleoncloud.org/project/leases/calendar/host/")
            print(e)
        except Exception as e:
            print("An unexpected error happened.")
            print(e)

        if not res_lease:
            return

        return res_lease



    def _create_server(self, user_lease):

        if self.vm.VmId:
            print('using user provided instance')
            instance = server.get_server(self.vm.VmId)
            return instance
        
        key = self._create_keypair()

        # Launch your compute node instances
        lease_id    = lease.get_node_reservation(user_lease["id"])
        server_name = 'hydraa-chi-{0}'.format(self.run_id)
        instance    = server.create_server(server_name, reservation_id=lease_id,
                                            image_name=self.vm.ImageId, count=1,
                                            key_name=key)
        

        # It will take approximately 10 minutes for the bare metal
        #  node to be successfully provisioned.
        print('waiting for the instance to become ACTIVE')
        server.wait_for_active(instance.id)
        print('instance {0} is active'.format(instance.id))

        return instance
    

    def _create_keypair(self):
        key_name = 'hydraa-keypair-{0}'.format(self.run_id)
        os.system('openstack keypair create {0}>{1}'.format(key_name, key_name))
        os.system('chmod 600 {0}'.format(key_name))

        return key_name



    def _create_and_assign_floating_ip(self, server):

        try:
            if self.vm.VmId:
                addresses = server.addresses.get('sharednet1', None)
                if addresses:
                    for address in addresses:
                        ip_type = address.get('OS-EXT-IPS:type')
                        if ip_type == 'floating':
                            ip = address.get('addr')
                            return ip
            else:
                ip = chi.server.associate_floating_ip(server.id)
                return ip

        except exc.exceptions.BadRequestException as e:
            raise Exception(e)


    def open_remote_connection(self, ip):
        try:
            self.check_ssh_connection(ip)
            remote = Remote(ip)
            return remote
        except Exception as e:
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


    def check_ssh_connection(self, ip):
        
        print(f"Waiting for SSH connectivity on {ip} ...")
        timeout = 60*2
        start_time = time.perf_counter() 
        # Repeatedly try to connect via SSH.
        while True:
            try:
                with socket.create_connection((ip, 22), timeout=timeout):
                    print("Connection successful")
                    break
            except OSError as ex:
                time.sleep(10)
                if time.perf_counter() - start_time >= timeout:
                    print(f"After {timeout} seconds, could not connect via SSH. Please try again.")
    

    def _bootstrap_local_kb_cluster(self):

        """
        deploy kubernetes cluster K8s on chi
        """
        with self.remote as conn:
            print('booting K8s cluster on the remote machine')
            loc = "HYDRAA/hydraa/services/caas_manager/config/chi/deploy_kuberentes_local.sh"
            cwd = os.getcwd()
            file_path = '{0}/{1}'.format(cwd, loc)
            conn.put(file_path)
            conn.run("chmod +x deploy_kuberentes_local.sh")
            conn.run("./deploy_kuberentes_local.sh")

            print('booting successfull')

    
    def _build_pod_object(self, ctasks):

        pods_file  = 'pods.json'
        containers = []
        for ctask in ctasks:
            envs = []
            if ctask.env_var:
                for env in env_vars:
                    pod_env  = client.V1EnvVar(name = env[0], value = env[1])
                    envs.append(pod_env)

            pod_cpu = "{0}m".format(ctask.vcpus * 1000)
            pod_mem = "{0}Mi".format(ctask.memory)

            resources=client.V1ResourceRequirements(requests={"cpu": pod_cpu, "memory": pod_mem},
                                                      limits={"cpu": pod_cpu, "memory": pod_mem})

            pod_container = client.V1Container(name = ctask.name, image = ctask.image,
                        resources = resources, command = ctask.cmd, env = envs)
            
            containers.append(pod_container)
        
        pod_metadata  = client.V1ObjectMeta(name = "hydraa-pods")

        # check if we need to restart the task
        if ctask.restart:
            restart_policy = ctask.restart
        else:
            restart_policy = 'Never'

        pod_spec      = client.V1PodSpec(containers=containers,
                                 restart_policy=restart_policy)

        pods          = client.V1Pod(api_version="v1", kind="Pod",
                             metadata=pod_metadata, spec=pod_spec)
        
        with open(pods_file, 'w') as f:
            json.dump(client.ApiClient().sanitize_for_serialization(pods), f)
        
        return pods_file

    
    def submit(self, ctasks):
        for ctask in ctasks:
            ctask.run_id      = self.run_id
            ctask.id          = self._task_id
            ctask.name        = 'ctask-{0}'.format(self._task_id)
            ctask.provider    = CHI
            ctask.launch_type = self.launch_type
            self._task_id +=1

        pods = self._build_pod_object(ctasks)

        self._submit_to_kuberentes(pods)


    def _submit_to_kuberentes(self, pods):

        # upload the pods file before bootstrapping
        # FIXME: we get socket closed if we did it
        # in the reverse order, because we modify 
        # the firewall of the node

        # upload the pods.json
        self.remote.put(pods)
        
        # bootup the cluster K8s
        self._bootstrap_local_kb_cluster()

        # deploy the pods.json on the cluster
        self.remote.run('sudo microk8s kubectl apply -f {0}'.format(pods))

        #FIXME: create a monitering of the pods/containers
        
        return True


    def _shutdown(self):

        lease.delete(self.lease['id'])
            
