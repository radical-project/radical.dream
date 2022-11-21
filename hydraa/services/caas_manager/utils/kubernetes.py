import os
import time
import math
import copy
import json
import uuid
import atexit
import datetime

import pandas        as pd
import threading     as mt
import radical.utils as ru

from .misc                  import sh_callout
from functools              import reduce
from kubernetes             import client, config
from azure.cli.core         import get_default_cli
from kubernetes.client.rest import ApiException


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

TFORMAT = '%Y-%m-%dT%H:%M:%fZ'


# --------------------------------------------------------------------------
#
class Cluster:

    def __init__(self, run_id, remote, cluster_size, sandbox):
        
        self.id           = run_id
        self.remote       = remote
        self.pod_counter  = 0
        self.sandbox      = sandbox
        self.profiler     = ru.Profiler(name=__name__, path=self.sandbox)
        self.size         = cluster_size

    def start(self):
        self.remote.run('sudo microk8s start')
        return True

    def restart(self):
        self.stop()
        self.start()
    
    def delete_completed_pods(self):
        self.remote.run('kubectl delete pod --field-selector=status.phase==Succeeded')
        return True
    

    # --------------------------------------------------------------------------
    #
    def stop_background(self, stop_event, threads):
        """
        stop the background task gracefully before exiting
        """
        # request the background thread stop
        stop_event.set()
        # wait for the background thread to stop
        for thread in threads:
            if thread.is_alive():
                thread.join()
    

    def bootstrap_local(self):
        
        """
        deploy kubernetes cluster K8s on chi
        """
        print('Building MicroK8s cluster on the remote machine')

        self.profiler.prof('bootstrap_start', uid=self.id)

        loc = os.path.join(os.path.dirname(__file__)).split('utils')[0]
        boostrapper = "{0}config/deploy_kuberentes_local.sh".format(loc)
        self.remote.put(boostrapper)
        self.remote.run("chmod +x deploy_kuberentes_local.sh")

        # FIXME: for now we use snap to install microk8s and
        # we sometimes fail: https://bugs.launchpad.net/snapd/+bug/1826662
        # as a workaround we wait for snap to be loaded
        self.remote.run("sudo snap wait system seed.loaded")

        # upload the bootstrapper code to the remote machine
        self.remote.run("./deploy_kuberentes_local.sh")

        self.profiler.prof('bootstrap_stop', uid=self.id)

        self.profiler.prof('cluster_warmup_start', uid=self.id)
        while True:
            # wait for the microk8s to be ready
            stream = self.remote.run('sudo microk8s status --wait-ready')

            # check if the cluster is ready to submit the pod
            if "microk8s is running" in stream.stdout:
                print('Booting Kuberentes cluster successful')
                break
            else:
                print('Waiting for Kuberentes cluster to be running')
                time.sleep(1)
        
        # the default ttl for Microk8s cluster to keep the historical
        # event is 5m we increase it to 24 hours
        set_ttl = ''
        set_ttl += 'sudo sed -i s/--event-ttl=5m/--event-ttl=1440m/ '
        set_ttl += '/var/snap/microk8s/current/args/kube-apiserver'
        
        # we are modifying a service of microk8s, we have to pay
        # the price of restarting microk8s to enable the new ttl
        self.remote.run(set_ttl)
        self.restart()
        self.profiler.prof('cluster_warmup_stop', uid=self.id)


    # --------------------------------------------------------------------------
    #
    def watch(self):
        cmd = 'kubectl get pods --watch'
        try:
            if self.remote:
                self.remote.run(cmd)
            else:
                sh_callout(cmd,  shell=True)
        except KeyboardInterrupt:
            return


    def generate_pods(self, ctasks):

        pods            = []
        pods_names      = []

        self.profiler.prof('schedule_pods_start', uid=self.id)
        batches         = self.schedule(ctasks)
        self.profiler.prof('schedule_pods_stop', uid=self.id)


        depolyment_file = '{0}/hydraa_pods.json'.format(self.sandbox, self.id)

        for batch in batches:
            pod_id     = str(self.pod_counter).zfill(6)
            containers = []

            self.profiler.prof('create_pod_start', uid=pod_id)
            for ctask in batch:
                envs = []
                if ctask.env_var:
                    for env in ctask.env_vars:
                        pod_env  = client.V1EnvVar(name = env[0], value = env[1])
                        envs.append(pod_env)

                pod_cpu = "{0}m".format(ctask.vcpus * 1000)
                pod_mem = "{0}Mi".format(ctask.memory)

                resources=client.V1ResourceRequirements(requests={"cpu": pod_cpu, "memory": pod_mem},
                                                        limits={"cpu": pod_cpu, "memory": pod_mem})

                pod_container = client.V1Container(name = ctask.name, image = ctask.image,
                            resources = resources, command = ctask.cmd, env = envs)
                
                containers.append(pod_container)
            
            pod_name      = "hydraa-pod-{0}".format(pod_id)
            pod_metadata  = client.V1ObjectMeta(name = pod_name)

            # check if we need to restart the task
            if ctask.restart:
                restart_policy = ctask.restart
            else:
                restart_policy = 'Never'

            pod_spec  = client.V1PodSpec(containers=containers,
                                restart_policy=restart_policy)

            pod_obj   = client.V1Pod(api_version="v1", kind="Pod",
                            metadata=pod_metadata, spec=pod_spec)

            # santize the json object
            sn_pod = client.ApiClient().sanitize_for_serialization(pod_obj)

            pods.append(sn_pod)
            pods_names.append(pod_name)
            
            self.pod_counter +=1
        
        self.profiler.prof('create_pod_stop', uid=pod_id)

        with open(depolyment_file, 'w') as f:
            for p in pods:
                print(p, file=f)

        # we are faking a json file here
        with open(depolyment_file, "r") as f:
            text = f.read()
            text = text.replace("'", '"')
        
        with open(depolyment_file, "w") as f:
            text = f.write(text)

        return depolyment_file, pods_names, batches


    # --------------------------------------------------------------------------
    #
    def wait(self):
        # FIXME: for some reason the completed pods
        #        are reported incorrectley, making
        #        the cluster hold the resources
        #        after the execution finished.

        cmd  = 'kubectl '
        cmd += 'get pod --field-selector=status.phase=Succeeded '
        cmd += '| grep Completed* | wc -l'
        
        while True:
            done_pods = 0
            if self.remote:
                done_pods = self.remote.run(cmd, hide=True).stdout.strip()
            else:
                out, err, _ = sh_callout(cmd, shell=True)
                done_pods = int(out.strip())

            if done_pods:
                print('Completed pods: {0}/{1}'.format(done_pods, self.pod_counter), end='\r')
                if self.pod_counter == int(done_pods):
                    print('{0} Pods finished with status "Completed"'.format(done_pods))
                    break
                else:
                    time.sleep(5)
        return True


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        # upload the pods file before bootstrapping
        # FIXME: we get socket closed if we did it
        # in the reverse order, because we modify 
        # the firewall of the node

        self.profiler.prof('generate_pods_start', uid=self.id)
        depolyment_file, pods_names, batches = self.generate_pods(ctasks)
        self.profiler.prof('generate_pods_stop', uid=self.id)

        name = os.path.basename(depolyment_file)

        # remote mode: NSF cluster (we manage it via SSH)
        # embeded mode: Azure/AWS we manage by merging
        # the config of the cluster to the local machine

        # check if we are in remote mode or embeded mode
        if self.remote:
            # upload the pods file to the remote machine
            self.remote.put(depolyment_file)
            cmd = 'kubectl apply -f {0}'.format(name)
            # deploy the pods.json on the cluster
            self.remote.run(cmd)
        
        # we are in the embeded mode
        else:
            # just invoke a shell process
            cmd = 'kubectl apply -f {0}'.format(depolyment_file)
            out, err, _ = sh_callout(cmd, shell=True)
            print(out, err)

        #FIXME: create a monitering of the pods/containers
        
        return depolyment_file, pods_names, batches


    # --------------------------------------------------------------------------
    #
    def schedule(self, tasks):

        task_batch = copy.deepcopy(tasks)
        batch_size = len(task_batch)
        if not batch_size:
            raise Exception('Batch size can not be 0')

        # containers per pod
        CPP = self.size

        tasks_per_pod = []

        container_grps = math.ceil(batch_size / CPP)

        # If we cannot split the
        # number into exactly 'container_grps of 10' parts
        if(batch_size < container_grps):
            print(-1)
    
        # If batch_size % container_grps == 0 then the minimum
        # difference is 0 and all
        # numbers are batch_size / container_grps
        elif (batch_size % container_grps == 0):
            for i in range(container_grps):
                tasks_per_pod.append(batch_size // container_grps)
        else:
            # upto container_grps-(batch_size % container_grps) the values
            # will be batch_size / container_grps
            # after that the values
            # will be batch_size / container_grps + 1
            zp = container_grps - (batch_size % container_grps)
            pp = batch_size // container_grps
            for i in range(container_grps):
                if(i>= zp):
                    tasks_per_pod.append(pp + 1)
                else:
                    tasks_per_pod.append(pp)
        
        batch_map = tasks_per_pod

        objs_batch = []
        for batch in batch_map:
           objs_batch.append(task_batch[:batch])
           task_batch[:batch]
           del task_batch[:batch]
        return(objs_batch)


    # --------------------------------------------------------------------------
    #
    def get_pod_status(self):

        cmd = 'kubectl get pod --field-selector=status.phase=Succeeded -o json > pod_status.json'
        if self.remote:
            self.remote.run(cmd, hide=True)
            self.remote.get('pod_status.json')
        else:
            sh_callout(cmd, shell=True)

        with open('pod_status.json', 'r') as f:
            response = json.load(f)

        if response:
            df = pd.DataFrame(columns=['Task_ID', 'Status', 'Start', 'Stop'])
            # iterate on pods
            i = 0
            for pod in response['items']:
                # get the status of each pod
                phase = pod['status']['phase']

                # iterate on containers
                for container in pod['status']['containerStatuses']:
                    c_name = container.get('name')
                    for k, v in  container['state'].items():
                        state = container.get('state', None)
                        if state:
                            for kk, vv in container['state'].items():
                                start_time = self.convert_time(v.get('startedAt', 0.0))
                                stop_time  = self.convert_time(v.get('finishedAt', 0.0))
                                df.loc[i] = (c_name, (kk, v.get('reason', None)), start_time, stop_time)
                                i +=1

                        else:
                            print('Pods did not finish yet or failed')

            os.remove('pod_status.json')

            return df
    

    # --------------------------------------------------------------------------
    #
    def get_pod_events(self):
        
        cmd = 'kubectl get events -A -o json > pod_events.json' 
        if self.remote:
            self.remote.run(cmd, hide=True)
            self.remote.get('pod_events.json')
        else:
            sh_callout(cmd, shell=True)
        with open('pod_events.json', 'r') as f:
            response = json.load(f)

        df = pd.DataFrame(columns=['Task_ID', 'Reason', 'FirstT', 'LastT'])
        if response:
            id = 0
            for it in response['items']:
                field = it['involvedObject'].get('fieldPath', None)
                if field:
                    if 'spec.containers' in field:
                        if 'ctask' in field:
                            cid        = field.split('}')[0].split('{')[1]
                            reason     = it.get('reason', None)
                            reason_fts = self.convert_time(it.get('firstTimestamp', 0.0))
                            reason_lts = self.convert_time(it.get('lastTimestamp', 0.0))
                            df.loc[id] = (cid, reason, reason_fts, reason_lts)
                            id +=1

        os.remove('pod_events.json')

        return df


    # --------------------------------------------------------------------------
    #
    def checkpoint_profiles(self):
        """
        **This method should start as a background thread.

        AKS/EKS does not allow to modify the ttl-events
        of the cluster meaning if we have an exeution
        for > 1 hour the profiles will be deleted
        from the cluster to be replaced by new ones, unless
        we enable Azure or AWS monitoring which == $$.

        This function will save a checkopint of the profiles
        as a dataframe every 55 minutes and merge them at the
        end of the execution.
        https://github.com/Azure/AKS/issues/2140
        """

        def get_profiles(ids):
            print('Registering a profiles checkpoint')
            fname = self.sandbox+'/'+'check_profiles.{0}.csv'.format(str(ids).zfill(6))
            df1 = self.get_pod_status()
            df2 = self.get_pod_events()
            df = (pd.merge(df1, df2, on='Task_ID'))

            df.to_csv(fname)
            print('Checkpoint profiles saved to {0}'.format(fname))

        ids = 0
        # iterate until the stop_event is triggered
        while not self.stop_event.is_set():
            for t in range(3300, 0, -1):
                if t == 1:
                    # save a checkpoint every ~ 55 minutes
                    get_profiles(ids)
                
                # exit the loop if stop_event is true
                if self.stop_event.is_set():
                    break

                else:
                    time.sleep(1)

            ids +=1
        # if we exist, then save a checkpoint as well
        get_profiles(ids)


    # --------------------------------------------------------------------------
    #
    def convert_time(self, timestamp):

        t  = datetime.datetime.strptime(timestamp, TFORMAT)
        ts = time.mktime(t.timetuple())

        return ts


    # --------------------------------------------------------------------------
    #
    def get_worker_nodes(self):
         pass
    

    # --------------------------------------------------------------------------
    #
    def join_master(self):
        """
        This method should allow
        x worker nodes to join the
        master node (different vms or
        pms) 
        """
        pass


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self.remote.run('sudo microk8s stop')


    def delete(self):
        pass
    

class Aks_Cluster(Cluster):
    """Represents a single/multi node Kubrenetes cluster.
       This class asssumes that:

       1- Your user has the correct permessions for AKS and CLI.
       2- Azure-cli is installed

       NOTE: This class will overide any existing kubernetes
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, run_id, vm, sandbox):
        self.id             = run_id
        self.cluster_name   = 'HydraaAksCluster'
        self.resource_group = vm.ResourceGroup
        self.nodes          = vm.MinCount
        self.max_pods       = 250
        self.size           = 0
        self.sandbox        = sandbox
        self.instance       = vm.InstanceID
        self.region         = vm.Region
        self.config         = None
        self.stop_event     = mt.Event()
        self.watch_profiles = mt.Thread(target=self.checkpoint_profiles, name="AKS_profiles_watcher")

        super().__init__(run_id, None, self.size, sandbox)

        self.watch_profiles.daemon = True

        az_cli = get_default_cli()
        az_cli.invoke(['login', '--use-device-code'])

        atexit.register(self.stop_background, self.stop_event, [self.watch_profiles])


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):

        self.profiler.prof('bootstrap_start', uid=self.id)
        if not self.size:
            self.size = self.get_vm_size(self.instance, self.region) - 1

        cmd  = 'az aks create '
        cmd += '-g {0} '.format(self.resource_group.name)
        cmd += '-n {0} '.format(self.cluster_name)
        cmd += '--enable-managed-identity '
        cmd += '--node-vm-size {0} '.format(self.instance)
        cmd += '--node-count {0} '.format(self.nodes)
        cmd += '--generate-ssh-keys'

        print('Building AKS cluster..')
        self.config = sh_callout(cmd, shell=True, munch=True)

        self.profiler.prof('configure_start', uid=self.id)
        self.configure()
        self.profiler.prof('bootstrap_stop', uid=self.id)


    # --------------------------------------------------------------------------
    #
    def configure(self):
        # FIXME: we need to find a way to keep the config
        #        of existing kubernetes (for multinode cluster)
        
        # To manage a Kubernetes cluster, we use the Kubernetes CLI and kubectl
        # NOTE: kubectl is already installed if you use Azure Cloud Shell.
        cmd  = 'az aks get-credentials '
        cmd += '--admin --name {0} '.format(self.cluster_name)
        cmd += '--resource-group {0} '.format(self.resource_group.name)
        cmd += '--overwrite-existing'

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return True


    # --------------------------------------------------------------------------
    #
    def add_nodes(self, nodes_type=None):
        """
        add nodes to an existing cluster
        """
        self.nodes_pool = 'hydraa_aks_nodepool'

        cmd  = 'az aks nodepool add '
        cmd += '--resource-group {0} '.format(self.resource_group)
        cmd += '--cluster-name {0} '.format(self.cluster_name)
        cmd += '--name  {0} '.format(self.nodes_pool)
        cmd += '--node-count {0} --no-wait'.format(self.nodes)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return True


    # --------------------------------------------------------------------------
    #
    def add_auto_scaler(self, nodes, nodes_type, range: list):
        """
        Create a node pool. This will not launch any nodes immediately but will
        scale up and down as needed. If you change the GPU type or the number of
        GPUs per node, you may need to change the machine-type.

        range: list of 2 index [min_nodes, max_nodes]
        """
        self.auto_scaler = 'hydraa_autoscale_aks_nodepool'

        cmd  = 'az aks nodepool add '
        cmd += '--resource-group {0} '.format(self.resource_group)
        cmd += '--cluster-name {0} '.format(self.cluster_name)
        cmd += '--name {0} '.format(self.auto_scaler)
        cmd += '--node-count {0} '.format(nodes)
        cmd += '--node-vm-size {0} '.format(nodes_type)
        cmd += '--enable-cluster-autoscaler '
        cmd += '--min-count {0} --max-count {1}'.format(range[0], range[1])

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return True
    

    # --------------------------------------------------------------------------
    #
    def update_auto_scaler(self, range: list):
        """
        Update an exsiting autoscaler with different number of nodes.
        
        range: list of 2 index [min_nodes, max_nodes]
        """

        if not self.auto_scaler:
            raise Exception('No autoscaler in this cluster, you need to create one')
        
        cmd = 'az aks nodepool update '
        cmd += '--update-cluster-autoscaler '
        cmd += '--min-count {0} --max-count {1} '.format(range[0], range[1])
        cmd += '--resource-group {0} '.format(self.resource_group)
        cmd += '--cluster-name {0} '.format(self.cluster_name)
        cmd += '--name {0}'.format(self.auto_scaler)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return True


    # --------------------------------------------------------------------------
    #
    def wait(self):
        # wait for all pods to finish
        if super().wait():
            self.profiler.prof('pods_finished', uid=self.id)
            self.stop_event.set()
            return True


    # --------------------------------------------------------------------------
    #
    def get_vm_size(self, vm_id, region):

        # obtain all of the vms in this region
        cmd = 'az vm list-sizes --location {0}'.format(region)
        vms = sh_callout(cmd, shell=True, munch=True)

        vcpus = 0
        # get the coresponding info of the targeted vm
        for vm in vms:
            name = vm.get('name', None)
            if name == vm_id:
                vcpus = vm['numberOfCores']
                break
        if not vcpus:
            raise Exception('Can not find VM size')
        return vcpus


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        depolyment_file, pods_names, batches = super().submit(ctasks)

        # start the profiles thread
        self.watch_profiles.start()

        return depolyment_file, pods_names, batches


class Eks_Cluster(Cluster):
    """Represents a single/multi node Kubrenetes cluster.
       This class asssumes that you did the one time
       preparational steps:

       1- AWS-> eksctl installed 
       2- AWS-> aws-iam-authenticator
       2- Kuberenetes-> kubectl installed

       FIXME: For every run, export $KUBECONFIG
       NOTE : This class will overide any existing kubernetes config
    """
    def __init__(self, run_id, sandbox, vm, iam, rclf, clf, ec2, eks, nodes=1):

        self.id             = run_id
        self.cluster_name   = 'Hydraa-Eks-Cluster'
        self.nodes          = nodes
        self.max_pods       = 250
        self.size           = 0
        self.sandbox        = sandbox
        self.vm             = vm
        self.config         = None
        self.stop_event     = mt.Event()

        self.iam            = iam
        self.clf            = clf
        self.ec2            = ec2
        self.eks            = eks
        self.rclf           = rclf

        self.watch_profiles = mt.Thread(target=self.checkpoint_profiles, name="EKS_profiles_watcher")

        super().__init__(run_id, None, self.size, sandbox)

        self.watch_profiles.daemon = True

        atexit.register(self.stop_background, self.delete,self.stop_event,
                                                    [self.watch_profiles])


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):

        # FIXME: let the user specify the kubernetes_v
        kubernetes_v  = '1.22'
        NodeGroupName = "Hydraa-Eks-NodeGroup"

        # FIXME: Find a way to workaround
        #        the limited avilability
        #        zones.
        # https://github.com/weaveworks/eksctl/issues/817

        Support_Zones = ['a', 'b']

        self.profiler.prof('bootstrap_start', uid=self.id)

        if not self.size:
            self.size = self.get_vm_size(self.vm.InstanceID, self.vm.Region) - 1
        
        cmd  = 'eksctl create cluster --name {0} '.format(self.cluster_name)
        cmd += '--region {0} --version {1} '.format(self.vm.Region, kubernetes_v)
        cmd += '--zones {0}{1},{0}{2} '.format(self.vm.Region, Support_Zones[0], Support_Zones[1])
        cmd += '--nodegroup-name {0} '.format(NodeGroupName)
        cmd += '--node-type {0} --nodes {1}'.format(self.vm.InstanceID, self.vm.MinCount)

        print('Building EKS cluster..')

        out, err, ret = sh_callout(cmd, shell=True)

        if ret:
            print(out, err)
            raise Exception('Failed to build EKS cluster')

        self.profiler.prof('cofigure_start', uid=self.id)
        self.configure()

        self.profiler.prof('bootstrap_stop', uid=self.id)


    # --------------------------------------------------------------------------
    #
    def configure(self):

        config_file = os.path.expanduser("~") + "/.kube/config"

        # write the kube config file
        if not os.path.isdir(os.path.expanduser("~") + "/.kube"):
            print('Creating .kube folder')
            os.mkdir(os.path.expanduser("~") + "/.kube")

        if not os.path.isfile(config_file):
            open(config_file, 'x')

        print("Writing kubectl configuration to ", config_file)

        cmd  = 'aws eks update-kubeconfig --region {0} '.format(self.vm.Region)
        cmd += '--name {0}'.format(self.cluster_name)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)


    # --------------------------------------------------------------------------
    #
    def add_auto_scaler(self, node_type, range=[2,2], volume_size=0):
        """
        range[0] int   minimum nodes in ASG (default 2)
        range[1] int   maximum nodes in ASG (default 2)
        """
        
        name = 'Hydraa-Eks-NodeGroup-AutoScaler{0}'.format(str(uuid.uuid4()))

        cmd  = 'eksctl create nodegroup --name {0} '.format(name)
        cmd += '--cluster {0} --node-type {1} '.format(self.cluster_name, 
                                                              node_type)

        if range:
            cmd += '--nodes-min {1} --nodes-max {2} '.format(range[0], range[1])
 
        if volume_size:
            cmd += '--node-volume-size {1}'.format(volume_size)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        self.vm.AutoScaler = [name]


    # --------------------------------------------------------------------------
    #
    def get_vm_size(vm_id, region):
        '''
        Modified version of get_instances() from
        https://github.com/powdahound/ec2instances.info/blob/master/ec2.py
        AWS provides no simple API for retrieving all instance types
        '''
        import boto3
        pricing_client = boto3.client('pricing', region_name=region)
        product_pager = pricing_client.get_paginator('get_products')

        product_iterator = product_pager.paginate(
            ServiceCode='AmazonEC2', Filters=[
                # We're gonna assume N. Virginia has all the available types
                {'Type': 'TERM_MATCH', 'Field': 'location',
                 'Value': 'US East (N. Virginia)'},])

        vcpus = 0
        for product_item in product_iterator:
            for offer_string in product_item.get('PriceList'):
                offer = json.loads(offer_string)
                product = offer.get('product')

                # Check if it's an instance
                if product.get('productFamily') not in ['Dedicated Host',
                                                        'Compute Instance',
                                                        'Compute Instance (bare metal)']:
                    continue

                product_attributes = product.get('attributes')
                instance_type = product_attributes.get('instanceType')
                if instance_type == vm_id:
                    vcpus = product_attributes.get('vcpu')
                    if not vcpus:
                        raise Exception('Could not find VM size')
                    return int(vcpus)


    # --------------------------------------------------------------------------
    #
    def add_nodes(self, group_name, range, nodes=0):
        """
        add nodes to an existing cluster
        """
        if self.vm.AutoScaler:
            if len(self.vm.AutoScaler) > 1:
                print('Please specify which NodeGroup to add nodes to: {0}'.format(self.vm.AutoScaler))
                return

            cmd  = 'eksctl scale nodegroup --name {0} '.format(group_name)
            cmd += '--cluster {0} '.format(self.cluster_name)
            if nodes:
                cmd += '--nodes={0} '
            
            cmd += '--nodes-min={1} --nodes-max={2}'.format(nodes, range[0], range[1])

            out, err, _ = sh_callout(cmd, shell=True)

            print(out, err)

    # --------------------------------------------------------------------------
    #
    def wait(self):
        if super().wait():
            self.profiler.prof('pods_finished', uid=self.id)
            self.stop_event.set()
            return True


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        depolyment_file, pods_names, batches = super().submit(ctasks)

        # start the profiles thread
        self.watch_profiles.start()

        return depolyment_file, pods_names, batches


    # --------------------------------------------------------------------------
    #
    def delete(self):
        
        cmd = 'eksctl delete cluster --name {0}'.format(self.cluster_name)

        print('Deleteing EKS cluster: {0}'.format(self.cluster_name))

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

