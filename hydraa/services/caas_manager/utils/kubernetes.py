import os
import time
import math
import copy
import json
import datetime
import pandas as pd
import radical.utils as ru

from pathlib import Path
from collections import OrderedDict
from kubernetes import client, config


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
    

    def restart(self):
        self.stop()
        self.start()

    
    def bootstrap_local(self):
        
        """
        deploy kubernetes cluster K8s on chi
        """
        print('booting K8s cluster on the remote machine')

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
                print('booting Kuberentes cluster successful')
                break
            else:
                print('waiting for Kuberentes cluster to be running')
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
        try:
            self.remote.run('sudo microk8s kubectl get pods --watch')
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
        while True:
            cmd = 'sudo microk8s kubectl '
            cmd += 'get pod --field-selector=status.phase=Succeeded '
            cmd += '| grep Completed* | wc -l'
            done_pods = self.remote.run(cmd, hide=True).stdout.strip()
            if self.pod_counter == int(done_pods):
                print('{0} pods with finished with status Completed'.format(done_pods))
                break
            else:
                time.sleep(2)
        return True


    def submit(self, ctasks):

        # upload the pods file before bootstrapping
        # FIXME: we get socket closed if we did it
        # in the reverse order, because we modify 
        # the firewall of the node

        self.profiler.prof('generate_pods_start', uid=self.id)
        depolyment_file, pods_names, batches = self.generate_pods(ctasks)
        self.profiler.prof('generate_pods_stop', uid=self.id)
        
        # upload the pods.json
        self.remote.put(depolyment_file)

        # deploy the pods.json on the cluster

        name = os.path.basename(depolyment_file)
        self.remote.run('sudo microk8s kubectl apply -f {0}'.format(name))

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


    def get_pod_status(self):

        cmd = 'sudo microk8s kubectl get pod --field-selector=status.phase=Succeeded -o json > pod_status.json'
        self.remote.run(cmd, hide=True)
        self.remote.get('pod_status.json')
        with open('pod_status.json', 'r') as f:
            response = json.load(f)

        # FIXME: generate profiles as pd dataframe
        if response:
            df = pd.DataFrame(columns=['Task_ID', 'Status', 'Start', 'Stop'])
            # iterate on pods
            i = 0
            for pod in response['items']:
                # get the status of each pod
                phase = pod['status']['phase']
                print('pod has phase:{0}'.format(phase))
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
                            print('pods did not finish yet or failed')

            os.remove('pod_status.json')

            return df
    

    # --------------------------------------------------------------------------
    #
    def get_pod_events(self):
        
        cmd = 'sudo microk8s kubectl get events -A -o json > pod_events.json' 
        self.remote.run(cmd, hide=True)
        self.remote.get('pod_events.json')
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


    # --------------------------------------------------------------------------
    #
    def __init__(self, run_id, resource_group, cluster_size, sandbox, nodes=1):
        self.id            = run_id
        self.cluster_name  = 'Hydraa_AKS_Cluster_{0}'.format(self.id)
        self.resource_grup = resource_group
        self.nodes         = nodes
        self.max_pods      = 250
        self.size          = cluster_size
        self.sandbox       = sandbox
        super().__init__(run_id, None, cluster_size, sandbox)

        print('AKS cluster requires to authenticate with Azure')
        os.popen('az login --use-device-code')


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):
        cmd  = 'az aks create '
        cmd += '-g {0} '.format(self.resource_grup.name)
        cmd += '-n {0} '.format(self.cluster_name)
        cmd += '--enable-managed-identity '
        cmd += '--node-count {0} '.format(self.nodes)
        cmd += '--generate-ssh-keys'

        os.popen(cmd)
    

    # --------------------------------------------------------------------------
    #
    def authenticate(self):
        cmd  = 'az aks get-credentials '
        cmd += '--admin --name {0}'.format(self.cluster_name)
        cmd += '--resource-group {0} -y'.format(self.resource_grup.name)

        os.popen(cmd)


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks):

        self.profiler.prof('generate_pods_start', uid=self.id)
        depolyment_file, pods_names, batches = self.generate_pods(ctasks)
        self.profiler.prof('generate_pods_stop', uid=self.id)

        name = os.path.basename(depolyment_file)
        self.remote.run('kubectl apply -f {0}'.format(name))

        #FIXME: create a monitering of the pods/containers
        
        return depolyment_file, pods_names, batches
