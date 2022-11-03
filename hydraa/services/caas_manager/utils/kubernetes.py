import os
import time
import json
import uuid
import datetime

from pathlib import Path
from collections import OrderedDict
from kubernetes import client, config

true    = True
false   = False
null    = None
HOME    = str(Path.home())
TFORMAT = '%Y-%m-%dT%H:%M:%fZ'

class Cluster:

    def __init__(self, run_id, remote):
        
        self.id      = run_id
        self.remote  = remote
        self.sandbox = '{0}/hydraa.sandbox.{1}'.format(HOME, self.id)

    
    def start(self):
        self.remote.run('sudo microk8s start')
    

    
    def bootstrap_local(self):

        """
        deploy kubernetes cluster K8s on chi
        """
        print('booting K8s cluster on the remote machine')
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


    def watch(self):
        try:
            self.remote.run('sudo microk8s kubectl get pods --watch')
        except KeyboardInterrupt:
            return


    def generate_pod(self, ctasks):

        pod_id     = str(uuid.uuid4())
        pod_file   = '{0}/hydraa_pod_{1}.json'.format(self.sandbox, pod_id)
        containers = []
        for ctask in ctasks:
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

        with open(pod_file, 'w') as f:
            sanitized_pod = client.ApiClient().sanitize_for_serialization(pod_obj)
            json.dump(sanitized_pod, f)

        return pod_file, pod_name
    

    def submit_pod(self, pod_file):

        # upload the pods file before bootstrapping
        # FIXME: we get socket closed if we did it
        # in the reverse order, because we modify 
        # the firewall of the node

        # upload the pods.json
        
        self.remote.put(pod_file)

        # deploy the pods.json on the cluster

        pod_name = os.path.basename(pod_file)
        self.remote.run('sudo microk8s kubectl apply -f {0}'.format(pod_name))

        #FIXME: create a monitering of the pods/containers
        
        return True


    def get_pod_status(self):

        #FIXME: get the ifno of a specifc pod by allowing 
        # this function to get pod_id
        cmd = 'sudo microk8s kubectl get pod --field-selector=status.phase=Succeeded -o json'
        status = self.remote.run(cmd).stdout
        response = eval(status)

        # FIXME: generate profiles as pd dataframe
        if response:
            # iterate on pods
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
                                print('container {0} state:  {1} becasue its {2}'.format(c_name, kk, v.get('reason', None)))
                                print('container {0} start:  {1}'.format(c_name, start_time))
                                print('container {0} stop :  {1}'.format(c_name, stop_time))
        else:
            print('pods did not finish yet or failed')
    

    def get_pod_events(self):
        
        cmd = 'sudo microk8s kubectl get events -A -o json' 
        events = self.remote.run(cmd).stdout
        response = eval(events)

        task_stamps = OrderedDict()

        if response:
            for it in response['items']:
                field = it['involvedObject'].get('fieldPath', None)
                if field:
                    if 'spec.containers' in field:
                        if 'ctask' in field:
                            cid  = field.split('}')[0].split('{')[1]
                            evt  = it.get('reason', None)
                            task_stamps[cid] = OrderedDict()
                            task_stamps[cid][evt+'StartedAt'] = self.convert_time(it.get('lastTimestamp', 0.0))
                            task_stamps[cid][evt+'StoppedAt']  = self.convert_time(it.get('lastTimestamp', 0.0))


        return task_stamps
    

    def convert_time(self, timestamp):

        t  = datetime.datetime.strptime(timestamp, TFORMAT)
        ts = time.mktime(t.timetuple())

        return ts

    
    def get_worker_nodes(self):
         pass
    

    def join(self):
        """
        This method should allow
        x worker nodes to join the
        master node (different vms or
        pms) 
        """
        pass


    def stop(self):
        self.remote.run('sudo microk8s stop')

    def delete(self):
        pass
    

