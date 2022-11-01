import os
import time
import json
from pathlib import Path
from kubernetes import client, config

true   = True
false  = False
null   = None

class Cluster:

    def __init__(self, run_id, remote):
        
        self.id     = run_id
        self.remote = remote


    def bootstrap_local(self):

        """
        deploy kubernetes cluster K8s on chi
        """
        print('booting K8s cluster on the remote machine')
        loc = os.path.join(os.path.dirname(__file__)).split('utils')[0]
        boostrapper = "{0}config/deploy_kuberentes_local.sh".format(loc)
        self.remote.put(boostrapper)
        self.remote.run("chmod +x deploy_kuberentes_local.sh")
        self.remote.run("./deploy_kuberentes_local.sh")
        while True:
            stream = self.remote.run('sudo microk8s status --wait-ready')
            # check if the cluster is ready to submit the pod
            if "microk8s is running" in stream.stdout:
                print('booting Kuberentes cluster successful')
                break
            else:
                print('waiting for Kuberentes cluster to be running')
                time.sleep(1)


    def watch(self, pod_id):
        try:
            self.remote.run('sudo microk8s kubectl get pods {0} --watch'.format(pod_id))
        except KeyboardInterrupt:
            return


    def generate_pod(self, ctasks):

        pod_file  = 'hydraa_pod.json'
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
        
        pod_name      = "hydraa-pod-{0}".format(self.id)
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
    

    def submit_pod(self, pods):

        # upload the pods file before bootstrapping
        # FIXME: we get socket closed if we did it
        # in the reverse order, because we modify 
        # the firewall of the node

        # upload the pods.json
        self.remote.put(pods)
        
        # bootup the cluster K8s
        self.bootstrap_local()

        # deploy the pods.json on the cluster
        self.remote.run('sudo microk8s kubectl apply -f {0}'.format(pods))

        #FIXME: create a monitering of the pods/containers
        
        return True


    def get_pod_status(self, pod_id):

        #FIXME: get the ifno of a specifc pod by allowing 
        # this function to get pod_id
        cmd = 'sudo microk8s kubectl get pod {0} --field-selector=status.phase=Succeeded -o json'.format(pod_id)
        out = self.remote.run(cmd).stdout
        response = eval(out)

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
                                start_time = os.popen("date -d {0} +%s".format(v.get('startedAt', 0.0)))
                                stop_time  = os.popen("date -d {0} +%s".format(v.get('finishedAt', 0.0)))
                                print('container {0} state:  {1} becasue its {2}'.format(c_name, kk, v.get('reason', None)))
                                print('container {0} start:  {1}'.format(c_name, start_time.readline().split('\n')[0]))
                                print('container {0} stop :  {1}'.format(c_name, stop_time.readline().split('\n')[0]))
        else:
            print('pods did not finish yet or failed')


    
    def _get_kb_worker_nodes(self):
         pass
    

    def delete(self):
        pass
    

