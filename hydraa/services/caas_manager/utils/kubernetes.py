
import os
import json
from pathlib import Path
from kubernetes import client, config

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
        print(boostrapper)
        self.remote.put(boostrapper)
        self.remote.run("chmod +x deploy_kuberentes_local.sh")
        self.remote.run("./deploy_kuberentes_local.sh")
        print('booting successfull')


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
    

    
    def _get_kb_worker_nodes(self):
         pass
    

    def delete(self):
        pass
    

