import os
import time
import math
import copy
import queue
import atexit
import shutil
import urllib3

import pandas as pd
import threading as mt
import radical.utils as ru

from typing import List
from typing import Dict
from typing import Tuple

from kubernetes import watch
from kubernetes import client
from kubernetes import config

from ..utils.misc import is_root
from ..utils.misc import build_pod
from ..utils.misc import load_yaml
from ..utils.misc import dump_yaml
from ..utils.misc import unique_id
from ..utils.misc import sh_callout
from ..utils.misc import generate_id
from ..utils.misc import convert_time
from ..utils.misc import dump_multiple_yamls


from ....cloud_vm.vm import AwsVM
from ....cloud_vm.vm import AzureVM
from ....cloud_task.task import Task
from ....cloud_vm.vm import LocalVM
from ....cloud_vm.vm import OpenStackVM


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

CFAILED_STATE = ['Error', 'StartError','OOMKilled',
                'ContainerCannotRun','DeadlineExceeded',
                'CrashLoopBackOff', 'ImagePullBackOff',
                'RunContainerError','ImageInspectError']

CRUNNING_STATE = ['ContainerCreating', 'Started', 'Running']
CCOMPLETED_STATE = ['Completed', 'completed']

PFAILED_STATE = ['Failed', 'OutOfCPU','OutOfMemory',
                'Error', 'CrashLoopBackOff',
                'ImagePullBackOff', 'InvalidImageName',
                'CreateContainerConfigError','RunContainerError',
                'OOMKilled','ErrImagePull','Evicted']
SLEEP = 2
BUSY = 'Busy'
READY = 'Ready'
LOCAL = 'local'
MAX_PODS = 110

KUBECTL = shutil.which('kubectl')
KUBE_VERSION = os.getenv('KUBE_VERSION')
KUBE_TIMEOUT = os.getenv('KUBE_TIMEOUT') # minutes
KUBE_CONTROL_HOSTS = os.getenv('KUBE_CONTROL_HOSTS', default=1)

POD = ['pod', 'Pod']
CONTAINER = ['container', 'Container']


# --------------------------------------------------------------------------
#
class K8sCluster:
    """
    This is a multithreaded Kuberentes base class that 
    is build on the top of k8s or microK8s Kuberentes flavor 
    (or any falvor). 
    
    This cluster controlls:

    1- Node(s) is a worker machine in Kubernetes and may be
       either a virtual or a physical machine, depending on
       the cluster.
    
    2- Pod(s) effectively a unit of work. It is a way to describe
      a series of containers.
    
    3- Container(s) a unit of software that packages code and its
       dependencies so the application.

    This cluster can:

    1- Manager multiple nodes across different physical and
       virtual nodes.
    
    2- Schedule and partion containers into pods based on
       the cluster size.
    """

    def __init__(self, run_id, vms, sandbox, log):
        """
        Constructor for the K8sCluster class.

        Parameters
        ----------
        run_id : str
            Unique ID delivered by the controller manager.

        vms : List[hydraa.vm.VirtualMachine]
            A list of AWS/Azure/OpenStack Hydraa VM instances.
        
        result_q : Queue to send results back to the controller manager.

        sandbox : str
            The path to the folder of the Hydraa manager.

        log : logging
            A logger object.
        """
        self.vms = vms
        self.id = run_id
        self.logger = log
        self.status = None
        self.pod_counter = 0
        self.sandbox = sandbox
        self.kube_config = None
        self.result_queue = queue.Queue()
        self.provider = self.vms[0].Provider
        self.size = self.get_cluster_allocatable_size()
        self.nodes = sum([vm.MinCount for vm in self.vms])
        self.profiler = ru.Profiler(name=__name__, path=self.sandbox)
        self.name = 'cluster-{0}-{1}'.format(self.provider, run_id)

        self.terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def add_nodes_properity(self):
        ndx = 0
        for vm in self.vms:
            for server in vm.Servers:
                if ndx == 0:
                    self.control_plane = server.remote
                else:
                    setattr(self, f'node{ndx}', server.remote)
                ndx +=1

    # --------------------------------------------------------------------------
    #
    def delete_completed_pods(self):
        """
        The function to delete completed pods
        if needed by the user. This function is mainly used if the user
        want to resue the cluster after exceeding the recommended 110 pods
        per cluster.

        Returns:
            bool: True if passed.
        """
        cmd = 'kubectl delete pod --field-selector=status.phase==Succeeded'
        self.control_plane.run(cmd)
        return True


    # --------------------------------------------------------------------------
    #
    def bootstrap(self, timeout=30):
        """
        Bootstrap a Kubernetes cluster with one master node and (n-1) worker nodes.

        This method orchestrates the process of building a Kubernetes cluster
        with n nodes (1 master) (n-1) workers using n virtual or physical machines
        by performing the following steps:

        1. Determine the number of worker nodes and print cluster details.
        2. Set the status of the cluster to 'BUSY'.
        3. Add hosts (IP and name) to each node.
        4. Adjust the timeout value if specified in the configuration.
        5. Set up the local mode if the provider is 'LOCAL'.
            - Join an existing cluster if the launch type is 'join'.
            - Create a new local cluster if the launch type is 'create'.
        6. Set up the remote mode if the provider is different from 'LOCAL'.
        7. Create a map of nodes for remote setup.
        8. Copy necessary SSH keys and the bootstrap script to the control plane node.
        9. Wait for the cluster to become ready within the specified timeout.
        10. Configure the Kubernetes client and set up an SSH tunnel.
        11. Set the status of the cluster to 'READY'.

        Parameters:
        - timeout (int): Maximum time (in seconds) to wait for the cluster to finish. Default is 30 seconds.

        Raises:
        - RuntimeError: If Kubectl is required but not installed.
        - RuntimeError: If creating a local Kubernetes cluster requires root access.
        - ValueError: If an unknown launch type is specified.

        Returns:
        - None

        Example:
        ```python
        cluster = K8sCluster()
        cluster.bootstrap(timeout=60)
        ```
        """
        head_node = self.vms[0]

        worker_nodes = len(self.get_worker_nodes())
        print('building {0} with x [{1}] worker nodes, [{2}] control plane node(s),'
              ' total of [{3}] nodes'.format(self.name, worker_nodes, KUBE_CONTROL_HOSTS,
                                             self.nodes))

        self.status = BUSY
        self.add_nodes_properity()

        if KUBE_TIMEOUT:
            timeout = int(KUBE_TIMEOUT)
        
        self.profiler.prof('bootstrap_cluster_start', uid=self.id)

        loc = os.path.join(os.path.dirname(__file__))
        boostrapper = "{0}/bootstrap_kubernetes.sh".format(loc)

        # local mode setup
        if self.provider == LOCAL:
            # join an existing cluster
            if head_node.LaunchType == 'join':
                # make sure kubectl is installed
                if not KUBECTL:
                    raise RuntimeError('Kubectl is required to join a Kuberentes cluster')

            # create a new local cluster
            elif head_node.LaunchType == 'create':
                # make sure the user is root to create a local cluster
                if is_root():
                    os.environ['KUBE_LOCAL'] = 'True'

                    # move the boostrapper to the local sandbox and build the boostrapping command
                    self.control_plane.get(boostrapper, local=self.sandbox)
                    boostrapper_path = f"{self.sandbox}/bootstrap_kubernetes.sh"
                    local_bootstrap_cmd = f'chmod +x {boostrapper_path} && nohup {boostrapper_path} '
                    local_bootstrap_cmd += '/dev/null < /dev/null &'

                    out, err, ret = sh_callout(local_bootstrap_cmd, shell=True)
                    if ret:
                        raise RuntimeError(f'failed to create a local Kuberentes cluster: {err}')

                else:
                    raise RuntimeError('Creating a local Kuberentes cluster requries root access')

            else:
                raise ValueError(f'Unknown launch type {head_node.launch_type}')

        # remote mode setup
        else:
            # Attempt to fix Paramiko issue #75 temporarily
            time.sleep(5)

            nodes_map = self.create_nodes_map()

            # bug in fabric: https://github.com/fabric/fabric/issues/323
            remote_ssh_path = '/home/{0}/.ssh'.format(self.control_plane.user)
            remote_ssh_name = head_node.KeyPair[0].split('.ssh/')[-1:][0]
            remote_key_path = remote_ssh_path + '/' + remote_ssh_name

            for key in head_node.KeyPair:
                self.control_plane.put(key, remote=remote_ssh_path, preserve_mode=True)
            # put the bootstrap script in the control plane node $HOME dir
            self.control_plane.put(boostrapper)

            bootstrap_cmd = 'chmod +x bootstrap_kubernetes.sh;'
            bootstrap_cmd += 'nohup ./bootstrap_kubernetes.sh '
            bootstrap_cmd += '-m "{0}" -u "{1}" -k "{2}" >& '.format(nodes_map,
                                                                    self.control_plane.user,
                                                                    remote_key_path)
            bootstrap_cmd += '/dev/null < /dev/null &'

            # start the bootstraping as a background process.
            self.control_plane.run(bootstrap_cmd, hide=True, warn=True)

        self.wait_for_cluster(timeout)

        self.profiler.prof('bootstrap_cluster_stop', uid=self.id)

        self.kube_config = self.configure(head_node)

        self.namespace = self.create_namespace()

        # start the watcher thread
        self._watch_pods_statuses()

        self.status = READY
        print('{0} is in {1} state'.format(self.name, self.status))


    # --------------------------------------------------------------------------
    #
    def configure(self, head_node):

        kube_config_path = None
        kube_config_locs = ['.kube/config', '~/.kube/config']

        self.logger.trace('creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        if hasattr(head_node, 'KubeConfigPath'):
            kube_config_path = head_node.KubeConfigPath
        else:
            for ploc in kube_config_locs:
                res = self.control_plane.run(f'cat {ploc}', warn=True,
                                                            hide=True)
                if not res.return_code:
                    kube_config_path = ploc
                    break

        if kube_config_path:
            self.control_plane.get(kube_config_path, local=config_file,
                                                     preserve_mode=True)
            self.logger.trace('setting kubeconfig path to: {0}'.format(config_file))
        else:
            raise FileNotFoundError(f'failed to find kubeconfig file under'
                                    ' {kube_config_locs}.\n You can set the'
                                    ' path via VM.KubeConfigPath="Your/Kube/Path"')

        if self.provider != LOCAL:
            kube_config = load_yaml(config_file)
            kube_server = kube_config['clusters'][0]['cluster']['server']

            # create the ssh tunnel
            self.tunnel = self.control_plane.setup_ssh_tunnel(kube_server)

            # update the Kube config file with the tunneled port
            kube_config['clusters'][0]['cluster']['server'] = \
                f'https://127.0.0.1:{self.tunnel.local_bind_port}'

            # write the changes to the disk
            dump_yaml(kube_config, config_file, safe=False)

        # set the kubeconfig in the kubernetes client
        config.load_kube_config(config_file)

        return config_file


    # --------------------------------------------------------------------------
    #
    def create_namespace(self, namespace=None):

        if not namespace:
            namespace = generate_id(prefix='hydraa-ns-')

        namespaces = client.CoreV1Api().list_namespace()
        all_namespaces = []
        for ns in namespaces.items:
            all_namespaces.append(ns.metadata.name)

        if namespace in all_namespaces:
            self.logger.warning(f"namespace {namespace} exists on {self.name}"
                                 " and will be reused")
        else:
            namespace_metadata = client.V1ObjectMeta(name=namespace)
            client.CoreV1Api().create_namespace(
                client.V1Namespace(metadata=namespace_metadata))

            self.logger.trace(f"namespace {namespace} is created on {self.name}")

        return namespace


    # --------------------------------------------------------------------------
    #
    def wait_for_cluster(self, timeout):
        start_time = time.time()

        while not self.terminate.is_set():
            check_cluster = self.control_plane.run('kubectl get nodes', warn=True,
                                                                        hide=True)
            if not check_cluster.return_code:
                self.logger.trace('{0} installation succeeded'.format(self.name))
                break
            else:
                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout * 60:
                    raise TimeoutError('failed to build {0} within' \
                                       ' [{1}] min. To increse the waiting' \
                                       ' time set $KUBE_TIMEOUT > 30 minutes'
                                       ' value'.format(self.name, timeout))
                else:
                    remaining_time = timeout * 60 - elapsed_time
                    self.logger.warning('installation of {0} is still'
                                        ' in progress. Waiting for ~ [{1}]'
                                        ' minutes.'.format(self.name,
                                        round(remaining_time / 60)))
                    time.sleep(60)


    # --------------------------------------------------------------------------
    #
    def create_nodes_map(self) -> str:
        nodes_map = []
        for vm in self.vms:
            for server in vm.Servers:
                # build then nodes map
                network = server.addresses.values()
                fixed_ip = next(iter(network))[0]['addr']
                hostname_ip = server.name + ',' + fixed_ip
                nodes_map.append(hostname_ip)

        # node1,10.0.0.1,192.168.10.1 node2,10.0.0.2 node3,10.0.0.3
        nodes_map = " ".join(str(x) for x in  tuple(nodes_map))

        return nodes_map


    # --------------------------------------------------------------------------
    #
    def generate_pods(self, ctasks) -> Tuple[str, list, list]: 
        """
        This function generates a deployment_file (pods) from a set of 
        scheduled tasks.

        Parameters:
            ctasks (list): a batch of tasks (HYDRAA.Task)
        
        Returns:
            deployment_file (str) : path for the deployment file.
        """
        scpp = [] # single container per pod
        mcpp = [] # multiple containers per pod

        deployment_file = '{0}/hydraa_pods_{1}.yaml'.format(self.sandbox,
                                                            unique_id())

        for ctask in ctasks:
            # Single Container Per Pod (SCPP)
            if any([p in [ctask.type] for p in POD]) or not ctask.type:
                scpp.append(ctask)

            # Multiple Containers Per Pod (MCPP).
            elif any([c in [ctask.type] for c in CONTAINER]):
                mcpp.append(ctask)

            else:
                raise Exception(f'Unknow task of type: {ctask.type}')

        if mcpp:
            _mcpp = []
            # FIXME: use orhestrator.scheduler
            self.profiler.prof('schedule_pods_start', uid=self.id)
            batches = self.schedule(mcpp)
            self.profiler.prof('schedule_pods_stop', uid=self.id)

            for batch in batches:
                self.pod_counter +=1
                pod = build_pod(batch, str(self.pod_counter).zfill(6))
                _mcpp.append(pod)

            dump_multiple_yamls(_mcpp, deployment_file)

        if scpp:
            _scpp = []
            for task in scpp:
                self.pod_counter +=1
                pod = build_pod([task], str(self.pod_counter).zfill(6))
                _scpp.append(pod)

            dump_multiple_yamls(_scpp, deployment_file)

        return deployment_file


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks: list=[], deployment_file: str=None) -> str:
        
        """
        This function to coordiante the submission of list of tasks.
        to the cluster main node.

        Parameters:
            ctasks (list): a batch of tasks (HYDRAA.Task)

            deployment_file (str): a path for the deployment file.
 
        Returns:
            deployment_file (str) : path for the deployment file.
        """
        if not ctasks and not deployment_file:
            self.logger.error('at least deployment or tasks must be specified')
            return None

        if deployment_file and ctasks:
            self.logger.error('can not submit both deployment and tasks')
            return None

        if ctasks:
            self.profiler.prof('generate_pods_start', uid=self.id)
            deployment_file = self.generate_pods(ctasks)
            self.profiler.prof('generate_pods_stop', uid=self.id)

        if os.path.exists(deployment_file):
            cmd = f'nohup kubectl apply -f {deployment_file} >> {self.sandbox}'
            cmd += '/apply_output.log 2>&1 </dev/null &'

            out, err, ret = sh_callout(cmd, shell=True, kube=self)

            if ret:
                self.logger.error(f'failed to submit {deployment_file} to {self.name}: {err}')

            # FIXME: this should be a message to the controller manager
            else:
                fname = deployment_file.split('/')[-1]
                print('deployment {0} is submitted to {1}'.format(fname, self.name))

            return deployment_file

        else:
            raise FileExistsError(f'failed to find {deployment_file}')
            return None


    # --------------------------------------------------------------------------
    #
    def schedule(self, tasks):

        """
        This function to schedule set of tasks into a smaller batches of tasks
        to fit the Kubernetes cluster node size.

        Parameters:
            tasks (list): a batch of tasks (HYDRAA.Task)
        
        Returns:
            objs_batch (list): a sliced list of list of tasks.

        """

        task_batch = copy.copy(tasks)
        batch_size = len(task_batch)
        if not batch_size:
            raise Exception('Batch size can not be 0')

        # containers per pod
        CPP = math.ceil(self.size['vcpus'] / max(vm.MinCount for vm in self.vms))

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
    def _watch_pods_statuses(self):

        """
        This function starts as a thread to monitor the status of the pods and
        their containers statuses and report them back to the controller manager.

        Parameters:
            None

        Returns:
            statuses (list): a list of list for all of the task statuses.
        """
        w = watch.Watch()

        def _watch():

            try:
                for event in w.stream(client.CoreV1Api().list_namespaced_pod,
                                      'default', _request_timeout=900000):

                    pod = event['object']

                    if pod and pod.status.phase in ['Pending', 'Running', 'Succeeded', 'Failed']:

                        containers = self._process_container_statuses(pod)

                        if containers:
                            self.result_queue.put({'pod_id':pod.metadata.name,
                                                   'pod_status': pod.status.phase,
                                                   'containers': containers})

            except (urllib3.exceptions.ProtocolError, urllib3.exceptions.httplib_IncompleteRead) as e:
                if self.terminate.is_set():
                    self.logger.trace(f'watcher thread recieved stop event')
                    w.stop()
                else:
                    raise e

        watcher = mt.Thread(target=_watch, daemon=True, name='KubeEventWatcher')
        watcher.start()

        self.logger.trace(f'watcher thread {watcher.ident} started on {self.name}')


    # --------------------------------------------------------------------------
    #
    def _process_container_statuses(self, pod):

        # get the pod containers names
        containers = []

        if not pod.status.container_statuses:
            return containers

        for cont in pod.status.container_statuses:
            msg = {}

            if not cont.name.startswith('ctask'):
                continue

            if cont.state.terminated:
                if cont.state.terminated.exit_code:
                    msg = {'id': cont.name,
                           'status': 'Failed',
                           'exception': cont.state.terminated.reason}
                else:
                    msg = {'id': cont.name,
                           'status': 'Completed'}

            elif cont.state.waiting:
                msg = {'id': cont.name,
                       'message': cont.state.waiting.message,
                       'status': 'Pending',
                       'reason': cont.state.waiting.reason}

            elif cont.state.running:
                msg = {'id': cont.name,
                       'status': 'Running'}
            else:
                msg = {'id': cont.name,
                       'status': cont.status.phase}

            if msg:
                containers.append(msg)


        return containers


    # --------------------------------------------------------------------------
    #
    def get_pod_logs(self, task: Task, save: bool = False) -> str:
        """Get the logs of a Kubernetes pod/container and return as a string.

        Parameters
        ----------
        task : Task
            The Task object or name of the task to get logs for
        save : bool, optional
            Whether to save the task logs to a file, by default False
        
        Returns
        -------
        str
            The task logs as a string, or path to saved log file if save=True

        Raises
        ------
        RuntimeError
            If there is an error getting the task logs
            If there is conflict between the task type and the operational mode

        """
        if not isinstance(task, Task):
            raise Exception(f'pod/container task must be an instance of {Task}')

        logs = []
        label = f"task_label={task.pod_name}"
        watcher = watch.Watch()

        # some pods with third party tools like kubeflow or workflows
        # can add suffix to the pod name when they create the pod.
        # so we need to get the pod name from the cluster based on the
        # pod name that is generated by Hydraa only. We use the task name
        # as an internal label of the pod/container (task) to get the logs.

        _pods = client.CoreV1Api().list_namespaced_pod(namespace='default',
                                                       label_selector=label)
        _pod_name = _pods.items[0].metadata.name

        logs = watcher.stream(client.CoreV1Api().read_namespaced_pod_log,
                              name=_pod_name, container=task.name, namespace='default')

        # check if we did get any logs
        if logs:
            if save:
                path = f'{self.sandbox}/{task.pod_name}.{task.name}.logs'
                with open(path, 'w') as f:
                    for log in logs:
                        f.write(log)
                return path
            return '\n'.join(logs)
        else:
            self.logger.error(f'No logs were found for {task.name}')
            return []


    # --------------------------------------------------------------------------
    #
    def get_worker_nodes(self) -> List[str]:
        """
        Get a list of worker node server names.

        Returns
        -------
        List[str]
            A list containing the server names of each worker node VM

        """
        first_vm = self.vms[0]
        server_names = [server.name for server in first_vm.Servers[1:]] + \
        [server.name for vm in self.vms[1:] for server in vm.Servers]

        return server_names


    # --------------------------------------------------------------------------
    #
    def add_node(self, vm):
        if not isinstance(vm, OpenStackVM):
            raise Exception('K8s cluster vm must be of type OpenStackVM')

        """
        1-Provision the new node and ensure it meets the Kubespray
          prerequisites (e.g. SSH access, Python, etc).
        2-Add the new node's IP/hostname to the [all] inventory group
          in inventory/mycluster/hosts.yml.
        3- ansible-playbook -i inventory/mycluster/hosts.yml scale.yml
        4- kubectl uncordon <node-name>
        5- self.vms.append(vm)
        6- self.get_cluster_allocatable_size()
        """
        raise NotImplementedError('adding node to a running K8s cluster'
                                  'is not implemented yet')


    # --------------------------------------------------------------------------
    #
    def get_instance_resources(self, vm):

        vcpus, memory, storage = 0, 0, 0
        if not isinstance(vm, OpenStackVM) and not isinstance(vm, LocalVM):
            raise TypeError(f'vm must be an instance of OpenStackVM or LocalVM')

        vcpus = vm.Servers[0].flavor.vcpus
        memory = vm.Servers[0].flavor.ram
        storage = vm.Servers[0].flavor.disk

        return vcpus, memory, storage


    # --------------------------------------------------------------------------
    #
    def get_cluster_allocatable_size(self) -> Dict[str, int]:
        """
        Get the total allocatable vCPUs, memory, and storage for a cluster.

        This sums the allocatable resources reported by each node in the
        cluster.

        Returns
        -------
        Dict[str, int]
            A dict with keys 'vcpus', 'memory', 'storage' indicating the 
            total allocatable amount of each resource in the cluster.
        """

        size = {'vcpus': 0, 'memory': 0, 'storage': 0}
        for vm in self.vms:
            vm_size = self.get_instance_resources(vm)
            size['vcpus'] += vm_size[0] * vm.MinCount
            size['memory'] += vm_size[1] * vm.MinCount
            size['storage'] += vm_size[2] * vm.MinCount

        return size

    # --------------------------------------------------------------------------
    #
    def restart(self):
        raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def recover(self):
        raise NotImplementedError


    # --------------------------------------------------------------------------
    #
    def shutdown(self):

        self.terminate.set()

        if self.control_plane:
            self.control_plane.close()
            if hasattr(self, 'tunnel'):
                if not self.control_plane.local:
                    self.tunnel.stop()


# --------------------------------------------------------------------------
#
class AKSCluster(K8sCluster):
    """
    Represents a single/multi node Kubernetes cluster
    (Azure Kubernetes Service - AKS).

    This class assumes that:
    1- Your user has the correct permissions for AKS and CLI.
    2- Kubectl is installed.

    Parameters
    ----------
    run_id : str
        The unique identifier for the cluster.

    vms : List[VirtualMachine]
        A list of virtual machine configurations for the cluster nodes.

    sandbox : str
        The sandbox directory for storing cluster-specific files.

    log : Logger
        A logger instance for logging cluster-related information.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, run_id, vms, sandbox, log):
        self.id = run_id
        self.config = None
        self.resource_group = vms[0].ResourceGroup

        super().__init__(run_id, vms, sandbox, log)

        self.name = generate_id(prefix='h-azure-aks-cluster')

        # shutdown resources on exit if something goes wrong
        atexit.register(self.shutdown)


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):
        """
        Bootstrap the EKS cluster with N group nodes (HYDRAA.VM).
    
        Parameters
        ----------
        None
    
        Returns
        -------
        None
        """
        self.profiler.prof('bootstrap_start', uid=self.id)

        if not KUBECTL:
            raise Exception('Kubectl is required to manage AKS cluster')

        first_vm = self.vms[0]
        varied_vms = any(vm.InstanceID != first_vm.InstanceID for vm in \
                         self.vms[1:])

        cmd  = 'az aks create '
        cmd += f'-g {self.resource_group.name} '
        cmd += f'-n {self.name} '
        cmd += '--enable-managed-identity '
        cmd += f'--node-vm-size {first_vm.InstanceID} '
        cmd += f'--node-count {first_vm.MinCount} '
        cmd += f'--generate-ssh-keys --location {first_vm.Region}'

        if KUBE_VERSION:
            version = KUBE_VERSION
            cmd += f' --kubernetes-version {version}'

        worker_nodes = len(self.get_worker_nodes())
        print('building {0} with x [{1}] worker nodes, [{2}] control plane node(s),'
              ' total of [{3}] nodes'.format(self.name, worker_nodes, KUBE_CONTROL_HOSTS,
                                             self.nodes))

        self.config, err, ret = sh_callout(cmd, shell=True, munch=True)

        if ret:
            raise Exception('failed to build {0}: {1}'.format(self.name,
                                                              err))

        # check if we have a single type or multiple types of vms
        if varied_vms and len(self.vms) > 1:
            for vm in self.vms[1:]:
                self.add_node_pool(vm)

        self.profiler.prof('configure_start', uid=self.id)
        self.kube_config = self.configure()
        self.profiler.prof('bootstrap_stop', uid=self.id)

        self.status = READY

        print('{0} is in {1} state'.format(self.name, self.status))


    # --------------------------------------------------------------------------
    #
    def configure(self):
        """
        Configure the AKS cluster by creating a .kube folder and setting up
        kubectl configuration.
    
        Returns
        -------
        str
            The path to the created configuration file.
        """

        self.logger.trace('Creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        self.logger.trace('setting AKS kubeconfig path to: {0}'.format(config_file))

        cmd  = 'az aks get-credentials '
        cmd += '--admin --name {0} '.format(self.name)
        cmd += '--resource-group {0} '.format(self.resource_group.name)
        cmd += '--name {0} '.format(self.name)
        cmd += '--file {0}'.format(config_file)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return config_file


    # --------------------------------------------------------------------------
    #
    def add_node_pool(self, vm):
        """
        Add an node pool to the AKS cluster.

        Parameters
        ----------
        vm : VirtualMachine
            The virtual machine configuration for the node pool.

        Returns
        -------
        bool
            True if the node pool was successfully added, False otherwise.
        """
        # if the user want to add new node pool to the cluster
        # then update the global vm list.
        if vm not in self.vms:
            self.vms.append(vm)

        np_name = 'nodepool{0}'.format(unique_id(starts_from=2))
        self.logger.trace('adding {0} with x [{1}] nodes to {2}' \
                          .format(np_name, vm.MinCount, self.name))

        cmd  = 'az aks nodepool add '
        cmd += '--resource-group {0} '.format(self.resource_group.name)
        cmd += '--cluster-name {0} '.format(self.name)
        cmd += '--name  {0} '.format(np_name)
        cmd += '--node-count {0} '.format(vm.MinCount)
        cmd += '--node-vm-size {0}'.format(vm.InstanceID)

        out, err, ret = sh_callout(cmd, shell=True)
        if ret:
            raise Exception('failed to add {0} to {1}: {2}' \
                            .format(np_name, self.name, err))

        vm.NodesPool = np_name


    # --------------------------------------------------------------------------
    #
    def get_instance_resources(self, vm):
        """
        Retrieve the compute resources of Azure virtual machine instance.

        This function uses the Azure CLI to get VM size in a specified
        region based on instance ID. It extracts the number of virtual CPUs,
        memory, and storage of the VM.

        Parameters:
        -----------
            vm (VirtualMachine): An instance of the VirtualMachine class
                                 containing information about the VM.

        Returns:
        ----------
            tuple: A tuple containing the number of virtual CPUs, memory
                   in MB, and storage size in MB.

        Raises:
        ----------
            Exception: If the function fails to calculate the resources
                       for the specified VM instance.
        """
        if not isinstance(vm, AzureVM):
            raise TypeError(f'vm must be an instance of {AzureVM}')

        cmd = f"az vm list-sizes -l {vm.Region} -o json "
        cmd += f"--query \"[?name=='{vm.InstanceID}']\""

        out, _, _ = sh_callout(cmd, shell=True, munch=True)

        vm_size = out[0]
        vcpus = vm_size.get('numberOfCores', 0)
        memory = vm_size.get('memoryInMb', 0) / 1024 # MB to Gi
        storage = vm_size.get('osDiskSizeInMb', 0)

        if not vcpus and not memory and not storage:
            raise Exception(f'failed to  calaulte {vm.InstanceID} size')

        return vcpus, memory, storage


    # --------------------------------------------------------------------------
    #
    def _delete(self):

        out, err, _ = sh_callout('az aks list', shell=True)
        if self.name in out:
            if not '"provisioningState": "Deleting"' in out:
                print('deleting AKS cluster: {0}'.format(self.name))
                cmd = f'az aks delete --resource-group {self.resource_group.name} '
                cmd += f'--name {self.name} --no-wait --yes'
                out, err, _ = sh_callout(cmd, shell=True)
                print(out, err)

    # --------------------------------------------------------------------------
    #
    def shutdown(self):

        self.terminate.set()
        self._delete()


# --------------------------------------------------------------------------
#
class EKSCluster(K8sCluster):
    """
    Represents a single/multi node AWS Elastic Kubernetes Service (EKS)
    cluster.

    This class assumes that:
    1- eksctl is installed
    2- aws-iam-authenticator is installed
    3- kubectl is installed
    4- In $HOME/.aws/credentials -> AWS credentials

    Parameters
    ----------
    run_id : str
        The unique identifier for the cluster.

    sandbox : str
        The sandbox directory for storing cluster-specific files.

    vms : List[VirtualMachine]
        A list of virtual machine configurations for the cluster nodes.

    iam : IAMClient
        An IAM client for managing IAM roles and policies.

    clf : CloudFormation
        A cloudformation client.

    rclf : ResourceCloudFormation
        A cloudformation resource client.

    ec2 : EC2Client
        An EC2 client for managing EC2 instances and resources.

    eks : EKSClient
        An EKS client for managing EKS clusters.

    prc : PricingClient
        A Pricing client for obtaining pricing information.

    log : Logger
        A logger instance for logging cluster-related information.
    """

    EKSCTL = shutil.which('eksctl')
    IAM_AUTH = shutil.which('aws-iam-authenticator')

    def __init__(self, run_id, sandbox, vms, ec2, log):

        self.id = run_id
        self.config = None
        self.ec2_client = ec2

        super().__init__(run_id, vms, sandbox, log)
        self.name = generate_id(prefix='h-aws-eks-cluster')

        atexit.register(self.shutdown)


    # -------------------------------------------------------------------------
    #
    def bootstrap(self):
        """
        Bootstrap the EKS cluster.

        Parameters
        ----------
        kubernetes_v : str, optional
            The Kubernetes version to use, by default '1.22'.

        Returns
        -------
        None
        """

        ng_name = generate_id(prefix='h-eks-nodegroup')

        # FIXME: Find a way to workaround the limited avilability
        #        zones https://github.com/weaveworks/eksctl/issues/817

        if not self.EKSCTL or not self.IAM_AUTH or not KUBECTL:
            raise Exception('eksctl/iam-auth/kubectl is required to manage EKS cluster')

        self.profiler.prof('bootstrap_start', uid=self.id)

        self.profiler.prof('cofigure_start', uid=self.id)
        self.kube_config = self.configure()

        first_vm = self.vms[0]
        varied_vms = any(vm.InstanceID != first_vm.InstanceID for vm in \
                         self.vms[1:])

        worker_nodes = len(self.get_worker_nodes())
        print('building {0} with x [{1}] worker nodes, [{2}] control plane node(s),'
              ' total of [{3}] nodes'.format(self.name, worker_nodes, KUBE_CONTROL_HOSTS,
                                             self.nodes))

        # step-1 Create EKS cluster control plane
        cmd  = f'{self.EKSCTL} create cluster --name {self.name} '
        cmd += f'--region {first_vm.Region} '
        cmd += f'--nodegroup-name {ng_name} --nodes {first_vm.MinCount} '
        cmd += f'--node-type {first_vm.InstanceID} '
        cmd += f'--nodes-min {first_vm.MinCount} '
        cmd += f'--nodes-max {first_vm.MaxCount} '
        cmd += f'--kubeconfig {self.kube_config}'

        if KUBE_VERSION:
            version = KUBE_VERSION
            cmd += f' --version {version}'

        out, err, ret = sh_callout(cmd, shell=True)

        if ret:
            raise Exception('failed to build {0}: {1}'.format(self.name, err))

        # step-2 Check if we have a single type or multiple types of vms
        if varied_vms and len(self.vms) > 1:
            for vm in self.vms[1:]:
                # step-3 Create EKS cluster node groups for each vm type
                self.add_node_group(vm)

        self.profiler.prof('bootstrap_stop', uid=self.id)
        self.status = READY

        print('{0} is in {1} state'.format(self.name, self.status))


    # --------------------------------------------------------------------------
    #
    def configure(self):
        """
        Configure the EKS cluster by creating a .kube folder
        and a configuration file inside the manager's sandbox.

        Returns
        -------
        str
            The path to the created configuration file.
        """

        self.logger.trace('Creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        self.logger.trace('setting EKS KUBECONFIG path to: {0}'.format(config_file))

        return config_file


    # --------------------------------------------------------------------------
    #
    def add_node_group(self, vm):
        """
        Add a node group to the EKS cluster.

        Parameters
        ----------
        vm : VirtualMachine
            The virtual machine configuration for the node group.

        Returns
        -------
        bool
            True if the node group was successfully added, False otherwise.
        """
        # if the user want to add new node group to the cluster
        # then update the global vm list.
        if vm not in self.vms:
            self.vms.append(vm)

        ng_name = generate_id(prefix='h-eks-nodegroup')
        self.logger.trace('adding node group {0} with x [{1}] nodes to {2}' \
                          .format(ng_name, vm.MinCount, self.name))

        cmd  = f'eksctl create nodegroup --name {ng_name} --nodes {vm.MinCount} '
        cmd += f'--cluster {self.name} --node-type {vm.InstanceID} '
        cmd += f'--nodes-min {vm.MinCount} --nodes-max {vm.MaxCount}'
 
        out, err, ret = sh_callout(cmd, shell=True)
        if ret:
            self.logger.error('failed to add {0} to {1}' \
                              .format(ng_name, self.name))
            print(out, err)

        vm.NodeGroupName = ng_name


    # --------------------------------------------------------------------------
    #
    def get_instance_resources(self, vm):
        """
        Get the number of vCPUs for a specified virtual machine.

        Parameters
        ----------
        vm : VirtualMachine
            The virtual machine configuration for which to determine the vCPUs.

        Returns
        -------
        int
            The number of vCPUs for the specified virtual machine.
        """
        if not isinstance(vm, AwsVM):
            raise TypeError(f'vm must be an instance of {AwsVM}')

        response = self.ec2_client.describe_instance_types(
            InstanceTypes=[vm.InstanceID])
        if response:
            vm_size = response['InstanceTypes'][0]
            vcpus = vm_size['VCpuInfo']['DefaultVCpus']
            memory = vm_size['MemoryInfo']['SizeInMiB'] / 1000 # Mib to Gi

            # some instance types with EBS only do not have storage
            storage = vm_size.get('InstanceStorageInfo', {})
            if storage:
                storage = storage.get('TotalSizeInGB', 0)
            else:
                storage = 0

        return vcpus, memory, storage


    # --------------------------------------------------------------------------
    #
    def _delete(self):

        out, err, ret = sh_callout(f"eksctl get cluster {self.name}", shell=True)
        if not ret:
            print('deleteing EKS cluster: {0}'.format(self.name))
            cmd = f'eksctl delete cluster --name {self.name}'
            out, err, _ = sh_callout(cmd, shell=True)
            print(out, err)


    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        self.terminate.set()
        self._delete()
