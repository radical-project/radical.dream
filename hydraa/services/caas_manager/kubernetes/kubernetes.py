import os
import time
import math
import copy
import atexit
import shutil

import pandas as pd
import threading as mt
import radical.utils as ru

from typing import List
from typing import Dict
from typing import Tuple

from ..utils.misc import build_pod
from ..utils.misc import unique_id
from ..utils.misc import sh_callout
from ..utils.misc import generate_id
from ..utils.misc import convert_time
from ..utils.misc import dump_multiple_yamls


from ....cloud_vm.vm import AwsVM
from ....cloud_vm.vm import AzureVM
from ....cloud_task.task import Task
from ....cloud_vm.vm import OpenStackVM


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

CFAILED_STATE = ['Error', 'StartError','OOMKilled',
                'ContainerCannotRun','DeadlineExceeded',
                'CrashLoopBackOff', 'ImagePullBackOff',
                'RunContainerError','ImageInspectError']

CRUNNING_STATE = ['ContainerCreating', 'Started', 'Running']
CCOMPLETED_STATE = ['Completed', 'completed']

PFAILED_STATE = ['OutOfCPU','OutOfMemory',
                'Error', 'CrashLoopBackOff',
                'ImagePullBackOff', 'InvalidImageName',
                'CreateContainerConfigError','RunContainerError',
                'OOMKilled','ErrImagePull','Evicted']
SLEEP = 2
BUSY = 'Busy'
READY = 'Ready'
MAX_PODS = 110
MAX_POD_LOGS_LENGTH = 1000000

KUBECTL = shutil.which('kubectl')
KUBE_VERSION = os.getenv('KUBE_VERSION')
KUBE_TIMEOUT = os.getenv('KUBE_TIMEOUT') # minutes
KUBE_CONTROL_HOSTS = os.getenv('KUBE_CONTROL_HOSTS', default=1)

POD = ['pod', 'Pod']
CONTAINER = ['container', 'Container']
TASK_PREFIX = ['hydraa', 'ctask']


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
    
    3- Moniter and collect tasks/containers/pods results and 
       performance metrics.
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
        self.kube_config  = None
        self.provider = self.vms[0].Provider
        self.size = self.get_cluster_allocatable_size()
        self.nodes = sum([vm.MinCount for vm in self.vms])
        self.profiler = ru.Profiler(name=__name__, path=self.sandbox)
        self.name = 'cluster-{0}-{1}'.format(self.provider, run_id)

        self.stop_event = mt.Event()
        self.updater_lock = mt.Lock()
        self.watch_profiles = mt.Thread(target=self._profiles_collector)
        

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
        The function to build Kuberentes n nodes (1 master) (n-1) workers
        using n virtual or physical machines and wait for them to finish.

        For each node this function does:

        1- Adding hosts (ip and name) to each node.
        2- Bootstrap Kuberentes on each node.
        3- Wait for each node to become active
        4- Request join token from the master for each worker node.
        5- Join each worker to the master node.
        """

        print('building {0} with x [{1}] nodes and [{2}] control plane' \
              .format(self.name, self.nodes, KUBE_CONTROL_HOSTS))

        self.status = BUSY

        self.add_nodes_properity()

        head_node = self.vms[0]

        if not KUBECTL:
            raise Exception('Kubectl is required to manage Kuberentes cluster')

        if KUBE_TIMEOUT:
            timeout = int(KUBE_TIMEOUT)

        self.profiler.prof('bootstrap_cluster_start', uid=self.id)

        nodes_map = self.create_nodes_map()
        loc = os.path.join(os.path.dirname(__file__))
        boostrapper = "{0}/bootstrap_kubernetes.sh".format(loc)

        self.control_plane.put(boostrapper)

        # bug in fabric: https://github.com/fabric/fabric/issues/323
        remote_ssh_path = '/home/{0}/.ssh'.format(self.control_plane.user)
        remote_ssh_name = head_node.KeyPair[0].split('.ssh/')[-1:][0]
        remote_key_path = remote_ssh_path + '/' + remote_ssh_name

        for key in head_node.KeyPair:
            self.control_plane.put(key, remote=remote_ssh_path, preserve_mode=True)

        # start the bootstraping as a background process.
        bootstrap_cmd = 'chmod +x bootstrap_kubernetes.sh;'
        bootstrap_cmd += 'nohup ./bootstrap_kubernetes.sh '
        bootstrap_cmd += '-m "{0}" -u "{1}" -k "{2}" >& '.format(nodes_map,
                                                                 self.control_plane.user,
                                                                 remote_key_path)
        bootstrap_cmd += '/dev/null < /dev/null &'
        self.control_plane.run(bootstrap_cmd, hide=True, warn=True)
        
        self.wait_for_cluster(timeout)

        self.profiler.prof('bootstrap_cluster_stop', uid=self.id)

        self.kube_config = self.configure()
        self._tunnel = self.control_plane.setup_ssh_tunnel(self.kube_config)

        self.status = READY
        print('{0} is in {1} state'.format(self.name, self.status))


    # --------------------------------------------------------------------------
    #
    def configure(self):

        self.logger.trace('creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        self.logger.trace('setting kubeconfig path to: {0}'.format(config_file))

        self.control_plane.get('.kube/config', local=config_file, preserve_mode=True)

        return config_file


    # --------------------------------------------------------------------------
    #
    def wait_for_cluster(self, timeout):
        start_time = time.time()

        while True:
            check_cluster = self.control_plane.run('kubectl get nodes', warn=True, hide=True)
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
            pods_names      (list): list of generated pods names.
            batches         (list): the actual tasks batches.
        """
        scpp = [] # single container per pod
        mcpp = [] # multiple containers per pod
        deployment_file = '{0}/hydraa_pods_{1}.yaml'.format(self.sandbox,
                                                            unique_id())

        for ctask in ctasks:
            # Single Container Per Pod (SCPP)
            if any([p in ctask.type for p in POD]) or not ctask.type:
                scpp.append(ctask)

            # Multiple Containers Per Pod (MCPP).
            elif any([c in ctask.type for c in CONTAINER]):
                mcpp.append(ctask)

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

        # FIXME: why are we returning two empty lists?
        return deployment_file, [], []


    # --------------------------------------------------------------------------
    #
    def submit(self, ctasks=[], deployment_file=None):
        
        """
        This function to coordiante the submission of list of tasks.
        to the cluster main node.

        Parameters:
            ctasks (list): a batch of tasks (HYDRAA.Task)
        
        Returns:
            deployment_file (str) : path for the deployment file.
            pods_names      (list): list of generated pods names.
            batches         (list): the actual tasks batches.
        """
        batches = []
        pods_names = []
        if not ctasks and not deployment_file:
            self.logger.error('at least deployment or tasks must be specified')
            return None, [], []

        if deployment_file and ctasks:
            self.logger.error('can not submit both deployment and tasks')
            return None, [], []

        if ctasks:
            self.profiler.prof('generate_pods_start', uid=self.id)
            deployment_file, pods_names, batches = self.generate_pods(ctasks)
            self.profiler.prof('generate_pods_stop', uid=self.id)

        if deployment_file:
            cmd = 'nohup kubectl apply -f {0} >> {1}'.format(deployment_file,
                                                            self.sandbox)
            cmd += '/apply_output.log 2>&1 </dev/null &'

            out, err, ret = sh_callout(cmd, shell=True, kube=self)

            msg = 'deployment {0} is created on {1}'.format(deployment_file.split('/')[-1],
                                                            self.name)
            if not ret:
                print(msg)
                self.logger.trace('{0}, deployemnt output is under'
                                  ' apply_output.log'.format(msg))
                return deployment_file, pods_names, batches

            # FIXME: we use nohup, to apply the deployemnt in the 
            # background, so how can we report error if we fail?
            else:
                self.logger.error(err)
                print('failed to submit pods, please check the logs for more info.')

        self.collect_profiles()


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
    def wait_to_finish(self, outgoing_q):

        cmd  = 'kubectl '
        cmd += 'get pod --field-selector=status.phase=Succeeded '
        cmd += '| grep Completed* | wc -l'
        cmd2  = 'kubectl get pods | grep -E "{0}" | wc -l'.format('|'.join(PFAILED_STATE))

        self.profiler.prof('wait_pods_start', uid=self.id)

        old_done  = 0
        old_fail  = 0

        while True:
            done_pods = 0
            fail_pods = 0

            old_done  = done_pods
            old_fail  = fail_pods

            out, err, _ = sh_callout(cmd, shell=True, kube=self)
            out2, err2, _ = sh_callout(cmd2, shell=True, kube=self)

            done_pods = int(out.strip())
            fail_pods = int(out2.strip())

            if done_pods or fail_pods:
                # logic error
                if not self.pod_counter:
                    continue

                if self.pod_counter == int(done_pods):
                    print('{0} pods finished with status "Completed"'.format(done_pods))
                    break

                elif self.pod_counter == int(fail_pods):
                    print('{0} pods failed'.format(fail_pods))
                    break

                elif int(sum([done_pods, fail_pods])) == self.pod_counter:
                    break
                else:
                    if old_done != done_pods or old_fail != fail_pods:
                        msg = {'done': done_pods, 'failed': fail_pods}
                        outgoing_q.put(msg)
                    time.sleep(60)

        self.status = READY

        self.profiler.prof('wait_pods_stop', uid=self.id)

        return True


    # --------------------------------------------------------------------------
    #
    def _get_task_statuses(self, pod_id=None):

        """
        This function to generate a json with the current containers statuses
        and collect STOPPED, RUNNING and FAILED containers to report them back
        to the controller manager.

        Parameters:
            pod_id (str): A name for the pod
        
        Returns:
            statuses (list): a list of list for all of the task statuses.
        """

        # FIXME: use batch labels to get the status of the tasks
        cmd = "kubectl get pods -A -o json"
        response = None

        response, _, _ = sh_callout(cmd, shell=True, munch=True, kube=self)

        statuses   = {}
        stopped    = []
        failed     = []
        running    = []

        if response:
            items = response.get('items', [])
            for item in items:
                if item['kind'] in POD:
                    pod_name = item['metadata'].get('name', '')
                    # in the check, we distinguish hydraa deployed
                    # tasks from any other pods on the same namespace.
                    if any(px in pod_name for px in TASK_PREFIX):
                        already_checked = []
                        # check if this pod completed successfully
                        for cond in item['status'].get('conditions', []):
                            it = item['status']
                            if cond.get('reason', ''):
                                for c in it.get('containerStatuses', []):
                                    if c['name'] in already_checked:
                                        continue
                                    # FIXME: container_msg should be injected in the 
                                    # task object not logger
                                    container_attr = list(c['state'].values())[0]
                                    container_msg  = container_attr.get('message', '')
                                    container_res  = container_attr.get('reason', '')
                                    # case-1 terminated signal
                                    if next(iter(c['state'])) == 'terminated':
                                        if container_res in CCOMPLETED_STATE:
                                            if list(c['state'].values())[0]['exitCode'] == 0:
                                                stopped.append(c['name'])
                                            else:
                                                failed.append(c['name'])
                                                self.logger.trace(container_msg)

                                        if container_res in CFAILED_STATE:
                                           failed.append(c['name'])
                                           self.logger.trace(container_msg)

                                    # case-2 running signal
                                    if next(iter(c['state'])) == 'running':
                                            if container_res in CRUNNING_STATE:
                                                running.append(c['name'])

                                    # case-3 waiting signal
                                    if next(iter(c['state'])) == 'waiting':
                                        if container_res in CRUNNING_STATE:
                                             running.append(c['name'])
                                        elif container_res in CFAILED_STATE:
                                            failed.append(c['name'])
                                            self.logger.trace(container_msg)

                                    already_checked.append(c['name'])

            statuses = {'stopped': stopped, 'failed': failed, 'running':running}

            return statuses


    # --------------------------------------------------------------------------
    #
    def collect_profiles(self):

        if not self.watch_profiles.is_alive():
            self.watch_profiles.daemon = True
            self.watch_profiles.start()

            self.logger.trace(f'profilies collection started on {self.name}')


    # --------------------------------------------------------------------------
    #
    def get_pod_status(self):

        cmd = 'kubectl get pod --field-selector=status.phase=Succeeded -o json'
        response = None

        response, _, _ = sh_callout(cmd, shell=True, munch=True, kube=self)

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
                                start_time = convert_time(v.get('startedAt', 0.0))
                                stop_time  = convert_time(v.get('finishedAt', 0.0))
                                df.loc[i] = (c_name, (kk, v.get('reason', None)), start_time, stop_time)
                                i +=1

                        else:
                            self.logger.trace('Pods did not finish yet or failed')

            return df


    # --------------------------------------------------------------------------
    #
    def get_pod_events(self):
        
        cmd = 'kubectl get events -A -o json' 

        response, _, _ = sh_callout(cmd, shell=True, munch=True, kube=self)

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
                            reason_fts = convert_time(it.get('firstTimestamp', 0.0))
                            reason_lts = convert_time(it.get('lastTimestamp', 0.0))
                            df.loc[id] = (cid, reason, reason_fts, reason_lts)
                            id +=1

        return df


    # --------------------------------------------------------------------------
    #
    def _profiles_collector(self, collect_every=3300):
        """
        Background Thread for Profiles Collection

        This method should be started as a background thread.

        AKS/EKS clusters do not permit modifying ttl-events, causing profiles
        to be deleted from the cluster after > 1 hour, potentially replaced
        by new ones (unless Azure/AWS monitoring is enabled, which incurs cost).

        This function saves profile checkpoints as dataframes approximately
        every 55 minutes, merging them at execution end.
        https://github.com/Azure/AKS/issues/2140

        Parameters:
        ----------
        None

        Returns:
        -------
        None

        Notes:
        ------
        - Operates within an internal loop for periodic collection.
        - Intended for use as a background thread.
        - Utilizes a stop_event to exit the loop and cease collection.

        Example:
        --------
        # Assuming `self` is an instance of the class
        collector_thread = threading.Thread(target=self._profiles_collector)
        collector_thread.start()

        # ...execute other tasks...

        # When done, signal the thread to stop and wait for it to finish
        self.stop_event.set()
        collector_thread.join()
        """
        ids = 0

        def collect(ids):
            fname = self.sandbox + '/'+'check_profiles.{0}.csv'.format(str(ids).zfill(6))
            
            df1 = self.get_pod_status()
            df2 = self.get_pod_events()
            df = (pd.merge(df1, df2, on='Task_ID'))
            df.to_csv(fname)
    
            self.logger.trace('checkpoint profiles saved to {0}'.format(fname))

        # Iterate until the stop_event is triggered
        while not self.stop_event.is_set():
            for t in range(collect_every, 0, -1):
                if t == 1:
                    # Save a checkpoint every ~ 55 minutes
                    collect(ids)

                # Exit the loop if stop_event is true
                if self.stop_event.is_set():
                    break

                else:
                    time.sleep(SLEEP)

            ids +=1
        # Save a checkpoint if the thread exits
        collect(ids)


    # --------------------------------------------------------------------------
    #
    def get_pod_logs(self, task: Task, related_containers: bool = False,
                     save: bool = False) -> str:
        """Get the logs of a Kubernetes pod/container and return as a string.

        Parameters
        ----------
        task : Task
            The Task object or name of the task to get logs for
        save : bool, optional
            Whether to save the task logs to a file, by default False
        related_containers: bool, optional
            Whether to pull the associated containers logs of the
            parent pod of the task container, by default False
        
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

        # some pods with third party tools like kubeflow or workflows
        # can add suffix to the task name when they create the pod.
        # so we need to get the pod name from the cluster based on the
        # pod name that is generated by Hydraa only. We use the task name
        # as an internal label of the pod/container (task) to get the logs.

        logs = []
        task_id = task.id
        pod_name = task.pod_name
        cmd = f'kubectl logs -l task_label='

        # container task, then pull the logs of the container
        if any([c in task.type for c in CONTAINER]):
            if related_containers:
                raise Exception('related containers is only supported'
                                ' for pod tasks')
            if hasattr(task, 'pod_name'):
                cmd = cmd + f'{pod_name} -c {task.name}'
                cmd+=f' --tail={MAX_POD_LOGS_LENGTH}'
                out, err, ret = sh_callout(cmd, shell=True, kube=self)
                logs.append(f'{task.name} logs:\n')
                logs.append(out)
            else:
                raise Exception(f'{task.name} does not have a pod name')

        # pod task, then the task name is the pod name
        elif any([p in task.type for p in POD]) or not task.type:
            # we pull all of the containers in the pod
            if related_containers:
                # get pods containers
                _cmd = f"kubectl get pod {pod_name}"
                _cmd +=" -o jsonpath='{.spec.containers[*].name}'"
                out, err, ret = sh_callout(_cmd, shell=True, kube=self)
                if not ret and out:
                    # iterate on N containers and get logs
                    containers = out.split(' ')
                    if containers:
                        for container in containers:
                            # ignore background containers
                            if not container.startswith('ctask'):
                                continue
                            _cmd = cmd
                            _cmd = _cmd + f'{pod_name} -c {container}'
                            _cmd+= f' --tail={MAX_POD_LOGS_LENGTH}'
                            logs.append(f'{container} logs:\n')
                            out, err, ret = sh_callout(_cmd, shell=True,
                                                       kube=self)
                            if not ret and out:
                                logs.append(out)
                            else:
                                self.logger.error(f'failed to get {pod_name} logs: {err}')
                                return
                    else:
                        self.logger.error(f'no related container(s) found for pod {pod_name}')
                        return
                # if no containers found in the pod then just use the pod name
                elif not ret and not out:
                    cmd = cmd + f'{pod_name} --tail={MAX_POD_LOGS_LENGTH}'
                    out, err, ret = sh_callout(cmd, shell=True, kube=self)
                    if not ret and out:
                        logs.append(f'{task.name} logs:\n')
                        logs.append(out)
                    else:
                        self.logger.error(f'failed to get {pod_name} logs: {err}')
                        return
                # we have an error and we failed
                else:
                    self.logger.error(f'failed to get {pod_name} logs: {err}')
                    return

            # if related containers was not specified then we default to
            # the first container in the pod or whatever logs we get.
            else:
                cmd = cmd + f'{pod_name} --tail={MAX_POD_LOGS_LENGTH}'
                out, err, ret = sh_callout(cmd, shell=True, kube=self)
                if not ret and out:
                    logs.append(f'{task.name} logs:\n')
                    logs.append(out)
                else:
                    self.logger.error(f'failed to get {pod_name} logs: {err}')
                    return

        # we failed for all cases
        else:
            raise Exception(f'Unknow task type {task.type}')

        # check if we did get any logs
        if logs and len(logs) > 1:
            if save:
                path = f'{self.sandbox}/{pod_name}.{task.name}.logs'
                with open(path, 'w') as f:
                    for log in logs:
                        f.write(log)
                return path
            return '\n'.join(logs)
        else:
            return None


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
        if not isinstance(vm, OpenStackVM):
            raise TypeError(f'vm must be an instance of {OpenStackVM}')

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
        size = {'vcpus': -1, 'memory': 0, 'storage': 0}
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
        # nothing to shutdown here besides closing
        # the ssh channels and tunnels
        if self.control_plane:
            self.control_plane.close()
            if hasattr(self, '_tunnel'):
                self._tunnel.stop()


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

        print('building {0} with x [{1}] nodes and [{2}] control plane'\
              .format(self.name, self.nodes, KUBE_CONTROL_HOSTS))
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

        self.stop_event.set()
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

        print('building {0} with x [{1}] nodes and [{2}] control plane' \
              .format(self.name, self.nodes, KUBE_CONTROL_HOSTS))

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

        out, err, ret = sh_callout(f"eksctl get cluster {self.name}",
                                   shell=True)
        if not ret:
            print('deleteing EKS cluster: {0}'.format(self.name))
            cmd = f'eksctl delete cluster --name {self.name}'
            out, err, _ = sh_callout(cmd, shell=True)
            print(out, err)


    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        self.stop_event.set()
        self._delete()
