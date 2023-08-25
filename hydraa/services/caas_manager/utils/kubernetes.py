import os
import time
import math
import copy
import json
import uuid
import atexit
import shutil
import datetime

import pandas        as pd
import threading     as mt
import radical.utils as ru

from .misc import build_pod
from .misc import sh_callout
from .misc import generate_eks_id
from .misc import dump_multiple_yamls


__author__ = 'Aymen Alsaadi <aymen.alsaadi@rutgers.edu>'

TFORMAT = '%Y-%m-%dT%H:%M:%fZ'

CFAILED_STATE = ['Error', 'StartError','OOMKilled',
                'ContainerCannotRun','DeadlineExceeded',
                'CrashLoopBackOff', 'ImagePullBackOff',
                'RunContainerError','ImageInspectError']

CRUNNING_STATE = ['ContainerCreating', 'Started']
CCOMPLETED_STATE = ['Completed', 'completed']

PFAILED_STATE = ['OutOfCPU','OutOfMemory',
                'Error', 'CrashLoopBackOff',
                'ImagePullBackOff', 'InvalidImageName',
                'CreateContainerConfigError','RunContainerError',
                'OOMKilled','ErrImagePull','Evicted']
SLEEP = 2
BUSY = 'Busy'
READY = 'Ready'
MAX_PODS = 250

KUBECTL = shutil.which('kubectl')

POD = ['pod', 'Pod']
CONTAINER = ['container', 'Container']
TASK_PREFIX = ['hydraa-', 'hydraa-launcher']

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
        The constructor for K8sCluster class.

        Parameters:
            run_id       (str)      : Unique id deliverd by the controller manager.
            vm           (hydraa.vm): A AWS/Azure/OpenStack Hydraa VM.
            sandbox      (str)      : A path for the folder of hydraa manager.
            log          (logging)  : A logger object.

        """
        self.id = run_id
        self.vms = vms
        self.nodes = sum([vm.MinCount for vm in self.vms])
        self.name  = 'cluster-{0}.{1}'.format(self.vms[0].Provider, run_id)
        self.pod_counter = 0
        self.sandbox = sandbox
        self.logger = log
        self.profiler = ru.Profiler(name=__name__, path=self.sandbox)
        self.size = {'vcpus': -1, 'memory': 0, 'storage': 0}
 
        self.kube_config  = None
        self.status = None

        self.stop_event = mt.Event()
        self.watch_profiles = mt.Thread(target=self._profiles_collector)
        self.updater_lock = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def restart(self):
        pass


    # --------------------------------------------------------------------------
    #
    def recover(self):
        pass

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

        print('building {0} with x [{1}] nodes'.format(self.name, self.nodes))

        self.status = BUSY

        self.add_nodes_properity()

        head_node = self.vms[0]

        if not KUBECTL:
            raise Exception('Kubectl is required to manage Kuberentes cluster')
    
        self.profiler.prof('bootstrap_cluster_start', uid=self.id)

        nodes_map = self.create_nodes_map()
        loc = os.path.join(os.path.dirname(__file__)).split('utils')[0]

        boostrapper = "{0}config/bootstrap_kubernetes.sh".format(loc)
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
                self.logger.trace('{0} installation succeeded, logs are under'.format(self.name))
                break
            else:
                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout * 60:
                    raise TimeoutError('failed to build {0} within' \
                                       ' [{1}] min.'.format(self.name, timeout))
                else:
                    self.logger.warning('installation of {0} is still in progress. ' \
                                        'Retrying in 1 minute.'.format(self.name))
                    time.sleep(60)


    # --------------------------------------------------------------------------
    #
    def create_nodes_map(self):
        nodes_map = []
        for vm in self.vms:
            for server in vm.Servers:
                # build then nodes map
                network = server.addresses.values()
                fixed_ip = next(iter(network))[0]['addr']
                hostname_ip = server.name + ',' + fixed_ip
                nodes_map.append(hostname_ip)

                # update the cluster size based on each node
                self.size['vcpus'] += server.flavor.vcpus
                self.size['memory'] += server.flavor.ram
                self.size['storage'] += server.flavor.disk

        # node1,10.0.0.1,192.168.10.1 node2,10.0.0.2 node3,10.0.0.3
        nodes_map = " ".join(str(x) for x in  tuple(nodes_map))

        return nodes_map


    # --------------------------------------------------------------------------
    #
    def generate_pods(self, ctasks):
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

        deployment_file = '{0}/hydraa_pods.yaml'.format(self.sandbox, self.id)

        for ctask in ctasks:
            pod_id = str(self.pod_counter).zfill(6)

            # Single Container Per Pod 
            # (use kubernetes default scheduler here)
            if not ctask.type or ctask.type in POD:
                scpp.append(ctask)

            # Multiple Containers Per Pod. 
            # TODO: use orhestrator.scheduler
            elif ctask.type in CONTAINER:
                mcpp.append(ctask)

        if mcpp:
            _mcpp = []
            # FIXME: use orhestrator.scheduler
            self.profiler.prof('schedule_pods_start', uid=self.id)
            batches = self.schedule(mcpp)
            self.profiler.prof('schedule_pods_stop', uid=self.id)

            for batch in batches:
                pod = build_pod(batch, pod_id)
                _mcpp.append(pod)
                self.pod_counter +=1
            dump_multiple_yamls(_mcpp, deployment_file)

        if scpp:
            _scpp = []
            for task in scpp:
                pod = build_pod([task], pod_id)
                _scpp.append(pod)
                self.pod_counter +=1
            dump_multiple_yamls(_scpp, deployment_file)

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
            cmd = 'nohup kubectl apply -f {0} > {1}/apply_output.log 2>&1 </dev/null &'.\
                   format(deployment_file, self.sandbox)
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

        cmd = "kubectl get pods -A -o json"
        response = None

        response = sh_callout(cmd, shell=True, munch=True, kube=self)

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

            self.logger.trace('profilies collecting thread started')


    # --------------------------------------------------------------------------
    #
    def get_pod_status(self):

        cmd = 'kubectl get pod --field-selector=status.phase=Succeeded -o json'
        response = None

        response = sh_callout(cmd, shell=True, munch=True, kube=self)

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
                            self.logger.trace('Pods did not finish yet or failed')

            return df


    # --------------------------------------------------------------------------
    #
    def get_pod_events(self):
        
        cmd = 'kubectl get events -A -o json' 

        response = sh_callout(cmd, shell=True, munch=True, kube=self)

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

        return df


    # --------------------------------------------------------------------------
    #
    def _profiles_collector(self):
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
        ids = 0

        def collect(ids):
            self.logger.trace('collecting a profiles checkpoint')
            fname = self.sandbox + '/'+'check_profiles.{0}.csv'.format(str(ids).zfill(6))
            df1 = self.get_pod_status()
            df2 = self.get_pod_events()
            df = (pd.merge(df1, df2, on='Task_ID'))

            df.to_csv(fname)
            self.logger.trace('checkpoint profiles saved to {0}'.format(fname))


        # iterate until the stop_event is triggered
        while not self.stop_event.is_set():
            for t in range(3300, 0, -1):
                if t == 1:
                    # save a checkpoint every ~ 55 minutes
                    collect(ids)

                # exit the loop if stop_event is true
                if self.stop_event.is_set():
                    break

                else:
                    time.sleep(SLEEP)

            ids +=1
        # if we exist, then save a checkpoint as well
        collect(ids)


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
    def add_node(self):
        pass


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
    """Represents a single/multi node Kubrenetes cluster.
       This class asssumes that:

       1- Your user has the correct permission for AKS and CLI.
       2- Kubectl is installed
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, run_id, vm, sandbox, log):
        self.id = run_id
        self.config = None
        self.cluster_name = 'hydraa-aks-cluster'
        self.resource_group = vm.ResourceGroup

        super().__init__(run_id, vm, sandbox, log)

        atexit.register(self.shutdown)


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):

        self.profiler.prof('bootstrap_start', uid=self.id)

        if not KUBECTL:
            raise Exception('Kubectl is required to manage AKS cluster')

        # FIXME: remove self.size
        self.size = self.get_vm_size(self.vms[0].InstanceID, self.vms[0].Region) - 1

        first_vm = self.vms[0]
        varied_vms = all(vm.InstanceID == first_vm.InstanceID for vm in self.vms)

        np = {'vm_size': self.vms[0].InstanceID,
              'vm_count': self.vms[0].MinCount}

        cmd  = 'az aks create '
        cmd += '-g {0} '.format(self.resource_group.name)
        cmd += '-n {0} '.format(self.cluster_name)
        cmd += '--enable-managed-identity '
        cmd += '--node-vm-size {0} '.format(np['vm_size'])
        cmd += '--node-count {0} '.format(np['vm_count'])
        cmd += '--generate-ssh-keys'

        print('building {0} with x [{1}] nodes'.format(self.name, self.nodes))
        self.config = sh_callout(cmd, shell=True, munch=True)

        # check if we have a single type or multiple types of vms
        if varied_vms and len(self.vms) > 2:
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
        # To manage a Kubernetes cluster, we use the Kubernetes CLI and kubectl
        # NOTE: kubectl is already installed if you use Azure Cloud Shell.

        self.logger.trace('Creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        self.logger.trace('setting AKS kubeconfig path to: {0}'.format(config_file))

        cmd  = 'az aks get-credentials '
        cmd += '--admin --name {0} '.format(self.cluster_name)
        cmd += '--resource-group {0} '.format(self.resource_group.name)
        cmd += '--name {0} '.format(self.cluster_name)
        cmd += '--file {0}'.format(config_file)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

        return config_file


    # --------------------------------------------------------------------------
    #
    def add_node_pool(self, vm):
        """
        add nodes to an existing cluster
        """
        nodes_pool = 'hydraa-aks-nodepool-{0}'.format(vm.InstanceID)

        cmd  = 'az aks nodepool add '
        cmd += '--resource-group {0} '.format(self.resource_group)
        cmd += '--cluster-name {0} '.format(self.cluster_name)
        cmd += '--name  {0} '.format(nodes_pool)
        cmd += '--node-count {0} '.format(vm.MinCount)
        cmd += '--node-vm-size {0}'.format(vm.InstanceID)

        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)

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

        deployment_file, pods_names, batches = super().submit(ctasks)

        # start the profiles thread
        super().collect_profiles()

        return deployment_file, pods_names, batches
    

    # --------------------------------------------------------------------------
    #
    def _delete(self):

        print('deleteing AKS cluster: {0}'.format(self.cluster_name))
        cmd = 'az aks delete --resource-group {0} --name {0}'.format(self.resource_group,
                                                                       self.cluster_name)
        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)  


    # --------------------------------------------------------------------------
    #
    def shutdown(self):

        self.stop_event.set()
        # self._delete()


# --------------------------------------------------------------------------
#
class EKSCluster(K8sCluster):
    """Represents a single/multi node Elastic Kubrenetes Service cluster.
       This class asssumes that you did the one time
       preparational steps:

       1- eksctl installed 
       2- aws-iam-authenticator is installed 
       3- Kubectl is installed 
       4- In $HOME/.aws/credentials -> aws credentials
    """
    EKSCTL = shutil.which('eksctl')
    IAM_AUTH = shutil.which('aws-iam-authenticator')

    def __init__(self, run_id, sandbox, vm, iam, rclf, clf, ec2, eks, prc, log):

        self.id = run_id
        self.cluster_name = generate_eks_id(prefix='hydraa-eks-cluster')
        self.config = None

        self.iam = iam
        self.clf = clf
        self.ec2 = ec2
        self.eks = eks
        self.rclf = rclf
        self.prc = prc

        super().__init__(run_id, vm, sandbox, log)

        atexit.register(self.shutdown)


    # --------------------------------------------------------------------------
    #
    def bootstrap(self):

        # FIXME: let the user specify the kubernetes_v
        kubernetes_v  = '1.22'
        NodeGroupName = generate_eks_id(prefix='hydraa-eks-nodegroup')

        # FIXME: Find a way to workaround the limited avilability
        #        zones https://github.com/weaveworks/eksctl/issues/817

        if not self.EKSCTL or not self.IAM_AUTH or not KUBECTL:
            raise Exception('eksctl/iam-auth/kubectl is required to manage EKS cluster')

        self.profiler.prof('bootstrap_start', uid=self.id)

        self.profiler.prof('cofigure_start', uid=self.id)
        self.kube_config = self.configure()

        first_vm = self.vms[0]
        varied_vms = all(vm.InstanceID == first_vm.InstanceID for vm in self.vms)

        # FIXME: remove self.size
        self.size = self.get_vm_size(self.vms[0].InstanceID) - 1

        print('building {0} with x [{1}] nodes'.format(self.name, self.nodes))

        # step-1 Create EKS cluster control plane
        cmd  = f'{self.EKSCTL} create cluster --name {self.cluster_name} '
        cmd += f'--region {self.vms.Region} --version {kubernetes_v} '
        cmd += f'--without-nodegroup --kubeconfig {self.kube_config}'

        sh_callout(cmd, shell=True)

        # step-2 Check if we have a single type or multiple types of vms
        if varied_vms and len(self.vms) > 2:
            for vm in self.vms:
                # step-3 Create EKS cluster node groups for each vm type
                self.add_node_group(vm)

        self.profiler.prof('bootstrap_stop', uid=self.id)

        self.status = READY

        print('{0} is in {1} state'.format(self.name, self.status))


    # --------------------------------------------------------------------------
    #
    def configure(self):

        # we isolate the .kube/config for each
        # service to prevent multiple services
        # config overwritting each other
        self.logger.trace('Creating .kube folder')
        config_file = self.sandbox + "/.kube/config"
        os.mkdir(self.sandbox + "/.kube")
        open(config_file, 'x')

        self.logger.trace('setting EKS KUBECONFIG path to: {0}'.format(config_file))

        return config_file


    # --------------------------------------------------------------------------
    #
    def add_node_group(self, vm):

        name = 'hydraa-eks-nodegroup-{0}'.format(vm.InstanceID)

        cmd  = f'eksctl create nodegroup --name {name} --nodes {vm.MinCount} '
        cmd += f'--cluster {self.cluster_name} --node-type {vm.InstanceID} '
        cmd += f'--nodes-min {vm.MinCount} --nodes-max {vm.MaxCount}'
 
        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)


    # --------------------------------------------------------------------------
    #
    def get_vm_size(self, vm_id):
        '''
        Modified version of get_instances() from
        https://github.com/powdahound/ec2instances.info/blob/master/ec2.py
        AWS provides no simple API for retrieving all instance types
        '''
        product_pager = self.prc.get_paginator('get_products')

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
    def submit(self, ctasks):

        deployment_file, pods_names, batches = super().submit(ctasks)

        # start the profiles thread
        super().collect_profiles()

        return deployment_file, pods_names, batches


    # --------------------------------------------------------------------------
    #
    def _delete(self):
        
        print('Deleteing EKS cluster: {0}'.format(self.cluster_name))
        cmd = 'eksctl delete cluster --name {0}'.format(self.cluster_name)
        out, err, _ = sh_callout(cmd, shell=True)

        print(out, err)


    # --------------------------------------------------------------------------
    #
    def shutdown(self):
        self.stop_event.set()
        self._delete()
