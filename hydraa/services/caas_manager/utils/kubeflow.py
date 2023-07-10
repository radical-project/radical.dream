import time

from .misc import sh_callout
from .misc import download_files
from .misc import load_multiple_yamls
from .misc import dump_multiple_yamls


# --------------------------------------------------------------------------
#
class KubeflowMPILauncher:

    def __init__(self, num_workers, slots_per_worker):
        self.num_workers = num_workers
        self.slots_per_worker = slots_per_worker

    # --------------------------------------------------------------------------
    #
    def launch_mpi_container(self, tasks):

        for task in tasks:
            task.type = 'container.mpi'
            task.mpi_setup = {"workers": self.num_workers,
                              "slots": self.slots_per_worker,
                              "scheduler": ""}
            self.manager.incoming_q.put(task)

    # --------------------------------------------------------------------------
    #
    def kill(self):
        cmd = "kubectl delete MPIJob"
        res = self.manager.cluster.remote.run(cmd)


# --------------------------------------------------------------------------
#
class Kubeflow():

    def __init__(self, manager):
        self.manager = manager
        self.launcher = None
        self.cluster = self.manager.cluster


   # --------------------------------------------------------------------------
   #
    def _install_kf_mpi(self):
        kf_cmd = "kubectl create -f "
        kf_cmd += "https://raw.githubusercontent.com/kubeflow/mpi-operator" \
                  "/master/deploy/v2beta1/mpi-operator.yaml"
        res = self.cluster.remote.run(kf_cmd, hide=True)


    # --------------------------------------------------------------------------
    #
    def check(self):
        """
        check if Kubeflow mpi-operator is deployed or not
        """
        cmd = "kubectl get crd"
        res = self.cluster.remote.run(cmd, hide=True)

        if res.return_code:
            self.cluster.logger.error('checking for Kubeflow CRD failed: {0}\
                                      '.format(res.stderr))
            return False

        if res.stdout:
            if "mpijobs.kubeflow" in res.stdout:
                return True
            else:
                return False


    # --------------------------------------------------------------------------
    #
    def _deploy_scheduler(self, scheduler):
        """
        As of now this function would deploy Kueue scheduler
        TODO: This should be a univeral function to add any
        scheduler to Kubeflow.
        """
        if scheduler:
            self.cluster.logger.error('adding a custom scheduler is not ' \
                                      'supported yet')
        else:
            self.cluster.logger.warning('no MPI scheduler was specified, using ' \
                                        'default Kueue job controller')
            self._start_kueue()


    # --------------------------------------------------------------------------
    #
    def _start_kueue(self):

        url1 = "https://github.com/kubernetes-sigs/kueue/releases" \
               "/download/v0.4.0/manifests.yaml"
        url2 = "https://raw.githubusercontent.com/kubernetes-sigs" \
               "/kueue/main/examples/single-clusterqueue-setup.yaml"

        # download both files to the cluster sandbox
        files = download_files([url1, url2], self.cluster.sandbox)

        # Some jobs need all pods to be running at the same time to operate;
        # for example, synchronized distributed training or MPI-based jobs
        # which require pod-to-pod communication. On a default Kueue configuration,
        # a pair of such jobs may deadlock if the physical availability of resources
        # do not match the configured quotas in Kueue. The same pair of jobs could
        # run to completion if their pods were scheduled sequentially.
        # https://kueue.sigs.k8s.io/docs/tasks/setup_sequential_admission/#enabling-waitforpodsready
        cmd = "sed -i -e 's/#waitForPodsReady/waitForPodsReady/' "
        cmd += "-e 's/#\s*enable:\s*true/  enable: true/' {0}".format(files[0])
        out, err, ret = sh_callout(cmd, shell=True)

        # load the Kueue cluster instances and update
        # the allocatable quota of Kueue instance
        kueue_ki = load_multiple_yamls(files[1])
        kueue_quota = kueue_ki[1]['spec']['resourceGroups'][0]['flavors'][0]['resources']

        # cpu nominalQuota & memory nominalQuota
        # https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/
        kueue_quota[0]['nominalQuota'] = self.cluster.size['vcpus']
        kueue_quota[1]['nominalQuota'] = "{0}Gi".format(self.cluster.size['memory'])

        dump_multiple_yamls(kueue_ki, files[1])

        # FIXME (logic issue): we can not invoke 2 Kubectl commands conccurently as our
        # "inject_kube_config" (invoked by sh_callout) can apply the cluster
        # config to one cmd at a time
        ret = None
        for idx, f in enumerate(files):
            out, err, ret = sh_callout("kubectl apply -f {0}".format(f),
                                        shell=True, kube=self.cluster)
            # reprot the error for any command
            if ret:
                self.cluster.logger.error(err)

            # wait for the first command to apply the changes
            if idx == 0:
                time.sleep(10)

        if not ret:
            self.cluster.logger.trace("Kueue is installed on the target cluster")


    # --------------------------------------------------------------------------
    #
    def start(self, launcher, scheduler=None):

        while True:
            if self.cluster and self.cluster.status =='Ready':
                kf_installed = self.check()
                if kf_installed:
                    return
                self.launcher = launcher
                self._install_kf_mpi()
                self._deploy_scheduler(scheduler)
                self.launcher.manager = self.manager
                break
            else:
                time.sleep(5)

        return self
