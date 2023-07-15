import time

from .misc import sh_callout
from .misc import download_files
from .misc import load_multiple_yamls
from .misc import dump_multiple_yamls


# --------------------------------------------------------------------------
#
class Kubeflow:
    """
    A class for managing Kubeflow deployments and operations.

    Args:
        manager (CaasManager): An instance of the hydraa.caas_manager class.

    Attributes:
        manager (CaasManager): An instance of the hydraa.caas_manager class.

    """

    def __init__(self, manager):
        self.manager = manager
        self.cluster = self.manager.cluster


   # --------------------------------------------------------------------------
   #
    def _install_kf_mpi(self):
        """
        Installs the Kubeflow MPI operator.

        Returns:
            str: None

        """
        kf_cmd = "kubectl create -f "
        kf_cmd += "https://raw.githubusercontent.com/kubeflow/mpi-operator" \
                  "/master/deploy/v2beta1/mpi-operator.yaml"
        res = self.cluster.remote.run(kf_cmd, hide=True)


    # --------------------------------------------------------------------------
    #
    def check(self):
        """
        Checks if Kubeflow mpi-operator is deployed or not.

        Returns:
            bool: True if mpi-operator is deployed, False otherwise.

        """
        cmd = "kubectl get crd"
        res = self.cluster.remote.run(cmd, hide=True)

        if res.return_code:
            self.cluster.logger.error('checking for Kubeflow CRD failed: {0}\
                                      '.format(res.stderr))
            return False

        if res.stdout:
            return "mpijobs.kubeflow" in res.stdout


    # --------------------------------------------------------------------------
    #
    def _deploy_scheduler(self, scheduler):
        """
        Deploys the specified MPI scheduler to Kubeflow.

        Args:
            scheduler (str): Name of the MPI scheduler to deploy.

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
        """
        Starts the Kueue job controller in Kubeflow.

        """

        url1 = "https://github.com/kubernetes-sigs/kueue/releases" \
               "/download/v0.4.0/manifests.yaml"
        url2 = "https://raw.githubusercontent.com/kubernetes-sigs/kueue/main" \
               "/site/static/examples/single-clusterqueue-setup.yaml"

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
        for idx, file in enumerate(files):
            out, err, ret = sh_callout("kubectl apply -f {0}".format(file),
                                       shell=True, kube=self.cluster)
            # reprot the error for any command
            if ret:
                self.cluster.logger.error(err)

            # wait for the first command to apply the changes
            if idx == 0:
                time.sleep(10)

        if not ret:
            self.cluster.logger.trace("Kueue is installed on {0}"\
                                      .format(self.cluster.name))


    # --------------------------------------------------------------------------
    #
    def start(self, scheduler=None):
        """
        Starts the Kubeflow deployment with the specified MPI scheduler.

        Args:
            scheduler (str, optional): Name of the MPI scheduler to deploy. Defaults to None.

        """

        while True:
            if self.cluster and self.cluster.status == 'Ready':
                if self.check():
                    self.cluster.logger.trace("Kueue job controller is " \
                                              "already installed on {0}"\
                                              .format(self.cluster.name))
                    return
                self._install_kf_mpi()
                self._deploy_scheduler(scheduler)
                break

            self.cluster.logger.trace("{0} is in {1} state, waiting..."\
                                     .format(self.cluster.name, self.cluster.status))
            time.sleep(5)


    # --------------------------------------------------------------------------
    #
    def submit(self, task):

        self.manager.incoming_q.put(task)


# --------------------------------------------------------------------------
#
class KubeflowMPILauncher(Kubeflow):
    """
    A class for launching MPI (Message Passing Interface) jobs using Kubeflow.

    Args:
        num_workers (int): Number of workers to launch for the MPI job.
        slots_per_worker (int): Number of slots per worker.

    Attributes:
        num_workers (int): Number of workers for the MPI job.
        slots_per_worker (int): Number of slots per worker.

    """

    def __init__(self, manager, num_workers, slots_per_worker, scheduler=None):
        self.num_workers = num_workers
        self.slots_per_worker = slots_per_worker

        super().__init__(manager)

        self.start(scheduler)


    # --------------------------------------------------------------------------
    #
    def launch(self, tasks):
        """
        Launches MPI jobs using Kubeflow. Each task results
        in an isolated MPI pod deployment with 1 launcher
        and N workers.

        Args:
            tasks (list): List of tasks to be launched.

        """
        for task in tasks:
            task.type = 'container.mpi'
            task.mpi_setup = {"workers": self.num_workers,
                              "slots": self.slots_per_worker,
                              "scheduler": ""}
            self.submit(task)


    # --------------------------------------------------------------------------
    #
    def kill(self):
        """
        Kills the MPI job.

        Returns:
            str: None
        """
        cmd = "kubectl delete MPIJob"
        res = self.manager.cluster.remote.run(cmd)


# --------------------------------------------------------------------------
#
class KubeflowTraning(Kubeflow):
    """
    A class for launching training operator provides Kubernetes custom
    resources that makes it easy to run distributed or
    non-distributed TensorFlow/PyTorch/Apache MXNet/XGBoost/MPI
    jobs on Kubernetes

    ref: https://github.com/kubeflow/training-operator
    """
    def __init__(self):
        raise NotImplementedError('KubeflowTraning operator not supported yet')


# --------------------------------------------------------------------------
#
class KubeflowPipelines(Kubeflow):
    """
    A class for Kubeflow pipelines a reusable end-to-end ML
    workflows built using the Kubeflow Pipelines SDK. The
    Kubeflow pipelines service has the following goals:

    * End to end orchestration: enabling and simplifying the
      orchestration of end to end machine learning pipelines
    * Easy experimentation: making it easy for you to try numerous
      ideas and techniques, and manage your various trials/experiments.
    * Easy re-use: enabling you to re-use components and pipelines to
      quickly cobble together end to end solutions, without having to
      re-build each time.

    ref: https://github.com/kubeflow/pipelines
    """
    def __init__(self):
        raise NotImplementedError('KubeflowPipelines operator not supported yet')
