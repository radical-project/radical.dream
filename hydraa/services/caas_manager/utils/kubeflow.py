import time
from .misc import build_mpi_deployment


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
            task.mpi_setup = [self.num_workers, self.slots_per_worker]
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
    def check(self):
        """
        check if Kubeflow mpi-operator is deployed or not
        """
        cmd = "kubectl get crd"
        res = self.cluster.remote.run(cmd, hide=True)

        if res.return_code:
            self.cluster.logger.error('checking for Kubeflow CRD failed: {0}'.format(res.stderr))
            return False

        if res.stdout:
            if "mpijobs.kubeflow" in res.stdout:
                return True
            else:
                return False


    # --------------------------------------------------------------------------
    #
    def start(self, launcher):

        while True:
            if self.cluster and self.cluster.status =='Ready':
                kf_installed = self.check()
                if kf_installed:
                    return

                kf_cmd = "kubectl create -f "
                kf_cmd += "https://raw.githubusercontent.com/kubeflow/mpi-operator/master/deploy/v2beta1/mpi-operator.yaml"

                self.launcher = launcher
                self.launcher.manager = self.manager
                res = self.cluster.remote.run(kf_cmd)
                break
            else:
                time.sleep(5)

        return self
