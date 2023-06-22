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
    def launch_mpi_container(self, task):

        deployment_path = '{0}/kubeflow_pods.json'.format(self.cluster.sandbox,
                                                          self.cluster.id)
        build_mpi_deployment(mpi_task=task, fp=deployment_path,
                            slots=self.slots_per_worker, workers=self.num_workers)
        
        self.cluster.pod_counter += 1
        self.cluster.submit(deployment_file=deployment_path)
    

    # --------------------------------------------------------------------------
    #
    def kill(self):
        cmd = "kubectl delete MPIJob"
        res = self.cluster.remote.run(cmd)


# --------------------------------------------------------------------------
#
class Kubeflow():

    def __init__(self, cluster, launcher):
        self.cluster = cluster
        self.launcher = launcher


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
    def __enter__(self):

        while True:
            if self.cluster and self.cluster.status =='Ready':
                kf_installed = self.check()
                if kf_installed:
                    return

                kf_cmd = "kubectl create -f "
                kf_cmd += "https://raw.githubusercontent.com/kubeflow/mpi-operator/master/deploy/v2beta1/mpi-operator.yaml"

                self.launcher.cluster = self.cluster
                res = self.cluster.remote.run(kf_cmd)
                break
            else:
                time.sleep(5)

        return self


    # --------------------------------------------------------------------------
    #
    def __exit__(self, type, value, traceback):
        pass
