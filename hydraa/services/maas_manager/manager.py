import os
import csv
import time
import threading as mt

from kubernetes import client, config

from hydraa.services.caas_manager.utils.misc import sh_callout
from hydraa.services.caas_manager.utils.misc import download_files
from hydraa.services.caas_manager.utils.misc import dump_multiple_yamls
from hydraa.services.caas_manager.utils.misc import load_multiple_yamls


# --------------------------------------------------------------------------
#
class ResourceWatcher(mt.Thread):


    # --------------------------------------------------------------------------
    #
    def __init__(self, watcher_name):
        
        mt.Thread.__init__(self, name=watcher_name)
        self.daemon = True
        self.terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def run(self):
        pass


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._stop_event.set()


# --------------------------------------------------------------------------
#
class KuberentesResourceWatcher(ResourceWatcher):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cluster, logger, watch_pods_resources=False):

        mt.Thread.__init__(self, name='KuberentesResourceWatcher')
        self.daemon = True
        self.logger = logger
        self.cluster = cluster
        self.terminate = mt.Event()
        self.watch_pods_resources = watch_pods_resources
        self.watcher_output_path = self.cluster.sandbox + '/nodes_resources.csv'


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            self._run()
        except Exception:
            self.logger.error('Error running KuberentesResourceWatcher')
            raise


    # --------------------------------------------------------------------------
    #
    def _start_mterics_server(self):

        file = 'https://github.com/kubernetes-sigs/metrics-server/'
        file+= 'releases/latest/download/components.yaml'
        fpath = download_files(urls=[file], destination=self.cluster.sandbox)[0]

        yamls = load_multiple_yamls(fpath)
        for yaml in yamls:
            if yaml['kind'] == 'Deployment':
                break

        command = ['/metrics-server', '--kubelet-insecure-tls',
                   '--kubelet-preferred-address-types=InternalIP']

        yaml['spec']['template']['spec']['containers'][0]['command'] = command

        dump_multiple_yamls(yamls, fpath)

        out, err, ret = sh_callout(f'kubectl apply -f {fpath}', shell=True,
                                   kube=self.cluster)
        
        if ret:
            raise RuntimeError(f'Error starting metrics server: {err}')


    # --------------------------------------------------------------------------
    #
    def _run(self):

        self._start_mterics_server()
        self.logger.info(f'Metrics server started on {self.cluster.name}')

        loc = os.path.join(os.path.dirname(__file__))
        kube_resource_watcher = os.path.join(loc, 'kuberentes_watcher.sh')

        cmd =f'chmod +x {kube_resource_watcher} && '
        cmd += f'{kube_resource_watcher} -f {self.watcher_output_path} &'

        out, err, ret = sh_callout(cmd, shell=True, kube=self.cluster)

        if ret:
            raise RuntimeError(f'Internal Error from KuberentesResourceWatcher: {err}')

        # FIXME: how to get the pid of the process started in the background?
        # in order to kill it when the service is stopped
        self.watcher_pid = out

        self.logger.info(f'Kuberentes Cluster Resource Watcher started on {self.cluster.name}')
    
        # starts the pod resources watcher thread
        if self.watch_pods_resources:
            self._watch_pods_resources()


    # --------------------------------------------------------------------------
    #
    def _watch_pods_resources(self):

        v1 = client.CoreV1Api()
        api = client.CustomObjectsApi()
        config.load_kube_config(self.cluster.kube_config)

        header = ['TimeStamp', 'PodName', 'ContainerName','CPUsUsageN', 'MemoryUsageMB']

        output_file = self.cluster.sandbox + '/pods_resources.csv'

        def write_to_csv(rows_to_write, output_file, single_row=False):
            """
            writes the rows to the csv file

            :param rows_to_write: list of rows (lists) to write
            :param output_file: path to the output file

            """
            with open(output_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if single_row:
                    writer.writerow(rows_to_write)
                else:
                    writer.writerows(rows_to_write)


        def watch():

            write_to_csv([header], output_file, single_row=True)

            while not self.terminate.is_set():
                rows_to_write = []
                for ns in ['default']:
                    resource = api.list_namespaced_custom_object(namespace=ns,
                                                                 plural="pods",
                                                                 version="v1beta1",
                                                                 group="metrics.k8s.io")
                    for pod in resource["items"]:
                        pod_name = pod['metadata']['name']
                        if not pod_name.startswith('hydraa'):
                            continue

                        for container in pod['containers']:
                            container_name = container['name']
                            cpu_usage_n = container['usage']['cpu']
                            mem_usage_mb = container['usage']['memory']

                            rows_to_write.append([time.time(), pod_name, container_name,
                                                  cpu_usage_n, mem_usage_mb])

                if rows_to_write:
                    if len(rows_to_write) == 1:
                        rows_to_write = rows_to_write[0]
                    write_to_csv(rows_to_write, output_file)

                time.sleep(1)

        watcher = mt.Thread(target=watch)
        watcher.start()
        self.logger.info(f'Kuberentes Pods Resource Watcher started on {self.cluster.name}')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        cmd = f'kill -9 {self.watcher_pid}'
        sh_callout(cmd, shell=True)
        self.terminate.set()
        self.stop()
