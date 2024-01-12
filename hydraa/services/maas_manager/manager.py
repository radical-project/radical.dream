import os
import csv
import time
import urllib3
import threading as mt

from kubernetes import client, config
from kubernetes.client.rest import ApiException

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
        self.terminate.set()



# --------------------------------------------------------------------------
#
class KuberentesResourceWatcher(ResourceWatcher):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cluster, logger, watch_pods_resources=False):

        super().__init__('KuberentesResourceWatcher')
        self.daemon = True
        self.logger = logger
        self.cluster = cluster
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

        self._watch_nodes_resources()
    
        # starts the pod resources watcher thread
        if self.watch_pods_resources:
            self._watch_pods_resources()


    # --------------------------------------------------------------------------
    #
    def _watch_nodes_resources(self):
        
        v1 = client.CoreV1Api()
        api = client.CustomObjectsApi()
        config.load_kube_config(self.cluster.kube_config)  

        header = ['TimeStamp', 'NodeName','CPUsCapacity', 'CPUsUsage(M)',
                  'MemoryCapacity(Ki)', 'MemoryUsage(Ki)']

        def _watch():

            self.write_to_csv(header, self.watcher_output_path, single_row=True)

            while not self.terminate.is_set():
                rows_to_write = []
                nodes = custom_api.list_cluster_custom_object(plural="nodes",
                                                              version="v1beta1",
                                                              group="metrics.k8s.io")
                for node in nodes['items']:
                    node_name = node['metadata']['name']
                    cpu_usage = node['usage']['cpu']
                    memory_usage = node['usage']['memory']

                    node = v1.read_node_status(node_name)
                    cpu_capacity = node.status.capacity['cpu']
                    memory_capacity = node.status.capacity['memory']

                    rows_to_write.append([time.time(), node_name, cpu_capacity,
                                          cpu_usage, memory_capacity, memory_usage])

                if rows_to_write:
                    self.write_to_csv(rows_to_write, self.watcher_output_path)

                time.sleep(2)

        watcher = mt.Thread(target=_watch, name='NodesResourceWatcher', daemon=True)
        watcher.start()

        self.logger.info(f'nodes resource watcher thread started on {self.cluster.name}')



    # --------------------------------------------------------------------------
    #
    def _watch_pods_resources(self):

        v1 = client.CoreV1Api()
        api = client.CustomObjectsApi()
        config.load_kube_config(self.cluster.kube_config)

        header = ['TimeStamp', 'PodName', 'ContainerName','CPUsUsage(M)',
                  'MemoryUsage(Ki)']

        output_file = self.cluster.sandbox + '/pods_resources.csv'

        def _watch():

            self.write_to_csv(header, output_file, single_row=True)

            while not self.terminate.is_set():
                rows_to_write = []
                for ns in ['default']:
                    try:
                        resource = api.list_namespaced_custom_object(namespace=ns,
                                                                     plural="pods",
                                                                     version="v1beta1",
                                                                     group="metrics.k8s.io")

                    except (ApiException, urllib3.exceptions.MaxRetryError) as e:
                        # https://github.com/kubernetes-client/python/issues/1173
                        if isinstance(e, ApiException) and e.status == 503:
                            self.logger.warning('Metrics server not available yet, retrying in 1s')
                            time.sleep(1)
                            continue

                        elif self.terminate.is_set():
                            self.logger.trace(f'Pods resource watcher thread recieved stop event')
                            break

                        else:
                            raise e

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
                    self.write_to_csv(rows_to_write, output_file)

                time.sleep(1)

        watcher = mt.Thread(target=_watch, name='PodsResourceWatcher', daemon=True)
        watcher.start()

        self.logger.info(f'pods resource watcher thread started on {self.cluster.name}')



    # --------------------------------------------------------------------------
    #
    def write_to_csv(self, rows_to_write, output_file, single_row=False):
        """
        writes the rows to the csv file

        :param rows_to_write: list of rows (lists) to write
        :param output_file: path to the output file
        :param single_row: if True, writes a single row to the csv file

        """
        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if single_row:
                writer.writerow(rows_to_write)
            else:
                writer.writerows(rows_to_write)
