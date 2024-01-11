import os
import time
import threading as mt
from datetime import datetime, timedelta

from hydraa.services.maas_manager.utils.misc import sh_callout
from hydraa.services.maas_manager.utils.misc import download_files
from hydraa.services.maas_manager.utils.misc import dump_multiple_yamls
from hydraa.services.maas_manager.utils.misc import load_multiple_yamls

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



class KuberentesResourceWatcher(ResourceWatcher):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cluster, logger):

        mt.Thread.__init__(self, name='KuberentesResourceWatcher')
        self.daemon = True
        self.logger = logger
        self.cluster = cluster
        self.terminate = mt.Event()
        self.watcher_output_path = self.cluster.sandbox + '/kuberentes_watcher.csv'


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


    # --------------------------------------------------------------------------
    #
    def _run(self):

        self._start_mterics_server()
        self.logger.info(f'Metrics server started on {self.cluster.name}')

        loc = os.path.join(os.path.dirname(__file__))
        kube_resource_watcher = os.path.join(loc, 'kuberentes_watcher.sh')

        cmd ='chmod +x kuberentes_watcher.sh && '
        cmd += f'./kuberentes_watcher.sh -fp {self.watcher_output_path} &'

        out, err, ret = sh_callout(cmd, shell=True, kube=self.cluster, munch=True)

        if ret:
            raise RuntimeError(f'Internal Error from KuberentesResourceWatcher: {err}')

        self.watcher_pid = out

        self.logger.info(f'KuberentesResourceWatcher started on {self.cluster.name}')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        cmd = f'kill -9 {self.watcher_pid}'
        sh_callout(cmd, shell=True)
        self.stop()
