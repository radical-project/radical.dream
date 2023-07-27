import os

from ..caas_manager.utils.misc import dump_yaml
from ..caas_manager.utils.misc import sh_callout
from ..caas_manager.utils.misc import load_multiple_yamls

PV = 'PersistentVolume'
PVC = 'PersistentVolumeClaim'


# --------------------------------------------------------------------------
#
class Volume:

    """
    Volume class

    This class represents a Kubernetes volume.

    __init__ method

    Args:
        targeted_cluster (Cluster): The targeted cluster.
        kind (str): The type of volume.
        accessModes (list): The access modes for the volume.
        storageClassName (str): The storage class name for the volume.
        name (str): The name of the volume.
        size (str): The size of the volume.
    
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, targeted_cluster, kind, accessModes,
                 storageClassName='standard', name='hydraa-volume-', size='1Gi'):

        self.kind = kind
        self.name = 'hydraa-volume-{0}'.format(self.kind) if not name else name
        self.size = size
        self.accessModes = accessModes
        self.targeted_cluster = targeted_cluster
        self.storageClassName = storageClassName


    # --------------------------------------------------------------------------
    #
    def build(self):
        """
        build method
    
        Returns:
            dict: The Kubernetes volume yaml.
    
        """

        loc = os.path.join(os.path.dirname(__file__)).split('volume-templates')[0]
        v_templates = load_multiple_yamls(loc)

        for t in v_templates:
            if t.get('kind') == self.kind:
                v_template = t
                break

        return v_template


# --------------------------------------------------------------------------
#
class PersistentVolume(Volume):
    """
    PersistentVolume class
    
    This class represents a Kubernetes persistent volume.

    __init__ method

    Args:
        targeted_cluster (Cluster): The targeted cluster.
        accessModes (list): The access modes for the volume.
        volumeMode (str): The volume mode for the volume.
        hostPath (dict): The host path for the volume.
        storageClassName (str): The storage class name for the volume.
        name (str): The name of the volume.
        size (str): The size of the volume.


    """

    def __init__(self, targeted_cluster, accessModes, volumeMode='Filesystem',
                 hostPath={'path': '/data', 'type': 'DirectoryOrCreate'},
                 storageClassName='standard', name='hydraa-volume-', size='1Gi'):
        
        kind = PV
        super().__init__(targeted_cluster, kind, accessModes,
                         storageClassName, name, size)

        self.hostPath = hostPath
        self.volumeMode = volumeMode

        pv_file = self.build_pv()
        sh_callout('kubectl apply -f {0}'.format(pv_file), shell=True,
                   kube=self.targeted_cluster)
        self.targeted_cluster.pv = self


    # --------------------------------------------------------------------------
    #
    def build_pv(self):
        """
        build_pv method

        Returns:
        str: The path to the Kubernetes persistent volume yaml.
        """

        pv  = super().build()
        pv['metadata']['name'] = self.name
        spec = pv['spec']
        spec['capacity']['storage'] = self.size
        spec['volumeMode'] = self.volumeMode
        spec['storageClassName'] = self.storageClassName
        spec['accessModes'] = self.accessModes
        spec['hostPath']['path'] = self.hostPath.get('path')
        spec['hostPath']['type'] = self.hostPath.get('type')

        pv_file = '{0}/{1}-pvc.yaml'.format(self.targeted_cluster.sandbox,
                                            self.targeted_cluster.name)
        dump_yaml(pv, pv_file)

        return pv_file


# --------------------------------------------------------------------------
#
class PersistentVolumeClaim(Volume):
    """
    PersistentVolumeClaim class
    
    This class represents a Kubernetes persistent volume claim.

    __init__ method

    Args:
        targeted_cluster (Cluster): The targeted cluster.
        accessModes (list): The access modes for the volume.
        volumeName (str): The name of the volume.
    
    """
    def __init__(self, targeted_cluster, accessModes, volumeName,
                 storageClassName='standard', name='hydraa-volume-', size='1Gi'):

        kind = PVC
        super().__init__(targeted_cluster, kind, accessModes,
                         storageClassName, name, size)

        self.volumeName = volumeName

        pvc_file = self.build_pvc()
        sh_callout('kubectl apply -f {0}'.format(pvc_file), shell=True,
                   kube=self.targeted_cluster)

        self.targeted_cluster.pvc = self

    # --------------------------------------------------------------------------
    #
    def build_pvc(self):

        """
        build_pvc method

        Returns:
        str: The path to the Kubernetes persistent claim volume yaml.
        """

        pvc = super().build()
        pvc['metadata']['name'] = self.name
        spec = pvc['spec']
        spec['volumeName'] = self.volumeName
        spec['storageClassName'] = self.storageClassName
        spec['accessModes'] = self.accessModes
        spec['resources']['requests']['storage'] = self.size

        pvc_file = '{0}/{1}-pvc.yaml'.format(self.targeted_cluster.sandbox,
                                             self.targeted_cluster.name)
        dump_yaml(pvc, pvc_file)

        return pvc_file
