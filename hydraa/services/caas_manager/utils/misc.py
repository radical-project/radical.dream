import os
import yaml
import math
import copy
import json
import shlex
import string
import random
import logging
import subprocess as sp

from pathlib import Path
from urllib import request
from kubernetes import client


TRUE=true=True
NONE=null=None
FALSE=false=False

HOME  = str(Path.home())

def sh_callout(cmd, stdout=True, stderr=True, shell=False, env=None, munch=False, kube=None):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''
    if kube:
        cmd = inject_kubeconfig(cmd, kube.kube_config, kube._tunnel.local_bind_port)
    # convert string into arg list if needed
    if hasattr(str, cmd) and \
       not shell: cmd    = shlex.split(cmd)

    if stdout   : stdout = sp.PIPE
    else        : stdout = None

    if stderr   : stderr = sp.PIPE
    else        : stderr = None

    p = sp.Popen(cmd, stdout=stdout, stderr=stderr, shell=shell, env=env)

    if not stdout and not stderr:
        ret = p.wait()
    else:
        stdout, stderr = p.communicate()
        ret            = p.returncode

    if munch:
        if not ret:
            out = eval(stdout.decode("utf-8"))
            # FIXME: return out, stderr, ret
            return out
    else:
        return stdout.decode("utf-8"), stderr.decode("utf-8"), ret


# --------------------------------------------------------------------------
#
def create_sandbox(id):

    main_sandbox = '{0}/hydraa.sandbox.{1}'.format(HOME, id)
    os.mkdir(main_sandbox, 0o777)

    return main_sandbox


# --------------------------------------------------------------------------
#
def logger(path, levelName='TRACE', levelNum=logging.DEBUG - 5, methodName=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    Why?
    using logger with Jupyter notebook or interactivley via IPython
    triggers others **imported packages loggers and then we have to disable
    them one by one. This approach prevents other loggers from intruding on
    our session.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
       raise AttributeError('{} already defined in logging module'.format(levelName))
    if hasattr(logging, methodName):
       raise AttributeError('{} already defined in logging module'.format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
       raise AttributeError('{} already defined in logger class'.format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)
    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)
    logging.basicConfig(filename=path , format='%(asctime)s | %(levelname)s: %(module)s: %(message)s')
    logging.getLogger(__name__).setLevel("TRACE")

    return logging.getLogger(__name__)


# --------------------------------------------------------------------------
#
def inject_kubeconfig(cmd, kube_config, local_bind_port):
    """
    Injects a custom kubeconfig into a kubectl command and modifies
    the endpoint and TLS settings to connect to a locally running
    Kubernetes API server.

    Args:
        cmd (str): The original kubectl command to be modified.
        kube_config (str): The path to the custom kubeconfig file.
        local_bind_port (int): The local port to which the Kubernetes
        API server is bound.

    Returns:
        str: The modified kubectl command with the custom kubeconfig and
        endpoint settings.

    Raises:
        None
    """
    cmd = cmd.split()
    kube_endpoint = '--server=https://localhost:{0}'.format(local_bind_port)
    kube_skip_tls = '--insecure-skip-tls-verify'

    for idx, c in enumerate(cmd):
        if c == 'kubectl':
            break

    cmd.insert(idx+1, '{0} {1} --kubeconfig {2}'.format(kube_skip_tls,
                                                        kube_endpoint,
                                                        kube_config))
    cmd = ' '.join(cmd)

    return cmd


# --------------------------------------------------------------------------
#
def generate_eks_id(prefix="eks", length=8):
    random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    cluster_id = "{0}-{1}".format(prefix, random_string)
    return cluster_id


# --------------------------------------------------------------------------
#
def build_pod(batch: list, pod_id):

    pod_name = "hydraa-pod-{0}".format(pod_id)
    pod_metadata = client.V1ObjectMeta(name = pod_name)

    # build n container(s)
    containers = []
    
    for ctask in batch:
        envs = []
        if ctask.env_var:
            for env in ctask.env_vars:
                pod_env  = client.V1EnvVar(name = env[0], value = env[1])
                envs.append(pod_env)

        pod_cpu = "{0}m".format(ctask.vcpus * 1000)
        pod_mem = "{0}Mi".format(ctask.memory)

        resources=client.V1ResourceRequirements(requests={"cpu": pod_cpu, "memory": pod_mem},
                                                limits={"cpu": pod_cpu, "memory": pod_mem})

        pod_container = client.V1Container(name=ctask.name, image=ctask.image,
                                           args=ctask.args, resources=resources,
                                           command=ctask.cmd, env=envs)

        containers.append(pod_container)

    # feed the containers to the pod object
    if ctask.restart:
        restart_policy = ctask.restart
    else:
        restart_policy = 'Never'

    pod_spec  = client.V1PodSpec(containers=containers, 
                                 restart_policy=restart_policy)

    pod_obj   = client.V1Pod(api_version="v1", kind="Pod",
                    metadata=pod_metadata, spec=pod_spec)

    # sanitize the json object
    sn_pod = client.ApiClient().sanitize_for_serialization(pod_obj)

    return sn_pod


# --------------------------------------------------------------------------
#
def calculate_kubeflow_workers(nodes, cpn, task):
    # FIXME: The work down need to be part of a
    # ``SCHEDULER``.
    num_workers = 0
    total_cpus = nodes * cpn
    
    if task.vcpus > total_cpus:
        print('Insufficient cpus to run container of size {0}'.format(task.vcpus))
        return num_workers

    if cpn < task.vcpus:
        num_workers = math.ceil(task.vcpus / cpn)
        return num_workers

    elif cpn >= task.vcpus:
        num_workers = 1


# --------------------------------------------------------------------------
#
def build_mpi_deployment(mpi_tasks):

    combined_deployments = []
    loc = os.path.join(os.path.dirname(__file__)).split('utils')[0]
    mpi_kubeflow_template = "{0}config/kubeflow_kubernetes.yaml".format(loc)

    kf_template = load_yaml(mpi_kubeflow_template)

    for mpi_task in mpi_tasks:

        kf_task = copy.deepcopy(kf_template)
        kf_task["metadata"]["name"] += "-" + mpi_task.name

        slots = mpi_task.mpi_setup['slots']
        workers = mpi_task.mpi_setup['workers']
       
        # FIXME: this function should be part of class: Kubeflow
        if not mpi_task.mpi_setup["scheduler"]:
            kf_task["metadata"]["labels"] = {"kueue.x-k8s.io/queue-name": "user-queue"}
        else:
            raise NotImplementedError("scheduler specfication not implmented yet")

        kf_task['spec']['slotsPerWorker'] = slots
        worker = kf_task['spec']['mpiReplicaSpecs']['Worker']
        launcher = kf_task['spec']['mpiReplicaSpecs']['Launcher']

        worker['replicas'] = workers
        worker['template']['spec']['containers'][0]['image'] = mpi_task.image
        worker['template']['spec']['containers'][0]['resources']['requests']['cpu'] = slots
        worker['template']['spec']['containers'][0]['resources']['limits']['cpu'] = slots

        cmd_list = mpi_task.cmd.split(" ")
        cmd_list.insert(0, str(mpi_task.vcpus))
        for c in cmd_list:
            launcher['template']['spec']['containers'][0]['args'].append(c)

        launcher['template']['spec']['containers'][0]['name'] = mpi_task.name
        launcher['template']['spec']['containers'][0]['image'] = mpi_task.image

        combined_deployments.append(kf_task)

    return combined_deployments


# --------------------------------------------------------------------------
#
def load_yaml(fp, safe=True):
    with open(fp, "r") as file:
        if not safe:
            yaml_obj = yaml.load(file)
        else:
            yaml_obj = yaml.safe_load(file)
    return yaml_obj

# --------------------------------------------------------------------------
#
def dump_yaml(obj, fp, safe=True):
    with open(fp, "w") as file:
        if not safe:
            yaml.dump(obj, file)
        else:
            yaml.safe_dump(obj, file)


# --------------------------------------------------------------------------
#
def load_multiple_yamls(fp):
    with open(fp, "r") as file:
        yaml_objs = list(yaml.safe_load_all(file))
    return yaml_objs


# --------------------------------------------------------------------------
#
def dump_multiple_yamls(yaml_objects: list, fp):
    # Dump the YAML objects into a single file
    with open(fp, 'w') as file:
        yaml.dump_all(yaml_objects, file)


# --------------------------------------------------------------------------
#
def download_files(urls, destination):    
    destinations = []
    for url in urls:
        try:
            dest = destination + "/" + url.split("/")[-1]
            request.urlretrieve(url, dest)
            destinations.append(dest)
        except Exception as e:
            raise(e)

    return destinations
