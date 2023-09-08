import os
import re
import time
import yaml
import math
import shlex
import string
import random
import logging
import datetime
import subprocess as sp

from pathlib import Path
from urllib import request
from kubernetes import client

from hydraa import AWS, AZURE


TRUE = true=True
NONE = null=None
FALSE = false=False

HOME = str(Path.home())
TFORMAT = '%Y-%m-%dT%H:%M:%fZ'

AWS_CHAR_LIMIT = 100
AZURE_CHAR_LIMIT = 80


def sh_callout(cmd, stdout=True, stderr=True,
               shell=False, env=None, munch=False, kube=None):
    """
    Execute a shell command and return its output, error, and return code.

    This function executes a shell command and captures its standard output,
    standard error, and return code. It can also process the command output
    to convert it into a Python object (munch).

    Parameters:
    -----------
        cmd (str or list): The command to execute as a string or list of arguments.
        stdout (bool): Capture standard output if True (default: True).
        stderr (bool): Capture standard error if True (default: True).
        shell (bool): Run the command in a shell if True (default: False).
        env (dict): Environment variables to pass to the command (default: None).
        munch (bool): Process output into a dictionary (default: False).
        kube (KubeConfig): An instance of the KubeConfig class (default: None).

    Returns:
    ----------
        tuple: A tuple containing the standard output, standard error, and return
               code of the executed command.

    Raises:
    ----------
        CalledProcessError: If the command exits with a non-zero status code.
    """
    if kube:
        if AWS in kube.provider or AZURE in kube.provider:
            cmd = inject_kubeconfig(cmd, kube.kube_config,
                                    local_bind_port=None)
        else:
            cmd = inject_kubeconfig(cmd, kube.kube_config,
                                    kube._tunnel.local_bind_port)

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
            return out, stderr.decode("utf-8"), ret
        else:
            return stdout.decode("utf-8"), stderr.decode("utf-8"), ret
    else:
        return stdout.decode("utf-8"), stderr.decode("utf-8"), ret


# --------------------------------------------------------------------------
#
def create_sandbox(id, sub=False):

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
def inject_kubeconfig(cmd, kube_config, local_bind_port=None):
    """
    Injects a custom kubeconfig into a kubectl command and modifies
    the endpoint and TLS settings to connect to a locally running
    Kubernetes API server.

    Parameters:
    -----------
        cmd (str): The original kubectl command to be modified.
        kube_config (str): The path to the custom kubeconfig file.
        local_bind_port (int): The local port to which the Kubernetes
        API server is bound.

    Returns:
    ----------
        str: The modified kubectl command with the custom kubeconfig and
        endpoint settings.

    Raises:
    ----------
        None
    """
    cmd = cmd.split()

    for idx, c in enumerate(cmd):
        if 'kubectl' in c:
            # insert port and endpoint settings for JET2 and CHI
            if local_bind_port:
                kube_endpoint = '--server=https://localhost:{0}'.format(local_bind_port)
                kube_skip_tls = '--insecure-skip-tls-verify'
                cmd.insert(idx+1, '{0} {1} --kubeconfig {2}'.format(kube_skip_tls,
                                                                    kube_endpoint,
                                                                    kube_config))

            # insert kubeconfig only for AWS and Azure
            else:
                cmd.insert(idx+1, '--kubeconfig {0}'.format(kube_config))

    cmd = ' '.join(cmd)

    return cmd


# --------------------------------------------------------------------------
#
def generate_id(prefix, length=8):
    random_string = ''.join(random.choices(string.ascii_lowercase + \
                                           string.digits, k=length))
    cluster_id = "{0}-{1}".format(prefix, random_string)
    return cluster_id


# --------------------------------------------------------------------------
#
def build_pod(batch: list, pod_id):

    pod_name = "hydraa-pod-{0}".format(pod_id)
    pod_metadata = client.V1ObjectMeta(name=pod_name,
                                       labels={"task_label": pod_name})

    containers = []
    for ctask in batch:
        envs = []
        if ctask.env_var:
            for env in ctask.env_vars:
                pod_env  = client.V1EnvVar(name=env[0], value=env[1])
                envs.append(pod_env)

        pod_cpu = "{0}m".format(ctask.vcpus * 1000)
        pod_mem = "{0}Mi".format(ctask.memory)

        volume = []
        if ctask.volume:
            volume = client.V1VolumeMount(name=ctask.volume.name+'-workdir',
                                          mount_path=ctask.volume.host_path)

        resources=client.V1ResourceRequirements(requests={"cpu": pod_cpu,
                                                          "memory": pod_mem},
                                                limits={"cpu": pod_cpu,
                                                        "memory": pod_mem})

        container = client.V1Container(name=ctask.name, image=ctask.image,
                                       args=ctask.args, resources=resources,
                                       command=ctask.cmd, env=envs,
                                       volume_mounts=volume)

        containers.append(container)

    # feed the containers to the pod object
    if ctask.restart:
        restart_policy = ctask.restart
    else:
        restart_policy = 'Never'

    pod_spec = client.V1PodSpec(containers=containers, 
                                 restart_policy=restart_policy)
    pod_obj = client.V1Pod(api_version="v1", kind="Pod",
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
def load_yaml(fp, safe=True):
    with open(fp, "r") as file:
        if not safe:
            yaml_obj = yaml.load(file)
        else:
            yaml_obj = yaml.safe_load(file)
    return yaml_obj

# --------------------------------------------------------------------------
#
def dump_yaml(obj, fp, safe=True, **kwargs):
    with open(fp, "w") as file:
        if not safe:
            yaml.dump(obj, file, **kwargs)
        else:
            yaml.safe_dump(obj, file, **kwargs)


# --------------------------------------------------------------------------
#
def load_multiple_yamls(fp):
    with open(fp, "r") as file:
        yaml_objs = list(yaml.safe_load_all(file))
    return yaml_objs


# --------------------------------------------------------------------------
#
def dump_multiple_yamls(yaml_objects: list, fp, **kwargs):
    # Dump the YAML objects into a single file
    with open(fp, 'w') as file:
        yaml.dump_all(yaml_objects, file, **kwargs)


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

# --------------------------------------------------------------------------
#
def convert_time(timestamp):

    t  = datetime.datetime.strptime(timestamp, TFORMAT)
    ts = time.mktime(t.timetuple())

    return ts


# --------------------------------------------------------------------------
#
def unique_id(starts_from=1):
    if not hasattr(unique_id, "counter"):
        unique_id.counter = starts_from
    else:
        unique_id.counter += 1
    return unique_id.counter
