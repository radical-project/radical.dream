import os
import shlex
import string
import random
import logging
import subprocess as sp

from pathlib import Path
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
    logging.basicConfig(filename=path , format='%(asctime)s | %(module)s: %(message)s')
    logging.getLogger(__name__).setLevel("TRACE")

    return logging.getLogger(__name__)


# --------------------------------------------------------------------------
#
def inject_kubeconfig(cmd, kube_config, local_bind_port):
    cmd = cmd.split()
    kube_endpoint = '--server=https://localhost:{0}'.format(local_bind_port)
    kube_skip_tls = '--insecure-skip-tls-verify'
    cmd.insert(1, '{0} {1} --kubeconfig {2}'.format(kube_skip_tls,
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

        pod_container = client.V1Container(name = ctask.name, image = ctask.image,
                    resources = resources, command = ctask.cmd, env = envs)

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

    # santize the json object
    sn_pod = client.ApiClient().sanitize_for_serialization(pod_obj)

    return sn_pod


# --------------------------------------------------------------------------
#
def calculate_kubeflow_workers(nodes, cpn, task):
    # FIXME: The work down need to be part of a
    # ``SCHEDULER``.
    num_workers = 0
    total_cpus = nodes * cpn
    
    if task.cpus > total_cpus:
        print('Insufficient cpus to run container of size {0}'.format(task.cpus))
        return num_workers

    if cpn < task.cpus:
        import math
        num_workers = math.ceil(task.cpus / cpn)
        return num_workers

    elif cpn >= task.cpus:
        num_workers = 1


# --------------------------------------------------------------------------
#
def build_mpi_deployment(mpi_task, fp, slots, launchers, workers):
    import yaml
    loc = os.path.join(os.path.dirname(__file__)).split('utils')[0]
    mpi_kubeflow_template = "{0}config/kubeflow_kubernetes.yaml".format(loc)

    with open(mpi_kubeflow_template, "r") as file:
        kubeflow_temp = yaml.safe_load(file)

    kubeflow_temp['spec']['slotsPerWorker'] = slots
    kubeflow_temp["metadata"]["name"] = mpi_task.name

    worker = kubeflow_temp['spec']['mpiReplicaSpecs']['Worker']
    launcher = kubeflow_temp['spec']['mpiReplicaSpecs']['Launcher']

    worker['replicas'] = workers
    launcher['replicas'] = launchers
    launcher['template']['spec']['containers'][0]['image'] = mpi_task.image
    worker['template']['spec']['containers'][0]['image']   = mpi_task.image
    launcher['template']['spec']['containers'][0]['args']  = mpi_task.cmd

    with open(fp, "w") as file:
        kubeflow_temp = yaml.dump(file)


# --------------------------------------------------------------------------
#
def dump_deployemnt(kube_pods, fp):
    with open(fp, 'w') as f:
        for p in kube_pods:
            print(p, file=f)

    # we are faking a json file here
    with open(fp, "r") as f:
        text = f.read()
        text = text.replace("'", '"')

    with open(fp, "w") as f:
        text = f.write(text)


