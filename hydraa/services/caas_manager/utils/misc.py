import os
import shlex
import string
import random
import logging
import subprocess as sp

from pathlib import Path


TRUE=true=True
NONE=null=None
FALSE=false=False

HOME  = str(Path.home())

def sh_callout(cmd, stdout=True, stderr=True, shell=False, env=None, munch=False):
    '''
    call a shell command, return `[stdout, stderr, retval]`.
    '''

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
