import os
import shlex
import subprocess as sp

from pathlib import Path


null  = None
true  = True
false = False
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



def create_sandbox(id):

    main_sandbox = '{0}/hydraa.sandbox.{1}'.format(HOME, id)
    os.mkdir(main_sandbox, 0o777)

    return main_sandbox