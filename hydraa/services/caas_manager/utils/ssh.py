import time
import socket
import fabric
from .misc import sh_callout
from sshtunnel import SSHTunnelForwarder

TRUE=true=True
NONE=null=None
FALSE=false=False

class Remote:
    def __init__(self, vm_keys, user, fip, log):

        self.ip     = fip        # public ip
        self.user   = user       # user name
        self.key    = vm_keys[0] # path to the private key
        self.logger = log
        self.conn   = self.__connect()


    # --------------------------------------------------------------------------
    #
    def __connect(self):
        conn = fabric.Connection(self.ip, port=22, user=self.user,
                        connect_kwargs={'key_filename' :self.key})
        self.check_ssh_connection(self.ip)

        return conn


    # --------------------------------------------------------------------------
    #
    def put(self, local_file, **kwargs):
        self.conn.put(local_file, **kwargs)


    # --------------------------------------------------------------------------
    #
    # TODO: pass *args and **kwargs to run method
    def run(self, cmd, munch=False, hide=False, logger=False, **kwargs):

        '''
        munch: only use it when you have
        a json output need to be processed
        into a json object
        '''

        if logger and munch:
            raise Exception('can not munch a loggable value')

        # supress the output and 
        # redirect both cmd out/err to the logger
        if logger:
            run = self.conn.run(cmd, hide=True, **kwargs)
            if run.stdout:
                out = run.stdout.split('\n')
                for l in out:
                    self.logger.trace(l)

            if run.stderr:
                err = run.stderr.split('\n')
                for l in err:
                    self.logger.trace(l)
            return run
        
        # otherwise let fabric.run prints
        # the stdout/stderr by default
        elif munch:
            false = False
            true  = True
            null  = None
            run = self.conn.run(cmd, hide=hide, **kwargs)
            val = eval(''.join(run.stdout.split('\n')))
            return val
        else:
            run = self.conn.run(cmd, hide=hide, **kwargs)
            return run


    # --------------------------------------------------------------------------
    #
    def get(self, remote_file, **kwargs):
        self.conn.get(remote_file, **kwargs)


    # --------------------------------------------------------------------------
    #
    def check_ssh_connection(self, ip):
        
        self.logger.trace("waiting for ssh connectivity on {0}".format(ip))
        timeout = 60 * 2
        start_time = time.perf_counter()
        # repeatedly try to connect via ssh.
        while True:
            try:
                with socket.create_connection((ip, 22), timeout=timeout):
                    self.logger.trace("ssh connection successful to {0}".format(ip))
                    break
            except OSError as ex:
                time.sleep(10)
                if time.perf_counter() - start_time >= timeout:
                    self.logger.trace("could not connect via ssh after waiting for {0} seconds, ".format(timeout))


    # --------------------------------------------------------------------------
    #
    def setup_ssh_tunnel(self, kube_config):
        # default Kube API service port
        out, err, ret = sh_callout('grep "server: https://" {0}'.format(kube_config),
                                                                          shell=True)

        if ret:
            raise Exception('failed to fetch kubectl config ip: {0}'.format(err))

        ip_port = out.strip().split('server: https://')[1]
        remote_port = 22
        # the local host is always set to 127.0.0.1.
        # FIXME: what if another cluster has to bind on the same port?
        # we need to bind the same port to another local ip, example: 127.0.0.2
        local_host  = ip_port.split(':')[0]
        local_port  = int(ip_port.split(':')[1])

        server = SSHTunnelForwarder((self.ip, remote_port),
            ssh_username=self.user,
            ssh_private_key=self.key,
            remote_bind_address=(local_host, local_port),
            local_bind_address=(local_host, local_port),)

        server.start()

        self.logger.trace('ssh tunnel is created for {0} on {1}'.format(self.ip, ip_port))

        return server


    # --------------------------------------------------------------------------
    #
    def close(self):
        self.logger.trace('closing all ssh connections')
        self.conn.close()
