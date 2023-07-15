import os
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
    def reconnect(self):

        reconn = self.__connect()

        return reconn


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

        # FIXME: why not?
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

        local_host, local_port = ip_port.split(":", 1)
        open_port = self.find_open_port(local_host)

        server = SSHTunnelForwarder((self.ip, remote_port),
            ssh_username=self.user,
            ssh_private_key=self.key,
            remote_bind_address=(local_host, int(local_port)),
            local_bind_address=(local_host, open_port),)

        server.start()

        self.logger.trace('ssh tunnel is created for {0} on {1}:{2}'.format(self.ip,
                                                             local_host, open_port))

        return server


    # --------------------------------------------------------------------------
    #
    def find_open_port(self, host):
    
        '''
        Check for the HYDRAA_USED_PORTS env var exist, which will hold
        the used port of the tunnelized Kuberentes cluster endpoints.
        if it exists, we update the varaible with the new accuired port.
        '''
        port = 6443 # default port for kubeconfig
        used_ports = eval(os.environ.get('HYDRAA_USED_PORTS', "[]"))
        # safe range of ports to check
        for p in range(49152, 65535):
            try:
                sock = socket.socket()
                # check if the port is open and free
                sock.bind((host, p))
                port = sock.getsockname()[1]
                # sometimes python reports a used port is free
                # regardless if it is open or not
                if port not in used_ports:
                    self.logger.trace("port {0} is open and will be used".format(port))
                    used_ports.append(port)
                    os.environ['HYDRAA_USED_PORTS'] = str(used_ports)
                    break
                else:
                    continue
            except PermissionError as e:
                # only sudo user can bind to this port
                continue
            except OSError as e:
                # already in use
                continue
            except socket.error as e:
                # connection refused (not open)
                continue
            finally:
                sock.close()
        
        return port


    # --------------------------------------------------------------------------
    #
    def close(self):
        self.logger.trace('closing all ssh connections')
        self.conn.close()
