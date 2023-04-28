import time
import socket
import fabric

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


    def __connect(self):
        conn = fabric.Connection(self.ip, port=22, user=self.user,
                        connect_kwargs={'key_filename' :self.key})
        self.check_ssh_connection(self.ip)

        return conn
    
    def put(self, local_file, **kwargs):
        self.conn.put(local_file, **kwargs)


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


    def get(self, remote_file, **kwargs):
        self.conn.get(remote_file, **kwargs)


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

    def close(self):
        self.sftp.close()