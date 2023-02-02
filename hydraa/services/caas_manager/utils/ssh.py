import time
import socket
import fabric


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
    
    def put(self, file):
        self.conn.put(file)
    

    # TODO: pass *args and **kwargs to run method
    def run(self, cmd, hide=False, logger=False):
        if logger:
            run = self.conn.run(cmd, hide=True)
            self.logger.trace('{0}, stderr{1}'.format(run.stdout.split('\n'),
                                                      run.stderr.split('\n')))
        else:
            run = self.conn.run(cmd, hide=hide)

        return run


    def get(self, file):
        self.conn.get(file)


    def check_ssh_connection(self, ip):
        
        print(f"Waiting for SSH connectivity on {ip} ...")
        timeout = 60*2
        start_time = time.perf_counter()
        # Repeatedly try to connect via SSH.
        while True:
            try:
                with socket.create_connection((ip, 22), timeout=timeout):
                    print("Connection successful")
                    break
            except OSError as ex:
                time.sleep(10)
                if time.perf_counter() - start_time >= timeout:
                    print(f"After {timeout} seconds, could not connect via SSH. Please try again.")

    def close(self):
        self.sftp.close()