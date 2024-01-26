import os
import sys
import shutil
import threading

from .exceptions import RcloneException
from ..caas_manager.utils.misc import sh_callout


if os.environ.get('RCLONE_PATH'):
    RCLONE_PATH = os.environ.get('RCLONE_PATH')
else: RCLONE_PATH = shutil.which('rclone')


# --------------------------------------------------------------------------
#
class DataManager:
    """
    DataManager: is a class that provides an interface to different data
    management backends (we support RClone only for now). It provides a set
    of methods that can be used to perform data management operations on the
    backend.

    The current data movement topology that we support is as follows:

    --------------------              -----------------              --------------------
    |                  |              |    Hydraa     |              |                  |
    |    Data Source   | <---File---> |  Data Manager | <---File---> |    Data Target   |
    |  HPC/Local/Cloud |              |               |              |  HPC/Local/Cloud |
    --------------------              -----------------              --------------------


    Future data movement topology that we will support is as follows:

    --------------------             -----------------             --------------------
    |                  |             |    Hydraa     |             |                  |
    |    Data Source   |             |  Data Manager |             |    Data Target   |
    |  HPC/Local/Cloud |             |               |             |  HPC/Local/Cloud |
    --------------------             -----------------             --------------------
             |                                |                               |
             |                                |                               |
             |<------------------------------CMD----------------------------->|
             |                                |                               |
    --------------------                      |                      --------------------
    |     Cloud VM     |<-------------------Files------------------->|     Cloud VM     |
    --------------------                                             --------------------

    NOTE:
    + Hydra Data Manager can be located on the same machine as the Data Source
      or Data Target or on a different machine.

    + Data Source and Data Target can be any of the following:
      - Local File System
      - SFTP File System
      - Remote File System
      - Cloud Storage (S3, azure, google drive, box, dropbox, s3, google cloud
                       storage, LFS, NFS, etc.)

    NOTE: to Setup RClone for the first time with OpenStack ObjectStore
          (Swift) that uses S3api backend, you must:
    1- create `openstack ec2 credentials`
    2- check the creds. `openstack ec2 credentials list`
    3- Where the values for aws_access_key_id and aws_secret_access_key
       correspond to the aforementioned OpenStack fields access and secret,
       respectively.

    4- Make sure that there is no Region or LocationRestration entry in the
       config file.
    """
    def __init__(self, provider, logger):
        
        self.logger = logger
        self.provider = provider

        if not RCLONE_PATH:
            raise RcloneException('DataManager requires rclone to be installed.'
                                  ' Make sure rclone is in the PATH or please set '
                                  ' the RCLONE_PATH environment variable.', None)


    # --------------------------------------------------------------------------
    #
    def _rclone_invoke(self, rclone_cmd, *args):
        """
        Invoke an ``rclone`` command with the specified arguments.

        Args:
            rclone_cmd (str): The RClone command to be executed.
            *args (str): Additional arguments for the RClone command.

        Raises:
            RcloneException: If the RClone command execution returns a non-zero
                status. The exception includes details about the error.

        Returns:
            None
        """
        cmd = [RCLONE_PATH, rclone_cmd]
        cmd.extend(args)

        cmd = " ".join(cmd)

        out, err, ret = sh_callout(cmd, shell=True)

        if ret:
           reason = self._process_error_message(err)
           raise RcloneException(reason, err)
        else:
            print(out)
            rclone_op_invoker = sys._getframe().f_back.f_code.co_name
            print(f'{rclone_op_invoker} operation completed successfully.')


    # --------------------------------------------------------------------------
    #
    def _process_error_message(self, message):
        message_body = [item for item in message.split('\n') if item != ""]
        
        return message_body[-1]


    # --------------------------------------------------------------------------
    #
    def configure(self):
        """
        Print information about configuring RClone using the command line.

        Returns:
            None
        """
        print('Configuring RClone must be done via the command line by calling "rclone config"')
        return


    # --------------------------------------------------------------------------
    #
    def copy(self, source, destination, *args):
        """
        Copy files from the source to the destination using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('copy', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def sync(self, source, destination, *args):
        """
        Make source and destination identical by modifying the destination using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('sync', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def move(self, source, destination, *args):
        """
        Move files from the source to the destination using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('move', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def delete(self, path, *args):
        """
        Remove the contents of the specified path using RClone.

        Args:
            path (str): Path to be deleted.

        Returns:
            None
        """
        self._rclone_invoke('delete', path, *args)


    # --------------------------------------------------------------------------
    #
    def purge(self, path, *args):
        """
        Remove the specified path and all of its contents using RClone.

        Args:
            path (str): Path to be purged.

        Returns:
            None
        """
        self._rclone_invoke('purge', path, *args)


    # --------------------------------------------------------------------------
    #
    def mkdir(self, path, *args):
        """
        Make the specified path if it doesn't already exist using RClone.

        Args:
            path (str): Path to be created.

        Returns:
            None
        """
        self._rclone_invoke('mkdir', path, *args)


    # --------------------------------------------------------------------------
    #
    def rmdir(self, path, *args):
        """
        Remove the specified path using RClone.

        Args:
            path (str): Path to be removed.

        Returns:
            None
        """
        self._rclone_invoke('rmdir', path, *args)


    # --------------------------------------------------------------------------
    #
    def rmdirs(self, path, *args):
        """
        Remove any empty directories under the specified path using RClone.

        Args:
            path (str): Path containing empty directories to be removed.

        Returns:
            None
        """
        self._rclone_invoke('rmdirs', path, *args)


    # --------------------------------------------------------------------------
    #
    def check(self, source, destination, *args):
        """
        Check if the files in the source and destination match using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('check', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def ls(self, path, *args):
        """
        List all the objects in the specified path with size and path using RClone.

        Args:
            path (str): Path to be listed.

        Returns:
            None
        """
        self._rclone_invoke('ls', path, *args)


    # --------------------------------------------------------------------------
    #
    def lsd(self, path, *args):
        """
        List all directories/containers/buckets in the specified path using RClone.

        Args:
            path (str): Path to be listed.

        Returns:
            None
        """
        self._rclone_invoke('lsd', path, *args)


    # --------------------------------------------------------------------------
    #
    def lsl(self, path, *args):
        """
        List all the objects in the specified path with size, modification time, and path using RClone.

        Args:
            path (str): Path to be listed.

        Returns:
            None
        """
        self._rclone_invoke('lsl', path,*args)


    # --------------------------------------------------------------------------
    #
    def md5sum(self, path):
        """
        Produce an md5sum file for all the objects in the specified path using RClone.

        Args:
            path (str): Path for which md5sum should be produced.

        Returns:
            None
        """
        self._rclone_invoke('md5sum', path, *args)


    # --------------------------------------------------------------------------
    #
    def sha1sum(self, path, *args):
        """
        Produce a sha1sum file for all the objects in the specified path using RClone.

        Args:
            path (str): Path for which sha1sum should be produced.

        Returns:
            None
        """
        self._rclone_invoke('sha1sum', path, *args)


    # --------------------------------------------------------------------------
    #
    def size(self, path, *args):
        """
        Return the total size and number of objects in the specified remote:path using RClone.

        Args:
            path (str): Path for which size information should be retrieved.

        Returns:
            None
        """
        self._rclone_invoke('size', path, *args)


    # --------------------------------------------------------------------------
    #
    def version(self, *args):
        """
        Show the version number of RClone.

        Returns:
            None
        """
        self._rclone_invoke('version')


    # --------------------------------------------------------------------------
    #
    def cleanup(self):
        """
        Clean up the remote if possible using RClone.

        Returns:
            None
        """
        self._rclone_invoke('cleanup', *args)


    # --------------------------------------------------------------------------
    #
    def dedupe(self, path, *args):
        """
        Interactively find duplicate files and delete/rename them using RClone.

        Args:
            path (str): Path to be checked for duplicate files.

        Returns:
            None
        """
        self._rclone_invoke('dedupe', path, *args)


    # --------------------------------------------------------------------------
    #
    def authorize(self, *args):
        """
        Perform remote authorization using RClone.

        Returns:
            None
        """
        self._rclone_invoke('authorize', *args)


    # --------------------------------------------------------------------------
    #
    def cat(self, path, *args):
        """
        Concatenate any files and send them to stdout using RClone.

        Args:
            path (str): Path of the files to be concatenated.

        Returns:
            None
        """
        self._rclone_invoke('cat', path, *args)


    # --------------------------------------------------------------------------
    #
    def copyto(self, source, destination, *args):
        """
        Copy files from source to dest, skipping already copied using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('copyto', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def genautocomplete(self, output_file, *args):
        """
        Output shell completion scripts for RClone.

        Args:
            output_file (str): Output file path.

        Returns:
            None
        """
        self._rclone_invoke('genautocomplete', output_file, *args)


    # --------------------------------------------------------------------------
    #
    def gendocs(self, output_directory, *args):
        """
        Output markdown docs for RClone to the specified directory.

        Args:
            output_directory (str): Output directory path.

        Returns:
            None
        """
        self._rclone_invoke('gendocs', output_directory, *args)


    # --------------------------------------------------------------------------
    #
    def listremotes(self, *args):
        """
        List all the remotes in the RClone config file.

        Returns:
            None
        """
        self._rclone_invoke('listremotes', *args)


    # --------------------------------------------------------------------------
    #
    def mount(self, mountpoint, *args):
        """
        Mount the remote as a mountpoint using RClone.

        Args:
            mountpoint (str): Mountpoint path.

        Returns:
            None
        """
        self._rclone_invoke('mount', mountpoint, *args)


    # --------------------------------------------------------------------------
    #
    def moveto(self, source, destination, *args):
        """
        Move file or directory from source to dest using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('moveto', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def obscure(self, password, *args):
        """
        Obscure password for use in the RClone configuration file.

        Args:
            password (str): Password to be obscured.

        Returns:
            None
        """
        self._rclone_invoke('obscure', password, *args)


    # --------------------------------------------------------------------------
    #
    def cryptcheck(self, source, destination, *args):
        """
        Check the integrity of an encrypted remote using RClone.

        Args:
            source (str): Source path.
            destination (str): Destination path.

        Returns:
            None
        """
        self._rclone_invoke('cryptcheck', source, destination, *args)


    # --------------------------------------------------------------------------
    #
    def about(self, *args):
        """
        Get quota information from the remote using RClone.

        Returns:
            None
        """
        self._rclone_invoke('about', *args)


    # --------------------------------------------------------------------------
    #
    def bcast(self, source, destinations: list):

        for idx, d in enumrate(destinations):
            t = threading.Thread(target=self.copy, args=(source, d,),
                                 name=f'BcastThreadCopy-{idx}', daemon=True)
            
            t.start()
            self.logger.info(f'bcast: broadcasting file {source} to {d}')
