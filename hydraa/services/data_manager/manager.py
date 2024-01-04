import os
import sys
import shutil

from .exceptions import RcloneException
from ..caas_manager.utils.misc import sh_callout


if os.environ.get('RCLONE_PATH'):
    RCLONE_PATH = os.environ.get('RCLONE_PATH')
else: RCLONE_PATH = shutil.which('rclone')


class DataManager:
    def __init__(self, provider):
        self.provider = provider

        if not RCLONE_PATH:
            raise RcloneException('DataManager requires rclone to be installed.'
                                  ' Make sure rclone is in the PATH or please set '
                                  ' the RCLONE_PATH environment variable.', None)

    def rclone(self, rclone_cmd, *args):

        cmd = [RCLONE_PATH, rclone_cmd]
        cmd.extend(args)

        cmd = " ".join(cmd)

        out, err, ret = sh_callout(cmd, shell=True)

        if ret:
           raise RcloneException(err, ret)
        else:
            if out:
                print(out)
            else:
                rclone_op_invokder = sys._getframe().f_back.f_code.co_name
                print(f'RClone operation {rclone_op_invokder} completed successfully.')

    def configure(self):
        print('Configuring RClone must be done via the command line by calling "rclone config"')
        return

    def copy(self, source, destination):
        self.rclone('copy', source, destination)

    def sync(self, source, destination):
        self.rclone('sync', source, destination)

    def move(self, source, destination):
        self.rclone('move', source, destination)

    def delete(self, path):
        self.rclone('delete', path)

    def purge(self, path):
        self.rclone('purge', path)

    def mkdir(self, path):
        self.rclone('mkdir', path)

    def rmdir(self, path):
        self.rclone('rmdir', path)

    def rmdirs(self, path):
        self.rclone('rmdirs', path)

    def check(self, source, destination):
        self.rclone('check', source, destination)

    def ls(self, path):
        self.rclone('ls', path)

    def lsd(self, path):
        self.rclone('lsd', path)

    def lsl(self, path):
        self.rclone('lsl', path)

    def md5sum(self, path):
        self.rclone('md5sum', path)

    def sha1sum(self, path):
        self.rclone('sha1sum', path)

    def size(self, path):
        self.rclone('size', path)

    def version(self):
        self.rclone('version')

    def cleanup(self):
        self.rclone('cleanup')

    def dedupe(self, path):
        self.rclone('dedupe', path)

    def authorize(self):
        self.rclone('authorize')

    def cat(self, path):
        self.rclone('cat', path)

    def copyto(self, source, destination):
        self.rclone('copyto', source, destination)

    def genautocomplete(self, output_file):
        self.rclone('genautocomplete', output_file)

    def gendocs(self, output_directory):
        self.rclone('gendocs', output_directory)

    def listremotes(self):
        self.rclone('listremotes')

    def mount(self, mountpoint):
        self.rclone('mount', mountpoint)

    def moveto(self, source, destination):
        self.rclone('moveto', source, destination)

    def obscure(self, password):
        self.rclone('obscure', password)

    def cryptcheck(self, source, destination):
        self.rclone('cryptcheck', source, destination)

    def about(self):
        self.rclone('about')
