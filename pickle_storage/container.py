import pathlib
import uuid
import time
import shutil

from pickle_storage.config import storage_settings
from pickle_storage.utils import db_relative_path, write_to_log
from pickle_storage.operations import Write, Read

class BaseStorageContainer():

    def __init__(self, *args, **kwargs):
        self.working_dir_path = kwargs.get("working_dir_path", pathlib.Path(
            storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY))
        self.signing_key_path = kwargs.get("signing_key_path", pathlib.Path(
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME))
        self.setup()

    def clear(self):
        shutil.rmtree(self.working_dir_path)
        self.setup()

    def contents(self):
        signing_key_path_with_ext = "".join([
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME,
            storage_settings.PICKLE_STORAGE_SUFFIX])
        return filter(lambda x: signing_key_path_with_ext not in str(x),
            pathlib.Path(
                storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY).glob("*.psf")
        )

    def read(self, *args, **kwargs):
        th = Read(*args, **kwargs)
        return th.join()

    def setup(self):
        if not self.working_dir_path.exists():
            self.working_dir_path.mkdir(parents=True, exist_ok=True)
        self.create_signing_key()

    def create_signing_key(self):
        """ Create the key used to validate data integrity on subsequent reads/writes. """
        
        if not self.signing_key_path.exists():
            Write(self.signing_key_path, uuid.uuid4().bytes, secure=False).join()


    def write(self, *args, wait=False, **kwargs):

        th = Write(*args, **kwargs)
        if wait:
            return th.join()
        return th
