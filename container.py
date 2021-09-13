import pathlib
import uuid
import time

from pickle_storage.config import storage_settings
from pickle_storage.utils import db_relative_path, write_to_log
from pickle_storage.operations import Write, Read

class BaseStorageContainer():

    def __init__(self, *args, **kwargs):

        # Confirm directory exists
        db_folder_path = pathlib.Path(
            storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)
        if not db_folder_path.exists():
            db_folder_path.mkdir(
                parents=True, exist_ok=True)

        # Make sure a private key exists, used later to validate store integrity
        signing_key_path = db_relative_path(
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME)
        if not signing_key_path.exists():
            Write(signing_key_path, uuid.uuid4().bytes, secure=False)


    def write(self, *args, wait=False, **kwargs):
        th = Write(*args, **kwargs)
        if wait:
            return th.join()
        return th
            

    def read(self, *args, **kwargs):
        th = Read(*args, **kwargs)
        return th.join()

