import pathlib
import uuid
import time
import shutil
import datetime

from pickle_storage.utils import db_relative_path, write_to_log
from pickle_storage.operations import Write, Read
from pickle_storage.config.tools import get_settings_config
storage_settings = get_settings_config()

class BaseStorageContainer():

    def __init__(self, *args, **kwargs):
        self.working_dir_path = kwargs.get("working_dir_path", pathlib.Path(
            storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY))
        self.signing_key_path = kwargs.get("signing_key_path", pathlib.Path(
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME))
        self.setup()

    def archive(self, *args, target=None, compression_format="gztar",
        time_format='%d_%m_%y_%H_%M_%S'):

        # Specified format may not always be avaiable.
        format_options = [ x[0] for x in shutil.get_archive_formats()]
        if compression_format not in format_options:
            compression_format = format_options[0]

        file_name = datetime.datetime.strftime(datetime.datetime.now(),
            time_format)
        source = pathlib.Path(storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)

        if not target:
            target = pathlib.Path(
                storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY,
                '_archive', file_name)

        shutil.make_archive(target, compression_format, source)
        return target

    def clear(self):
        shutil.rmtree(self.working_dir_path)
        self.setup()
        return True

    def contents(self):
        signing_key_path_with_ext = "".join([
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME,
            storage_settings.PICKLE_STORAGE_SUFFIX])
        return filter(lambda x: signing_key_path_with_ext not in str(x),
            pathlib.Path(
                storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY).glob("*.psf")
        )

    def create_signing_key(self):
        """ Create the key used to validate data integrity on subsequent reads/writes. """

        if not self.signing_key_path.exists():
            Write(self.signing_key_path, uuid.uuid4().bytes, secure=False).join()

    def exists(self, file_name):
        return db_relative_path(file_name).exists()

    def read(self, *args, **kwargs):
        th = Read(*args, **kwargs)
        return th.join()

    def setup(self):
        if not self.working_dir_path.exists():
            self.working_dir_path.mkdir(parents=True, exist_ok=True)
        self.create_signing_key()

    def write(self, *args, wait=False, **kwargs):

        th = Write(*args, **kwargs)
        if wait:
            return th.join()
        return th
