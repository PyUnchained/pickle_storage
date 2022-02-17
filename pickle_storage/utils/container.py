import shutil
import pathlib
import datetime

from pickle_storage.config import  storage_settings
from pickle_storage.utils import write_to_log

def archive(file_name, compression_format="gztar"):
    # Specified format may not always be avaiable.
    format_options = [ x[0] for x in shutil.get_archive_formats()]
    if compression_format not in format_options:
        compression_format = format_options[0]

    timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%d_%m_%y_%H_%M")
    file_name = f"{file_name}_{timestamp}"
    source = pathlib.Path(
        storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)
    target = pathlib.Path(
        storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY, '_archive', file_name)
    shutil.make_archive(target, compression_format, source)

def load_fixture(source, target):
    shutil.unpack_archive(source, target)