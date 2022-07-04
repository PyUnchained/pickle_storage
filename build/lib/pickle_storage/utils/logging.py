import datetime
import logging
import threading
import time 
import traceback
import re
import wrapt
import pathlib
import importlib
import sys

logging_re_pattern = r'write_to_log[\s]*\('

def import_class(import_string):
    path_elements = import_string.split('.')
    class_name = path_elements[-1]
    module = importlib.import_module('.'.join(path_elements[:-1]))
    return getattr(module, class_name)

@wrapt.decorator
def log_errors(wrapped, instance, args, kwargs):
    try:
        return wrapped(*args, **kwargs)
    except:
        write_to_log('See trace above for more details.', level = 'error')

@wrapt.decorator
def timeit(wrapped, instance, args, kwargs):
    ts = time.time()
    result = wrapped(*args, **kwargs)
    te = time.time()
    write_to_log("Function time {} : {} sec".format(wrapped.__name__, te - ts))
    return result

def db_relative_path(target_path, is_dir=False):
    from pickle_storage.config.tools import get_settings_config
    storage_settings = get_settings_config()
    
    storage_path = pathlib.Path(
        storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)
    
    # Convert strings to path instances
    if isinstance(target_path, str):
        target_path = pathlib.Path(target_path)

    if not is_dir:
        if not target_path.suffix:
            file_name = target_path.parts[-1]
            target_path = target_path.with_name(
                f"{file_name}{storage_settings.PICKLE_STORAGE_SUFFIX}")

    if not target_path.parents[0] == storage_path and not target_path == storage_path:
        target_path = storage_path.joinpath(target_path)

    return target_path

class Timer():

    def __enter__(self):
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info):
        """Stop the context manager timer"""
        self.stop()

    def __init__(self, name = ''):
        if name:
            name = f' ({name})'
            
        self.name = name
        self._start_time = None

    def start(self):
        """Start a new timer"""
        self._start_time = time.perf_counter()


    def stop(self):
        """Stop the timer, and report the elapsed time"""
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        write_to_log(f"Elapsed time{self.name}: {elapsed_time} seconds")


def write_to_log(msg, *extra_msg_args, level='info', limit=20,  include_traceback=False,
    log_tag="PickleStorageLog", regex_pattern=logging_re_pattern, ignored_files =[]):

    # Elevate the level during tests
    try:
        if 'test' in sys.argv[-1]:
            if level in ['info', 'debug']:
                level = 'warning'
    except IndexError:
        pass

    now = datetime.datetime.now().strftime('%d %b %H:%M:%S')
    msg = str(msg)
    for extra_arg in extra_msg_args:
        msg +=  '\n' + str(extra_arg)

    log_call = getattr(logging, level)
    extra_details = ''
    use_current_line = False
    for summary in traceback.extract_stack(limit = limit):

        # if use_current_line:
        #     extra_details = f'{log_tag}: {summary.filename} {summary.lineno} ({now})'
        #     break
        
        filename_checks = ['pickle_storage/utils.py' not in summary.filename,
            'contrib/utils.py' not in summary.filename]
        for fn in ignored_files:
            filename_checks.append(fn not in summary.filename)
        outside_utils = all(filename_checks)

        if re.search(regex_pattern, summary.line) and outside_utils:
            extra_details = f'{log_tag}: {summary.filename} {summary.lineno} ({now})'
            # if not ignore_filenames:
            #     extra_details = f'{log_tag}: {summary.filename} {summary.lineno} ({now})'
            # else:
            #     use_current_line = True


    if include_traceback or level == 'error':
        extra_details = extra_details + '\n\n' + traceback.format_exc()

    if extra_details:
        if not re.match(f'{log_tag}:*', extra_details):
            extra_details = f'{log_tag}: ' + extra_details
        log_call(extra_details + '\n' + msg)
        return

    if not re.match(f'{log_tag}:*', msg):
        msg = f'{log_tag}: ' + msg
    log_call(msg)
