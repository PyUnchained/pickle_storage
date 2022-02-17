import itertools
import threading
import pickle
import re

from pickle_storage.errors import ForbiddenFileError
from pickle_storage.config import storage_settings
from pickle_storage.utils import write_to_log, db_relative_path, log_errors
from pickle_storage.mixins import HMACMixin

class BaseStorageOperation(threading.Thread):
    id_iterator = itertools.count()

    def __init__(self, *args, **kwargs):
        self._return = False
        self.thread_id = next(self.id_iterator)
        kwargs['name'] = f"{self.__class__.__name__}-{self.thread_id}"
        kwargs['daemon'] = True
        self.on_complete = kwargs.pop('on_complete', None)
        self.__kwargs = kwargs
        self.__args = args
        super().__init__(*args, **kwargs)
        self.start()

    def pre_operation(self, *args, **kwargs):
        return True
    
    def do_operation(self, *args, **kwargs):
        raise NotImplementedError()

    def post_operation(self, *args, **kwargs):
        pass

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        if self.on_complete:
            self.on_complete(self._return)
        return self._return

    @log_errors
    def run(self):
        if not self.pre_operation():
            self._return = False
            return False
        self._return = self.do_operation()
        self.post_operation(self._return, **self.__kwargs)


class Write(HMACMixin, BaseStorageOperation):
    

    def __init__(self, path='', content=None, *args, secure=True, **kwargs):

        self.content = content
        if path:
            self.path = db_relative_path(path)
        else:
            self.path = None
        self.secure = secure
        super().__init__(*args, **kwargs)


    def do_operation(self, *args, **kwargs):
        binary_content = pickle.dumps(self.content)
        if self.secure:
            if self.path.name == self.signing_key_filename:
                raise ForbiddenFileError('Permission denied.')

            digest = self.hmac_digest(binary_content)
            with open(self.path, "wb") as f:
                f.write(digest)
                f.write(bytearray(1))
                f.write(binary_content)
        else:
            with open(self.path, "wb") as f:
                f.write(binary_content)

        return True

    def pre_operation(self, *args, **kwargs):
        if not self.path or not self.content:
            return False
        return super().pre_operation(*args, **kwargs)

    @property
    def signing_key_filename(self):
        name = storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME
        name += storage_settings.PICKLE_STORAGE_SUFFIX
        return name

class Read(HMACMixin, BaseStorageOperation):
    
    def __init__(self, path='', *args, **kwargs):
        if path:
            self.path = db_relative_path(path)
        else:
            self.path = None
        super().__init__(*args, **kwargs)


    def do_operation(self, *args, **kwargs):
        attempts = 0
        while attempts < 3:

            unsafe_content = False
            try:
            
                with open(self.path, 'rb') as f:
                    digest = f.read(32) 
                    f.seek(33) # One null byte separates the digest from content
                    content = f.read()

                if self.is_safe(digest, content):
                    return pickle.loads(content)
                else:
                    unsafe_content = True
                time.sleep(0.05)

            except:
                pass

            attempts +=1

        if unsafe_content:
            write_to_log(f'Failed to read "{self.path}"'
                    ' digest did not match content.', level='warning')
        return None

    def pre_operation(self, *args, **kwargs):
        if not self.path:
            return False
        return super().pre_operation(*args, **kwargs)