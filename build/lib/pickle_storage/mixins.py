import hmac
import pathlib
import hashlib
import uuid
import pickle


from pickle_storage.utils import write_to_log, db_relative_path
from pickle_storage.config.tools import get_settings_config
storage_settings = get_settings_config()

class HMACMixin():
    """ Provides methods used to more securely pickle binary data using the
    pathlib and hmac libraries """
    hashing_algorithm = hashlib.sha256

    def __init__(self, *args, **kwargs):
        self.__cached_key = None
        super().__init__(*args, **kwargs)
        
    @property
    def data_dir(self):
        return pathlib.Path(storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)

    @property
    def key_file(self):
        return db_relative_path(
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME)

    @property
    def _signing_key(self):
        """ Secret key used to create digests """

        if not self.__cached_key:
            with self.key_file.open('rb') as f:
                self.__cached_key = pickle.loads(f.read())
            return self.__cached_key
        else:
            return self.__cached_key

    def hmac_digest(self, content):
        """ Create digest of binary data """
        
        return hmac.new(self._signing_key, content,
                        self.hashing_algorithm).digest()

    def is_safe(self, digest, content):
        """ Verify that content is as expected. """
        
        return hmac.compare_digest(digest, self.hmac_digest(content))
