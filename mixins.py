import hmac
import pathlib
import hashlib

from kivy_tastypie.config import orm_settings
from kivy_tastypie.utils import write_to_log

class HMACMixin():
    """ Provides methods used to more securely pickle binary data using the
    pathlib and hmac libraries """

    def __init__(self, *args, **kwargs):
        self.__cached_key = None
        super().__init__(*args, **kwargs)
        

    @property
    def data_dir(self):
        return pathlib.Path(orm_settings.DB_FOLDER_PATH)

    @property
    def _signing_key(self):
        """ Secret key used to create digests """

        if not self.__cached_key:
            try:
                file_path = orm_settings.PICKLE_SIGNING_KEY_FILEPATH
                with file_path.open('r') as f:
                    key_str = f.read().encode('utf-8')
                self.__cached_key = key_str
                return key_str
            except:
                write_to_log(f'DB signing key not found at {file_path}',
                             level='warning', include_traceback=True)
        else:
            return self.__cached_key

    def hmac_digest(self, content):
        """ Create digest of binary data """
        return hmac.new(self._signing_key, content, hashlib.sha256).digest()

    def is_safe(self, digest, content):
        """ Verify that content is as expected. """
        test_digest = self.hmac_digest(content)
        return hmac.compare_digest(digest, test_digest)
