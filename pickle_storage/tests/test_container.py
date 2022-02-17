import unittest
import os
import time
import pathlib

from pickle_storage.errors import ConfigError, ForbiddenFileError
from pickle_storage.config import ConfigObject

from pickle_storage.utils import write_to_log, db_relative_path
from pickle_storage.config import storage_settings

__all__ = ['StorageContainerTestCase']

class StorageContainerTestCase(unittest.TestCase):

    def test_hot_settings_update(self):
        storage_settings.update_setting('NEW_SETTING', True)
        self.assertTrue(storage_settings.NEW_SETTING)
        time.sleep(0.1)

    def test_read_and_write(self):
        test_storage = storage_settings.active_storage
        test_data = {'Test':'Data', "SomeNum45":9569596}

        # Test writing to storage
        self.assertTrue(test_storage.write('write_test', test_data, wait=True))
        self.assertTrue(test_storage.write(
            'unsecure_data', test_data, wait=True, secure=False))
        self.assertFalse(test_storage.write(
            storage_settings.PICKLE_STORAGE_SIGNING_KEY_FILENAME,
            test_data, wait=True)) # Should be protected from writes
        test_storage.write('asynch_write', True)
        
        # Test reading from storage
        read_data = test_storage.read('write_test')
        self.assertEqual(test_data, read_data)
        self.assertFalse(test_storage.read('unsecure_data')) # Should not be readable


        # Test badly configured Write operation
        self.assertFalse(test_storage.write(wait=True))
        self.assertFalse(test_storage.read())

