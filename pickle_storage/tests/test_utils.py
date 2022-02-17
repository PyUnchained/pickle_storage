import unittest
import os
import time
import pathlib

from pickle_storage.utils import db_relative_path, write_to_log, timeit, Timer, import_class
from pickle_storage.config import storage_settings, ConfigObject
from pickle_storage.mixins import HMACMixin

class UtilsTestCase(unittest.TestCase):        

    def test_db_relative_path(self):
        storage_path = pathlib.Path(
        storage_settings.PICKLE_STORAGE_WORKING_DIRECTORY)
        # Test making path relative
        str_path = 'test_path'
        str_path_with_suffix = f'test_path{storage_settings.PICKLE_STORAGE_SUFFIX}'
        path_instance = pathlib.Path(str_path)
        path_instance_with_suffix = pathlib.Path(str_path_with_suffix)

        result_paths = []
        for test_path in [str_path, str_path_with_suffix, path_instance,
            path_instance_with_suffix]:
            result_paths.append(db_relative_path(test_path))

        for p in result_paths:
            self.assertTrue(isinstance(p, pathlib.Path))
            self.assertEqual(p.parents[0], storage_path) # Relative to storage DIR

    def test_mixins(self):
        HMACObject = HMACMixin()
        HMACObject.data_dir

    def test_logging(self):
        write_to_log('One', 'Two')

    def test_class_import(self):
        ImportedHMACMixin = import_class("pickle_storage.mixins.HMACMixin")
        self.assertEqual(ImportedHMACMixin, HMACMixin)

    def test_decorators(self):

        @timeit
        def timed_function(*args, **kwargs):
            return

        with Timer('Timed Decorator'):
            timed_function()


class ConfigTestCase(unittest.TestCase):

    def test_config_class(self, *args, **kwargs):
        config_obj = ConfigObject(user_defined_settings=None)