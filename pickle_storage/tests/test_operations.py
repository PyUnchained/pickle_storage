import unittest
import time
import functools

from pickle_storage.operations import BaseStorageOperation
from pickle_storage.utils import write_to_log

class StorageContainerTestCase(unittest.TestCase):

    def test_base_operation(self):

        def hook_fn(*args, **kwargs):
            self.assertTrue(True)

    # with self.assertRaises(NotImplementedError):
        # Test on_complete hook
        BaseStorageOperation(on_complete=hook_fn).join()

        # Test pre_operation hook
        class NullOperation(BaseStorageOperation):
            def pre_operation(self, *args, **kwargs):
                return False

        NullOperation(on_complete=hook_fn).join()
