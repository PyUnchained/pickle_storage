import logging
import os
import pathlib
import functools
import importlib

from pickle_storage.errors import ConfigError
from pickle_storage.utils import write_to_log, import_class

__all__ = ['ConfigObject', 'storage_settings', 'import_class']

class ConfigObject():
    """ Respresents the current settings specified for the pickle_storage package. """
    
    def __init__(self, user_defined_settings = os.getenv('PICKLE_STORAGE_SETTINGS', None), *args, **kwargs):
        #Find default settings module
        self.user_defined_settings = user_defined_settings
        settings_modules = []
        settings_modules.append(importlib.import_module('pickle_storage.config.defaults'))

        # Attempt to import user defined settings
        if not self.user_defined_settings:
            write_to_log('"PICKLE_STORAGE_SETTINGS" environment variable'
            ' not set. Using default settings', level='warning')
        else:
            settings_modules.append(importlib.import_module(
                self.user_defined_settings))

        # Set attributes depending on the contents of the settings modules,
        # overriding any defaults
        for module in settings_modules:
            for setting_name in dir(module):
                if setting_name.isupper():
                    setattr(self, setting_name,
                            getattr(module, setting_name))


    @functools.cached_property    
    def active_storage(self):
        """ Represents the instance of the currently active storage. """
        
        StorageClass = import_class(
            self.PICKLE_STORAGE_CONTAINER_CLASS)
        instance = StorageClass()
        return instance
        
    def update_setting(self, setting_name, value):
        setattr(self, setting_name, value)

storage_settings  = ConfigObject()