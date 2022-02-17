#!/usr/bin/env python

from distutils.core import setup

setup(name='pickle-storage',
      version='0.1',
      description='Python Pickle-Based Data Storage',
      author='Tatenda Tambo',
      author_email='tatendatambo@gmail.com',
      packages=['pickle_storage'],
      install_requires=['wrapt>=1.12.1']
      )