#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='pickle-storage',
      version='0.1',
      description='Python Pickle-Based Data Storage',
      author='Tatenda Tambo',
      author_email='tatendatambo@gmail.com',
      packages=find_packages(),
      install_requires=['wrapt>=1.12.1']
      )