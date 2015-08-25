#!/usr/bin/env python

"""HdfsCLI: API and command line interface for HDFS."""

from os import environ
from setuptools import find_packages, setup
import re


def _get_version():
  """Extract version from package."""
  with open('hdfs/__init__.py') as reader:
    match = re.search(
      r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
      reader.read(),
      re.MULTILINE
    )
    if match:
      return match.group(1)
    else:
      raise RuntimeError('Unable to extract version.')

def _get_long_description():
  """Get README contents."""
  with open('README.rst') as reader:
    return reader.read()

# Allow configuration of the CLI alias.
ENTRY_POINT = environ.get('HDFSCLI_ENTRY_POINT', 'hdfscli')

setup(
  name='hdfs',
  version=_get_version(),
  description=__doc__,
  long_description=_get_long_description(),
  author='Matthieu Monsch',
  author_email='monsch@alum.mit.edu',
  url='http://hdfscli.readthedocs.org',
  license='MIT',
  packages=find_packages(),
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
  ],
  install_requires=[
    'docopt',
    'requests>=2.7.0',
    'six>=1.9.0',
  ],
  extras_require={
    'avro': ['fastavro==0.9.2'],
    'kerberos': ['requests-kerberos>=0.7.0'],
    'dataframe': ['fastavro==0.9.2', 'pandas>=0.14.1'],
  },
  entry_points={'console_scripts': [
    '%s = hdfs.__main__:main' % (ENTRY_POINT, ),
    '%s-avro = hdfs.ext.avro.__main__:main' % (ENTRY_POINT, ),
  ]},
)
