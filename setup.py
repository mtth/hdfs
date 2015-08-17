#!/usr/bin/env python

"""HdfsCLI: API and command line interface for HDFS."""

from hdfs import __version__
from os import environ
from setuptools import find_packages, setup


# Allow configuration of the CLI alias.
ENTRY_POINT = environ.get('HDFSCLI_ENTRY_POINT', 'hdfscli')

setup(
  name='hdfs',
  version=__version__,
  description=__doc__,
  long_description=open('README.rst').read(),
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
    'avro': ['fastavro>=0.8.6'],
    'kerberos': ['requests-kerberos>=0.7.0'],
    'dataframe': ['fastavro>=0.8.6', 'pandas>=0.14.1'],
  },
  entry_points={'console_scripts': [
    '%s = hdfs.__main__:main' % (ENTRY_POINT, ),
    '%s-avro = hdfs.ext.avro.__main__:main' % (ENTRY_POINT, ),
  ]},
)
