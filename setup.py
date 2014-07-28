#!/usr/bin/env python

"""HdfsCLI: a command line interface for WebHDFS."""

from hdfs import __version__
from setuptools import find_packages, setup


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
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 2.6',
      'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
      'docopt',
      'requests>=2.0.1',
    ],
    extras_require={
      'avro': ['avro'],
      'kerberos': ['requests-kerberos'],
      'dataframe': ['numpy', 'pandas>=0.14.1', 'fastavro'],
    },
    entry_points={'console_scripts': [
      'hdfs = hdfs.__main__:main',
      'hdfsavro = hdfs.ext.avro:main',
    ]},
)
