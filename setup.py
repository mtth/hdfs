#!/usr/bin/env python

"""HdfsCLI."""

from hdfs import __version__
from setuptools import find_packages, setup

setup(
    name='hdfs',
    version=__version__,
    description='HdfsCLI',
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
    ],
    install_requires=[
      'docopt',
      'requests>=2.0.1',
      'requests-kerberos',
      'avro',
    ],
    entry_points={'console_scripts': [
      'hdfs = hdfs.__main__:main',
      'hdfsavro = hdfs.ext.avro:main',
    ]},
)
