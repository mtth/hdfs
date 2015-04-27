.. default-role:: code


HdfsCLI |build_image|
---------------------

.. |build_image| image:: https://travis-ci.org/mtth/hdfs.png?branch=master
  :target: https://travis-ci.org/mtth/hdfs

API and command line interface for HDFS.


Features
--------

* Python bindings for the `WebHDFS API`_, supporting both secure and insecure 
  clusters.
* Lightweight CLI with aliases for convenient namenode URL caching.
* Additional functionality through optional extensions:

  + `avro`, allowing reading/writing Avro files directly from JSON.
  + `dataframe`, enabling fast loading/saving of pandas_ dataframes from/to 
    HDFS.
  + `kerberos`, adding support for Kerberos authenticated clusters.


Installation
------------

Using pip_:

.. code-block:: bash

  $ pip install hdfs

By default none of the package requirements for extensions are installed. To do 
so simply suffix the package name with the desired extensions:

.. code-block:: bash

  $ pip install hdfs[avro,dataframe,kerberos]

By default the command line entry point will be named `hdfs`. If this conflicts 
with another utility, you can choose another name by specifying the 
`HDFS_ENTRY_POINT` environment variable:

.. code-block:: bash

  $ HDFS_ENTRY_POINT=hdfscli pip install hdfs


API
---

Sample snippet using a python client to create a file on HDFS, rename it, 
download it locally, and finally delete the remote copy.

.. code-block:: python

  from hdfs import KerberosClient

  client = KerberosClient('http://namenode:port', root='/user/alice')
  client.write('hello.md', 'Hello, world!')
  client.rename('hello.md', 'hello.rst')
  client.download('hello.rst', 'hello.rst')
  client.delete('hello.rst')


CLI
---

Sample commands (see below for how to configure cluster aliases):

.. code-block:: bash

  $ hdfs --read logs/1987-03-23 >>logs
  $ hdfs --write -o data/weights.tsv <weights.tsv

Cf. `hdfs --help` for the full list of commands and options.


Configuration
-------------

You can configure which clusters to connect to by writing your own 
configuration at `~/.hdfsrc` (this will also enable the `Client.from_alias` 
method).

Sample configuration defining two aliases, `foo` and `bar`:

.. code-block:: cfg

  [hdfs]
  default.alias = foo # Used when no alias is specified at the command line.

  [foo_alias]
  client = KerberosClient
  root = /some/directory
  url = http://url.to.namenode:port

  [bar_alias]
  url = http://url.to.another.namenode:port

All options other than `url` can be omitted. `client` determines which class to 
use (defaulting to the generic `Client`), and the remaining options are passed 
as named arguments to the appropriate constructor.


Documentation
-------------

The full documentation can be found here_.


.. _here: http://hdfscli.readthedocs.org/
.. _pip: http://www.pip-installer.org/en/latest/
.. _pandas: http://pandas.pydata.org/
.. _WebHDFS API: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
