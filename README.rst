.. default-role:: code


HdfsCLI |build_image|
---------------------

.. |build_image| image:: https://travis-ci.org/mtth/hdfs.png?branch=master
  :target: https://travis-ci.org/mtth/hdfs

API and command line interface for HDFS.


Features
--------

* Python bindings for the WebHDFS_ API, supporting both secure and insecure 
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

By default the command line entry point will be named `hdfscli`. You can choose 
another name by specifying the `HDFSCLI_ENTRY_POINT` environment variable:

.. code-block:: bash

  $ HDFSCLI_ENTRY_POINT=hdfs pip install hdfs


Quickstart
----------

API
***

Sample snippet using a python client to create a file on HDFS, rename it, 
download it locally, and finally delete the remote copy.

.. code-block:: python

  from hdfs import TokenClient

  client = TokenClient('http://namenode:port', 'token', root='/user/alice')
  client.write('hello.md', 'Hello, world!')
  client.rename('hello.md', 'hello.rst')
  client.download('hello.rst', 'hello.rst')
  client.delete('hello.rst')

Refer to the documentation_ for the full API and extensions.


CLI
***

Sample commands (see below for information on how to configure cluster 
aliases):

.. code-block:: bash

  $ # Read all files inside a folder from HDFS and store them locally.
  $ hdfscli download export/results/ "results-$(date +%F)"
  $ # Read a file from HDFS and append its contents to a local log file.
  $ hdfscli download logs/1987-03-23.txt - >>logs
  $ # Write a single file to HDFS.
  $ hdfscli upload --alias=dev weights.json models/

Python shell integration (using IPython_ if available):

.. code-block:: bash

  $ hdfscli --alias=dev
  Welcome to the interactive HDFS python shell.
  The HDFS client is available as `CLIENT`.

  In [1]: CLIENT.list('data/')
  Out[1]: ['1.json', '2.json']

  In [2]: CLIENT.delete('data/2.json')
  Out[2]: True

Cf. `hdfscli --help` for the full list of commands and options.


Configuration
*************

You can configure which clusters to connect to by writing your own 
configuration at `~/.hdfsrc` (or elsewhere by setting the `HDFSCLI_RCPATH` 
environment variable correspondingly). This will also enable the 
`Client.from_alias` method.

Sample configuration defining two aliases, `dev` and `prod`:

.. code-block:: cfg

  [hdfs]
  default.alias = dev # Used when no alias is specified at the command line.

  [dev.alias]
  url = http://url.to.dev.namenode:port

  [prod.alias]
  url = https://url.to.prod.namenode:port
  client = KerberosClient
  root = /jobs/
  verify = false

All options other than `url` can be omitted. `client` determines which class to 
use (defaulting to the generic `Client`), and the remaining options are passed 
as named arguments to the appropriate constructor.


Testing
-------

HdfsCLI is tested against both WebHDFS_ and HttpFS_. There are two ways of 
running tests:

.. code-block:: bash

  $ HDFSCLI_TEST_ALIAS=dev nosetests # Using an alias.
  $ HDFSCLI_TEST_URL=http://localhost:50070 nosetests # Using the URL.

See `scripts/` for helpers to set up a suitable HDFS cluster.


.. _documentation: http://hdfscli.readthedocs.org/
.. _pip: http://www.pip-installer.org/en/latest/
.. _pandas: http://pandas.pydata.org/
.. _WebHDFS: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
.. _HttpFS: http://hadoop.apache.org/docs/current/hadoop-hdfs-httpfs/
.. _IPython: http://ipython.org/
