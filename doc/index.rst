.. default-role:: code


HdfsCLI
=======

API and command line interface for HDFS.

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

By default the command line entry point will be named `hdfscli`. You can choose 
another name by specifying the `HDFSCLI_ENTRY_POINT` environment variable:

.. code-block:: bash

  $ HDFSCLI_ENTRY_POINT=hdfs pip install hdfs


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

  $ # Read all files inside a folder from HDFS and store them locally.
  $ hdfscli download export/results/ "results-$(date +%F)"

  $ # Read a file from HDFS and append its contents to a local log file.
  $ hdfscli download logs/1987-03-23.txt - >>logs

  $ # Write a single file to HDFS.
  $ hdfscli upload weights.json static/

Fully featured shell (using IPython_ if available):

.. code-block:: bash

  $ hdfscli
  Welcome to the interactive HDFS python shell.
  The HDFS client is available as `CLIENT`.

  In [1]: CLIENT.list('data/')
  Out[1]: ['1.json', '2.json']

  In [2]: CLIENT.delete('data/2.json')
  Out[2]: True

Cf. `hdfscli --help` for the full list of commands and options.


Table of contents
-----------------

.. toctree::
  :maxdepth: 2

  api
  extensions


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _pip: http://www.pip-installer.org/en/latest/
.. _WebHDFS API: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
.. _pandas: http://pandas.pydata.org/
.. _IPython: http://ipython.org/
