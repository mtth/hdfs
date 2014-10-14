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


API
---

Sample usage of using a python client to create a file on HDFS, rename it, 
download it locally, and finally delete the remote copy.

.. code-block:: python

  from hdfs import KerberosClient

  # Instantiate the client (`root` is an optional argument enabling the use of 
  # relative paths for all client commands requiring a path).
  client = KerberosClient('http://namenode:port', root='/user/alice')
  client.write('hello.md', 'Hello, world!') # create the file on HDFS
  client.rename('hello.md', 'hello.rst') # rename the file
  client.download('hello.rst', 'hello.rst') # download the file as `hello.rst`
  client.delete('hello.rst') # delete the remote file


CLI
---

.. code-block:: bash

  $ hdfs --info --depth=1
     0 B    3d  D  /user/alice

  $ echo 'Hello, world!' | hdfs hello.rst --write

  $ hdfs --info --depth=1
    14 B    1m  D  /user/alice
    14 B    1m  F  /user/alice/hello.rst

  $ hdfs hello.rst --read
  Hello, world!

Cf. `hdfs --help` for the full list of commands and options.


Documentation
-------------

The full documentation can be found here_.


.. _here: http://hdfscli.readthedocs.org/
.. _pip: http://www.pip-installer.org/en/latest/
.. _pandas: http://pandas.pydata.org/
.. _WebHDFS API: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
