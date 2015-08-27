.. default-role:: code


HdfsCLI |build_image| |pypi_image|
==================================

.. |build_image| image:: https://travis-ci.org/mtth/hdfs.png?branch=master
  :target: https://travis-ci.org/mtth/hdfs

.. |pypi_image| image:: https://badge.fury.io/py/hdfs.svg
  :target: https://pypi.python.org/pypi/hdfs/

API and command line interface for HDFS.

.. code-block:: bash

  $ hdfscli --alias=dev

  Welcome to the interactive HDFS python shell.
  The HDFS client is available as `CLIENT`.

  In [1]: CLIENT.list('models/')
  Out[1]: ['1.json', '2.json']

  In [2]: CLIENT.status('models/2.json')
  Out[2]: {
    'accessTime': 1439743128690,
    'blockSize': 134217728,
    'childrenNum': 0,
    'fileId': 16389,
    'group': 'supergroup',
    'length': 48,
    'modificationTime': 1439743129392,
    'owner': 'drwho',
    'pathSuffix': '',
    'permission': '755',
    'replication': 1,
    'storagePolicy': 0,
    'type': 'FILE'
  }

  In [3]: with CLIENT.read('models/2.json', encoding='utf-8') as reader:
    ...:     from json import load
    ...:     model = load(reader)
    ...:


Features
--------

* Python (2 and 3) bindings for the WebHDFS_ (and HttpFS_) API, supporting both 
  secure and insecure clusters.
* Command line interface to transfer files and start an interactive client 
  shell, with aliases for convenient namenode URL caching.
* Additional functionality through optional extensions:

  + `avro`, to `read and write Avro files directly from HDFS`_.
  + `dataframe`, to `load and save Pandas dataframes`_.
  + `kerberos`, to `support Kerberos authenticated clusters`_.

See the documentation_ to learn more.


Getting started
---------------

.. code-block:: bash

  $ pip install hdfs

Then hop on over to the quickstart_ guide.


Testing
-------

HdfsCLI is tested against both WebHDFS_ and HttpFS_. There are two ways of 
running tests (see `scripts/` for helpers to set up a test HDFS cluster):

.. code-block:: bash

  $ HDFSCLI_TEST_URL=http://localhost:50070 nosetests # Using a namenode's URL.
  $ HDFSCLI_TEST_ALIAS=dev nosetests # Using an alias.


Contributing
------------

We'd love to hear what you think on the issues_ page. Pull requests are also 
most welcome!


.. _HttpFS: http://hadoop.apache.org/docs/current/hadoop-hdfs-httpfs/
.. _WebHDFS: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
.. _read and write Avro files directly from HDFS: http://hdfscli.readthedocs.org/en/latest/api.html#module-hdfs.ext.avro
.. _load and save Pandas dataframes: http://hdfscli.readthedocs.org/en/latest/api.html#module-hdfs.ext.dataframe
.. _support Kerberos authenticated clusters: http://hdfscli.readthedocs.org/en/latest/api.html#module-hdfs.ext.kerberos
.. _documentation: http://hdfscli.readthedocs.org/
.. _quickstart: http://hdfscli.readthedocs.org/en/latest/quickstart.html
.. _issues: https://github.com/mtth/hdfs/issues
