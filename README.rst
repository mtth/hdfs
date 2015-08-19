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

  In [2]: with CLIENT.read('models/2.json') as reader:
    ...:     from json import load
    ...:     model = load(reader)
    ...:     model['normalize'] = False
    ...:

  In [3]: with CLIENT.write('models/2.json', overwrite=True) as writer:
    ...:     from json import dump
    ...:     dump(model, writer)
    ...:


Features
--------

* Python (2 and 3) bindings for the WebHDFS_ (and HttpFS_) API, supporting both 
  secure and insecure clusters.
* Command line interface to transfer files and start an interactive client 
  shell, with aliases for convenient namenode URL caching.
* Additional functionality through optional extensions:

  + `kerberos`, adding support for Kerberos_ authenticated clusters.
  + `avro`, allowing reading and writing Avro_ files directly from HDFS.
  + `dataframe`, enabling fast loading and saving of pandas_ dataframes on 
    HDFS.

See the documentation_ to learn more.


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


.. _documentation: http://hdfscli.readthedocs.org/
.. _HttpFS: http://hadoop.apache.org/docs/current/hadoop-hdfs-httpfs/
.. _Avro: https://avro.apache.org/docs/1.7.7/index.html
.. _pandas: http://pandas.pydata.org/
.. _WebHDFS: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
.. _Kerberos: http://web.mit.edu/kerberos/
.. _issues: https://github.com/mtth/hdfs/issues
