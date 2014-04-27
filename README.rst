.. default-role:: code


HdfsCLI |build_image|
---------------------

.. |build_image| image:: https://travis-ci.org/mtth/hdfs.png?branch=master
  :target: https://travis-ci.org/mtth/hdfs

API and command line interface for HDFS.


Features
--------

* Works with secure and insecure clusters (including Kerberos authentication).
* Comprehensive python bindings for the `WebHDFS API`_.
* Lightweight CLI.


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

Other options include support for merging part-files, progress meters. Cf.
`hdfs --help` for more.


API
---

Sample usage of the python bindings:

.. code-block:: python

  from hdfs import KerberosClient

  # Instantiate the client
  client = KerberosClient('http://namenode:port', root='/user/alice')

  # Write a file '/user/alice/hello.md' on HDFS with contents 'Hello, world!'
  client.write('hello.md', 'Hello, world!')

  # Rename it
  client.rename('hello.md', 'hello.rst')

  # Download it locally
  client.download('hello.rst', 'hello.rst')

  # Remove it from HDFS
  client.delete('hello.rst')


Documentation
-------------

The full documentation can be found here_.


.. _here: http://hdfscli.readthedocs.org/
.. _WebHDFS API: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
