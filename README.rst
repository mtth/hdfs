.. default-role:: code

HdfsCLI
=======

API and command line interface for HDFS.


Features
--------

* Works with secure and insecure clusters (including Kerberos authentication).
* Comprehensive python bindings for the `WebHDFS API`_.
* Lightweight CLI (under development).


Example
-------

.. code-block:: python

  from hdfs import KerberosClient

  # Instantiate the client
  client = KerberosClient('http://namenode:port')

  # Create a file on HDFS named 'foo' with contents 'Hello, world!'
  client.write('foo', 'Hello, world!')

  # Rename it to bar
  client.rename('foo', 'bar')

  # Download it locally as baz
  client.download('bar', 'baz')

  # Remove it from HDFS
  client.delete('bar')



Documentation
-------------

The full documentation can be found here_.


.. _here: http://hdfscli.readthedocs.org/
.. _WebHDFS API: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
