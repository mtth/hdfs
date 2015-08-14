.. default-role:: code


.. _advance_usage:

Advanced usage
==============


Path expansion
--------------

All :class:`~hdfs.client.Client` methods provide a path expansion functionality 
via the :meth:`~hdfs.client.Client.resolve` method. It enables the use of 
special markers to identify paths. For example, it currently supports the 
`#LATEST` marker which expands to the last modified file inside a given folder.

.. code-block:: python

  # Load the most recent data in the `tracking` folder.
  with client.read('tracking/#LATEST') as reader:
    data = reader.read()

See the method's documentation for more information.


.. _custom_client:

Custom client support
---------------------

In order for the CLI to be able to instantiate arbitrary client classes, it has 
to be able to discover these first. This can be done either by specifying where 
they are defined in the `global` section. For example, here is how we can make 
the Kerberos client available:

.. code-block:: cfg

  [global]
  autoload.modules = hdfs.ext.kerberos

There are two options for telling the CLI where to load the clients from:

+ `autoload.modules`, a comma-separated list of modules (which must be on 
  python's path).
+ `autoload.paths`, a comma-separated list of paths to python files.

Implementing custom clients can be particularly useful for passing default 
options (e.g. a custom `session` argument to each client). Here is a working 
example implementing a secure client with optional custom certificate support.

We first implement our new client and save it somewhere, for example 
`/etc/hdfscli.py`.

.. code-block:: python

  from hdfs import Client
  from requests import Session

  class SecureClient(Client):

    """A new client subclass for handling HTTPS connections.

    :param url: URL to namenode.
    :param cert: Local certificate. See `requests` documentation for details
      on how to use this.
    :param verify: Whether to check the host's certificate.
    :param \*\*kwargs: Keyword arguments passed to the default `Client` 
      constructor.

    """

    def __init__(self, url, cert=None, verify=True, **kwargs):
      session = Session()
      if ',' in cert:
        sessions.cert = [path.strip() for path in cert.split(',')]
      else:
        session.cert = cert
      if isinstance(verify, basestring): # Python 2.
        verify = verify.lower() in ('true', 'yes', 'ok')
      session.verify = verify
      super(SecureClient, self).__init__(url, session=session, **kwargs)

We then edit our configuration to tell the CLI how to load this module and 
define a `prod` alias using our new client:

.. code-block:: cfg

  [global]
  autoload.paths = /etc/hdfscli.py

  [prod.alias]
  client = SecureClient
  url = https://host:port
  cert = /etc/server.crt, /etc/key


Note that options used to instantiate clients from the CLI (using 
:meth:`Client.from_options` under the hood) are always passed in as strings. 
This is why we had to implement some parsing logic in the `SecureClient` 
constructor above.


Tracking transfer progress
--------------------------

The :meth:`~hdfs.client.Client.read`, :meth:`~hdfs.client.Client.upload`, 
:meth:`~hdfs.client.Client.download` methods accept a `progress` callback 
argument which can be used to track transfers. The passed function will be 
called every `chunk_size` bytes with two arguments:

+ The source path of the file currently being transferred.
+ The number of bytes currently transferred for this file or `-1` to signal 
  that this file's transfer has just finished.

Below is an implementation of a toy tracker which simply outputs to standard 
error the total number of transferred bytes each time a file transfer completes 
(we must still take care to ensure correct behavior even during multi-threaded 
transfer).

.. code-block:: python

  from sys import stderr
  from threading import Lock

  class Progress(object):

    """Basic progress tracker callback."""

    def __init__(self):
      self._data = {}
      self._lock = Lock()

    def __call__(self, hdfs_path, nbytes):
      with self._lock:
        if nbytes >= 0:
            self._data[hdfs_path] = nbytes
        else:
          stderr.write('%s\n' % (sum(self._data.values()), ))

Finally, note that the :meth:`~hdfs.client.Client.write` method doesn't expose 
a `progress` argument since this functionality can be replicated by passing a 
custom `data` generator (or within the context manager).


Logging configuration
---------------------

It is possible to configure and disable where the CLI logs are written for each 
entry point. To do this, we can set the following options in its corresponding 
section. For example:

.. code-block:: cfg

  [hdfscli-avro.command] # Section is named COMMAND.command
  log.level = INFO
  log.path = /tmp/hdfscli/avro.log

The following options are available:

+ `log.level`, handler log level (defaults to `DEBUG`).
+ `log.path`, path to log file. The log is rotated every day (keeping a single 
  copy). The default is a file named `COMMAND.log` in your current temporary 
  directory. It is possible to view the currently active log file at any time 
  by using the `--log` option at the command line.
+ `log.disable`, disable logging to a file entirely (defaults to `False`).


Renaming entry points
---------------------

By default the command line entry point will be named `hdfscli`. You can choose 
another name by specifying the `HDFSCLI_ENTRY_POINT` environment variable at 
installation time:

.. code-block:: bash

  $ HDFSCLI_ENTRY_POINT=hdfs pip install hdfs

Extension prefixes will be adjusted similarly (e.g. in the previous example, 
`hdfscli-avro` would become `hdfs-avro`).
