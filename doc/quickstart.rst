.. default-role:: code


Quickstart
==========


Installation
------------

Using pip_:

.. code-block:: bash

  $ pip install hdfs

By default none of the package requirements for extensions are installed. To do 
so simply suffix the package name with the desired extensions:

.. code-block:: bash

  $ pip install hdfs[avro,dataframe,kerberos]


Configuration
-------------

HdfsCLI uses *aliases* to figure out how to connect to different HDFS clusters. 
These are defined in HdfsCLI's configuration file, located by default at 
`~/.hdfscli.cfg` (or elsewhere by setting the `HDFSCLI_CONFIG` environment 
variable correspondingly). See below for a sample configuration defining two 
aliases, `dev` and `prod`:

.. code-block:: cfg

  [global]
  default.alias = dev

  [dev.alias]
  url = http://dev.namenode:port

  [prod.alias]
  url = http://prod.namenode:port
  root = /jobs/

Each alias is defined as its own `ALIAS.alias` section which must at least 
contain a `url` option with the URL to the namenode (including protocol and 
port). All other options can be omitted. If specified, `client` determines 
which :class:`hdfs.client.Client` class to use and the remaining options are 
passed as keyword arguments to the appropriate constructor. The currently 
available client classes are:

+ :class:`~hdfs.client.InsecureClient` (the default)
+ :class:`~hdfs.client.TokenClient`

See the :ref:`Kerberos extension <kerberos_extension>` to enable the 
:class:`~hdfs.ext.kerberos.KerberosClient` and :ref:`custom_client` to learn 
how to use other client classes.

Finally, note the `default.alias` entry in the global configuration section 
which will be used as default alias if none is specified.


Command line interface
----------------------

HdfsCLI comes by default with a single entry point `hdfscli` which provides a 
convenient interface to perform common actions. All its commands accept an 
`--alias` argument (described above), which defines against which cluster to 
operate.


Downloading and uploading files
*******************************

HdfsCLI supports downloading and uploading files and folders transparently from 
HDFS (we can also specify the degree of parallelism by using the `--threads` 
option).

.. code-block:: bash

  $ # Write a single file to HDFS.
  $ hdfscli upload --alias=dev weights.json models/
  $ # Read all files inside a folder from HDFS and store them locally.
  $ hdfscli download export/results/ "results-$(date +%F)"

If reading (resp. writing) a single file, its contents can also be streamed to 
standard out (resp. from standard in) by using `-` as path argument:

.. code-block:: bash

  $ # Read a file from HDFS and append its contents to a local log file.
  $ hdfscli download logs/1987-03-23.txt - >>logs

By default HdfsCLI will throw an error if trying to write to an existing path 
(either locally or on HDFS). We can force the path to be overwritten with the 
`--force` option.


.. _interactive_shell:

Interactive shell
*****************

The `interactive` command (used also when no command is specified) will create 
an HDFS client and expose it inside a python shell (using IPython_ if 
available). This makes is convenient to perform file system operations on HDFS 
and interact with its data. See :ref:`python_bindings` below for an overview of 
the methods available.

.. code-block:: bash

  $ hdfscli --alias=dev

  Welcome to the interactive HDFS python shell.
  The HDFS client is available as `CLIENT`.

  In [1]: CLIENT.list('data/')
  Out[1]: ['1.json', '2.json']

  In [2]: CLIENT.status('data/2.json')
  Out[2]: {
    'accessTime': 1439743128690,
    'blockSize': 134217728,
    'childrenNum': 0,
    'fileId': 16389,
    'group': 'supergroup',
    'length': 2,
    'modificationTime': 1439743129392,
    'owner': 'drwho',
    'pathSuffix': '',
    'permission': '755',
    'replication': 1,
    'storagePolicy': 0,
    'type': 'FILE'
  }

  In [3]: CLIENT.delete('data/2.json')
  Out[3]: True

Using the full power of python lets us easily perform more complex operations 
such as renaming folder which match some pattern, deleting files which haven't 
been accessed for some duration, finding all paths owned by a certain user, 
etc.


More
****

Cf. `hdfscli --help` for the full list of commands and options.


.. _python_bindings:

Python bindings
---------------


Instantiating a client
**********************

The simplest way of getting a :class:`hdfs.client.Client` instance is by using 
the :ref:`interactive_shell` described above, where the client will be 
automatically available. To instantiate a client programmatically, there are 
two options:

The first is to import the client class and call its constructor directly. This 
is the most straightforward and flexible, but doesn't let us reuse our 
configured aliases:

.. code-block:: python

  from hdfs import InsecureClient
  client = InsecureClient('http://host:port')

The second leverages the :class:`hdfs.config.Config` class to load an existing 
configuration file (defaulting to the same one as the CLI) and create clients 
from existing aliases:

.. code-block:: python

  from hdfs import Config
  client = Config().get_client('dev')


Reading and writing files
*************************

The :meth:`~hdfs.client.Client.read` method provides a file-like interface for 
reading files from HDFS. It must be used in a `with` block (making sure that 
connections are always properly closed):

.. code-block:: python

  # Loading a file in memory.
  with client.read('features') as reader:
    features = reader.read()

  # Directly deserializing a JSON object.
  with client.read('model.json') as reader:
    from json import load
    model = load(reader)

If a `chunk_size` argument is passed, the method will return a generator 
instead, making it sometimes simpler to stream the file's contents.

.. code-block:: python

  # Stream a file.
  with client.read('features', chunk_size=8096) as reader:
    for chunk in reader:
      pass

Writing files to HDFS is done using the :meth:`~hdfs.client.Client.write` 
method which returns a file-like writable object:

.. code-block:: python

  # Writing part of a file.
  with open('samples') as reader, client.write('samples') as writer:
    for line in reader:
      if line.startswith('-'):
        writer.write(line)

  # Writing a serialized JSON object.
  with client.write('model.json') as writer:
    from json import dump
    dump(model, writer)

For convenience, it is also possible to pass an iterable `data` argument 
directly to the method.

.. code-block:: python

  # This is equivalent to the JSON example above.
  from json import dumps
  client.write('model.json', dumps(model))


Exploring the file system
*************************

All :class:`~hdfs.client.Client` subclasses expose a variety of methods to 
interact with HDFS. Most are modeled directly after the WebHDFS operations, a 
few of these are shown in the snippet below:

.. code-block:: python

  # Retrieving a file or folder content summary.
  content = client.content('dat')

  # Listing all files inside a directory.
  fnames = client.list('dat')

  # Retrieving a file or folder status.
  status = client.status('dat/features')

  # Renaming ("moving") a file.
  client.rename('dat/features', 'features')

  # Deleting a file or folder.
  client.delete('dat', recursive=True)

Other methods build on these to provide more advanced features:

.. code-block:: python

  # Download a file or folder locally.
  client.download('dat', 'dat', n_threads=5)

  # Get all files under a given folder (arbitrary depth).
  import posixpath as psp
  fpaths = [
    psp.join(dpath, fname)
    for dpath, _, fnames in client.walk('predictions')
    for fname in fnames
  ]

See the :ref:`api_reference` for the comprehensive list of methods available.


Checking existence
******************

Most of the methods described above will raise an :class:`~hdfs.util.HdfsError` 
if called on a missing path. The recommended way of checking whether a path 
exists is using the :meth:`~hdfs.client.Client.content` or 
:meth:`~hdfs.client.Client.status` methods with a `strict=False` argument (in 
which case they will return `None` on a missing path).


More
****

See the :ref:`advance_usage` section to learn more.


.. _pip: http://www.pip-installer.org/en/latest/
.. _IPython: http://ipython.org/
