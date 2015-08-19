.. default-role:: code


HdfsCLI
=======

API and command line interface for HDFS.

+ `Project homepage on GitHub`_
+ `PyPI entry`_


Installation
------------

Using pip_:

.. code-block:: bash

  $ pip install hdfs

By default none of the package requirements for extensions are installed. To do 
so simply suffix the package name with the desired extensions:

.. code-block:: bash

  $ pip install hdfs[avro,dataframe,kerberos]


User guide
----------

.. toctree::
  :maxdepth: 2

  quickstart
  advanced
  api


.. _Project homepage on GitHub: https://github.com/mtth/hdfs
.. _PyPI entry: https://pypi.python.org/pypi/hdfs/
.. _pip: http://www.pip-installer.org/en/latest/
