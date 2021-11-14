# HdfsCLI [![CI](https://github.com/mtth/hdfs/actions/workflows/ci.yml/badge.svg)](https://github.com/mtth/hdfs/actions/workflows/ci.yml) [![Pypi badge](https://badge.fury.io/py/hdfs.svg)](https://pypi.python.org/pypi/hdfs/) [![Downloads badge](https://img.shields.io/pypi/dm/hdfs.svg)](https://pypistats.org/packages/hdfs)

API and command line interface for HDFS.

```
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
```

## Features

* Python (2 and 3) bindings for the [WebHDFS][] (and [HttpFS][]) API,
  supporting both secure and insecure clusters.
* Command line interface to transfer files and start an interactive client
  shell, with aliases for convenient namenode URL caching.
* Additional functionality through optional extensions:

  + `avro`, to [read and write Avro files directly from HDFS][].
  + `dataframe`, to [load and save Pandas dataframes][].
  + `kerberos`, to [support Kerberos authenticated clusters][].

See the [documentation][] to learn more.

## Getting started

```sh
$ pip install hdfs
```

Then hop on over to the [quickstart][] guide. A [Conda
feedstock](https://github.com/conda-forge/python-hdfs-feedstock) is also
available.

## Testing

HdfsCLI is tested against both [WebHDFS][] and [HttpFS][]. There are two ways
of running tests (see `scripts/` for helpers to set up a test HDFS cluster):

```sh
$ HDFSCLI_TEST_URL=http://localhost:50070 nosetests # Using a namenode's URL.
$ HDFSCLI_TEST_ALIAS=dev nosetests # Using an alias.
```

## Contributing

We'd love to hear what you think on the [issues][] page. Pull requests are also
most welcome!

[HttpFS]: http://hadoop.apache.org/docs/current/hadoop-hdfs-httpfs/
[WebHDFS]: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
[read and write Avro files directly from HDFS]: https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.ext.avro
[load and save Pandas dataframes]: https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.ext.dataframe
[support Kerberos authenticated clusters]: https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.ext.kerberos
[documentation]: https://hdfscli.readthedocs.io/
[quickstart]: https://hdfscli.readthedocs.io/en/latest/quickstart.html
[issues]: https://github.com/mtth/hdfs/issues
