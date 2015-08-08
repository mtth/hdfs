#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI Avro: an Avro extension for HdfsCLI.

Usage:
  hdfscli-avro schema [-a ALIAS] [-v...] HDFS_PATH
  hdfscli-avro read [-a ALIAS] [-v...] [-f FREQ|-n NUM] [-p PARTS] HDFS_PATH
  hdfscli-avro -h | -L

Commands:
  schema                        Pretty print schema.
  read                          Read Avro records as JSON.

Arguments:
  HDFS_PATH                     Remote path to Avro file or directory
                                containing Avro part-files.

Options:
  -L --log                      Show path to current log file and exit.
  -a ALIAS --alias=ALIAS        Alias.
  -f FREQ --freq=FREQ           Probability of sampling a record.
  -h --help                     Show this message and exit.
  -n NUM --num=NUM              Cap number of records to output.
  -p PARTS --parts=PARTS        Part-files to read. Specify a number to
                                randomly select that many, or a comma-separated
                                list of numbers to read only these. Use a
                                number followed by a comma (e.g. `1,`) to get a
                                unique part-file. The default is to read all
                                part-files.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).

Examples:
  hdfscli-avro schema /data/impressions.avro
  hdfscli-avro read -a dev snapshot.avro >snapshot.jsonl
  hdfscli-avro read -f 0.1 -p 2,3 clicks.avro

"""

from __future__ import absolute_import
from ..__main__ import CliConfig, parse_arg
from ..client import Client
from ..util import HdfsError, catch
from collections import deque
from docopt import docopt
from io import BytesIO
from itertools import islice
from json import dumps
from random import random
import fastavro
import io
import logging as lg
import os
import posixpath as psp
import sys


def _get_type(obj, allow_null=False):
  """Infer Avro type corresponding to a python object.

  :param obj: Python primitive.
  :param allow_null: Allow null values.

  """
  if allow_null:
    raise NotImplementedError('TODO')
  if isinstance(obj, bool):
    schema_type = 'boolean'
  elif isinstance(obj, string_types):
    schema_type = 'string'
  elif isinstance(obj, int):
    schema_type = 'int'
  elif isinstance(obj, long):
    schema_type = 'long'
  elif isinstance(obj, float):
    schema_type = 'float'
  return schema_type # TODO: Add array and record support.

def infer_schema(obj):
  """Infer schema from dictionary.

  :param obj: Dictionary.

  """
  return {
    'type': 'record',
    'name': 'element',
    'fields': [
      {'name': k, 'type': _get_type(v)}
      for k, v in obj.items()
    ]
  }


class _SeekableReader(object):

  """Customized reader for Avro.

  :param reader: Non-seekable reader.
  :param size: For testing.

  It detects reads of sync markers' sizes and will buffer these. Note that this
  reader is heavily particularized to how the `fastavro` library performs Avro
  decoding.

  """

  sync_size = 16

  def __init__(self, reader, size=None):
    self._reader = reader
    self._size = size or self.sync_size
    self._buffer = None
    self._saught = False

  def read(self, nbytes):
    buf = self._buffer
    if self._saught:
      assert buf
      missing_bytes = nbytes - len(buf)
      if missing_bytes < 0:
        chunk = buf[:nbytes]
        self._buffer = buf[nbytes:]
      else:
        chunk = buf
        if missing_bytes:
          chunk += self._reader.read(missing_bytes)
        self._buffer = None
        self._saught = False
    else:
      self._buffer = None
      chunk = self._reader.read(nbytes)
      if nbytes == self._size:
        self._buffer = chunk
    return chunk

  def seek(self, offset, whence):
    assert offset == - self._size
    assert whence == os.SEEK_CUR
    assert self._buffer
    self._saught = True


class _AvroReader(object):

  """Lazy remote Avro file reader.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Cf. :meth:`hdfs.client.Client.parts`.

  Usage:

  .. code-block:: python

    with _AvroReader(client, 'foo.avro') as reader:
      schema = reader.schema # The remote file's Avro schema.
      content = reader.content # Content metadata (e.g. size).
      for record in reader:
        pass # and its records

  """

  def __init__(self, client, hdfs_path, parts=None):
    self.content = client.content(hdfs_path)
    self.schema = None
    if self.content['directoryCount']:
      # This is a folder.
      self._paths = [
        psp.join(hdfs_path, fname)
        for fname in client.parts(hdfs_path, parts)
      ]
    else:
      # This is a single file.
      self._paths = [hdfs_path]
    self._client = client

  def __enter__(self):

    def _reader():
      """Record generator over all part-files."""
      for path in self._paths:
        with self._client.read(path, chunk_size=0) as bytes_reader:
          avro_reader = fastavro.reader(_SeekableReader(bytes_reader))
          if not self.schema:
            yield avro_reader.schema
          for record in avro_reader:
            yield record

    self.records = _reader()
    self.schema = self.records.next() # Prime generator to get schema.
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.records.close()

  def __iter__(self):
    return self.records


def read(client, hdfs_path, parts=None):
  """Read an Avro file from HDFS into python dictionaries.

  :param client: :class:`~hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Cf. :meth:`~hdfs.client.Client.parts`.

  Sample usage:

  .. code::

    with read(client, 'data.avro') as reader:
      schema = reader.schema # The remote file's Avro schema.
      content = reader.content # Content metadata (e.g. size).
      for record in reader:
        pass # The records.

  """
  return _AvroReader(client, hdfs_path, parts=parts)

def write(client, hdfs_path, records, schema, **kwargs):
  """Write an Avro file on HDFS from python dictionaries.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param records: Generator of records to write.
  :param schema: Avro schema. See :func:`infer_schema` for an easy way to
    generate schemas in most cases.
  :param \*\*kwargs: Keyword arguments forwarded to :meth:`Client.write`.

  """
  # TODO: Add schema inference.
  with client.write(hdfs_path, **kwargs) as bytes_writer:
    fastavro.writer(bytes_writer, schema, records)


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__)
  config = CliConfig('hdfscli-avro', args['--verbose'])
  client = config.create_client(args['--alias'])
  if args['--log']:
    handler = config.get_file_handler()
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      sys.stdout.write('No log file active.\n')
    sys.exit(0)
  parts = parse_arg(args, '--parts', int, ',')
  with read(client, args['HDFS_PATH'], parts) as reader:
    if args['schema']:
      sys.stdout.write('%s\n' % (dumps(reader.schema, indent=2), ))
    elif args['read']:
      num = parse_arg(args, '--num', int)
      freq = parse_arg(args, '--freq', float)
      if freq:
        for record in reader:
          if random() <= freq:
            sys.stdout.write('%s\n' % (dumps(record), ))
      else:
        for record in islice(reader, num):
          sys.stdout.write('%s\n' % (dumps(record), ))

if __name__ == '__main__':
  main()
