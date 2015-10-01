#!/usr/bin/env python
# encoding: utf-8
# pylint: disable=protected-access

"""Read and write Avro_ files directly from HDFS.

This extension enables streaming decoding and encoding of files from and to
HDFS. It requires the `fastavro` library.

+ :class:`AvroWriter` writes Avro files on HDFS from python objects.
+ :class:`AvroReader` reads Avro files from HDFS into an iterable of records.

Sample usage:

.. literalinclude:: ../examples/avro.py

It also features an entry point (named `hdfscli-avro` by default) which
provides access to the above functionality from the shell. For usage examples
and more information:

.. code-block:: bash

  $ hdfscli-avro --help

.. _Avro: https://avro.apache.org/docs/1.7.7/index.html

"""

from ...util import HdfsError
from json import dumps
from six import integer_types, string_types
import fastavro
import io
import logging as lg
import os
import posixpath as psp
import sys


_logger = lg.getLogger(__name__)

def _infer_schema(obj):
  """Infer Avro type corresponding to a python object.

  :param obj: Python primitive.

  There are multiple limitations with this functions, among which:

  + Nullable fields aren't supported.
  + Only Avro integers will be inferred, so some values may overflow.
  + Records are unnamed.

  """
  if isinstance(obj, bool):
    return 'boolean'
  elif isinstance(obj, string_types):
    return 'string'
  elif isinstance(obj, integer_types): # Python 3 doesn't have `long`.
    return 'int'
  elif isinstance(obj, float):
    return 'float'
  elif isinstance(obj, list):
    if not obj:
      raise ValueError('Cannot infer type of empty array.')
    return {
      'type': 'array',
      'items': _infer_schema(obj[0])
    }
  elif isinstance(obj, dict):
    if not obj:
      raise ValueError('Cannot infer type of empty record.')
    return {
      'type': 'record',
      'fields': [
        {'name': k, 'type': _infer_schema(v)}
        for k, v in obj.items()
      ]
    }
  raise ValueError('Cannot infer type from %s: %r' % (type(obj), obj))


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
    """Read bytes, caching the read if the size matches."""
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
    """Go back using the cached bytes."""
    assert offset == - self._size
    assert whence == os.SEEK_CUR
    assert self._buffer
    self._saught = True


class AvroReader(object):

  """HDFS Avro file reader.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Part-files to read, when reading a distributed file. The
    default is to read all part-files in order. See
    :meth:`hdfs.client.Client.parts` for details.

  The contents of the file will be decoded in a streaming manner, as the data
  is transferred. This makes it possible to use on files of arbitrary size. As
  a convenience, the content summary object of the remote file is available on
  the reader's `content` attribute.

  Usage:

  .. code-block:: python

    with AvroReader(client, 'foo.avro') as reader:
      schema = reader.schema # The remote file's Avro schema.
      content = reader.content # Content metadata (e.g. size).
      for record in reader:
        pass # and its records

  """

  def __init__(self, client, hdfs_path, parts=None):
    self.content = client.content(hdfs_path) #: Content summary of Avro file.
    self._schema = None
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
    self._records = None
    _logger.debug('Instantiated %r.', self)

  def __repr__(self):
    return '<AvroReader(paths=%r)>' % (self._paths, )

  def __enter__(self):

    def _reader():
      """Record generator over all part-files."""
      for path in self._paths:
        with self._client.read(path) as bytes_reader:
          avro_reader = fastavro.reader(_SeekableReader(bytes_reader))
          if not self._schema:
            schema = avro_reader.schema
            _logger.debug('Read schema from %r.', path)
            yield schema
          for record in avro_reader:
            yield record

    self._records = _reader()
    self._schema = next(self._records) # Prime generator to get schema.
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self._records.close()
    _logger.debug('Closed records iterator for %r.', self)

  def __iter__(self): # pylint: disable=non-iterator-returned
    if not self._records:
      raise HdfsError('Iteration is only supported inside a `with` block.')
    return self._records

  @property
  def schema(self):
    """Get the underlying file's schema.

    The schema will only be available after entering the reader's corresponding
    `with` block.

    """
    if not self._schema:
      raise HdfsError('Schema not yet inferred.')
    return self._schema


class AvroWriter(object):

  """Write an Avro file on HDFS from python dictionaries.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param schema: Avro schema. If not specified, the writer will try to infer it
    from the first record sent. There are however limitations regarding what
    can be inferred.
  :param codec: Compression codec. The default is `'null'` (no compression).
  :param sync_interval: Number of bytes after which a block will be written.
  :param sync_marker: 16 byte tag used for synchronization. If not specified,
    one will be generated at random.
  :param \*\*kwargs: Keyword arguments forwarded to
    :meth:`hdfs.client.Client.write`.

  Usage:

  .. code-block:: python

    with AvroWriter(client, 'data.avro') as writer:
      for record in records:
        writer.write(record)

  """

  def __init__(self, client, hdfs_path, schema=None, codec=None,
    sync_interval=None, sync_marker=None, **kwargs):
    self._hdfs_path = hdfs_path
    self._fo = client.write(hdfs_path, **kwargs)
    self._schema = schema
    self._codec = codec or 'null'
    self._sync_interval = sync_interval or 1000 * fastavro._writer.SYNC_SIZE
    self._sync_marker = sync_marker or os.urandom(fastavro._writer.SYNC_SIZE)
    self._writer = None
    _logger.info('Instantiated %r.', self)

  def __repr__(self):
    return '<AvroWriter(hdfs_path=%r)>' % (self._hdfs_path, )

  def __enter__(self):
    if self._writer:
      raise HdfsError('Avro writer cannot be reused.')
    _logger.debug('Opening underlying writer.')
    self._writer = self._write(self._fo.__enter__())
    try:
      self._writer.send(None) # Prime coroutine.
    except Exception: # pylint: disable=broad-except
      if not self._fo.__exit__(*sys.exc_info()):
        raise
    else:
      return self

  def __exit__(self, *exc_info):
    try:
      self._writer.close()
      _logger.debug('Closed underlying writer.')
    finally:
      self._fo.__exit__(*exc_info)

  @property
  def schema(self):
    """Avro schema."""
    if not self._schema:
      raise HdfsError('Schema not yet inferred.')
    return self._schema

  def write(self, record):
    """Store a record.

    :param record: Record object to store.

    Only available inside the `with` block.

    """
    if not self._writer:
      raise HdfsError('Avro writer not available outside context block.')
    self._writer.send(record)

  def _write(self, fo):
    """Coroutine to write to a file object."""
    buf = fastavro._writer.MemoryIO()
    block_writer = fastavro._writer.BLOCK_WRITERS[self._codec]
    n_records = 0
    n_block_records = 0

    # Cache a few variables.
    sync_interval = self._sync_interval
    write_data = fastavro._writer.write_data
    write_long = fastavro._writer.write_long

    def dump_header():
      """Write header."""
      metadata = {
        'avro.codec': self._codec,
        'avro.schema': dumps(self._schema),
      }
      fastavro._writer.write_header(fo, metadata, self._sync_marker)
      _logger.debug('Wrote header. Sync marker: %r', self._sync_marker)
      fastavro._writer.acquaint_schema(self._schema)

    def dump_data():
      """Write contents of memory buffer to file object."""
      write_long(fo, n_block_records)
      block_writer(fo, buf.getvalue())
      fo.write(self._sync_marker)
      buf.seek(0)
      buf.truncate(0)

    if self._schema:
      dump_header()
    try:
      while True:
        record = (yield)
        if not n_records:
          if not self._schema:
            self._schema = _infer_schema(record)
            _logger.info('Inferred schema: %s', dumps(self._schema))
            dump_header()
          schema = self._schema
        write_data(buf, record, schema)
        n_block_records += 1
        n_records += 1
        if buf.tell() >= sync_interval:
          dump_data()
          n_block_records = 0
    except GeneratorExit:
      if buf.tell():
        dump_data()
      fo.flush()
      _logger.info('Finished writing %s records.', n_records)
