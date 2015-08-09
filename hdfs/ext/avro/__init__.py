#!/usr/bin/env python
# encoding: utf-8

"""Avro extension.

TODO

Without this extension:

.. code-block:: python

  with client.write(hdfs_path) as bytes_writer:
    fastavro.writer(bytes_writer, schema, records)

"""

from ...util import HdfsError
import fastavro
import logging as lg
import os
import posixpath as psp


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


class AvroReader(object):

  """Lazy remote Avro file reader.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Cf. :meth:`hdfs.client.Client.parts`.

  Usage:

  .. code-block:: python

    with AvroReader(client, 'foo.avro') as reader:
      schema = reader.schema # The remote file's Avro schema.
      content = reader.content # Content metadata (e.g. size).
      for record in reader:
        pass # and its records

  """

  def __init__(self, client, hdfs_path, parts=None):
    self.content = client.content(hdfs_path)
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

  def __enter__(self):

    def _reader():
      """Record generator over all part-files."""
      for path in self._paths:
        with self._client.read(path, chunk_size=0) as bytes_reader:
          avro_reader = fastavro.reader(_SeekableReader(bytes_reader))
          if not self._schema:
            yield avro_reader.schema
          for record in avro_reader:
            yield record

    self.records = _reader()
    self._schema = self.records.next() # Prime generator to get schema.
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.records.close()

  def __iter__(self):
    return self.records

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
  :param records: Generator of records to write.
  :param schema: Avro schema. See :func:`infer_schema` for an easy way to
    generate schemas in most cases.
  :param \*\*kwargs: Keyword arguments forwarded to :meth:`Client.write`.

  Usage:

  .. code::

    with AvroWriter(client, 'data.avro') as writer:
      for record in records:
        writer.send(record)

  """

  def __init__(self, client, hdfs_path, schema=None, **kwargs):
    self._writer = client.write(hdfs_path, **kwargs)
    self._schema = schema

  def __enter__(self):
    self._writer.__enter__()

    def _writer(schema=self._schema):
      """Records coroutine."""
      writer = None
      try:
        while True:
          obj = (yield)
      except GeneratorExit: # No more records.
        pass
        if writer:
          writer.flush() # make sure everything has been written to the buffer
          buf.seek(0)
          self._client.write(hdfs_path, buf, overwrite=overwrite)
      finally:
        if writer:
          writer.close()

    self._records = _writer()
    self._records.send(None) # Prime coroutine.
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    return self._writer.__exit__(exc_type, exc_value, traceback)

  @property
  def schema(self):
    """Avro schema."""
    if not self._schema:
      raise HdfsError('Schema not yet inferred.')
    return self._schema

  def send(self, record):
    """Store a record.

    :param record: Record object to store.

    """
    self._writer.send(record)
