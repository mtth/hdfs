#!/usr/bin/env python
# encoding: utf-8

"""HdfsAvro: an Avro extension for HdfsCLI.

Usage:
  hdfsavro [-a ALIAS] --schema PATH
  hdfsavro [-a ALIAS] --head [-n NUM] [-p PARTS] PATH

Commands:
  --head                        Show first few records.
  --schema                      Pretty print schema.

Options:
  -a ALIAS --alias=ALIAS        Alias.
  -h --help                     Show this message and exit.
  -n NUM --num=NUM              Number of records to output [default: 5].
  -p PARTS --parts=PARTS        Part-files. `1,` to get a unique part-file. By
                                default, use all part-files.

"""

from __future__ import absolute_import
from .. import get_client_from_alias
from ..util import HdfsError, catch
import zlib
from avro.io import BinaryDecoder, DatumReader
from docopt import docopt
from itertools import islice
from json import dumps
from tempfile import mkstemp
import avro as av
import avro.datafile as avd
import os
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
try:
  import snappy
except ImportError:
  pass


class LazyBinaryDecoder(BinaryDecoder):

  """Custom BinaryDecoder that can read lazily from a remote source.

  :param reader: Output of :meth:`~hdfs.client.Client.read`.

  """

  reader = None # override read-only property

  def __init__(self, reader, path):
    self.file = open(path, 'w+')
    self.reader = reader
    self.loaded = 0
    self.exhausted = False

  def load(self, size):
    """Load more data. Should only be called when at the end of the file!"""
    pos = self.file.tell()
    while size > 0:
      try:
        chunk = self.reader.next()
      except StopIteration:
        self.exhausted = True
        break
      else:
        size -= len(chunk)
        self.file.write(chunk)
    self.file.seek(pos)

  def read(self, size):
    """Read from file. We use the fact that size is always specified here."""
    data = self.file.read(size)
    size -= len(data)
    if size and not self.exhausted:
      self.load(size)
      data += self.file.read(size)
    return data

  def seek(self, offset, whence=None):
    """Jump to position in file."""
    self.file.seek(offset, whence)

  def skip(self, size):
    """Skip bytes."""
    missing = len(self.file.read(size)) - size
    if missing and not self.exhausted:
      self.load(missing)
      self.file.seek(missing)


class LazyFileReader(avd.DataFileReader):

  """Read remote avro file.

  :param reader: Output of :meth:`~hdfs.client.Client.read`.

  This is a more efficient implementation of the :class:`~avro.DataFileReader`,
  customized for reading remote files.

  """

  def __init__(self, reader):
    self._reader = reader
    self._datum_reader = DatumReader()
    self._datum_decoder = None
    self._block_count = 0

  def __enter__(self):
    desc, self._path = mkstemp()
    os.close(desc)
    # load header data
    self._raw_decoder = LazyBinaryDecoder(self.reader, self._path)
    header = self._read_header()
    self._sync_marker = header['sync']
    self._meta = header['meta']
    self._codec = self._meta['avro.codec']
    # get ready to read
    self.schema = av.schema.parse(self._meta.get(avd.SCHEMA_KEY))
    self._datum_reader.writers_schema = self.schema
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    os.remove(self._path)

  def __iter__(self):
    return self

  def _read_header(self):
    """Read header into dictionary."""
    header = self._datum_reader.read_data(
      avd.META_SCHEMA, avd.META_SCHEMA, self._raw_decoder
    )
    magic = header.get('magic')
    if magic != avd.MAGIC:
      msg = 'Invalid magic #: %s doesn\'t match %s.' % (magic, avd.MAGIC)
      raise av.schema.AvroException(msg)
    codec = header['meta'].setdefault('avro.codec', 'null')
    if codec not in avd.VALID_CODECS:
      raise avd.DataFileException('Unknown codec: %r.' % (codec, ))
    return header

  def _read_block_header(self):
    self._block_count = self._raw_decoder.read_long()
    if self._codec == 'null':
      # Skip a long; we don't need to use the length.
      self._raw_decoder.skip_long()
      self._datum_decoder = self._raw_decoder
    elif self._codec == 'deflate':
      # Compressed data is stored as (length, data), which
      # corresponds to how the "bytes" type is encoded.
      data = self._raw_decoder.read_bytes()
      # -15 is the log of the window size; negative indicates
      # "raw" (no zlib headers) decompression.  See zlib.h.
      uncompressed = zlib.decompress(data, -15)
      self._datum_decoder = BinaryDecoder(StringIO(uncompressed))
    elif self._codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = self._raw_decoder.read_long()
      data = self._raw_decoder.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._datum_decoder = BinaryDecoder(StringIO(uncompressed))
      self._raw_decoder.check_crc32(uncompressed)
    else:
      raise avd.DataFileException("Unknown codec: %r" % self.codec)

  def next(self):
    """Return the next datum in the file."""
    if not self.block_count:
      proposed_sync_marker = self._raw_decoder.read(avd.SYNC_SIZE)
      if proposed_sync_marker != self._sync_marker:
        self._raw_decoder.seek(-avd.SYNC_SIZE, 1)
      self._read_block_header()
    datum = self._datum_reader.read(self._datum_decoder)
    self.block_count -= 1
    return datum

def get_reader(client, hdfs_path, parts):
  """Return schema and record generator.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Cf. :meth:`hdfs.client.Client.parts`.

  """
  paths = client.parts(hdfs_path, parts)
  if not paths:
    raise HdfsError('No avro file found at %r.', hdfs_path)
  def _reader():
    """Record generator over all part-files."""
    for index, path in enumerate(paths):
      with LazyFileReader(client.read(path)) as reader:
        if not index:
          yield reader.schema # hackish but convenient
        for record in reader:
          yield record
  gen = _reader()
  schema = gen.next()
  return schema, gen


@catch(HdfsError)
def main(args):
  """Entry point.

  :param args: `docopt` dictionary.

  """
  client = get_client_from_alias(args['--alias'])
  try:
    parts = args['--parts'] or '0'
    if ',' in parts:
      parts = [int(p) for p in parts.split(',') if p]
    else:
      parts = int(parts)
  except ValueError:
    raise HdfsError('Invalid `--parts` option: %r.', args['--parts'])
  schema, reader = get_reader(client, args['PATH'] or '', parts)
  if args['--schema']:
    print dumps(schema.to_json(), indent=2)
  elif args['--head']:
    try:
      n_records = int(args['--num'])
    except ValueError:
      raise HdfsError('Invalid `--num` option: %r.', args['--num'])
    for record in islice(reader, n_records):
      print dumps(record, indent=2)

if __name__ == '__main__':
  main(docopt(__doc__))
