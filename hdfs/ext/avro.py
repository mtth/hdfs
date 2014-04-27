#!/usr/bin/env python
# encoding: utf-8

"""HdfsAvro: an Avro extension for HdfsCLI.

Usage:
  hdfsavro [-a ALIAS] --schema PATH
  hdfsavro [-a ALIAS] --head [-n NUM] [-p PARTS] PATH

Commands:
  --head                        Pretty print first few records.
  --schema                      Pretty print schema.

Arguments:
  PATH                          Remote path to Avro file. `#LATEST` can be used
                                to indicate the most recently updated file.

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
from docopt import docopt
from itertools import islice
from json import dumps
from tempfile import mkstemp
import avro as av
import avro.datafile as avd
import avro.io as avi
import os
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
try:
  import snappy
except ImportError:
  pass


class _DatumReader(avi.DatumReader):

  """DatumReader that HEX-encodes "fixed" fields.

  If we don't do this, the JSON encoder will blow up when faced with non-
  unicode characters. This is simpler and more efficient than the alternative
  of writing our custom JSON encoder.

  """

  def read_fixed(self, writers_schema, readers_schema, decoder):
    """Return HEX-encoded value instead of raw byte-string."""
    return '0x' + decoder.read(writers_schema.size).encode('hex')


class _LazyBinaryDecoder(avi.BinaryDecoder):

  """Custom BinaryDecoder that can read lazily from a remote source.

  :param reader: Output of :meth:`hdfs.client.Client.read`.
  :param path: Path to file used as local buffer.

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


class _LazyFileReader(object):

  """Read remote avro file.

  :param reader: Output of :meth:`~hdfs.client.Client.read`.

  This is a more efficient implementation of the :class:`~avro.DataFileReader`,
  customized for reading remote files. The default one reads the entire file
  before emitting any records which is dramatically slow in our case.

  """

  def __init__(self, reader):
    self.schema = None
    self._reader = reader
    self._datum_reader = _DatumReader()
    self._raw_decoder = None
    self._datum_decoder = None
    self._block_count = 0
    self._sync_marker = None
    self._meta = None
    self._codec = None
    self._path = None

  def __enter__(self):
    # create temporary file to serve as buffer
    desc, self._path = mkstemp()
    os.close(desc)
    self._raw_decoder = _LazyBinaryDecoder(self._reader, self._path)
    # load header data
    header = self._read_header()
    self._sync_marker = header['sync']
    self._meta = header['meta']
    self._codec = self._meta['avro.codec']
    # get ready to read by attaching schema to the datum reader
    self.schema = av.schema.parse(self._meta.get(avd.SCHEMA_KEY))
    self._datum_reader.writers_schema = self.schema
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    os.remove(self._path)

  def __iter__(self):
    return self

  def _read_header(self):
    """Parse header into dictionary."""
    header = self._datum_reader.read_data(
      avd.META_SCHEMA, avd.META_SCHEMA, self._raw_decoder
    )
    magic = header.get('magic')
    if magic != '0x4f626a01': # HEX encoding of avd.MAGIC
      msg = 'Invalid magic #: %s doesn\'t match HEX(%s).' % (magic, avd.MAGIC)
      raise av.schema.AvroException(msg)
    codec = header['meta'].setdefault('avro.codec', 'null')
    if codec not in avd.VALID_CODECS:
      raise avd.DataFileException('Unknown codec: %r.' % (codec, ))
    return header

  def _read_block_header(self):
    """Find block count and set the decoder accordingly."""
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
      self._datum_decoder = avi.BinaryDecoder(StringIO(uncompressed))
    elif self._codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = self._raw_decoder.read_long()
      data = self._raw_decoder.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._datum_decoder = avi.BinaryDecoder(StringIO(uncompressed))
      self._raw_decoder.check_crc32(uncompressed)
    else:
      raise avd.DataFileException("Unknown codec: %r" % self.codec)

  def next(self):
    """Return the next datum in the file."""
    if not self._block_count:
      proposed_sync_marker = self._raw_decoder.read(avd.SYNC_SIZE)
      if proposed_sync_marker != self._sync_marker:
        self._raw_decoder.seek(-avd.SYNC_SIZE, 1)
      self._read_block_header()
    datum = self._datum_reader.read(self._datum_decoder)
    self._block_count -= 1
    return datum


def load(client, hdfs_path, parts=None):
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
      with _LazyFileReader(client.read(path)) as reader:
        if not index:
          yield reader.schema # avoids having to expose the readers directly
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
  schema, reader = load(client, args['PATH'] or '', parts)
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