#!/usr/bin/env python
# encoding: utf-8

"""HdfsAvro: an Avro extension for HdfsCLI.

Usage:
  hdfsavro [-a ALIAS] --schema PATH
  hdfsavro [-a ALIAS] --head [-n NUM] [-p PARTS] PATH
  hdfsavro [-a ALIAS] --sample (-f FREQ | -n NUM) PATH

Commands:
  --schema                      Pretty print schema.
  --head                        Pretty print first few records as JSON.
  --sample                      Sample records and output JSON.

Arguments:
  PATH                          Remote path to Avro file.

Options:
  -a ALIAS --alias=ALIAS        Alias.
  -h --help                     Show this message and exit.
  -f FREQ --freq=FREQ           Probability of sampling a record.
  -n NUM --num=NUM              Number of records to output [default: 5].
  -p PARTS --parts=PARTS        Part-files. `1,` to get a unique part-file. By
                                default, use all part-files.

"""

from __future__ import absolute_import
from ..client import Client
from ..util import HdfsError, catch
from docopt import docopt
from itertools import islice
from json import dumps
from random import random, shuffle
from tempfile import mkstemp
import avro as av
import avro.datafile as avd
import avro.io as avi
import os
import zlib
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
try:
  import snappy
except ImportError:
  pass


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

  def _load(self, size):
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
      self._load(size)
      data += self.file.read(size)
    return data

  def seek(self, offset, whence=None):
    """Jump to position in file."""
    self.file.seek(offset, whence)

  def skip(self, size):
    """Skip bytes."""
    missing = len(self.file.read(size)) - size
    if missing and not self.exhausted:
      self._load(missing)
      self.file.seek(missing)


class _DatumReader(avi.DatumReader):

  """DatumReader that HEX-encodes "fixed" fields.

  If we don't do this, the JSON encoder will blow up when faced with non-
  unicode characters. This is simpler and more efficient than the alternative
  of writing our custom JSON encoder.

  """

  def read_fixed(self, writers_schema, readers_schema, decoder):
    """Return HEX-encoded value instead of raw byte-string."""
    return '0x' + decoder.read(writers_schema.size).encode('hex')


class _DataFileReader(object):

  """Read remote avro file.

  :param reader: Output of :meth:`~hdfs.client.Client.read`.

  This is a more efficient implementation of the :class:`~avro.DataFileReader`,
  customized for reading remote files. The default one reads the entire file
  before emitting any records which is dramatically slow in our case.

  It should be used as a context manager. E.g.

  .. code-block:: python

    reader = client.read('foo.avro')
    with _DataFileReader(reader) as avro_reader:
      schema = avro_reader.schema
      for record in avro_reader:
        pass # do something ...

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
    self._sync_marker = header['sync'][2:].decode('hex')
    self._meta = header['meta']
    self._codec = self._meta['avro.codec']
    # get ready to read by attaching schema to the datum reader
    self.schema = av.schema.parse(self._meta.get(avd.SCHEMA_KEY))
    self._datum_reader.writers_schema = self.schema
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self._reader.close()
    os.remove(self._path)

  def __iter__(self):
    return self

  def _read_header(self):
    """Parse header into dictionary."""
    header = self._datum_reader.read_data(
      avd.META_SCHEMA, avd.META_SCHEMA, self._raw_decoder
    )
    magic = header.get('magic')[2:].decode('hex')
    if magic != avd.MAGIC:
      msg = 'Invalid magic #: %s doesn\'t match %s.' % (magic, avd.MAGIC)
      raise av.schema.AvroException(msg)
    codec = header['meta'].setdefault('avro.codec', 'null')
    if codec not in avd.VALID_CODECS:
      raise avd.DataFileException('Unknown codec: %r.' % (codec, ))
    return header

  def _read_block_header(self):
    """Find block count and set the decoder accordingly."""
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
      raise avd.DataFileException("Unknown codec: %r" % self._codec)

  def next(self):
    """Return the next datum in the file."""
    if not self._block_count:
      proposed_sync_marker = self._raw_decoder.read(avd.SYNC_SIZE)
      if proposed_sync_marker != self._sync_marker:
        self._raw_decoder.seek(-avd.SYNC_SIZE, 1)
      try:
        self._block_count = self._raw_decoder.read_long()
      except TypeError: # end of file
        raise StopIteration
      else:
        self._read_block_header()
    datum = self._datum_reader.read(self._datum_decoder)
    self._block_count -= 1
    return datum


class AvroReader(object):

  """Lazy remote Avro file reader.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param parts: Cf. :meth:`hdfs.client.Client.parts`.

  """

  def __init__(self, client, hdfs_path, parts=None):
    self._client = client
    self._parts = client.parts(hdfs_path, parts)
    if not self._parts:
      raise HdfsError('No Avro file found at %r.', hdfs_path)
    def _reader():
      """Record generator over all part-files."""
      paths = self._parts.keys()
      if not parts:
        shuffle(paths)
      for index, path in enumerate(paths):
        with _DataFileReader(client.read(path)) as reader:
          if not index:
            yield reader.schema # avoids having to expose the readers directly
          for record in reader:
            yield record

    #:  Avro record generator. For convenience, you can also iterate directly
    #:  on the :class:`AvroReader` object. E.g.
    #:
    #:  .. code-block:: python
    #:
    #:    reader = AvroReader(client, 'foo.avro')
    #:    for record in reader:
    #:      print record.to_json()
    #:
    self.records = _reader()

    #:  Avro schema.
    self.schema = self.records.next()

  def __iter__(self):
    return self.records

  @property
  def length(self):
    """Total reader length in bytes."""
    return sum(s['length'] for s in self._parts.values())


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__)
  client = Client.from_alias(args['--alias'])
  try:
    parts = args['--parts'] or '0'
    if ',' in parts:
      parts = [int(p) for p in parts.split(',') if p]
    else:
      parts = int(parts)
  except ValueError:
    raise HdfsError('Invalid `--parts` option: %r.', args['--parts'])
  avro_file = AvroReader(client, args['PATH'] or '', parts)
  if args['--schema']:
    print dumps(avro_file.schema.to_json(), indent=2)
  elif args['--head']:
    try:
      n_records = int(args['--num'])
    except ValueError:
      raise HdfsError('Invalid `--num` option: %r.', args['--num'])
    for record in islice(avro_file, n_records):
      print dumps(record, indent=2)
  elif args['--sample']:
    num = args['--num']
    frq = args['--freq']
    if frq:
      try:
        freq = float(frq)
      except ValueError:
        raise HdfsError('Invalid `--freq` option: %r.', args['--freq'])
      for record in avro_file:
        if random() <= freq:
          print dumps(record)
    else:
      try:
        n_records = int(num)
      except ValueError:
        raise HdfsError('Invalid `--num` option: %r.', num)
      for record in islice(avro_file, n_records):
        print dumps(record)

if __name__ == '__main__':
  main()
