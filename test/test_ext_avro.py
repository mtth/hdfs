#!/usr/bin/env python
# encoding: utf-8

"""Test Avro extension."""

from hdfs.util import HdfsError, temppath
from json import dumps, load, loads
from nose.plugins.skip import SkipTest
from nose.tools import eq_, ok_, raises
from util import _IntegrationTest
import os
import os.path as osp
import sys

try:
  from hdfs.ext.avro import (_SeekableReader, _infer_schema, AvroReader,
    AvroWriter)
  from hdfs.ext.avro.__main__ import main
except ImportError:
  SKIP = True
else:
  SKIP = False


class TestSeekableReader(object):

  def setup(self):
    if SKIP:
      raise SkipTest

  def test_normal_read(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('abcd')
      with open(tpath) as reader:
        sreader = _SeekableReader(reader)
        eq_(sreader.read(3), 'abc')
        eq_(sreader.read(2), 'd')
        ok_(not sreader.read(1))

  def test_buffered_read(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('abcdefghi')
      with open(tpath) as reader:
        sreader = _SeekableReader(reader, 3)
        eq_(sreader.read(1), 'a')
        eq_(sreader.read(3), 'bcd')
        sreader.seek(-3, os.SEEK_CUR)
        eq_(sreader.read(2), 'bc')
        eq_(sreader.read(6), 'defghi')
        ok_(not sreader.read(1))


class TestInferSchema(object):

  def _assert_schemas_equal(self, schema1, schema2):
    # Only works for flat records.
    ok_('fields' in schema1 and 'fields' in schema2)
    fields = lambda schema: sorted(schema['fields'], key=lambda f: f['name'])
    eq_(fields(schema1), fields(schema2))

  def test_flat_record(self):
    self._assert_schemas_equal(
      _infer_schema({'foo': 1, 'bar': 'hello'}),
      {
        'type': 'record',
        'fields': [
          {'type': 'int', 'name': 'foo'},
          {'type': 'string', 'name': 'bar'},
        ]
      }
    )

  def test_array(self):
    self._assert_schemas_equal(
      _infer_schema({'foo': 1, 'bar': ['hello']}),
      {
        'type': 'record',
        'fields': [
          {'type': 'int', 'name': 'foo'},
          {'type': {'type': 'array', 'items': 'string'}, 'name': 'bar'},
        ]
      }
    )


class _AvroIntegrationTest(_IntegrationTest):

  dpath = osp.join(osp.dirname(__file__), 'dat')
  schema = None
  records = None

  @classmethod
  def setup_class(cls):
    if SKIP:
      return
    super(_AvroIntegrationTest, cls).setup_class()
    with open(osp.join(cls.dpath, 'weather.avsc')) as reader:
      cls.schema = loads(reader.read())
    with open(osp.join(cls.dpath, 'weather.jsonl')) as reader:
      cls.records = [loads(line) for line in reader]

  @classmethod
  def _get_data_bytes(cls, fpath):
    # Get Avro bytes, skipping header (order of schema fields is undefined) and
    # sync marker. This assumes that the file can be written in a single block.
    with open(fpath, 'rb') as reader:
      reader.seek(-16, os.SEEK_END) # Sync marker always last 16 bytes.
      sync_marker = reader.read()
      reader.seek(0)
      content = reader.read()
      sync_pos = content.find(sync_marker)
      return content[sync_pos + 16:-16]


class TestRead(_AvroIntegrationTest):

  def test_read(self):
    self.client.upload('weather.avro', osp.join(self.dpath, 'weather.avro'))
    with AvroReader(self.client, 'weather.avro') as reader:
      eq_(list(reader), self.records)


class TestWriter(_AvroIntegrationTest):

  def test_write(self):
    writer = AvroWriter(
      self.client,
      'weather.avro',
      schema=self.schema,
    )
    with writer:
      for record in self.records:
        writer.write(record)
    with temppath() as tpath:
      self.client.download('weather.avro', tpath)
      eq_(
        self._get_data_bytes(osp.join(self.dpath, 'weather.avro')),
        self._get_data_bytes(tpath)
      )

  def test_write_in_multiple_blocks(self):
    writer = AvroWriter(
      self.client,
      'weather.avro',
      schema=self.schema,
      sync_interval = 1 # Flush block on every write.
    )
    with writer:
      for record in self.records:
        writer.write(record)
    with AvroReader(self.client, 'weather.avro') as reader:
      eq_(list(reader), self.records)

  def test_write_empty(self):
    with AvroWriter(self.client, 'empty.avro', schema=self.schema):
      pass
    with AvroReader(self.client, 'empty.avro') as reader:
      eq_(reader.schema, self.schema)
      eq_(list(reader), [])

  @raises(HdfsError)
  def test_write_overwrite_error(self):
    # To check that the background `AsyncWriter` thread doesn't hang.
    self.client.makedirs('weather.avro')
    with AvroWriter(self.client, 'weather.avro', schema=self.schema) as writer:
      for record in self.records:
        writer.write(record)

  def test_infer_schema(self):
    with AvroWriter(self.client, 'weather.avro') as writer:
      for record in self.records:
        writer.write(record)
    with AvroReader(self.client, 'weather.avro') as reader:
      eq_(list(reader), self.records)


class TestMain(_AvroIntegrationTest):

  def test_schema(self):
    self.client.upload('weather.avro', osp.join(self.dpath, 'weather.avro'))
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        main(['schema', 'weather.avro'], client=self.client, stdout=writer)
      with open(tpath) as reader:
        schema = load(reader)
      eq_(self.schema, schema)

  def test_read(self):
    self.client.upload('weather.avro', osp.join(self.dpath, 'weather.avro'))
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        main(
          ['read', 'weather.avro', '--num', '2'],
          client=self.client,
          stdout=writer
        )
      with open(tpath) as reader:
        records = [loads(line) for line in reader]
      eq_(records, self.records[:2])

  def test_read_part_file(self):
    data = {
      'part-m-00000.avro': [{'name': 'jane'}, {'name': 'bob'}],
      'part-m-00001.avro': [{'name': 'john'}, {'name': 'liz'}],
    }
    for fname, records in data.items():
      with AvroWriter(self.client, 'data.avro/%s' % (fname, )) as writer:
        for record in records:
          writer.write(record)
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        main(
          ['read', 'data.avro', '--parts', '1,'],
          client=self.client,
          stdout=writer
        )
      with open(tpath) as reader:
        records = [loads(line) for line in reader]
      eq_(records, data['part-m-00001.avro'])

  def test_write(self):
    with open(osp.join(self.dpath, 'weather.jsonl')) as reader:
      main(
        [
          'write', 'weather.avro',
          '--schema', dumps(self.schema),
          '--codec', 'null',
        ],
        client=self.client,
        stdin=reader
      )
    with temppath() as tpath:
      self.client.download('weather.avro', tpath)
      eq_(
        self._get_data_bytes(tpath),
        self._get_data_bytes(osp.join(self.dpath, 'weather.avro'))
      )

  def test_write_codec(self):
    with open(osp.join(self.dpath, 'weather.jsonl')) as reader:
      main(
        [
          'write', 'weather.avro',
          '--schema', dumps(self.schema),
          '--codec', 'deflate',
        ],
        client=self.client,
        stdin=reader
      )
    # Correct content.
    with AvroReader(self.client, 'weather.avro') as reader:
      records = list(reader)
    eq_(records, self.records)
    # Different size (might not be smaller, since very small file).
    compressed_size = self.client.content('weather.avro')['length']
    uncompressed_size = osp.getsize(osp.join(self.dpath, 'weather.avro'))
    ok_(compressed_size != uncompressed_size)
