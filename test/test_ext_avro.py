#!/usr/bin/env python
# encoding: utf-8

"""Test Avro extension."""

from hdfs.util import temppath
from nose.plugins.skip import SkipTest
from nose.tools import *
from util import _IntegrationTest
import os

try:
  from hdfs.ext.avro import _SeekableReader, read, write
except ImportError:
  SKIP = True
else:
  SKIP = False


class _AvroTestSession(_IntegrationTest):

  @classmethod
  def setup_class(cls):
    if not SKIP:
      super(_AvroTestSession, cls).setup_class()


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
        eq_(sreader.read(3), b'abc')
        eq_(sreader.read(2), b'd')
        ok_(not sreader.read(1))

  def test_buffered_read(self):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write('abcdefghi')
      with open(tpath) as reader:
        sreader = _SeekableReader(reader, 3)
        eq_(sreader.read(1), b'a')
        eq_(sreader.read(3), b'bcd')
        sreader.seek(-3, os.SEEK_CUR)
        eq_(sreader.read(2), b'bc')
        eq_(sreader.read(6), b'defghi')
        ok_(not sreader.read(1))


class TestInferSchema(object):

  pass


class TestRead(_AvroTestSession):

  pass


class TestWriter(_AvroTestSession):

  @nottest
  def test_write_inferring_schema(self):
    with self.writer as writer:
      self.writer.records.send({'foo': 'value1'})
      self.writer.records.send({'foo': 'value2', 'bar': 'that'})
    data = self.client._open('aw.avro').content
    ok_('foo' in data)
    ok_('value1' in data)
    ok_('value2' in data)
    ok_(not 'that' in data)

  @nottest
  def test_invalid_schema(self):
    with self.writer as writer:
      self.writer.records.send({'foo': 'value1'})
      self.writer.records.send({'bar': 'value2'})

  def test_write_read(self):
    schema = {
      'doc': 'A weather reading.',
      'name': 'Weather',
      'namespace': 'test',
      'type': 'record',
      'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
      ],
    }
    records = [
      {'station': '011990-99999', 'temp': 0, 'time': 1433269388},
      {'station': '011990-99999', 'temp': 22, 'time': 1433270389},
      {'station': '011990-99999', 'temp': -11, 'time': 1433273379},
      {'station': '012650-99999', 'temp': 111, 'time': 1433275478},
    ]
    write(self.client, 'weather.avro', records, schema=schema)
    with read(self.client, 'weather.avro') as reader:
      eq_(reader.schema, schema)
      eq_(list(reader), records)
