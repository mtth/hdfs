#!/usr/bin/env python
# encoding: utf-8

"""Test HdfsAvro extension."""

from helpers import _TestSession
from nose.tools import *
try:
  from hdfs.ext.avro import *
except ImportError:
  AvroTypeException = Exception
  SKIP = True
else:
  from avro.io import AvroTypeException
  SKIP = False


class TestWriter(_TestSession):

  def setup(self):
    if SKIP:
      self.client = None
    super(TestWriter, self).setup()
    # if we reach here, self.client is defined
    self.writer = AvroWriter(self.client, 'aw.avro')

  def test_write_inferring_schema(self):
    with self.writer as writer:
      self.writer.records.send({'foo': 'value1'})
      self.writer.records.send({'foo': 'value2', 'bar': 'that'})
    data = self.client._open('aw.avro').content
    ok_('foo' in data)
    ok_('value1' in data)
    ok_('value2' in data)
    ok_(not 'that' in data)

  @raises(AvroTypeException)
  def test_invalid_schema(self):
    with self.writer as writer:
      self.writer.records.send({'foo': 'value1'})
      self.writer.records.send({'bar': 'value2'})
