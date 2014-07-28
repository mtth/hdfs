#!/usr/bin/env python
# encoding: utf-8

"""Test HdfsAvro extension."""

from hdfs.ext.avro import *
from helpers import _TestSession
from nose.tools import *


class TestWriter(_TestSession):

  def setup(self):
    super(TestWriter, self).setup()
    self.writer = AvroWriter(self.client, 'aw.avro')

  def test_simple(self):
    self.writer.records.send({'key': 'value1'})
    self.writer.records.send({'key': 'value2', 'bar': 'that'})
    self.writer.records.close()
    data = self.client._open('aw.avro').content
    ok_('value1' in data)
    ok_('value2' in data)
