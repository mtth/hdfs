#!/usr/bin/env python
# encoding: utf-8

"""Test Dataframe extension."""

from hdfs.util import HdfsError, temppath
from json import loads
from test.util import _IntegrationTest
import os.path as osp

try:
  from hdfs.ext.avro import AvroReader
  from hdfs.ext.dataframe import read_dataframe, write_dataframe
  from pandas.testing import assert_frame_equal
  import pandas as pd
except ImportError:
  SKIP = True
else:
  SKIP = False


class _DataFrameIntegrationTest(_IntegrationTest):

  dpath = osp.join(osp.dirname(__file__), 'dat')
  records = None
  df = None

  @classmethod
  def setup_class(cls):
    if SKIP:
      return
    super(_DataFrameIntegrationTest, cls).setup_class()
    with open(osp.join(cls.dpath, 'weather.jsonl')) as reader:
      cls.records = [loads(line) for line in reader]
      cls.df = pd.DataFrame.from_records(cls.records)


class TestReadDataFrame(_DataFrameIntegrationTest):

  def test_read(self):
    self.client.upload('weather.avro', osp.join(self.dpath, 'weather.avro'))
    assert_frame_equal(
      read_dataframe(self.client, 'weather.avro'),
      self.df
    )


class TestWriteDataFrame(_DataFrameIntegrationTest):

  def test_write(self):
    write_dataframe(self.client, 'weather.avro', self.df)
    with AvroReader(self.client, 'weather.avro') as reader:
      assert list(reader) == self.records


class TestReadWriteDataFrame(_DataFrameIntegrationTest):

  def test_column_order(self):
    # Column order should be preserved, not just alphabetical.
    df = self.df[['temp', 'station', 'time']]
    write_dataframe(self.client, 'weather-ordered.avro', df)
    assert_frame_equal(
      read_dataframe(self.client, 'weather-ordered.avro'),
      df
    )
