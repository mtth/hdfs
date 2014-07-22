#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs Dataframe extension."""

import shutil
import os
import tempfile


from nose.plugins.skip import SkipTest
from helpers import _TestSession

SKIP = False

try:
  import numpy
  import pandas as pd

except ImportError:
  SKIP = True
  pass

if not SKIP:
  from hdfs.ext.dataframe import read_df, write_df
  from pandas.util.testing import assert_frame_equal

class TestDataframe(_TestSession):

  def setup(self):
    if SKIP:
      raise SkipTest

    super(TestDataframe, self).setup()

    self.test_df = pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
         {'A' : 11, 'B' : 23},
         {'A' :100, 'B' :211},
         {'A' : 23, 'B' :  1}])

  def run_write_read(self, df, format, use_gzip = False, sep = '\t', 
      index_cols = None, local_dir = None, num_threads = None):

    # Location on HDFS
    ext = format + ('.gz' if use_gzip else '')
    f = '/tmp/akolchin/dfreader_test/test.' + ext

    write_df(df, self.client, f, 'csv', sep=sep, use_gzip=use_gzip, 
      overwrite=True, num_parts=2)

    returned_df = read_df(self.client, f, 'csv', sep=sep, use_gzip=use_gzip, 
      index_cols=index_cols, local_dir=local_dir, num_threads=num_threads)

    assert_frame_equal(df, returned_df)
    return returned_df

  def test_csv(self):
    self.run_write_read(self.test_df, format='csv')

  def test_csv_gz(self):
    self.run_write_read(self.test_df, format='csv', use_gzip=True)

  def test_csv_comma(self):
    self.run_write_read(self.test_df, format='csv', sep=',')

  def test_csv_index(self):
    df = self.test_df.copy().set_index('A')
    self.run_write_read(df, format='csv', index_cols = ['A'])

  def test_avro(self):
    self.run_write_read(self.test_df, format='avro')

  def test_avro_index(self):
    df = self.test_df.copy().set_index('A')
    self.run_write_read(df, format='avro', index_cols = ['A'])

  def test_local_tmp_path(self):
    temp1 = tempfile.mkdtemp()
    try:
      returned_df = self.run_write_read(self.test_df, 'csv', temp1)

      temp2 = tempfile.mkdtemp()
      try:
        # Should be cached now
        self.run_write_read(returned_df, 'csv', temp2)

      finally:
        shutil.rmtree(temp2)

    finally:
      shutil.rmtree(temp1)

  def test_parallel_download(self):
    self.run_write_read(self.test_df, 'csv', num_threads = -1)
