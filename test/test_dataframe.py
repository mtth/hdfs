#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs Dataframe extension."""

import shutil
import os
import tempfile


from hdfs.ext.dataframe import read_df, write_df

from nose.plugins.skip import SkipTest

import pandas as pd
from pandas.util.testing import assert_frame_equal

from helpers import _TestSession


class TestDataframe(_TestSession):

  def setup(self):
    super(TestDataframe, self).setup()

  @staticmethod
  def get_df():
    return pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
         {'A' : 11, 'B' : 23},
         {'A' :100, 'B' :211},
         {'A' : 23, 'B' :  1}])

  @classmethod
  def local_temp_dir(cls):
    local_dir = tempfile.mkdtemp()
    if os.path.exists(local_dir):
      shutil.rmtree(local_dir)
    os.mkdir(local_dir)
    return local_dir

  def run_write_read(
      self, df, format, 
      use_gzip   = False, 
      sep        = '\t', 
      index_cols = None, 
      local_dir = None, 
      num_tasks  = None):

    ext = format + ('.gz' if use_gzip else '')
    f = '/tmp/akolchin/dfreader_test/test.' + ext

    write_df(
      df, self.client, f, 'csv', 
      sep=sep, use_gzip=use_gzip, do_overwrite=True, rows_per_part=2)

    returned_df = read_df(
      self.client, f, 'csv', 
      sep=sep, use_gzip=use_gzip, index_cols=index_cols, 
      local_dir=local_dir, num_tasks=num_tasks)

    assert_frame_equal(df, returned_df)
    return returned_df

  def test_csv(self):
    self.run_write_read(self.get_df(), format='csv')

  def test_csv_gz(self):
    self.run_write_read(self.get_df(), format='csv', use_gzip=True)

  def test_csv_comma(self):
    self.run_write_read(self.get_df(), format='csv', sep=',')

  def test_csv_index(self):
    df = self.get_df().set_index('A')
    self.run_write_read(df, format='csv', index_cols = ['A'])

  def test_avro(self):
    self.run_write_read(self.get_df(), format='avro')

  def test_avro_index(self):
    df = self.get_df().set_index('A')
    self.run_write_read(df, format='avro', index_cols = ['A'])

  def test_local_tmp_path(self):
    df = self.get_df()
    returned_df = self.run_write_read(df, 'csv', self.local_temp_dir())
    # Should be cached now
    self.run_write_read(returned_df, 'csv', self.local_temp_dir())

  def test_parallel_download(self):
    try:
      import joblib
    except ImportError:
      raise SkipTest

    self.run_write_read(self.get_df(), 'csv', num_tasks = -1)

   