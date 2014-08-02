#!/usr/bin/env python
# encoding: utf-8

"""Test Hdfs Dataframe extension."""

from helpers import _TestSession
import os
import shutil
import tempfile
from hdfs.util import HdfsError
from nose.tools import raises
try:
  from hdfs.ext.dataframe import *
except ImportError:
  SKIP = True
else:
  from pandas.util.testing import assert_frame_equal
  import pandas as pd
  import numpy
  SKIP = False


class TestDataframe(_TestSession):

  def setup(self):
    if SKIP:
      self.client = None
    super(TestDataframe, self).setup()
    self.test_df = pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
        {'A' : 11, 'B' : 23},
        {'A' :100, 'B' :211},
        {'A' : 23, 'B' :  1}])

  def run_write_read(self, df, format, use_gzip = False, sep = '\t', 
      index_cols = None, local_dir = None, n_threads = None,
      hdfs_filename = 'dfreader_test', read_hdfs_filename=None, n_parts = 2):

    # Location on HDFS
    ext = format + ('.gz' if use_gzip else '')

    write_df(df, self.client, hdfs_filename, format, sep=sep, use_gzip=use_gzip, 
      overwrite=True, n_parts=n_parts)

    if read_hdfs_filename is not None:
      r_filename = read_hdfs_filename
    else:
      r_filename = hdfs_filename
    returned_df = read_df(self.client, r_filename, format, sep=sep, 
      use_gzip=use_gzip, index_cols=index_cols, local_dir=local_dir, 
      n_threads=n_threads)

    assert_frame_equal(df, returned_df)
    return returned_df

  def test_csv(self):
    self.run_write_read(self.test_df, format='csv')

  def test_sep_suffix(self):
    self.run_write_read(self.test_df, format='csv', 
      hdfs_filename='dfreader_test/test/')

  def test_dir_with_single_part(self):
    self.run_write_read(self.test_df, format='csv', n_parts=1)

  @raises(HdfsError)
  def test_single_file(self):
    self.run_write_read(self.test_df, format='csv', 
      hdfs_filename='dfreader_test/test',
      read_hdfs_filename='dfreader_test/test/part-r-00000')

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
    self.run_write_read(self.test_df, 'csv', n_threads = -1)
