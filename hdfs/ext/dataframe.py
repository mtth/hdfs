#!/usr/bin/env python
# encoding: utf-8

"""Dataframe: an HdfsCLI extension for reading and writing `pandas` dataframes.

Dataframes can be read and written into HDFS files stored in one of two
formats: either Avro or CSV (comma separated values, though other separators
such as TAB also commonly used).

This extension requires the presence of `pandas` and `numpy` libraries.  For
reading Avro files, the `fastavro` library is necessary.  This extension
supports downloading multiple HDFS part files in parallel.

"""

from __future__ import absolute_import
from ..client import Client
from ..util import HdfsError
from itertools import chain
from multiprocessing.pool import ThreadPool
import avro
import gzip
import io
import logging
import math
import numpy as np
import operator
import os
import os.path as osp
import pandas as pd
import posixpath
import shutil
import subprocess
import sys
import tempfile
import time


logger = logging.getLogger(__name__)


def gzip_compress(uncompressed):
  """Utility function that compresses its input using gzip.

  :param uncompressed: String to compress.

  """

  compressed = io.BytesIO()
  with gzip.GzipFile(fileobj=compressed, mode='w') as f:
    f.write(uncompressed)
  return compressed.getvalue()

def gzip_decompress(compressed):
  """Utility function that decompresses its gzipped-input.

  :param compressed: Bytes to uncompress.

  """

  # zcat substantially faster than built-in gzip library
  p = subprocess.Popen(
      ['zcat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
  out, _ = p.communicate(compressed)

  return out

def convert_dtype(dtype):
  """Utility function to convert `numpy` datatypes to their Avro equivalents.

  """
  if np.issubdtype(dtype, np.floating):
    return 'float'
  elif np.issubdtype(dtype, np.integer) \
      or np.issubdtype(dtype, np.unsignedinteger):
    return 'int'
  elif np.issubdtype(dtype, np.character):
    return 'string'
  elif np.issubdtype(dtype, np.bool_):
    return 'boolean'
  else:
    raise HdfsError('Dont know Avro equivalent of type %r.', dtype)


def read_df(client, hdfs_path, format, use_gzip = False, sep = '\t',
  index_cols = None, n_threads = -1, local_dir = None, overwrite=False):
  """Function to read in pandas `DataFrame` from a remote HDFS file.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path, must be a partitioned file.
  :param format: Indicates format of remote file, currently either `'avro'`
    or `'csv'`.
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only
    available for `'csv'` format.
  :param sep: Separator to use for `'csv'` file format.
  :param index_cols: Which columns of remote file should be made index columns
    of `pandas` dataframe.  If set to `None`, `pandas` will create a row
    number index.
  :param n_threads: Number of threads to use for parallel downloading of
    part-files. A value of `None` or `1` indicates that parallelization won't
    be used; `-1` (default) uses as many threads as there are part-files.
  :param local_dir: Local directory in which to save downloaded files. If
    set to `None`, a temporary directory will be used.  Otherwise, if remote
    files already exist in the local directory and are older than downloaded
    version, they will not be downloaded again.

  E.g.:

  .. code-block:: python

    df = read_df(client, '/tmp/data.tsv', 'csv', n_threads=-1)

  """
  is_temp_dir = False

  remote_status = client.status(hdfs_path)
  if remote_status['type'] == 'FILE':
    raise HdfsError('Remote location %r must be a directory containing ' +
      'part-files', hdfs_path)

  try:
    if local_dir is None:
      local_dir = tempfile.mkdtemp()
      logger.info('local_dir not specified, using %r.', local_dir)

    if format == 'csv':
      logger.info('Loading %r CSV formatted data from %r',
        'compressed' if use_gzip else 'uncompressed',
        hdfs_path)

      if sep is None:
        raise HdfsError('sep parameter must not be None for CSV reading.')

      def _process_function(data_files):
          # Loads downloaded csv files and return a pandas dataframe

          PIG_CSV_HEADER = '.pig_header'
          hdfs_header_file = posixpath.join(hdfs_path, PIG_CSV_HEADER)
          client.download(hdfs_header_file, local_dir, overwrite=overwrite)
          merged_files = io.BytesIO()
          with open(osp.join(local_dir, PIG_CSV_HEADER), 'r') as f:
            merged_files.write(f.read())

          for filename in data_files:
            s = time.time()
            with open(filename, 'rb') as f:
              contents = f.read()
              if use_gzip:
                contents = gzip_decompress(contents)
              merged_files.write(contents)
            delay = time.time() - s
            logger.info('Loaded %s in %0.3fs', filename, delay)

          merged_files.seek(0)

          logger.info("Loading dataframe")

          df = pd.io.parsers.read_csv(merged_files, sep=sep)
          if index_cols is not None:
            df = df.set_index(index_cols)

          merged_files.close()

          return df

    elif format == 'avro':
      logger.info('Loading Avro formatted data from %r', hdfs_path)
      if use_gzip:
        raise HdfsError('Cannot use gzip compression with Avro format.')

      def _process_function(data_files):
        # Loads downloaded avro files and returns a pandas dataframe

        try:
          import fastavro
        except ImportError:
          raise HdfsError('Need to have fastavro library installed ' +
                          'in order to dataframes from Avro files.')

        open_files = []
        try:
          for f in data_files:
            logger.info("Opening %s", f)
            open_files.append( open(f,'rb') )

          reader_chain = chain(*[fastavro.reader(f) for f in open_files])
          df = pd.DataFrame.from_records(reader_chain, index=index_cols)

        finally:
          for f in open_files:
            f.close()

        return df

    else:
      raise ValueError('Unknown data format %s' % (format,) )


    t = time.time()

    lpath = client.download(
      hdfs_path, local_dir, n_threads=n_threads, overwrite=overwrite
    )

    data_files = [osp.join(lpath, fname) for fname in os.listdir(lpath)]
    df = _process_function(data_files)

    logger.info('Done in %0.3f', time.time() - t)

  finally:
    if is_temp_dir:
      shutil.rmtree(local_dir)

  return df


def write_df(df, client, hdfs_path, format, use_gzip = False, sep = '\t',
  overwrite = False, n_parts = 1):
  """Function to write a pandas `DataFrame` to a remote HDFS file.

  :param df: `pandas` dataframe object to write.
  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param format: Indicates format of remote file, currently either `'avro'`
    or `'csv'`.
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only
    available for `'csv'` format.
  :param sep: Separator to use for `'csv'` file format.
  :param overwrite: Whether to overwrite files on HDFS if they exist.
  :param n_parts: Indicates into how many part-files to split the dataframe.

  E.g.:

  .. code-block:: python

    df = pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
         {'A' : 11, 'B' : 23}])
    write_df(df, client, '/tmp/data.tsv', 'csv')

  """
  try:
    import pandas as pd
  except ImportError:
    raise HdfsError('pandas library needed for dataframe extension.')

  # Include index columns in output if they do not seem to be a
  # row-id automatically added by `pandas` (resulting in a single,
  # unnamed index column)
  if len(df.index.names) > 1 or df.index.names[0] is not None:
    df = df.reset_index()

  parts_ext = ''
  if format == 'csv':

    def _process_function(df):
      r = df.to_csv(sep=sep, header=False, index=False)
      if use_gzip:
        r = gzip_compress(r)
      return r

    def _finish_function(df):
      header = df[0:0].to_csv(sep=sep, header=True, index=False).strip()
      header_hdfs_filename = posixpath.join(hdfs_path, '.pig_header')
      client.write(header_hdfs_filename, header + "\n", overwrite=True)

    if use_gzip:
      parts_ext = '.gz'
    logger.info('Writing CSV formatted data to %s', hdfs_path)

  elif format == 'avro':

    def _process_function(df):
      fields_str = [
          '{"name": "%s", "type": "%s"}' % (fldname, convert_dtype(dtype))
          for fldname, dtype in zip(df.columns, df.dtypes)]
      schema_str = """\
    { "type": "record",
      "name": "dfrecord",
      "fields": [
      """ + ",\n".join(fields_str) + """
      ]
    }"""
      schema = avro.schema.parse(schema_str)
      # Create a 'record' (datum) writer
      rec_writer  = avro.io.DatumWriter(schema)
      out_buffer  = io.BytesIO()
      # Create a 'data file' (avro file) writer
      avro_writer = avro.datafile.DataFileWriter(
          out_buffer,
          rec_writer,
          writers_schema = schema)
      for r in df.to_dict(outtype='records'):
        avro_writer.append(r)
      avro_writer.flush()
      r = out_buffer.getvalue()
      avro_writer.close()
      return r

    def _finish_function(df):
      pass

    if use_gzip:
      raise HdfsError('Cannot use gzip compression with Avro format.')
    parts_ext = '.avro'
    logger.info('Writing Avro formatted data to %r', hdfs_path)

  else:
    raise ValueError('Unkown data format %r.' % (format,) )

  t = time.time()

  try:
    client.status(hdfs_path)
    if overwrite:
      client.delete(hdfs_path, recursive=True)
    else:
      raise HdfsError('%r already exists.', hdfs_path)
  except HdfsError:
    pass

  num_rows = len(df)
  rows_per_part = int(math.ceil(num_rows / float(n_parts)))
  for part_num, start_ndx in enumerate(range(0, num_rows, rows_per_part)):
    end_ndx = min(start_ndx + rows_per_part, num_rows)
    client.write(
        posixpath.join(hdfs_path, ('part-r-%05d' % part_num) + parts_ext),
        _process_function(df.iloc[start_ndx:end_ndx]),
        overwrite=False)

  _finish_function(df)

  logger.info('Done in %0.3f', time.time() - t)

