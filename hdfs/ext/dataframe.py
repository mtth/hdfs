#!/usr/bin/env python
# encoding: utf-8

"""Dataframe: an HdfsCLI extension for reading and writing `pandas` dataframes.
Dataframes can be read and written into HDFS files stored in one of two formats:
either Avro or CSV (comma separated values, though other separators such as TAB
also commonly used).

This extension requires the presence of `pandas` and `numpy` libraries.  For 
reading Avro files, the `fastavro` library is necessary.  This extension
supports downloading multiple HDFS part files in parallel.

"""

from __future__ import absolute_import
from ..client import Client
from ..util import HdfsError

import time
import os
import posixpath
import sys
import itertools
import operator
import tempfile
import io
import subprocess
import logging

import gzip
import avro

from multiprocessing.pool import ThreadPool

try:
  import pandas as pd
  import numpy as np
except ImportError:
  raise HdfsError('pandas and numpy libraries needed for dataframe extension')

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
    raise HdfsError('Dont know Avro equivalent of type %s' % dtype)





def read_df(client, hdfs_path, format, use_gzip = False, sep = '\t', 
  index_cols = None, num_threads = None, local_dir = None):
  """Function to read in pandas `DataFrame` from a remote HDFS file.

  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param format: Indicates format of remote file, currently either `'avro'` 
    or `'csv'`.
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only 
    available for `'csv'` format.
  :param sep: Separator to use for `'csv'` file format.
  :param index_cols: Which columns of remote file should be made index columns
    of `pandas` dataframe.  If set to `None`, `pandas` will create a row
    number index.
  :param num_threads: Number of threads in which to parallelize downloading of 
    parts of HDFS file.  This can speed up 
    downloading speed significantly. A value of `None` or `1` indicates that 
    parallelization will not be used.  A value of `-1` indicates creates as
    many tasks as there are parts to the HDFS file.
  :param local_dir: Local directory in which to save downloaded files. If 
    set to `None`, a temporary directory will be used.  Otherwise, if remote
    files already exist in the local directory and are older than downloaded
    version, they will not be downloaded again.

  E.g.:

  .. code-block:: python

    df = read_df(client, '/tmp/data.tsv', 'csv', num_threads=-1)

  """

  # Function to download a file from HDFS and save locally
  def _do_download(hdfs_file, local_name):

    local_name_partial = local_name + '.partial'

    try: 

      dir_name = os.path.dirname(local_name)
      if not os.path.exists(dir_name):
        logging.info("Creating directory %s", dir_name)
        os.makedirs(dir_name)

      client.download(
          hdfs_file, local_name_partial, overwrite=True, chunk_size=1024*1024)

      os.rename(local_name_partial, local_name)

    finally:
      if os.path.exists(local_name_partial):
        os.remove(local_name_partial)


  def _get_local_filename(hdfs_file):
    # Convert a remote HDFS filename into a local filename
    return os.path.join(local_dir, hdfs_file.replace('/', '_'))


  def _download_files():
    # Download file parts in directory

    TEMPORARY_NAME = '_temporary'

    file_infos = dict(client.walk(hdfs_path, depth=1))

    all_parts      = []
    download_parts = []
    for hdfs_file in sorted(file_infos.keys()):
      if file_infos[hdfs_file]['type'] != 'FILE': 
        continue
      if posixpath.basename(hdfs_file) == TEMPORARY_NAME:
        continue

      file_dict  = file_infos[hdfs_file]
      local_name = _get_local_filename(hdfs_file)

      all_parts.append((hdfs_file, local_name))

      if os.path.exists(local_name):
        if 1000*os.path.getmtime(local_name) < file_dict['modificationTime']:
          logging.info('%s already exists, but older than HDFS version', 
            local_name)
        elif os.path.getsize(local_name) != file_dict['length']:
          logging.info('%s already exists, but file sizes differ', local_name)
        else:
          logging.info('%s already exists, skipping', local_name)
          continue

      download_parts.append((hdfs_file, local_name))

      logging.info("Will save %s to %s [%dMB]", hdfs_file, local_name, 
        round(file_dict['length']/(1024*1024))) 
   
    logging.info('Downloading %d files', len(download_parts))

    use_num_threads = len(to_download) if num_threads == -1 else num_threads
    if use_num_threads is not None and use_num_threads > 1:
      logging.info('(running in parallel, %d threads)', use_num_threads)
      p = ThreadPool(use_num_threads)
      call_download_func = lambda x: _do_download(*x)
      p.map(call_download_func, download_parts)

    else:
      for args in to_download:
        _do_download(*args)

    return all_parts


  def _process_csv(saved_files):
    # Loads downloaded csv files and return a pandas dataframe

    PIG_HEADER = '.pig_header'
    PIG_SCHEMA = '.pig_schema'

    header_files = [local_name 
      for hdfs_name, local_name, _ in saved_files 
      if posixpath.basename(hdfs_name) == PIG_HEADER]
    data_files   = sorted([local_name 
      for hdfs_name, local_name, _ in saved_files 
      if posixpath.basename(hdfs_name) not in (PIG_HEADER, PIG_SCHEMA) ])

    if len(header_files) != 1:
      if len(header_files) > 1:
        raise HdfsError('More than 1 header file found!')
      else:
        raise HdfsError('No header file found!')
    header_file = header_files[0]

    if not data_files:
      raise HdfsError('No data files found')

    merged_files = io.BytesIO()
    with open(header_file, 'r') as f:
      header_data = f.read()
      merged_files.write(header_data)

    
    contents = []
    for filename in data_files:
      with open(filename, 'rb') as f:
        contents.append( f.read() )

    for filename in data_files:
      s = time.time()
      with open(filename, 'rb') as f:
        contents = f.read()
        if use_gzip:
          contents = gzip_decompress(contents)
        merged_files.write(contents)
      delay = time.time() - s
      logging.info('Loaded %s in %0.3fs', filename, delay)


    merged_files.seek(0)

    logging.info("Loading dataframe")

    df = pd.io.parsers.read_csv(merged_files, sep=sep)
    if index_cols is not None:
      df = df.set_index(index_cols)

    merged_files.close()

    return df


  def _process_avro(saved_files):

    # Loads downloaded avro files and returns a pandas dataframe

    try:
      import fastavro
    except ImportError:
      raise HdfsError('Need to have fastavro library installed ' + 
                      'in order to dataframes from Avro files')


    open_files = []
    try:
      for filename in saved_files:
        logging.info("Opening %s", filename)
        open_files.append( open(filename,'rb') )

      reader_chain = itertools.chain(*[fastavro.reader(f) for f in open_files])
      df = pd.DataFrame.from_records(reader_chain, index=index_cols)

    finally:
      for f in open_files: 
        f.close()

    return df

  if format == 'csv':
    logging.info('Loading %s CSV formatted data from %s', 
      'compressed' if use_gzip else 'uncompressed',
      hdfs_path)
    if sep is None:
      raise HdfsError('sep parameter must not be None for CSV reading')
    process_function = _process_csv

  elif format == 'avro':
    logging.info('Loading Avro formatted data from %s', hdfs_path)
    if use_gzip:
      raise HdfsError('Cannot use gzip compression with Avro format')
    process_function = _process_avro
  else:
    raise Exception('Unkown data format %s' % format)

  if local_dir is None:
    local_dir = tempfile.mkdtemp()
    logging.info('local_dir not specified, using %s', local_dir)

  t = time.time()

  saved_files = _download_files()
  df = process_function(saved_files)

  logging.info('Done in %0.3f', time.time() - t)
  
  return df


def write_df(df, client, hdfs_path, format, use_gzip = False, sep = '\t', 
  do_overwrite = False, rows_per_part = -1):
  """Function to write a pandas `DataFrame` to a remote HDFS file.

  :param df: `pandas` dataframe object to write.
  :param client: :class:`hdfs.client.Client` instance.
  :param hdfs_path: Remote path.
  :param format: Indicates format of remote file, currently either `'avro'` 
    or `'csv'`.
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only 
    available for `'csv'` format.
  :param sep: Separator to use for `'csv'` file format.
  :param do_overwrite: Whether to overwrite files on HDFS if they exist.
  :param rows_per_part: Indicates how to split dataframe into separate part
    files on HDFS.  If set to `-1`, then a single part file will be created
    containing all dataframe rows.  Otherwise, a set of part files will be 
    created, with each part file containing `rows_per_part` rows.

  E.g.:

  .. code-block:: python

    df = pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
         {'A' : 11, 'B' : 23}])
    write_df(df, client, '/tmp/data.tsv', 'csv')

  """

  def _get_csv_use_index(df):
    # Only include index columns in the CSV output if they do not seem to be a
    # row-id automatically added by `pandas` (resulting in a single,
    # unnamed index column)
    r = len(df.index.names) > 1 or df.index.names[0] is not None
    return r

  def _get_csv(df):
    use_index = _get_csv_use_index(df)
    r = df.to_csv(sep=sep, header=False, index=use_index)
    if use_gzip:
      r = gzip_compress(r)
    return r

  def _finish_csv(df):
    use_index = _get_csv_use_index(df)
    header = df[0:0].to_csv(sep=sep, header=True, index=use_index).strip()
    header_hdfs_filename = posixpath.join(hdfs_path, '.pig_header')
    client.write(header_hdfs_filename, header + "\n", overwrite=True)

  def _get_avro(df):

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

  def _finish_avro(df):
    pass


  if format == 'csv':

    logging.info('Writing CSV formatted data to %s', hdfs_path)

    process_function = _get_csv
    finish_function  = _finish_csv

  elif format == 'avro':

    logging.info('Writing Avro formatted data to %s', hdfs_path)

    if use_gzip:
      raise HdfsError('Cannot use gzip compression with Avro format')

    process_function = _get_avro
    finish_function  = _finish_avro

  else:
    raise ValueError('Unkown data format %s' % format)

  t = time.time()

  try:
    client.status(hdfs_path)
    if do_overwrite:
      client.delete(hdfs_path, recursive=True)
    else:
      raise HdfsError('%s already exists' % hdfs_path)
  except HdfsError:
    pass


  if rows_per_part == -1:
    rows_per_part = len(df)
  for part_num, start_ndx in enumerate(range(0, len(df), rows_per_part)):
    end_ndx = min(start_ndx + rows_per_part, len(df))
    client.write(
        posixpath.join(hdfs_path, 'part-r-%05d' % part_num), 
        process_function(df.iloc[start_ndx:end_ndx]), 
        overwrite=False)

  finish_function(df)

  logging.info('Done in %0.3f', time.time() - t)
  
  return df
