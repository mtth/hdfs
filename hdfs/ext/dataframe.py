#!/usr/bin/env python
# encoding: utf-8

"""Dataframe: an HdfsCLI extension for reading and writing `pandas` dataframes.

"""

from __future__ import absolute_import
from ..client import Client
from ..util import HdfsError

import time
import os
import sys
import itertools
import operator
from collections import OrderedDict
import io
import subprocess

import avro
import gzip

try:
  import pandas as pd
  import numpy as np
except ImportError:
  raise HdfsError('pandas and numpy libraries needed for dataframe extension')


def gzip_compress(uncompressed):
  """Utility function that compresses its input using gzip

  :param uncompressed: String to compress
  """

  compressed = io.BytesIO()
  with gzip.GzipFile(fileobj = compressed, mode = 'w') as f:
    f.write(uncompressed)
  return compressed.getvalue()

def gzip_decompress(compressed):
  """Utility function that decompresses its gzipped-input

  :param compressed: Bytes to uncompress
  """

  # zcat substantially faster than built-in gzip library
  p = subprocess.Popen(
      ['zcat'],stdin=subprocess.PIPE, stdout=subprocess.PIPE)
  out, _ = p.communicate(compressed)

  return out

def _verbose_print(verbose, msg):
  # Print error message if verbose flag is set to True
  if verbose:
    print '#', msg

# Function to download a file from HDFS and save locally
# TODO: Move into client code?
# Can't be nested inside another function so that joblib can pickle it
def _do_download(client, local_path, hdfs_path, file_dict, verbose = False):
  save_name = local_path + hdfs_path.replace('/', '_')
  save_name_partial = save_name + '.partial'

  try: # print our own exception messages, because joblib swallows exceptions

    do_download = True
    if os.path.exists(save_name):
      _verbose_print(verbose, '%s already exists' % save_name)
      if 1000*os.path.getmtime(save_name) > file_dict['modificationTime']:
        do_download = False
      else:
        _verbose_print(verbose, '... but older than HDFS version')

    if do_download:
      total_size = file_dict['length']

      _verbose_print(verbose, "Saving from %s to %s [%dMB]" % \
            (hdfs_path, save_name, round(total_size/(1024*1024))) )
 
      dir_name = os.path.dirname(save_name)
      if not os.path.exists(dir_name):
        _verbose_print(verbose, "Creating directory %s" % dir_name)
        os.makedirs(dir_name)

      client.download(
          hdfs_path, save_name_partial, overwrite=True, chunk_size=1024*1024)

      os.rename(save_name_partial, save_name)

    sys.stdout.flush()

  except Exception, e:
    raise HdfsError(str(e))

  return save_name 

def read_df(
    client, hdfs_path, format, 
    use_gzip   = False, 
    sep        = '\t', 
    index_cols = None,
    num_tasks  = None, 
    local_path = None, 
    verbose    = False):
  """Function to read in pandas `DataFrame` from a remote HDFS file

  :param client: :class:`hdfs.client.Client` instance
  :param hdfs_path: Remote path
  :param format: Indicates format of remote file, currently either `'avro'` 
    or `'csv'`
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only 
    available for `'csv'` format
  :param sep: Separator to use for `'csv'` file format
  :param index_cols: Which columns of remote file should be made index columns
    of `pandas` dataframe.  If set to `None`, `pandas` will create a row
    number index
  :param num_tasks: Number of tasks in which to parallelize downloading of 
    parts of HDFS file using the `joblib` library.  This can speed up 
    downloading speed significantly. A value of `None` or `1` indicates that 
    parallelization will not be used.  A value of `-1` indicates creates as
    many tasks as there are parts to the HDFS file
  :param local_path: Local directory in which to save downloaded files. If 
    set to `None`, a temporary directory will be used.  Otherwise, if remote
    files already exist in the local directory and are older than downloaded
    version, they will not be downloaded again
  :param verbose: Whether to enable printing of extra debugging information

  E.g.:

  .. code-block:: python

    df = read_df(client, '/tmp/data.tsv', 'csv', num_tasks=-1, verbose=True)

  """

  def _run_in_parallel(func, args_list, passed_num_tasks, backend="threading"):
    # Utility function to launch multiple tasks if necessary using joblib 
    if passed_num_tasks is not None:
      try:
        import joblib
      except ImportError:
        raise HdfsError('Need to have joblib library installed ' +
                        'in order to run jobs in parallel')

    if passed_num_tasks == -1:
      local_num_tasks = len(args_list)
    else:
      local_num_tasks = passed_num_tasks

    if local_num_tasks is not None and local_num_tasks > 1:

      _verbose_print(verbose, '   (running in parallel)')

      task_wrapper = joblib.delayed
      task_runner  = joblib.Parallel(
          n_jobs=local_num_tasks, verbose=False, backend=backend)

    else:
      task_wrapper = lambda x: x
      task_runner  = lambda x: x

    return task_runner( task_wrapper(func)(*args) for args in args_list )



  def _download_files():
    # Download file parts in directory
    parts = dict(
        (fileName, fileInfo) 
        for fileName, fileInfo in client.walk(hdfs_path, depth=1)
        if fileInfo['type'] == 'FILE' )

    # Sort file parts by name
    parts = OrderedDict(sorted(parts.items(), key=operator.itemgetter(0)))

    _verbose_print(verbose, 'Downloading %d files' % len(parts))

    file_names = _run_in_parallel(_do_download, [
        (client, local_path, file_name, file_dict, verbose) 
        for (file_name, file_dict) in parts.iteritems()
        ], num_tasks, "threading")

    return file_names


  def _process_csv(saved_files):
    # Loads downloaded csv files and return a pandas dataframe

    PIG_HEADER = '_.pig_header'
    PIG_SCHEMA = '_.pig_schema'

    header_files = [f for f in saved_files if f[-len(PIG_HEADER):]==PIG_HEADER]
    data_files   = sorted([
        f for f in saved_files 
        if f[-len(PIG_HEADER):]!=PIG_HEADER 
        and f[-len(PIG_SCHEMA):]!=PIG_SCHEMA])

    if len(header_files) != 1:
      if len(header_files) > 1:
        raise HdfsError('More than 1 header file found!')
      else:
        raise HdfsError('No header file found!')
    header_file = header_files[0]

    if not len(data_files):
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
      _verbose_print(verbose, 'Loaded %s in %0.3fs' % (filename, delay))


    merged_files.seek(0)

    _verbose_print(verbose, "Loading dataframe")

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
    for filename in saved_files:
      _verbose_print(verbose, "Opening %s" % filename)
      open_files.append( open(filename,'rb') )

    reader_chain = itertools.chain(*[fastavro.reader(f) for f in open_files])
    df = pd.DataFrame.from_records(reader_chain, index=index_cols)

    for f in open_files: 
      f.close()

    return df


  # *****************************************
  # ***** MAIN FUNCTION BODY of read_df *****
  # *****************************************

  if hdfs_path[-1] != '/':
    hdfs_path = hdfs_path + '/'

  if format == 'csv':
    _verbose_print(verbose, 'Loading CSV formatted data from %s' % hdfs_path)
    if sep is None:
      raise HdfsError('sep parameter must be not None for CSV reading')
    process_function = _process_csv

  elif format == 'avro':
    _verbose_print(verbose, 'Loading Avro formatted data from %s' % hdfs_path)
    if use_gzip:
      raise HdfsError('Cannot use gzip compression with Avro format')
    process_function = _process_avro
  else:
    raise Exception('Unkown data format %s' % format)

  if local_path is None:
    import tempfile
    local_path = tempfile.mkdtemp()
    _verbose_print(verbose, "local_path not specified, using %s" % local_path)

  t = time.time()

  TEMPORARY_NAME = '_temporary'

  saved_files = _download_files()
  saved_files = [
      f for f in saved_files 
      if f[-len(TEMPORARY_NAME):] != TEMPORARY_NAME]

  df = process_function(saved_files)

  _verbose_print(verbose, 'Done in %0.3f' % (time.time() - t))
  
  return df



def write_df(
    df, client, hdfs_path, format, 
    use_gzip   = False, 
    sep        = '\t', 
    do_overwrite = False, 
    rows_per_part = -1, 
    verbose = False):
  """Function to write a pandas `DataFrame` to a remote HDFS file

  :param df: `pandas` dataframe object to write
  :param client: :class:`hdfs.client.Client` instance
  :param hdfs_path: Remote path
  :param format: Indicates format of remote file, currently either `'avro'` 
    or `'csv'`
  :param use_gzip: Whether remote file is gzip-compressed or not.  Only 
    available for `'csv'` format
  :param sep: Separator to use for `'csv'` file format
  :param do_overwrite: Whether to overwrite files on HDFS if they exist
  :param rows_per_part: Indicates how to split dataframe into separate part
    files on HDFS.  If set to `-1`, then a single part file will be created
    containing all dataframe rows.  Otherwise, a set of part files will be 
    created, with each part file containing `rows_per_part` rows
  :param verbose: Whether to enable printing of extra debugging information

  E.g.:

  .. code-block:: python

    df = pd.DataFrame.from_records(
        [{'A' :  1, 'B' :  2},
         {'A' : 11, 'B' : 23}])
    write_df(df, client, '/tmp/data.tsv', 'csv', verbose=True)

  """

  def _get_csv_use_index(df):
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
    client.write(hdfs_path + '.pig_header', header + "\n", overwrite=True)

  def _get_avro(df):

    def convert_dtype(dtype):
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
        raise Exception('Dont know Avro equivalent of type %s' % dtype)


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


  # *******************************************
  # ***** MAIN FUNCTION BODY FOR write_df *****
  # *******************************************
  if hdfs_path[-1] != '/':
    hdfs_path = hdfs_path + '/'

  if format == 'csv':

    _verbose_print(verbose, 'Writing CSV formatted data to %s' % hdfs_path)

    process_function = _get_csv
    finish_function  = _finish_csv

  elif format == 'avro':

    _verbose_print(verbose, 'Writing Avro formatted data to %s' % hdfs_path)

    if use_gzip:
      raise HdfsError('Cannot use gzip compression with Avro format')

    process_function = _get_avro
    finish_function  = _finish_avro

  else:
    raise Exception('Unkown data format %s' % format)

  t = time.time()

  try:
    client.status(hdfs_path)
    if do_overwrite:
      client.delete(hdfs_path, recursive=True)
    else:
      raise Exception('%s already exists' % hdfs_path)
  except HdfsError:
    pass


  if rows_per_part == -1:
    rows_per_part = len(df)
  for part_num, start_ndx in enumerate(range(0, len(df), rows_per_part)):
    end_ndx = min(start_ndx + rows_per_part, len(df))
    client.write(
        hdfs_path + 'part-r-%05d' % part_num, 
        process_function(df.iloc[start_ndx:end_ndx]), 
        overwrite=False)

  finish_function(df)

  _verbose_print(verbose, 'Done in %0.3f' % (time.time() - t))
  
  return df

