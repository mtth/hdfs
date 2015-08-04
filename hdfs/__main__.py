#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfscli [interactive] [-a ALIAS]
  hdfscli download [-fsa ALIAS] [-t THREADS] HDFS_PATH LOCAL_PATH
  hdfscli upload [-sa ALIAS] [-A | -f] [-t THREADS] LOCAL_PATH HDFS_PATH
  hdfscli -h | -L | -V

Commands:
  download                      Download a file or folder from HDFS. If a
                                single file is downloaded, - can be
                                specified as LOCAL_PATH to stream it to
                                standard out.
  interactive                   Start the client and expose it via the python
                                interpreter (using iPython if available).
  upload                        Upload a file or folder to HDFS. - can be
                                specified as LOCAL_PATH to read from standard
                                in.

Arguments:
  HDFS_PATH                     Remote HDFS path.
  LOCAL_PATH                    Path to local file or directory.

Options:
  -A --append                   Append data to an existing file. Only supported
                                if uploading a single file or from standard in.
  -L --log                      Show path to current log file and exit.
  -V --version                  Show version and exit.
  -a ALIAS --alias=ALIAS        Alias, defaults to alias pointed to by
                                default.alias in ~/.hdfsrc.
  -f --force                    Allow overwriting any existing files.
  -s --silent                   Don't display progress status.
  -t THREADS --threads=THREADS  Number of threads to use for parallelization.
                                0 allocates a thread per file. [default: 0]

Examples:
  hdfscli -a prod /user/foo
  hdfscli download features.avro dat/
  hdfscli download logs/1987-03-23 - >>logs
  hdfscli upload -f - data/weights.tsv <weights.tsv

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from hdfs import __version__
from hdfs.client import Client
from hdfs.util import Config, HdfsError, catch
from threading import Lock
import logging as lg
import os
import os.path as osp
import sys


class _Progress(object):

  """Progress tracker callback.

  :param nbytes: Total number of bytes that will be transferred.
  :param nfiles: Total number of files that will be transferred.

  """

  def __init__(self, nbytes, nfiles):
    self._total_bytes = nbytes
    self._pending_files = nfiles
    self._downloading_files = 0
    self._complete_files = 0
    self._lock = Lock()
    self._data = {}

  def __call__(self, hdfs_path, nbytes):
    # TODO: improve lock granularity.
    with self._lock:
      data = self._data
      if hdfs_path not in data:
        self._pending_files -= 1
        self._downloading_files += 1
      if nbytes == -1:
        self._downloading_files -= 1
        self._complete_files += 1
      else:
        data[hdfs_path] = nbytes
      if self._pending_files + self._downloading_files > 0:
        sys.stderr.write(
          '%3.1f%%\t[ pending: %d | downloading: %d | complete: %d ]   \r' %
          (
            100. * sum(data.values()) / self._total_bytes,
            self._pending_files,
            self._downloading_files,
            self._complete_files,
          )
        )
      else:
        sys.stderr.write('%79s\r' % ('', ))

  @classmethod
  def from_hdfs_path(cls, client, hdfs_path):
    """Instantiate from remote path.

    :param client: HDFS client.
    :param hdfs_path: HDFS path.

    """
    content = client.content(hdfs_path)
    return cls(content['length'], content['fileCount'])

  @classmethod
  def from_local_path(cls, local_path):
    """Instantiate from a local path.

    :param local_path: Local path.

    """
    if osp.isdir(local_path):
      nbytes = 0
      nfiles = 0
      for dpath, _, fnames in os.walk(local_path):
        for fname in fnames:
          nbytes += osp.getsize(osp.join(dpath, fname))
          nfiles += 1
    elif osp.exists(local_path):
      nbytes = osp.getsize(local_path)
      nfiles = 1
    else:
      raise HdfsError('No file found at: %s', local_path)
    return cls(nbytes, nfiles)


@catch(HdfsError)
def main():
  """Entry point."""
  args = docopt(__doc__, version=__version__)
  # Set up logging.
  logger = lg.getLogger('hdfs')
  logger.setLevel(lg.DEBUG)
  handler = Config().get_file_handler('hdfscli')
  if handler:
    logger.addHandler(handler)
  # Set up client and fix arguments.
  client = Client.from_alias(args['--alias'])
  hdfs_path = args['HDFS_PATH']
  local_path = args['LOCAL_PATH']
  force = args['--force']
  silent = args['--silent']
  chunk_size = 1 << 16 # 65kB to avoid calling progress too often.
  try:
    n_threads = int(args['--threads'])
  except ValueError:
    raise HdfsError('Invalid `--threads` option: %r.', args['--threads'])
  # Run command.
  if args['--log']:
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      raise HdfsError('No log file active.')
  elif args['download']:
    if local_path == '-':
      if not sys.stdout.isatty() and sys.stderr.isatty() and not silent:
        progress = _Progress.from_hdfs_path(client, hdfs_path)
      else:
        progress = None
      with client.read(
        hdfs_path,
        chunk_size=chunk_size,
        progress=progress,
      ) as reader:
        for chunk in reader:
          sys.stdout.write(chunk)
    else:
      if sys.stderr.isatty() and not silent:
        progress = _Progress.from_hdfs_path(client, hdfs_path)
      else:
        progress = None
      client.download(
        hdfs_path,
        local_path,
        overwrite=force,
        n_threads=n_threads,
        chunk_size=chunk_size,
        progress=progress,
      )
  elif args['upload']:
    append = args['--append']
    if local_path == '-':
      client.write(
        hdfs_path,
        (line for line in sys.stdin), # Doesn't work with stdin.
        append=append,
        overwrite=force,
      )
    else:
      if append:
        # TODO: add progress tracking here.
        if osp.isfile(local_path):
          with open(local_path) as reader:
            client.write(hdfs_path, reader, append=True)
        else:
          raise HdfsError('Can only append when uploading a single file.')
      else:
        if sys.stderr.isatty() and not silent:
          progress = _Progress.from_local_path(local_path)
        else:
          progress = None
        client.upload(
          hdfs_path,
          local_path,
          overwrite=force,
          n_threads=n_threads,
          chunk_size=chunk_size,
          progress=progress,
        )
  else:
    logger.setLevel(lg.WARNING) # iPython likes to print all logged messages.
    banner = (
      'Welcome to the interactive HDFS python shell.\n'
      'The HDFS client is available as `CLIENT`.\n'
    )
    namespace = {'CLIENT': client}
    try:
      from IPython import embed
    except ImportError:
      from code import interact
      interact(banner=banner, local=namespace)
    else:
      embed(banner1=banner, user_ns=namespace)

if __name__ == '__main__':
  main()
