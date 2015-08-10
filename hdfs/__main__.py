#!/usr/bin/env python
# encoding: utf-8

"""HdfsCLI: a command line interface for WebHDFS.

Usage:
  hdfscli [interactive] [-v...] [-a ALIAS]
  hdfscli download [-fsa ALIAS] [-v...] [-t THREADS] HDFS_PATH LOCAL_PATH
  hdfscli upload [-sa ALIAS] [-v...] [-A|-f] [-t THREADS] LOCAL_PATH HDFS_PATH
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
  -a ALIAS --alias=ALIAS        Alias of namenode to connect to.
  -f --force                    Allow overwriting any existing files.
  -s --silent                   Don't display progress status.
  -t THREADS --threads=THREADS  Number of threads to use for parallelization.
                                0 allocates a thread per file. [default: 0]
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).

Examples:
  hdfscli -a prod /user/foo
  hdfscli download features.avro dat/
  hdfscli download logs/1987-03-23 - >>logs
  hdfscli upload -f - data/weights.tsv <weights.tsv

HdfsCLI exits with return status 1 if an error occurred and 0 otherwise.

"""

from . import __version__
from .client import Client
from .util import Config, HdfsError, catch
from docopt import docopt
from logging.handlers import TimedRotatingFileHandler
from tempfile import gettempdir
from threading import Lock
import logging as lg
import os
import os.path as osp
import sys


class CliConfig(Config):

  """CLI specific configuration.

  :param command: The command to load the configuration for. All options will
    be looked up in the `[COMMAND]` section.
  :param verbosity: Stream handler log level: 0 for error, 1 for warnings, 2
    for info, 3 and above for debug. A negative value will disable logging
    handlers entirely (useful for tests).
  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty. If not specified, the value of the
    `HDFSCLI_CONFIG` environment variable is used if it exists, otherwise it
    defaults to `~/.hdfscli.cfg`.

  """

  def __init__(self, command, verbosity=0, path=None):
    Config.__init__(self, path=path)
    self._command = command
    self._root_logger = lg.getLogger()
    if verbosity >= 0:
      self._setup_logging(verbosity)

  def create_client(self, alias=None):
    """Load HDFS client.

    :param alias: The client to look up. If not specified, the default alias in
      the command's section will be used (`default.alias`), or an error will be
      raised if the key doesn't exist.

    """
    if not alias:
      if not self.has_option(self.global_section, 'default.alias'):
        raise HdfsError('No alias specified and no default alias found.')
      alias = self.get(self.global_section, 'default.alias')
    return Client.from_alias(alias, path=self.path)

  def get_file_handler(self):
    """Get current logfile, if any."""
    handlers = [
      handler
      for handler in self._root_logger.handlers
      if isinstance(handler, lg.FileHandler)
    ]
    if handlers:
      return handlers[0]

  def _setup_logging(self, verbosity):
    """Configure logging with optional time-rotating file handler."""
    # Patch `request_kerberos`'s logger (it's very verbose).
    lg.getLogger('requests_kerberos.kerberos_').setLevel(lg.INFO)
    # Setup root logger.
    self._root_logger.setLevel(lg.DEBUG)
    # Stderr logging.
    stream_handler = lg.StreamHandler()
    if verbosity:
      stream_level = {1: lg.WARNING, 2: lg.INFO}.get(verbosity, lg.DEBUG)
    else:
      stream_level = lg.ERROR
    stream_handler.setLevel(stream_level)
    fmt = '%(levelname)s\t%(message)s'
    stream_handler.setFormatter(lg.Formatter(fmt))
    self._root_logger.addHandler(stream_handler)
    # File handling.
    key = 'log.disable'
    section = '%s.command' % (self._command, )
    if not self.has_option(section, key) or not self.getboolean(section, key):
      if self.has_option(section, 'log.path'):
        path = self.get(section, 'log.path')
      else:
        path = osp.join(gettempdir(), '%s.log' % (section, ))
      file_handler = TimedRotatingFileHandler(
        path,
        when='midnight', # daily backups
        backupCount=1,
        encoding='utf-8',
      )
      fmt = '%(asctime)s\t%(name)-16s\t%(levelname)-5s\t%(message)s'
      file_handler.setFormatter(lg.Formatter(fmt))
      if self.has_option(section, 'log.level'):
        file_handler.setLevel(getattr(lg, self.get(section, 'log.level')))
      else:
        file_handler.setLevel(lg.DEBUG)
      self._root_logger.addHandler(file_handler)


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
    # TODO: Improve lock granularity.
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


def parse_arg(args, name, parser, separator=None):
  """Parse command line argument, raising an appropriate error on failure.

  :param args: Arguments dictionary.
  :param name: Name of option to look up.
  :param parser: Function to parse option.
  :param separator: For parsing lists.

  """
  value = args[name]
  if not value:
    return
  try:
    if separator and separator in value:
      return [parser(part) for part in value.split(separator) if part]
    else:
      return parser(value)
  except ValueError:
    raise HdfsError('Invalid %r option: %r.', name, args[name])

@catch(HdfsError)
def main(argv=None, client=None):
  """Entry point."""
  args = docopt(__doc__, argv=argv, version=__version__)
  config = CliConfig('hdfscli', args['--verbose'])
  if args['--log']:
    handler = config.get_file_handler()
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      sys.stdout.write('No log file active.\n')
    sys.exit(0)
  client = client or config.create_client(args['--alias']) # Hook for testing.
  hdfs_path = args['HDFS_PATH']
  local_path = args['LOCAL_PATH']
  n_threads = parse_arg(args, '--threads', int)
  force = args['--force']
  silent = args['--silent']
  chunk_size = 1 << 16 # 65kB to avoid calling progress too often.
  if args['download']:
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
        # TODO: Add progress tracking here.
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
    banner = (
      '\n'
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
      # import pdb; pdb.set_trace()
      embed(banner1=banner, user_ns=namespace)

if __name__ == '__main__':
  main()
